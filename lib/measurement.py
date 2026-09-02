"""Shared pre-import quality gates for automation and force-import.

The auto-import path (lib.download_processing.process_completed_album), the force-import
path (lib.dispatch.dispatch_import_from_db), all MUST run the same quality gates:
audio integrity and spectral transcode
detection. The only gate that differs between paths is the beets *distance*
check — that is what --force on import_one.py overrides. Every other gate is
shared, so it lives here in a single function.

Rationale: force-import previously called dispatch_import_core() directly,
skipping the audio + spectral gates that ``process_completed_album()`` now
runs before handing off to the shared auto-import seam. A transcode rejected
by auto-import's spectral gate could be force-imported into beets, replacing
an existing copy of the same quality with no real upgrade. See the
"No Parallel Code Paths" rule in
.claude/rules/code-quality.md.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

import msgspec

from lib.audio_hash import AudioHashError, hash_audio_content
from lib.json_narrow import json_dict as _json_dict
from lib.json_narrow import json_list as _json_list
from lib.media_readiness import MediaReadinessError, inspect_media

# Extensions audio_hash.py currently knows how to hash. AUDIO_EXTS is broader
# (includes wav, alac); the bad-hash gate filters to this subset so legitimate
# wav/alac albums don't trip a per-track warning every validation cycle.
_BAD_HASH_SUPPORTED_EXTS: frozenset[str] = frozenset({"flac", "mp3", "m4a", "aac", "ogg", "opus"})
from lib.quality import (
    AacLatticeCapture,
    AudioValidationMeasurementError,
    AudioValidationReport,
    CdRipBitVerification,
    CodecFamily,
    SpectralAnalysisDetail,
    SpectralDetail,
    SpectralMeasurement,
    SpectralTrackDetail,
    legacy_unrecorded_audio_validation_report,
)
from lib.util import validate_audio

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db import BadAudioHashRow

logger = logging.getLogger("cratedigger")


class BadHashGateDB(Protocol):
    """The exact pipeline-DB surface the bad-hash gate uses.

    Narrow port (the #1277 ``DispatchDB`` pattern): the concrete
    ``PipelineDB`` and ``FakePipelineDB`` both satisfy it structurally, so
    tests drive the gate without bridging through ``Any``. Extend only when
    measurement itself calls a new DB method.
    """

    def has_any_bad_audio_hashes(self) -> bool: ...

    def lookup_bad_audio_hash(
        self,
        hash_value: bytes,
        audio_format: str,
    ) -> BadAudioHashRow | None: ...


# Lazy import proxy — keeps sox out of import-time deps.
def spectral_analyze(folder: str, trim_seconds: int = 30) -> Any:
    """Proxy to spectral_check.analyze_album (lazy import).

    Callers inside lib.measurement must use this proxy so patches on
    ``lib.measurement.spectral_analyze`` take effect.
    """
    from lib.spectral_check import analyze_album
    return analyze_album(folder, trim_seconds=trim_seconds)


def measure_aac_lattice(folder: str) -> AacLatticeCapture:
    """Proxy to aac_lattice.measure_album_aac_lattice (lazy import).

    Lazy for the same reason ``spectral_analyze`` is: it keeps numpy and the
    detector's per-rate table construction out of the import-time footprint
    of every web request, CLI invocation, and pipeline cycle that never
    measures a lattice.
    """
    from lib.aac_lattice import measure_album_aac_lattice
    return measure_album_aac_lattice(folder)


AacLatticeMeasureFn = Callable[[str], AacLatticeCapture]
CdRipVerifyFn = Callable[[str, "CratediggerConfig"], CdRipBitVerification | None]

# The promotion-plausible cohort, and the ONLY reason this expensive
# measurement is gated at all (issue #829 AAC-lattice leg, design comment
# https://github.com/abl030/cratedigger/issues/829#issuecomment-5144283616):
# an AAC launder that survives the spectral gate is exactly the album the
# lattice can still see. Album grades are today genuine/suspect/
# likely_transcode/error, so this reads as "spectrally clean"; ``marginal``
# is listed because the per-track vocabulary carries it and an album grade
# must never silently fall out of the gated cohort if it ever surfaces.
AAC_LATTICE_GATED_SPECTRAL_GRADES: frozenset[str] = frozenset(
    {"genuine", "marginal"}
)


def analyze_spectral_audit_path(path: str) -> SpectralAnalysisDetail:
    """Analyze one path into display-only attempt audit evidence."""
    grade: str | None = None
    bitrate_kbps: int | None = None
    suspect_pct: float | None = None
    per_track: list[SpectralTrackDetail] = []
    cliff_hz: int | None = None
    codec_family: CodecFamily | None = None
    ultrasonic_deficit_db: float | None = None
    spectral_measurement_version: int | None = None
    try:
        result = spectral_analyze(path, trim_seconds=30)
        grade = result.grade
        bitrate_kbps = result.estimated_bitrate_kbps
        suspect_pct = result.suspect_pct
        cliff_hz = result.cliff_hz
        codec_family = result.codec_family
        ultrasonic_deficit_db = result.ultrasonic_deficit_db
        spectral_measurement_version = result.spectral_measurement_version
        for track in result.tracks:
            per_track.append(SpectralTrackDetail(
                grade=track.grade,
                hf_deficit_db=round(track.hf_deficit_db, 1),
                cliff_detected=track.cliff_detected,
                cliff_freq_hz=track.cliff_freq_hz,
                estimated_bitrate_kbps=track.estimated_bitrate_kbps,
                error=getattr(track, "error", None),
            ))
    except Exception as exc:
        logger.exception("SPECTRAL AUDIT: failed for %s", path)
        return SpectralAnalysisDetail(
            attempted=True,
            grade=grade,
            bitrate_kbps=bitrate_kbps,
            suspect_pct=suspect_pct,
            per_track=per_track,
            error=f"{type(exc).__name__}: {exc}",
            cliff_hz=cliff_hz,
            codec_family=codec_family,
            ultrasonic_deficit_db=ultrasonic_deficit_db,
            spectral_measurement_version=spectral_measurement_version,
        )
    return SpectralAnalysisDetail(
        attempted=True,
        grade=grade,
        bitrate_kbps=bitrate_kbps,
        suspect_pct=suspect_pct,
        per_track=per_track,
        cliff_hz=cliff_hz,
        codec_family=codec_family,
        ultrasonic_deficit_db=ultrasonic_deficit_db,
        spectral_measurement_version=spectral_measurement_version,
    )


def collect_attempt_spectral_audit(
    candidate_path: str,
    existing_path: str | None,
) -> SpectralDetail:
    """Measure candidate and exact-release installed files independently."""
    candidate = analyze_spectral_audit_path(candidate_path)
    existing = (
        analyze_spectral_audit_path(existing_path)
        if existing_path is not None
        else SpectralAnalysisDetail(attempted=False)
    )
    return SpectralDetail(candidate=candidate, existing=existing)


SpectralDetailAnalyzer = Callable[[str], SpectralAnalysisDetail]


@dataclass(frozen=True)
class ExistingSpectralAuditLookup:
    """Exact-release path, policy bitrate, and fail-soft lookup audit."""

    path: str | None = None
    min_bitrate_kbps: int | None = None
    failure: SpectralAnalysisDetail | None = None


ExistingSpectralResolver = Callable[
    [str],
    ExistingSpectralAuditLookup,
]


def _fail_soft_spectral_analysis(
    path: str,
    analyzer: SpectralDetailAnalyzer,
) -> SpectralAnalysisDetail:
    try:
        return analyzer(path)
    except Exception as exc:
        logger.exception("SPECTRAL AUDIT: failed for %s", path)
        return SpectralAnalysisDetail(
            attempted=True,
            error=f"{type(exc).__name__}: {exc}",
        )


def collect_release_attempt_spectral_audit(
    candidate_path: str,
    mb_release_id: str,
    *,
    existing_spectral_evidence: SpectralAnalysisDetail,
    preserve_existing_source_spectral: bool,
    analyzer: SpectralDetailAnalyzer,
    existing_resolver: ExistingSpectralResolver,
    candidate_detail: SpectralAnalysisDetail | None = None,
    existing_detail: SpectralAnalysisDetail | None = None,
) -> tuple[SpectralDetail, ExistingSpectralAuditLookup]:
    """Own conditional HAVE collection for every attempted-import adapter.

    A lossless source converted to Opus/V0 keeps the source-side spectral
    measurement as its authoritative HAVE provenance; analyzing that installed
    derivative can rewrite a transcode-like FLAC as apparently genuine.
    Content-addressed candidate and HAVE facts may be projected through
    ``candidate_detail`` / ``existing_detail`` after their callers prove the
    respective snapshots still match; otherwise the exact paths are analyzed.
    """
    candidate = (
        candidate_detail
        if candidate_detail is not None
        else _fail_soft_spectral_analysis(candidate_path, analyzer)
    )
    if existing_detail is not None:
        return (
            SpectralDetail(candidate=candidate, existing=existing_detail),
            ExistingSpectralAuditLookup(),
        )
    try:
        lookup = (
            existing_resolver(mb_release_id)
            if mb_release_id
            else ExistingSpectralAuditLookup()
        )
    except Exception as exc:
        logger.exception("SPECTRAL AUDIT: exact-release lookup failed")
        lookup = ExistingSpectralAuditLookup(
            failure=SpectralAnalysisDetail(
                attempted=True,
                error=f"{type(exc).__name__}: {exc}",
            ),
        )
    if preserve_existing_source_spectral:
        existing = existing_spectral_evidence
    elif lookup.failure is not None:
        existing = lookup.failure
    elif lookup.path is not None:
        existing = _fail_soft_spectral_analysis(lookup.path, analyzer)
    else:
        existing = SpectralAnalysisDetail(attempted=False)
    return SpectralDetail(candidate=candidate, existing=existing), lookup


def resolve_existing_spectral_audit(
    mb_release_id: str,
    cfg: CratediggerConfig,
) -> ExistingSpectralAuditLookup:
    """Resolve exact-release files, preserving lookup failure as audit data."""
    if not mb_release_id:
        return ExistingSpectralAuditLookup()
    from lib.beets_db import BeetsDB

    try:
        with BeetsDB(library_root=getattr(cfg, "beets_directory", "")) as beets:
            existing_info = beets.get_album_info(
                mb_release_id,
                cfg.quality_ranks,
            )
        if existing_info is not None:
            return ExistingSpectralAuditLookup(
                path=(existing_info.album_path
                      if os.path.isdir(existing_info.album_path or "") else None),
                min_bitrate_kbps=existing_info.min_bitrate_kbps,
            )
    except Exception as exc:
        logger.exception("SPECTRAL AUDIT: failed to resolve existing exact release")
        return ExistingSpectralAuditLookup(
            failure=SpectralAnalysisDetail(
                attempted=True,
                error=f"{type(exc).__name__}: {exc}",
            ),
        )
    return ExistingSpectralAuditLookup()


def existing_spectral_resolver_for_config(
    cfg: CratediggerConfig,
) -> ExistingSpectralResolver:
    return lambda release_id: resolve_existing_spectral_audit(release_id, cfg)


def spectral_detail_from_persisted_source(
    grade: object,
    bitrate_kbps: object,
    *,
    cliff_hz: int | None = None,
    codec_family: CodecFamily | None = None,
    ultrasonic_deficit_db: float | None = None,
    spectral_measurement_version: int | None = None,
) -> SpectralAnalysisDetail:
    """Project one durable spectral generation into attempt-audit shape."""
    spectral_grade = grade if isinstance(grade, str) and grade else None
    spectral_bitrate = (
        bitrate_kbps if isinstance(bitrate_kbps, int) else None
    )
    return SpectralAnalysisDetail(
        attempted=spectral_grade is not None or spectral_bitrate is not None,
        grade=spectral_grade,
        bitrate_kbps=spectral_bitrate,
        cliff_hz=cliff_hz,
        codec_family=codec_family,
        ultrasonic_deficit_db=ultrasonic_deficit_db,
        spectral_measurement_version=spectral_measurement_version,
    )


def spectral_measurement_from_attempt(
    detail: SpectralAnalysisDetail | None,
) -> SpectralMeasurement | None:
    """Project a successful attempt audit into policy/persistence evidence."""
    if detail is None or not detail.attempted or detail.grade is None:
        return None
    return SpectralMeasurement.from_parts(
        detail.grade,
        detail.bitrate_kbps,
        cliff_hz=detail.cliff_hz,
        codec_family=detail.codec_family,
        ultrasonic_deficit_db=detail.ultrasonic_deficit_db,
        spectral_measurement_version=detail.spectral_measurement_version,
    )


class PreimportMeasurement(msgspec.Struct, frozen=True):
    """Facts gathered by ``measure_preimport_state``. No decision fields.

    The measurement helper has no opinion on accept/reject — it only reports
    what is on disk. The persisted ``AlbumQualityEvidence`` row carries the
    same facts (audio_corrupt, folder_layout, audio_file_count,
    matched_bad_audio_hash_*); the unified decider
    ``lib.quality.full_pipeline_decision_from_evidence`` consumes them as
    early-exit reject branches (U11).

    Persistable fields map directly onto ``AlbumQualityEvidence``. The
    attempt-local ``lossless_candidate`` fact additionally lets preview and
    harness routing reuse the exact classification that selected the scan.
    """
    corrupt_files: list[str] = msgspec.field(default_factory=list[str])
    audio_validation: AudioValidationReport = msgspec.field(
        default_factory=legacy_unrecorded_audio_validation_report,
    )
    audio_corrupt: bool = False
    audio_error: str | None = None
    matched_bad_hash_id: int | None = None
    matched_bad_track_path: str | None = None
    download_spectral: SpectralMeasurement | None = None
    existing_spectral: SpectralMeasurement | None = None
    existing_min_bitrate: int | None = None
    existing_spectral_path: str | None = None
    folder_layout: Literal["flat", "nested"] = "flat"
    audio_file_count: int = 0
    filetype_band: str = ""
    lossless_candidate: bool = False
    min_bitrate_kbps: int | None = None
    is_vbr: bool | None = None
    spectral_audit: SpectralDetail = msgspec.field(default_factory=SpectralDetail)
    # issue #829 AAC-lattice leg PR-A. None means the cohort gate did not
    # fire (or the measurement itself failed outright); a capture with
    # ``scored_tracks == 0`` means it ran and nothing scored.
    aac_lattice: AacLatticeCapture | None = None
    # Positive-only exact CD bit evidence. Absence includes every provider
    # failure/non-match and therefore has no policy effect.
    cd_rip_verification: CdRipBitVerification | None = None


AUDIO_EXTS = ("mp3", "flac", "alac", "m4a", "ogg", "opus", "wav", "aac")


@dataclass
class LocalFileInspection:
    """Result of inspecting audio files on disk at a force-import path.

    Populated by ``inspect_local_files`` so callers of ``measure_preimport_state``
    that have no DownloadFile metadata (force-import paths) can still supply
    filetype / bitrate / vbr hints.

    ``has_nested_audio`` reports whether any audio files were found below the
    root directory. Callers should reject nested layouts early: the
    preimport gates (validate_audio / analyze_album) recurse, but the
    downstream beets harness (``harness/import_one.py``) still uses
    ``os.listdir`` for bitrate measurement and conversion, so a nested
    force-import would pass gates and then produce a misclassified/
    empty measurement in the harness.

    ``is_vbr`` is mutagen's ``bitrate_mode`` — the encoder-written
    Xing/Info/VBRI header, recorded as a persisted fact about the download.
    It is not a gate input: since issue #1145 the spectral gate reads the
    codec alone.
    """
    filetype: str = ""           # comma-separated lowercase extensions
    min_bitrate_bps: int | None = None
    is_vbr: bool | None = None
    has_nested_audio: bool = False


def _canonical_filetype_label(codec: str, container: str, fallback: str) -> str:
    """Project admitted stream facts into the existing filetype vocabulary."""

    if codec in {"flac", "alac", "mp3", "aac", "opus", "vorbis"}:
        return codec
    if codec.startswith("wma"):
        return "wma"
    if container == "wav" and codec.startswith("pcm_"):
        return "wav"
    return fallback


def inspect_local_files(path: str) -> LocalFileInspection:
    """Scan ``path`` recursively for audio files and report filetype + bitrate + VBR hints.

    Walks subdirectories so multi-disc layouts (e.g. ``Album/CD1/*.mp3``)
    classify correctly — otherwise the spectral gate silently skips nested
    force-imports because ``download_filetype`` comes back empty.

    Uses mutagen for MP3 VBR detection; all other bitrate/filetype info comes
    from extensions and file headers. Exceptions are swallowed so a corrupt or
    unreadable file never hard-errors the gate pipeline — the audio gate
    upstream catches those.
    """
    if not os.path.isdir(path):
        return LocalFileInspection()

    extensions: set[str] = set()
    min_bitrate: int | None = None
    any_vbr: bool | None = None
    has_nested_audio = False
    try:
        readiness_by_path = {
            fact.path: fact for fact in inspect_media(path).files
        }
    except MediaReadinessError:
        readiness_by_path = {}

    for root, _dirs, files in os.walk(path):
        for name in files:
            if "." not in name:
                continue
            ext = name.rsplit(".", 1)[-1].lower()
            if ext not in AUDIO_EXTS:
                continue
            if root != path:
                has_nested_audio = True
            full = os.path.join(root, name)
            facts = readiness_by_path.get(os.path.abspath(full))
            # The inventory has already identified the actual codec/container.
            # Keep only malformed-file fallback extension based: a valid AAC
            # named .flac must not enter the lossless lane, and vice versa.
            extensions.add(
                _canonical_filetype_label(facts.codec, facts.container, ext)
                if facts is not None else ext
            )
            if facts is not None and facts.average_bitrate_kbps is not None:
                bitrate = facts.average_bitrate_kbps * 1000
                min_bitrate = (
                    bitrate if min_bitrate is None else min(min_bitrate, bitrate)
                )
            if ext == "mp3":
                try:
                    from mutagen.mp3 import MP3
                    mp3 = MP3(full)
                    br = getattr(mp3.info, "bitrate", None)
                    br_mode = getattr(mp3.info, "bitrate_mode", None)
                    if isinstance(br, int) and br > 0:
                        # The canonical stream fact wins when available; this
                        # fallback keeps VBR classification available for
                        # malformed sources that preview will subsequently
                        # classify through the strict validation contract.
                        observed_bitrate = (
                            facts.average_bitrate_kbps * 1000
                            if facts is not None and facts.average_bitrate_kbps is not None
                            else br
                        )
                        min_bitrate = (
                            observed_bitrate
                            if min_bitrate is None
                            else min(min_bitrate, observed_bitrate)
                        )
                    # mutagen BitrateMode: UNKNOWN=0, CBR=1, VBR=2, ABR=3
                    if br_mode is not None:
                        is_vbr_file = int(br_mode) in (2, 3)
                        any_vbr = is_vbr_file if any_vbr is None else (any_vbr or is_vbr_file)
                except Exception:
                    logger.debug(f"inspect_local_files: failed to read {full}",
                                 exc_info=True)

    return LocalFileInspection(
        filetype=", ".join(sorted(extensions)),
        min_bitrate_bps=min_bitrate,
        is_vbr=any_vbr,
        has_nested_audio=has_nested_audio,
    )


AudioCodecProbe = Callable[[str], str | None]


class AudioCodecProbeError(RuntimeError):
    """Raised when an ambiguous container's codec cannot be measured."""


def ffprobe_first_audio_stream(
    fpath: str,
    entries: str,
) -> dict[str, object] | None:
    """Return the first audio stream's requested ffprobe entries, or None.

    The one ffprobe invocation shape in the repository: ``-select_streams
    a:0``, JSON out, fail-soft to ``None`` on any error. ``entries`` is the
    comma-separated ``-show_entries stream=...`` field list. Callers own
    ``_safe_path`` for peer-controlled filenames and own the narrowing of
    whichever field they asked for.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "a:0",
                "-show_entries", f"stream={entries}", "-of", "json", fpath,
            ],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            return None
        payload: object = json.loads(result.stdout or "{}")
    except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return None

    streams = _json_list(_json_dict(payload).get("streams"))
    if not streams:
        return None
    return _json_dict(streams[0])


def ffprobe_audio_codec_name(fpath: str) -> str | None:
    """Return the first audio stream codec name reported by ffprobe."""
    stream = ffprobe_first_audio_stream(fpath, "codec_name")
    if stream is None:
        return None
    codec = stream.get("codec_name")
    if not isinstance(codec, str):
        return None
    return codec.strip().lower() or None


def has_supported_lossless_audio(
    filetype: str,
    audio_files: Sequence[Path],
    *,
    codec_probe: AudioCodecProbe | None = None,
) -> bool:
    """Identify lossless candidates from an already-enumerated file set.

    FLAC, WAV, and ALAC extensions are unambiguous. M4A is only lossless when
    ffprobe reports an ALAC audio stream; AAC-in-M4A remains a lossy candidate.
    Accepting the caller's paths keeps directory walking and codec probing at
    the measurement boundary instead of repeating them in downstream callers.
    """
    extensions = {
        part.strip().lstrip(".")
        for part in (filetype or "").lower().split(",")
        if part.strip()
    }
    if extensions & {"flac", "wav", "alac"}:
        return True
    if "m4a" not in extensions:
        return False
    probe = codec_probe or ffprobe_audio_codec_name
    codecs: list[str] = []
    for path in sorted(audio_files):
        if path.suffix.lower() != ".m4a":
            continue
        try:
            codec = probe(str(path))
        except Exception as exc:
            raise AudioCodecProbeError(
                f"M4A codec probe failed for {path}: {type(exc).__name__}: {exc}"
            ) from exc
        if codec is None:
            raise AudioCodecProbeError(
                f"M4A codec probe returned no codec for {path}"
            )
        codecs.append(codec.strip().lower())
    return any(codec == "alac" for codec in codecs)


def _needs_spectral_check(
    filetype: str,
    *,
    lossless_candidate: bool,
) -> bool:
    """Decide whether to run spectral analysis as a preimport gate.

    Three rules, all about the CODEC and nothing else:

      - A caller-classified supported lossless source (FLAC, WAV, ALAC,
        including ALAC-in-M4A) → run. Verification requires affirmative
        preview-time spectral evidence. AAC-in-M4A remains lossy.
      - MP3 → run. Every MP3, always.
      - Any other codec → skip; they have no calibrated cliff policy.

    **Measurement decides; no presumption** (issue #1145). This used to skip a
    VBR MP3 whose album average cleared a threshold, on the premise that a
    high-average VBR MP3 is self-evidently genuine. Neither half of that
    premise is evidence about provenance, for two different reasons.
    ``is_vbr`` is a self-declaration: ``inspect_local_files`` reads mutagen's
    ``bitrate_mode``, which reports the Xing/Info/VBRI header the encoder
    wrote. The average IS genuinely measured from the frames — but a
    transcode re-encoded at a high bitrate genuinely HAS a high average, so
    clearing the threshold says nothing about what the audio came from.

    ``lib.quality.gates.spectral_gate_trigger`` is the simulator/Decisions-tab
    mirror of this helper, and it does NOT read the same input: it is handed
    an already-resolved ``codec_family``, which fails closed to ``None`` on a
    mixed-codec album, where the substring test below still says True for any
    filetype naming MP3. That divergence is deliberate and is documented in
    full on the mirror; it only ever makes the mirror withhold an opinion.

    This helper is pure: filesystem enumeration and any M4A codec probe happen
    once at the measurement boundary and arrive as ``lossless_candidate``.
    """
    filetype_lower = (filetype or "").lower()
    if lossless_candidate:
        return True
    return "mp3" in filetype_lower and "flac" not in filetype_lower


@dataclass(frozen=True)
class _BadHashMatch:
    """Result of ``_check_bad_audio_hashes`` on a positive match."""
    bad_hash_id: int
    track_path: str


def _iter_audio_files(path: str) -> list[Path]:
    """List audio files at ``path`` (recursive) suitable for bad-hash hashing.

    Mirrors ``inspect_local_files`` directory walk so the gate sees the same
    set of tracks downstream gates do, including nested multi-disc layouts.
    Files with unsupported extensions are skipped.
    """
    out: list[Path] = []
    if not os.path.isdir(path):
        return out
    for root, _dirs, files in os.walk(path):
        for name in files:
            if "." not in name:
                continue
            ext = name.rsplit(".", 1)[-1].lower()
            if ext not in AUDIO_EXTS:
                continue
            out.append(Path(root) / name)
    return out


def _check_bad_audio_hashes(
    paths: list[Path],
    db: BadHashGateDB,
) -> _BadHashMatch | None:
    """Return the first matched bad-hash row, or None.

    Hashing or DB-lookup failures on a single track are non-fatal: the bad-hash
    gate is a *defense*, not a *requirement*, so a hashing error on one file
    must not block the entire validation pipeline. Each failure is logged at
    WARNING and skipped; the loop continues to the next track.
    """
    for p in paths:
        ext = p.suffix.lstrip(".").lower()
        if not ext or ext not in _BAD_HASH_SUPPORTED_EXTS:
            # alac / wav are in AUDIO_EXTS but audio_hash.py doesn't support
            # them yet; skip silently rather than logging a warning per track
            # for every legitimate album in those formats.
            continue
        try:
            digest = hash_audio_content(p, ext)
        except AudioHashError:
            logger.warning(
                "bad-hash gate: failed to hash %s, skipping", p, exc_info=True)
            continue
        try:
            row = db.lookup_bad_audio_hash(digest, ext)
        except Exception:
            logger.warning(
                "bad-hash gate: lookup failed for %s, skipping", p, exc_info=True)
            continue
        if row is not None:
            return _BadHashMatch(bad_hash_id=row.id, track_path=str(p))
    return None


def _filetype_band(download_filetype: str) -> str:
    """Lowercase, comma-joined filetype band for the measurement Struct.

    Mirrors the existing ``LocalFileInspection.filetype`` shape. Used both by
    the auto path (which gets filetype from slskd) and the measurement helper
    when no caller-supplied filetype is available.
    """
    return (download_filetype or "").lower()


def measure_preimport_state(
    *,
    path: str,
    mb_release_id: str,
    label: str,
    download_filetype: str,
    download_min_bitrate_bps: int | None,
    download_is_vbr: bool | None,
    cfg: CratediggerConfig,
    bad_hash_db: BadHashGateDB | None = None,
    existing_spectral_evidence: SpectralAnalysisDetail | None = None,
    reuse_existing_spectral_evidence: bool = False,
    preserve_existing_source_spectral: bool = False,
    precomputed_inspection: LocalFileInspection | None = None,
    spectral_detail_analyzer: SpectralDetailAnalyzer | None = None,
    existing_spectral_resolver: ExistingSpectralResolver | None = None,
    aac_lattice_measure_fn: AacLatticeMeasureFn | None = None,
    cd_rip_verify_fn: CdRipVerifyFn | None = None,
) -> PreimportMeasurement:
    """Collect pre-import measurement facts. Returns ``PreimportMeasurement``.

    This is the pure measurement helper introduced in U3. It has NO decision
    fields, no denylist writes, no requeue decisions, and no DB writes at
    all: its only DB access is the read-only bad-hash gate through
    ``bad_hash_db``. From the measurement lanes, HAVE spectral state
    persists exclusively through the content-addressed evidence row
    (``lib.current_library_evidence.persist_exact_current_spectral_from_attempt``),
    which writes ONLY a real measured existing spectral — the candidate
    download's spectral is never adopted as HAVE state (issue #815 bail).
    (Dispatch separately stamps ``album_requests.current_spectral_*`` at
    import acceptance; that is its writer, not measurement's.)

    As of U11 there is exactly one decision function: persisted evidence
    flows into ``lib.quality.full_pipeline_decision_from_evidence``, whose
    four early-exit branches handle the folder/audio-integrity facts that
    used to live in the deleted ``preimport_decide``. Callers invoke
    ``measure_preimport_state`` to gather the facts, persist them to
    ``AlbumQualityEvidence``, and let the unified decider decide.

    Args:
        path: Filesystem path containing the files to validate.
        mb_release_id: MusicBrainz release ID — used to find the existing
            album's container bitrate in beets.
        label: "Artist - Title" string, for log output only.
        download_filetype: Comma-separated filetypes ("mp3", "flac", ...).
        download_min_bitrate_bps: Caller-supplied container min bitrate (bps).
        download_is_vbr: Caller-supplied VBR hint.
        cfg: Runtime CratediggerConfig.
        bad_hash_db: Bad-hash gate port — pass to enable the curator
            bad-rip hash lookup. Every producer of persisted candidate
            evidence must supply it; ``None`` skips the gate entirely.
        existing_spectral_evidence: Persisted HAVE detail from a separately
            authorized current-evidence row.
        reuse_existing_spectral_evidence: The preview caller has matched that
            row to the exact current release and established snapshot and
            proved its spectral grade decision-usable. Re-project it without
            another HAVE lookup or analyzer call.
        aac_lattice_measure_fn: Supplying this ENABLES the AAC frame-lattice
            capture (issue #829 PR-A); the default ``None`` measures nothing.
            Opt-in because the measurement costs tens of seconds of CPU per
            track: the measure-and-persist evidence producer supplies it, and
            the read-only classify contract (wrong-match triage UI, CLI
            inspection) deliberately does not — those are synchronous
            operator surfaces that must not block on it.

    Returns:
        PreimportMeasurement with all gate facts populated. Audio-corrupt and
        bad-hash matches short-circuit the spectral steps to avoid wasting
        cycles, but the returned Struct still has the corresponding flag set.

    Note: media readiness runs before measurement on a canonical processing
    album or private preview snapshot. This helper remains observational and
    never repairs an arbitrary source path itself.
    """
    filetype_band = _filetype_band(download_filetype)
    # Enumerate candidate audio once. The same stable path set owns file-count
    # and layout facts, bad-hash lookup, and lossless-container detection. In
    # particular, M4A codec probes happen here exactly once per necessary file.
    audio_files_for_count = _iter_audio_files(path)
    audio_file_count = len(audio_files_for_count)
    folder_layout: Literal["flat", "nested"] = (
        "nested"
        if any(str(audio.parent) != path for audio in audio_files_for_count)
        else "flat"
    )
    lossless_candidate = has_supported_lossless_audio(
        filetype_band,
        audio_files_for_count,
    )
    # This audit is intentionally separate from policy-facing
    # download_spectral/existing_spectral below. Early measurement-only exits
    # populate it here; MP3 policy analysis reuses its own result; normal
    # harness-bound codecs populate it in import_one.py.
    persisted_existing = (
        existing_spectral_evidence
        or SpectralAnalysisDetail(attempted=False)
    )
    reusable_existing = (
        persisted_existing if reuse_existing_spectral_evidence else None
    )
    audit_analyzer = spectral_detail_analyzer or analyze_spectral_audit_path
    audit_resolver = (
        existing_spectral_resolver
        or existing_spectral_resolver_for_config(cfg)
    )
    spectral_audit = SpectralDetail(
        candidate=SpectralAnalysisDetail(attempted=False),
        existing=persisted_existing,
    )
    existing_spectral_path: str | None = None

    # --- Audio integrity gate ---
    corrupt_files: list[str] = []
    audio_corrupt = False
    audio_error: str | None = None
    audio_result = validate_audio(path, cfg.audio_check_mode)
    audio_validation = audio_result.report
    if audio_result.measurement_failed:
        raise AudioValidationMeasurementError(audio_validation)
    if not audio_result.valid:
        audio_corrupt = True
        audio_error = audio_result.error
        corrupt_files = [name for name, _ in audio_result.failed_files]
        spectral_audit, existing_lookup = collect_release_attempt_spectral_audit(
            path,
            mb_release_id,
            existing_spectral_evidence=persisted_existing,
            preserve_existing_source_spectral=(
                preserve_existing_source_spectral
            ),
            analyzer=audit_analyzer,
            existing_resolver=audit_resolver,
            existing_detail=reusable_existing,
        )
        existing_spectral_path = existing_lookup.path
        download_spectral = spectral_measurement_from_attempt(
            spectral_audit.candidate,
        )
        return PreimportMeasurement(
            corrupt_files=corrupt_files,
            audio_validation=audio_validation,
            audio_corrupt=audio_corrupt,
            audio_error=audio_error,
            folder_layout=folder_layout,
            audio_file_count=audio_file_count,
            filetype_band=filetype_band,
            lossless_candidate=lossless_candidate,
            min_bitrate_kbps=(
                download_min_bitrate_bps // 1000
                if download_min_bitrate_bps
                and download_min_bitrate_bps >= 1000 else
                download_min_bitrate_bps
            ),
            is_vbr=download_is_vbr,
            download_spectral=download_spectral,
            existing_spectral_path=existing_spectral_path,
            spectral_audit=spectral_audit,
        )

    # --- Bad-audio-hash gate (plan 2026-04-29-005 / U5) ---
    # Hash candidate tracks and compare against the curator-reported
    # ``bad_audio_hashes`` table. Sits AFTER MP3 header repair, AFTER
    # audio-integrity, BEFORE spectral (cheaper to reject early on a known
    # match than run sox).
    matched_bad_hash_id: int | None = None
    matched_bad_track_path: str | None = None
    if bad_hash_db is not None:
        try:
            any_bad = bad_hash_db.has_any_bad_audio_hashes()
        except Exception:
            logger.warning(
                "bad-hash gate: has_any_bad_audio_hashes probe failed, skipping",
                exc_info=True)
            any_bad = False
        if any_bad:
            match = _check_bad_audio_hashes(audio_files_for_count, bad_hash_db)
            if match is not None:
                matched_bad_hash_id = match.bad_hash_id
                matched_bad_track_path = match.track_path
                logger.warning(
                    f"BAD HASH MATCH: {label} "
                    f"hash_id={match.bad_hash_id} track={match.track_path}")
                spectral_audit, existing_lookup = collect_release_attempt_spectral_audit(
                    path,
                    mb_release_id,
                    existing_spectral_evidence=persisted_existing,
                    preserve_existing_source_spectral=(
                        preserve_existing_source_spectral
                    ),
                    analyzer=audit_analyzer,
                    existing_resolver=audit_resolver,
                    existing_detail=reusable_existing,
                )
                existing_spectral_path = existing_lookup.path
                download_spectral = spectral_measurement_from_attempt(
                    spectral_audit.candidate,
                )
                return PreimportMeasurement(
                    corrupt_files=[],
                    audio_validation=audio_validation,
                    audio_corrupt=False,
                    matched_bad_hash_id=matched_bad_hash_id,
                    matched_bad_track_path=matched_bad_track_path,
                    folder_layout=folder_layout,
                    audio_file_count=audio_file_count,
                    filetype_band=filetype_band,
                    lossless_candidate=lossless_candidate,
                    min_bitrate_kbps=(
                        download_min_bitrate_bps // 1000
                        if download_min_bitrate_bps
                        and download_min_bitrate_bps >= 1000 else
                        download_min_bitrate_bps
                    ),
                    is_vbr=download_is_vbr,
                    download_spectral=download_spectral,
                    existing_spectral_path=existing_spectral_path,
                    spectral_audit=spectral_audit,
                )

    # --- Resolve VBR / min_bitrate / layout via filesystem inspection ---
    # ``precomputed_inspection`` lets a caller that already ran
    # ``inspect_local_files`` (both preview lanes, via
    # ``lib.import_preview._measure_lane_world``) avoid a second mutagen
    # walk. Only the read-only classify lane
    # (``lib.import_preview.preview_import_from_path``) uses its own
    # inspection to reject on ``has_nested_audio`` before ever calling this
    # function; the measure-and-persist lane passes one along for the
    # bitrate/VBR hints AND as a second nested-layout witness OR'd into
    # ``folder_layout`` below (issue #1355 item 1) — it never rejects on
    # that witness before measuring, unlike the classify lane. Auto path
    # passes None and does the walk here.
    inspection: LocalFileInspection | None = None
    if "mp3" in filetype_band and "flac" not in filetype_band:
        inspection = (precomputed_inspection if precomputed_inspection is not None
                      else inspect_local_files(path))
        if download_is_vbr is None and inspection.is_vbr is not None:
            download_is_vbr = inspection.is_vbr
        if download_min_bitrate_bps is None:
            download_min_bitrate_bps = inspection.min_bitrate_bps
    elif precomputed_inspection is not None:
        # Non-MP3 paths with a precomputed inspection — capture layout / count
        # without redoing the bitrate walk.
        inspection = precomputed_inspection

    # Prefer the caller's inspection when it already observed nested audio;
    # otherwise derive layout from the single path enumeration above.
    if inspection is not None and inspection.has_nested_audio:
        folder_layout = "nested"

    # Min bitrate in kbps for the measurement Struct (bps→kbps, only for
    # values that look like bps).
    if download_min_bitrate_bps is not None and download_min_bitrate_bps >= 1000:
        min_bitrate_kbps = download_min_bitrate_bps // 1000
    else:
        min_bitrate_kbps = download_min_bitrate_bps

    # --- Exact CD rip authenticity proof ---
    # Integrity and bad-hash checks above remain mandatory. A positive result
    # makes spectral/AAC/V0 authenticity work redundant, while every provider
    # miss or failure is represented by None and falls through unchanged.
    cd_rip_verification: CdRipBitVerification | None = None
    if cd_rip_verify_fn is not None and lossless_candidate and folder_layout == "flat":
        try:
            cd_rip_verification = cd_rip_verify_fn(path, cfg)
        except Exception:
            logger.exception("CD RIP: verifier failed for %s", path)
            cd_rip_verification = None

    # --- Spectral gate ---
    # Codec-only since issue #1145: every MP3 and every lossless candidate is
    # scanned, whatever its declared mode or average. The one remaining
    # bypass is an exact CD-rip bit verification, which is stronger evidence
    # than a spectral estimate rather than an assumption about one.
    download_spectral: SpectralMeasurement | None = None
    existing_spectral: SpectralMeasurement | None = None
    existing_min_bitrate: int | None = None

    if cd_rip_verification is None and _needs_spectral_check(
        download_filetype,
        lossless_candidate=lossless_candidate,
    ):
        spectral_audit, existing_lookup = collect_release_attempt_spectral_audit(
            path,
            mb_release_id,
            existing_spectral_evidence=persisted_existing,
            preserve_existing_source_spectral=(
                preserve_existing_source_spectral
            ),
            analyzer=audit_analyzer,
            existing_resolver=audit_resolver,
            existing_detail=reusable_existing,
        )
        existing_spectral_path = existing_lookup.path
        candidate_audit = spectral_audit.candidate
        assert candidate_audit is not None
        download_spectral = spectral_measurement_from_attempt(candidate_audit)
        if download_spectral is not None:
            cliff_count = sum(
                1 for track in candidate_audit.per_track
                if track.cliff_detected
            )
            logger.info(
                f"SPECTRAL: {label} grade={candidate_audit.grade}, "
                f"estimated_bitrate={candidate_audit.bitrate_kbps}kbps, "
                f"suspect={candidate_audit.suspect_pct or 0:.0f}%, "
                f"cliffs={cliff_count}")

        existing_audit = spectral_audit.existing
        assert existing_audit is not None
        measured_existing_min = existing_lookup.min_bitrate_kbps
        # issue #829 Phase 5 PR1 review round 2, should-fix 12: the HAVE
        # side runs through the exact same analyze_album/analyze_track
        # pipeline as the candidate, which always measures the 4 extension
        # slices — carry the result through rather than measuring it and
        # then throwing it away (no downstream consumer yet, matching the
        # rest of this PR's capture-only scope; a future PR3 proof-gate
        # comparison against the current library copy is the first
        # candidate consumer).
        measured_existing = SpectralMeasurement.from_parts(
            existing_audit.grade,
            existing_audit.bitrate_kbps,
            cliff_hz=existing_audit.cliff_hz,
            codec_family=existing_audit.codec_family,
            ultrasonic_deficit_db=existing_audit.ultrasonic_deficit_db,
            spectral_measurement_version=existing_audit.spectral_measurement_version,
        )
        # Preserve the old policy input: an existing spectral measurement was
        # considered only when candidate spectral analysis succeeded. The
        # independently gathered existing audit remains display-only.
        if download_spectral is not None:
            existing_min_bitrate = measured_existing_min
            existing_spectral = measured_existing

    # --- AAC MDCT frame-lattice capture (issue #829 AAC-lattice leg PR-A) ---
    # THE cohort gate: lossless containers whose album spectral grade came
    # back clean. That is the only cohort where the lattice can still change
    # anything — an Apple CVBR-256 launder is spectrally invisible by
    # construction, which is precisely why this detector exists — and the
    # only cohort worth tens of seconds of CPU per track on the serial
    # preview worker. A caller that supplies no measure fn measures nothing.
    # Capture-only in PR-A: nothing reads ``aac_lattice`` to decide anything.
    aac_lattice: AacLatticeCapture | None = None
    if (
        aac_lattice_measure_fn is not None
        and lossless_candidate
        and download_spectral is not None
        and download_spectral.grade in AAC_LATTICE_GATED_SPECTRAL_GRADES
    ):
        try:
            aac_lattice = aac_lattice_measure_fn(path)
        except Exception:
            # Invariant A-I4. Per-track failures are already recorded as
            # evidence inside the measurement; this is the composition guard
            # for a failure of the measurement itself, which must cost the
            # album its lattice and nothing else.
            logger.exception("AAC LATTICE: measurement failed for %s", path)
            aac_lattice = None

    if not (spectral_audit.candidate and spectral_audit.candidate.attempted):
        # Normal harness-bound codecs collect the candidate inside
        # import_one.py before conversion. Fill only HAVE here so the attempt
        # remains two-sided without paying for a duplicate candidate scan.
        spectral_audit, existing_lookup = collect_release_attempt_spectral_audit(
            path,
            mb_release_id,
            existing_spectral_evidence=persisted_existing,
            preserve_existing_source_spectral=(
                preserve_existing_source_spectral
            ),
            analyzer=audit_analyzer,
            existing_resolver=audit_resolver,
            candidate_detail=spectral_audit.candidate,
            existing_detail=reusable_existing,
        )
        existing_spectral_path = existing_lookup.path

    return PreimportMeasurement(
        corrupt_files=corrupt_files,
        audio_validation=audio_validation,
        audio_corrupt=audio_corrupt,
        audio_error=audio_error,
        matched_bad_hash_id=matched_bad_hash_id,
        matched_bad_track_path=matched_bad_track_path,
        download_spectral=download_spectral,
        existing_spectral=existing_spectral,
        existing_min_bitrate=existing_min_bitrate,
        existing_spectral_path=existing_spectral_path,
        folder_layout=folder_layout,
        audio_file_count=audio_file_count,
        filetype_band=filetype_band,
        lossless_candidate=lossless_candidate,
        min_bitrate_kbps=min_bitrate_kbps,
        is_vbr=download_is_vbr,
        spectral_audit=spectral_audit,
        aac_lattice=aac_lattice,
        cd_rip_verification=cd_rip_verification,
    )


# Bound on the legacy subprocess-stderr fallback used when the harness fails
# before returning a typed result. Conversion failures use ConversionInfo's
# bounded diagnostics instead; this fallback remains a breadcrumb, not a log.
STDERR_DIAGNOSTIC_MAX_CHARS = 2000


def diagnostic_from_stderr(stderr: str, max_chars: int = STDERR_DIAGNOSTIC_MAX_CHARS) -> str:
    """Return a bounded recent-line breadcrumb from arbitrary stderr."""
    if not stderr or not stderr.strip():
        return ""

    lines = [ln.strip() for ln in stderr.splitlines() if ln.strip()]

    # Keep recent whole lines within the budget. A single oversized line is
    # hard-truncated below so arbitrary subprocess output stays bounded.
    kept: list[str] = []
    total = 0
    for line in reversed(lines):
        joiner_cost = 3 if kept else 0  # " / "
        added = len(line) + joiner_cost
        if total + added > max_chars and kept:
            break
        kept.append(line)
        total += added
    kept.reverse()

    result = " / ".join(kept)
    if len(result) > max_chars:
        # Pathological single oversized line — hard ceiling wins.
        result = result[:max_chars]
    return result
