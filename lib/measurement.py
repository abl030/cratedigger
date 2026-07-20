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
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from lib.audio_hash import AudioHashError, hash_audio_content

# Extensions audio_hash.py currently knows how to hash. AUDIO_EXTS is broader
# (includes wav, alac); the bad-hash gate filters to this subset so legitimate
# wav/alac albums don't trip a per-track warning every validation cycle.
_BAD_HASH_SUPPORTED_EXTS: frozenset[str] = frozenset({"flac", "mp3", "m4a", "aac", "ogg", "opus"})
from lib.pipeline_db import RequestSpectralStateUpdate
from lib.quality import (
    SpectralAnalysisDetail,
    SpectralDetail,
    SpectralMeasurement,
    SpectralTrackDetail,
)
from lib.util import validate_audio

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")


# Lazy import proxy — keeps sox out of import-time deps.
def spectral_analyze(folder: str, trim_seconds: int = 30) -> Any:
    """Proxy to spectral_check.analyze_album (lazy import).

    Callers inside lib.measurement must use this proxy so patches on
    ``lib.measurement.spectral_analyze`` take effect.
    """
    from lib.spectral_check import analyze_album
    return analyze_album(folder, trim_seconds=trim_seconds)


def analyze_spectral_audit_path(path: str) -> SpectralAnalysisDetail:
    """Analyze one path into display-only attempt audit evidence."""
    grade: str | None = None
    bitrate_kbps: int | None = None
    suspect_pct: float | None = None
    per_track: list[SpectralTrackDetail] = []
    try:
        result = spectral_analyze(path, trim_seconds=30)
        grade = result.grade
        bitrate_kbps = result.estimated_bitrate_kbps
        suspect_pct = result.suspect_pct
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
        )
    return SpectralAnalysisDetail(
        attempted=True,
        grade=grade,
        bitrate_kbps=bitrate_kbps,
        suspect_pct=suspect_pct,
        per_track=per_track,
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
) -> tuple[SpectralDetail, ExistingSpectralAuditLookup]:
    """Own conditional HAVE collection for every attempted-import adapter.

    A lossless source converted to Opus/V0 keeps the source-side spectral
    measurement as its authoritative HAVE provenance; analyzing that installed
    derivative can rewrite a transcode-like FLAC as apparently genuine. Every
    other exact-release copy is analyzed from the files currently on disk.
    """
    candidate = (
        candidate_detail
        if candidate_detail is not None
        else _fail_soft_spectral_analysis(candidate_path, analyzer)
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
    cfg: "CratediggerConfig",
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
    cfg: "CratediggerConfig",
) -> ExistingSpectralResolver:
    return lambda release_id: resolve_existing_spectral_audit(release_id, cfg)


def spectral_detail_from_persisted_source(
    grade: object,
    bitrate_kbps: object,
) -> SpectralAnalysisDetail:
    """Project durable pre-conversion fields into attempt-audit shape."""
    spectral_grade = grade if isinstance(grade, str) and grade else None
    spectral_bitrate = (
        bitrate_kbps if isinstance(bitrate_kbps, int) else None
    )
    return SpectralAnalysisDetail(
        attempted=spectral_grade is not None or spectral_bitrate is not None,
        grade=spectral_grade,
        bitrate_kbps=spectral_bitrate,
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
    corrupt_files: list[str] = msgspec.field(default_factory=list)
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

    ``avg_bitrate_bps`` is the mean bitrate across all readable MP3 files —
    used by the VBR spectral-gate threshold (issue #93). Genuine V0 averages
    ~240-260kbps; VBR transcodes masquerading as V0 average well below that.
    """
    filetype: str = ""           # comma-separated lowercase extensions
    min_bitrate_bps: int | None = None
    avg_bitrate_bps: int | None = None
    is_vbr: bool | None = None
    has_nested_audio: bool = False


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
    mp3_bitrates: list[int] = []
    any_vbr: bool | None = None
    has_nested_audio = False

    for root, _dirs, files in os.walk(path):
        for name in files:
            if "." not in name:
                continue
            ext = name.rsplit(".", 1)[-1].lower()
            if ext not in AUDIO_EXTS:
                continue
            if root != path:
                has_nested_audio = True
            extensions.add(ext)
            if ext == "mp3":
                full = os.path.join(root, name)
                try:
                    from mutagen.mp3 import MP3  # type: ignore[import-untyped]
                    mp3 = MP3(full)
                    br = getattr(mp3.info, "bitrate", None)
                    br_mode = getattr(mp3.info, "bitrate_mode", None)
                    if br is not None:
                        min_bitrate = br if min_bitrate is None else min(min_bitrate, br)
                        mp3_bitrates.append(br)
                    # mutagen BitrateMode: UNKNOWN=0, CBR=1, VBR=2, ABR=3
                    if br_mode is not None:
                        is_vbr_file = int(br_mode) in (2, 3)
                        any_vbr = is_vbr_file if any_vbr is None else (any_vbr or is_vbr_file)
                except Exception:
                    logger.debug(f"inspect_local_files: failed to read {full}",
                                 exc_info=True)

    avg_bitrate = sum(mp3_bitrates) // len(mp3_bitrates) if mp3_bitrates else None

    return LocalFileInspection(
        filetype=", ".join(sorted(extensions)),
        min_bitrate_bps=min_bitrate,
        avg_bitrate_bps=avg_bitrate,
        is_vbr=any_vbr,
        has_nested_audio=has_nested_audio,
    )


AudioCodecProbe = Callable[[str], str | None]


class AudioCodecProbeError(RuntimeError):
    """Raised when an ambiguous container's codec cannot be measured."""


def ffprobe_audio_codec_name(fpath: str) -> str | None:
    """Return the first audio stream codec name reported by ffprobe."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "a:0",
                "-show_entries", "stream=codec_name", "-of", "json", fpath,
            ],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=10,
        )
        if result.returncode != 0:
            return None
        payload: object = json.loads(result.stdout or "{}")
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None
    streams = payload.get("streams")
    if not isinstance(streams, list) or not streams:
        return None
    stream = streams[0]
    if not isinstance(stream, dict):
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
    is_vbr: bool | None,
    *,
    lossless_candidate: bool,
    avg_bitrate_kbps: int | None = None,
    vbr_threshold_kbps: int | None = None,
) -> bool:
    """Decide whether to run spectral analysis as a preimport gate.

    Rules:
      - A caller-classified supported lossless source (FLAC, WAV, ALAC,
        including ALAC-in-M4A) → always run. Verification requires affirmative
        preview-time spectral evidence. AAC-in-M4A remains lossy.
      - Other non-MP3 codecs → skip; they have no calibrated cliff policy.
      - CBR MP3 or unknown VBR (is_vbr is None) → run. CBR is the classic
        transcode-cliff case; unknown VBR is the conservative default
        (issue #39: resumed downloads without slskd metadata).
      - VBR MP3 → run only when ``avg_bitrate_kbps`` is unknown (conservative)
        or below ``vbr_threshold_kbps``. Issue #93: a VBR MP3 at avg 182kbps
        (well below genuine V0's ~240-260kbps range) was an obvious transcode
        that the old ``is_vbr``-only gate let through. The threshold comes
        from ``cfg.quality_ranks.mp3_vbr.excellent``. This is a scan-selection
        policy only; the later transcode decision consumes the resulting
        spectral grade, not the bitrate threshold.

    This helper is pure: filesystem enumeration and any M4A codec probe happen
    once at the measurement boundary and arrive as ``lossless_candidate``.
    ``avg_bitrate_kbps`` / ``vbr_threshold_kbps`` are keyword-only to keep the
    VBR scan threshold explicit; both must be known for that branch to skip.
    """
    filetype_lower = (filetype or "").lower()
    if lossless_candidate:
        return True
    is_mp3 = "mp3" in filetype_lower and "flac" not in filetype_lower
    if not is_mp3:
        return False
    if not bool(is_vbr):
        return True
    if avg_bitrate_kbps is None or vbr_threshold_kbps is None:
        return True
    return avg_bitrate_kbps < vbr_threshold_kbps


def _persist_spectral_state(
    *,
    db: "PipelineDB",
    request_id: int,
    download_spectral: SpectralMeasurement | None,
    existing_spectral: SpectralMeasurement | None,
    existing_min_bitrate: int | None,
    label: str,
    propagate_download_to_existing: bool = True,
) -> SpectralMeasurement | None:
    """Write the on-disk spectral state to album_requests.

    When ``propagate_download_to_existing`` is True and there's no measured
    existing spectral but there IS an existing album on disk
    (existing_min_bitrate set), adopt the download's spectral as the current
    on-disk measurement. This helps same-tier downgrade detection for
    subsequent imports — the download and on-disk characterize the same
    quality tier, so reusing the download's spectral is a reasonable proxy.

    Pass ``propagate_download_to_existing=False`` from the force-import
    import path: that path evaluates the gate *before* the subprocess import
    runs, so propagating a download's spectral into on-disk state would be
    speculative. If the downstream import fails (downgrade, no JSON,
    timeout) the DB would otherwise be left claiming that the failed
    download is on-disk, skewing later ``compute_effective_override_bitrate``
    and quality-gate decisions.

    Returns the measurement actually written (or None if nothing to write).
    """
    to_write = existing_spectral
    if (to_write is None
            and propagate_download_to_existing
            and download_spectral is not None
            and existing_min_bitrate is not None):
        to_write = download_spectral
        logger.info(
            f"SPECTRAL PROPAGATE: {label} on-disk spectral=NULL, "
            f"adopting download spectral grade={to_write.grade}")
    if to_write is not None:
        try:
            applied = db.update_spectral_state(
                request_id,
                RequestSpectralStateUpdate(current=to_write),
            )
            if not applied:
                logger.warning(
                    "Skipped on-disk spectral update for frozen/missing "
                    "request %s",
                    request_id,
                )
                return None
        except Exception:
            logger.exception("Failed to update on-disk spectral data")
            return None
    return to_write


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
    db: "PipelineDB",
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
    cfg: "CratediggerConfig",
    db: "PipelineDB | None" = None,
    request_id: int | None = None,
    existing_spectral_evidence: SpectralAnalysisDetail | None = None,
    preserve_existing_source_spectral: bool = False,
    propagate_download_to_existing: bool = True,
    precomputed_inspection: "LocalFileInspection | None" = None,
    spectral_detail_analyzer: SpectralDetailAnalyzer | None = None,
    existing_spectral_resolver: ExistingSpectralResolver | None = None,
) -> PreimportMeasurement:
    """Collect pre-import measurement facts. Returns ``PreimportMeasurement``.

    This is the pure measurement helper introduced in U3. It has NO decision
    fields, no denylist writes, no requeue decisions. It DOES persist on-disk
    spectral state to ``album_requests`` via ``_persist_spectral_state`` when
    a DB is wired — that propagation is part of "we measured this candidate"
    and must fire whether or not the downstream decision is accept or reject
    (issue #90).

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
        db: Pipeline DB — pass to enable spectral audit persistence and
            bad-hash lookup.
        request_id: Required when ``db`` is supplied.

    Returns:
        PreimportMeasurement with all gate facts populated. Audio-corrupt and
        bad-hash matches short-circuit the spectral steps to avoid wasting
        cycles, but the returned Struct still has the corresponding flag set.

    Note: ``repair_mp3_headers`` is **not** called here. Callers must run
    mp3val on the source before measurement (and before snapshotting, in
    the preview worker) so that header fixes are visible in the evidence
    snapshot and never mutate the source after the importer's freshness
    check.
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
    if cfg.audio_check_mode != "off":
        audio_result = validate_audio(path, cfg.audio_check_mode)
        if not audio_result.valid:
            audio_corrupt = True
            audio_error = audio_result.error
            corrupt_files = [name for name, _ in audio_result.failed_files]
            logger.warning(
                f"AUDIO CORRUPT: {label} "
                f"({len(corrupt_files)} files failed ffmpeg decode)")
            spectral_audit, existing_lookup = collect_release_attempt_spectral_audit(
                path,
                mb_release_id,
                existing_spectral_evidence=persisted_existing,
                preserve_existing_source_spectral=(
                    preserve_existing_source_spectral
                ),
                analyzer=audit_analyzer,
                existing_resolver=audit_resolver,
            )
            existing_spectral_path = existing_lookup.path
            return PreimportMeasurement(
                corrupt_files=corrupt_files,
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
    if db is not None:
        try:
            any_bad = db.has_any_bad_audio_hashes()
        except Exception:
            logger.warning(
                "bad-hash gate: has_any_bad_audio_hashes probe failed, skipping",
                exc_info=True)
            any_bad = False
        if any_bad:
            match = _check_bad_audio_hashes(audio_files_for_count, db)
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
                )
                existing_spectral_path = existing_lookup.path
                return PreimportMeasurement(
                    corrupt_files=[],
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
                    existing_spectral_path=existing_spectral_path,
                    spectral_audit=spectral_audit,
                )

    # --- Resolve VBR / min_bitrate / avg bitrate / layout via filesystem inspection ---
    # ``precomputed_inspection`` lets the force-import path (which already
    # inspected to decide the nested-layout gate) avoid a second mutagen
    # walk. Auto path passes None and does the walk here.
    inspection: LocalFileInspection | None = None
    avg_bitrate_bps: int | None = None
    if "mp3" in filetype_band and "flac" not in filetype_band:
        inspection = (precomputed_inspection if precomputed_inspection is not None
                      else inspect_local_files(path))
        if download_is_vbr is None and inspection.is_vbr is not None:
            download_is_vbr = inspection.is_vbr
        if download_min_bitrate_bps is None:
            download_min_bitrate_bps = inspection.min_bitrate_bps
        avg_bitrate_bps = inspection.avg_bitrate_bps
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

    # --- Spectral gate ---
    # Threshold: cfg.quality_ranks.mp3_vbr.excellent. This controls whether a
    # VBR MP3 is scanned; it is not itself transcode-decision evidence.
    avg_bitrate_kbps = (avg_bitrate_bps // 1000) if avg_bitrate_bps else None
    download_spectral: SpectralMeasurement | None = None
    existing_spectral: SpectralMeasurement | None = None
    existing_min_bitrate: int | None = None

    if _needs_spectral_check(
        download_filetype, download_is_vbr,
        lossless_candidate=lossless_candidate,
        avg_bitrate_kbps=avg_bitrate_kbps,
        vbr_threshold_kbps=cfg.quality_ranks.mp3_vbr.excellent,
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
        )
        existing_spectral_path = existing_lookup.path
        candidate_audit = spectral_audit.candidate
        assert candidate_audit is not None
        download_spectral = SpectralMeasurement.from_parts(
            candidate_audit.grade, candidate_audit.bitrate_kbps)
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
        measured_existing = SpectralMeasurement.from_parts(
            existing_audit.grade,
            existing_audit.bitrate_kbps,
        )
        # Preserve the old policy input: an existing spectral measurement was
        # considered only when candidate spectral analysis succeeded. The
        # independently gathered existing audit remains display-only.
        if download_spectral is not None:
            existing_min_bitrate = measured_existing_min
            existing_spectral = measured_existing
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
        )
        existing_spectral_path = existing_lookup.path

    # --- Persist spectral state to DB (issue #90 propagation) ---
    # This MUST fire on every measurement where spectral was collected,
    # regardless of whether the downstream decision accepts or rejects. The
    # request stamps remain accurate for audit and rendering. Persists AFTER
    # the existing_spectral snapshot
    # used by the decision is taken — propagation can't poison the comparison
    # because the decision runs in ``full_pipeline_decision_from_evidence``
    # on the *persisted* candidate evidence row (or the returned Struct for
    # legacy callers), neither of which reads the request stamps.
    if download_spectral is not None and db is not None and request_id is not None:
        try:
            _persist_spectral_state(
                db=db, request_id=request_id,
                download_spectral=download_spectral,
                existing_spectral=existing_spectral,
                existing_min_bitrate=existing_min_bitrate,
                label=label,
                propagate_download_to_existing=propagate_download_to_existing,
            )
        except Exception:
            logger.exception("failed to persist spectral state")

    return PreimportMeasurement(
        corrupt_files=corrupt_files,
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
    )
