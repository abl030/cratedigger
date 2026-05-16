"""Shared pre-import quality gates for auto-import, force-import, and manual-import.

The auto-import path (lib.download.process_completed_album), the force-import
path (lib.import_dispatch.dispatch_import_from_db), and the manual-import path
all MUST run the same quality gates: audio integrity and spectral transcode
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

import logging
import os
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
from lib.quality import SPECTRAL_TRANSCODE_GRADES, SpectralMeasurement
from lib.util import repair_mp3_headers, validate_audio

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")


# Lazy import proxy — keeps sox out of import-time deps.
def spectral_analyze(folder: str, trim_seconds: int = 30) -> Any:
    """Proxy to spectral_check.analyze_album (lazy import).

    Mirrors lib.download.spectral_analyze so tests can patch one or the other
    depending on which module is under test. Callers inside lib.measurement must
    use this proxy (not the one in lib.download) so patches on
    ``lib.measurement.spectral_analyze`` take effect.
    """
    from lib.spectral_check import analyze_album
    return analyze_album(folder, trim_seconds=trim_seconds)


class PreimportMeasurement(msgspec.Struct, frozen=True):
    """Facts gathered by ``measure_preimport_state``. No decision fields.

    The measurement helper has no opinion on accept/reject — it only reports
    what is on disk. The persisted ``AlbumQualityEvidence`` row carries the
    same facts (audio_corrupt, folder_layout, audio_file_count,
    matched_bad_audio_hash_*); the unified decider
    ``lib.quality.full_pipeline_decision_from_evidence`` consumes them as
    early-exit reject branches (U11).

    Fields map 1:1 onto the new ``AlbumQualityEvidence`` columns added in U1
    (``audio_corrupt``, ``folder_layout``, ``audio_file_count``,
    ``filetype_band``, ``matched_bad_audio_hash_*``) so U5/U6 can wire the
    measurement directly into evidence persistence.
    """
    corrupt_files: list[str] = msgspec.field(default_factory=list)
    audio_corrupt: bool = False
    matched_bad_hash_id: int | None = None
    matched_bad_track_path: str | None = None
    download_spectral: SpectralMeasurement | None = None
    existing_spectral: SpectralMeasurement | None = None
    existing_min_bitrate: int | None = None
    folder_layout: Literal["flat", "nested"] = "flat"
    audio_file_count: int = 0
    filetype_band: str = ""
    min_bitrate_kbps: int | None = None
    is_vbr: bool | None = None


AUDIO_EXTS = ("mp3", "flac", "alac", "m4a", "ogg", "opus", "wav", "aac")


@dataclass
class LocalFileInspection:
    """Result of inspecting audio files on disk at a force/manual import path.

    Populated by ``inspect_local_files`` so callers of ``measure_preimport_state``
    that have no DownloadFile metadata (force/manual paths) can still supply
    filetype / bitrate / vbr hints.

    ``has_nested_audio`` reports whether any audio files were found below the
    root directory. Callers should reject nested layouts early: the
    preimport gates (validate_audio / analyze_album / repair_mp3_headers)
    recurse, but the downstream beets harness (``harness/import_one.py``)
    still uses ``os.listdir`` for bitrate measurement and conversion, so a
    nested force/manual import would pass gates and then produce a
    misclassified/empty measurement in the harness.

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
    manual/force imports because ``download_filetype`` comes back empty.

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


def _needs_spectral_check(
    filetype: str,
    is_vbr: bool | None,
    *,
    avg_bitrate_kbps: int | None = None,
    vbr_threshold_kbps: int | None = None,
) -> bool:
    """Decide whether to run spectral analysis as a preimport gate.

    Rules:
      - Non-MP3 (FLAC, ALAC, ...) → skip. FLAC uses a different flow (convert
        → V0 → compare); other codecs have no cliff-detection calibration.
      - CBR MP3 or unknown VBR (is_vbr is None) → run. CBR is the classic
        transcode-cliff case; unknown VBR is the conservative default
        (issue #39: resumed downloads without slskd metadata).
      - VBR MP3 → run only when ``avg_bitrate_kbps`` is unknown (conservative)
        or below ``vbr_threshold_kbps``. Issue #93: a VBR MP3 at avg 182kbps
        (well below genuine V0's ~240-260kbps range) was an obvious transcode
        that the old ``is_vbr``-only gate let through. The threshold comes
        from ``cfg.quality_ranks.mp3_vbr.excellent`` — the same value
        ``transcode_detection()`` already uses as its VBR transcode boundary.

    ``avg_bitrate_kbps`` / ``vbr_threshold_kbps`` are keyword-only to keep
    the call site self-documenting: the VBR branch requires both to skip, so
    callers pass both or neither.
    """
    filetype_lower = (filetype or "").lower()
    is_mp3 = "mp3" in filetype_lower and "flac" not in filetype_lower
    if not is_mp3:
        return False
    if not bool(is_vbr):
        return True
    if avg_bitrate_kbps is None or vbr_threshold_kbps is None:
        return True
    return avg_bitrate_kbps < vbr_threshold_kbps


def _analyze_existing(
    mb_release_id: str,
    cfg: "CratediggerConfig",
) -> tuple[int | None, SpectralMeasurement | None]:
    """Look up existing beets album and spectral-analyze its files.

    Returns ``(existing_min_bitrate_kbps, existing_spectral)``. Either or both
    may be None if the album isn't in beets or the on-disk path is missing.
    Exceptions are logged and swallowed so a missing existing copy never
    blocks a new import.
    """
    from lib.beets_db import BeetsDB

    existing_min: int | None = None
    existing_spectral: SpectralMeasurement | None = None
    try:
        with BeetsDB(library_root=getattr(cfg, "beets_directory", "")) as beets:
            existing_info = beets.get_album_info(
                mb_release_id, cfg.quality_ranks)
        if existing_info:
            existing_min = existing_info.min_bitrate_kbps
            if os.path.isdir(existing_info.album_path):
                sp = spectral_analyze(existing_info.album_path,
                                      trim_seconds=30)
                existing_spectral = SpectralMeasurement.from_parts(
                    sp.grade, sp.estimated_bitrate_kbps)
                logger.info(
                    f"SPECTRAL: existing on disk: grade={sp.grade}, "
                    f"estimated_bitrate={sp.estimated_bitrate_kbps}kbps, "
                    f"beets_min={existing_min}kbps")
    except Exception:
        logger.exception("SPECTRAL: failed to check existing files")
    return existing_min, existing_spectral


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

    Pass ``propagate_download_to_existing=False`` from the force/manual
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
            db.update_spectral_state(
                request_id,
                RequestSpectralStateUpdate(current=to_write),
            )
        except Exception:
            logger.exception("Failed to update on-disk spectral data")
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
    propagate_download_to_existing: bool = True,
    precomputed_inspection: "LocalFileInspection | None" = None,
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
            album in beets for spectral comparison.
        label: "Artist - Title" string, for log output only.
        download_filetype: Comma-separated filetypes ("mp3", "flac", ...).
        download_min_bitrate_bps: Caller-supplied container min bitrate (bps).
        download_is_vbr: Caller-supplied VBR hint.
        cfg: Runtime CratediggerConfig.
        db: Pipeline DB — pass to enable spectral state persistence + bad-hash
            lookup + persisted-spectral fallback.
        request_id: Required when ``db`` is supplied.

    Returns:
        PreimportMeasurement with all gate facts populated. Audio-corrupt and
        bad-hash matches short-circuit the spectral steps to avoid wasting
        cycles, but the returned Struct still has the corresponding flag set.
    """
    # --- MP3 header repair (unconditional) ---
    # mp3val runs regardless of audio_check_mode: deployments with
    # audio_check=off still want fixable MP3 header issues cleaned up before
    # spectral analysis and the import subprocess. Matches the auto path's
    # original behavior pre-refactor.
    try:
        repair_mp3_headers(path)
    except Exception:
        logger.debug("repair_mp3_headers failed", exc_info=True)

    filetype_band = _filetype_band(download_filetype)

    # --- Audio integrity gate ---
    corrupt_files: list[str] = []
    audio_corrupt = False
    if cfg.audio_check_mode != "off":
        audio_result = validate_audio(path, cfg.audio_check_mode)
        if not audio_result.valid:
            audio_corrupt = True
            corrupt_files = [name for name, _ in audio_result.failed_files]
            logger.warning(
                f"AUDIO CORRUPT: {label} "
                f"({len(corrupt_files)} files failed ffmpeg decode)")
            return PreimportMeasurement(
                corrupt_files=corrupt_files,
                audio_corrupt=audio_corrupt,
                folder_layout="flat",
                audio_file_count=0,
                filetype_band=filetype_band,
                min_bitrate_kbps=(
                    download_min_bitrate_bps // 1000
                    if download_min_bitrate_bps
                    and download_min_bitrate_bps >= 1000 else
                    download_min_bitrate_bps
                ),
                is_vbr=download_is_vbr,
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
            audio_files = _iter_audio_files(path)
            match = _check_bad_audio_hashes(audio_files, db)
            if match is not None:
                matched_bad_hash_id = match.bad_hash_id
                matched_bad_track_path = match.track_path
                logger.warning(
                    f"BAD HASH MATCH: {label} "
                    f"hash_id={match.bad_hash_id} track={match.track_path}")
                return PreimportMeasurement(
                    corrupt_files=[],
                    audio_corrupt=False,
                    matched_bad_hash_id=matched_bad_hash_id,
                    matched_bad_track_path=matched_bad_track_path,
                    folder_layout="flat",
                    audio_file_count=0,
                    filetype_band=filetype_band,
                    min_bitrate_kbps=(
                        download_min_bitrate_bps // 1000
                        if download_min_bitrate_bps
                        and download_min_bitrate_bps >= 1000 else
                        download_min_bitrate_bps
                    ),
                    is_vbr=download_is_vbr,
                )

    # --- Resolve VBR / min_bitrate / avg bitrate / layout via filesystem inspection ---
    # ``precomputed_inspection`` lets the force/manual path (which already
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

    # Folder layout + file count: walk the filesystem once when not already
    # known. ``_iter_audio_files`` mirrors the gate's directory walk so
    # downstream gates and the importer see the same set.
    if inspection is not None and (
        inspection.has_nested_audio or inspection.filetype
    ):
        audio_files_for_count = _iter_audio_files(path)
        audio_file_count = len(audio_files_for_count)
        folder_layout: Literal["flat", "nested"] = (
            "nested" if inspection.has_nested_audio else "flat")
    else:
        audio_files_for_count = _iter_audio_files(path)
        audio_file_count = len(audio_files_for_count)
        # Layout: any audio file outside ``path`` (i.e. in a subdirectory) is
        # nested. Otherwise flat. Cheap derivation from the walk we already did.
        folder_layout = "flat"
        for p in audio_files_for_count:
            if str(p.parent) != path:
                folder_layout = "nested"
                break

    # Min bitrate in kbps for the measurement Struct (bps→kbps, only for
    # values that look like bps).
    if download_min_bitrate_bps is not None and download_min_bitrate_bps >= 1000:
        min_bitrate_kbps = download_min_bitrate_bps // 1000
    else:
        min_bitrate_kbps = download_min_bitrate_bps

    # --- Spectral gate ---
    # Threshold: cfg.quality_ranks.mp3_vbr.excellent — same V0 boundary
    # transcode_detection() uses.
    avg_bitrate_kbps = (avg_bitrate_bps // 1000) if avg_bitrate_bps else None
    download_spectral: SpectralMeasurement | None = None
    existing_spectral: SpectralMeasurement | None = None
    existing_min_bitrate: int | None = None

    if _needs_spectral_check(
        download_filetype, download_is_vbr,
        avg_bitrate_kbps=avg_bitrate_kbps,
        vbr_threshold_kbps=cfg.quality_ranks.mp3_vbr.excellent,
    ):
        try:
            dl_sp = spectral_analyze(path, trim_seconds=30)
            dl_grade = dl_sp.grade
            dl_cliff_bitrate = dl_sp.estimated_bitrate_kbps
            dl_suspect_pct = dl_sp.suspect_pct
            cliff_count = sum(
                1 for track in getattr(dl_sp, "tracks", [])
                if getattr(track, "cliff_detected", False)
            )
            download_spectral = SpectralMeasurement.from_parts(
                dl_grade, dl_cliff_bitrate)
            logger.info(
                f"SPECTRAL: {label} grade={dl_grade}, "
                f"estimated_bitrate={dl_cliff_bitrate}kbps, "
                f"suspect={dl_suspect_pct:.0f}%, cliffs={cliff_count}")
        except Exception:
            logger.exception(f"SPECTRAL: failed for {label}")
            download_spectral = None

        if download_spectral is not None and mb_release_id:
            existing_min_bitrate, existing_spectral = _analyze_existing(
                mb_release_id, cfg)

        # --- Fall back to persisted spectral state when BeetsDB couldn't walk ---
        beets_knows_album = existing_min_bitrate is not None
        if (download_spectral is not None
                and beets_knows_album
                and existing_spectral is None
                and db is not None and request_id is not None):
            try:
                req = db.get_request(request_id)
                if req:
                    stored_grade = req.get("current_spectral_grade")
                    stored_bitrate = req.get("current_spectral_bitrate")
                    if stored_grade in SPECTRAL_TRANSCODE_GRADES:
                        stored = SpectralMeasurement.from_parts(
                            stored_grade, stored_bitrate)
                        if stored is not None:
                            existing_spectral = stored
                            logger.info(
                                f"SPECTRAL: {label} using persisted "
                                f"current_spectral (grade={stored.grade}, "
                                f"bitrate={stored.bitrate_kbps}kbps) — BeetsDB "
                                "lookup returned no spectral measurement")
            except Exception:
                logger.debug("failed to read persisted spectral state",
                             exc_info=True)

    # --- Persist spectral state to DB (issue #90 propagation) ---
    # This MUST fire on every measurement where spectral was collected,
    # regardless of whether the downstream decision accepts or rejects. The
    # next attempt needs accurate ``album_requests.current_spectral_*`` to
    # make a sound comparison. Persists AFTER the existing_spectral snapshot
    # used by the decision is taken — propagation can't poison the comparison
    # because the decision runs in ``full_pipeline_decision_from_evidence``
    # on the *persisted* candidate evidence row (or the returned Struct for
    # legacy callers), neither of which sees the post-propagation
    # ``album_requests.current_spectral_*``.
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
        matched_bad_hash_id=matched_bad_hash_id,
        matched_bad_track_path=matched_bad_track_path,
        download_spectral=download_spectral,
        existing_spectral=existing_spectral,
        existing_min_bitrate=existing_min_bitrate,
        folder_layout=folder_layout,
        audio_file_count=audio_file_count,
        filetype_band=filetype_band,
        min_bitrate_kbps=min_bitrate_kbps,
        is_vbr=download_is_vbr,
    )


