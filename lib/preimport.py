"""Shared pre-import quality gates for auto-import, force-import, and manual-import.

The auto-import path (lib.download.process_completed_album), the force-import
path (lib.import_dispatch.dispatch_import_from_db), and the manual-import path
all MUST run the same quality gates: audio integrity and spectral transcode
detection. The only gate that differs between paths is the beets *distance*
check — that is what --force on import_one.py overrides. Every other gate is
shared, so it lives here in a single function.

Rationale: force-import previously called dispatch_import_core() directly,
skipping the audio + spectral gates that _process_beets_validation ran in the
auto path. A transcode rejected by auto-import's spectral gate could be
force-imported into beets, replacing an existing copy of the same quality with
no real upgrade. See the "No Parallel Code Paths" rule in
.claude/rules/code-quality.md.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from lib.pipeline_db import RequestSpectralStateUpdate
from lib.quality import (SPECTRAL_TRANSCODE_GRADES, SpectralMeasurement,
                         spectral_import_decision)
from lib.util import repair_mp3_headers, validate_audio

if TYPE_CHECKING:
    from lib.config import SoularrConfig
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("soularr")


# Lazy import proxy — keeps sox out of import-time deps.
def spectral_analyze(folder: str, trim_seconds: int = 30) -> Any:
    """Proxy to spectral_check.analyze_album (lazy import).

    Mirrors lib.download.spectral_analyze so tests can patch one or the other
    depending on which module is under test. Callers inside lib.preimport must
    use this proxy (not the one in lib.download) so patches on
    ``lib.preimport.spectral_analyze`` take effect.
    """
    lib_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lib")
    if lib_dir not in sys.path:
        sys.path.insert(0, lib_dir)
    from spectral_check import analyze_album
    return analyze_album(folder, trim_seconds=trim_seconds)


@dataclass
class PreImportGateResult:
    """Outcome of the shared pre-import gate pipeline.

    ``valid=False`` means the import must be rejected. ``scenario`` and
    ``detail`` mirror the fields on ``ValidationResult`` so callers can fold
    the result into an existing ``ValidationResult`` without translation.

    The spectral fields are populated whenever a spectral analysis ran
    (regardless of pass/reject) so callers can persist them to download_log.
    """
    valid: bool = True
    scenario: str | None = None      # "audio_corrupt" | "spectral_reject"
    detail: str | None = None
    corrupt_files: list[str] = field(default_factory=list)
    download_spectral: SpectralMeasurement | None = None
    existing_spectral: SpectralMeasurement | None = None
    existing_min_bitrate: int | None = None


AUDIO_EXTS = ("mp3", "flac", "alac", "m4a", "ogg", "opus", "wav", "aac")


@dataclass
class LocalFileInspection:
    """Result of inspecting audio files on disk at a force/manual import path.

    Populated by ``inspect_local_files`` so callers of ``run_preimport_gates``
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
    cfg: "SoularrConfig",
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
        with BeetsDB() as beets:
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


def run_preimport_gates(
    *,
    path: str,
    mb_release_id: str,
    label: str,
    download_filetype: str,
    download_min_bitrate_bps: int | None,
    download_is_vbr: bool | None,
    cfg: "SoularrConfig",
    db: "PipelineDB | None" = None,
    request_id: int | None = None,
    usernames: set[str] | None = None,
    propagate_download_to_existing: bool = True,
) -> PreImportGateResult:
    """Run shared pre-import gates: audio integrity, then spectral transcode detection.

    Side effects when ``db`` and ``request_id`` are supplied:
      * Updates ``album_requests.current_spectral_{grade,bitrate}`` via
        ``update_spectral_state``.
      * On spectral reject, adds a ``source_denylist`` entry for every
        username in ``usernames`` with a ``spectral: Xkbps <= existing Ykbps``
        reason.

    The caller owns:
      * Writing ``download_log`` (the result carries scenario/detail to fold
        into the log).
      * Filesystem moves (auto path moves files to ``failed_imports/`` on
        reject; force/manual paths leave them where they are).
      * Requeue decisions (auto path requeues on reject; force/manual do not).

    Args:
        path: Filesystem path containing the files to validate.
        mb_release_id: MusicBrainz release ID — used to find the existing
            album in beets for spectral comparison.
        label: "Artist - Title" string, for log output only.
        download_filetype: Comma-separated filetypes (e.g. "mp3", "flac",
            "mp3, flac"). Controls whether spectral check runs.
        download_min_bitrate_bps: Minimum container bitrate across download
            files, in bps (or kbps if < 1000). Normalized internally.
        download_is_vbr: True when any file in the download reports VBR.
            Spectral check is skipped for VBR MP3s.
        cfg: Runtime SoularrConfig (for ``audio_check_mode`` and
            ``quality_ranks``).
        db: Pipeline DB — pass to enable denylist + spectral state side effects.
        request_id: Required when ``db`` is supplied.
        usernames: Soulseek users to denylist on spectral reject.

    Returns:
        PreImportGateResult with ``valid`` and populated spectral fields.
    """
    result = PreImportGateResult()

    # --- MP3 header repair (unconditional) ---
    # mp3val runs regardless of audio_check_mode: deployments with
    # audio_check=off still want fixable MP3 header issues cleaned up before
    # spectral analysis and the import subprocess. Matches the auto path's
    # original behavior pre-refactor.
    try:
        repair_mp3_headers(path)
    except Exception:
        logger.debug("repair_mp3_headers failed", exc_info=True)

    # --- Audio integrity gate ---
    if cfg.audio_check_mode != "off":
        audio_result = validate_audio(path, cfg.audio_check_mode)
        if not audio_result.valid:
            result.valid = False
            result.scenario = "audio_corrupt"
            result.detail = audio_result.error
            result.corrupt_files = [
                name for name, _ in audio_result.failed_files]
            logger.warning(
                f"AUDIO CORRUPT: {label} "
                f"({len(result.corrupt_files)} files failed ffmpeg decode)")
            return result

    # --- Resolve VBR status and avg bitrate via filesystem inspection ---
    # Callers supply VBR/min_bitrate from different sources:
    #   * Auto path → slskd metadata (usually populated, but None on resumed
    #     downloads rebuilt from ActiveDownloadState).
    #   * Force/manual path → mutagen on the local files (can be None on
    #     broken headers).
    # Neither source provides an average bitrate, which the VBR threshold
    # gate (issue #93) needs to tell genuine V0 (~245kbps avg) apart from a
    # VBR transcode masquerading as V0 (~180kbps avg). Always inspect MP3
    # downloads so we have avg data; a mutagen walk on a 12-track album is
    # ~100ms and far cheaper than the spectral analysis it might save.
    avg_bitrate_bps: int | None = None
    filetype_lower = (download_filetype or "").lower()
    if "mp3" in filetype_lower and "flac" not in filetype_lower:
        inspection = inspect_local_files(path)
        if download_is_vbr is None and inspection.is_vbr is not None:
            download_is_vbr = inspection.is_vbr
        if download_min_bitrate_bps is None:
            download_min_bitrate_bps = inspection.min_bitrate_bps
        avg_bitrate_bps = inspection.avg_bitrate_bps

    # --- Spectral gate ---
    # Threshold: cfg.quality_ranks.mp3_vbr.excellent (the same V0 boundary
    # transcode_detection() uses). Single source of truth per the
    # "No Parallel Code Paths" rule — retuning one also retunes the other.
    avg_bitrate_kbps = (avg_bitrate_bps // 1000) if avg_bitrate_bps else None
    if not _needs_spectral_check(
        download_filetype, download_is_vbr,
        avg_bitrate_kbps=avg_bitrate_kbps,
        vbr_threshold_kbps=cfg.quality_ranks.mp3_vbr.excellent,
    ):
        return result

    try:
        dl_sp = spectral_analyze(path, trim_seconds=30)
        dl_grade = dl_sp.grade
        dl_cliff_bitrate = dl_sp.estimated_bitrate_kbps
        dl_suspect_pct = dl_sp.suspect_pct
        cliff_count = sum(
            1 for track in getattr(dl_sp, "tracks", [])
            if getattr(track, "cliff_detected", False)
        )
        result.download_spectral = SpectralMeasurement.from_parts(
            dl_grade, dl_cliff_bitrate)
        logger.info(
            f"SPECTRAL: {label} grade={dl_grade}, "
            f"estimated_bitrate={dl_cliff_bitrate}kbps, "
            f"suspect={dl_suspect_pct:.0f}%, cliffs={cliff_count}")
    except Exception:
        # Keep the log prefix pre-refactor had ("SPECTRAL: failed for ...")
        # so operators grepping for it continue to match.
        logger.exception(f"SPECTRAL: failed for {label}")
        return result

    # --- Existing-album lookup + analysis ---
    if mb_release_id:
        existing_min, existing_spectral = _analyze_existing(mb_release_id, cfg)
        result.existing_min_bitrate = existing_min
        result.existing_spectral = existing_spectral

    # --- Fall back to persisted spectral state when BeetsDB couldn't walk ---
    # Only fires when BeetsDB FOUND the album (existing_min_bitrate set) but
    # couldn't measure spectral from its album_path (stale/missing directory).
    # When BeetsDB returns no album at all — album deleted, beets DB offline,
    # or first-time import — we must NOT use stale album_requests.min_bitrate
    # as "proof" that something is still on disk, or the gate would reject
    # a valid redownload against non-existent state.
    #
    # GRADE-AWARE: only use the stored spectral_bitrate when the grade is a
    # transcode grade (suspect/likely_transcode). A stored
    # ``grade=genuine, bitrate=96`` is stale (genuine files have no cliff),
    # and admitting a 96kbps "existing" would let a real transcode be imported
    # as an upgrade. Matches ``compute_effective_override_bitrate`` and
    # ``load_quality_gate_state`` — the same rule applied across the pipeline.
    beets_knows_album = result.existing_min_bitrate is not None
    if (beets_knows_album
            and result.existing_spectral is None
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
                        result.existing_spectral = stored
                        logger.info(
                            f"SPECTRAL: {label} using persisted "
                            f"current_spectral (grade={stored.grade}, "
                            f"bitrate={stored.bitrate_kbps}kbps) — BeetsDB "
                            "lookup returned no spectral measurement")
        except Exception:
            logger.debug("failed to read persisted spectral state",
                         exc_info=True)

    # --- Persist spectral state to DB (if wired) ---
    if db is not None and request_id is not None:
        written = _persist_spectral_state(
            db=db, request_id=request_id,
            download_spectral=result.download_spectral,
            existing_spectral=result.existing_spectral,
            existing_min_bitrate=result.existing_min_bitrate,
            label=label,
            propagate_download_to_existing=propagate_download_to_existing,
        )
        result.existing_spectral = written

    # --- Spectral decision ---
    existing_cliff_bitrate = (
        result.existing_spectral.bitrate_kbps
        if result.existing_spectral is not None else None
    )
    # 4-arg form matches main's _apply_spectral_decision exactly. The
    # cliff-count heuristic (``download_min_bitrate`` + ``cliff_track_count``
    # kwargs on spectral_import_decision) lives on a separate branch with
    # its own tests; wire it in here once that lands.
    spectral_decision = spectral_import_decision(
        dl_grade, dl_cliff_bitrate, existing_cliff_bitrate,
        existing_min_bitrate=result.existing_min_bitrate,
    )

    effective_existing = (
        existing_cliff_bitrate or result.existing_min_bitrate or 0)
    if spectral_decision == "reject":
        result.valid = False
        result.scenario = "spectral_reject"
        result.detail = (
            f"spectral {dl_cliff_bitrate}kbps <= existing {effective_existing}kbps")
        logger.warning(
            f"SPECTRAL REJECT: {label} "
            f"new spectral {dl_cliff_bitrate}kbps <= existing {effective_existing}kbps")
        if db is not None and request_id is not None and usernames:
            for username in usernames:
                try:
                    db.add_denylist(
                        request_id, username,
                        f"spectral: {dl_cliff_bitrate}kbps <= existing {effective_existing}kbps")
                except Exception:
                    logger.exception(
                        f"Failed to denylist {username} for request {request_id}")
            logger.info(f"  Denylisted {usernames} for request {request_id}")
    elif spectral_decision == "import_upgrade":
        logger.info(
            f"SPECTRAL UPGRADE: {label} suspect at {dl_cliff_bitrate}kbps "
            f"but > existing {effective_existing}kbps, importing")
    elif spectral_decision == "import_no_exist":
        logger.info(
            f"SPECTRAL: {label} suspect at {dl_cliff_bitrate}kbps "
            f"but no existing album, importing")

    return result
