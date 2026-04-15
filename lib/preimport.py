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
from lib.quality import SpectralMeasurement, spectral_import_decision
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
    download_min_bitrate_kbps: int | None = None
    cliff_track_count: int | None = None


def _normalize_bps_to_kbps(value: int | None) -> int | None:
    """Normalize a bitrate that may be bps or kbps to kbps."""
    if value is None:
        return None
    return value // 1000 if value > 1000 else value


AUDIO_EXTS = ("mp3", "flac", "alac", "m4a", "ogg", "opus", "wav", "aac")


@dataclass
class LocalFileInspection:
    """Result of inspecting audio files on disk at a force/manual import path.

    Populated by ``inspect_local_files`` so callers of ``run_preimport_gates``
    that have no DownloadFile metadata (force/manual paths) can still supply
    filetype / bitrate / vbr hints.
    """
    filetype: str = ""           # comma-separated lowercase extensions
    min_bitrate_bps: int | None = None
    is_vbr: bool | None = None


def inspect_local_files(path: str) -> LocalFileInspection:
    """Scan ``path`` for audio files and report filetype + bitrate + VBR hints.

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

    for name in os.listdir(path):
        full = os.path.join(path, name)
        if not os.path.isfile(full) or "." not in name:
            continue
        ext = name.rsplit(".", 1)[-1].lower()
        if ext not in AUDIO_EXTS:
            continue
        extensions.add(ext)
        if ext == "mp3":
            try:
                from mutagen.mp3 import MP3  # type: ignore[import-untyped]
                mp3 = MP3(full)
                br = getattr(mp3.info, "bitrate", None)
                br_mode = getattr(mp3.info, "bitrate_mode", None)
                if br is not None:
                    min_bitrate = br if min_bitrate is None else min(min_bitrate, br)
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
    )


def _needs_spectral_check(filetype: str, is_vbr: bool | None) -> bool:
    """Spectral check runs on non-VBR MP3 downloads only.

    FLAC uses a different flow (convert → V0 → compare). VBR MP3 carries its
    own bitrate signal. CBR MP3 is where spectral cliff detection pays off.
    """
    filetype_lower = (filetype or "").lower()
    is_mp3 = "mp3" in filetype_lower and "flac" not in filetype_lower
    return is_mp3 and not bool(is_vbr)


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
) -> SpectralMeasurement | None:
    """Write the on-disk spectral state to album_requests.

    When there's no measured existing spectral but there IS an existing
    album on disk (existing_min_bitrate set), propagate the download's
    spectral as current. Without the min-bitrate guard, a fresh album with
    nothing on disk would adopt its own download spectral and self-reject.
    Returns the measurement actually written (or None if nothing to write).
    """
    to_write = existing_spectral
    if (to_write is None
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

    # --- Audio integrity gate ---
    if cfg.audio_check_mode != "off":
        try:
            repair_mp3_headers(path)
        except Exception:
            logger.debug("repair_mp3_headers failed", exc_info=True)
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

    # --- Spectral gate (non-VBR MP3 only) ---
    if not _needs_spectral_check(download_filetype, download_is_vbr):
        return result

    dl_bitrate_kbps = _normalize_bps_to_kbps(download_min_bitrate_bps)
    result.download_min_bitrate_kbps = dl_bitrate_kbps

    try:
        dl_sp = spectral_analyze(path, trim_seconds=30)
        dl_grade = dl_sp.grade
        dl_cliff_bitrate = dl_sp.estimated_bitrate_kbps
        dl_suspect_pct = dl_sp.suspect_pct
        cliff_count = sum(
            1 for track in getattr(dl_sp, "tracks", [])
            if getattr(track, "cliff_detected", False)
        )
        result.cliff_track_count = cliff_count
        result.download_spectral = SpectralMeasurement.from_parts(
            dl_grade, dl_cliff_bitrate)
        logger.info(
            f"SPECTRAL: {label} grade={dl_grade}, "
            f"estimated_bitrate={dl_cliff_bitrate}kbps, "
            f"suspect={dl_suspect_pct:.0f}%, cliffs={cliff_count}")
    except Exception:
        logger.exception(f"SPECTRAL: failed to analyze download for {label}")
        return result

    # --- Existing-album lookup + analysis ---
    if mb_release_id:
        existing_min, existing_spectral = _analyze_existing(mb_release_id, cfg)
        result.existing_min_bitrate = existing_min
        result.existing_spectral = existing_spectral

    # --- Persist spectral state to DB (if wired) ---
    if db is not None and request_id is not None:
        written = _persist_spectral_state(
            db=db, request_id=request_id,
            download_spectral=result.download_spectral,
            existing_spectral=result.existing_spectral,
            existing_min_bitrate=result.existing_min_bitrate,
            label=label,
        )
        result.existing_spectral = written

    # --- Spectral decision ---
    existing_cliff_bitrate = (
        result.existing_spectral.bitrate_kbps
        if result.existing_spectral is not None else None
    )
    # Uses the 4-arg form of spectral_import_decision (matches main's
    # _apply_spectral_decision exactly). The cliff_track_count is captured on
    # PreImportGateResult for audit/logging but not forwarded — the
    # decision-function side of the cliff heuristic lives on a separate branch
    # and will be wired in once it lands.
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
