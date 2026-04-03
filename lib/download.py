"""Download processing — monitoring, completion, and orchestration.

Extracted from soularr.py. All functions receive a SoularrContext
instead of reading module-level globals.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, TYPE_CHECKING

import music_tag

from lib.grab_list import GrabListEntry, DownloadFile
from lib.quality import (spectral_import_decision, SpectralContext,
                         ActiveDownloadState, ActiveDownloadFileState)
from lib.import_dispatch import (_build_download_info, dispatch_import)
from lib.util import (sanitize_folder_name, move_failed_import, stage_to_ai,
                      repair_mp3_headers, validate_audio, log_validation_result)
from lib.beets_db import BeetsDB

if TYPE_CHECKING:
    from lib.context import SoularrContext
    from lib.quality import ValidationResult

logger = logging.getLogger("soularr")


# Lazy import for spectral analysis — avoids hard dep on sox at import time
def spectral_analyze(folder: str, trim_seconds: int = 30) -> Any:
    """Proxy to spectral_check.analyze_album (lazy import)."""
    lib_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lib")
    if lib_dir not in sys.path:
        sys.path.insert(0, lib_dir)
    from spectral_check import analyze_album
    return analyze_album(folder, trim_seconds=trim_seconds)


# === slskd transfer helpers ===

def cancel_and_delete(files: list[Any], ctx: SoularrContext) -> None:
    """Cancel downloads and remove their directories."""
    for file in files:
        if not file.id:
            continue  # Transfer vanished or never assigned — skip cancel
        try:
            ctx.slskd.transfers.cancel_download(username=file.username, id=file.id)
        except Exception:
            logger.warning(f"Failed to cancel download {file.filename} for {file.username}",
                           exc_info=True)
        delete_dir = os.path.join(ctx.cfg.slskd_download_dir, file.file_dir.split("\\")[-1])
        if os.path.isdir(delete_dir):
            shutil.rmtree(delete_dir)


def slskd_download_status(downloads: list[Any], ctx: SoularrContext) -> bool:
    """Get status of each download file from slskd API."""
    ok = True
    for file in downloads:
        try:
            status = ctx.slskd.transfers.get_download(file.username, file.id)
            file.status = status
        except Exception:
            logger.exception(f"Error getting download status of {file.filename}")
            file.status = None
            ok = False
    return ok


def slskd_do_enqueue(username: str, files: list[dict[str, Any]],
                     file_dir: str, ctx: SoularrContext) -> list[DownloadFile] | None:
    """Enqueue files for download via slskd. Returns DownloadFile list or None."""
    try:
        enqueue = ctx.slskd.transfers.enqueue(username=username, files=files)
    except Exception:
        logger.debug("Enqueue failed", exc_info=True)
        return None
    if not enqueue:
        return None

    downloads: list[DownloadFile] = []
    time.sleep(5)
    try:
        download_list = ctx.slskd.transfers.get_downloads(username=username)
    except Exception:
        logger.warning(f"Failed to get download status for {username} after enqueue",
                       exc_info=True)
        return None
    for file in files:
        for directory in download_list.get("directories", []):
            if directory["directory"] == file_dir:
                for slskd_file in directory["files"]:
                    if file["filename"] == slskd_file["filename"]:
                        downloads.append(DownloadFile(
                            filename=file["filename"],
                            id=slskd_file["id"],
                            file_dir=file_dir,
                            username=username,
                            size=file["size"],
                        ))
    return downloads


def downloads_all_done(downloads: list[Any]) -> tuple[bool, list[Any] | None, int]:
    """Check status of all files. Returns (all_done, error_list_or_none, remote_queue_count)."""
    all_done = True
    error_list: list[Any] = []
    remote_queue = 0
    for file in downloads:
        if file.status is not None:
            state = file.status.get("state", "")
            if state != "Completed, Succeeded":
                all_done = False
            if state in (
                "Completed, Cancelled",
                "Completed, TimedOut",
                "Completed, Errored",
                "Completed, Rejected",
                "Completed, Aborted",
            ):
                error_list.append(file)
            if file.status["state"] == "Queued, Remotely":
                remote_queue += 1
    return all_done, error_list if error_list else None, remote_queue


# === Spectral context gathering ===

def _gather_spectral_context(album_data: GrabListEntry, import_folder: str,
                             ctx: SoularrContext) -> SpectralContext:
    """Gather spectral analysis data for a non-VBR MP3 download.

    Runs spectral analysis on the downloaded files and (if the album exists
    in beets) on the existing files for comparison. Returns a SpectralContext
    with all data needed by spectral_import_decision().
    """
    dl_pre = _build_download_info(album_data)
    filetype_str = (dl_pre.filetype or "").lower()
    is_vbr = dl_pre.is_vbr or False
    is_mp3 = "mp3" in filetype_str and "flac" not in filetype_str
    if not (is_mp3 and not is_vbr):
        return SpectralContext(needs_check=False)

    spec_ctx = SpectralContext(needs_check=True)
    try:
        spectral_result = spectral_analyze(import_folder, trim_seconds=30)
        spec_ctx.grade = spectral_result.grade
        spec_ctx.bitrate = spectral_result.estimated_bitrate_kbps
        spec_ctx.suspect_pct = spectral_result.suspect_pct
        logger.info(f"SPECTRAL: {album_data.artist} - {album_data.title} "
                    f"grade={spec_ctx.grade}, estimated_bitrate={spec_ctx.bitrate}kbps, "
                    f"suspect={spec_ctx.suspect_pct:.0f}%")
        # Check existing beets files for comparison
        mb_id = album_data.mb_release_id
        if mb_id:
            try:
                with BeetsDB() as beets:
                    existing_info = beets.get_album_info(mb_id)
                if existing_info:
                    spec_ctx.existing_min_bitrate = existing_info.min_bitrate_kbps
                    if os.path.isdir(existing_info.album_path):
                        existing_spectral = spectral_analyze(
                            existing_info.album_path, trim_seconds=30)
                        spec_ctx.existing_spectral_bitrate = (
                            existing_spectral.estimated_bitrate_kbps)
                        spec_ctx.existing_spectral_grade = existing_spectral.grade
                        logger.info(
                            f"SPECTRAL: existing on disk: "
                            f"grade={existing_spectral.grade}, "
                            f"estimated_bitrate="
                            f"{existing_spectral.estimated_bitrate_kbps}kbps, "
                            f"beets_min={existing_info.min_bitrate_kbps}kbps")
            except Exception:
                logger.exception("SPECTRAL: failed to check existing files")
    except Exception:
        logger.exception(f"SPECTRAL: failed for {album_data.artist} - {album_data.title}")
    return spec_ctx


# === Download completion processing ===

def process_completed_album(album_data: GrabListEntry, failed_grab: list[Any],
                            ctx: SoularrContext) -> bool:
    """Process a fully-downloaded album: move files, tag, validate, stage/import."""
    import_folder_name = sanitize_folder_name(
        f"{album_data.artist} - {album_data.title} ({album_data.year})")
    import_folder_fullpath = os.path.join(ctx.cfg.slskd_download_dir, import_folder_name)
    rm_dirs: list[str] = []
    moved_files_history: list[tuple[str, str]] = []
    if not os.path.exists(import_folder_fullpath):
        os.mkdir(import_folder_fullpath)
    for file in album_data.files:
        file_folder = file.file_dir.split("\\")[-1]
        filename = file.filename.split("\\")[-1]
        src_folder = os.path.join(ctx.cfg.slskd_download_dir, file_folder)
        if src_folder not in rm_dirs:
            rm_dirs.append(src_folder)
        src_file = os.path.join(src_folder, filename)
        if file.disk_no is not None and file.disk_count is not None and file.disk_count > 1:
            filename = f"Disk {file.disk_no} - {filename}"
        dst_file = os.path.join(import_folder_fullpath, filename)
        file.import_path = dst_file
        try:
            shutil.move(src_file, dst_file)
            moved_files_history.append((src_file, dst_file))
        except Exception:
            logger.exception(f"Failed to move: {file.filename} to temp location for import. Rolling back...")
            for src, dst in reversed(moved_files_history):
                try:
                    shutil.move(dst, src)
                except Exception:
                    logger.exception(f"Critical failure during rollback: could not move {dst} back to {src}")
            try:
                os.rmdir(import_folder_fullpath)
            except OSError:
                logger.warning(f"Could not remove temp import directory {import_folder_fullpath}")
            return False
    else:  # Only runs if all files are successfully moved
        for rm_dir in rm_dirs:
            if rm_dir != import_folder_fullpath:
                try:
                    os.rmdir(rm_dir)
                except OSError:
                    logger.warning(f"Skipping removal of {rm_dir} because it's not empty.")
        logger.info(f"Processing completed download: {album_data.artist} - {album_data.title}")
        for file in album_data.files:
            try:
                song = music_tag.load_file(file.import_path)
                assert song is not None
                if file.disk_no is not None:
                    song["discnumber"] = file.disk_no
                    song["totaldiscs"] = file.disk_count
                song["albumartist"] = album_data.artist
                song["album"] = album_data.title
                song.save()
            except Exception:
                logger.exception(f"Error writing tags for: {file.import_path}")
        if ctx.cfg.beets_validation_enabled and album_data.mb_release_id:
            _process_beets_validation(album_data, import_folder_fullpath, ctx)
        return True


def _process_beets_validation(album_data: GrabListEntry, import_folder_fullpath: str,
                              ctx: SoularrContext) -> None:
    """Beets validation sub-path of process_completed_album."""
    from lib.beets import beets_validate as _bv
    bv_result = _bv(ctx.cfg.beets_harness_path, import_folder_fullpath,
                    album_data.mb_release_id, ctx.cfg.beets_distance_threshold)
    # Populate source info
    usernames_pre = set(f.username for f in album_data.files if f.username)
    bv_result.soulseek_username = ", ".join(sorted(usernames_pre)) if usernames_pre else None
    bv_result.download_folder = import_folder_fullpath

    if bv_result.valid:
        repair_mp3_headers(import_folder_fullpath)
        audio_result = validate_audio(import_folder_fullpath, ctx.cfg.audio_check_mode)
        if not audio_result.valid:
            bv_result.valid = False
            bv_result.scenario = "audio_corrupt"
            bv_result.detail = audio_result.error
            bv_result.corrupt_files = [
                name for name, _err in audio_result.failed_files]

    # Spectral check for non-VBR MP3 downloads
    if bv_result.valid:
        spec_ctx = _gather_spectral_context(album_data, import_folder_fullpath, ctx)
        if spec_ctx.needs_check and spec_ctx.grade:
            _apply_spectral_decision(album_data, bv_result, spec_ctx,
                                     import_folder_fullpath, ctx)

    if bv_result.valid:
        _handle_valid_result(album_data, bv_result, import_folder_fullpath, ctx)
    else:
        _handle_rejected_result(album_data, bv_result, import_folder_fullpath, ctx)


def _apply_spectral_decision(album_data: GrabListEntry, bv_result: ValidationResult,
                             spec_ctx: SpectralContext,
                             import_folder_fullpath: str,
                             ctx: SoularrContext) -> None:
    """Apply spectral import decision and update album_data/bv_result accordingly."""
    album_data.spectral_grade = spec_ctx.grade
    album_data.spectral_bitrate = spec_ctx.bitrate
    album_data.existing_spectral_bitrate = spec_ctx.existing_spectral_bitrate
    album_data.existing_min_bitrate = spec_ctx.existing_min_bitrate

    # Write on-disk spectral data back to album_requests
    request_id = album_data.db_request_id
    if request_id and ctx.pipeline_db_source:
        try:
            update_kwargs: dict[str, object] = {}
            if spec_ctx.existing_spectral_grade:
                update_kwargs["on_disk_spectral_grade"] = spec_ctx.existing_spectral_grade
            if spec_ctx.existing_spectral_bitrate is not None:
                update_kwargs["on_disk_spectral_bitrate"] = spec_ctx.existing_spectral_bitrate
            if update_kwargs:
                db = ctx.pipeline_db_source._get_db()
                req = db.get_request(request_id)
                if req:
                    db._execute(
                        "UPDATE album_requests SET "
                        + ", ".join(f"{k} = %s" for k in update_kwargs)
                        + " WHERE id = %s",
                        list(update_kwargs.values()) + [request_id])
        except Exception:
            logger.exception("Failed to update on-disk spectral data")

    new_quality = spec_ctx.bitrate
    existing_quality = spec_ctx.existing_spectral_bitrate or 0
    label = f"{album_data.artist} - {album_data.title}"

    spectral_decision = spectral_import_decision(
        spec_ctx.grade, new_quality, existing_quality,
        existing_min_bitrate=spec_ctx.existing_min_bitrate)

    if spectral_decision == "reject":
        logger.warning(
            f"SPECTRAL REJECT: {label} "
            f"new spectral {new_quality}kbps <= existing {existing_quality}kbps")
        usernames = set(f.username for f in album_data.files if f.username)
        if request_id and ctx.pipeline_db_source:
            db = ctx.pipeline_db_source._get_db()
            for username in usernames:
                db.add_denylist(request_id, username,
                                f"spectral: {new_quality}kbps <= existing {existing_quality}kbps")
            logger.info(f"  Denylisted {usernames} for request {request_id}")
        # Set bv_result fields so _handle_rejected_result logs one row with spectral detail
        bv_result.valid = False
        bv_result.scenario = "spectral_reject"
        bv_result.detail = f"spectral {new_quality}kbps <= existing {existing_quality}kbps"
        # Attach spectral info to album_data so _handle_rejected_result picks it up
        album_data.spectral_grade = spec_ctx.grade
        album_data.spectral_bitrate = new_quality
        album_data.existing_spectral_bitrate = existing_quality
    elif spectral_decision == "import_upgrade":
        logger.info(
            f"SPECTRAL UPGRADE: {label} "
            f"suspect at {new_quality}kbps but > existing {existing_quality}kbps, importing")
    elif spectral_decision == "import_no_exist":
        logger.info(
            f"SPECTRAL: {label} "
            f"suspect at {new_quality}kbps but no existing album, importing")


def _handle_valid_result(album_data: GrabListEntry, bv_result: ValidationResult,
                         import_folder_fullpath: str,
                         ctx: SoularrContext) -> None:
    """Handle a valid beets validation result: stage and optionally auto-import."""
    dest = stage_to_ai(album_data, import_folder_fullpath, ctx.cfg.beets_staging_dir)
    log_validation_result(album_data, bv_result, ctx.cfg, dest_path=dest)
    logger.info(f"STAGED: {album_data.artist} - {album_data.title} "
                f"(scenario={bv_result.scenario}, "
                f"distance={bv_result.distance:.4f}) → {dest}")

    dl_info = _build_download_info(album_data)
    dl_info.validation_result = bv_result.to_json()
    if album_data.spectral_grade:
        dl_info.spectral_grade = album_data.spectral_grade
        dl_info.spectral_bitrate = album_data.spectral_bitrate
        dl_info.existing_spectral_bitrate = album_data.existing_spectral_bitrate
        dl_info.existing_min_bitrate = album_data.existing_min_bitrate
        dl_info.slskd_filetype = dl_info.filetype
        dl_info.actual_filetype = dl_info.filetype
    source_type = album_data.db_source or "redownload"
    request_id = album_data.db_request_id
    dist = bv_result.distance if bv_result.distance is not None else 1.0
    if source_type == "request" and dist <= ctx.cfg.beets_distance_threshold:
        dispatch_import(album_data, bv_result, dest, dl_info, request_id, ctx)
    else:
        ctx.pipeline_db_source.mark_done(album_data, bv_result, dest_path=dest,
                                         download_info=dl_info)


def _handle_rejected_result(album_data: GrabListEntry, bv_result: ValidationResult,
                            import_folder_fullpath: str,
                            ctx: SoularrContext) -> None:
    """Handle a rejected beets validation result."""
    failed_dest = move_failed_import(import_folder_fullpath)
    bv_result.failed_path = failed_dest
    log_validation_result(album_data, bv_result, ctx.cfg)
    usernames = set(f.username for f in album_data.files)
    bv_result.denylisted_users = sorted(usernames)
    dl_info = _build_download_info(album_data)
    dl_info.validation_result = bv_result.to_json()
    if album_data.spectral_grade:
        dl_info.spectral_grade = album_data.spectral_grade
        dl_info.spectral_bitrate = album_data.spectral_bitrate
        dl_info.existing_spectral_bitrate = album_data.existing_spectral_bitrate
        dl_info.slskd_filetype = dl_info.filetype
        dl_info.actual_filetype = dl_info.filetype
    ctx.pipeline_db_source.mark_failed(album_data, bv_result, usernames=usernames,
                                       download_info=dl_info)
    logger.warning(f"REJECTED: {album_data.artist} - {album_data.title} "
                   f"(scenario={bv_result.scenario}, "
                   f"distance={bv_result.distance}, "
                   f"detail={bv_result.detail}) "
                   f"| denylisted users: {', '.join(usernames)}")


# === Transfer ID re-derivation ===

def match_transfer_id(
    downloads: dict[str, Any],
    target_filename: str,
) -> str | None:
    """Find the slskd transfer ID for a filename in a get_downloads() response.

    downloads is the return value of slskd.transfers.get_downloads(username).
    Returns the transfer ID string, or None if not found.
    """
    for directory in downloads.get("directories", []):
        for slskd_file in directory.get("files", []):
            if slskd_file.get("filename") == target_filename:
                return slskd_file.get("id", "")
    return None


def rederive_transfer_ids(
    entry: GrabListEntry,
    slskd_client: Any,
) -> None:
    """Re-derive slskd transfer IDs for all files in a GrabListEntry.

    Queries the slskd API for each unique username and matches by filename.
    Updates file.id in-place. Files whose transfers have vanished keep id="".
    """
    by_user: dict[str, list[DownloadFile]] = {}
    for f in entry.files:
        by_user.setdefault(f.username, []).append(f)

    for username, files in by_user.items():
        try:
            downloads = slskd_client.transfers.get_downloads(username=username)
        except Exception:
            logger.warning(f"Failed to get downloads for {username} — transfers may have vanished")
            continue
        for f in files:
            tid = match_transfer_id(downloads, f.filename)
            if tid is not None:
                f.id = tid
            else:
                logger.debug(f"Transfer not found for {f.filename} from {username}")


# === GrabListEntry reconstruction from DB ===

def reconstruct_grab_list_entry(
    request: dict[str, Any],
    state: ActiveDownloadState,
) -> GrabListEntry:
    """Rebuild GrabListEntry from a DB row + persisted download state.

    Does NOT set slskd transfer IDs — those are ephemeral and must be
    re-derived from the live slskd API by the caller.
    """
    files = []
    for f in state.files:
        files.append(DownloadFile(
            filename=f.filename,
            id="",                  # Must be re-derived from slskd API
            file_dir=f.file_dir,
            username=f.username,
            size=f.size,
            disk_no=f.disk_no,
            disk_count=f.disk_count,
        ))
    year = request.get("year")
    return GrabListEntry(
        album_id=request["id"],
        files=files,
        filetype=state.filetype,
        title=request["album_title"],
        artist=request["artist_name"],
        year=str(year) if year else "",
        mb_release_id=request.get("mb_release_id") or "",
        db_request_id=request["id"],
        db_source=request.get("source"),
        db_quality_override=request.get("quality_override"),
    )


# === Async download polling ===

def _reset_to_wanted(
    db: Any,
    request_id: int,
) -> None:
    """Atomically clear download state and reset to wanted in a single UPDATE."""
    now = datetime.now(timezone.utc)
    db._execute("""
        UPDATE album_requests
        SET status = 'wanted',
            active_download_state = NULL,
            updated_at = %s
        WHERE id = %s
    """, (now, request_id))
    db.conn.commit()


def _timeout_album(
    entry: GrabListEntry,
    request_id: int,
    reason: str,
    ctx: SoularrContext,
) -> None:
    """Handle download timeout: cancel, log, reset to wanted."""
    cancel_and_delete(entry.files, ctx)

    total = len(entry.files)
    completed = sum(1 for f in entry.files
                    if f.status and f.status.get("state") == "Completed, Succeeded")

    dl_info = _build_download_info(entry)

    logger.info(f"DOWNLOAD TIMEOUT: {entry.artist} - {entry.title} "
                f"({completed}/{total} files done, reason={reason})")

    db = ctx.pipeline_db_source._get_db()
    db.log_download(
        request_id=request_id,
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        outcome="timeout",
        error_message=reason,
    )
    db.record_attempt(request_id, "download")
    _reset_to_wanted(db, request_id)


def poll_active_downloads(ctx: SoularrContext) -> None:
    """Poll slskd for status of all downloading albums.

    For each album with status='downloading':
    1. Reconstruct GrabListEntry from DB + ActiveDownloadState
    2. Re-derive slskd transfer IDs
    3. Mark files with vanished transfers as errored (synthetic status)
    4. Poll file status for remaining files
    5. If all complete → process_completed_album()
    6. If timeout exceeded → cancel, log, reset to wanted
    7. If errors → retry individual files (in-memory, max 5 per file)
    """
    db = ctx.pipeline_db_source._get_db()
    downloading = db.get_downloading()

    if not downloading:
        return

    logger.info(f"Polling {len(downloading)} active download(s)...")

    for row in downloading:
        request_id = row["id"]
        raw_state = row.get("active_download_state")
        if not raw_state:
            # Crash recovery: downloading with no state means process_completed_album
            # crashed on a previous run. Reset to wanted so it gets re-searched.
            logger.error(f"Downloading album {request_id} has no active_download_state — "
                         f"resetting to wanted")
            _reset_to_wanted(db, request_id)
            continue

        # psycopg2 returns JSONB as dict, not string — use from_dict directly
        if isinstance(raw_state, dict):
            state = ActiveDownloadState.from_dict(raw_state)
        else:
            state = ActiveDownloadState.from_json(raw_state)
        entry = reconstruct_grab_list_entry(row, state)

        # Re-derive transfer IDs from slskd
        rederive_transfer_ids(entry, ctx.slskd)

        # Check if all transfers have vanished (slskd restart, user offline)
        all_vanished = all(f.id == "" for f in entry.files)
        if all_vanished:
            _timeout_album(entry, request_id, "all transfers vanished from slskd", ctx)
            continue

        # Mark files with vanished transfers as errored
        for f in entry.files:
            if f.id == "":
                f.status = {"state": "Completed, Errored"}

        # Check absolute timeout from enqueued_at
        enqueued_at = datetime.fromisoformat(state.enqueued_at)
        now = datetime.now(timezone.utc)
        elapsed_seconds = (now - enqueued_at).total_seconds()

        if elapsed_seconds >= ctx.cfg.stalled_timeout:
            _timeout_album(entry, request_id,
                          f"stalled_timeout {ctx.cfg.stalled_timeout}s exceeded "
                          f"({elapsed_seconds:.0f}s elapsed)", ctx)
            continue

        # Poll status for files that have transfer IDs
        files_with_ids = [f for f in entry.files if f.id]
        if not slskd_download_status(files_with_ids, ctx):
            logger.warning(f"API error polling {entry.artist} - {entry.title} — "
                          f"will retry next cycle")
            continue

        album_done, problems, queued = downloads_all_done(entry.files)

        # Remote queue timeout: all files stuck in remote queue
        if queued == len(entry.files) and elapsed_seconds >= ctx.cfg.remote_queue_timeout:
            _timeout_album(entry, request_id,
                          f"remote_queue_timeout {ctx.cfg.remote_queue_timeout}s exceeded "
                          f"(all {queued} files queued remotely)", ctx)
            continue

        if album_done and problems is None:
            logger.info(f"Download complete: {entry.artist} - {entry.title}")
            # Clear active_download_state but keep status='downloading'.
            # process_completed_album will set final status via mark_done/mark_failed.
            db.clear_download_state(request_id)
            success = process_completed_album(entry, [], ctx)
            # Safety net: if process_completed_album returned without setting
            # a final status (happens when beets_validation_enabled=False or
            # mb_release_id is empty), the album is still 'downloading'.
            refreshed = db.get_request(request_id)
            if refreshed and refreshed["status"] == "downloading":
                if success:
                    logger.info(f"  process_completed_album succeeded without "
                               f"setting status — setting imported")
                    db.update_status(request_id, "imported")
                else:
                    logger.warning(f"  process_completed_album failed without "
                                  f"setting status — resetting to wanted")
                    _reset_to_wanted(db, request_id)
            continue

        if problems is not None:
            # All files errored → timeout the album
            if len(problems) == len(entry.files):
                _timeout_album(entry, request_id,
                              f"all {len(problems)} files errored", ctx)
                continue

            # Partial errors: attempt re-enqueue for errored files
            for file in problems:
                state_str = file.status.get("state", "") if file.status else ""
                if state_str in ("Completed, Cancelled", "Completed, TimedOut",
                                 "Completed, Errored", "Completed, Aborted",
                                 "Completed, Rejected"):
                    for df in entry.files:
                        if df.filename == file.filename:
                            if df.retry is None:
                                df.retry = 0
                            df.retry += 1
                            if df.retry < 5:
                                logger.info(f"Re-enqueue failed file (attempt {df.retry}): "
                                           f"{file.filename}")
                                requeue = slskd_do_enqueue(
                                    file.username,
                                    [{"filename": file.filename, "size": file.size}],
                                    file.file_dir, ctx)
                                if requeue:
                                    df.id = requeue[0].id
                            else:
                                logger.warning(f"File exceeded retry limit: {file.filename}")
                            break

        # Still in progress — log and continue to next album
        files_done = sum(1 for f in entry.files
                        if f.status and f.status.get("state") == "Completed, Succeeded")
        logger.info(f"In progress: {entry.artist} - {entry.title} "
                    f"({files_done}/{len(entry.files)} files, "
                    f"{elapsed_seconds/60:.1f}min elapsed)")


# === Download monitoring ===

def monitor_downloads(grab_list: dict[Any, GrabListEntry],
                      failed_grab: list[Any],
                      ctx: SoularrContext) -> None:
    """Monitor active downloads, handle retries/failures, process completions."""
    def delete_album(reason: str) -> None:
        entry = grab_list[album_id]
        cancel_and_delete(entry.files, ctx)
        usernames = set(f.username for f in entry.files if f.username)
        total = len(entry.files)
        completed = sum(1 for f in entry.files
                        if f.status and f.status.get("state") == "Completed, Succeeded")
        elapsed = time.time() - (entry.count_start or time.time())
        elapsed_min = elapsed / 60
        logger.info(f"{reason} Album: {entry.title} Artist: {entry.artist} "
                    f"({completed}/{total} files done, {elapsed_min:.1f}min elapsed, "
                    f"stalled_timeout={ctx.cfg.stalled_timeout}s, "
                    f"cfg.remote_queue_timeout={ctx.cfg.remote_queue_timeout}s)")
        del grab_list[album_id]

    while True:
        done_count = 0
        for album_id in list(grab_list.keys()):
            entry = grab_list[album_id]
            if slskd_download_status(entry.files, ctx):
                album_done, problems, queued = downloads_all_done(entry.files)
                if entry.count_start is None:
                    entry.count_start = time.time()
                if (time.time() - entry.count_start) >= ctx.cfg.stalled_timeout:
                    delete_album("Timeout waiting for download of")
                    continue
                if queued == len(entry.files):
                    if (time.time() - entry.count_start) >= ctx.cfg.remote_queue_timeout:
                        delete_album("Timeout waiting for download of")
                        continue
                if queued > 0 and done_count > 0:
                    completed = sum(1 for f in entry.files
                                    if f.status and f.status["state"] == "Completed, Succeeded")
                    if completed + queued == len(entry.files):
                        if (time.time() - entry.count_start) >= ctx.cfg.remote_queue_timeout:
                            delete_album("Timeout waiting for stuck remote queue file in")
                            continue
                done_count += album_done
                if problems is not None:
                    logger.debug("We got problems!")
                    _handle_download_problems(problems, entry, album_id,
                                              grab_list, delete_album, ctx)
                else:
                    if album_done:
                        logger.info(f"Completed download of Album: {entry.title} "
                                    f"Artist: {entry.artist}")
                        process_completed_album(entry, failed_grab, ctx)
                        del grab_list[album_id]
            else:
                if entry.error_count is None:
                    entry.error_count = 0
                entry.error_count += 1
                if entry.error_count >= 60:
                    logger.error(f"API errors for {entry.artist} - {entry.title} "
                                 f"({entry.error_count} consecutive), giving up")
                    delete_album("API errors for")

        if len(grab_list) < 1:
            break
        time.sleep(5)


def _handle_download_problems(problems: list[Any], entry: GrabListEntry,
                              album_id: Any, grab_list: dict[Any, GrabListEntry],
                              delete_album: Callable[[str], None],
                              ctx: SoularrContext) -> None:
    """Handle problem files during download monitoring."""
    for file in problems:
        logger.debug(f"Checking {file.filename}")
        match file.status["state"]:
            case (
                "Completed, Cancelled" | "Completed, TimedOut"
                | "Completed, Errored" | "Completed, Aborted"
            ):
                abort = False
                if len(problems) == len(entry.files):
                    delete_album("Failed grab of")
                    break
                for download_file in entry.files:
                    if file.filename == download_file.filename:
                        if download_file.retry is None:
                            download_file.retry = 0
                        download_file.retry += 1
                        if download_file.retry < 5:
                            retry = download_file.retry
                            size = file.size
                            data_dict = [{"filename": file.filename, "size": size}]
                            logger.info(f"Download error. Requeue file: {file.filename}")
                            requeue = slskd_do_enqueue(
                                file.username, data_dict, file.file_dir, ctx)
                            if requeue is not None:
                                download_file.id = requeue[0].id
                                download_file.retry = retry
                                time.sleep(1)
                                _ = slskd_download_status(entry.files, ctx)
                            else:
                                delete_album("Failed grab of")
                                abort = True
                                break
                        else:
                            delete_album("Failed grab of")
                            abort = True
                            break
                if abort:
                    break
            case "Completed, Rejected":
                if len(problems) == len(entry.files):
                    delete_album("Failed grab of")
                    break
                else:
                    if entry.rejected_retries is None:
                        entry.rejected_retries = 0
                    working_count = len(entry.files) - len(problems)
                    for gfile in entry.files:
                        if gfile.status and gfile.status["state"] in [
                            "Completed, Succeeded",
                            "Queued, Remotely",
                            "Queued, Locally",
                        ]:
                            working_count -= 1
                    if working_count == 0:
                        if entry.rejected_retries < int(len(entry.files) * 1.2):
                            abort = False
                            for gfile in entry.files:
                                if gfile.filename == file.filename:
                                    size = file.size
                                    data_dict = [{"filename": file.filename, "size": size}]
                                    logger.info(f"Download error. Requeue file: {file.filename}")
                                    requeue = slskd_do_enqueue(
                                        file.username, data_dict, file.file_dir, ctx)
                                    if requeue is not None:
                                        gfile.id = requeue[0].id
                                        entry.rejected_retries += 1
                                        _ = slskd_download_status(entry.files, ctx)
                                        abort = True
                                        break
                                    else:
                                        cancel_and_delete(entry.files, ctx)
                                        logger.info(f"Failed grab of Album: {entry.title} "
                                                    f"Artist: {entry.artist}")
                                        del grab_list[album_id]
                                        abort = True
                                        break
                            if abort:
                                break
                        else:
                            delete_album("Failed grab of")
                            break
            case _:
                logger.error(
                    "Not sure how I got here. This shouldn't be possible for problem files!")


# === Top-level orchestration ===

def grab_most_wanted(albums: list[Any],
                     search_and_queue: Callable[..., tuple[dict, list, list]],
                     ctx: SoularrContext) -> int:
    """Search, enqueue, monitor, and process wanted albums.

    search_and_queue is injected to avoid circular imports with soularr.py's
    search logic.
    """
    grab_list, failed_search, failed_grab = search_and_queue(albums)

    total_albums = len(grab_list)
    logger.info(f"Total Downloads added: {total_albums}")
    for album_id in grab_list:
        logger.info(f"Album: {grab_list[album_id].title} Artist: {grab_list[album_id].artist}")
    logger.info(f"Failed to grab: {len(failed_grab)}")
    for album in failed_grab:
        logger.info(f"Album: {album.title} Artist: {album.artist_name}")

    logger.info("-------------------")
    logger.info(f"Waiting for downloads... monitor at: "
                f"{''.join([ctx.cfg.slskd_host_url, ctx.cfg.slskd_url_base, 'downloads'])}")

    monitor_downloads(grab_list, failed_grab, ctx)

    count = len(failed_search) + len(failed_grab)
    for album in failed_search:
        logger.info(f"Search failed for Album: {album.title} - Artist: {album.artist_name}")
    for album in failed_grab:
        logger.info(f"Download failed for Album: {album.title} - Artist: {album.artist_name}")

    return count
