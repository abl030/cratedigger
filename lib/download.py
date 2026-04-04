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
                         ActiveDownloadState, ActiveDownloadFileState,
                         DownloadDecision, decide_download_action)
from lib.import_dispatch import (_build_download_info, dispatch_import)
from lib.transitions import apply_transition
from lib.util import (sanitize_folder_name, move_failed_import, stage_to_ai,
                      repair_mp3_headers, validate_audio, log_validation_result)
from lib.beets_db import BeetsDB

if TYPE_CHECKING:
    from lib.context import SoularrContext
    from lib.quality import ValidationResult

logger = logging.getLogger("soularr")
MAX_FILE_RETRIES = 5


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


def slskd_download_status(downloads: list[Any], ctx: SoularrContext,
                          *, snapshot: list[dict[str, Any]] | None = None) -> bool:
    """Get status of each download file from slskd API.

    When snapshot is provided, matches locally against the pre-fetched bulk
    download list instead of making per-file API calls.
    """
    ok = True
    for file in downloads:
        try:
            if snapshot is not None:
                transfer = match_transfer(snapshot, file.filename, username=file.username)
                if transfer is not None:
                    file.status = dict(transfer)
                else:
                    file.status = None
                    ok = False
            else:
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
    download_list = _get_all_downloads_snapshot(
        ctx.slskd,
        purpose=f"download status for {username} after enqueue",
    )
    if download_list is None:
        return None
    for file in files:
        transfer_id = match_transfer_id(
            download_list,
            file["filename"],
            username=username,
        )
        if transfer_id is not None:
            downloads.append(DownloadFile(
                filename=file["filename"],
                id=transfer_id,
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


def _all_files_remotely_queued(downloads: list[Any], remote_queue_count: int) -> bool:
    """Return True when every tracked file is still queued on the remote peer.

    This is intentionally separate from stalled transfer detection: a file that has
    never started uploading should be governed by the remote queue timeout, not the
    no-progress timeout used for active transfers.
    """
    return bool(downloads) and remote_queue_count == len(downloads)


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
        if os.path.exists(dst_file) and not os.path.exists(src_file):
            # Resume safely after a crash that already moved this file.
            continue
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
        assert request_id is not None, "pipeline request must have db_request_id"
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


# === ActiveDownloadState building ===

def build_active_download_state(
    entry: GrabListEntry,
    *,
    enqueued_at: str | None = None,
    last_progress_at: str | None = None,
    processing_started_at: str | None = None,
) -> ActiveDownloadState:
    """Build an ActiveDownloadState from a GrabListEntry.

    Callers can pass the original enqueued_at/processing_started_at when
    persisting updated retry state across polling cycles.
    """
    enqueued_at_value = enqueued_at or datetime.now(timezone.utc).isoformat()
    files = [
        ActiveDownloadFileState(
            username=f.username,
            filename=f.filename,
            file_dir=f.file_dir,
            size=f.size,
            disk_no=f.disk_no,
            disk_count=f.disk_count,
            retry_count=f.retry or 0,
            bytes_transferred=f.bytes_transferred or 0,
            last_state=f.last_state,
        )
        for f in entry.files
    ]
    return ActiveDownloadState(
        filetype=entry.filetype,
        enqueued_at=enqueued_at_value,
        last_progress_at=last_progress_at or enqueued_at_value,
        files=files,
        processing_started_at=processing_started_at,
    )


# === Transfer ID re-derivation ===

def match_transfer_id(
    downloads: dict[str, Any] | list[dict[str, Any]],
    target_filename: str,
    username: str | None = None,
) -> str | None:
    """Find the slskd transfer ID for a filename in slskd download responses.

    downloads may be a single-user transfer dict or the list returned by
    slskd.transfers.get_all_downloads(). When a list is provided, username
    narrows the search to one peer.
    Returns the transfer ID string, or None if not found.
    """
    transfer = match_transfer(downloads, target_filename, username=username)
    if transfer is None:
        return None
    return transfer.get("id", "")


def _parse_transfer_timestamp(value: Any) -> datetime:
    """Parse slskd transfer timestamps for ordering duplicate snapshots."""
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)

    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _transfer_priority(transfer: dict[str, Any]) -> tuple[int, int, datetime]:
    """Rank duplicate transfer snapshots for the same username+filename.

    Prefer active transfers over terminal ones. Among terminal snapshots,
    prefer successful completions over cancelled/errored attempts, and then
    pick the newest lifecycle timestamp.
    """
    state = str(transfer.get("state", ""))
    is_terminal = state.startswith("Completed,")
    is_success = state == "Completed, Succeeded"
    latest_ts = max(
        _parse_transfer_timestamp(transfer.get("endedAt")),
        _parse_transfer_timestamp(transfer.get("startedAt")),
        _parse_transfer_timestamp(transfer.get("enqueuedAt")),
        _parse_transfer_timestamp(transfer.get("requestedAt")),
    )
    return (0 if is_terminal else 1, 1 if is_success else 0, latest_ts)


def match_transfer(
    downloads: dict[str, Any] | list[dict[str, Any]],
    target_filename: str,
    username: str | None = None,
) -> dict[str, Any] | None:
    """Find the best slskd transfer snapshot for a username+filename pair."""
    groups = downloads if isinstance(downloads, list) else [downloads]
    candidates: list[dict[str, Any]] = []
    for group in groups:
        if username is not None and group.get("username") not in (None, "", username):
            continue
        for directory in group.get("directories", []):
            for slskd_file in directory.get("files", []):
                if slskd_file.get("filename") == target_filename:
                    candidates.append(slskd_file)

    if not candidates:
        return None
    return max(candidates, key=_transfer_priority)


def _get_all_downloads_snapshot(
    slskd_client: Any,
    *,
    purpose: str,
) -> list[dict[str, Any]] | None:
    """Fetch the full slskd download snapshot via the bulk endpoint.

    The username-scoped endpoint is unreliable for some valid peer names
    containing spaces/punctuation, so monitoring code uses the bulk list and
    matches locally instead.
    """
    try:
        downloads = slskd_client.transfers.get_all_downloads(includeRemoved=True)
    except Exception:
        logger.warning(f"Failed to get all downloads for {purpose}", exc_info=True)
        return None

    if not isinstance(downloads, list):
        logger.warning("Unexpected get_all_downloads() response type %s",
                       type(downloads).__name__)
        return None

    return downloads


def rederive_transfer_ids(
    entry: GrabListEntry,
    slskd_client: Any,
    *,
    snapshot: list[dict[str, Any]] | None = None,
) -> bool:
    """Re-derive slskd transfer IDs for all files in a GrabListEntry.

    Queries the slskd bulk download API once and matches by username+filename.
    Updates file.id in-place. Files whose transfers have vanished keep id="".
    When snapshot is provided, uses it instead of fetching from the API.
    """
    downloads = snapshot
    if downloads is None:
        downloads = _get_all_downloads_snapshot(
            slskd_client,
            purpose=f"transfer re-derivation for {entry.artist} - {entry.title}",
        )
    if downloads is None:
        return False

    for f in entry.files:
        transfer = match_transfer(downloads, f.filename, username=f.username)
        if transfer is not None:
            f.id = transfer.get("id", "")
            state = str(transfer.get("state", ""))
            if state.startswith("Completed,"):
                f.status = dict(transfer)
            else:
                f.status = None
        else:
            logger.debug(f"Transfer not found for {f.filename} from {f.username}")
    return True


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
            retry=f.retry_count,
            bytes_transferred=f.bytes_transferred,
            last_state=f.last_state,
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
    apply_transition(db, request_id, "wanted",
                     from_status="downloading",
                     attempt_type="download")


def _persist_updated_download_state(
    db: Any,
    request_id: int,
    entry: GrabListEntry,
    state: ActiveDownloadState,
) -> None:
    """Persist retry counters or processing markers back to JSONB."""
    db.update_download_state(
        request_id,
        build_active_download_state(
            entry,
            enqueued_at=state.enqueued_at,
            last_progress_at=state.last_progress_at,
            processing_started_at=state.processing_started_at,
        ).to_json(),
    )


_NON_PROGRESS_STATES = {
    "",
    "Queued, Remotely",
    "Completed, Cancelled",
    "Completed, TimedOut",
    "Completed, Errored",
    "Completed, Rejected",
    "Completed, Aborted",
}


def _capture_download_progress(
    downloads: list[DownloadFile],
    state: ActiveDownloadState,
    now: datetime,
) -> bool:
    """Record byte/state progress from fresh slskd status snapshots.

    Returns True when any file made observable forward progress this cycle.
    """
    progress_made = False
    for file in downloads:
        if not file.status:
            continue

        current_state = str(file.status.get("state", ""))
        current_bytes = int(file.status.get("bytesTransferred") or 0)
        previous_bytes = file.bytes_transferred or 0
        previous_state = file.last_state or ""

        if current_bytes > previous_bytes:
            progress_made = True
        elif current_state != previous_state and current_state not in _NON_PROGRESS_STATES:
            progress_made = True

        file.bytes_transferred = current_bytes
        file.last_state = current_state or file.last_state

    if progress_made:
        state.last_progress_at = now.isoformat()

    return progress_made


def _run_completed_processing(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: Any,
    ctx: SoularrContext,
) -> None:
    """Run or resume local post-download processing for a completed album."""
    if state.processing_started_at is None:
        state.processing_started_at = datetime.now(timezone.utc).isoformat()
        _persist_updated_download_state(db, request_id, entry, state)

    try:
        success = process_completed_album(entry, [], ctx)
    except Exception:
        logger.exception(f"Error processing completed download {entry.artist} - {entry.title} "
                         f"— will retry local processing next cycle")
        return

    refreshed = db.get_request(request_id)
    if refreshed and refreshed["status"] == "downloading":
        if success:
            logger.info(f"  process_completed_album succeeded without "
                        f"setting status — setting imported")
            apply_transition(db, request_id, "imported",
                             from_status="downloading")
        else:
            logger.warning(f"  process_completed_album failed without "
                           f"setting status — resetting to wanted")
            apply_transition(db, request_id, "wanted",
                             from_status="downloading",
                             attempt_type="download")


def poll_active_downloads(ctx: SoularrContext) -> None:
    """Poll slskd for status of all downloading albums.

    For each album with status='downloading':
    1. Reconstruct GrabListEntry from DB + ActiveDownloadState
    2. Re-derive slskd transfer IDs
    3. Mark files with vanished transfers as errored (synthetic status)
    4. Poll file status for remaining files
    5. If all complete → process_completed_album()
    6. If timeout exceeded → cancel, log, reset to wanted
    7. If errors → retry individual files (persisted, max 5 retries per file)
    """
    db = ctx.pipeline_db_source._get_db()
    downloading = db.get_downloading()

    if not downloading:
        return

    logger.info(f"Polling {len(downloading)} active download(s)...")

    # One bulk snapshot for the entire poll cycle — avoids per-file API calls
    cycle_snapshot = _get_all_downloads_snapshot(
        ctx.slskd, purpose="poll cycle snapshot")
    if cycle_snapshot is None:
        logger.warning("Failed to get download snapshot — skipping poll cycle")
        return

    for row in downloading:
        request_id = row["id"]
        raw_state = row.get("active_download_state")
        if not raw_state:
            # Crash recovery: downloading with no state means process_completed_album
            # crashed on a previous run. Reset to wanted so it gets re-searched.
            logger.error(f"Downloading album {request_id} has no active_download_state — "
                         f"resetting to wanted")
            apply_transition(db, request_id, "wanted",
                             from_status="downloading")
            continue

        # psycopg2 returns JSONB as dict, not string — use from_dict directly
        if isinstance(raw_state, dict):
            state = ActiveDownloadState.from_dict(raw_state)
        else:
            state = ActiveDownloadState.from_json(raw_state)
        entry = reconstruct_grab_list_entry(row, state)

        if state.processing_started_at is not None:
            _run_completed_processing(entry, request_id, state, db, ctx)
            continue

        # Re-derive transfer IDs from pre-fetched snapshot
        if not rederive_transfer_ids(entry, ctx.slskd, snapshot=cycle_snapshot):
            logger.warning(f"API error re-deriving transfers for {entry.artist} - {entry.title} "
                           f"— will retry next cycle")
            continue

        # Check if all transfers have vanished (slskd restart, user offline)
        all_vanished = all(f.id == "" for f in entry.files)
        if all_vanished:
            _timeout_album(entry, request_id, "all transfers vanished from slskd", ctx)
            continue

        # Mark files with vanished transfers as errored
        for f in entry.files:
            if f.id == "":
                f.status = {"state": "Completed, Errored"}

        # Track total album age separately from stall/progress timing.
        enqueued_at = datetime.fromisoformat(state.enqueued_at)
        now = datetime.now(timezone.utc)
        elapsed_seconds = (now - enqueued_at).total_seconds()

        # Poll live status only for transfers that are still active in slskd.
        files_requiring_status = [
            f for f in entry.files
            if f.id and not (f.status and str(f.status.get("state", "")).startswith("Completed,"))
        ]
        if files_requiring_status and not slskd_download_status(
                files_requiring_status, ctx, snapshot=cycle_snapshot):
            logger.warning(f"API error polling {entry.artist} - {entry.title} — "
                          f"will retry next cycle")
            continue

        album_done, problems, queued = downloads_all_done(entry.files)
        statusful_files = [f for f in entry.files if f.status is not None]
        state_changed = _capture_download_progress(statusful_files, state, now)

        all_remote_queued = _all_files_remotely_queued(entry.files, queued)
        error_filenames = [f.filename for f in problems] if problems is not None else None
        file_retries = {f.filename: (f.retry or 0) for f in entry.files}

        progress_at = state.last_progress_at or state.enqueued_at
        idle_seconds = (now - datetime.fromisoformat(progress_at)).total_seconds()

        verdict = decide_download_action(
            album_done=album_done,
            error_filenames=error_filenames,
            total_files=len(entry.files),
            all_remote_queued=all_remote_queued,
            elapsed_seconds=elapsed_seconds,
            idle_seconds=idle_seconds,
            remote_queue_timeout=ctx.cfg.remote_queue_timeout,
            stalled_timeout=ctx.cfg.stalled_timeout,
            file_retries=file_retries,
            max_file_retries=MAX_FILE_RETRIES,
            processing_started=False,
        )

        if verdict.decision == DownloadDecision.timeout_remote_queue:
            _timeout_album(entry, request_id, verdict.reason, ctx)
            continue

        if verdict.decision == DownloadDecision.complete:
            logger.info(f"Download complete: {entry.artist} - {entry.title}")
            _run_completed_processing(entry, request_id, state, db, ctx)
            continue

        if verdict.decision == DownloadDecision.timeout_all_errored:
            _timeout_album(entry, request_id, verdict.reason, ctx)
            continue

        if verdict.decision == DownloadDecision.timeout_stalled:
            _timeout_album(entry, request_id, verdict.reason, ctx)
            continue

        if verdict.decision == DownloadDecision.retry_files:
            for retry_filename in verdict.files_to_retry:
                for df in entry.files:
                    if df.filename == retry_filename:
                        retries_used = (df.retry or 0) + 1
                        df.retry = retries_used
                        logger.info(f"Re-enqueue failed file "
                                    f"({retries_used}/{MAX_FILE_RETRIES} retries): "
                                    f"{retry_filename}")
                        # Find the problem file for username/size/dir
                        file = next((f for f in entry.files if f.filename == retry_filename), None)
                        if file:
                            requeue = slskd_do_enqueue(
                                file.username,
                                [{"filename": file.filename, "size": file.size}],
                                file.file_dir, ctx)
                            state_changed = True
                            if requeue:
                                df.id = requeue[0].id
                                df.bytes_transferred = 0
                                df.last_state = None
                                state.last_progress_at = now.isoformat()
                            else:
                                logger.warning(f"Failed to re-enqueue file: {retry_filename}")
                        break

            refreshed = db.get_request(request_id)
            if refreshed and refreshed["status"] != "downloading":
                continue

        # In progress — persist state and log
        refreshed = db.get_request(request_id)
        if refreshed and refreshed["status"] != "downloading":
            continue
        if state_changed:
            _persist_updated_download_state(db, request_id, entry, state)

        # Still in progress — log and continue to next album
        files_done = sum(1 for f in entry.files
                        if f.status and f.status.get("state") == "Completed, Succeeded")
        logger.info(f"In progress: {entry.artist} - {entry.title} "
                    f"({files_done}/{len(entry.files)} files, "
                    f"{elapsed_seconds/60:.1f}min elapsed)")


# === Top-level orchestration ===

def grab_most_wanted(albums: list[Any],
                     search_and_queue: Callable[..., tuple[dict, list, list]],
                     ctx: SoularrContext) -> int:
    """Search, enqueue, persist download state, return immediately.

    Does NOT block waiting for downloads. Download monitoring happens
    in poll_active_downloads() on subsequent runs.
    """
    grab_list, failed_search, failed_grab = search_and_queue(albums)

    total_albums = len(grab_list)
    logger.info(f"Total Downloads added: {total_albums}")
    for album_id in grab_list:
        entry = grab_list[album_id]
        logger.info(f"Album: {entry.title} Artist: {entry.artist}")

        # Persist download state to DB
        request_id = entry.db_request_id
        if request_id:
            state = build_active_download_state(entry)
            db = ctx.pipeline_db_source._get_db()
            apply_transition(db, request_id, "downloading",
                             from_status="wanted",
                             state_json=state.to_json())
            logger.info(f"  Set status=downloading, {len(entry.files)} files tracked")

    logger.info(f"Failed to grab: {len(failed_grab)}")
    for album in failed_grab:
        logger.info(f"Album: {album.title} Artist: {album.artist_name}")

    count = len(failed_search) + len(failed_grab)
    for album in failed_search:
        logger.info(f"Search failed for Album: {album.title} - Artist: {album.artist_name}")
    for album in failed_grab:
        logger.info(f"Download failed for Album: {album.title} - Artist: {album.artist_name}")

    return count
