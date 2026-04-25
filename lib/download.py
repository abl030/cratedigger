"""Download processing — monitoring, completion, and orchestration.

Extracted from cratedigger.py. All functions receive a CratediggerContext
instead of reading module-level globals.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import time
from datetime import datetime, timezone
from typing import Any, Callable, TYPE_CHECKING

import music_tag

from lib.download_recovery import (
    classify_processing_path,
    reconcile_processing_current_path,
)
from lib.grab_list import GrabListEntry, DownloadFile
from lib.processing_paths import (
    canonical_processing_path,
    directory_has_entries,
    normalize_processing_path,
    path_is_within_root,
    stage_to_ai_path,
    stage_to_ai_root,
)
from lib.quality import (ActiveDownloadState, ActiveDownloadFileState,
                         DownloadDecision, ValidationResult,
                         decide_download_action,
                         compute_effective_override_bitrate,
                         extract_usernames,
                         rejection_backfill_override)
from lib import transitions
from lib.import_dispatch import (DispatchOutcome, _build_download_info,
                                 _record_rejection_and_maybe_requeue,
                                 dispatch_import_core)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    automation_import_dedupe_key,
    automation_import_payload,
)
from lib.staged_album import StagedAlbum
from lib.util import move_failed_import, log_validation_result

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")
MAX_FILE_RETRIES = 5
AUTO_TRIAGE_EXCLUDED_REJECTION_SCENARIOS: frozenset[str] = frozenset({
    "audio_corrupt",
    "spectral_reject",
})


# Lazy import for spectral analysis — avoids hard dep on sox at import time
def spectral_analyze(folder: str, trim_seconds: int = 30) -> Any:
    """Proxy to spectral_check.analyze_album (lazy import)."""
    from lib.spectral_check import analyze_album
    return analyze_album(folder, trim_seconds=trim_seconds)


def _run_post_rejection_wrong_match_triage(
    ctx: "CratediggerContext",
    download_log_id: object,
    *,
    scenario: str | None,
) -> Any:
    """Immediately triage newly-created Wrong Matches rows.

    This runs after the rejected download_log row exists and only for the
    review-queue scenarios that Wrong Matches exposes. Bad-file scenarios have
    their own buckets and should not be deleted through wrong-match policy.
    """
    if not isinstance(download_log_id, int) or isinstance(download_log_id, bool):
        return None
    if scenario in AUTO_TRIAGE_EXCLUDED_REJECTION_SCENARIOS:
        return None
    if ctx.pipeline_db_source is None:
        return None
    get_db = getattr(ctx.pipeline_db_source, "_get_db", None)
    if get_db is None:
        return None
    try:
        from lib.wrong_match_triage import triage_wrong_match

        result = triage_wrong_match(get_db(), download_log_id)
        logger.info(
            "WRONG-MATCH TRIAGE: download_log_id=%s action=%s verdict=%s reason=%s",
            download_log_id,
            getattr(result, "action", None),
            getattr(getattr(result, "preview", None), "verdict", None),
            getattr(result, "reason", None),
        )
        return result
    except Exception:
        logger.exception(
            "WRONG-MATCH TRIAGE FAILED: download_log_id=%s",
            download_log_id,
        )
        return None


# === slskd on-disk path resolution ===
#
# Most installs save a remote path like
#   @@user\Share\Artist\Album\CD1\17 - Track.mp3
# to
#   {download_root}/CD1/17 - Track.mp3
# but production has seen two backwards-compat variants:
# - files still under ``{download_root}/incomplete/...``
# - on-disk names that differ only by filename casing from the remote path
# We therefore resolve in layers: fast leaf-folder lookup first, then
# case-insensitive / collision-suffix matches, then bounded recursive
# searches rooted in likely ancestor folders.

_TICKS_SUFFIX = re.compile(r"^(?P<base>.+)_(?P<ticks>\d{17,20})$")
_REMOTE_PATH_SEPARATORS = re.compile(r"[\\/]+")
_REQUEST_SCOPED_STAGE_SUFFIX = re.compile(r" \[request-\d+\]$")


def _remote_path_components(path: str) -> list[str]:
    """Split a Soulseek remote path into non-empty components."""
    return [part for part in _REMOTE_PATH_SEPARATORS.split(path) if part]


def slskd_local_folder(file_dir: str, slskd_download_dir: str) -> str:
    """Return the on-disk folder slskd places this file_dir's downloads into."""
    components = _remote_path_components(file_dir)
    leaf = components[-1] if components else file_dir
    return os.path.join(slskd_download_dir, leaf)


def _is_request_scoped_auto_import_path(
    *,
    current_path: str,
    staging_dir: str,
) -> bool:
    """Return True when ``current_path`` is under auto-import request staging."""
    normalized_path = normalize_processing_path(current_path)
    if not _REQUEST_SCOPED_STAGE_SUFFIX.search(os.path.basename(normalized_path)):
        return False
    return path_is_within_root(
        normalized_path,
        stage_to_ai_root(staging_dir=staging_dir, auto_import=True),
    )


def _matching_slskd_paths(
    search_root: str,
    *,
    expected_name: str,
    max_depth: int,
) -> list[str]:
    """Return files under ``search_root`` matching the expected basename.

    Matches both exact basenames (case-insensitive) and slskd's
    ``_<ticks>`` collision suffix. ``max_depth`` is relative to
    ``search_root``: 0 = only the root dir, 1 = one level below, etc.
    """
    if not os.path.isdir(search_root):
        return []

    expected_fold = expected_name.casefold()
    expected_base, expected_ext = os.path.splitext(expected_name)
    expected_base_fold = expected_base.casefold()
    expected_ext_fold = expected_ext.casefold()
    matches: list[str] = []

    for dirpath, dirnames, filenames in os.walk(search_root):
        rel = os.path.relpath(dirpath, search_root)
        depth = 0 if rel == "." else rel.count(os.sep) + 1
        if depth > max_depth:
            dirnames[:] = []
            continue
        if depth == max_depth:
            dirnames[:] = []

        for filename in filenames:
            basename_fold = filename.casefold()
            if basename_fold == expected_fold:
                matches.append(os.path.join(dirpath, filename))
                continue

            stem, ext = os.path.splitext(filename)
            if ext.casefold() != expected_ext_fold:
                continue
            suffix = _TICKS_SUFFIX.fullmatch(stem)
            if suffix is None:
                continue
            if suffix.group("base").casefold() == expected_base_fold:
                matches.append(os.path.join(dirpath, filename))

    return matches


def _choose_slskd_match(
    matches: list[str],
    *,
    expected_name: str,
    expected_folder: str,
    file_size: int | None,
    context: str,
) -> str:
    """Pick the best on-disk file from a candidate list and log why."""
    if len(matches) == 1:
        chosen = matches[0]
        logger.info(
            f"slskd file resolved via {context}: {expected_name} → "
            f"{os.path.relpath(chosen, expected_folder)}")
        return chosen

    if file_size:
        size_matches = [c for c in matches if _safe_getsize(c) == file_size]
        if len(size_matches) == 1:
            chosen = size_matches[0]
            logger.info(
                f"slskd file resolved by size via {context}: {expected_name} → "
                f"{os.path.relpath(chosen, expected_folder)} "
                f"(size={file_size}, {len(matches)} candidates)")
            return chosen

    exact_case_matches = [c for c in matches if os.path.basename(c) == expected_name]
    if len(exact_case_matches) == 1:
        chosen = exact_case_matches[0]
        logger.info(
            f"slskd file resolved by exact basename via {context}: "
            f"{expected_name} → {os.path.relpath(chosen, expected_folder)}")
        return chosen

    casefold_matches = [
        c for c in matches
        if os.path.basename(c).casefold() == expected_name.casefold()
    ]
    if len(casefold_matches) == 1:
        chosen = casefold_matches[0]
        logger.info(
            f"slskd file resolved by case-insensitive basename via {context}: "
            f"{expected_name} → {os.path.relpath(chosen, expected_folder)}")
        return chosen

    chosen = sorted(matches)[0]
    logger.warning(
        f"AMBIGUOUS slskd path resolution for {expected_name} via {context}: "
        f"{len(matches)} candidates, picked "
        f"{os.path.relpath(chosen, expected_folder)} deterministically. "
        f"Target size={file_size}, candidates="
        f"{[(os.path.relpath(c, expected_folder), _safe_getsize(c)) for c in matches]}")
    return chosen


def _slskd_search_roots(
    file_dir: str,
    slskd_download_dir: str,
) -> list[tuple[str, int, str]]:
    """Ordered search roots for resolving one downloaded file on disk.

    Intentionally stops at the album/ancestor folders derived from the
    Soulseek remote path. A whole-download-root scan can silently pick an
    unrelated sibling album that happens to contain the same basename.
    """
    components = _remote_path_components(file_dir)
    incomplete_root = os.path.join(slskd_download_dir, "incomplete")
    plans: list[tuple[str, int, str]] = []
    seen: set[tuple[str, int]] = set()

    def add(root: str, max_depth: int, label: str) -> None:
        key = (root, max_depth)
        if key in seen:
            return
        seen.add(key)
        plans.append((root, max_depth, label))

    leaf = components[-1] if components else ""
    if leaf:
        add(os.path.join(slskd_download_dir, leaf), 0, "leaf folder")
        add(os.path.join(incomplete_root, leaf), 0, "incomplete leaf folder")

    for width in range(2, min(len(components), 3) + 1):
        suffix = components[-width:]
        joined = os.path.join(*suffix)
        add(os.path.join(slskd_download_dir, joined), 1, "leaf suffix")
        add(os.path.join(incomplete_root, joined), 1, "incomplete leaf suffix")

    for component in reversed(components[-3:]):
        add(os.path.join(slskd_download_dir, component), 2, "ancestor folder")
        add(os.path.join(incomplete_root, component), 2, "incomplete ancestor folder")
    return plans


def resolve_slskd_local_path(file: "DownloadFile",
                             slskd_download_dir: str) -> str | None:
    """Resolve the actual on-disk path of a downloaded slskd file.

    Returns the full path, or ``None`` if the file cannot be located.
    Tries the exact expected path first, then falls back to matching the
    ``_<ticks>`` collision-rename variants. When multiple collision variants
    exist, prefers the one whose byte size matches ``file.size``, else
    picks deterministically and logs a warning so ambiguous cases are
    visible in journald. The fallback search is restricted to path-derived
    album folders; it never walks the whole slskd root because basename-only
    matches there can cross album boundaries.
    """
    filename_components = _remote_path_components(file.filename)
    expected_name = filename_components[-1] if filename_components else file.filename
    expected_folder = slskd_local_folder(file.file_dir, slskd_download_dir)

    for root, max_depth, label in _slskd_search_roots(file.file_dir, slskd_download_dir):
        matches = _matching_slskd_paths(
            root, expected_name=expected_name, max_depth=max_depth)
        if not matches:
            continue
        return _choose_slskd_match(
            matches,
            expected_name=expected_name,
            expected_folder=slskd_download_dir,
            file_size=file.size,
            context=label,
        )

    # Keep the old fast-path fallback shape in the logs: callers still build
    # ``src_folder/expected_name`` when this returns None.
    logger.debug(
        f"slskd local path not found for {expected_name}; expected under "
        f"{expected_folder} or compatible legacy layouts")
    return None


def _safe_getsize(path: str) -> int | None:
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def _canonical_import_folder_path(
    album_data: GrabListEntry,
    slskd_download_dir: str,
) -> str:
    return canonical_processing_path(
        artist=album_data.artist,
        title=album_data.title,
        year=album_data.year,
        slskd_download_dir=slskd_download_dir,
    )


# === slskd transfer helpers ===

def cancel_and_delete(files: list[Any], ctx: CratediggerContext) -> None:
    """Cancel downloads and remove their directories."""
    for file in files:
        if not file.id:
            continue  # Transfer vanished or never assigned — skip cancel
        try:
            ctx.slskd.transfers.cancel_download(username=file.username, id=file.id)
        except Exception:
            logger.warning(f"Failed to cancel download {file.filename} for {file.username}",
                           exc_info=True)
        delete_dir = slskd_local_folder(file.file_dir, ctx.cfg.slskd_download_dir)
        if os.path.isdir(delete_dir):
            shutil.rmtree(delete_dir)


def slskd_download_status(downloads: list[Any], ctx: CratediggerContext,
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
                     file_dir: str, ctx: CratediggerContext) -> list[DownloadFile] | None:
    """Enqueue files for download via slskd. Returns DownloadFile list or None."""
    try:
        enqueue = ctx.slskd.transfers.enqueue(username=username, files=files)
    except Exception:
        logger.debug("Enqueue failed", exc_info=True)
        return None
    if not enqueue:
        return None

    # Poll for transfer IDs — slskd needs time to register the enqueue.
    # Typically resolves in 1-2s; max 10s before giving up.
    max_wait = 10.0
    interval = 1.0
    elapsed = 0.0
    download_list: list[dict[str, Any]] | None = None

    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        download_list = _get_all_downloads_snapshot(
            ctx.slskd,
            purpose=f"download status for {username} after enqueue",
        )
        if download_list is None:
            continue
        if all(
            match_transfer_id(download_list, f["filename"], username=username)
            is not None
            for f in files
        ):
            break

    if download_list is None:
        return None

    downloads: list[DownloadFile] = []
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




# === Download completion processing ===
def _log_post_move_resume_blocked(
    album_data: GrabListEntry,
    *,
    current_path: str,
    detail: str,
) -> None:
    logger.error(
        "POST-MOVE RESUME BLOCKED: request_id=%s %s - %s "
        "current_path=%s %s See docs/advisory-locks.md.",
        album_data.db_request_id,
        album_data.artist,
        album_data.title,
        current_path,
        detail,
    )


def _materialize_processing_dir(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
) -> bool | None:
    """Ensure ``staged_album.current_path`` holds the album's local files."""
    canonical_path = _canonical_import_folder_path(
        album_data, ctx.cfg.slskd_download_dir)
    db = (ctx.pipeline_db_source._get_db()
          if ctx.pipeline_db_source is not None else None)
    request_id = album_data.db_request_id
    if request_id is None and _is_request_scoped_auto_import_path(
        current_path=staged_album.current_path,
        staging_dir=ctx.cfg.beets_staging_dir,
    ):
        _log_post_move_resume_blocked(
            album_data,
            current_path=staged_album.current_path,
            detail=(
                "already lives at the request-scoped auto-import staged "
                "path but is missing db_request_id. Automatic retry is "
                "disabled because import ownership can no longer be "
                "verified; manual recovery is required."
            ),
        )
        return None
    current_path_location = classify_processing_path(
        current_path=staged_album.current_path,
        artist=album_data.artist,
        title=album_data.title,
        year=album_data.year,
        request_id=request_id or 0,
        staging_dir=ctx.cfg.beets_staging_dir,
        slskd_download_dir=ctx.cfg.slskd_download_dir,
    )

    if current_path_location.kind != "canonical":
        if not os.path.isdir(staged_album.current_path):
            if current_path_location.blocks_post_move_retry:
                _log_post_move_resume_blocked(
                    album_data,
                    current_path=staged_album.current_path,
                    detail=(
                        "already lives at the request-scoped auto-import "
                        "staged path but the directory is missing. "
                        "Automatic retry is disabled because beets may "
                        "already have consumed the staged folder; manual "
                        "recovery is required."
                    ),
                )
                return None
            logger.error(f"Current staged path missing: {staged_album.current_path}")
            return False
        staged_album.bind_import_paths(album_data.files)
        missing_paths: list[str] = []
        for file in album_data.files:
            import_path = file.import_path
            assert import_path is not None
            if not os.path.isfile(import_path):
                missing_paths.append(import_path)
        if missing_paths:
            if current_path_location.blocks_post_move_retry:
                _log_post_move_resume_blocked(
                    album_data,
                    current_path=staged_album.current_path,
                    detail=(
                        "already lives at the request-scoped auto-import "
                        f"staged path but tracked files are missing ({', '.join(missing_paths)}). "
                        "Automatic retry is disabled because import may "
                        "already have started; manual recovery is required."
                    ),
                )
                return None
            logger.error(
                "Current staged path is missing tracked files: %s",
                ", ".join(missing_paths),
            )
            return False
        if current_path_location.blocks_auto_import_dispatch:
            detail = (
                "already lives at the request-scoped auto-import staged "
                "path. Automatic retry is disabled to avoid duplicate "
                "import; manual recovery is required."
            )
            if current_path_location.kind == "legacy_shared_staged":
                detail = (
                    "already lives at the legacy shared staged path. "
                    "Automatic retry is disabled because the path is "
                    "ambiguous across editions; manual recovery is required."
                )
            _log_post_move_resume_blocked(
                album_data,
                current_path=staged_album.current_path,
                detail=detail,
            )
            return None
        album_data.import_folder = staged_album.current_path
        return True

    rm_dirs: list[str] = []
    moved_files_history: list[tuple[str, str]] = []
    if os.path.exists(canonical_path):
        logger.info(f"Staging folder {canonical_path} already exists — "
                    f"resuming or reusing prior attempt")
    else:
        os.mkdir(canonical_path)

    for file in album_data.files:
        # Destination filename keeps the remote basename (no ticks suffix,
        # even if slskd appended one on the source).
        filename_components = _remote_path_components(file.filename)
        filename = filename_components[-1] if filename_components else file.filename
        expected_folder = slskd_local_folder(file.file_dir, ctx.cfg.slskd_download_dir)
        resolved_src = resolve_slskd_local_path(file, ctx.cfg.slskd_download_dir)
        src_file = resolved_src if resolved_src is not None \
            else os.path.join(expected_folder, filename)
        src_folder = os.path.dirname(src_file)
        if src_folder not in rm_dirs:
            rm_dirs.append(src_folder)
        dst_file = staged_album.import_path_for(file)
        file.import_path = dst_file
        if os.path.exists(dst_file) and not os.path.exists(src_file):
            # Resume safely after a crash that already moved this file.
            logger.info(f"Already-moved file detected: {dst_file} (src gone, skipping)")
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
                os.rmdir(canonical_path)
            except OSError:
                logger.warning(f"Could not remove temp import directory {canonical_path}")
            return False

    for rm_dir in rm_dirs:
        if os.path.abspath(rm_dir) == os.path.abspath(canonical_path):
            continue
        try:
            os.rmdir(rm_dir)
        except OSError:
            logger.warning(f"Skipping removal of {rm_dir} because it's not empty.")

    album_data.import_folder = staged_album.current_path
    staged_album.persist_current_path(db)
    return True


def process_completed_album(album_data: GrabListEntry, failed_grab: list[Any],
                            ctx: CratediggerContext) -> "bool | None":
    """Process a fully-downloaded album: move files, tag, validate, stage/import.

    Returns three-valued ``bool | None``:
    - ``True`` — local non-dispatch processing succeeded. Outer caller may
      finalize only if the request row is still ``downloading``.
    - ``False`` — local non-dispatch processing failed. Outer caller resets
      to ``wanted`` only if the request row is still ``downloading``.
    - ``None`` — the validation / dispatch path either already owned the
      request transition or intentionally left state untouched for retry /
      manual recovery. Outer caller must NOT touch status.
    """
    staged_album = StagedAlbum.from_entry(
        album_data,
        default_path=_canonical_import_folder_path(
            album_data, ctx.cfg.slskd_download_dir),
    )
    materialized = _materialize_processing_dir(album_data, staged_album, ctx)
    if materialized is not True:
        return materialized

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
        outcome = _process_beets_validation(
            album_data, staged_album, ctx)
        if outcome is not None:
            if outcome.deferred:
                # Release-lock contention. Propagate ``None`` so
                # ``_run_completed_processing`` leaves the request's
                # status, active_download_state, and staged files
                # untouched for the next cycle to retry.
                return None
            # DispatchOutcome is an import summary only. The dispatch path
            # must perform any request transition itself through
            # lib.transitions; do not let a summary bool mask a missing
            # typed transition in the outer poller.
            return None
    return True


def _process_beets_validation(album_data: GrabListEntry,
                              staged_album: StagedAlbum,
                              ctx: CratediggerContext) -> "DispatchOutcome | None":
    """Beets validation sub-path of process_completed_album.

    After beets validation passes, delegates to ``lib.preimport.run_preimport_gates``
    for the shared audio + spectral gates. The force/manual-import path
    (``dispatch_import_from_db``) calls the same function — only the beets
    distance check is path-specific.

    Returns the dispatch outcome when the auto-import path fires,
    ``None`` when beets validation rejects (``_handle_rejected_result``
    already handles the state transition) or when the non-auto
    redownload path takes over in ``_handle_valid_result``. Guarded
    ownership-less rejects also return a deferred outcome so callers
    keep the row untouched for manual recovery.
    """
    from lib.beets import beets_validate as _bv
    from lib.preimport import run_preimport_gates
    current_path = staged_album.current_path
    bv_result = _bv(ctx.cfg.beets_harness_path, current_path,
                    album_data.mb_release_id, ctx.cfg.beets_distance_threshold)
    usernames_pre = set(f.username for f in album_data.files if f.username)
    bv_result.soulseek_username = ", ".join(sorted(usernames_pre)) if usernames_pre else None
    bv_result.download_folder = current_path

    if bv_result.valid:
        dl_pre = _build_download_info(album_data)
        db = (ctx.pipeline_db_source._get_db()
              if ctx.pipeline_db_source is not None else None)
        preimport = run_preimport_gates(
            path=current_path,
            mb_release_id=album_data.mb_release_id or "",
            label=f"{album_data.artist} - {album_data.title}",
            download_filetype=dl_pre.filetype or "",
            download_min_bitrate_bps=dl_pre.bitrate,
            download_is_vbr=dl_pre.is_vbr,
            cfg=ctx.cfg,
            db=db,
            request_id=album_data.db_request_id,
            usernames=usernames_pre,
        )
        album_data.download_spectral = preimport.download_spectral
        album_data.current_spectral = preimport.existing_spectral
        album_data.current_min_bitrate = preimport.existing_min_bitrate
        if not preimport.valid:
            bv_result.valid = False
            bv_result.scenario = preimport.scenario
            bv_result.detail = preimport.detail
            if preimport.corrupt_files:
                bv_result.corrupt_files = preimport.corrupt_files

    if bv_result.valid:
        return _handle_valid_result(
            album_data, bv_result, staged_album, ctx)
    _handle_rejected_result(
        album_data, bv_result, staged_album, ctx)
    return None


def _resolved_request_rejection_id(
    album_data: GrabListEntry,
    ctx: CratediggerContext,
) -> tuple[Any | None, int | None]:
    """Resolve the backing request row for defensive auto-import rejects."""
    if ctx.pipeline_db_source is None:
        return None, None
    db = ctx.pipeline_db_source._get_db()
    if album_data.db_request_id is not None:
        return db, album_data.db_request_id

    candidate_request_id = album_data.album_id
    if not isinstance(candidate_request_id, int) or isinstance(candidate_request_id, bool):
        return db, None
    # ``AlbumRecord.id`` is negative on the search path, so only positive
    # ids can safely be treated as ``album_requests.id`` candidates here.
    if candidate_request_id <= 0:
        return db, None

    request_row = db.get_request(candidate_request_id)
    if not isinstance(request_row, dict):
        return db, None
    if str(request_row.get("artist_name") or "") != album_data.artist:
        return db, None
    if str(request_row.get("album_title") or "") != album_data.title:
        return db, None
    request_year = request_row.get("year")
    if (
        album_data.year
        and request_year not in (None, "")
        and str(request_year) != album_data.year
    ):
        return db, None
    album_release_id = str(album_data.mb_release_id or "")
    request_release_id = str(request_row.get("mb_release_id") or "")
    if bool(album_release_id) != bool(request_release_id):
        return db, None
    if album_release_id and request_release_id != album_release_id:
        return db, None
    return db, candidate_request_id


def _reject_request_auto_import(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    detail: str,
    scenario: str,
    error: str,
) -> DispatchOutcome:
    """Reject a request auto-import when ownership can be proven safely."""
    db, request_id = _resolved_request_rejection_id(album_data, ctx)
    if db is None or request_id is None:
        logger.error(
            "AUTO-IMPORT REJECT BLOCKED WITHOUT REQUEST AUDIT: album_id=%s %s - %s "
            "(scenario=%s) could not resolve a safe pipeline request row; "
            "files remain at %s and automatic retry/import is disabled until "
            "manual recovery.",
            album_data.album_id,
            album_data.artist,
            album_data.title,
            scenario,
            staged_album.current_path,
        )
        return DispatchOutcome(
            success=False,
            message=detail,
            deferred=True,
        )

    failed_result = ValidationResult(
        distance=bv_result.distance if bv_result.distance is not None else 0.0,
        scenario=scenario,
        detail=detail,
        error=error,
    )
    failed_result.failed_path = move_failed_import(
        staged_album.current_path,
        scenario=failed_result.scenario,
    )
    logger.error(
        "AUTO-IMPORT REJECTED: %s - %s — %s",
        album_data.artist,
        album_data.title,
        detail,
    )
    log_validation_result(album_data, failed_result, ctx.cfg)

    dl_info = _build_download_info(album_data)
    if album_data.download_spectral is not None:
        dl_info.download_spectral = album_data.download_spectral
        dl_info.current_spectral = album_data.current_spectral
        dl_info.existing_min_bitrate = album_data.current_min_bitrate
        dl_info.slskd_filetype = dl_info.filetype
        dl_info.actual_filetype = dl_info.filetype
    download_log_id = _record_rejection_and_maybe_requeue(
        db,
        request_id,
        dl_info,
        distance=failed_result.distance if failed_result.distance is not None else 0.0,
        scenario=failed_result.scenario or scenario,
        detail=detail,
        error=failed_result.error,
        requeue=True,
        validation_result=failed_result.to_json(),
    )
    _run_post_rejection_wrong_match_triage(
        ctx,
        download_log_id,
        scenario=failed_result.scenario,
    )

    return DispatchOutcome(success=False, message=detail)


def _handle_valid_result(album_data: GrabListEntry, bv_result: ValidationResult,
                         staged_album: StagedAlbum,
                         ctx: CratediggerContext) -> "DispatchOutcome | None":
    """Handle a valid beets validation result: stage and optionally auto-import.

    Returns the ``DispatchOutcome`` summary from ``dispatch_import_core``
    when the auto-import path fires (source='request', distance within
    threshold), or ``None`` for the redownload path that just stages
    and marks done. ``process_completed_album()`` only uses the summary
    to detect deferred no-op cases; request-state changes remain owned
    by the dispatch/finalization seam itself.

    This function acquires the RELEASE advisory lock outer for the
    auto-import path *before* ``StagedAlbum.move_to`` runs, so
    contention is a true no-op: files stay at their current local
    processing path, ``active_download_state.current_path`` stays
    unchanged, and the next cycle can idempotently re-enter without
    any extra filesystem churn. Redownload paths don't take the lock
    — they just move into staging and mark done, so no cross-process
    race applies.

    See ``docs/advisory-locks.md`` for namespaces, keys, ordering,
    and contention behaviour (including the staged-move rationale for
    acquiring at this level rather than inside
    ``dispatch_import_core``).
    """
    from contextlib import nullcontext
    from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                 release_id_to_lock_key)

    source_type = album_data.db_source or "redownload"
    request_id = album_data.db_request_id
    dist = bv_result.distance if bv_result.distance is not None else 1.0
    wants_auto_import = (
        source_type == "request"
        and dist <= ctx.cfg.beets_distance_threshold)

    if wants_auto_import and request_id is None:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail=(
                "Request auto-import is missing db_request_id; automatic "
                "resume/import is disabled."
            ),
            scenario="request_missing_request_id",
            error="missing_request_id",
        )

    current_path_location = classify_processing_path(
        current_path=staged_album.current_path,
        artist=album_data.artist,
        title=album_data.title,
        year=album_data.year,
        request_id=request_id or 0,
        staging_dir=ctx.cfg.beets_staging_dir,
        slskd_download_dir=ctx.cfg.slskd_download_dir,
    )

    if wants_auto_import and not album_data.mb_release_id:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail="Request auto-import requires a MusicBrainz release ID",
            scenario="request_missing_mbid",
            error="missing_mbid",
        )

    will_auto_import = wants_auto_import
    pdb = None

    if (
        will_auto_import
        and current_path_location.blocks_auto_import_dispatch
    ):
        _log_post_move_resume_blocked(
            album_data,
            current_path=staged_album.current_path,
            detail=(
                f"already lives at the {current_path_location.display_name}. "
                "Automatic retry is disabled to avoid duplicate import; "
                "manual recovery is required."
            ),
        )
        return DispatchOutcome(
            success=False,
            message=(
                "Auto-import may already have started for this staged "
                f"album ({album_data.mb_release_id})"
            ),
            deferred=True,
        )

    if will_auto_import and album_data.mb_release_id:
        pdb = ctx.pipeline_db_source._get_db()
        lock_ctx = pdb.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(album_data.mb_release_id))
    else:
        lock_ctx = nullcontext(True)

    with lock_ctx as got_release_lock:
        if not got_release_lock:
            logger.warning(
                f"AUTO-IMPORT DEFERRED: {album_data.artist} - "
                f"{album_data.title} — release lock held by another "
                f"process (mbid={album_data.mb_release_id}); skipping "
                "staged move and dispatch. Files stay at "
                f"{staged_album.current_path} so the next cycle can "
                "idempotently resume from process_completed_album.")
            return DispatchOutcome(
                success=False,
                message=("Another import is already in progress for "
                         f"this release ({album_data.mb_release_id})"),
                deferred=True,
            )

        db = (ctx.pipeline_db_source._get_db()
              if ctx.pipeline_db_source is not None else None)
        dest = staged_album.move_to(
            stage_to_ai_path(
                artist=album_data.artist,
                title=album_data.title,
                staging_dir=ctx.cfg.beets_staging_dir,
                request_id=request_id,
                auto_import=will_auto_import,
            ),
            db=db,
        )
        album_data.import_folder = dest
        log_validation_result(album_data, bv_result, ctx.cfg, dest_path=dest)
        logger.info(f"STAGED: {album_data.artist} - {album_data.title} "
                    f"(scenario={bv_result.scenario}, "
                    f"distance={bv_result.distance:.4f}) → {dest}")

        dl_info = _build_download_info(album_data)
        dl_info.validation_result = bv_result.to_json()
        if album_data.download_spectral is not None:
            dl_info.download_spectral = album_data.download_spectral
            dl_info.current_spectral = album_data.current_spectral
            dl_info.existing_min_bitrate = album_data.current_min_bitrate
            dl_info.slskd_filetype = dl_info.filetype
            dl_info.actual_filetype = dl_info.filetype
        if will_auto_import:
            assert request_id is not None, "pipeline request must have db_request_id"
            assert pdb is not None, "auto-import path must hold a pipeline DB handle"
            override_min_bitrate: int | None = None
            try:
                req = pdb.get_request(request_id)
                if req:
                    override_min_bitrate = compute_effective_override_bitrate(
                        req.get("min_bitrate"),
                        req.get("current_spectral_bitrate"),
                        req.get("current_spectral_grade"),
                    )
            except Exception:
                logger.debug("DB lookup failed for override-min-bitrate")

            return dispatch_import_core(
                path=dest,
                mb_release_id=album_data.mb_release_id or "",
                request_id=request_id,
                label=f"{album_data.artist} - {album_data.title}",
                override_min_bitrate=override_min_bitrate,
                target_format=album_data.db_target_format,
                verified_lossless_target=ctx.cfg.verified_lossless_target,
                beets_harness_path=ctx.cfg.beets_harness_path,
                db=pdb,
                dl_info=dl_info,
                distance=bv_result.distance if bv_result.distance is not None else 0.0,
                scenario=bv_result.scenario or "auto_import",
                files=album_data.files,
                cfg=ctx.cfg,
                requeue_on_failure=True,
                cooled_down_users=ctx.cooled_down_users,
            )
        ctx.pipeline_db_source.mark_done(
            album_data, bv_result, dest_path=dest, download_info=dl_info)
        return None


def _handle_rejected_result(album_data: GrabListEntry, bv_result: ValidationResult,
                            staged_album: StagedAlbum,
                            ctx: CratediggerContext) -> None:
    """Handle a rejected beets validation result."""
    failed_dest = move_failed_import(
        staged_album.current_path,
        scenario=bv_result.scenario,
    )
    bv_result.failed_path = failed_dest
    log_validation_result(album_data, bv_result, ctx.cfg)
    usernames = set(f.username for f in album_data.files)
    bv_result.denylisted_users = sorted(usernames)
    dl_info = _build_download_info(album_data)
    dl_info.validation_result = bv_result.to_json()
    if album_data.download_spectral is not None:
        dl_info.download_spectral = album_data.download_spectral
        dl_info.current_spectral = album_data.current_spectral
        dl_info.existing_min_bitrate = album_data.current_min_bitrate
        dl_info.slskd_filetype = dl_info.filetype
        dl_info.actual_filetype = dl_info.filetype

    # Backfill search_filetype_override for pre-quality-gate albums stuck in loops
    backfill_override = _compute_rejection_backfill(album_data, ctx)

    download_log_id = ctx.pipeline_db_source.reject_and_requeue(
        album_data,
        bv_result,
        usernames=usernames,
        download_info=dl_info,
        search_filetype_override=backfill_override,
        cooled_down_users=ctx.cooled_down_users,
    )
    _run_post_rejection_wrong_match_triage(
        ctx,
        download_log_id,
        scenario=bv_result.scenario,
    )
    logger.warning(f"REJECTED: {album_data.artist} - {album_data.title} "
                   f"(scenario={bv_result.scenario}, "
                   f"distance={bv_result.distance}, "
                   f"detail={bv_result.detail}) "
                   f"| denylisted users: {', '.join(usernames)}")


def _compute_rejection_backfill(album_data: GrabListEntry,
                                ctx: CratediggerContext) -> str | None:
    """Check if search_filetype_override should be backfilled on rejection.

    Only fires when search_filetype_override is currently NULL and the on-disk state
    is genuine + decent quality + not verified lossless.
    """
    request_id = album_data.db_request_id
    if not request_id or not ctx.pipeline_db_source:
        return None
    if album_data.db_search_filetype_override:
        return None
    try:
        db = ctx.pipeline_db_source._get_db()
        req = db.get_request(request_id)
        if not req or req.get("search_filetype_override"):
            return None
        from lib.beets_db import BeetsDB
        with BeetsDB() as beets:
            info = beets.get_album_info(
                album_data.mb_release_id, ctx.cfg.quality_ranks)
        if not info:
            return None
        override = rejection_backfill_override(
            is_cbr=info.is_cbr,
            min_bitrate_kbps=info.min_bitrate_kbps,
            spectral_grade=req.get("current_spectral_grade"),
            verified_lossless=bool(req.get("verified_lossless")),
            cfg=ctx.cfg.quality_ranks,
        )
        if override:
            logger.info(
                f"BACKFILL: {album_data.artist} - {album_data.title} "
                f"search_filetype_override=NULL → '{override}' "
                f"(on-disk: {info.min_bitrate_kbps}kbps, cbr={info.is_cbr}, "
                f"spectral={req.get('current_spectral_grade')})")
        return override
    except Exception:
        logger.debug("BACKFILL: failed to check on-disk state", exc_info=True)
        return None


# === ActiveDownloadState building ===

def build_active_download_state(
    entry: GrabListEntry,
    *,
    enqueued_at: str | None = None,
    last_progress_at: str | None = None,
    processing_started_at: str | None = None,
    current_path: str | None = None,
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
        current_path=(
            current_path
            if current_path is not None
            else entry.import_folder
        ),
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
        db_search_filetype_override=request.get("search_filetype_override"),
        db_target_format=request.get("target_format"),
        import_folder=state.current_path,
    )


# === Async download polling ===

def _timeout_album(
    entry: GrabListEntry,
    request_id: int,
    reason: str,
    ctx: CratediggerContext,
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
    for username in extract_usernames(entry.files):
        if db.check_and_apply_cooldown(username):
            ctx.cooled_down_users.add(username)
    transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
        ),
    )


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
            current_path=entry.import_folder,
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
    ctx: CratediggerContext,
) -> bool | None:
    """Run or resume local post-download processing for a completed album."""
    if state.processing_started_at is None:
        if entry.import_folder is None:
            entry.import_folder = _canonical_import_folder_path(
                entry,
                ctx.cfg.slskd_download_dir,
            )
        state.processing_started_at = datetime.now(timezone.utc).isoformat()
        _persist_updated_download_state(db, request_id, entry, state)

    try:
        outcome = process_completed_album(entry, [], ctx)
    except Exception:
        logger.exception(f"Error processing completed download {entry.artist} - {entry.title} "
                         f"— will retry local processing next cycle")
        return None

    # Three-valued return from ``process_completed_album``:
    # - True  → processing succeeded; flip to 'imported' if status is
    #   still 'downloading'.
    # - False → a non-deferred failure path returned; reset to
    #   'wanted' only if the request row is still 'downloading'.
    # - None  → leave the row untouched. This covers release-lock
    #   contention, guarded post-move staged paths, and ownership-less
    #   request rejects that require manual recovery. Do NOT touch state
    #   here.
    if outcome is None:
        return None

    refreshed = db.get_request(request_id)
    if refreshed and refreshed["status"] == "downloading":
        if outcome:
            logger.info(f"  process_completed_album succeeded without "
                        f"setting status — setting imported")
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_imported(
                    from_status="downloading",
                ),
            )
        else:
            logger.warning(f"  process_completed_album failed without "
                           f"setting status — resetting to wanted")
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                    attempt_type="download",
                ),
            )
    return outcome


def _active_automation_import_job(db: Any, request_id: int):
    return db.get_import_job_by_dedupe_key(
        automation_import_dedupe_key(request_id),
        active_only=True,
    )


def _enqueue_completed_processing(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: Any,
    ctx: CratediggerContext,
) -> Any:
    """Submit completed-download processing to the shared import queue."""
    if state.processing_started_at is None:
        if entry.import_folder is None:
            entry.import_folder = _canonical_import_folder_path(
                entry,
                ctx.cfg.slskd_download_dir,
            )
        state.processing_started_at = datetime.now(timezone.utc).isoformat()
        _persist_updated_download_state(db, request_id, entry, state)
    job = db.enqueue_import_job(
        IMPORT_JOB_AUTOMATION,
        request_id=request_id,
        dedupe_key=automation_import_dedupe_key(request_id),
        payload=automation_import_payload(),
        message=f"Automation import queued for {entry.artist} - {entry.title}",
        # Automation downloads are not yet safe for the async preview lane:
        # the importer owns materializing slskd files into the stable
        # processing folder. Previewing first checks a path that may not exist
        # yet and terminal-fails valid completed downloads as path_missing.
        preview_enabled=False,
    )
    if getattr(job, "deduped", False):
        logger.info(
            "Automation import already queued/running for request %s "
            "(job %s)",
            request_id,
            getattr(job, "id", "?"),
        )
    else:
        logger.info(
            "Queued automation import for request %s as job %s",
            request_id,
            getattr(job, "id", "?"),
        )
    return job


def _processing_path_ready_for_importer(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: Any,
    ctx: CratediggerContext,
) -> bool:
    """Fail closed before enqueueing a job that cannot resume local files."""
    if state.processing_started_at is None or state.current_path is None:
        return True

    current_path_location = classify_processing_path(
        current_path=state.current_path,
        artist=entry.artist,
        title=entry.title,
        year=entry.year,
        request_id=request_id,
        staging_dir=ctx.cfg.beets_staging_dir,
        slskd_download_dir=ctx.cfg.slskd_download_dir,
    )
    if not os.path.isdir(state.current_path):
        # The canonical processing folder may not exist yet. The importer
        # materializes it from the completed slskd files as its first step.
        if current_path_location.kind == "canonical":
            return True
        if current_path_location.blocks_post_move_retry:
            _log_post_move_resume_blocked(
                entry,
                current_path=state.current_path,
                detail=(
                    "already lives at the request-scoped auto-import "
                    "staged path but the directory is missing. "
                    "Automatic retry is disabled because beets may "
                    "already have consumed the staged folder; manual "
                    "recovery is required."
                ),
            )
            return False
        logger.error("Current staged path missing: %s", state.current_path)
        transitions.finalize_request(
            db,
            request_id,
            transitions.RequestTransition.to_wanted(
                from_status="downloading",
                attempt_type="download",
            ),
        )
        return False

    staged_album = StagedAlbum.from_entry(entry, default_path=state.current_path)
    staged_album.bind_import_paths(entry.files)
    missing_paths: list[str] = []
    for file in entry.files:
        import_path = file.import_path
        if import_path is not None and not os.path.isfile(import_path):
            missing_paths.append(import_path)
    if not missing_paths:
        return True

    if current_path_location.blocks_post_move_retry:
        _log_post_move_resume_blocked(
            entry,
            current_path=state.current_path,
            detail=(
                "already lives at the request-scoped auto-import "
                f"staged path but tracked files are missing ({', '.join(missing_paths)}). "
                "Automatic retry is disabled because import may "
                "already have started; manual recovery is required."
            ),
        )
        return False

    logger.error(
        "Current staged path is missing tracked files: %s",
        ", ".join(missing_paths),
    )
    transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
        ),
    )
    return False


def poll_active_downloads(ctx: CratediggerContext) -> None:
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
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                ),
            )
            continue

        # psycopg2 returns JSONB as dict, not string — use from_dict directly
        if isinstance(raw_state, dict):
            state = ActiveDownloadState.from_dict(raw_state)
        else:
            state = ActiveDownloadState.from_json(raw_state)
        active_import_job = _active_automation_import_job(db, request_id)
        if active_import_job is not None:
            logger.info(
                "Request %s is waiting on importer job %s (%s)",
                request_id,
                getattr(active_import_job, "id", "?"),
                getattr(active_import_job, "status", "?"),
            )
            continue
        if state.processing_started_at is not None:
            recovery_decision = reconcile_processing_current_path(
                current_path=state.current_path,
                artist=row["artist_name"],
                title=row["album_title"],
                year=str(row["year"] or ""),
                request_id=request_id,
                staging_dir=ctx.cfg.beets_staging_dir,
                slskd_download_dir=ctx.cfg.slskd_download_dir,
                has_entries=directory_has_entries,
            )
            if recovery_decision.blocked_reason == "multiple_populated_paths":
                rendered_candidates = ", ".join(
                    f"{location.short_label}={location.path}"
                    for location in recovery_decision.populated_locations
                )
                logger.error(
                    "MID-PROCESS RESUME BLOCKED: request_id=%s %s - %s "
                    "found multiple populated recovery paths (%s). "
                    "Manual recovery is required.",
                    request_id,
                    row["artist_name"],
                    row["album_title"],
                    rendered_candidates,
                )
                continue
            if recovery_decision.blocked_reason == "legacy_shared_only":
                logger.error(
                    "LEGACY STAGED RESUME BLOCKED: request_id=%s %s - %s "
                    "persisted current_path=%s could not be resumed, "
                    "canonical_path=%s has no files, "
                    "and staged_path=%s is ambiguous across editions. "
                    "Manual recovery is required.",
                    request_id,
                    row["artist_name"],
                    row["album_title"],
                    state.current_path,
                    recovery_decision.canonical_path,
                    recovery_decision.legacy_shared_path,
                )
                continue
            assert recovery_decision.selected_location is not None
            selected_path = recovery_decision.selected_location.path
            if selected_path != state.current_path:
                state.current_path = selected_path
                db.update_download_state_current_path(
                    request_id,
                    state.current_path,
                )
        entry = reconstruct_grab_list_entry(row, state)

        if state.processing_started_at is not None:
            if not _processing_path_ready_for_importer(
                entry,
                request_id,
                state,
                db,
                ctx,
            ):
                continue
            _enqueue_completed_processing(entry, request_id, state, db, ctx)
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
            _enqueue_completed_processing(entry, request_id, state, db, ctx)
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
                     ctx: CratediggerContext) -> int:
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
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_downloading(
                    from_status="wanted",
                    state_json=state.to_json(),
                ),
            )
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
