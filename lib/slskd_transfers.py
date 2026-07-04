"""slskd transfer-level helpers — enqueue, status, cancel, re-derivation.

Split out of lib/download.py (issue #146 phase 3). Everything here
talks to slskd's transfers API or reasons about its snapshots; pipeline
polling lives in lib/download.py and staging/completion processing
in lib/download_processing.py.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, TYPE_CHECKING

from lib.grab_list import DownloadFile, GrabListEntry
from lib.processing_paths import path_is_within_root
from lib.slskd_client import parse_transfer_snapshot

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


# === slskd transfer helpers ===


@dataclass(frozen=True)
class SlskdEnqueueOutcome:
    """Structured result for one slskd enqueue request."""

    status: Literal["accepted", "rejected", "unknown"]
    downloads: list[DownloadFile] | None = None

def cancel_and_delete(files: list[Any], ctx: CratediggerContext) -> bool:
    """Cancel downloads and delete their completed payloads.

    Deletion targets come from slskd's own answers only (issue #146
    phase 3): the ingested ``local_path`` stamp, topped up by a fresh
    events-page lookup for files that completed after this cycle's
    ingest pass. Files are removed individually and their directories
    pruned only when empty — never a recursive delete of an inferred
    folder, which nuked unrelated albums sharing a generic leaf name
    (the ``CD1/`` hazard). In-flight partials are slskd's to clean up.

    Returns whether every requested transfer had an ID and cancel call
    completed. Callers that only need best-effort cleanup can ignore it.
    """
    ok = True
    for file in files:
        if not file.id:
            ok = False
            continue  # Transfer vanished or never assigned — skip cancel
        try:
            if not ctx.slskd.transfers.cancel_download(
                username=file.username,
                id=file.id,
            ):
                ok = False
        except Exception:
            ok = False
            logger.warning(f"Failed to cancel download {file.filename} for {file.username}",
                           exc_info=True)
    _delete_completed_payloads(files, ctx)
    return ok


def _delete_completed_payloads(
    files: list[Any],
    ctx: CratediggerContext,
) -> None:
    """Remove completed files at their authoritative local paths."""
    from lib.slskd_events import recent_completion_paths

    root = ctx.cfg.slskd_download_dir
    needs_lookup = any(not getattr(file, "local_path", None) for file in files)
    recent = (
        recent_completion_paths(ctx.slskd)
        if needs_lookup
        else None
    )

    prune_dirs: list[str] = []
    for file in files:
        local_path = file.local_path or (
            recent.files.get((file.username, file.filename))
            if recent is not None
            else None
        )
        if not local_path:
            continue
        if not path_is_within_root(local_path, root):
            logger.warning(
                "Refusing to delete %s for %s — outside the slskd "
                "download root", local_path, file.filename)
            continue
        try:
            os.remove(local_path)
        except FileNotFoundError:
            pass
        except OSError:
            logger.warning("Failed to delete %s", local_path, exc_info=True)
            continue
        parent = os.path.dirname(local_path)
        if parent not in prune_dirs:
            prune_dirs.append(parent)

    if recent is not None:
        for file in files:
            local_dir = recent.directories.get((file.username, file.file_dir))
            if local_dir and local_dir not in prune_dirs:
                prune_dirs.append(local_dir)

    for prune_dir in prune_dirs:
        if not path_is_within_root(prune_dir, root):
            continue
        if os.path.realpath(prune_dir) == os.path.realpath(root):
            continue
        try:
            os.rmdir(prune_dir)
        except OSError:
            pass  # Non-empty (shared with another album) or already gone.


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
                    file.status = parse_transfer_snapshot(transfer)
                    if file.status is None:
                        ok = False
                else:
                    file.status = None
                    ok = False
            else:
                file.status = ctx.slskd.transfers.get_download(file.username, file.id)
        except Exception:
            logger.exception(f"Error getting download status of {file.filename}")
            file.status = None
            ok = False
    return ok


def _is_user_offline_http_error(exc: BaseException) -> bool:
    """slskd surfaces a peer-offline rejection as an HTTPError whose
    response body contains 'appears to be offline' (verified against
    Soulseek.UserOfflineException — see the 2026-05-08 pooyork incident).

    Match on body substring rather than status code: slskd has shipped 400 /
    500 / 504 across versions for this case, but the body string is durable.
    Detection is structural — any exception carrying a ``.response`` with
    a readable ``.text`` containing the marker counts. Avoiding
    ``isinstance(requests.exceptions.HTTPError)`` keeps the helper safe
    when ``sys.modules["requests"]`` is monkey-patched in tests.
    """
    response = getattr(exc, "response", None)
    if response is None:
        return False
    try:
        body = response.text or ""
    except Exception:
        return False
    if not isinstance(body, str):
        return False
    return "appears to be offline" in body.lower()


def slskd_enqueue_with_outcome(
    username: str,
    files: list[dict[str, Any]],
    file_dir: str,
    ctx: CratediggerContext,
) -> SlskdEnqueueOutcome:
    """Enqueue files for download via slskd with an explicit outcome."""
    try:
        enqueue = ctx.slskd.transfers.enqueue(username=username, files=files)
    except Exception as exc:
        if _is_user_offline_http_error(exc):
            logger.info(
                "slskd reports peer %s offline at enqueue; classifying as rejected",
                username,
            )
            return SlskdEnqueueOutcome(status="rejected")
        logger.debug("Enqueue failed", exc_info=True)
        return SlskdEnqueueOutcome(status="unknown")
    if not enqueue:
        return SlskdEnqueueOutcome(status="rejected")

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

    downloads: list[DownloadFile] = []
    for file in files:
        transfer_id = (
            match_transfer_id(
                download_list,
                file["filename"],
                username=username,
            )
            if download_list is not None
            else None
        )
        downloads.append(DownloadFile(
            filename=file["filename"],
            id=transfer_id or "",
            file_dir=file_dir,
            username=username,
            size=file["size"],
        ))

    reconciled = sum(1 for download in downloads if download.id)
    if reconciled != len(downloads):
        logger.warning(
            "slskd accepted enqueue for %s but only reconciled %s/%s "
            "transfer IDs; tracking filenames for next-cycle rederivation",
            username,
            reconciled,
            len(downloads),
        )
    return SlskdEnqueueOutcome(status="accepted", downloads=downloads)


def slskd_do_enqueue(username: str, files: list[dict[str, Any]],
                     file_dir: str, ctx: CratediggerContext) -> list[DownloadFile] | None:
    """Enqueue files for download via slskd. Returns DownloadFile list or None."""
    outcome = slskd_enqueue_with_outcome(username, files, file_dir, ctx)
    if outcome.status != "accepted":
        return None
    return outcome.downloads


def downloads_all_done(downloads: list[Any]) -> tuple[bool, list[Any] | None, int]:
    """Check status of all files. Returns (all_done, error_list_or_none, remote_queue_count)."""
    all_done = True
    error_list: list[Any] = []
    remote_queue = 0
    for file in downloads:
        if file.status is not None:
            state = file.status.state
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
            if state == "Queued, Remotely":
                remote_queue += 1
    return all_done, error_list if error_list else None, remote_queue


def _all_files_remotely_queued(downloads: list[Any], remote_queue_count: int) -> bool:
    """Return True when every tracked file is still queued on the remote peer.

    This is intentionally separate from stalled transfer detection: a file that has
    never started uploading should be governed by the remote queue timeout, not the
    no-progress timeout used for active transfers.
    """
    return bool(downloads) and remote_queue_count == len(downloads)





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


def _transfer_latest_timestamp(transfer: dict[str, Any]) -> datetime:
    return max(
        _parse_transfer_timestamp(transfer.get("endedAt")),
        _parse_transfer_timestamp(transfer.get("startedAt")),
        _parse_transfer_timestamp(transfer.get("enqueuedAt")),
        _parse_transfer_timestamp(transfer.get("requestedAt")),
    )


def _transfer_priority(transfer: dict[str, Any]) -> tuple[int, int, datetime]:
    """Rank duplicate transfer snapshots for the same username+filename.

    Prefer active transfers over terminal ones. Among terminal snapshots,
    prefer successful completions over cancelled/errored attempts, and then
    pick the newest lifecycle timestamp.
    """
    state = str(transfer.get("state", ""))
    is_terminal = state.startswith("Completed,")
    is_success = state == "Completed, Succeeded"
    latest_ts = _transfer_latest_timestamp(transfer)
    return (0 if is_terminal else 1, 1 if is_success else 0, latest_ts)


def _is_terminal_transfer_before(
    transfer: dict[str, Any],
    not_before: str | None,
) -> bool:
    if not_before is None:
        return False
    state = str(transfer.get("state", ""))
    if not state.startswith("Completed,"):
        return False
    threshold = _parse_transfer_timestamp(not_before)
    latest_ts = _transfer_latest_timestamp(transfer)
    if latest_ts == datetime.min.replace(tzinfo=timezone.utc):
        return False
    return latest_ts < threshold


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


def converge_slskd_orphans(ctx: CratediggerContext) -> int:
    """Phase 0 convergence (#278): cancel live slskd transfers no
    ``downloading`` row owns.

    Operator actions that supersede a downloading request (Replace is the
    canonical one — see CLAUDE.md invariant 7) deliberately leave its
    in-flight slskd transfers running; this is the convergence that reaps
    them. Runs once per cycle, before Phase 1/Phase 2 start, while nothing
    is enqueuing — so a live transfer without an owner is genuinely
    orphaned, not mid-write. The slskd snapshot is taken BEFORE the DB
    read: a transfer enqueued after the snapshot can't appear in it, so
    ordering alone rules out false orphans.

    Best-effort: a snapshot failure skips the pass, a cancel failure is
    logged and the remaining orphans are still attempted. Returns the
    number of transfers successfully cancelled.
    """
    from lib.quality import find_slskd_orphans

    downloads = _get_all_downloads_snapshot(
        ctx.slskd, purpose="orphan-transfer convergence")
    if downloads is None:
        return 0
    db = ctx.pipeline_db_source._get_db()
    orphans = find_slskd_orphans(downloads, db.get_downloading())
    cancelled = 0
    for orphan in orphans:
        try:
            ctx.slskd.transfers.cancel_download(
                orphan.username, orphan.transfer_id)
            cancelled += 1
            logger.warning(
                "SLSKD ORPHAN: cancelled unowned transfer "
                f"user={orphan.username!r} file={orphan.filename!r} "
                f"state={orphan.state!r} id={orphan.transfer_id}")
        except Exception:
            logger.exception(
                "SLSKD ORPHAN: failed to cancel unowned transfer "
                f"user={orphan.username!r} file={orphan.filename!r} "
                f"id={orphan.transfer_id} — will retry next cycle")
    if orphans:
        logger.info(
            f"SLSKD ORPHAN: convergence cancelled {cancelled}/{len(orphans)} "
            "unowned live transfer(s)")
    return cancelled


def rederive_transfer_ids(
    entry: GrabListEntry,
    slskd_client: Any,
    *,
    snapshot: list[dict[str, Any]] | None = None,
    not_before: str | None = None,
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
        if transfer is not None and _is_terminal_transfer_before(
            transfer,
            not_before,
        ):
            transfer = None
        if transfer is not None:
            f.id = transfer.get("id", "")
            state = str(transfer.get("state", ""))
            if state.startswith("Completed,"):
                f.status = parse_transfer_snapshot(transfer)
            else:
                f.status = None
        else:
            logger.debug(f"Transfer not found for {f.filename} from {f.username}")
    return True
