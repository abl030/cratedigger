"""Orphan/inconsistency detection + repair suggestions.

Originally extracted verbatim from the monolithic ``lib/quality.py`` (issue
#477), which parked it under ``lib/quality/`` even though it isn't
quality-decision logic; refreshed to also carry ``SlskdOrphanTransfer`` /
``find_slskd_orphans`` (issue #278, the inverse orphan direction — live
slskd transfers no downloading row owns); relocated to this top-level
module (issue #512). Pure moves both times: every definition here is
AST-identical to its prior incarnation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # DownloadUser is only used for the find_slskd_orphans() type
    # annotation, never constructed here — deferred so this module doesn't
    # need lib.slskd_client at runtime import time.
    from lib.slskd_client import DownloadUser


# --- Repair / orphan detection (pure functions) ---

@dataclass(frozen=True)
class OrphanInfo:
    """A detected inconsistency in pipeline DB state."""
    request_id: int
    issue_type: str  # corrupt_downloading | orphaned_download | blocked_post_move | blocked_recovery | auto_abandon_import
    detail: str


@dataclass(frozen=True)
class RepairAction:
    """Suggested repair for a detected inconsistency."""
    request_id: int
    action: str  # "reset_to_wanted", "manual_review"
    detail: str


def find_orphaned_downloads(
    db_rows: list[dict[str, Any]],
    active_transfers: set[tuple[str, str]],
    *,
    existing_local_paths: set[str] | None,
) -> list[OrphanInfo]:
    """Detect downloading rows whose slskd transfers no longer exist. Pure — no I/O.

    Args:
        db_rows: album_requests rows (must include status, active_download_state).
        active_transfers: set of (username, filename) tuples from slskd API.
        existing_local_paths: set of persisted ``current_path`` values that
            still exist on disk, supplied by the caller when local filesystem
            visibility is available. Pass ``None`` when the caller cannot
            inspect local processing paths.

    Returns OrphanInfo for each downloading row where NONE of its files
    appear in active_transfers, plus ``blocked_post_move`` when a row is
    already in local processing but its persisted ``current_path`` is gone.
    Rows with ``processing_started_at`` set are treated as local-processing
    rows even when ``current_path`` is missing; caller-side blocked recovery
    detection owns that ambiguity.
    """
    issues: list[OrphanInfo] = []
    for row in db_rows:
        if row["status"] != "downloading":
            continue
        state = row.get("active_download_state")
        if not state:
            continue  # corrupt_downloading — handled by find_inconsistencies
        files = state.get("files", [])
        if not files:
            continue
        has_active = any(
            (f.get("username"), f.get("filename")) in active_transfers
            for f in files
        )
        current_path = state.get("current_path")
        if state.get("processing_started_at") is not None:
            if (
                current_path
                and not has_active
                and existing_local_paths is not None
                and current_path not in existing_local_paths
            ):
                issues.append(OrphanInfo(
                    request_id=row["id"],
                    issue_type="blocked_post_move",
                    detail=(
                        "persisted processing path missing after local "
                        "processing: "
                        f"{current_path}"
                    ),
                ))
            # Local processing continues after slskd has finished, so
            # transferless rows in this phase are not ordinary orphans.
            continue
        if not has_active:
            usernames = sorted(set(f.get("username", "?") for f in files))
            issues.append(OrphanInfo(
                request_id=row["id"],
                issue_type="orphaned_download",
                detail=f"no active slskd transfers (users: {', '.join(usernames)})"))
    return issues


@dataclass(frozen=True)
class SlskdOrphanTransfer:
    """A live slskd transfer that no downloading request owns (#278)."""
    username: str
    transfer_id: str
    filename: str
    state: str


def find_slskd_orphans(
    downloads: list[DownloadUser],
    db_rows: list[dict[str, Any]],
) -> list[SlskdOrphanTransfer]:
    """Detect live slskd transfers no downloading row owns. Pure — no I/O.

    The inverse direction of ``find_orphaned_downloads``: operator actions
    (Replace being the canonical one) abandon a downloading request's
    ``active_download_state`` without cancelling its in-flight slskd
    transfers, leaving each transfer to complete into the slskd download
    dir with no owner.

    Args:
        downloads: slskd ``transfers.get_all_downloads()`` snapshot
            (username → directories → files groups), already decoded via
            ``lib.slskd_client.parse_downloads_envelope`` (issue #507).
        db_rows: album_requests rows (must include status,
            active_download_state).

    Ownership is strictly ``status='downloading'`` rows — a replaced row's
    frozen ``active_download_state`` must NOT shield its stranded
    transfers, since reaping those is the point of this convergence.
    Transfers in a terminal state (``Completed, *``) are skipped: there is
    nothing to cancel, and ``remove_completed_downloads()`` already reaps
    their UI entries at end of cycle. A missing state is treated as live
    so an unowned transfer never dodges convergence by omitting it.
    """
    owned: set[tuple[str, str]] = set()
    for row in db_rows:
        if row["status"] != "downloading":
            continue
        state = row.get("active_download_state")
        if not state:
            continue
        for f in state.get("files", []):
            owned.add((f.get("username"), f.get("filename")))

    orphans: list[SlskdOrphanTransfer] = []
    for user_group in downloads:
        username = user_group.username
        for directory in user_group.directories:
            for transfer in directory.files:
                transfer_state = transfer.state
                if transfer_state.startswith("Completed"):
                    continue
                filename = transfer.filename
                if not filename:
                    continue
                if (username, filename) in owned:
                    continue
                orphans.append(SlskdOrphanTransfer(
                    username=username,
                    transfer_id=transfer.id,
                    filename=filename,
                    state=transfer_state,
                ))
    return orphans


def find_inconsistencies(db_rows: list[dict[str, Any]]) -> list[OrphanInfo]:
    """Detect inconsistent rows in album_requests. Pure — no I/O.

    Checks:
    - downloading row with no active_download_state (corrupt crash recovery)

    ``imported_path`` is NOT checked against status: it means "files are on
    disk at this path" and survives a status=wanted re-queue (transcode
    upgrade, quality-gate upgrade search). Clearing it on status=wanted
    would wipe the correct beets destination for any album the pipeline is
    actively searching for a better version of. See issue #93.
    """
    issues: list[OrphanInfo] = []
    for row in db_rows:
        rid = row["id"]
        status = row["status"]
        state = row.get("active_download_state")

        if status == "downloading" and not state:
            issues.append(OrphanInfo(
                request_id=rid,
                issue_type="corrupt_downloading",
                detail="downloading with no active_download_state"))

    return issues


def suggest_repair(issue: OrphanInfo) -> RepairAction:
    """Suggest a repair action for a detected inconsistency. Pure."""
    if issue.issue_type in ("corrupt_downloading", "orphaned_download"):
        return RepairAction(
            request_id=issue.request_id,
            action="reset_to_wanted",
            detail="Reset downloading row to wanted (transfers gone)")
    if issue.issue_type in ("blocked_post_move", "blocked_recovery"):
        return RepairAction(
            request_id=issue.request_id,
            action="manual_review",
            detail="Inspect blocked local-processing row and finish or reset it explicitly",
        )
    if issue.issue_type == "auto_abandon_import":
        return RepairAction(
            request_id=issue.request_id,
            action="wait_for_automatic_recovery",
            detail=(
                "Poller/importer will quarantine the interrupted "
                "auto-import and reset it to wanted"
            ),
        )
    else:
        return RepairAction(
            request_id=issue.request_id,
            action="manual_review",
            detail=f"Unknown issue type: {issue.issue_type}")
