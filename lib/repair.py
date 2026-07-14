"""Orphan/inconsistency detection + repair suggestions.

Originally extracted verbatim from the monolithic ``lib/quality.py`` (issue
#477), which parked it under ``lib/quality/`` even though it isn't
quality-decision logic; refreshed to also carry ``SlskdOrphanTransfer`` /
``find_slskd_orphans`` (issue #278, the inverse orphan direction — live
slskd transfers no downloading row owns); relocated to this top-level
module (issue #512). ``find_slskd_orphans`` was flipped to positive
ownership (issue #571 PR 3): a live transfer is only ever reported as an
orphan when its queue key has a confirmed accepted enqueue — never merely
"unowned" by a downloading row, which used to risk cancelling a human's
transfer on a shared slskd instance. ``find_completed_transfers_to_purge``
applies the same doctrine to terminal transfers. slskd re-issues transfer
IDs when it retries a queued file, so the durable ownership identity is the
accepted ``(username, filename)`` queue key.
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
    """A live, ledger-owned slskd transfer with no owning ``downloading``
    row — a cratedigger-created stray (#278, ledger-positive since #571
    PR 3)."""
    username: str
    transfer_id: str
    filename: str
    state: str


@dataclass(frozen=True)
class SlskdTransferOwnership:
    """Full ownership classification of the live slskd transfer snapshot
    (#571 good-citizen flip, PR 3) — the single helper both
    ``converge_slskd_orphans`` and the read-only ``scripts/repair.py``
    report consume, so the two never classify foreign-vs-stray
    differently.

    ``orphans`` — ledger-owned strays (C2): cratedigger's own, ripe to
    cancel. ``foreign_count`` — live transfers absent from the ledger
    entirely (C1): never touched, counted here only so callers can
    report how many other-owned transfers share the instance.
    """
    orphans: list[SlskdOrphanTransfer]
    foreign_count: int


def find_slskd_orphans(
    downloads: list[DownloadUser],
    db_rows: list[dict[str, Any]],
    owned_keys: set[tuple[str, str]],
) -> SlskdTransferOwnership:
    """Classify live slskd transfers against ledger ownership (#571
    good-citizen flip). Pure — no I/O.

    AMENDMENT (PR #585 review, binding): an open ``slskd_transfer_ledger``
    row is NOT itself evidence a transfer is still live — retries mint a
    fresh row per (username, filename) and orphan rows stay open until
    prune. This function therefore starts from the LIVE slskd snapshot
    (``downloads``) and checks ledger membership for each entry; it never
    walks the ledger and asks "is this still in slskd".

    Args:
        downloads: slskd ``transfers.get_all_downloads()`` snapshot
            (username → directories → files groups), already decoded via
            ``lib.slskd_client.parse_downloads_envelope`` (issue #507).
        db_rows: album_requests rows (must include status,
            active_download_state).
        owned_keys: ``(username, filename)`` pairs with an accepted enqueue
            in ``slskd_transfer_ledger`` — pending intent is excluded
            (``lib.pipeline_db.transfer_ledger.get_owned_transfer_keys``).

    A live, non-terminal transfer classifies as exactly one of:

      * **FOREIGN (C1)** — ``(username, filename)`` absent from
        ``owned_keys``. Cratedigger never proved it created the transfer (a
        human sharing the
        slskd instance, most likely); it is NEVER reported as an orphan,
        whatever its state or age. Rolled into ``foreign_count`` only.
      * **owned STRAY (C2, returned in ``orphans``)** — confirmed, but NOT
        backed by any currently-``downloading`` row's
        ``active_download_state``. Cratedigger created it and no longer
        has a claim: the classic Replace-abandons-transfer case, and
        also a confirmed transfer whose row already self-healed back to
        ``wanted`` after a failed cancel attempt (still a stray — the
        ledger row, not the request's current status, proves creation).
      * **still in flight** — confirmed AND backed by a ``downloading``
        row. Reported nowhere; this is the common case every cycle.

    Ownership backing is strictly ``status='downloading'`` rows — a
    replaced row's frozen ``active_download_state`` must NOT shield its
    stranded transfers, since reaping those is the point of this
    convergence. Transfers in a terminal state (``Completed, *``) are
    skipped entirely: there is nothing to cancel, and the completed-
    transfer purge is its own convergence (#571 PR 5). A missing state
    string is treated as live so a stray never dodges convergence by
    omitting it.

    A ledger row that has since been pruned (``prune_transfer_ledger``,
    T3 — old AND its request inactive) is indistinguishable here from a
    transfer that was never ledgered: it becomes FOREIGN, never reaped by
    this convergence. Accepted: prune only fires well past any transfer's
    legitimate lifetime, and the safe direction of that miss is "leave it
    alone", never "delete a human's transfer".
    """
    backed: set[tuple[str, str]] = set()
    for row in db_rows:
        if row["status"] != "downloading":
            continue
        state = row.get("active_download_state")
        if not state:
            continue
        for f in state.get("files", []):
            backed.add((f.get("username"), f.get("filename")))

    orphans: list[SlskdOrphanTransfer] = []
    foreign_count = 0
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
                key = (username, filename)
                if key not in owned_keys:
                    foreign_count += 1
                    continue
                if key in backed:
                    continue
                orphans.append(SlskdOrphanTransfer(
                    username=username,
                    transfer_id=transfer.id,
                    filename=filename,
                    state=transfer_state,
                ))
    return SlskdTransferOwnership(orphans=orphans, foreign_count=foreign_count)


@dataclass(frozen=True)
class CompletedTransferToRemove:
    """One terminal transfer action from the pure classifier."""
    username: str
    transfer_id: str
    filename: str


@dataclass(frozen=True)
class CompletedTransferOwnership:
    """Terminal-transfer classification by stable queue ownership."""
    to_remove: list[CompletedTransferToRemove]
    foreign_count: int
    nonterminal_count: int


def find_completed_transfers_to_purge(
    downloads: list[DownloadUser],
    owned_keys: set[tuple[str, str]],
) -> CompletedTransferOwnership:
    """Classify the live slskd snapshot for terminal convergence. Pure.

    Every terminal attempt for a confirmed ``(username, filename)`` queue key
    is owned, including successor IDs that slskd creates while retrying the
    original enqueue. Unknown keys remain foreign and untouched. Nonterminal
    records are counted but never mutated. A terminal record without an ID
    cannot be addressed and is left alone.
    """
    to_remove: list[CompletedTransferToRemove] = []
    foreign_count = 0
    nonterminal_count = 0
    for user_group in downloads:
        username = user_group.username
        for directory in user_group.directories:
            for transfer in directory.files:
                if not transfer.state.startswith("Completed,"):
                    if transfer.id:
                        nonterminal_count += 1
                    continue
                transfer_id = transfer.id
                if not transfer_id:
                    continue
                if (username, transfer.filename) not in owned_keys:
                    foreign_count += 1
                    continue
                to_remove.append(CompletedTransferToRemove(
                    username=username,
                    transfer_id=transfer_id,
                    filename=transfer.filename,
                ))
    return CompletedTransferOwnership(
        to_remove=to_remove,
        foreign_count=foreign_count,
        nonterminal_count=nonterminal_count,
    )


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
