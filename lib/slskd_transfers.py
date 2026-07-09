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
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_processing_path,
    normalize_processing_path,
    path_is_within_root,
)
from lib.quality import ActiveDownloadState
from lib.slskd_client import DownloadUser, TransferSnapshot

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


# === slskd transfer helpers ===


@dataclass(frozen=True)
class SlskdEnqueueOutcome:
    """Structured result for one slskd enqueue request.

    ``reason`` (issue #564 C4) carries the real enqueue-failure cause
    when one was captured — a peer-offline classification, the raw
    slskd response body (e.g. a ``Soulseek.DownloadEnqueueException``
    message), or a generic HTTP-status/exception fallback. ``None`` when
    the outcome is ``"accepted"``, or when it's the falsy-return
    ``"rejected"`` branch that never raised (no exception to extract a
    reason from).
    """

    status: Literal["accepted", "rejected", "unknown"]
    downloads: list[DownloadFile] | None = None
    reason: str | None = None

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


def slskd_download_status(downloads: list[DownloadFile], *,
                          snapshot: list[DownloadUser]) -> bool:
    """Get status of each download file by matching locally against the
    pre-fetched bulk poll-cycle snapshot (issue #508: every caller already
    has one — there is no per-file API fallback)."""
    ok = True
    for file in downloads:
        try:
            transfer = match_transfer(snapshot, file.filename, username=file.username)
            if transfer is not None:
                file.status = transfer
            else:
                file.status = None
                ok = False
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


_ENQUEUE_FAILURE_REASON_MAX_LEN = 500
_ENQUEUE_FAILURE_REASON_TRUNCATE_LEN = 300


def _extract_enqueue_failure_reason(exc: BaseException) -> str | None:
    """Best-effort structural extraction of slskd's enqueue-failure body
    (issue #564 C4) — e.g. ``Soulseek.DownloadEnqueueException: File not
    shared.``, the shape behind 1,236 ``status="unknown"`` writes in 14
    days that previously discarded the real reason entirely.

    Returns ``None`` when there's no readable response body, the body
    looks like an HTML error page (starts with ``<``), or it's
    implausibly long to be a short exception message — callers fall
    back to a generic HTTP-status/exception message in that case.
    """
    response = getattr(exc, "response", None)
    if response is None:
        return None
    try:
        body = response.text
    except Exception:
        return None
    if not isinstance(body, str):
        return None
    body = body.strip()
    if not body or body.startswith("<"):
        return None
    if len(body) > _ENQUEUE_FAILURE_REASON_MAX_LEN:
        return None
    return body[:_ENQUEUE_FAILURE_REASON_TRUNCATE_LEN]


def _enqueue_failure_fallback_reason(exc: BaseException) -> str:
    """Generic reason when the response body isn't usable (issue #564 C4)."""
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code is not None:
        return f"slskd enqueue failed (HTTP {status_code})"
    return str(exc)


def _write_ahead_transfer_ledger(
    username: str,
    files: list[dict[str, Any]],
    ctx: CratediggerContext,
    *,
    request_id: int | None,
    attempt_fp: str | None,
) -> None:
    """Write-ahead ownership ledger insert (issue #571, T1) -- MUST run
    BEFORE ``ctx.slskd.transfers.enqueue(...)`` so a process death at any
    point after the POST still leaves a durable ownership record
    (migration 045).

    ``slskd_enqueue_with_outcome`` is the ONLY production call site of
    ``ctx.slskd.transfers.enqueue`` (``slskd_do_enqueue`` just wraps it),
    so putting the write-ahead insert here covers every enqueue call site
    in the pipeline -- the main single-disc path, multi-disc, and the
    poll-loop's single-file retry re-enqueue -- without hunting each one
    down individually.

    ``request_id``/``ctx.download_ownership`` are expected to be present
    on every production call (every enqueue is for an album_requests
    row, and the top-level context always carries the worker-safe
    ownership writer). Skips silently only in the legacy/test fallback
    shape ``grab_most_wanted`` already documents (no
    ``ctx.download_ownership`` collaborator wired) -- the SAME guard
    ``_claim_initial_download_ownership`` (lib/enqueue.py) already uses
    for the ownership claim itself, so this never diverges from whether
    a claim was even attempted.

    A DB failure here deliberately propagates (write-ahead: no POST
    before the ledger commits; DB-down is already cycle-fatal), matching
    the search ledger's ``record_search_id`` precedent.
    """
    writer = getattr(ctx, "download_ownership", None)
    if writer is None or request_id is None or not files:
        return
    from lib.pipeline_db import TransferLedgerRow

    rows = [
        TransferLedgerRow(
            request_id=request_id,
            username=username,
            filename=str(f["filename"]),
            attempt_fingerprint=attempt_fp,
        )
        for f in files
    ]
    writer.record_transfer_enqueue(rows)


def slskd_enqueue_with_outcome(
    username: str,
    files: list[dict[str, Any]],
    file_dir: str,
    ctx: CratediggerContext,
    *,
    request_id: int | None = None,
    attempt_fp: str | None = None,
) -> SlskdEnqueueOutcome:
    """Enqueue files for download via slskd with an explicit outcome."""
    _write_ahead_transfer_ledger(
        username, files, ctx, request_id=request_id, attempt_fp=attempt_fp)
    try:
        enqueue = ctx.slskd.transfers.enqueue(username=username, files=files)
    except Exception as exc:
        if _is_user_offline_http_error(exc):
            logger.info(
                "slskd reports peer %s offline at enqueue; classifying as rejected",
                username,
            )
            return SlskdEnqueueOutcome(
                status="rejected", reason="peer appears to be offline")
        reason = (
            _extract_enqueue_failure_reason(exc)
            or _enqueue_failure_fallback_reason(exc)
        )
        logger.debug("Enqueue failed: %s", reason, exc_info=True)
        return SlskdEnqueueOutcome(status="unknown", reason=reason)
    if not enqueue:
        return SlskdEnqueueOutcome(status="rejected")

    # Poll for transfer IDs — slskd needs time to register the enqueue.
    # Typically resolves in 1-2s; max 10s before giving up.
    max_wait = 10.0
    interval = 1.0
    elapsed = 0.0
    download_list: list[DownloadUser] | None = None

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
                     file_dir: str, ctx: CratediggerContext,
                     *, request_id: int | None = None,
                     attempt_fp: str | None = None) -> list[DownloadFile] | None:
    """Enqueue files for download via slskd. Returns DownloadFile list or None."""
    outcome = slskd_enqueue_with_outcome(
        username, files, file_dir, ctx,
        request_id=request_id, attempt_fp=attempt_fp)
    if outcome.status != "accepted":
        return None
    return outcome.downloads


def downloads_all_done(
    downloads: list[DownloadFile],
) -> tuple[bool, list[DownloadFile] | None, int]:
    """Check status of all files. Returns (all_done, error_list_or_none, remote_queue_count)."""
    all_done = True
    error_list: list[DownloadFile] = []
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


def _all_files_remotely_queued(
    downloads: list[DownloadFile], remote_queue_count: int,
) -> bool:
    """Return True when every tracked file is still queued on the remote peer.

    This is intentionally separate from stalled transfer detection: a file that has
    never started uploading should be governed by the remote queue timeout, not the
    no-progress timeout used for active transfers.
    """
    return bool(downloads) and remote_queue_count == len(downloads)





# === Transfer ID re-derivation ===

def match_transfer_id(
    downloads: DownloadUser | list[DownloadUser],
    target_filename: str,
    username: str | None = None,
) -> str | None:
    """Find the slskd transfer ID for a filename in slskd download responses.

    downloads may be a single user-group entry or the list returned by
    slskd.transfers.get_all_downloads(). When a list is provided, username
    narrows the search to one peer.
    Returns the transfer ID string, or None if not found.
    """
    transfer = match_transfer(downloads, target_filename, username=username)
    if transfer is None:
        return None
    return transfer.id


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


def _transfer_latest_timestamp(transfer: TransferSnapshot) -> datetime:
    return max(
        _parse_transfer_timestamp(transfer.ended_at),
        _parse_transfer_timestamp(transfer.started_at),
        _parse_transfer_timestamp(transfer.enqueued_at),
        _parse_transfer_timestamp(transfer.requested_at),
    )


def _transfer_priority(transfer: TransferSnapshot) -> tuple[int, int, datetime]:
    """Rank duplicate transfer snapshots for the same username+filename.

    Prefer active transfers over terminal ones. Among terminal snapshots,
    prefer successful completions over cancelled/errored attempts, and then
    pick the newest lifecycle timestamp.
    """
    state = transfer.state
    is_terminal = state.startswith("Completed,")
    is_success = state == "Completed, Succeeded"
    latest_ts = _transfer_latest_timestamp(transfer)
    return (0 if is_terminal else 1, 1 if is_success else 0, latest_ts)


def _is_terminal_transfer_before(
    transfer: TransferSnapshot,
    not_before: str | None,
) -> bool:
    if not_before is None:
        return False
    state = transfer.state
    if not state.startswith("Completed,"):
        return False
    threshold = _parse_transfer_timestamp(not_before)
    latest_ts = _transfer_latest_timestamp(transfer)
    if latest_ts == datetime.min.replace(tzinfo=timezone.utc):
        return False
    return latest_ts < threshold


def match_transfer(
    downloads: DownloadUser | list[DownloadUser],
    target_filename: str,
    username: str | None = None,
) -> TransferSnapshot | None:
    """Find the best slskd transfer snapshot for a username+filename pair."""
    groups = downloads if isinstance(downloads, list) else [downloads]
    candidates: list[TransferSnapshot] = []
    for group in groups:
        if username is not None and group.username not in ("", username):
            continue
        for directory in group.directories:
            for slskd_file in directory.files:
                if slskd_file.filename == target_filename:
                    candidates.append(slskd_file)

    if not candidates:
        return None
    return max(candidates, key=_transfer_priority)


def _get_all_downloads_snapshot(
    slskd_client: Any,
    *,
    purpose: str,
    include_removed: bool = True,
) -> list[DownloadUser] | None:
    """Fetch the full slskd download snapshot via the bulk endpoint.

    The username-scoped endpoint is unreliable for some valid peer names
    containing spaces/punctuation, so monitoring code uses the bulk list and
    matches locally instead.

    ``include_removed`` defaults to True because most callers (poll,
    re-derivation) need terminal transfers as evidence. The #278 orphan
    convergence only reasons about live transfers (it skips ``Completed*``
    states itself), so it passes False to trim the payload.
    """
    try:
        return slskd_client.transfers.get_all_downloads(
            includeRemoved=include_removed)
    except Exception:
        logger.warning(f"Failed to get all downloads for {purpose}", exc_info=True)
        return None


def converge_slskd_orphans(ctx: CratediggerContext) -> int:
    """Phase 0 convergence (#278; ledger-positive since #571 PR 3): cancel
    live slskd transfers cratedigger created but no longer owns.

    Operator actions that supersede a downloading request (Replace is the
    canonical one — see CLAUDE.md invariant 7) deliberately leave its
    in-flight slskd transfers running; this is the convergence that reaps
    them. Runs once per cycle, before Phase 1/Phase 2 start, while nothing
    is enqueuing — so a live transfer without a backing ``downloading``
    row is genuinely stranded, not mid-write. The slskd snapshot is taken
    BEFORE the DB read: a transfer enqueued after the snapshot can't
    appear in it, so ordering alone still rules out false strays on the
    "backed" side of the check. (It buys nothing on the ledger side — a
    fresh write-ahead row from a mid-flight enqueue makes that transfer
    MORE ledgered, never less, so it can only turn a would-be stray into
    correctly-still-in-flight, never the other way.)

    Good-citizen doctrine (#571): a live transfer is cancelled ONLY when
    it is BOTH (a) present in cratedigger's write-ahead
    ``slskd_transfer_ledger`` (proof cratedigger created it — see
    ``lib.repair.find_slskd_orphans`` for the full classification) AND
    (b) not currently backed by a ``downloading`` row. A transfer absent
    from the ledger is foreign — on a shared slskd instance that may be a
    human's — and is NEVER cancelled, whatever its state or age.

    Best-effort: a snapshot failure skips the pass, a cancel failure is
    logged and the remaining strays are still attempted. Returns the
    number of transfers successfully cancelled.
    """
    from lib.repair import find_slskd_orphans

    downloads = _get_all_downloads_snapshot(
        ctx.slskd, purpose="orphan-transfer convergence",
        include_removed=False)
    if downloads is None:
        return 0
    db = ctx.pipeline_db_source._get_db()
    ownership = find_slskd_orphans(
        downloads, db.get_downloading(), db.get_owned_transfer_keys())
    orphans = ownership.orphans
    cancelled = 0
    for orphan in orphans:
        try:
            ctx.slskd.transfers.cancel_download(
                orphan.username, orphan.transfer_id)
            cancelled += 1
            logger.warning(
                "SLSKD ORPHAN: cancelled ledger-owned stray transfer "
                f"user={orphan.username!r} file={orphan.filename!r} "
                f"state={orphan.state!r} id={orphan.transfer_id}")
        except Exception:
            logger.exception(
                "SLSKD ORPHAN: failed to cancel ledger-owned stray transfer "
                f"user={orphan.username!r} file={orphan.filename!r} "
                f"id={orphan.transfer_id} — will retry next cycle")
    if orphans:
        logger.info(
            f"SLSKD ORPHAN: convergence cancelled {cancelled}/{len(orphans)} "
            f"ledger-owned stray transfer(s); {ownership.foreign_count} "
            "foreign live transfer(s) left untouched")
    return cancelled


def rederive_transfer_ids(
    entry: GrabListEntry,
    slskd_client: Any,
    *,
    snapshot: list[DownloadUser] | None = None,
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
            f.id = transfer.id
            if transfer.state.startswith("Completed,"):
                f.status = transfer
            else:
                f.status = None
        else:
            logger.debug(f"Transfer not found for {f.filename} from {f.username}")
    return True


# === Disk orphan reaper (issue #550 defect 3) ===

# No config knob (single-operator doctrine — .claude/rules/scope.md):
# completed-but-unconsumed downloads have no slskd-side handle once
# remove_completed_downloads() purges the transfer record at the end of
# every cycle, so a fixed grace window is the only safety net before the
# filesystem sweep below reaps a folder.
ORPHAN_MIN_AGE_DAYS = 7


@dataclass(frozen=True)
class DiskReapSummary:
    """Aggregate result of one on-disk orphan sweep.

    ``mutated`` gates the cycle's INFO summary line: only a sweep that
    actually removed files or pruned directories says anything —
    protected/young counts alone stay silent, so a steady-state cycle
    produces no log traffic. ``aborted`` marks a fail-closed cycle (a
    downloading row's ownership could not be decoded); nothing was
    deleted.
    """
    removed: int = 0
    removed_bytes: int = 0
    pruned_dirs: int = 0
    protected: int = 0
    skipped_young: int = 0
    aborted: bool = False

    @property
    def mutated(self) -> bool:
        return bool(self.removed or self.pruned_dirs)


class DiskReapOwnershipError(Exception):
    """A downloading row's ownership could not be established.

    Raised when a ``downloading`` row's ``active_download_state`` is
    missing or fails to decode: the protected set would silently omit
    that row's canonical folder and stamped files, leaving them
    reap-eligible (fail-open). A deletion sweep must fail CLOSED
    instead — ``reap_disk_orphans`` aborts the entire cycle's sweep
    (zero deletions) when this is raised.
    """


def _protected_paths_for_downloading(
    root: str,
    downloading_rows: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    """Return (protected_dirs, protected_files) the reaper must never touch.

    ``protected_dirs`` always includes the ``failed_imports/`` quarantine
    subtree (Wrong Match cards reference these paths) plus, for every
    ``downloading`` row, its canonical processing folder derived the SAME
    way materialize does — ``canonical_processing_path`` keyed by the
    attempt fingerprint of the row's persisted (username, filename) set
    (mirrors ``lib/download_processing.py``'s ``_canonical_import_folder_path``
    / ``_attempt_fingerprint_for`` without importing that module, which
    would create an import cycle: ``lib.download`` imports
    ``lib.slskd_transfers`` at module scope). ``protected_files`` adds each
    row's stamped ``local_path`` entries directly, as a second, independent
    guard beyond the directory-level protection.

    Raises ``DiskReapOwnershipError`` when ANY downloading row's
    ``active_download_state`` is missing or undecodable — partial
    ownership knowledge must abort the sweep (fail-closed), never let
    the unparseable row's files become reap-eligible.
    """
    protected_dirs = {
        normalize_processing_path(os.path.join(root, "failed_imports")),
    }
    protected_files: set[str] = set()

    for row in downloading_rows:
        raw_state = row.get("active_download_state")
        try:
            # from_raw raises ValueError on None/missing state too —
            # a downloading row with no state is crash-recovery limbo
            # (Phase 1 resets it to wanted this same cycle); until it
            # heals, its ownership is unknowable and the sweep must not
            # proceed on guesswork.
            state = ActiveDownloadState.from_raw(raw_state)
        except Exception as exc:
            raise DiskReapOwnershipError(
                f"request {row.get('id')}: active_download_state is "
                "missing or undecodable — cannot establish which files "
                "this downloading row owns"
            ) from exc

        for f in state.files:
            if f.local_path:
                protected_files.add(normalize_processing_path(f.local_path))

        fingerprint = attempt_fingerprint(
            [(f.username, f.filename) for f in state.files])
        canonical = canonical_processing_path(
            artist=row.get("artist_name") or "",
            title=row.get("album_title") or "",
            year=str(row.get("year") or ""),
            slskd_download_dir=root,
            attempt_fingerprint=fingerprint,
        )
        protected_dirs.add(normalize_processing_path(canonical))

    return protected_dirs, protected_files


def _is_within_any(path: str, roots: set[str]) -> bool:
    return any(path_is_within_root(path, candidate) for candidate in roots)


def _prune_empty_upward(start_dir: str, root: str) -> int:
    """Remove now-empty directories walking upward from ``start_dir``,
    stopping at (never removing) ``root``.

    Extends ``_delete_completed_payloads``'s single-level prune to walk
    multiple levels, for nested orphan folders (e.g. a multi-disc
    ``CD 01/`` subfolder under the canonical album folder). No age check
    here — the files that emptied this directory were already age-gated
    individually; unconditional ``os.rmdir`` is the same "fails harmlessly
    if non-empty" safety used throughout this module.
    """
    pruned = 0
    current = start_dir
    root_real = os.path.realpath(root)
    while (
        path_is_within_root(current, root)
        and os.path.realpath(current) != root_real
    ):
        try:
            os.rmdir(current)
        except OSError:
            break
        pruned += 1
        current = os.path.dirname(current)
    return pruned


def _prune_stale_empty_dirs(
    root: str,
    protected_dirs: set[str],
    threshold: float,
) -> int:
    """Remove directories that were already empty — never touched by this
    sweep's file removals — and are older than ``threshold`` by their own
    mtime.

    There are no files to age-gate in an already-empty directory, so its
    own mtime is the safety net that stops a folder slskd only just
    created (and hasn't populated yet) from being reaped mid-download.
    Walks deepest-first so a chain of nested empty directories collapses
    in one pass.
    """
    candidates: list[str] = []
    root_norm = normalize_processing_path(root)
    for dirpath, dirnames, _filenames in os.walk(root, topdown=True):
        norm_dirpath = normalize_processing_path(dirpath)
        if _is_within_any(norm_dirpath, protected_dirs):
            dirnames[:] = []
            continue
        dirnames[:] = [
            d for d in dirnames
            if not _is_within_any(
                normalize_processing_path(os.path.join(dirpath, d)),
                protected_dirs)
        ]
        if norm_dirpath == root_norm:
            continue
        candidates.append(dirpath)

    candidates.sort(key=lambda p: p.count(os.sep), reverse=True)

    pruned = 0
    for dirpath in candidates:
        if not os.path.isdir(dirpath):
            continue  # Already removed as a side effect of a deeper prune.
        try:
            with os.scandir(dirpath) as entries:
                if any(True for _ in entries):
                    continue
        except OSError:
            continue
        try:
            mtime = os.path.getmtime(dirpath)
        except OSError:
            continue
        if mtime >= threshold:
            continue
        try:
            os.rmdir(dirpath)
        except OSError:
            continue
        pruned += 1
    return pruned


def reap_disk_orphans(ctx: CratediggerContext) -> DiskReapSummary:
    """Phase 0 sweep (issue #550 defect 3): reap completed-but-unconsumed
    downloads that have no slskd-side handle.

    ``converge_slskd_orphans`` above only cancels LIVE unowned transfers —
    it deliberately skips ``Completed*`` states — and
    ``remove_completed_downloads()`` purges slskd's completed-transfer
    records at the end of every cycle, so a stranded completed folder has
    no slskd handle left to reason from. This reaper reasons from
    filesystem + DB state instead: any file under ``slskd_download_dir``
    that isn't in the ``failed_imports/`` quarantine, isn't owned by a
    live ``downloading`` row's canonical folder or stamped ``local_path``,
    and is older than ``ORPHAN_MIN_AGE_DAYS`` is deleted individually
    (never a recursive folder ``rmtree`` — CLAUDE.md), with now-empty
    parent directories pruned afterward. This also reaps stale
    bare-canonical processing folders left over from before the
    per-attempt fingerprint suffix (issue #550 phase 2, PR #560): no
    current attempt computes that bare name any more, so it was never in
    the protected set to begin with — no special-casing needed.

    Fail-closed: if ANY downloading row's ownership can't be decoded,
    the whole sweep is skipped for the cycle (zero deletions,
    ``aborted=True``). Best-effort otherwise, and silent unless it
    actually removed files or pruned directories — matching
    ``converge_slskd_orphans``'s Phase 0 contract.
    """
    root = ctx.cfg.slskd_download_dir
    if not root or not os.path.isdir(root):
        return DiskReapSummary()

    db = ctx.pipeline_db_source._get_db()
    try:
        protected_dirs, protected_files = _protected_paths_for_downloading(
            root, db.get_downloading())
    except DiskReapOwnershipError:
        logger.exception(
            "DISK-REAP ABORTED: could not establish ownership for a "
            "downloading row; skipping the entire sweep this cycle "
            "(zero deletions) — fail-closed")
        return DiskReapSummary(aborted=True)

    threshold = time.time() - ORPHAN_MIN_AGE_DAYS * 86400
    removed = 0
    removed_bytes = 0
    protected_count = 0
    skipped_young = 0
    touched_dirs: set[str] = set()

    for dirpath, dirnames, filenames in os.walk(root, topdown=True):
        norm_dirpath = normalize_processing_path(dirpath)
        if _is_within_any(norm_dirpath, protected_dirs):
            dirnames[:] = []
            continue
        dirnames[:] = [
            d for d in dirnames
            if not _is_within_any(
                normalize_processing_path(os.path.join(dirpath, d)),
                protected_dirs)
        ]
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            norm_path = normalize_processing_path(full_path)
            if not path_is_within_root(norm_path, root):
                continue  # Defensive — os.walk(root) can't yield this.
            if norm_path in protected_files:
                protected_count += 1
                continue
            try:
                mtime = os.path.getmtime(full_path)
            except OSError:
                continue
            if mtime >= threshold:
                skipped_young += 1
                continue
            try:
                size = os.path.getsize(full_path)
            except OSError:
                size = 0
            try:
                os.remove(full_path)
            except OSError:
                logger.warning(
                    "DISK-REAP: failed to remove %s", full_path,
                    exc_info=True)
                continue
            removed += 1
            removed_bytes += size
            touched_dirs.add(dirpath)
            logger.info(
                "DISK-REAP removed %s (age>=%dd, %d bytes)",
                full_path, ORPHAN_MIN_AGE_DAYS, size)

    pruned = 0
    for touched_dir in touched_dirs:
        pruned += _prune_empty_upward(touched_dir, root)
    pruned += _prune_stale_empty_dirs(root, protected_dirs, threshold)

    summary = DiskReapSummary(
        removed=removed,
        removed_bytes=removed_bytes,
        pruned_dirs=pruned,
        protected=protected_count,
        skipped_young=skipped_young,
    )
    if summary.mutated:
        logger.info(
            "DISK-REAP summary removed=%d bytes=%d pruned_dirs=%d "
            "protected=%d skipped_young=%d",
            summary.removed, summary.removed_bytes, summary.pruned_dirs,
            summary.protected, summary.skipped_young)
    return summary
