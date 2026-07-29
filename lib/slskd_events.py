"""slskd event-feed ingestion — issue #146 phase 1.

Once per cycle (start of Phase 1, before the per-request poll loop) the
pipeline pages slskd's ``/api/v0/events`` feed and stamps the
authoritative ``localFilename`` from every new ``DownloadFileComplete``
event onto the matching file in ``active_download_state``. A single-row
``slskd_event_cursor`` table records the newest event processed so each
event is consumed exactly once across cycles.

Matching key is ``(username, remote filename)`` — slskd transfer ids are
NOT persisted in ``active_download_state`` and are re-issued when a file
is retried, while the remote path is the durable identity both sides
share. Active-state stamping reads current downloading incarnations after
collecting the event window and chooses the first newest-first completion
whose occurrence is not before that incarnation's enqueue witness.

Phase 3 is active: the stamped ``local_path`` is the ONLY source of
file locations. ``process_completed_album`` hard-fails an unstamped
file (grep key ``EVENT-PATH MISSING``); the exact processor owner handles
the failure without path inference.

Failure isolation: the caller wraps ingestion in try/except — an events
API outage stamps nothing that cycle and never blocks the downloader's
ownership handoff. Processor source-path validation remains authoritative.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import msgspec

from lib.quality import ActiveDownloadState
from lib.slskd_client import (
    DOWNLOAD_DIRECTORY_COMPLETE,
    DOWNLOAD_FILE_COMPLETE,
    SlskdRawEvent,
    decode_download_directory_complete,
    decode_download_file_complete,
)

logger = logging.getLogger("cratedigger")

EVENT_PAGE_LIMIT = 500
# Bounds one cycle's catch-up scan at 10k events (~ a very heavy day on
# doc2). If the cursor is not found within the cap the scan stops, the
# cursor still advances to the newest event, and ``cursor_gap=True`` is
# reported. Older unprocessed completions stay unstamped; the exact processor
# owner then enforces the source-path contract and recovery policy.
MAX_EVENT_PAGES = 20


@dataclass(frozen=True)
class EventIngestResult:
    """Outcome of one ingestion pass, for the cycle log."""

    outcome: str  # "bootstrapped" | "ingested" | "no_new_events" | "empty_feed"
    events_seen: int = 0
    file_events: int = 0
    files_stamped: int = 0
    requests_updated: int = 0
    # T2 (issue #571): transfer-ledger rows stamped this pass. Distinct
    # from files_stamped/requests_updated (active_download_state, only
    # currently-downloading rows) — a ledger row can be stamped for a
    # request that has since left 'downloading' too.
    transfers_stamped: int = 0
    cursor_gap: bool = False
    cursor_advanced: bool = False
    cursor_hold_reason: str | None = None

    def to_log_line(self) -> str:
        return (
            f"SLSKD EVENTS: outcome={self.outcome} events_seen={self.events_seen} "
            f"file_events={self.file_events} files_stamped={self.files_stamped} "
            f"requests_updated={self.requests_updated} "
            f"transfers_stamped={self.transfers_stamped} "
            f"cursor_gap={self.cursor_gap} "
            f"cursor_advanced={self.cursor_advanced} "
            f"cursor_hold_reason={self.cursor_hold_reason}"
        )


def _parse_event_timestamp(value: str) -> datetime | None:
    """Tolerant parse of slskd's ISO-8601 event timestamps (7-digit fractions).

    Returns ``None`` when unparseable — callers must NOT treat an
    unparseable timestamp as "older than the cursor", or one bad new
    event would silently terminate the scan and strand everything
    behind it.
    """
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _collect_new_events(
    slskd: Any,
    last_event_id: str,
    last_event_timestamp: str,
) -> tuple[list[SlskdRawEvent], bool]:
    """Page the feed newest-first until the cursor event is reached.

    Returns ``(new_events_newest_first, cursor_gap)``. Events arriving
    mid-pagination shift offsets and can duplicate an event across pages;
    stamping is idempotent so duplicates are harmless, and the missed
    newcomers are beyond the cursor we persist, so the next cycle picks
    them up.
    """
    last_ts = _parse_event_timestamp(last_event_timestamp)
    collected: list[SlskdRawEvent] = []
    seen_ids: set[str] = set()
    offset = 0
    for _ in range(MAX_EVENT_PAGES):
        page = slskd.events.list(limit=EVENT_PAGE_LIMIT, offset=offset)
        if not page.events:
            return collected, False
        for event in page.events:
            if event.id == last_event_id:
                return collected, False
            event_ts = _parse_event_timestamp(event.timestamp)
            if last_ts is not None and event_ts is not None and event_ts < last_ts:
                # Cursor event pruned/missing — everything older is seen.
                return collected, False
            # Mid-scan arrivals shift offsets and can repeat an event
            # across pages — collect each id once.
            if event.id not in seen_ids:
                seen_ids.add(event.id)
                collected.append(event)
        offset += len(page.events)
        if page.total_count is not None and offset >= page.total_count:
            return collected, False
    return collected, True


@dataclass(frozen=True)
class _EventCompletionInfo:
    """One decoded DownloadFileComplete event's payload, keyed by
    (username, remote filename) — what one ingestion pass needs to stamp
    both ``active_download_state`` and the transfer ledger."""

    local_path: str
    occurred_at: datetime | None


def _completion_info_from_events(
    events: list[SlskdRawEvent],
) -> dict[tuple[str, str], list[_EventCompletionInfo]]:
    """Decode completion candidates without conflating their consumers.

    Active-state classification retains every newest-first candidate so a
    pre-incarnation entry cannot shadow an older eligible entry. The transfer
    ledger intentionally preserves its existing newest-decoded projection and
    authority, independent of occurrence-time qualification.
    """
    active_candidates: dict[
        tuple[str, str],
        list[_EventCompletionInfo],
    ] = {}
    for event in events:
        if event.type != DOWNLOAD_FILE_COMPLETE:
            continue
        try:
            payload = decode_download_file_complete(event)
        except msgspec.DecodeError:
            # Parent of ValidationError — also catches malformed (non-JSON)
            # ``data`` strings, which must skip one event, not kill the pass.
            logger.warning(
                "SLSKD EVENTS: undecodable DownloadFileComplete payload "
                "(event id=%s) — skipping", event.id, exc_info=True)
            continue
        key = (payload.transfer.username, payload.transfer.filename)
        # slskd 0.24.5 authors the DownloadFileComplete envelope timestamp
        # from the nested transfer's EndedAt before inserting the event. It is
        # therefore occurrence evidence; receipt time and feed position are
        # not substitutes for this causal bound.
        completion = _EventCompletionInfo(
            local_path=payload.local_filename,
            occurred_at=_parse_event_timestamp(event.timestamp),
        )
        active_candidates.setdefault(key, []).append(completion)
    return active_candidates


@dataclass(frozen=True)
class _StampLocalPathsResult:
    """Persistence outcome, including valid dirty writes that lost CAS."""

    files_stamped: int = 0
    requests_updated: int = 0
    lost_current_incarnation_writes: int = 0


def _stamp_local_paths(
    db: Any,
    downloading: Sequence[Mapping[str, Any]],
    completion_candidates: dict[
        tuple[str, str],
        list[_EventCompletionInfo],
    ],
) -> _StampLocalPathsResult:
    """Write matched local paths into each request's persisted state.

    A row is dirty only when a completion has a provable occurrence at or
    after its valid enqueue witness. A rejected CAS is distinct from a clean
    no-op: a complete event window must retain its cursor for replay.
    """
    files_stamped = 0
    requests_updated = 0
    lost_current_incarnation_writes = 0
    for row in downloading:
        raw_state = row.get("active_download_state")
        if not raw_state:
            continue
        try:
            state = ActiveDownloadState.from_raw(raw_state)
        except Exception:
            logger.warning(
                "SLSKD EVENTS: unparseable active_download_state for "
                "request %s — skipping", row.get("id"), exc_info=True)
            continue
        enqueued_at = _parse_event_timestamp(state.enqueued_at)
        if enqueued_at is None:
            logger.warning(
                "SLSKD EVENTS: invalid enqueued_at witness for request %s "
                "— excluding active completion classification",
                row.get("id"),
            )
            continue
        row_stamped = 0
        for file_state in state.files:
            candidates = completion_candidates.get(
                (file_state.username, file_state.filename),
                (),
            )
            info = next(
                (
                    candidate
                    for candidate in candidates
                    if candidate.occurred_at is not None
                    and candidate.occurred_at >= enqueued_at
                ),
                None,
            )
            if info is not None and file_state.local_path != info.local_path:
                file_state.local_path = info.local_path
                row_stamped += 1
        if row_stamped and db.update_download_state_if_downloading(
                row["id"],
                state.to_json(),
                expected_enqueued_at=state.enqueued_at):
            # Count only what actually persisted — a row that left
            # 'downloading' or gained a newer incarnation mid-ingest
            # contributes nothing.
            requests_updated += 1
            files_stamped += row_stamped
        elif row_stamped:
            lost_current_incarnation_writes += 1
    return _StampLocalPathsResult(
        files_stamped=files_stamped,
        requests_updated=requests_updated,
        lost_current_incarnation_writes=lost_current_incarnation_writes,
    )


def _stamp_transfer_ledger(
    db: Any,
    completion_info: dict[tuple[str, str], _EventCompletionInfo],
) -> int:
    """Write-ahead ledger completion stamp (issue #571, T2).

    Matches by the SAME ``(username, remote filename)`` keys
    ``_stamp_local_paths`` already uses for ``active_download_state``, in
    the SAME ingestion pass (one new call, no second cursor, no separate
    scan). Only an already POST-confirmed ledger row may receive the path.
    Pending intent and unledgered pairs stamp nothing and never invent or
    promote a row; a later human same-key event cannot claim a rejected
    enqueue. Transfer IDs are deliberately ignored because slskd re-issues
    them while retrying one durable queue key.
    """
    stamped = 0
    for (username, filename), info in completion_info.items():
        stamped += db.stamp_transfer_completion(
            username, filename, info.local_path)
    return stamped


@dataclass(frozen=True)
class RecentCompletionPaths:
    """Authoritative local paths from one fresh events-page fetch.

    ``files`` maps ``(username, remote filename)`` → local file path;
    ``directories`` maps ``(username, remote directory)`` → local
    directory. Consumed by ``cancel_and_delete`` to locate payloads that
    completed after the cycle's ingest pass and therefore carry no
    ``local_path`` stamp yet.
    """

    files: dict[tuple[str, str], str]
    directories: dict[tuple[str, str], str]


def recent_completion_paths(slskd: Any) -> RecentCompletionPaths:
    """One page of the newest events, mapped to authoritative local paths.

    Best-effort: any feed failure returns empty maps — callers degrade
    to stamped-paths-only cleanup, never blocking a cancel.
    """
    empty = RecentCompletionPaths(files={}, directories={})
    try:
        page = slskd.events.list(limit=EVENT_PAGE_LIMIT, offset=0)
    except Exception:
        logger.warning(
            "SLSKD EVENTS: fresh completion-path lookup failed — "
            "cleanup degrades to stamped paths only", exc_info=True)
        return empty
    files: dict[tuple[str, str], str] = {}
    directories: dict[tuple[str, str], str] = {}
    for event in page.events:
        try:
            if event.type == DOWNLOAD_FILE_COMPLETE:
                payload = decode_download_file_complete(event)
                files.setdefault(
                    (payload.transfer.username, payload.transfer.filename),
                    payload.local_filename)
            elif event.type == DOWNLOAD_DIRECTORY_COMPLETE:
                dir_payload = decode_download_directory_complete(event)
                directories.setdefault(
                    (dir_payload.username, dir_payload.remote_directory_name),
                    dir_payload.local_directory_name)
        except msgspec.DecodeError:
            # Parent of ValidationError — also catches malformed (non-JSON)
            # ``data`` strings.
            logger.warning(
                "SLSKD EVENTS: undecodable %s payload (event id=%s) — "
                "skipping", event.type, event.id, exc_info=True)
    return RecentCompletionPaths(files=files, directories=directories)


def ingest_download_file_events(
    db: Any,
    slskd: Any,
) -> EventIngestResult:
    """One ingestion pass: page new events, stamp local paths, advance cursor.

    Current downloading incarnations are read after the event window is
    collected. Runs even when there are no downloading rows so the cursor keeps
    tracking the feed during idle stretches instead of accumulating a
    10k-event backlog for the next active cycle.
    """
    cursor = db.get_slskd_event_cursor()
    if cursor is None:
        # Bootstrap: seed from the newest event without backfilling the
        # (389k+ on doc2) historical feed.
        page = slskd.events.list(limit=1, offset=0)
        if not page.events:
            return EventIngestResult(outcome="empty_feed")
        newest = page.events[0]
        db.upsert_slskd_event_cursor(newest.id, newest.timestamp)
        return EventIngestResult(
            outcome="bootstrapped",
            cursor_advanced=True,
        )

    new_events, cursor_gap = _collect_new_events(
        slskd,
        str(cursor["last_event_id"]),
        str(cursor["last_event_timestamp"]),
    )
    if not new_events:
        return EventIngestResult(outcome="no_new_events", cursor_gap=cursor_gap)

    completion_candidates = _completion_info_from_events(new_events)
    stamp_result = (
        _stamp_local_paths(
            db,
            db.get_downloading(),
            completion_candidates,
        )
        if completion_candidates
        else _StampLocalPathsResult()
    )
    # T2 (issue #571): same pass and decoded events, but a deliberately
    # separate newest-event projection. Active incarnation qualification
    # grants no transfer-ledger authority.
    ledger_newest = {
        key: candidates[0]
        for key, candidates in completion_candidates.items()
    }
    transfers_stamped = (
        _stamp_transfer_ledger(db, ledger_newest)
        if ledger_newest
        else 0
    )

    newest = new_events[0]
    cursor_hold_reason: str | None = None
    if stamp_result.lost_current_incarnation_writes and not cursor_gap:
        cursor_hold_reason = "lost_current_incarnation_write"
        logger.warning(
            "SLSKD EVENTS: holding complete event window for replay after "
            "%d current-incarnation write(s) lost their witness",
            stamp_result.lost_current_incarnation_writes,
        )
    else:
        if stamp_result.lost_current_incarnation_writes:
            logger.warning(
                "SLSKD EVENTS: cursor-gap fail-open after %d "
                "current-incarnation write(s) lost their witness; advancing "
                "to the newest collected event; processor source-path "
                "validation remains authoritative for omitted history",
                stamp_result.lost_current_incarnation_writes,
            )
        db.upsert_slskd_event_cursor(newest.id, newest.timestamp)

    return EventIngestResult(
        outcome="ingested",
        events_seen=len(new_events),
        file_events=sum(
            1 for e in new_events if e.type == DOWNLOAD_FILE_COMPLETE),
        files_stamped=stamp_result.files_stamped,
        requests_updated=stamp_result.requests_updated,
        transfers_stamped=transfers_stamped,
        cursor_gap=cursor_gap,
        cursor_advanced=cursor_hold_reason is None,
        cursor_hold_reason=cursor_hold_reason,
    )
