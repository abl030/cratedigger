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
share. Newest event wins when the same file completed more than once
(a re-download after retry): the feed is newest-first, so the first
occurrence seen is kept.

Phase 3 is active: the stamped ``local_path`` is the ONLY source of
file locations. ``process_completed_album`` hard-fails an unstamped
file (grep key ``EVENT-PATH MISSING``); the poller retries within a
grace window (benign completion-vs-event-write race) and self-heals to
re-download past it.

Failure isolation: the caller wraps ingestion in try/except — an events
API outage stamps nothing that cycle and never blocks polling; affected
completions ride the materialize grace window until the next successful
ingest.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
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
# reported — older unprocessed completions stay unstamped and ride the
# materialize grace window (self-heal to re-download past it).
MAX_EVENT_PAGES = 20


@dataclass(frozen=True)
class EventIngestResult:
    """Outcome of one ingestion pass, for the cycle log."""

    outcome: str  # "bootstrapped" | "ingested" | "no_new_events" | "empty_feed"
    events_seen: int = 0
    file_events: int = 0
    files_stamped: int = 0
    requests_updated: int = 0
    cursor_gap: bool = False

    def to_log_line(self) -> str:
        return (
            f"SLSKD EVENTS: outcome={self.outcome} events_seen={self.events_seen} "
            f"file_events={self.file_events} files_stamped={self.files_stamped} "
            f"requests_updated={self.requests_updated} cursor_gap={self.cursor_gap}"
        )


def _parse_event_timestamp(value: str) -> datetime | None:
    """Tolerant parse of slskd's ISO-8601 event timestamps (7-digit fractions).

    Returns ``None`` when unparseable — callers must NOT treat an
    unparseable timestamp as "older than the cursor", or one bad new
    event would silently terminate the scan and strand everything
    behind it.
    """
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def _local_paths_from_events(
    events: list[SlskdRawEvent],
) -> dict[tuple[str, str], str]:
    """Map ``(username, remote filename)`` → authoritative local path.

    ``events`` is newest-first; the first occurrence of a key wins.
    """
    local_paths: dict[tuple[str, str], str] = {}
    file_events = 0
    for event in events:
        if event.type != DOWNLOAD_FILE_COMPLETE:
            continue
        file_events += 1
        try:
            payload = decode_download_file_complete(event)
        except msgspec.ValidationError:
            logger.warning(
                "SLSKD EVENTS: undecodable DownloadFileComplete payload "
                "(event id=%s) — skipping", event.id, exc_info=True)
            continue
        key = (payload.transfer.username, payload.transfer.filename)
        local_paths.setdefault(key, payload.local_filename)
    return local_paths


def _stamp_local_paths(
    db: Any,
    downloading: list[dict[str, Any]],
    local_paths: dict[tuple[str, str], str],
) -> tuple[int, int]:
    """Write matched local paths into each request's persisted state.

    Returns ``(files_stamped, requests_updated)``.
    """
    files_stamped = 0
    requests_updated = 0
    for row in downloading:
        raw_state = row.get("active_download_state")
        if not raw_state:
            continue
        try:
            state = (
                ActiveDownloadState.from_dict(raw_state)
                if isinstance(raw_state, dict)
                else ActiveDownloadState.from_json(str(raw_state))
            )
        except Exception:
            logger.warning(
                "SLSKD EVENTS: unparseable active_download_state for "
                "request %s — skipping", row.get("id"), exc_info=True)
            continue
        row_stamped = 0
        for file_state in state.files:
            local_path = local_paths.get(
                (file_state.username, file_state.filename))
            if local_path is not None and file_state.local_path != local_path:
                file_state.local_path = local_path
                row_stamped += 1
        if row_stamped and db.update_download_state_if_downloading(
                row["id"], state.to_json()):
            # Count only what actually persisted — a row that left
            # 'downloading' mid-ingest contributes nothing.
            requests_updated += 1
            files_stamped += row_stamped
    return files_stamped, requests_updated


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
        except msgspec.ValidationError:
            logger.warning(
                "SLSKD EVENTS: undecodable %s payload (event id=%s) — "
                "skipping", event.type, event.id, exc_info=True)
    return RecentCompletionPaths(files=files, directories=directories)


def ingest_download_file_events(
    db: Any,
    slskd: Any,
    downloading: list[dict[str, Any]],
) -> EventIngestResult:
    """One ingestion pass: page new events, stamp local paths, advance cursor.

    ``downloading`` is the row list Phase 1 already fetched — passed in so
    the poll cycle doesn't query it twice. Runs even when it is empty so
    the cursor keeps tracking the feed during idle stretches instead of
    accumulating a 10k-event backlog for the next active cycle.
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
        return EventIngestResult(outcome="bootstrapped")

    new_events, cursor_gap = _collect_new_events(
        slskd,
        str(cursor["last_event_id"]),
        str(cursor["last_event_timestamp"]),
    )
    if not new_events:
        return EventIngestResult(outcome="no_new_events", cursor_gap=cursor_gap)

    local_paths = _local_paths_from_events(new_events)
    files_stamped, requests_updated = (
        _stamp_local_paths(db, downloading, local_paths)
        if local_paths
        else (0, 0)
    )

    newest = new_events[0]
    db.upsert_slskd_event_cursor(newest.id, newest.timestamp)
    return EventIngestResult(
        outcome="ingested",
        events_seen=len(new_events),
        file_events=sum(
            1 for e in new_events if e.type == DOWNLOAD_FILE_COMPLETE),
        files_stamped=files_stamped,
        requests_updated=requests_updated,
        cursor_gap=cursor_gap,
    )
