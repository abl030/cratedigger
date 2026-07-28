"""Generated slskd event-ingestion tests — issue #548 follow-up, extended
for issue #571 T2.

Property-based tests over ``lib/slskd_events.py::ingest_download_file_events``
— the pass that stamps authoritative completed-file locations from the
slskd events feed (the stamp is the ONLY source of completed-file
locations; issue #146).

Four properties over generated feed histories:

1. **Stamping oracle** — for worlds with a clean cursor (its id present in
   the feed, unique event ids), every downloading file ends up stamped
   with exactly the newest decodable DownloadFileComplete event for its
   ``(username, remote filename)`` key inside the new-events window — and
   nothing else: no invented paths, no stamps from behind the cursor, no
   writes to rows that left ``downloading`` before fresh classification.
   The SAME test also covers T2 (issue #571): a subset of world keys are
   pre-ledgered (``slskd_transfer_ledger`` rows, migration 045) and must be
   stamped with the SAME newest-event oracle, in the SAME pass — regardless
   of whether the owning request left ``downloading`` before fresh
   classification (the ledger stamp is independent of
   active_download_state's request-status gate).
2. **Totality + exactly-once** — for arbitrary worlds (duplicate event
   ids, garbage timestamps, undecodable payloads, pruned cursors,
   bootstrap): ingestion never raises, every stamped path (both
   active_download_state AND the ledger) originates from the feed, and
   an immediate second pass over the unchanged feed is a no-op
   (``no_new_events``/``empty_feed``, identical states and cursor).
3. **Duplicate-id invariance** — a feed with duplicated events (the
   mid-pagination offset-shift shape) produces exactly the same outcome
   as the same feed deduplicated.
4. **Incarnation classification + replay** — fresh current B rows classify
   the event window; before/at/after-B occurrence times, shared and changed
   keys, candidate shadowing, replacement-before-write, and replay preserve
   the current path, cursor commit marker, and idempotent ledger.

Multi-page scans and the page-cap ``cursor_gap`` path stay pinned by the
hand tests in tests/test_slskd_events.py (they need >500-event feeds).
The T2 deterministic pins live in
``tests/test_slskd_events.py::TestTransferLedgerStamping``.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import json
import os
import sys
import unittest
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.pipeline_db import TransferLedgerRow
from lib.quality import ActiveDownloadState
from lib.slskd_events import EventIngestResult, ingest_download_file_events
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import (
    make_active_download_file_state,
    make_active_download_state_json,
    make_file_complete_event_data,
)

_FILE_COMPLETE = "DownloadFileComplete"
_DIR_COMPLETE = "DownloadDirectoryComplete"
_OTHER_TYPES = ("SearchRequestResponded", "RoomMessageReceived")

# Small pools force key collisions between rows and events (that's match
# probability, not a plausibility filter — matching is exact-string).
_USERNAMES = ("peer0", "peer1", "péer♪2", "PEER3")
_FILENAMES = (
    "Music\\Artist\\Album\\01 track.flac",
    "Music\\Artist\\Album\\02 track.flac",
    "Music\\Ártîst 音\\Å l b u m\\01.mp3",
    "@@direct\\weird/../path.opus",
    "single.flac",
)

_VALID_OUTCOMES = ("bootstrapped", "ingested", "no_new_events", "empty_feed")


@dataclass(frozen=True)
class FeedEvent:
    """One generated feed event (newest-first position implied by index)."""
    id: str
    timestamp: str
    type: str
    username: str | None
    filename: str | None
    local_filename: str | None
    decodable: bool


@dataclass(frozen=True)
class RequestWorld:
    request_id: int
    file_keys: tuple[tuple[str, str], ...]  # (username, remote filename)
    leaves_before_classification: bool


@dataclass(frozen=True)
class EventWorld:
    rows: tuple[RequestWorld, ...]
    events: tuple[FeedEvent, ...]  # newest-first
    # None = bootstrap (no cursor row). An int in 0..len(events) is the
    # index of the cursor event; len(events) means a synthetic cursor
    # older than the whole feed (its id is absent, timestamp pre-dates
    # every event), i.e. the entire feed is new.
    cursor_index: int | None
    garbage_cursor_timestamp: bool = False
    # T2 (issue #571): a subset of the world's (username, filename) keys
    # pre-ledgered (one open slskd_transfer_ledger row each) BEFORE
    # ingestion runs. Drawn from the same row_keys pool so every key here
    # is also reachable through expected_oracle_stamps' newest-event map.
    ledgered_keys: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class IncarnationEventWorld:
    """One stale-A/fresh-B event window with relative occurrence times."""

    shared_attempt_key: bool
    # -1 = before B, 0 = exactly B, 1 = after B. Tuple order is feed
    # order (newest position first), deliberately independent of time so a
    # newer ineligible entry can precede an older eligible one.
    event_relations: tuple[int, ...]
    replace_before_write: bool
    witness_text: str


class _BeforeStateWriteDB(FakePipelineDB):
    """One-shot replacement seam for generated CAS interleavings."""

    def __init__(self) -> None:
        super().__init__()
        self.before_state_write: Callable[[], None] | None = None
        self.expected_enqueued_at_calls: list[str] = []

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        self.expected_enqueued_at_calls.append(expected_enqueued_at)
        before_state_write = self.before_state_write
        self.before_state_write = None
        if before_state_write is not None:
            before_state_write()
        return super().update_download_state_if_downloading(
            request_id,
            state_json,
            expected_enqueued_at=expected_enqueued_at,
        )


def _timestamp_for(index: int, garbage: bool) -> str:
    if garbage:
        return "not-a-timestamp"
    # Newest-first: larger index = older. Fractions mimic slskd's 7-digit form.
    return f"2026-07-08T11:{59 - index:02d}:00.1234567Z"


@st.composite
def _feed_events(draw, *, row_keys: tuple[tuple[str, str], ...],
                 count: int, unique_ids: bool,
                 allow_garbage_timestamps: bool) -> tuple[FeedEvent, ...]:
    key_pool = list(row_keys) + [
        ("peer-unrelated", "Music\\Other\\01.flac"),
        ("peer0", "Music\\Other\\02.flac"),
    ]
    events: list[FeedEvent] = []
    for i in range(count):
        garbage_ts = allow_garbage_timestamps and draw(
            st.booleans() if i > 0 else st.just(False))
        kind = draw(st.sampled_from(
            (_FILE_COMPLETE,) * 6 + (_DIR_COMPLETE,) + _OTHER_TYPES))
        if kind == _FILE_COMPLETE:
            username, filename = draw(st.sampled_from(key_pool))
            events.append(FeedEvent(
                id=f"ev-{i}",
                timestamp=_timestamp_for(i, garbage_ts),
                type=kind,
                username=username,
                filename=filename,
                local_filename=f"/downloads/complete/{i}",
                decodable=draw(st.booleans() | st.just(True)),
            ))
        else:
            events.append(FeedEvent(
                id=f"ev-{i}",
                timestamp=_timestamp_for(i, garbage_ts),
                type=kind,
                username=None,
                filename=None,
                local_filename=None,
                decodable=False,
            ))
    if not unique_ids and events:
        # Duplicate some events at older positions (mid-pagination shape).
        dup_count = draw(st.integers(min_value=0, max_value=len(events)))
        for _ in range(dup_count):
            source = draw(st.integers(min_value=0, max_value=len(events) - 1))
            events.insert(source + 1, events[source])
    return tuple(events)


@st.composite
def _rows(draw) -> tuple[RequestWorld, ...]:
    row_count = draw(st.integers(min_value=1, max_value=3))
    rows = []
    for rid in range(1, row_count + 1):
        keys = draw(st.lists(
            st.tuples(st.sampled_from(_USERNAMES), st.sampled_from(_FILENAMES)),
            min_size=1, max_size=3, unique=True))
        rows.append(RequestWorld(
            request_id=rid,
            file_keys=tuple(keys),
            leaves_before_classification=draw(st.booleans()),
        ))
    return tuple(rows)


@st.composite
def _ledgered_keys(draw, *, row_keys: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    """A subset of ``row_keys`` to pre-ledger (T2, issue #571). Unique so
    each key gets exactly one open ledger row -- no in-world retries."""
    if not row_keys:
        return ()
    return tuple(draw(st.lists(
        st.sampled_from(row_keys), unique=True,
        max_size=len(set(row_keys)))))


@st.composite
def oracle_worlds(draw) -> EventWorld:
    """Clean-cursor worlds where the expected stamps are computable."""
    rows = draw(_rows())
    row_keys = tuple(k for row in rows for k in row.file_keys)
    count = draw(st.integers(min_value=1, max_value=10))
    events = draw(_feed_events(
        row_keys=row_keys, count=count, unique_ids=True,
        allow_garbage_timestamps=False))
    cursor_index = draw(st.integers(min_value=0, max_value=len(events)))
    ledgered_keys = draw(_ledgered_keys(row_keys=row_keys))
    return EventWorld(
        rows=rows, events=events, cursor_index=cursor_index,
        ledgered_keys=ledgered_keys)


@st.composite
def wild_worlds(draw) -> EventWorld:
    """Anything the feed can throw: dup ids, garbage timestamps, pruned or
    absent cursors, undecodable payloads, empty feeds."""
    rows = draw(_rows())
    row_keys = tuple(k for row in rows for k in row.file_keys)
    count = draw(st.integers(min_value=0, max_value=10))
    events = draw(_feed_events(
        row_keys=row_keys, count=count, unique_ids=draw(st.booleans()),
        allow_garbage_timestamps=True))
    cursor_index = draw(st.one_of(
        st.none(), st.integers(min_value=0, max_value=len(events))))
    ledgered_keys = draw(_ledgered_keys(row_keys=row_keys))
    return EventWorld(
        rows=rows, events=events, cursor_index=cursor_index,
        garbage_cursor_timestamp=draw(st.booleans()),
        ledgered_keys=ledgered_keys,
    )


@st.composite
def incarnation_event_worlds(draw) -> IncarnationEventWorld:
    witness_instant = datetime(
        2026,
        7,
        8,
        10,
        0,
        second=draw(st.sampled_from((0, 1, 30))),
        microsecond=draw(st.sampled_from((0, 125000, 123456))),
        tzinfo=UTC,
    )
    witness_style = draw(st.sampled_from((
        "canonical_offset",
        "z",
        "positive_offset",
        "negative_offset",
        "fractional_z",
        "fractional_offset",
        "malformed",
    )))
    if witness_style == "canonical_offset":
        witness_text = witness_instant.isoformat(timespec="seconds")
    elif witness_style == "z":
        witness_text = (
            witness_instant.isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
    elif witness_style == "positive_offset":
        witness_text = witness_instant.astimezone(
            timezone(timedelta(hours=8)),
        ).isoformat(timespec="microseconds")
    elif witness_style == "negative_offset":
        witness_text = witness_instant.astimezone(
            timezone(-timedelta(hours=5, minutes=30)),
        ).isoformat(timespec="microseconds")
    elif witness_style == "fractional_z":
        witness_text = (
            witness_instant.strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{witness_instant.microsecond:06d}0Z"
        )
    elif witness_style == "fractional_offset":
        witness_text = witness_instant.isoformat(timespec="microseconds")
    else:
        witness_text = "not-an-iso-witness"
    return IncarnationEventWorld(
        shared_attempt_key=draw(st.booleans()),
        event_relations=tuple(draw(st.lists(
            st.sampled_from((-1, 0, 1)),
            min_size=1,
            max_size=4,
        ))),
        replace_before_write=draw(st.booleans()),
        witness_text=witness_text,
    )


def _build_harness(world: EventWorld) -> tuple[FakePipelineDB, FakeSlskdAPI]:
    """Seed fakes from the generated event world."""
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()

    for row in world.rows:
        db.seed_request({
            "id": row.request_id,
            "status": "downloading",
            "artist_name": "Artist",
            "album_title": f"Album {row.request_id}",
            "active_download_state": json.loads(make_active_download_state_json([
                make_active_download_file_state(username=u, filename=f)
                for u, f in row.file_keys
            ])),
        })

    raw_events = []
    for event in world.events:
        if event.type == _FILE_COMPLETE and event.decodable:
            assert event.username is not None and event.filename is not None
            assert event.local_filename is not None
            data = make_file_complete_event_data(
                username=event.username,
                filename=event.filename,
                local_filename=event.local_filename,
                transfer_id=f"transfer-{event.id}",
            )
        else:
            data = "{not-json"
        raw_events.append(slskd.events.make_event(
            id=event.id, timestamp=event.timestamp,
            type=event.type, data=data))
    slskd.events.set_events(raw_events)

    if world.cursor_index is not None:
        if world.cursor_index < len(world.events):
            cursor_event = world.events[world.cursor_index]
            cursor_ts = (
                "also-not-a-timestamp" if world.garbage_cursor_timestamp
                else cursor_event.timestamp)
            db.upsert_slskd_event_cursor(cursor_event.id, cursor_ts)
        else:
            # Synthetic cursor older than the entire feed: id absent,
            # timestamp pre-dates every generated event.
            cursor_ts = (
                "also-not-a-timestamp" if world.garbage_cursor_timestamp
                else "2026-07-08T09:00:00.0000000Z")
            db.upsert_slskd_event_cursor("ev-absent", cursor_ts)

    for row in world.rows:
        if row.leaves_before_classification:
            db.reset_downloading_to_wanted(
                row.request_id,
                expected_status="downloading",
            )

    # T2 (issue #571): seed one open ledger row per pre-ledgered key,
    # AFTER the leaves-before-classification flip above -- the ledger stamp must
    # apply regardless of the owning request's CURRENT status.
    for username, filename in world.ledgered_keys:
        owner = _owning_request_id(world, (username, filename))
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=owner, username=username, filename=filename),
        ])
        db.confirm_transfer_enqueue(username, filename)
    return db, slskd


def _owning_request_id(world: EventWorld, key: tuple[str, str]) -> int:
    """The first row whose file_keys contains ``key`` -- every ledgered
    key is drawn FROM some row's file_keys, so this always resolves."""
    for row in world.rows:
        if key in row.file_keys:
            return row.request_id
    raise AssertionError(f"ledgered key {key!r} owned by no row in {world.rows!r}")


def _stamped_paths(db: FakePipelineDB, world: EventWorld) -> dict:
    """(request_id, key) → local_path for every world file, from the DB."""
    stamped = {}
    for row in world.rows:
        raw_state = db.request(row.request_id)["active_download_state"]
        if raw_state is None:
            # A real downloading -> wanted lifecycle transition clears the
            # active state atomically. Preserve the oracle's key universe
            # while representing every cleared file as unstamped.
            for key in row.file_keys:
                stamped[(row.request_id, key)] = None
            continue
        state = ActiveDownloadState.from_dict(raw_state)
        for file_state in state.files:
            stamped[(row.request_id, (file_state.username, file_state.filename))] = (
                file_state.local_path)
    return stamped


def _newest_event_per_key(world: EventWorld) -> dict[tuple[str, str], str]:
    """Newest decodable DownloadFileComplete event per (username, remote
    filename) key inside the new-events window -- the oracle BOTH
    active_download_state stamping (issue #146) and transfer-ledger
    stamping (T2, issue #571) are measured against."""
    assert world.cursor_index is not None
    window = world.events[:world.cursor_index]
    newest_per_key: dict[tuple[str, str], str] = {}
    for event in window:  # newest-first: first occurrence wins
        if event.type == _FILE_COMPLETE and event.decodable:
            assert event.username is not None and event.filename is not None
            assert event.local_filename is not None
            newest_per_key.setdefault(
                (event.username, event.filename), event.local_filename)
    return newest_per_key


def expected_oracle_stamps(world: EventWorld) -> dict:
    """The invariant: newest decodable file event per key in the new window."""
    newest_per_key = _newest_event_per_key(world)
    expected = {}
    for row in world.rows:
        for key in row.file_keys:
            expected[(row.request_id, key)] = (
                None
                if row.leaves_before_classification
                else newest_per_key.get(key)
            )
    return expected


def expected_ledger_stamps(world: EventWorld) -> dict[tuple[str, str], str | None]:
    """T2 invariant: every pre-ledgered key gets the SAME newest-event
    oracle value, regardless of the owning request's current status
    -- the ledger stamp is independent of active_download_state's
    request-status gate."""
    newest_per_key = _newest_event_per_key(world)
    return {key: newest_per_key.get(key) for key in world.ledgered_keys}


def _owned_local_paths(db: FakePipelineDB, world: EventWorld) -> dict[tuple[str, str], str | None]:
    """Ledgered key -> stamped local_path (None if not yet stamped)."""
    actual: dict[tuple[str, str], str | None] = {key: None for key in world.ledgered_keys}
    for row in db._transfer_ledger.values():
        key = (row.username, row.filename)
        if key in actual:
            actual[key] = row.local_path
    return actual


def _seed_incarnation(
    db: FakePipelineDB,
    *,
    key: tuple[str, str],
    enqueued_at: str,
) -> None:
    db.seed_request({
        "id": 1,
        "status": "downloading",
        "artist_name": "Artist",
        "album_title": "Album",
        "active_download_state": json.loads(ActiveDownloadState(
            filetype="flac",
            enqueued_at=enqueued_at,
            files=[make_active_download_file_state(
                username=key[0],
                filename=key[1],
            )],
        ).to_json()),
    })


def _current_local_path(db: FakePipelineDB) -> str | None:
    state = ActiveDownloadState.from_dict(
        db.request(1)["active_download_state"])
    return state.files[0].local_path


def _current_witness(db: FakePipelineDB) -> str:
    state = ActiveDownloadState.from_dict(
        db.request(1)["active_download_state"])
    return state.enqueued_at


def _parse_witness_oracle(value: str) -> datetime | None:
    """Independent ISO parser for generated expected worlds."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _event_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(
        timespec="microseconds",
    ).replace("+00:00", "Z")


def _build_incarnation_harness(
    world: IncarnationEventWorld,
) -> tuple[
    _BeforeStateWriteDB,
    FakeSlskdAPI,
    tuple[str, str],
]:
    db = _BeforeStateWriteDB()
    slskd = FakeSlskdAPI()
    attempt_a_key = ("peer-a", "A\\01.flac")
    attempt_b_key = (
        attempt_a_key
        if world.shared_attempt_key
        else ("peer-b", "B\\01.flac")
    )
    witness_instant = _parse_witness_oracle(world.witness_text)
    event_reference = witness_instant or datetime(
        2026, 7, 8, 10, 0, tzinfo=UTC)
    _seed_incarnation(
        db,
        key=attempt_a_key,
        enqueued_at=_event_timestamp(event_reference - timedelta(hours=3)),
    )
    _seed_incarnation(
        db,
        key=attempt_b_key,
        enqueued_at=world.witness_text,
    )
    cursor_timestamp = _event_timestamp(
        event_reference - timedelta(hours=2))
    db.upsert_slskd_event_cursor(
        "ev-cursor", cursor_timestamp)
    events = [
        slskd.events.make_event(
            id=f"ev-{index}",
            timestamp=_event_timestamp(
                event_reference + timedelta(hours=relation)),
            type=_FILE_COMPLETE,
            data=make_file_complete_event_data(
                username=attempt_b_key[0],
                filename=attempt_b_key[1],
                local_filename=f"/downloads/incarnation/{index}",
            ),
        )
        for index, relation in enumerate(world.event_relations)
    ]
    events.append(slskd.events.make_event(
        id="ev-cursor",
        timestamp=cursor_timestamp,
        type="Noise",
        data="{}",
    ))
    slskd.events.set_events(events)

    db.record_transfer_enqueue([
        TransferLedgerRow(
            request_id=1,
            username=attempt_b_key[0],
            filename=attempt_b_key[1],
        ),
    ])
    db.confirm_transfer_enqueue(*attempt_b_key)
    return db, slskd, attempt_b_key


def assert_stamps_match(expected: dict, actual: dict) -> None:
    """Stamping-oracle checker (module-level for the known-bad self-test)."""
    if expected.keys() != actual.keys():
        raise AssertionError(
            f"file-key sets diverged: {expected.keys() ^ actual.keys()}")
    diffs = [
        f"{key}: expected={expected[key]!r} actual={actual[key]!r}"
        for key in expected if expected[key] != actual[key]
    ]
    if diffs:
        raise AssertionError(
            "stamped local paths diverged from the event oracle:\n  "
            + "\n  ".join(diffs))


def assert_ledger_stamps_match(expected: dict, actual: dict) -> None:
    """T2 checker (module-level for the known-bad self-test)."""
    if expected.keys() != actual.keys():
        raise AssertionError(
            f"ledgered-key sets diverged: {expected.keys() ^ actual.keys()}")
    diffs = [
        f"{key}: expected={expected[key]!r} actual={actual[key]!r}"
        for key in expected if expected[key] != actual[key]
    ]
    if diffs:
        raise AssertionError(
            "ledger-stamped local paths diverged from the event oracle:\n  "
            + "\n  ".join(diffs))


def assert_result_well_formed(result: EventIngestResult) -> None:
    if result.outcome not in _VALID_OUTCOMES:
        raise AssertionError(f"unknown ingest outcome: {result.outcome!r}")
    for field in ("events_seen", "file_events", "files_stamped",
                  "requests_updated", "transfers_stamped"):
        if getattr(result, field) < 0:
            raise AssertionError(f"negative counter {field}: {result!r}")
    if result.cursor_advanced and result.cursor_hold_reason is not None:
        raise AssertionError(
            f"advanced cursor cannot also report a hold: {result!r}")
    if result.cursor_hold_reason is not None and result.cursor_gap:
        raise AssertionError(
            f"cursor-gap fail-open cannot claim a replay hold: {result!r}")


def assert_incarnation_window(
    *,
    expected_path: str | None,
    actual_path: str | None,
    expected_cursor_advanced: bool,
    result: EventIngestResult,
) -> None:
    """Issue #898 U3 oracle checker, kept independent of production logic."""
    if actual_path != expected_path:
        raise AssertionError(
            "current-incarnation stamp diverged: "
            f"expected={expected_path!r} actual={actual_path!r}")
    if result.cursor_advanced != expected_cursor_advanced:
        raise AssertionError(
            "cursor commit-marker divergence: "
            f"expected={expected_cursor_advanced!r} "
            f"actual={result.cursor_advanced!r}")
    if expected_cursor_advanced and result.cursor_hold_reason is not None:
        raise AssertionError(
            f"advanced cursor unexpectedly held: {result.cursor_hold_reason!r}")
    if (
        not expected_cursor_advanced
        and result.cursor_hold_reason != "lost_current_incarnation_write"
    ):
        raise AssertionError(
            "lost dirty write lacked the safe hold reason: "
            f"{result.cursor_hold_reason!r}")


class TestGeneratedEventStamping(unittest.TestCase):
    """Property 1: the stamping oracle on clean-cursor worlds."""

    @given(world=oracle_worlds())
    def test_stamps_match_newest_decodable_event_in_window(self, world):
        db, slskd = _build_harness(world)
        result = ingest_download_file_events(db, slskd)

        assert_result_well_formed(result)
        expected = expected_oracle_stamps(world)
        assert_stamps_match(expected, _stamped_paths(db, world))

        # T2 (issue #571): ledgered keys follow the SAME oracle,
        # independent of whether the owner left downloading.
        expected_ledger = expected_ledger_stamps(world)
        assert_ledger_stamps_match(expected_ledger, _owned_local_paths(db, world))
        self.assertEqual(
            result.transfers_stamped,
            sum(1 for path in expected_ledger.values() if path is not None))

        window = world.events[:world.cursor_index]
        self.assertEqual(
            result.outcome, "ingested" if window else "no_new_events")
        self.assertEqual(result.events_seen, len(window))
        self.assertEqual(
            result.file_events,
            sum(1 for e in window if e.type == _FILE_COMPLETE))
        self.assertEqual(
            result.files_stamped,
            sum(1 for path in expected.values() if path is not None))
        self.assertEqual(
            result.requests_updated,
            sum(
                1 for row in world.rows
                if not row.leaves_before_classification and any(
                    expected[(row.request_id, key)] is not None
                    for key in row.file_keys)
            ))

        cursor = db.get_slskd_event_cursor()
        assert cursor is not None
        if window:
            self.assertEqual(cursor["last_event_id"], world.events[0].id)


class TestGeneratedIncarnationEventStamping(unittest.TestCase):
    """Issue #898 U3: time-qualified fresh classification and replay."""

    @given(world=incarnation_event_worlds())
    @example(world=IncarnationEventWorld(
        shared_attempt_key=True,
        event_relations=(-1, 0),
        replace_before_write=False,
        witness_text="2026-07-08T18:00:00+08:00",
    ))
    @example(world=IncarnationEventWorld(
        shared_attempt_key=False,
        event_relations=(0,),
        replace_before_write=False,
        witness_text="2026-07-08T10:00:00.0000000Z",
    ))
    @example(world=IncarnationEventWorld(
        shared_attempt_key=False,
        event_relations=(0,),
        replace_before_write=False,
        witness_text="2026-07-08T10:00:00+00:00",
    ))
    @example(world=IncarnationEventWorld(
        shared_attempt_key=True,
        event_relations=(1,),
        replace_before_write=False,
        witness_text="2026-07-08T10:00:00Z",
    ))
    @example(world=IncarnationEventWorld(
        shared_attempt_key=True,
        event_relations=(1,),
        replace_before_write=True,
        witness_text="2026-07-08T04:30:00-05:30",
    ))
    @example(world=IncarnationEventWorld(
        shared_attempt_key=False,
        event_relations=(0, 1),
        replace_before_write=True,
        witness_text="not-an-iso-witness",
    ))
    def test_current_incarnation_oracle_and_replay(
        self,
        world: IncarnationEventWorld,
    ) -> None:
        db, slskd, attempt_b_key = (
            _build_incarnation_harness(world)
        )
        witness_instant = _parse_witness_oracle(world.witness_text)
        valid_witness = witness_instant is not None
        self.assertEqual(_current_witness(db), world.witness_text)
        first_eligible_b = next(
            (
                index
                for index, relation in enumerate(world.event_relations)
                if valid_witness and relation >= 0
            ),
            None,
        )
        loses_dirty_write = (
            world.replace_before_write and first_eligible_b is not None
        )
        replacement_witness: str | None = None
        if loses_dirty_write:
            assert witness_instant is not None
            replacement_witness = _event_timestamp(
                witness_instant + timedelta(minutes=30))
            db.before_state_write = lambda: _seed_incarnation(
                db,
                key=attempt_b_key,
                enqueued_at=replacement_witness,
            )

        if loses_dirty_write or not valid_witness:
            with self.assertLogs("cratedigger", level="WARNING"):
                first = ingest_download_file_events(
                    db, slskd)
        else:
            first = ingest_download_file_events(db, slskd)

        assert_result_well_formed(first)
        expected_first_path = (
            None
            if first_eligible_b is None or loses_dirty_write
            else f"/downloads/incarnation/{first_eligible_b}"
        )
        assert_incarnation_window(
            expected_path=expected_first_path,
            actual_path=_current_local_path(db),
            expected_cursor_advanced=not loses_dirty_write,
            result=first,
        )
        self.assertEqual(
            db.expected_enqueued_at_calls,
            [world.witness_text] if first_eligible_b is not None else [],
        )
        self.assertEqual(
            _current_witness(db),
            replacement_witness if loses_dirty_write else world.witness_text,
        )
        self.assertEqual(first.transfers_stamped, 1)
        ledger_rows = list(db._transfer_ledger.values())
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(
            ledger_rows[0].local_path,
            "/downloads/incarnation/0",
        )

        second = ingest_download_file_events(db, slskd)

        assert_result_well_formed(second)
        self.assertEqual(second.transfers_stamped, 0)
        if loses_dirty_write:
            assert replacement_witness is not None
            first_eligible_c = next(
                (
                    index
                    for index, relation in enumerate(world.event_relations)
                    if relation > 0
                ),
                None,
            )
            expected_replay_path = (
                None
                if first_eligible_c is None
                else f"/downloads/incarnation/{first_eligible_c}"
            )
            assert_incarnation_window(
                expected_path=expected_replay_path,
                actual_path=_current_local_path(db),
                expected_cursor_advanced=True,
                result=second,
            )
            expected_calls = [world.witness_text]
            if first_eligible_c is not None:
                expected_calls.append(replacement_witness)
            self.assertEqual(
                db.expected_enqueued_at_calls,
                expected_calls,
            )
            self.assertEqual(
                _current_witness(db),
                replacement_witness,
            )
        else:
            self.assertEqual(second.outcome, "no_new_events")
            self.assertEqual(_current_local_path(db), expected_first_path)
            self.assertEqual(_current_witness(db), world.witness_text)
        self.assertEqual(
            ledger_rows[0].local_path,
            "/downloads/incarnation/0",
        )


class TestGeneratedEventIngestTotality(unittest.TestCase):
    """Property 2: totality + exactly-once on arbitrary worlds."""

    @given(world=wild_worlds())
    def test_ingest_never_crashes_and_second_pass_is_noop(self, world):
        db, slskd = _build_harness(world)
        generated_paths = {
            e.local_filename for e in world.events
            if e.local_filename is not None}

        first = ingest_download_file_events(db, slskd)
        assert_result_well_formed(first)

        stamped_after_first = _stamped_paths(db, world)
        for path in stamped_after_first.values():
            if path is not None and path not in generated_paths:
                raise AssertionError(
                    f"stamped path {path!r} does not originate from the feed")

        # T2: same totality property for the ledger.
        ledger_after_first = _owned_local_paths(db, world)
        for path in ledger_after_first.values():
            if path is not None and path not in generated_paths:
                raise AssertionError(
                    f"ledger-stamped path {path!r} does not originate "
                    "from the feed")
        cursor_after_first = db.get_slskd_event_cursor()

        second = ingest_download_file_events(db, slskd)
        assert_result_well_formed(second)
        self.assertIn(second.outcome, ("no_new_events", "empty_feed"))
        self.assertEqual(stamped_after_first, _stamped_paths(db, world))
        self.assertEqual(ledger_after_first, _owned_local_paths(db, world))
        self.assertEqual(cursor_after_first, db.get_slskd_event_cursor())


class TestGeneratedEventDuplicateInvariance(unittest.TestCase):
    """Property 3: duplicated events (mid-pagination shape) change nothing."""

    @given(world=oracle_worlds(), data=st.data())
    def test_duplicate_ids_are_invariant(self, world, data):
        events = list(world.events)
        if events:
            dup_count = data.draw(
                st.integers(min_value=1, max_value=len(events)),
                label="dup_count")
            for _ in range(dup_count):
                source = data.draw(
                    st.integers(min_value=0, max_value=len(events) - 1),
                    label="dup_source")
                events.insert(source + 1, events[source])
        # The cursor event's id now locates the scan stop; recompute its
        # index so the duplicated world is the SAME world description.
        if world.cursor_index is not None and world.cursor_index < len(world.events):
            cursor_id = world.events[world.cursor_index].id
            new_cursor_index = next(
                i for i, e in enumerate(events) if e.id == cursor_id)
        else:
            new_cursor_index = len(events)
        dup_world = EventWorld(
            rows=world.rows, events=tuple(events),
            cursor_index=new_cursor_index, ledgered_keys=world.ledgered_keys)

        base_db, base_slskd = _build_harness(world)
        base_result = ingest_download_file_events(base_db, base_slskd)

        dup_db, dup_slskd = _build_harness(dup_world)
        dup_result = ingest_download_file_events(dup_db, dup_slskd)

        self.assertEqual(
            _stamped_paths(base_db, world), _stamped_paths(dup_db, dup_world))
        self.assertEqual(
            _owned_local_paths(base_db, world),
            _owned_local_paths(dup_db, dup_world))
        self.assertEqual(base_result.outcome, dup_result.outcome)
        self.assertEqual(base_result.files_stamped, dup_result.files_stamped)
        self.assertEqual(
            base_result.requests_updated, dup_result.requests_updated)
        self.assertEqual(base_result.events_seen, dup_result.events_seen)
        self.assertEqual(
            base_result.transfers_stamped, dup_result.transfers_stamped)


class TestEventCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests for the event-ingest checkers."""

    def test_stamp_checker_trips_on_wrong_path(self):
        key = (1, ("peer0", "single.flac"))
        with self.assertRaises(AssertionError):
            assert_stamps_match({key: "/downloads/complete/0"}, {key: None})

    def test_stamp_checker_trips_on_invented_stamp(self):
        key = (1, ("peer0", "single.flac"))
        with self.assertRaises(AssertionError):
            assert_stamps_match({key: None}, {key: "/invented/path"})

    def test_result_checker_trips_on_unknown_outcome(self):
        with self.assertRaises(AssertionError):
            assert_result_well_formed(EventIngestResult(outcome="exploded"))

    def test_ledger_stamp_checker_trips_on_wrong_path(self):
        key = ("peer0", "single.flac")
        with self.assertRaises(AssertionError):
            assert_ledger_stamps_match(
                {key: "/downloads/complete/0"}, {key: None})

    def test_ledger_stamp_checker_trips_on_invented_stamp(self):
        key = ("peer0", "single.flac")
        with self.assertRaises(AssertionError):
            assert_ledger_stamps_match({key: None}, {key: "/invented/path"})

    def test_result_checker_trips_on_negative_transfers_stamped(self):
        with self.assertRaises(AssertionError):
            assert_result_well_formed(
                EventIngestResult(outcome="ingested", transfers_stamped=-1))

    def test_incarnation_checker_trips_on_stale_attempt_stamp(self):
        with self.assertRaises(AssertionError):
            assert_incarnation_window(
                expected_path="/downloads/current-b",
                actual_path="/downloads/stale-a",
                expected_cursor_advanced=True,
                result=EventIngestResult(
                    outcome="ingested",
                    cursor_advanced=True,
                ),
            )

    def test_incarnation_checker_trips_on_unconditional_cursor_advance(self):
        with self.assertRaises(AssertionError):
            assert_incarnation_window(
                expected_path=None,
                actual_path=None,
                expected_cursor_advanced=False,
                result=EventIngestResult(
                    outcome="ingested",
                    cursor_advanced=True,
                ),
            )

    def test_result_checker_trips_on_advance_and_hold(self):
        with self.assertRaises(AssertionError):
            assert_result_well_formed(EventIngestResult(
                outcome="ingested",
                cursor_advanced=True,
                cursor_hold_reason="lost_current_incarnation_write",
            ))


if __name__ == "__main__":
    unittest.main()
