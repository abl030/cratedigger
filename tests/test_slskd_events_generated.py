#!/usr/bin/env python3
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
   writes to rows that left ``downloading`` mid-ingest. The SAME test
   also covers T2 (issue #571): a subset of world keys are pre-ledgered
   (``slskd_transfer_ledger`` rows, migration 045) and must be stamped
   with the SAME newest-event oracle, in the SAME pass — regardless of
   whether the owning request left 'downloading' mid-ingest (the ledger
   stamp is independent of active_download_state's request-status gate).
2. **Totality + exactly-once** — for arbitrary worlds (duplicate event
   ids, garbage timestamps, undecodable payloads, pruned cursors,
   bootstrap): ingestion never raises, every stamped path (both
   active_download_state AND the ledger) originates from the feed, and
   an immediate second pass over the unchanged feed is a no-op
   (``no_new_events``/``empty_feed``, identical states and cursor).
3. **Duplicate-id invariance** — a feed with duplicated events (the
   mid-pagination offset-shift shape) produces exactly the same outcome
   as the same feed deduplicated.

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
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

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
    leaves_mid_ingest: bool


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
            leaves_mid_ingest=draw(st.booleans()),
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


def _build_harness(world: EventWorld) -> tuple[FakePipelineDB, FakeSlskdAPI, list]:
    """Seed fakes from the world; returns (db, slskd, prefetched rows)."""
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

    downloading = db.get_downloading()
    for row in world.rows:
        if row.leaves_mid_ingest:
            db.reset_downloading_to_wanted(
                row.request_id,
                expected_status="downloading",
            )

    # T2 (issue #571): seed one open ledger row per pre-ledgered key,
    # AFTER the leaves_mid_ingest flip above -- the ledger stamp must
    # apply regardless of the owning request's CURRENT status.
    for username, filename in world.ledgered_keys:
        owner = _owning_request_id(world, (username, filename))
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=owner, username=username, filename=filename),
        ])
    return db, slskd, downloading


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
                None if row.leaves_mid_ingest else newest_per_key.get(key))
    return expected


def expected_ledger_stamps(world: EventWorld) -> dict[tuple[str, str], str | None]:
    """T2 invariant: every pre-ledgered key gets the SAME newest-event
    oracle value, regardless of the owning request's leaves_mid_ingest
    status -- the ledger stamp is independent of
    active_download_state's request-status gate."""
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


class TestGeneratedEventStamping(unittest.TestCase):
    """Property 1: the stamping oracle on clean-cursor worlds."""

    @given(world=oracle_worlds())
    def test_stamps_match_newest_decodable_event_in_window(self, world):
        db, slskd, downloading = _build_harness(world)
        result = ingest_download_file_events(db, slskd, downloading)

        assert_result_well_formed(result)
        expected = expected_oracle_stamps(world)
        assert_stamps_match(expected, _stamped_paths(db, world))

        # T2 (issue #571): ledgered keys follow the SAME oracle,
        # independent of leaves_mid_ingest.
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
                if not row.leaves_mid_ingest and any(
                    expected[(row.request_id, key)] is not None
                    for key in row.file_keys)
            ))

        cursor = db.get_slskd_event_cursor()
        assert cursor is not None
        if window:
            self.assertEqual(cursor["last_event_id"], world.events[0].id)


class TestGeneratedEventIngestTotality(unittest.TestCase):
    """Property 2: totality + exactly-once on arbitrary worlds."""

    @given(world=wild_worlds())
    def test_ingest_never_crashes_and_second_pass_is_noop(self, world):
        db, slskd, downloading = _build_harness(world)
        generated_paths = {
            e.local_filename for e in world.events
            if e.local_filename is not None}

        first = ingest_download_file_events(db, slskd, downloading)
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

        second = ingest_download_file_events(
            db, slskd, db.get_downloading())
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

        base_db, base_slskd, base_rows = _build_harness(world)
        base_result = ingest_download_file_events(
            base_db, base_slskd, base_rows)

        dup_db, dup_slskd, dup_rows = _build_harness(dup_world)
        dup_result = ingest_download_file_events(dup_db, dup_slskd, dup_rows)

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


if __name__ == "__main__":
    unittest.main()
