"""Generated slskd event-ingestion tests — issue #548 follow-up, extended
for issue #571 T2.

Property-based tests over ``lib/slskd_events.py::ingest_download_file_events``
— the pass that stamps authoritative completed-file locations from the
slskd events feed (the stamp is the ONLY source of completed-file
locations; issue #146).

Four properties over generated feed histories:

1. **Stamping oracle** — for worlds with a clean cursor (its id present in
   the feed, unique event ids), every downloading file whose queue key the
   ledger proves is OURS ends up stamped with exactly the newest decodable
   DownloadFileComplete event for its ``(username, remote filename)`` key
   inside the new-events window — and nothing else: no invented paths, no
   stamps from behind the cursor, no writes to rows that left
   ``downloading`` before fresh classification, and no stamp at all on a
   key that was never accepted (issue #1278 item 1).
   The SAME test also covers T2 (issue #571): a subset of world keys are
   pre-ledgered (``slskd_transfer_ledger`` rows, migration 045) and must be
   stamped with the SAME newest-event oracle, in the SAME pass — regardless
   of whether the owning request left ``downloading`` before fresh
   classification (the ledger stamp is independent of
   active_download_state's request-status GATE ON REQUEST STATUS; it shares
   active stamping's ownership gate).
2. **Totality + exactly-once** — for arbitrary worlds (duplicate event
   ids, garbage timestamps, undecodable payloads, pruned cursors,
   bootstrap): ingestion never raises, every stamped path (both
   active_download_state AND the ledger) originates from the feed, neither
   stamp reaches a key the ledger's accepted-POST rule refuses, and
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

Per-clause proof (issue #1094): every checker clause here has a named
world in ``TestEventCheckersTripOnViolations`` asserting that clause's own
message exactly (anchored for the raising checkers, by list equality for the
accumulating ones), plus a production mutant killed at the gating (``suite``)
tier. Three clauses are deliberately kept as fail-closed legislation with a
self-test but no reachable production world: ``assert_ledger_stamps_match``'s
key-set clause (expected and actual are both projected from the world's
``ledgered_keys``, so divergence is unreachable by construction),
``assert_result_well_formed``'s cursor-gap-plus-hold clause (production
couples the two, and the page-cap world needs a >10k-event feed — pinned by
``tests/test_slskd_events.py::TestIncarnationAwareStamping::
test_cursor_gap_fail_open_advances_despite_lost_dirty_write``), and
``ownership_agreement_violations``'s ledger clause (see that function's own
docstring: ``stamp_transfer_completion`` writes a path only onto an already
accepted row, so no mutant in ``lib/slskd_events.py`` can separate the two).
Note also
that a request leaving ``downloading`` is excluded here by
``get_downloading()``, not by the CAS status predicate: dropping that
predicate from the DB layer changes nothing in these worlds, so this module
does not patrol an interleaved status flip.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import json
import os
import re
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
        db.confirm_transfer_enqueue(
            username, filename, request_id=owner)
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
    """The invariant: newest decodable file event per key in the new window,
    for a key the ledger proves is OURS.

    The ownership term is issue #1278 item 1. The events feed is
    instance-wide, so a ``(username, filename)`` match is not evidence the
    completed bytes are ours -- only an accepted POST is, which is exactly
    what the ledger's own stamp already required in this same pass. A key
    outside ``world.ledgered_keys`` was never accepted, so it stamps
    nothing on either surface.
    """
    newest_per_key = _newest_event_per_key(world)
    owned = set(world.ledgered_keys)
    expected = {}
    for row in world.rows:
        for key in row.file_keys:
            expected[(row.request_id, key)] = (
                None
                if row.leaves_before_classification or key not in owned
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


def _active_stamped_keys(stamped: dict) -> set[tuple[str, str]]:
    """The ``(username, filename)`` keys this pass wrote a path onto.

    Every generated world starts with every ``local_path`` unset, so a
    non-``None`` value after the pass is this pass's own write.
    """
    return {key for (_request_id, key), path in stamped.items() if path is not None}


def _ledger_stamped_keys(db: FakePipelineDB) -> set[tuple[str, str]]:
    """Every ledger row carrying a completion path, keyed by queue key.

    Reads the WHOLE ledger rather than the world's ``ledgered_keys``, so a
    row the pass invented would be visible here instead of filtered out.
    """
    return {
        (row.username, row.filename)
        for row in db._transfer_ledger.values()
        if row.local_path is not None
    }


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
    db.confirm_transfer_enqueue(*attempt_b_key, request_id=1)
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


def feed_origin_violations(
    *,
    stamped: dict,
    ledger: dict,
    feed_paths: set[str],
) -> list[str]:
    """Totality checker: no stamp invents a path (issue #571 good citizen).

    Accumulating (``list[str]``) so the active-state clause cannot mask the
    ledger clause: an ingest pass that invents BOTH must report both, and
    each clause carries its own message for the known-bad self-test.
    """
    violations = []
    for path in stamped.values():
        if path is not None and path not in feed_paths:
            violations.append(
                f"stamped path {path!r} does not originate from the feed")
    for path in ledger.values():
        if path is not None and path not in feed_paths:
            violations.append(
                f"ledger-stamped path {path!r} does not originate "
                "from the feed")
    return violations


def ownership_agreement_violations(
    *,
    owned_keys: set[tuple[str, str]],
    active_stamped_keys: set[tuple[str, str]],
    ledger_stamped_keys: set[tuple[str, str]],
) -> list[str]:
    """One ingestion pass, one ownership rule (issue #1278 item 1).

    ``_stamp_local_paths`` and ``_stamp_transfer_ledger`` run in the same
    pass off the same decoded events. Neither may stamp a queue key the
    other's ownership rule refuses: only an accepted POST
    (``accepted_at IS NOT NULL``) proves the completed bytes at an
    instance-wide ``(username, filename)`` are ours, and ``owned_keys`` is
    the ledger's own answer to that question, read after the pass.

    Accumulating (``list[str]``) so the active clause cannot mask the
    ledger clause and each carries its own message for the known-bad
    self-test.

    **Reachability, honestly.** The active clause is reachable: deleting
    the ``owned_keys`` gate in ``_stamp_local_paths`` fires it. The ledger
    clause is fail-closed legislation with a self-test but no reachable
    production world -- ``stamp_transfer_completion`` writes ``local_path``
    only onto a row that already carries ``accepted_at``, so a
    ledger-stamped key is an owned key by construction of the DB method,
    and no mutant inside ``lib/slskd_events.py`` can separate them. It
    legislates for a future writer that stamps the ledger some other way.
    """
    violations = []
    for key in sorted(active_stamped_keys - owned_keys):
        violations.append(
            f"active_download_state stamped {key!r}, which the transfer "
            "ledger does not prove is ours")
    for key in sorted(ledger_stamped_keys - owned_keys):
        violations.append(
            f"the transfer ledger stamped {key!r}, which its own "
            "accepted-POST rule does not prove is ours")
    return violations


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

    # Per-clause audit (issue #1094): the gating tier drew ZERO worlds in
    # which any STAMPED key carried two decodable completions, so "newest
    # decodable event wins" — the whole point of the oracle — had no
    # decisive example. A production mutant selecting the OLDEST candidate
    # survived this property entirely. This world pins four decisive arms:
    # newest-wins (key A), newest-DECODABLE-wins over a newer undecodable
    # payload (key B), no active stamp for a request that left downloading
    # (key C), and a ledger stamp for that same left-downloading request
    # (T2, issue #571 — the ledger is independent of the status gate).
    @example(world=EventWorld(
        rows=(
            RequestWorld(
                request_id=1,
                file_keys=(
                    ("peer0", "single.flac"),
                    ("peer1", "Music\\Artist\\Album\\01 track.flac"),
                ),
                leaves_before_classification=False,
            ),
            RequestWorld(
                request_id=2,
                file_keys=(("PEER3", "Music\\Ártîst 音\\Å l b u m\\01.mp3"),),
                leaves_before_classification=True,
            ),
        ),
        events=(
            FeedEvent(
                id="ev-0", timestamp=_timestamp_for(0, False),
                type=_FILE_COMPLETE, username="peer1",
                filename="Music\\Artist\\Album\\01 track.flac",
                local_filename="/downloads/complete/0", decodable=False),
            FeedEvent(
                id="ev-1", timestamp=_timestamp_for(1, False),
                type=_FILE_COMPLETE, username="peer0",
                filename="single.flac",
                local_filename="/downloads/complete/1", decodable=True),
            FeedEvent(
                id="ev-2", timestamp=_timestamp_for(2, False),
                type=_FILE_COMPLETE, username="peer1",
                filename="Music\\Artist\\Album\\01 track.flac",
                local_filename="/downloads/complete/2", decodable=True),
            FeedEvent(
                id="ev-3", timestamp=_timestamp_for(3, False),
                type=_FILE_COMPLETE, username="peer0",
                filename="single.flac",
                local_filename="/downloads/complete/3", decodable=True),
            FeedEvent(
                id="ev-4", timestamp=_timestamp_for(4, False),
                type=_FILE_COMPLETE, username="PEER3",
                filename="Music\\Ártîst 音\\Å l b u m\\01.mp3",
                local_filename="/downloads/complete/4", decodable=True),
        ),
        cursor_index=5,
        ledgered_keys=(
            ("peer0", "single.flac"),
            ("PEER3", "Music\\Ártîst 音\\Å l b u m\\01.mp3"),
        ),
    ))
    @given(world=oracle_worlds())
    def test_stamps_match_newest_decodable_event_in_window(self, world):
        db, slskd = _build_harness(world)
        result = ingest_download_file_events(db, slskd)

        assert_result_well_formed(result)
        expected = expected_oracle_stamps(world)
        stamped = _stamped_paths(db, world)
        assert_stamps_match(expected, stamped)

        # #1278 item 1: neither stamp may reach a key the ledger's own
        # accepted-POST rule refuses.
        self.assertEqual(
            ownership_agreement_violations(
                owned_keys=db.get_owned_transfer_keys(),
                active_stamped_keys=_active_stamped_keys(stamped),
                ledger_stamped_keys=_ledger_stamped_keys(db)),
            [])

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

        expected_first_path = (
            None
            if first_eligible_b is None or loses_dirty_write
            else f"/downloads/incarnation/{first_eligible_b}"
        )
        # Checker order is attribution (issue #1094): assert_result_well_formed
        # rejects advance-plus-hold generally, so running it first made
        # assert_incarnation_window's "advanced cursor unexpectedly held"
        # clause unreachable — every mutant that could fire it died on the
        # earlier checker's message. The window checker legislates this
        # window's commit marker, so it judges first; the general result
        # shape is still asserted immediately after, and still owns the
        # clause on every other property.
        assert_incarnation_window(
            expected_path=expected_first_path,
            actual_path=_current_local_path(db),
            expected_cursor_advanced=not loses_dirty_write,
            result=first,
        )
        assert_result_well_formed(first)
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

    # Per-clause audit (issue #1094): 105 of the gating tier's 150 wild
    # worlds bootstrap (no cursor row) and stamp nothing at all, leaving
    # only ~4 examples in which either feed-origin clause has a non-None
    # path to judge. This world always stamps both surfaces, so an
    # invented-path mutant cannot survive on entropy.
    @example(world=EventWorld(
        rows=(RequestWorld(
            request_id=1,
            file_keys=(("peer0", "single.flac"),),
            leaves_before_classification=False),),
        events=(FeedEvent(
            id="ev-0", timestamp=_timestamp_for(0, False),
            type=_FILE_COMPLETE, username="peer0", filename="single.flac",
            local_filename="/downloads/complete/0", decodable=True),),
        cursor_index=1,
        ledgered_keys=(("peer0", "single.flac"),),
    ))
    @given(world=wild_worlds())
    def test_ingest_never_crashes_and_second_pass_is_noop(self, world):
        db, slskd = _build_harness(world)
        generated_paths = {
            e.local_filename for e in world.events
            if e.local_filename is not None}

        first = ingest_download_file_events(db, slskd)
        assert_result_well_formed(first)

        stamped_after_first = _stamped_paths(db, world)
        # T2: the same totality clause covers the ledger, accumulated so
        # neither stamp surface can mask the other.
        ledger_after_first = _owned_local_paths(db, world)
        self.assertEqual(
            feed_origin_violations(
                stamped=stamped_after_first,
                ledger=ledger_after_first,
                feed_paths=generated_paths),
            [])
        # #1278 item 1: the same ownership rule, patrolled where no oracle
        # exists (garbage timestamps, pruned cursors, bootstrap worlds).
        self.assertEqual(
            ownership_agreement_violations(
                owned_keys=db.get_owned_transfer_keys(),
                active_stamped_keys=_active_stamped_keys(stamped_after_first),
                ledger_stamped_keys=_ledger_stamped_keys(db)),
            [])
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


_KEY_A = (1, ("peer0", "single.flac"))
_KEY_B = (2, ("peer1", "02 track.flac"))
_LEDGER_A = ("peer0", "single.flac")
_LEDGER_B = ("peer1", "02 track.flac")


def _exact(message: str) -> str:
    """Whole-message anchor — the clause's message is fully determined."""
    return "^" + re.escape(message) + "$"


def _prefix(message: str) -> str:
    """Head anchor — the clause's tail carries a generated repr."""
    return "^" + re.escape(message)


class TestEventCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: one anchored world per checker CLAUSE.

    Issue #1094 per-clause proof. Every case makes exactly one clause's
    condition true while every EARLIER clause in the same checker passes,
    and asserts that clause's OWN message anchored. The anchoring is
    load-bearing rather than decorative here: ``ledger-stamped local paths
    diverged from the event oracle`` contains the active-state clause's
    entire message, and ``ledger-stamped path ... does not originate from
    the feed`` contains the active-state feed-origin clause's, so an
    unanchored substring assertion for either active-state clause is
    satisfied by its ledger sibling.
    """

    def test_stamp_checker_clauses(self):
        cases = [
            (
                "C1 key set diverged — a file left active_download_state",
                {_KEY_A: None, _KEY_B: None},
                {_KEY_A: None},
                _exact("file-key sets diverged: "
                       "{(2, ('peer1', '02 track.flac'))}"),
            ),
            (
                "C2 expected stamp missing",
                {_KEY_A: "/downloads/complete/0"},
                {_KEY_A: None},
                _exact(
                    "stamped local paths diverged from the event oracle:\n"
                    "  (1, ('peer0', 'single.flac')): "
                    "expected='/downloads/complete/0' actual=None"),
            ),
            (
                "C2 invented stamp",
                {_KEY_A: None},
                {_KEY_A: "/invented/path"},
                _exact(
                    "stamped local paths diverged from the event oracle:\n"
                    "  (1, ('peer0', 'single.flac')): "
                    "expected=None actual='/invented/path'"),
            ),
        ]
        for clause, expected, actual, pattern in cases:
            with (self.subTest(clause=clause),
                  self.assertRaisesRegex(AssertionError, pattern)):
                assert_stamps_match(expected, actual)

    def test_ledger_stamp_checker_clauses(self):
        cases = [
            (
                "C3 ledgered-key set diverged",
                {_LEDGER_A: None, _LEDGER_B: None},
                {_LEDGER_A: None},
                _exact("ledgered-key sets diverged: "
                       "{('peer1', '02 track.flac')}"),
            ),
            (
                "C4 expected ledger stamp missing",
                {_LEDGER_A: "/downloads/complete/0"},
                {_LEDGER_A: None},
                _exact(
                    "ledger-stamped local paths diverged from the event "
                    "oracle:\n  ('peer0', 'single.flac'): "
                    "expected='/downloads/complete/0' actual=None"),
            ),
            (
                "C4 invented ledger stamp",
                {_LEDGER_A: None},
                {_LEDGER_A: "/invented/path"},
                _exact(
                    "ledger-stamped local paths diverged from the event "
                    "oracle:\n  ('peer0', 'single.flac'): "
                    "expected=None actual='/invented/path'"),
            ),
        ]
        for clause, expected, actual, pattern in cases:
            with (self.subTest(clause=clause),
                  self.assertRaisesRegex(AssertionError, pattern)):
                assert_ledger_stamps_match(expected, actual)

    def test_result_checker_clauses(self):
        cases = [
            (
                "C5 unknown outcome",
                EventIngestResult(outcome="exploded"),
                _exact("unknown ingest outcome: 'exploded'"),
            ),
            # One raise site, five clauses: the loop stands in for every
            # counter, so each field owes its own world (issue #1094).
            (
                "C6 negative events_seen",
                EventIngestResult(outcome="ingested", events_seen=-1),
                _prefix("negative counter events_seen: EventIngestResult("),
            ),
            (
                "C7 negative file_events",
                EventIngestResult(outcome="ingested", file_events=-1),
                _prefix("negative counter file_events: EventIngestResult("),
            ),
            (
                "C8 negative files_stamped",
                EventIngestResult(outcome="ingested", files_stamped=-1),
                _prefix("negative counter files_stamped: EventIngestResult("),
            ),
            (
                "C9 negative requests_updated",
                EventIngestResult(outcome="ingested", requests_updated=-1),
                _prefix(
                    "negative counter requests_updated: EventIngestResult("),
            ),
            (
                "C10 negative transfers_stamped",
                EventIngestResult(outcome="ingested", transfers_stamped=-1),
                _prefix(
                    "negative counter transfers_stamped: EventIngestResult("),
            ),
            (
                "C11 advanced cursor also reports a hold",
                EventIngestResult(
                    outcome="ingested",
                    cursor_advanced=True,
                    cursor_hold_reason="lost_current_incarnation_write"),
                _prefix("advanced cursor cannot also report a hold: "
                        "EventIngestResult("),
            ),
            (
                # cursor_advanced=False so the advance/hold clause above
                # passes and this clause is the one that fires.
                "C12 cursor-gap fail-open claims a replay hold",
                EventIngestResult(
                    outcome="ingested",
                    cursor_gap=True,
                    cursor_advanced=False,
                    cursor_hold_reason="lost_current_incarnation_write"),
                _prefix("cursor-gap fail-open cannot claim a replay hold: "
                        "EventIngestResult("),
            ),
        ]
        for clause, result, pattern in cases:
            with (self.subTest(clause=clause),
                  self.assertRaisesRegex(AssertionError, pattern)):
                assert_result_well_formed(result)

    def test_incarnation_checker_clauses(self):
        cases = [
            (
                "C13 stale-attempt stamp",
                "/downloads/current-b",
                "/downloads/stale-a",
                True,
                EventIngestResult(outcome="ingested", cursor_advanced=True),
                _exact("current-incarnation stamp diverged: "
                       "expected='/downloads/current-b' "
                       "actual='/downloads/stale-a'"),
            ),
            (
                "C14 cursor advanced over a lost dirty write",
                None,
                None,
                False,
                EventIngestResult(outcome="ingested", cursor_advanced=True),
                _exact("cursor commit-marker divergence: "
                       "expected=False actual=True"),
            ),
            (
                "C15 advance held anyway",
                None,
                None,
                True,
                EventIngestResult(
                    outcome="ingested",
                    cursor_advanced=True,
                    cursor_hold_reason="lost_current_incarnation_write"),
                _exact("advanced cursor unexpectedly held: "
                       "'lost_current_incarnation_write'"),
            ),
            (
                "C16 lost dirty write held with no reason",
                None,
                None,
                False,
                EventIngestResult(outcome="ingested", cursor_advanced=False),
                _exact("lost dirty write lacked the safe hold reason: None"),
            ),
            (
                "C16 lost dirty write held for the wrong reason",
                None,
                None,
                False,
                EventIngestResult(
                    outcome="ingested",
                    cursor_advanced=False,
                    cursor_hold_reason="lost_write"),
                _exact("lost dirty write lacked the safe hold reason: "
                       "'lost_write'"),
            ),
        ]
        for (clause, expected_path, actual_path, expected_advanced,
             result, pattern) in cases:
            with (self.subTest(clause=clause),
                  self.assertRaisesRegex(AssertionError, pattern)):
                assert_incarnation_window(
                    expected_path=expected_path,
                    actual_path=actual_path,
                    expected_cursor_advanced=expected_advanced,
                    result=result,
                )

    def test_feed_origin_checker_clauses(self):
        feed = {"/downloads/complete/0"}
        active_violation = (
            "stamped path '/invented/active' does not originate from the feed")
        ledger_violation = (
            "ledger-stamped path '/invented/ledger' does not originate "
            "from the feed")
        with self.subTest(clause="C17 active stamp not from the feed"):
            self.assertEqual(
                feed_origin_violations(
                    stamped={_KEY_A: "/invented/active"},
                    ledger={_LEDGER_A: "/downloads/complete/0"},
                    feed_paths=feed),
                [active_violation])
        with self.subTest(clause="C18 ledger stamp not from the feed"):
            self.assertEqual(
                feed_origin_violations(
                    stamped={_KEY_A: "/downloads/complete/0"},
                    ledger={_LEDGER_A: "/invented/ledger"},
                    feed_paths=feed),
                [ledger_violation])
        with self.subTest(clause="both clauses report — neither masks"):
            self.assertEqual(
                feed_origin_violations(
                    stamped={_KEY_A: "/invented/active"},
                    ledger={_LEDGER_A: "/invented/ledger"},
                    feed_paths=feed),
                [active_violation, ledger_violation])

    def test_ownership_agreement_checker_clauses(self):
        owned: set[tuple[str, str]] = {_LEDGER_A}
        active_violation = (
            "active_download_state stamped ('peer1', '02 track.flac'), "
            "which the transfer ledger does not prove is ours")
        ledger_violation = (
            "the transfer ledger stamped ('peer1', '02 track.flac'), "
            "which its own accepted-POST rule does not prove is ours")
        with self.subTest(clause="C19 active stamp on an unowned key"):
            self.assertEqual(
                ownership_agreement_violations(
                    owned_keys=owned,
                    active_stamped_keys={_LEDGER_A, _LEDGER_B},
                    ledger_stamped_keys={_LEDGER_A}),
                [active_violation])
        with self.subTest(clause="C20 ledger stamp on an unowned key"):
            self.assertEqual(
                ownership_agreement_violations(
                    owned_keys=owned,
                    active_stamped_keys={_LEDGER_A},
                    ledger_stamped_keys={_LEDGER_A, _LEDGER_B}),
                [ledger_violation])
        with self.subTest(clause="both clauses report — neither masks"):
            self.assertEqual(
                ownership_agreement_violations(
                    owned_keys=owned,
                    active_stamped_keys={_LEDGER_B},
                    ledger_stamped_keys={_LEDGER_B}),
                [active_violation, ledger_violation])

    def test_owning_request_id_fails_closed_on_a_foreign_key(self):
        world = EventWorld(
            rows=(RequestWorld(
                request_id=1,
                file_keys=(("peer0", "single.flac"),),
                leaves_before_classification=False),),
            events=(),
            cursor_index=0,
        )
        with self.assertRaisesRegex(
            AssertionError,
            _prefix("ledgered key ('peer9', 'nope.flac') owned by no row in"),
        ):
            _owning_request_id(world, ("peer9", "nope.flac"))

    def test_checkers_accept_conforming_worlds(self):
        """Must-still-work: none of the clauses fires on a clean pass."""
        assert_stamps_match(
            {_KEY_A: "/downloads/complete/0", _KEY_B: None},
            {_KEY_A: "/downloads/complete/0", _KEY_B: None})
        assert_ledger_stamps_match(
            {_LEDGER_A: "/downloads/complete/0", _LEDGER_B: None},
            {_LEDGER_A: "/downloads/complete/0", _LEDGER_B: None})
        for outcome in _VALID_OUTCOMES:
            assert_result_well_formed(EventIngestResult(
                outcome=outcome, events_seen=2, file_events=1,
                files_stamped=1, requests_updated=1, transfers_stamped=1,
                cursor_advanced=True))
        assert_result_well_formed(EventIngestResult(
            outcome="ingested",
            cursor_advanced=False,
            cursor_hold_reason="lost_current_incarnation_write"))
        assert_result_well_formed(EventIngestResult(
            outcome="ingested", cursor_gap=True, cursor_advanced=True))
        assert_incarnation_window(
            expected_path="/downloads/incarnation/0",
            actual_path="/downloads/incarnation/0",
            expected_cursor_advanced=True,
            result=EventIngestResult(outcome="ingested", cursor_advanced=True))
        assert_incarnation_window(
            expected_path=None,
            actual_path=None,
            expected_cursor_advanced=False,
            result=EventIngestResult(
                outcome="ingested",
                cursor_advanced=False,
                cursor_hold_reason="lost_current_incarnation_write"))
        self.assertEqual(
            feed_origin_violations(
                stamped={_KEY_A: "/downloads/complete/0", _KEY_B: None},
                ledger={_LEDGER_A: None},
                feed_paths={"/downloads/complete/0"}),
            [])
        self.assertEqual(
            ownership_agreement_violations(
                owned_keys={_LEDGER_A, _LEDGER_B},
                active_stamped_keys={_LEDGER_A},
                ledger_stamped_keys={_LEDGER_A}),
            [])
        self.assertEqual(
            ownership_agreement_violations(
                owned_keys=set(),
                active_stamped_keys=set(),
                ledger_stamped_keys=set()),
            [])


if __name__ == "__main__":
    unittest.main()
