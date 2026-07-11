#!/usr/bin/env python3
"""Generated tests for the slskd transfer write-ahead ownership ledger
(issue #571, migration 045).

Three properties over generated worlds:

1. **T1 (write-ahead ownership)** — for worlds with an ownership context
   (a real ``request_id`` AND a wired ``download_ownership`` writer), the
   ledger insert for ``lib.slskd_transfers.slskd_enqueue_with_outcome``
   (the ONE production call site of ``ctx.slskd.transfers.enqueue``)
   ALWAYS precedes the POST, and EVERY file in the enqueue call ends up
   with a matching ledger row — including when the POST itself raises
   (a simulated kill mid-enqueue). Worlds without ownership context never
   write a row, but the enqueue is never blocked by that absence.
2. **T3 (bounded, forensic)** — a ledger row is pruned iff it is BOTH
   past the retention cutoff AND its request is not currently
   wanted/downloading; a request_id with no matching row (hard-deleted
   elsewhere) counts as inactive.
3. **TS1 (transfer_id capture, issue #571 PR 5)** — over any sequence of
   T1.5 (``stamp_transfer_id``) and T2 (``stamp_transfer_completion``
   with ``transfer_id=``) capture calls for one ledger row, the row's
   final ``transfer_id`` is whichever value the FIRST call in the
   sequence carried — every later call either finds ``transfer_id``
   already non-NULL (a no-op) or, for a later completion call, finds
   ``completed_at`` already non-NULL (a full no-op). This is what makes
   the per-id completed-transfer purge's ownership proof durable
   regardless of which capture path (enqueue-response or the completion
   event) wins the race.

The deterministic pins for these same invariants live in
``tests/test_download.py::TestTransferLedgerWriteAheadOrdering`` (T1 +
T1.5), ``tests/test_slskd_events.py::TestTransferLedgerStamping`` (T2 +
TS1's event-fallback wiring), and
``tests/test_pipeline_db.py::TestTransferLedgerRoundTrip`` /
``tests/test_fakes.py::TestFakePipelineDBTransferLedger`` (T3, TS1).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

from lib.download_ownership import DownloadOwnershipWriter
from lib.pipeline_db import TransferLedgerRow
from tests.fakes import FakePipelineDB, FakeSlskdAPI

_USERNAMES = ("peer0", "peer1", "péer♪2")
_FILENAMES = (
    "Music\\Artist\\Album\\01 track.flac",
    "Music\\Artist\\Album\\02 track.flac",
    "@@direct\\weird/../path.opus",
    "single.flac",
    "Music\\Ártîst 音\\Å l b u m\\03.mp3",
)


# --- T1: write-ahead ownership -----------------------------------------


@dataclass(frozen=True)
class EnqueueWorld:
    filenames: tuple[str, ...]
    username: str
    request_id: int | None
    attempt_fp: str | None
    has_download_ownership: bool
    enqueue_raises: bool


@st.composite
def enqueue_worlds(draw) -> EnqueueWorld:
    filenames = tuple(draw(st.lists(
        st.sampled_from(_FILENAMES), min_size=1, max_size=4, unique=True)))
    return EnqueueWorld(
        filenames=filenames,
        username=draw(st.sampled_from(_USERNAMES)),
        request_id=draw(st.one_of(
            st.none(), st.integers(min_value=1, max_value=10_000))),
        attempt_fp=draw(st.one_of(
            st.none(), st.text(min_size=1, max_size=8))),
        has_download_ownership=draw(st.booleans()),
        enqueue_raises=draw(st.booleans()),
    )


def _run_enqueue(world: EnqueueWorld) -> tuple[list[str], FakePipelineDB]:
    """Drive the REAL production write-ahead seam
    (``slskd_enqueue_with_outcome``) over one generated world. Returns
    (call-order log, the db the ledger landed in)."""
    import configparser

    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from lib.slskd_transfers import slskd_enqueue_with_outcome
    from tests.fakes import FakePipelineDBSource

    order: list[str] = []
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    if world.enqueue_raises:
        slskd.transfers.enqueue_error = RuntimeError("simulated kill mid-POST")

    real_record = db.record_transfer_enqueue

    def recording_record(rows):
        order.append(f"ledger:{len(rows)}")
        return real_record(rows)

    db.record_transfer_enqueue = recording_record  # type: ignore[method-assign]

    real_enqueue = slskd.transfers.enqueue

    def recording_enqueue(*, username, files):
        order.append(f"post:{len(files)}")
        return real_enqueue(username=username, files=files)

    slskd.transfers.enqueue = recording_enqueue  # type: ignore[method-assign]

    ctx = CratediggerContext(
        cfg=CratediggerConfig.from_ini(configparser.ConfigParser()),
        slskd=slskd,
        pipeline_db_source=FakePipelineDBSource(FakePipelineDB()),
    )
    if world.has_download_ownership:
        ctx.download_ownership = DownloadOwnershipWriter(db_factory=lambda: db)

    files = [{"filename": f, "size": 1} for f in world.filenames]
    with patch("time.sleep"):
        slskd_enqueue_with_outcome(
            world.username, files, "dir", ctx,
            request_id=world.request_id, attempt_fp=world.attempt_fp)
    return order, db


def assert_write_ahead_holds(world: EnqueueWorld, order: list[str], db: FakePipelineDB) -> None:
    """T1 checker (module-level for the known-bad self-test).

    Owned worlds (real request_id + wired ownership writer): the ledger
    write must precede the POST, and EVERY file must have a matching
    ledger row — regardless of whether the POST itself succeeded or
    raised (the simulated-kill worlds).  Un-owned worlds must write
    nothing, but the enqueue call itself must still have been attempted
    (`order` contains a "post:" entry) — absence of ownership context
    never blocks the enqueue.
    """
    owned = world.request_id is not None and world.has_download_ownership
    post_entries = [o for o in order if o.startswith("post:")]
    if not post_entries:
        raise AssertionError(f"enqueue POST was never issued for {world!r}")
    if not owned:
        rows = db.record_transfer_enqueue_calls
        if rows:
            raise AssertionError(
                f"un-owned world wrote ledger rows it shouldn't have: {rows!r}")
        return
    ledger_entries = [o for o in order if o.startswith("ledger:")]
    if not ledger_entries:
        raise AssertionError(f"owned world never wrote a ledger row: {world!r}")
    if order.index(ledger_entries[0]) > order.index(post_entries[0]):
        raise AssertionError(
            f"ledger write did not precede the POST: order={order!r}")
    rows = db.record_transfer_enqueue_calls
    ledgered_filenames = {r.filename for r in rows}
    if ledgered_filenames != set(world.filenames):
        raise AssertionError(
            f"ledgered filenames {ledgered_filenames!r} != "
            f"enqueued filenames {set(world.filenames)!r}")
    for row in rows:
        if row.attempt_fingerprint != world.attempt_fp:
            raise AssertionError(
                f"attempt_fingerprint drifted: {row!r} vs {world.attempt_fp!r}")


class TestGeneratedTransferLedgerWriteAhead(unittest.TestCase):
    """T1 property: write-ahead ownership over generated enqueue worlds,
    including simulated-kill (POST raises) worlds."""

    @given(world=enqueue_worlds())
    def test_write_ahead_holds_across_worlds(self, world):
        order, db = _run_enqueue(world)
        assert_write_ahead_holds(world, order, db)


# --- T3: bounded, forensic prune -----------------------------------------


@dataclass(frozen=True)
class LedgerPruneRow:
    request_id: int
    age_days: float
    request_status: str | None  # None = request row doesn't exist


_STATUSES = ("wanted", "downloading", "imported", "manual", "replaced")
_RETENTION_DAYS = 90


@st.composite
def prune_worlds(draw) -> tuple[LedgerPruneRow, ...]:
    count = draw(st.integers(min_value=0, max_value=8))
    rows = []
    for i in range(count):
        # Deliberately avoid the exact retention boundary (~90 days): the
        # seed timestamp and the prune call's `older_than` are computed
        # at two different `datetime.now()` reads a few microseconds
        # apart, so a value landing exactly on the boundary is a genuine
        # clock-skew race, not a meaningful invariant edge to pin.
        age_days = draw(st.one_of(
            st.floats(min_value=0.0, max_value=_RETENTION_DAYS - 0.1,
                      allow_nan=False, allow_infinity=False),
            st.floats(min_value=_RETENTION_DAYS + 0.1, max_value=400.0,
                      allow_nan=False, allow_infinity=False),
        ))
        rows.append(LedgerPruneRow(
            request_id=i + 1,
            age_days=age_days,
            request_status=draw(st.one_of(
                st.none(), st.sampled_from(_STATUSES))),
        ))
    return tuple(rows)


def _build_prune_db(rows: tuple[LedgerPruneRow, ...]) -> FakePipelineDB:
    db = FakePipelineDB()
    now = datetime.now(timezone.utc)
    for row in rows:
        if row.request_status is not None:
            db.seed_request({"id": row.request_id, "status": row.request_status})
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=row.request_id, username="p0",
                filename=f"f-{row.request_id}.flac"),
        ])
        ledger_id = next(
            fid for fid, r in db._transfer_ledger.items()
            if r.request_id == row.request_id)
        db._transfer_ledger[ledger_id].enqueued_at = (
            now - timedelta(days=row.age_days))
    return db


def expected_prune_survivors(rows: tuple[LedgerPruneRow, ...]) -> set[int]:
    """T3 invariant: a row survives iff it's within retention OR its
    request is currently active (wanted/downloading)."""
    survivors = set()
    for row in rows:
        within_retention = row.age_days < _RETENTION_DAYS
        active = row.request_status in ("wanted", "downloading")
        if within_retention or active:
            survivors.add(row.request_id)
    return survivors


def assert_prune_matches_oracle(
    rows: tuple[LedgerPruneRow, ...], survivors_after: set[int],
) -> None:
    """T3 checker (module-level for the known-bad self-test)."""
    expected = expected_prune_survivors(rows)
    if expected != survivors_after:
        raise AssertionError(
            f"prune survivors diverged: expected={expected!r} "
            f"actual={survivors_after!r}")


class TestGeneratedTransferLedgerPrune(unittest.TestCase):
    """T3 property: retention + active-request gating over generated
    ledger-row worlds."""

    @given(rows=prune_worlds())
    def test_prune_respects_retention_and_active_status(self, rows):
        db = _build_prune_db(rows)

        db.prune_transfer_ledger(
            older_than=datetime.now(timezone.utc) - timedelta(days=_RETENTION_DAYS))

        survivors_after = {r.request_id for r in db._transfer_ledger.values()}
        assert_prune_matches_oracle(rows, survivors_after)


# --- TS1: transfer_id capture, first-known-wins (issue #571 PR 5) ------


@dataclass(frozen=True)
class CaptureOp:
    kind: str  # "stamp_id" | "stamp_completion"
    transfer_id: str


@st.composite
def capture_op_sequences(draw) -> tuple[CaptureOp, ...]:
    op_count = draw(st.integers(min_value=0, max_value=5))
    ops = []
    for i in range(op_count):
        kind = draw(st.sampled_from(("stamp_id", "stamp_completion")))
        # Distinct-looking ids per position so a wrong final value is
        # never accidentally right.
        ops.append(CaptureOp(kind=kind, transfer_id=f"tid-{i}"))
    return tuple(ops)


def _run_capture_sequence(ops: tuple[CaptureOp, ...]) -> FakePipelineDB:
    """Drive the REAL production write methods (via FakePipelineDB,
    proven at parity with real PG by the round-trip pins in
    tests/test_pipeline_db.py) over one generated capture-op sequence for
    a single ledgered (username, filename) row -- no retries, so ledger
    row identity never ambiguous."""
    db = FakePipelineDB()
    db.record_transfer_enqueue([
        TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
    ])
    for op in ops:
        if op.kind == "stamp_id":
            db.stamp_transfer_id("p0", "a.flac", op.transfer_id)
        else:
            db.stamp_transfer_completion(
                "p0", "a.flac", "/downloads/a.flac",
                datetime.now(timezone.utc), transfer_id=op.transfer_id)
    return db


def expected_final_transfer_id(ops: tuple[CaptureOp, ...]) -> str | None:
    """TS1 invariant: the final transfer_id is whichever value the FIRST
    op in the sequence carried (write-ahead leaves it NULL; the first
    capture call -- either path -- always finds the row still open and
    writes; every later call finds no matching row left to touch)."""
    return ops[0].transfer_id if ops else None


def assert_transfer_id_capture_matches_oracle(
    ops: tuple[CaptureOp, ...], actual: str | None,
) -> None:
    """TS1 checker (module-level for the known-bad self-test)."""
    expected = expected_final_transfer_id(ops)
    if expected != actual:
        raise AssertionError(
            f"final transfer_id diverged: expected={expected!r} "
            f"actual={actual!r} ops={ops!r}")


class TestGeneratedTransferIdCapture(unittest.TestCase):
    """TS1 property: first-known-wins over generated capture-op
    sequences, through the REAL stamp_transfer_id / stamp_transfer_
    completion write methods."""

    @given(ops=capture_op_sequences())
    def test_first_known_transfer_id_wins(self, ops):
        db = _run_capture_sequence(ops)

        rows = list(db._transfer_ledger.values())
        self.assertEqual(len(rows), 1)
        assert_transfer_id_capture_matches_oracle(ops, rows[0].transfer_id)


class TestTransferLedgerCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker must trip on a planted
    violating world/state."""

    def test_transfer_id_capture_checker_trips_when_a_later_id_clobbers(self):
        ops = (
            CaptureOp(kind="stamp_id", transfer_id="tid-0"),
            CaptureOp(kind="stamp_completion", transfer_id="tid-1"),
        )
        with self.assertRaises(AssertionError):
            assert_transfer_id_capture_matches_oracle(ops, "tid-1")

    def test_transfer_id_capture_checker_trips_on_empty_ops_with_a_value(self):
        with self.assertRaises(AssertionError):
            assert_transfer_id_capture_matches_oracle((), "tid-invented")

    def test_write_ahead_checker_trips_when_post_precedes_ledger(self):
        world = EnqueueWorld(
            filenames=("a.flac",), username="p0", request_id=1,
            attempt_fp=None, has_download_ownership=True,
            enqueue_raises=False)
        db = FakePipelineDB()
        with self.assertRaises(AssertionError):
            assert_write_ahead_holds(world, ["post:1", "ledger:1"], db)

    def test_write_ahead_checker_trips_when_a_file_is_unledgered(self):
        world = EnqueueWorld(
            filenames=("a.flac", "b.flac"), username="p0", request_id=1,
            attempt_fp=None, has_download_ownership=True,
            enqueue_raises=False)
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        with self.assertRaises(AssertionError):
            assert_write_ahead_holds(world, ["ledger:1", "post:2"], db)

    def test_write_ahead_checker_trips_on_unowned_world_with_a_row(self):
        world = EnqueueWorld(
            filenames=("a.flac",), username="p0", request_id=None,
            attempt_fp=None, has_download_ownership=False,
            enqueue_raises=False)
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        with self.assertRaises(AssertionError):
            assert_write_ahead_holds(world, ["post:1"], db)

    def test_prune_checker_trips_when_an_expected_survivor_is_missing(self):
        rows = (LedgerPruneRow(request_id=1, age_days=5.0, request_status="wanted"),)
        with self.assertRaises(AssertionError):
            assert_prune_matches_oracle(rows, survivors_after=set())

    def test_prune_checker_trips_when_an_unexpected_row_survives(self):
        rows = (LedgerPruneRow(request_id=1, age_days=200.0, request_status="imported"),)
        with self.assertRaises(AssertionError):
            assert_prune_matches_oracle(rows, survivors_after={1})


if __name__ == "__main__":
    unittest.main()
