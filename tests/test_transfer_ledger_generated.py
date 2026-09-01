"""Generated tests for the slskd transfer write-ahead ownership ledger
(issue #571, migration 045).

Two properties over generated worlds:

1. **T1/T1.5 (write-ahead intent and accepted ownership)** — for worlds with
   an ownership context
   (a real ``request_id`` AND a wired ``download_ownership`` writer), the
   ledger insert for ``lib.slskd_transfers.slskd_enqueue_with_outcome``
   (the ONE production call site of ``ctx.slskd.transfers.enqueue``)
   ALWAYS precedes the POST, and EVERY file in the enqueue call ends up
   with a matching intent row — including rejected and unknown POST outcomes.
   Destructive ownership exists iff that POST succeeds. Worlds without
   ownership context never write a row, but the
   enqueue is never blocked by that absence.
2. **T3 (bounded, forensic)** — a pending intent row is pruned once it is
   strictly past the retention cutoff regardless of request status. An
   accepted row past the cutoff survives only while its request is currently
   wanted/downloading; a request_id with no matching row (hard-deleted
   elsewhere) counts as inactive.
The deterministic pins for these same invariants live in
``tests/test_download.py::TestTransferLedgerWriteAheadOrdering`` (T1), and
``tests/test_pipeline_db.py::TestTransferLedgerRoundTrip`` /
``tests/test_fakes.py::TestFakePipelineDBTransferLedger`` (T3).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import os
import re
import sys
import unittest
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.download_ownership import DownloadOwnershipWriter
from lib.pipeline_db import TransferLedgerRow
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import (
    make_cycle_collaborators,
    rebind_collaborators,
)

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
    enqueue_outcome: Literal["accepted", "rejected", "unknown"]


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
        enqueue_outcome=draw(st.sampled_from(
            ("accepted", "rejected", "unknown"))),
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
    if world.enqueue_outcome == "unknown":
        slskd.transfers.enqueue_error = RuntimeError("simulated kill mid-POST")
    elif world.enqueue_outcome == "rejected":
        slskd.transfers.enqueue_result = False

    real_record = db.record_transfer_enqueue

    def recording_record(rows):
        order.append(f"ledger:{len(rows)}")
        return real_record(rows)

    db.record_transfer_enqueue = recording_record

    real_enqueue = slskd.transfers.enqueue

    def recording_enqueue(*, username, files):
        order.append(f"post:{len(files)}")
        return real_enqueue(username=username, files=files)

    slskd.transfers.enqueue = recording_enqueue  # type: ignore[method-assign]

    ctx = CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=CratediggerConfig.from_ini(configparser.ConfigParser()),
            slskd=slskd,
            pipeline_db_source=FakePipelineDBSource(FakePipelineDB()),
        ),
    )
    if world.has_download_ownership:
        rebind_collaborators(
            ctx,
            download_ownership=DownloadOwnershipWriter(db_factory=lambda: db),
        )

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
    failed or returned false. Un-owned worlds must write
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
    expected_owned = (
        {(world.username, filename) for filename in world.filenames}
        if world.enqueue_outcome == "accepted"
        else set()
    )
    actual_owned = db.get_owned_transfer_keys()
    if actual_owned != expected_owned:
        raise AssertionError(
            f"destructive ownership {actual_owned!r} != {expected_owned!r}"
        )


# Decisive-arm pins (issue #1094 per-clause proof). The suite tier draws
# owned worlds in roughly one example of five and owned+accepted worlds in
# 7 of 150, so the arms that decide who may CANCEL a stranger's transfer
# rested on a handful of derandomized draws that any edit to this property
# reshuffles. Each world below is producible by ``enqueue_worlds``; the
# names say which clause it makes decisive.
_OWNED_ACCEPTED_MULTIFILE = EnqueueWorld(
    filenames=(_FILENAMES[0], _FILENAMES[1]),
    username=_USERNAMES[0],
    request_id=4242,
    attempt_fp="fp-decisive",
    has_download_ownership=True,
    enqueue_outcome="accepted",
)
_OWNED_REJECTED = EnqueueWorld(
    filenames=(_FILENAMES[0], _FILENAMES[2]),
    username=_USERNAMES[1],
    request_id=4243,
    attempt_fp="fp-rejected",
    has_download_ownership=True,
    enqueue_outcome="rejected",
)
_OWNED_UNKNOWN = EnqueueWorld(
    filenames=(_FILENAMES[3],),
    username=_USERNAMES[2],
    request_id=4244,
    attempt_fp=None,
    has_download_ownership=True,
    enqueue_outcome="unknown",
)
_UNOWNED_WRITER_WITHOUT_REQUEST = EnqueueWorld(
    filenames=(_FILENAMES[4],),
    username=_USERNAMES[0],
    request_id=None,
    attempt_fp="fp-unowned",
    has_download_ownership=True,
    enqueue_outcome="accepted",
)
_UNOWNED_REQUEST_WITHOUT_WRITER = EnqueueWorld(
    filenames=(_FILENAMES[0],),
    username=_USERNAMES[1],
    request_id=4245,
    attempt_fp=None,
    has_download_ownership=False,
    enqueue_outcome="accepted",
)


class TestGeneratedTransferLedgerWriteAhead(unittest.TestCase):
    """T1 property: write-ahead ownership over generated enqueue worlds,
    including rejected and unknown POST outcomes."""

    @given(world=enqueue_worlds())
    @example(world=_OWNED_ACCEPTED_MULTIFILE)
    @example(world=_OWNED_REJECTED)
    @example(world=_OWNED_UNKNOWN)
    @example(world=_UNOWNED_WRITER_WITHOUT_REQUEST)
    @example(world=_UNOWNED_REQUEST_WITHOUT_WRITER)
    def test_write_ahead_holds_across_worlds(self, world):
        order, db = _run_enqueue(world)
        assert_write_ahead_holds(world, order, db)


# --- T3: bounded, forensic prune -----------------------------------------


@dataclass(frozen=True)
class LedgerPruneRow:
    request_id: int
    age_seconds: int
    request_status: str | None  # None = request row doesn't exist
    accepted: bool


_STATUSES = (
    "wanted", "downloading", "imported", "unsearchable", "replaced")
_RETENTION_DAYS = 90
_RETENTION_SECONDS = _RETENTION_DAYS * 24 * 60 * 60
_PRUNE_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_PRUNE_CUTOFF = _PRUNE_NOW - timedelta(seconds=_RETENTION_SECONDS)


@st.composite
def prune_worlds(draw) -> tuple[LedgerPruneRow, ...]:
    count = draw(st.integers(min_value=0, max_value=8))
    rows = []
    for i in range(count):
        age_seconds = draw(st.one_of(
            st.just(_RETENTION_SECONDS),
            st.integers(min_value=0, max_value=400 * 24 * 60 * 60),
            # Third branch (issue #1094): the two branches above put a row
            # strictly PAST the cutoff -- the only state in which the prune
            # policy decides anything at all -- in 14 of 151 suite-tier
            # worlds, because Hypothesis' integer generation is biased
            # toward small values. Without it the accepted/active
            # protection arm ran in 2 worlds, one of them the @example pin.
            st.integers(
                min_value=_RETENTION_SECONDS + 1,
                max_value=400 * 24 * 60 * 60),
        ))
        rows.append(LedgerPruneRow(
            request_id=i + 1,
            age_seconds=age_seconds,
            request_status=draw(st.one_of(
                st.none(), st.sampled_from(_STATUSES))),
            accepted=draw(st.booleans()),
        ))
    return tuple(rows)


def _build_prune_db(rows: tuple[LedgerPruneRow, ...]) -> FakePipelineDB:
    db = FakePipelineDB()
    for row in rows:
        if row.request_status is not None:
            db.seed_request({"id": row.request_id, "status": row.request_status})
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=row.request_id, username="p0",
                filename=f"f-{row.request_id}.flac"),
        ])
        if row.accepted:
            db.confirm_transfer_enqueue(
                "p0", f"f-{row.request_id}.flac",
                request_id=row.request_id)
        ledger_id = next(
            fid for fid, r in db._transfer_ledger.items()
            if r.request_id == row.request_id)
        db._transfer_ledger[ledger_id].enqueued_at = (
            _PRUNE_NOW - timedelta(seconds=row.age_seconds))
    return db


def expected_prune_survivors(rows: tuple[LedgerPruneRow, ...]) -> set[int]:
    """T3 invariant: rows within retention survive; after it, only accepted
    rows for active requests retain ownership evidence."""
    survivors = set()
    for row in rows:
        within_retention = row.age_seconds <= _RETENTION_SECONDS
        accepted_active = (
            row.accepted
            and row.request_status in ("wanted", "downloading")
        )
        if within_retention or accepted_active:
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
    """T3 property: pending-intent bounds plus accepted-row protection."""

    @example(rows=(
        LedgerPruneRow(1, 200 * 24 * 60 * 60, "wanted", False),
        LedgerPruneRow(2, 200 * 24 * 60 * 60, "downloading", False),
        LedgerPruneRow(3, 200 * 24 * 60 * 60, "wanted", True),
        LedgerPruneRow(4, 200 * 24 * 60 * 60, "downloading", True),
        LedgerPruneRow(5, _RETENTION_SECONDS, "imported", False),
        LedgerPruneRow(6, 200 * 24 * 60 * 60, None, True),
    ))
    @given(rows=prune_worlds())
    def test_prune_respects_intent_acceptance_retention_and_status(self, rows):
        db = _build_prune_db(rows)

        db.prune_transfer_ledger(older_than=_PRUNE_CUTOFF)

        survivors_after = {r.request_id for r in db._transfer_ledger.values()}
        assert_prune_matches_oracle(rows, survivors_after)


def _exactly(message: str) -> str:
    """Anchor a clause's COMPLETE message.

    A bare substring proves only that *something* raised: every clause in
    ``assert_write_ahead_holds`` raises ``AssertionError``, and a
    short-circuiting checker evaluates them in order, so a self-test whose
    world violates two clauses silently proves the earlier one while going
    on advertising the later one in its name (docs/generated-testing.md
    "Per-clause proof").
    """
    return "^" + re.escape(message) + "$"


class TestTransferLedgerCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker clause must trip on a planted
    violating world/state, and must trip with ITS OWN message.

    Every clause of ``assert_write_ahead_holds`` and
    ``assert_prune_matches_oracle`` owns at least one row below, and each
    world makes exactly that clause's condition true while every earlier
    clause in the same function passes.
    """

    def test_write_ahead_clause_trips_with_its_own_message(self):
        owned_single = EnqueueWorld(
            filenames=("a.flac",), username="p0", request_id=1,
            attempt_fp=None, has_download_ownership=True,
            enqueue_outcome="accepted")
        owned_pair = EnqueueWorld(
            filenames=("a.flac", "b.flac"), username="p0", request_id=1,
            attempt_fp=None, has_download_ownership=True,
            enqueue_outcome="accepted")
        unowned = EnqueueWorld(
            filenames=("a.flac",), username="p0", request_id=None,
            attempt_fp=None, has_download_ownership=False,
            enqueue_outcome="accepted")
        fingerprinted = EnqueueWorld(
            filenames=("a.flac",), username="p0", request_id=1,
            attempt_fp="fp1", has_download_ownership=True,
            enqueue_outcome="accepted")
        unknown_post = EnqueueWorld(
            filenames=("a.flac",), username="p0", request_id=1,
            attempt_fp=None, has_download_ownership=True,
            enqueue_outcome="unknown")
        rejected_post = EnqueueWorld(
            filenames=("a.flac",), username="p0", request_id=1,
            attempt_fp=None, has_download_ownership=True,
            enqueue_outcome="rejected")
        row_a = TransferLedgerRow(
            request_id=1, username="p0", filename="a.flac")
        unfingerprinted = TransferLedgerRow(
            request_id=1, username="p0", filename="a.flac",
            attempt_fingerprint=None)

        cases: tuple[
            tuple[str, EnqueueWorld, list[str], list[TransferLedgerRow],
                  bool, str],
            ...,
        ] = (
            # (clause, world, order, seeded rows, confirm rows?, expected)
            (
                "L1 the enqueue POST was never issued",
                owned_single, [], [], False,
                _exactly(
                    f"enqueue POST was never issued for {owned_single!r}"),
            ),
            (
                "L2 an un-owned world wrote ledger rows",
                unowned, ["post:1"], [row_a], False,
                _exactly(
                    "un-owned world wrote ledger rows it shouldn't have: "
                    f"{[row_a]!r}"),
            ),
            (
                "L3 an owned world never wrote a ledger row",
                owned_single, ["post:1"], [], False,
                _exactly(
                    f"owned world never wrote a ledger row: {owned_single!r}"),
            ),
            (
                "L4 the ledger write did not precede the POST",
                owned_single, ["post:1", "ledger:1"], [row_a], True,
                _exactly(
                    "ledger write did not precede the POST: "
                    f"order={['post:1', 'ledger:1']!r}"),
            ),
            (
                "L5 an enqueued file was never ledgered",
                owned_pair, ["ledger:1", "post:2"], [row_a], True,
                # Prefix anchor: the tail interpolates two set reprs, whose
                # iteration order is not stable across interpreter runs.
                "^ledgered filenames ",
            ),
            (
                "L6 the attempt fingerprint drifted at the boundary",
                fingerprinted, ["ledger:1", "post:1"], [unfingerprinted], True,
                _exactly(
                    f"attempt_fingerprint drifted: {unfingerprinted!r} vs "
                    f"{'fp1'!r}"),
            ),
            (
                "L7a an UNKNOWN POST was promoted to destructive ownership",
                unknown_post, ["ledger:1", "post:1"], [row_a], True,
                "^destructive ownership ",
            ),
            (
                "L7b a REJECTED POST was promoted to destructive ownership",
                rejected_post, ["ledger:1", "post:1"], [row_a], True,
                "^destructive ownership ",
            ),
        )

        for clause, world, order, rows, confirm, expected in cases:
            with self.subTest(clause=clause):
                db = FakePipelineDB()
                if rows:
                    db.record_transfer_enqueue(list(rows))
                if confirm:
                    for row in rows:
                        db.confirm_transfer_enqueue(
                            row.username, row.filename,
                            request_id=row.request_id)
                with self.assertRaisesRegex(AssertionError, expected):
                    assert_write_ahead_holds(world, order, db)

    def test_prune_clause_trips_with_its_own_message(self):
        cases = (
            (
                "an accepted row for an active request was pruned",
                (LedgerPruneRow(
                    request_id=1,
                    age_seconds=200 * 24 * 60 * 60,
                    request_status="wanted",
                    accepted=True,
                ),),
                set[int](),
            ),
            (
                "an old PENDING row for an active request survived",
                (LedgerPruneRow(
                    request_id=1,
                    age_seconds=200 * 24 * 60 * 60,
                    request_status="downloading",
                    accepted=False,
                ),),
                {1},
            ),
            (
                "the exact retention boundary row was pruned",
                (LedgerPruneRow(
                    request_id=1,
                    age_seconds=_RETENTION_SECONDS,
                    request_status=None,
                    accepted=False,
                ),),
                set[int](),
            ),
        )
        for clause, rows, survivors_after in cases:
            # Prefix anchor: the tail interpolates two set reprs.
            with self.subTest(clause=clause), self.assertRaisesRegex(
                    AssertionError, "^prune survivors diverged: "):
                assert_prune_matches_oracle(rows, survivors_after)


if __name__ == "__main__":
    unittest.main()
