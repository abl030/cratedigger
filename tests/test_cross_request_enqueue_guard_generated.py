"""Generated tests for the #1178 PR2 cross-request enqueue guard.

Two requests for different pressings of the same album can both browse to
the SAME peer directory and both accept the SAME ``(username, filename)``
queue keys -- nothing previously asked "is this queue key already held by
another request" before claiming ownership (#1178: requests 8953/8954,
17 shared keys from peer TheBun, 240ms apart). The fix is two composed
layers in ``lib.enqueue._cross_request_conflict_ids``, consulted in this
order:

1. **Cross-cycle** (checked FIRST, read-only): a callable bound to ONE
   ``lib.download_ownership.DownloadOwnershipWriter.
   open_conflict_check_session`` handle -- one per calling
   ``try_enqueue`` / ``try_multi_enqueue`` invocation, never per
   candidate (review F7) -- reads the transfer ledger for a PRIOR
   cycle's accepted ownership whose owner is CURRENTLY ``'downloading'``
   on its CURRENT attempt -- never ``'processing'`` (CLAUDE.md critical
   invariant 10), and never a stale abandoned attempt (review F2: an
   owner's OWN earlier, superseded attempt must not block a sibling on
   that attempt's keys).
2. **Same-cycle** (checked SECOND, only once the cross-cycle layer is
   clear): ``ctx.claimed_queue_keys_registry``
   (``lib.enqueue.ClaimedQueueKeysRegistry``, one instance per cycle),
   checked and registered atomically under one lock. Registering only
   after the cross-cycle layer clears is load-bearing -- see the module
   comment in ``lib/enqueue.py`` above ``ClaimedQueueKeysRegistry`` for
   the registry-poisoning bug this ordering fixes, which this very
   property found.

One property drives BOTH real layers together
(``lib.enqueue._cross_request_conflict_ids``, composed with a real
``ClaimedQueueKeysRegistry`` and a real ``FakePipelineDB``-backed
``DownloadOwnershipWriter``) over generated worlds of N requests x
overlapping key sets x owner statuses, across multiple simulated cycles.
Request ids may be REUSED across cycles (a retry with a fresh attempt --
review F3): each won attempt stamps the owning request's
``active_download_state.enqueued_at`` witness (mirroring
``claim.enqueued_at``, captured before the ledger write, same as
production) so the real attempt-boundary predicate in
``lib/pipeline_db/transfer_ledger.py`` has something real to scope
against -- without retries, a request's OWN abandoned earlier attempt can
never appear, which is exactly the world that hid F2 from every earlier
version of this property. This mirrors the T1 property in
``tests/test_transfer_ledger_generated.py``, which drives
``slskd_enqueue_with_outcome`` rather than the full ``try_enqueue``/
``find_download`` outer adapter: the deterministic composition pins that
drive the OUTERMOST real adapters (``try_enqueue`` / ``try_multi_enqueue``)
live in ``tests/test_enqueue_fanout.py::TestCrossRequestEnqueueGuard*``;
this module patrols the combinatorial decision space of the shared guard
both call sites funnel through.

Invariant (two clauses):
  G1 no ``(username, filename)`` key is claimed-and-enqueued by more than
     one request whose owner status is ``'downloading'`` on its CURRENT
     attempt at decision time (no double-claim).
  G2 every request whose keys were entirely free at decision time
     proceeded (no over-blocking).

Profiles and promotion policy: ``tests/_hypothesis_profiles.py`` and
``docs/generated-testing.md``.
"""
from __future__ import annotations

import os
import re
import sys
import unittest
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.download import build_active_download_state
from lib.download_ownership import DownloadOwnershipWriter
from lib.enqueue import ClaimedQueueKeysRegistry, _cross_request_conflict_ids
from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import TransferLedgerRow
from lib.processing_paths import attempt_fingerprint
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_request_row

_USERNAMES = ("peer0", "peer1")
_FILENAMES = ("a.flac", "b.flac", "c.flac")
_OwnerStatus = Literal[
    "downloading", "wanted", "replaced", "imported", "processing"]
# 'processing' is included specifically so a status-filter mutant
# (r.status = 'downloading' -> IN ('downloading', 'processing')) has a
# reachable world to fail on (#1178 PR2 review F1) -- not because a
# successful claim can really settle a request at 'processing' (only the
# processing-ownership handoff, a different subsystem, ever writes that
# status).
_STATUSES: tuple[_OwnerStatus, ...] = (
    "downloading", "wanted", "replaced", "imported", "processing")

_BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class GuardAttempt:
    request_id: int
    keys: tuple[tuple[str, str], ...]
    resulting_status: _OwnerStatus
    # #1196 item 1: whether a won attempt's ``active_download_state``
    # carries the ``attempt_fingerprint`` key. Defaults True so every
    # existing (pre-#1196) example pin below exercises the fingerprint-
    # equality mechanism without modification. False simulates a state
    # that exists but lacks the key -- the ledger rows STILL carry a
    # fingerprint (that column predates this change) while the state
    # does not, so the guard fails CLOSED unconditionally (#1199 item 2:
    # the deploy-window time-predicate fallback this used to exercise
    # was deleted -- the measured cohort was empty -- so this now blocks
    # EVERY accepted key the owner has ever won, not just its current
    # attempt's keys).
    state_has_fingerprint: bool = True


@dataclass(frozen=True)
class GuardCycle:
    attempts: tuple[GuardAttempt, ...]


@dataclass(frozen=True)
class GuardWorld:
    cycles: tuple[GuardCycle, ...]


def _keys_strategy():
    return st.lists(
        st.tuples(st.sampled_from(_USERNAMES), st.sampled_from(_FILENAMES)),
        min_size=1, max_size=2, unique=True,
    ).map(tuple)


@st.composite
def guard_worlds(draw) -> GuardWorld:
    """Request ids are unique WITHIN one cycle (a search-plan cycle never
    hands the same request two candidates at once) but MAY be reused
    ACROSS cycles -- a retry: the request's earlier attempt is abandoned
    and a fresh one begins (review F3)."""
    next_id = [1]
    existing_ids: list[int] = []

    n_cycles = draw(st.integers(min_value=1, max_value=3))
    cycles = []
    for _ in range(n_cycles):
        n_attempts = draw(st.integers(min_value=1, max_value=3))
        used_this_cycle: set[int] = set()
        attempts = []
        for _ in range(n_attempts):
            reusable = [
                rid for rid in existing_ids if rid not in used_this_cycle]
            reuse = bool(reusable) and draw(st.booleans())
            if reuse:
                request_id = draw(st.sampled_from(reusable))
            else:
                request_id = next_id[0]
                next_id[0] += 1
                existing_ids.append(request_id)
            used_this_cycle.add(request_id)
            attempts.append(GuardAttempt(
                request_id=request_id,
                keys=draw(_keys_strategy()),
                resulting_status=draw(st.sampled_from(_STATUSES)),
                state_has_fingerprint=draw(st.booleans()),
            ))
        cycles.append(GuardCycle(attempts=tuple(attempts)))
    return GuardWorld(cycles=tuple(cycles))


def _run_world(world: GuardWorld) -> dict[tuple[int, int], bool]:
    """Drive the REAL guard (``_cross_request_conflict_ids``, composed
    with a real ``ClaimedQueueKeysRegistry`` and a real
    ``FakePipelineDB``-backed ``DownloadOwnershipWriter``) over every
    attempt in every cycle, in draw order. Returns
    ``{(cycle_idx, request_id): proceeded}``.

    A proceeding attempt writes real write-ahead + confirmed ledger rows
    and a real ``active_download_state`` (mirroring what a real
    successful ``try_enqueue`` claim leaves behind) and settles its
    request at ``resulting_status``. The state's ``enqueued_at`` witness
    is stamped strictly BEFORE the ledger write, same ordering
    ``claim.enqueued_at`` uses in production (captured before
    ``writer.claim_downloading``); the ledger rows are then stamped
    fractionally AFTER it, so the real attempt-boundary predicate in
    ``lib/pipeline_db/transfer_ledger.py`` sees exactly the ordering
    production produces.

    #1196 item 1: every ledger row for a won attempt carries the SAME
    ``attempt_fingerprint`` (``lib.processing_paths.attempt_fingerprint``
    over the attempt's own keys) production writes -- the ledger's
    fingerprint column predates this change and is never conditional.
    The STATE's ``attempt_fingerprint`` key is written only when
    ``attempt.state_has_fingerprint`` is True, so the property drives
    the guard's real fingerprint-equality arm AND its fail-closed
    missing-fingerprint arm (#1199 item 2) across the same generated
    world space.

    (#1196 item 1, review F1) The two sides are deliberately NOT sourced
    from one shared local variable: the STATE'S value comes from calling
    the REAL production writer,
    ``lib.download.build_active_download_state``, over a synthetic
    ``GrabListEntry`` built from this attempt's own keys -- the actual
    function ``lib.enqueue._claim_initial_download_ownership`` calls in
    production. The LEDGER'S value is the direct
    ``attempt_fingerprint()`` call, mirroring
    ``lib.enqueue._enqueue_with_claim_outcome``'s own inline computation
    (there is no separate "ledger writer" function to call — that inline
    call IS the production site). A production divergence between either
    real site therefore has a genuine chance of producing two different
    values here, rather than one test-local variable trivially agreeing
    with itself.
    """
    db = FakePipelineDB()
    writer = DownloadOwnershipWriter(db_factory=lambda: db)
    ctx = make_ctx_with_fake_db(db)
    ctx.download_ownership = writer

    proceeded: dict[tuple[int, int], bool] = {}
    ordinal = [0]
    seen_request_ids: set[int] = set()
    for cycle_idx, cycle in enumerate(world.cycles):
        ctx.claimed_queue_keys_registry = ClaimedQueueKeysRegistry()
        for attempt in cycle.attempts:
            if attempt.request_id not in seen_request_ids:
                # Seed ONLY on first appearance. A retry (a request_id
                # reused in a LATER cycle) represents the request having
                # bounced back to 'wanted' off-screen (e.g. a vanished-
                # transfer timeout) between attempts -- re-seeding a fresh
                # row here would silently wipe the PRIOR win's
                # active_download_state/ledger history that a later
                # cross-cycle check against this same request (as an
                # OWNER) needs to see, which is exactly the harness bug
                # this property caught while exercising the widened F3
                # retry worlds.
                db.seed_request(make_request_row(
                    id=attempt.request_id, status="wanted"))
                seen_request_ids.add(attempt.request_id)
            files = [
                DownloadFile(
                    filename=fn, id="", file_dir="", username=un, size=0)
                for un, fn in attempt.keys
            ]
            # One session per attempt, mirroring one try_enqueue /
            # try_multi_enqueue invocation's cross-cycle DB handle
            # (issue #1178 PR2 review F7).
            with writer.open_conflict_check_session() as check_cross_cycle:
                conflicting = _cross_request_conflict_ids(
                    files, attempt.request_id, ctx,
                    check_cross_cycle=check_cross_cycle,
                )
            won = not conflicting
            proceeded[(cycle_idx, attempt.request_id)] = won
            if won:
                ordinal[0] += 1
                witness = _BASE_TIME + timedelta(seconds=ordinal[0])
                # STATE side: the real production writer, over the SAME
                # `files` list just used for the guard check above.
                written_state = build_active_download_state(
                    GrabListEntry(
                        album_id=0, filetype="flac", title="T",
                        artist="A", year="2020", mb_release_id="mbid",
                        files=files,
                    ),
                    enqueued_at=witness.isoformat(),
                )
                state: dict[str, object] = {
                    "filetype": "flac", "enqueued_at": witness.isoformat(),
                    "files": [],
                }
                if attempt.state_has_fingerprint:
                    state["attempt_fingerprint"] = (
                        written_state.attempt_fingerprint)
                db._requests[attempt.request_id]["active_download_state"] = (
                    state)
                # LEDGER side: the direct pure-function call, mirroring
                # _enqueue_with_claim_outcome's own inline computation
                # -- independently derived from the STATE side above.
                ledger_fingerprint = attempt_fingerprint(list(attempt.keys))
                before_ids = set(db._transfer_ledger)
                rows = [
                    TransferLedgerRow(
                        request_id=attempt.request_id, username=un,
                        filename=fn, attempt_fingerprint=ledger_fingerprint)
                    for un, fn in attempt.keys
                ]
                db.record_transfer_enqueue(rows)
                new_ids = set(db._transfer_ledger) - before_ids
                for ledger_id in new_ids:
                    db._transfer_ledger[ledger_id].enqueued_at = (
                        witness + timedelta(milliseconds=1))
                for un, fn in attempt.keys:
                    db.confirm_transfer_enqueue(
                        un, fn, request_id=attempt.request_id)
                db._requests[attempt.request_id]["status"] = (
                    attempt.resulting_status)
    return proceeded


def expected_guard_decisions(world: GuardWorld) -> dict[tuple[int, int], bool]:
    """Reference model: same rules as the guard, independently computed.

    Same-cycle: a key claimed by ANY request in a cycle stays claimed to
    that request for the REST of that cycle (the registry never releases
    mid-cycle in this property's scope -- F5's release-on-refusal is a
    separate real seam, exercised by the deterministic try_enqueue pins
    in tests/test_enqueue_fanout.py, not by this guard-only property).

    Cross-cycle (review F2/F3, #1199 item 2): an owner's CURRENT (most
    recently WON) attempt's ``state_has_fingerprint`` decides the scope
    of what can conflict --

    * ``True`` (the #1196 fingerprint-equality arm): only the CURRENT
      attempt's own keys can conflict -- ``current_attempt_keys`` is
      overwritten on every win, so an earlier, superseded attempt for
      the SAME request is excluded exactly like the real fingerprint
      predicate excludes it (a stale attempt's ledger rows carry a
      different fingerprint and never match).
    * ``False`` (the #1199 fail-closed arm): EVERY key the owner has
      EVER won, across every attempt, can conflict -- there is no
      attempt-boundary rescue once the current state lacks the
      fingerprint key, mirroring the real query's unconditional
      ``ELSE TRUE``. ``all_accepted_keys`` therefore accumulates across
      every win and is never overwritten, unlike ``current_attempt_keys``.
    """
    current_status: dict[int, _OwnerStatus] = {}
    current_attempt_keys: dict[int, frozenset[tuple[str, str]]] = {}
    current_state_has_fingerprint: dict[int, bool] = {}
    all_accepted_keys: dict[int, set[tuple[str, str]]] = {}
    expected: dict[tuple[int, int], bool] = {}
    for cycle_idx, cycle in enumerate(world.cycles):
        claimed_this_cycle: dict[tuple[str, str], int] = {}
        for attempt in cycle.attempts:
            same_cycle_conflict = any(
                claimed_this_cycle.get(k) not in (None, attempt.request_id)
                for k in attempt.keys
            )
            cross_cycle_conflict = not same_cycle_conflict and any(
                owner_id != attempt.request_id
                and current_status.get(owner_id) == "downloading"
                and (
                    any(
                        k in current_attempt_keys[owner_id]
                        for k in attempt.keys
                    )
                    if current_state_has_fingerprint.get(owner_id, False)
                    else any(
                        k in all_accepted_keys[owner_id] for k in attempt.keys
                    )
                )
                for owner_id in all_accepted_keys
            )
            won = not same_cycle_conflict and not cross_cycle_conflict
            expected[(cycle_idx, attempt.request_id)] = won
            if won:
                for k in attempt.keys:
                    claimed_this_cycle[k] = attempt.request_id
                current_status[attempt.request_id] = attempt.resulting_status
                current_attempt_keys[attempt.request_id] = frozenset(
                    attempt.keys)
                current_state_has_fingerprint[attempt.request_id] = (
                    attempt.state_has_fingerprint)
                all_accepted_keys.setdefault(
                    attempt.request_id, set()).update(attempt.keys)
    return expected


def assert_no_double_claim(
    world: GuardWorld, proceeded: dict[tuple[int, int], bool],
) -> None:
    """G1: no request the guard let through despite the reference model
    predicting a conflict for it -- i.e. no key is ever claimed-and-
    enqueued by more than one active owner."""
    expected = expected_guard_decisions(world)
    for key, won in proceeded.items():
        if won and not expected[key]:
            raise AssertionError(
                f"double-claim: {key} proceeded despite a predicted "
                f"conflict (world={world!r} proceeded={proceeded!r})")


def assert_no_over_blocking(
    world: GuardWorld, proceeded: dict[tuple[int, int], bool],
) -> None:
    """G2: no request the reference model predicted as free was blocked
    by the guard."""
    expected = expected_guard_decisions(world)
    for key, won in proceeded.items():
        if expected[key] and not won:
            raise AssertionError(
                f"over-blocked: {key} was skipped despite free keys "
                f"(world={world!r} proceeded={proceeded!r})")


# Decisive-arm pins (issue #1094 per-clause proof), mirroring #1178 itself
# and the must-not-break scenarios named in the PR2 brief.

_SAME_CYCLE_COLLISION = GuardWorld(cycles=(
    GuardCycle(attempts=(
        GuardAttempt(1, (("TheBun", "01.flac"), ("TheBun", "02.flac")),
                     "downloading"),
        GuardAttempt(2, (("TheBun", "01.flac"), ("TheBun", "02.flac")),
                     "downloading"),
    )),
))
_CROSS_CYCLE_DOWNLOADING_BLOCKS = GuardWorld(cycles=(
    GuardCycle(attempts=(
        GuardAttempt(1, (("TheBun", "01.flac"),), "downloading"),
    )),
    GuardCycle(attempts=(
        GuardAttempt(2, (("TheBun", "01.flac"),), "downloading"),
    )),
))
_REPLACE_LINEAGE_DOES_NOT_BLOCK = GuardWorld(cycles=(
    GuardCycle(attempts=(
        GuardAttempt(1, (("peer", "x.flac"),), "replaced"),
    )),
    GuardCycle(attempts=(
        GuardAttempt(2, (("peer", "x.flac"),), "downloading"),
    )),
))
_IMPORTED_OWNER_DOES_NOT_BLOCK = GuardWorld(cycles=(
    GuardCycle(attempts=(
        GuardAttempt(1, (("peer", "x.flac"),), "imported"),
    )),
    GuardCycle(attempts=(
        GuardAttempt(2, (("peer", "x.flac"),), "downloading"),
    )),
))
# #1178 PR2 review F2's exact reproduction: the OWNER (request 1) itself
# retries -- its first attempt (peer OLD) is abandoned in favour of a
# fresh second attempt (peer NEW), both settling 'downloading'. A sibling
# (request 2) trying the OLD, now-superseded key must NOT be blocked; only
# a key from the owner's CURRENT attempt (NEW) would block.
_ABANDONED_ATTEMPT_DOES_NOT_BLOCK = GuardWorld(cycles=(
    GuardCycle(attempts=(
        GuardAttempt(1, (("OLD", "old.flac"),), "downloading"),
    )),
    GuardCycle(attempts=(
        GuardAttempt(1, (("NEW", "new.flac"),), "downloading"),
        GuardAttempt(2, (("OLD", "old.flac"),), "downloading"),
    )),
))
# Same reproduction as above, but with the owner's state carrying NO
# ``attempt_fingerprint`` key on either attempt (a state that exists but
# lacks the key) -- #1199 item 2 deleted the deploy-window time-predicate
# fallback this used to exercise (the measured cohort was empty), so the
# guard now fails CLOSED unconditionally: request 2's sibling attempt on
# the OWNER's OLD, abandoned key is now BLOCKED too, unlike the
# fingerprint-scoped world above where only the CURRENT key blocks.
_MISSING_FINGERPRINT_STATE_BLOCKS_EVERY_HISTORICAL_KEY = GuardWorld(cycles=(
    GuardCycle(attempts=(
        GuardAttempt(1, (("OLD", "old.flac"),), "downloading",
                     state_has_fingerprint=False),
    )),
    GuardCycle(attempts=(
        GuardAttempt(1, (("NEW", "new.flac"),), "downloading",
                     state_has_fingerprint=False),
        GuardAttempt(2, (("OLD", "old.flac"),), "downloading",
                     state_has_fingerprint=False),
    )),
))
# Same-request re-claim WITHIN one cycle (poll-loop / multi-wave retry:
# exclude_request_id and the registry's same-request allowance) is proven
# directly at the registry level -- guard_worlds() never repeats a
# request id within a single cycle, so it cannot arise here. See
# tests/test_enqueue_fanout.py::TestClaimedQueueKeysRegistry
# ::test_same_request_reclaiming_its_own_keys_never_conflicts.


class TestGeneratedCrossRequestEnqueueGuard(unittest.TestCase):
    """G1/G2 property: the composed same-cycle + cross-cycle guard over
    generated multi-request, multi-cycle (incl. retry) worlds."""

    @given(world=guard_worlds())
    @example(world=_SAME_CYCLE_COLLISION)
    @example(world=_CROSS_CYCLE_DOWNLOADING_BLOCKS)
    @example(world=_REPLACE_LINEAGE_DOES_NOT_BLOCK)
    @example(world=_IMPORTED_OWNER_DOES_NOT_BLOCK)
    @example(world=_ABANDONED_ATTEMPT_DOES_NOT_BLOCK)
    @example(world=_MISSING_FINGERPRINT_STATE_BLOCKS_EVERY_HISTORICAL_KEY)
    def test_guard_never_double_claims_or_over_blocks(self, world: GuardWorld):
        proceeded = _run_world(world)
        assert_no_double_claim(world, proceeded)
        assert_no_over_blocking(world, proceeded)


def _exactly(message: str) -> str:
    """Anchor a clause's COMPLETE message (docs/generated-testing.md
    "Per-clause proof")."""
    return "^" + re.escape(message) + "$"


class TestCrossRequestGuardCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker clause must trip on a planted
    violating ``proceeded`` dict, with ITS OWN message, while the world's
    OTHER (real) outcome stays untouched."""

    def test_g1_double_claim_clause_trips_with_its_own_message(self):
        world = GuardWorld(cycles=(
            GuardCycle(attempts=(
                GuardAttempt(1, (("p0", "a.flac"),), "downloading"),
                GuardAttempt(2, (("p0", "a.flac"),), "downloading"),
            )),
        ))
        real = _run_world(world)
        self.assertEqual(real, {(0, 1): True, (0, 2): False})
        # Plant the violation: falsely claim request 2 also proceeded.
        planted = dict(real)
        planted[(0, 2)] = True

        with self.assertRaisesRegex(AssertionError, _exactly(
            f"double-claim: (0, 2) proceeded despite a predicted "
            f"conflict (world={world!r} proceeded={planted!r})",
        )):
            assert_no_double_claim(world, planted)
        # G2 must NOT trip on this same planted dict -- it only checks
        # requests the model predicted free, and (0, 2) was never free.
        assert_no_over_blocking(world, planted)

    def test_g2_over_blocking_clause_trips_with_its_own_message(self):
        world = GuardWorld(cycles=(
            GuardCycle(attempts=(
                GuardAttempt(1, (("p0", "a.flac"),), "downloading"),
                GuardAttempt(2, (("p1", "b.flac"),), "downloading"),
            )),
        ))
        real = _run_world(world)
        self.assertEqual(real, {(0, 1): True, (0, 2): True})
        # Plant the violation: falsely claim request 2 was blocked.
        planted = dict(real)
        planted[(0, 2)] = False

        with self.assertRaisesRegex(AssertionError, _exactly(
            f"over-blocked: (0, 2) was skipped despite free keys "
            f"(world={world!r} proceeded={planted!r})",
        )):
            assert_no_over_blocking(world, planted)
        # G1 must NOT trip on this same planted dict -- it only checks
        # requests that proceeded, and (0, 2) did not.
        assert_no_double_claim(world, planted)


class TestCrossRequestGuardDecisivePins(unittest.TestCase):
    """Direct assertions on the decisive-arm pins above, independent of
    the property -- documents exactly what each world proves."""

    def test_same_cycle_collision_only_first_wins(self):
        self.assertEqual(
            _run_world(_SAME_CYCLE_COLLISION),
            {(0, 1): True, (0, 2): False},
        )

    def test_cross_cycle_downloading_owner_blocks_next_cycle(self):
        self.assertEqual(
            _run_world(_CROSS_CYCLE_DOWNLOADING_BLOCKS),
            {(0, 1): True, (1, 2): False},
        )

    def test_replace_lineage_owner_does_not_block_next_cycle(self):
        self.assertEqual(
            _run_world(_REPLACE_LINEAGE_DOES_NOT_BLOCK),
            {(0, 1): True, (1, 2): True},
        )

    def test_imported_owner_does_not_block_next_cycle(self):
        self.assertEqual(
            _run_world(_IMPORTED_OWNER_DOES_NOT_BLOCK),
            {(0, 1): True, (1, 2): True},
        )

    def test_abandoned_attempt_does_not_block_but_current_attempt_does(self):
        """#1178 PR2 review F2: request 1's OLD, superseded attempt never
        blocks request 2; only request 1's CURRENT attempt could. With
        ``state_has_fingerprint`` defaulting True, this now resolves
        through the #1196 item 1 fingerprint-equality arm."""
        self.assertEqual(
            _run_world(_ABANDONED_ATTEMPT_DOES_NOT_BLOCK),
            {(0, 1): True, (1, 1): True, (1, 2): True},
        )

    def test_missing_fingerprint_state_blocks_every_historical_key(self):
        """#1199 item 2: the same reproduction with no
        ``attempt_fingerprint`` on the owner's state now fails CLOSED --
        request 2's attempt on the owner's OLD, abandoned key is BLOCKED
        (unlike the fingerprint-scoped
        test_abandoned_attempt_does_not_block_but_current_attempt_does
        above, where the same shape does NOT block), because the deleted
        time-predicate fallback was the only thing that used to rescue
        an abandoned attempt when the state lacks a fingerprint."""
        self.assertEqual(
            _run_world(_MISSING_FINGERPRINT_STATE_BLOCKS_EVERY_HISTORICAL_KEY),
            {(0, 1): True, (1, 1): True, (1, 2): False},
        )


if __name__ == "__main__":
    unittest.main()
