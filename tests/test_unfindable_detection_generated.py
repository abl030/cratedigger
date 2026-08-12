"""Generated properties for the unfindable-detection submit-resilience
invariants (issue #1090).

2026-08-12 root cause: a ~3s burst of 49/50 unfindable-probe submits got
HTTP 409 from ``POST /api/v0/searches`` when slskd's Soulseek connection
reset and reconnected; the oneshot still exited 0 having lost half its
cohort. The fix's three invariants (see ``lib.search_exec.execute_search``'s
bounded ``submit_retry``, ``UnfindableDetectionService.categorise_due_batch``'s
circuit breaker, and ``scripts.run_unfindable_detection._process_batch``'s
exit code):

* **I-A** — a transient slskd submit rejection never consumes a cohort
  slot: a candidate is either cleanly PROBED (submitted, whether the
  probe ultimately succeeded or failed) or its row is left completely
  BYTE-UNTOUCHED for the next run. Nothing in between.
* **I-B** — no probe outcome that is not a clean terminal harvest ever
  writes ``last_artist_probe_at`` / ``last_artist_probe_match_count`` /
  ``unfindable_category``. A ``RESULT_PROBE_FAILED`` outcome (including
  one caused by an exhausted submit-retry budget) writes nothing.
* **I-C** — a run that did not classify its whole batch (the circuit
  breaker tripped) is distinguishable, by process exit code, from a
  fully classified run — without parking anything on any request row.

Two properties drive the REAL production code:

1. ``test_batch_write_invariants_hold_over_submit_failure_patterns`` runs
   the REAL ``UnfindableDetectionService.categorise_due_batch`` over the
   REAL production ``run_artist_probe`` (``_fast_probe_runner`` only
   injects a no-op sleep so a generated example doesn't really wait out
   the 2s/5s/10s backoff schedule — the retry/backoff/settle LOGIC in
   ``lib.search_exec.execute_search`` is unmodified and fully exercised)
   against generated per-candidate 409-burst patterns, checking I-A and
   I-B.
2. ``test_exit_code_matches_batch_completeness`` runs the REAL
   ``scripts.run_unfindable_detection._process_batch`` over the same
   world shapes, checking I-C.

Fakes are leaf-seam only: ``FakePipelineDB`` (the DB) and ``FakeSlskdAPI``
(the slskd HTTP boundary, seeded with a real ``requests.HTTPError(409)``
per test-fidelity Rule B). Deterministic pins for the same invariants live
in ``tests/test_unfindable_detection_service.py`` (pins (a)/(b)/(c)) and
``tests/test_run_unfindable_detection.py`` (pin (d)).
"""

from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.unfindable_detection_service import (
    PROBE_SUBMIT_RETRY_MAX_ATTEMPTS,
    RESULT_PROBE_FAILED,
    ArtistProbeResult,
    UnfindableBatchResult,
    UnfindableDetectionService,
    run_artist_probe,
)
from scripts.run_unfindable_detection import EXIT_INCOMPLETE_RUN, _process_batch
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import make_requests_http_error


def _fast_probe_runner(
    slskd_client: FakeSlskdAPI, *, artist_name: str,
    db: FakePipelineDB, request_id: int | None = None,
) -> ArtistProbeResult:
    """The REAL production ``run_artist_probe`` with its retry/settle/
    watchdog sleeps injected as no-ops -- a batch has up to 6 candidates
    and each generated example draws a fresh batch (up to 150 examples
    per property), and the production backoff schedule alone is
    2s/5s/10s real wall time per retried candidate otherwise."""
    return run_artist_probe(
        slskd_client, artist_name=artist_name, db=db, request_id=request_id,
        poll_sleep=lambda _s: None,
    )


# ---------------------------------------------------------------------------
# World model.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CandidateWorld:
    """One cohort member's submit-behaviour: the number of leading HTTP
    409s its slskd submit hits before either succeeding or (once
    ``submit_burst_len >= PROBE_SUBMIT_RETRY_MAX_ATTEMPTS``) exhausting
    the probe's own bounded retry budget."""

    submit_burst_len: int


@dataclass(frozen=True)
class BatchWorld:
    candidates: tuple[CandidateWorld, ...]


_MAX_BURST = PROBE_SUBMIT_RETRY_MAX_ATTEMPTS + 2  # covers both sides of the budget


@st.composite
def candidate_worlds(draw, *, count: int) -> tuple[CandidateWorld, ...]:
    return tuple(
        CandidateWorld(
            submit_burst_len=draw(st.integers(min_value=0, max_value=_MAX_BURST)),
        )
        for _ in range(count)
    )


@st.composite
def batch_worlds(draw) -> BatchWorld:
    n = draw(st.integers(min_value=1, max_value=6))
    return BatchWorld(candidates=draw(candidate_worlds(count=n)))


def _exhausts_budget(cand: CandidateWorld) -> bool:
    return cand.submit_burst_len >= PROBE_SUBMIT_RETRY_MAX_ATTEMPTS


def _build_batch(
    world: BatchWorld,
) -> tuple[FakePipelineDB, FakeSlskdAPI, list[int]]:
    """Seed a fresh ``FakePipelineDB`` + ``FakeSlskdAPI`` for ``world``.

    Each candidate's slice of the shared ``search_text_error_sequence`` is
    concatenated in cohort-processing order (all candidates seed with a
    NULL ``last_artist_probe_at``, so ``FakePipelineDB`` sorts them oldest-
    first by ascending id — the same insertion order used here), so the
    flat sequence lines up with the real per-candidate submit calls
    regardless of how many candidates the circuit breaker ultimately lets
    the batch reach.
    """
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    rids: list[int] = []
    error_sequence: list[Exception | None] = []
    for i, cand in enumerate(world.candidates):
        rid = db.add_request(
            artist_name=f"Artist{i}",
            album_title=f"Album{i}",
            source="request",
            mb_release_id=f"mb-gen-{i}",
        )
        db.set_tracks(rid, [
            {"disc_number": 1, "track_number": 1, "title": "T1"},
            {"disc_number": 1, "track_number": 2, "title": "T2"},
        ])
        rids.append(rid)
        attempts_that_fail = min(cand.submit_burst_len, PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
        error_sequence.extend(
            make_requests_http_error("conflict", status_code=409)
            for _ in range(attempts_that_fail)
        )
        if not _exhausts_budget(cand):
            error_sequence.append(None)  # the attempt after the burst succeeds
    slskd.searches.search_text_error_sequence = error_sequence
    return db, slskd, rids


def _snapshot(db: FakePipelineDB, rids: list[int]) -> dict[int, dict[str, object]]:
    return {rid: dict(db.request(rid)) for rid in rids}


# ---------------------------------------------------------------------------
# Invariant checkers (module-level so the known-bad self-tests can call
# them directly).
# ---------------------------------------------------------------------------


def assert_no_batch_slot_lost(
    world: BatchWorld,
    rids: list[int],
    batch: UnfindableBatchResult,
    before: dict[int, dict[str, object]],
    after: dict[int, dict[str, object]],
) -> None:
    """I-A: every candidate is either cleanly PROBED (attempted, whatever
    the outcome) or its row is left completely byte-untouched."""
    attempted_ids = {r.request_id for r in batch.results}
    for i, rid in enumerate(rids):
        if rid in attempted_ids:
            continue
        if before[rid] != after[rid]:
            raise AssertionError(
                f"candidate index {i} (request {rid}, world="
                f"{world.candidates[i]}) was never attempted this run but "
                f"its row changed: before={before[rid]} after={after[rid]}"
            )


def assert_probe_failed_writes_nothing(
    rids: list[int],
    batch: UnfindableBatchResult,
    before: dict[int, dict[str, object]],
    after: dict[int, dict[str, object]],
) -> None:
    """I-B: a RESULT_PROBE_FAILED outcome never writes the probe/category
    columns -- the conservative rule holds for every attempted-but-failed
    candidate, submit-exhaustion included."""
    results_by_rid = {r.request_id: r for r in batch.results}
    watched = (
        "last_artist_probe_at", "last_artist_probe_match_count",
        "unfindable_category",
    )
    for rid in rids:
        result = results_by_rid.get(rid)
        if result is None or result.outcome != RESULT_PROBE_FAILED:
            continue
        for key in watched:
            if before[rid][key] != after[rid][key]:
                raise AssertionError(
                    f"request {rid}: RESULT_PROBE_FAILED but {key} changed "
                    f"{before[rid][key]!r} -> {after[rid][key]!r}"
                )


def assert_breaker_trips_exactly_when_expected(
    world: BatchWorld, batch: UnfindableBatchResult,
) -> None:
    """I-A (precise form): the circuit breaker trips if and only if the
    cohort-processing order contains a run of
    ``CIRCUIT_BREAKER_CONSECUTIVE_SUBMIT_FAILURES`` CONSECUTIVE candidates
    that each individually exhaust their own submit-retry budget -- non-
    consecutive failures (a blip that recovers) must never accumulate
    toward tripping it. Independently derives the expected trip point
    from ``world`` (never reads the production breaker's own counter) so
    a mutant that drops the breaker, miscounts its threshold, or forgets
    to reset on a non-failure outcome is caught."""
    from lib.unfindable_detection_service import (
        CIRCUIT_BREAKER_CONSECUTIVE_SUBMIT_FAILURES as threshold,
    )

    expected_trip_at: int | None = None
    consecutive = 0
    for i, cand in enumerate(world.candidates):
        consecutive = consecutive + 1 if _exhausts_budget(cand) else 0
        if consecutive >= threshold:
            expected_trip_at = i
            break

    if expected_trip_at is not None:
        if not batch.breaker_tripped:
            raise AssertionError(
                f"expected the breaker to trip at candidate index "
                f"{expected_trip_at} (world={world}) but "
                f"batch.breaker_tripped=False"
            )
        if len(batch.results) != expected_trip_at + 1:
            raise AssertionError(
                f"breaker tripped but attempted {len(batch.results)} "
                f"candidates, expected exactly {expected_trip_at + 1} "
                f"(world={world})"
            )
    else:
        if batch.breaker_tripped:
            raise AssertionError(
                f"breaker tripped but no {threshold}-consecutive-failure "
                f"run exists in world={world}"
            )
        if len(batch.results) != len(world.candidates):
            raise AssertionError(
                f"breaker did not trip but only {len(batch.results)}/"
                f"{len(world.candidates)} candidates were attempted "
                f"(world={world})"
            )


def assert_exit_code_matches_completeness(
    batch: UnfindableBatchResult, exit_code: int,
) -> None:
    """I-C: exit code distinguishes an incomplete (breaker-tripped) run
    from a fully classified one -- and only those two values ever occur."""
    if batch.breaker_tripped:
        if exit_code != EXIT_INCOMPLETE_RUN:
            raise AssertionError(
                f"breaker tripped but exit_code={exit_code}, "
                f"expected EXIT_INCOMPLETE_RUN={EXIT_INCOMPLETE_RUN}"
            )
    elif exit_code != 0:
        raise AssertionError(
            f"breaker NOT tripped but exit_code={exit_code}, expected 0"
        )


# ---------------------------------------------------------------------------
# Generated properties.
# ---------------------------------------------------------------------------


class TestGeneratedSubmitResiliencePatrol(unittest.TestCase):
    """I-A + I-B over generated per-candidate submit-failure patterns,
    driven through the REAL service and the REAL production probe
    (``run_artist_probe`` -> ``execute_search``'s bounded submit retry)."""

    # Sustained outage: every candidate exhausts its retry budget. Pins
    # the multi-minute-outage shape the circuit breaker exists for.
    @example(world=BatchWorld(candidates=tuple(
        CandidateWorld(submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
        for _ in range(5)
    )))
    # No failures at all: a fully clean run never trips the breaker.
    @example(world=BatchWorld(candidates=(
        CandidateWorld(submit_burst_len=0),
        CandidateWorld(submit_burst_len=1),
        CandidateWorld(submit_burst_len=2),
        CandidateWorld(submit_burst_len=0),
    )))
    # A transient blip that recovers inside the retry budget must never
    # trip the breaker on its own.
    @example(world=BatchWorld(candidates=(
        CandidateWorld(submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS - 1),
        CandidateWorld(submit_burst_len=0),
    )))
    @given(world=batch_worlds())
    def test_batch_write_invariants_hold_over_submit_failure_patterns(
        self, world: BatchWorld,
    ) -> None:
        db, slskd, rids = _build_batch(world)
        before = _snapshot(db, rids)

        svc = UnfindableDetectionService(db, slskd, probe_runner=_fast_probe_runner)
        batch = svc.categorise_due_batch(limit=100)

        after = _snapshot(db, rids)
        assert_no_batch_slot_lost(world, rids, batch, before, after)
        assert_probe_failed_writes_nothing(rids, batch, before, after)
        assert_breaker_trips_exactly_when_expected(world, batch)

    @given(world=batch_worlds())
    def test_exit_code_matches_batch_completeness(self, world: BatchWorld) -> None:
        db, slskd, _rids = _build_batch(world)
        svc = UnfindableDetectionService(db, slskd, probe_runner=_fast_probe_runner)
        # A parallel, freshly-seeded batch (same world, independent fakes)
        # so the exit-code decision is checked against its own real
        # categorise_due_batch call rather than double-consuming shared
        # fake state from a prior run in this example.
        exit_code = _process_batch(svc, limit=100)

        db2, slskd2, _rids2 = _build_batch(world)
        svc2 = UnfindableDetectionService(db2, slskd2, probe_runner=_fast_probe_runner)
        batch2 = svc2.categorise_due_batch(limit=100)

        assert_exit_code_matches_completeness(batch2, exit_code)


# ---------------------------------------------------------------------------
# Known-bad self-tests: each checker must trip on a planted violation.
# ---------------------------------------------------------------------------


class TestInvariantCheckersTripOnViolations(unittest.TestCase):

    def test_no_batch_slot_lost_trips_when_untouched_row_changed(self) -> None:
        world = BatchWorld(candidates=(CandidateWorld(submit_burst_len=0),))
        batch = UnfindableBatchResult(results=[], candidates_considered=1)
        before: dict[int, dict[str, object]] = {7: {"unfindable_category": None}}
        after: dict[int, dict[str, object]] = {
            7: {"unfindable_category": "artist_absent"},  # never attempted, yet changed
        }
        with self.assertRaises(AssertionError):
            assert_no_batch_slot_lost(world, [7], batch, before, after)

    def test_no_batch_slot_lost_passes_when_attempted_row_changes(self) -> None:
        from lib.unfindable_detection_service import (
            RESULT_CATEGORISED,
            UnfindableServiceResult,
        )
        world = BatchWorld(candidates=(CandidateWorld(submit_burst_len=0),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_CATEGORISED, request_id=7)],
            candidates_considered=1,
        )
        before: dict[int, dict[str, object]] = {7: {"unfindable_category": None}}
        after: dict[int, dict[str, object]] = {
            7: {"unfindable_category": "artist_absent"},
        }
        assert_no_batch_slot_lost(world, [7], batch, before, after)  # must not raise

    def test_probe_failed_writes_nothing_trips_when_column_written(self) -> None:
        from lib.unfindable_detection_service import UnfindableServiceResult
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_PROBE_FAILED, request_id=7,
                error_message="SearchSubmitError: exhausted")],
            candidates_considered=1,
        )
        before: dict[int, dict[str, object]] = {7: {
            "last_artist_probe_at": None,
            "last_artist_probe_match_count": None,
            "unfindable_category": None,
        }}
        after: dict[int, dict[str, object]] = {7: {
            "last_artist_probe_at": "2026-08-12",
            "last_artist_probe_match_count": 0,
            "unfindable_category": None,
        }}
        with self.assertRaises(AssertionError):
            assert_probe_failed_writes_nothing([7], batch, before, after)

    def test_exit_code_checker_trips_when_incomplete_run_reports_zero(self) -> None:
        batch = UnfindableBatchResult(
            results=[], candidates_considered=5, breaker_tripped=True)
        with self.assertRaises(AssertionError):
            assert_exit_code_matches_completeness(batch, 0)

    def test_exit_code_checker_trips_when_complete_run_reports_nonzero(self) -> None:
        batch = UnfindableBatchResult(
            results=[], candidates_considered=1, breaker_tripped=False)
        with self.assertRaises(AssertionError):
            assert_exit_code_matches_completeness(batch, EXIT_INCOMPLETE_RUN)

    def test_breaker_expectation_trips_when_consecutive_run_did_not_trip(
        self,
    ) -> None:
        """3 consecutive exhausted candidates, but the batch (planted)
        never tripped -- the checker must catch a disabled/broken
        breaker even though no row was mutated."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
            for _ in range(4)
        ))
        from lib.unfindable_detection_service import UnfindableServiceResult
        batch = UnfindableBatchResult(
            results=[
                UnfindableServiceResult(
                    outcome=RESULT_PROBE_FAILED, request_id=i,
                    error_message="SearchSubmitError: exhausted")
                for i in range(4)
            ],
            candidates_considered=4,
            breaker_tripped=False,  # planted bug: should have tripped at index 2
        )
        with self.assertRaises(AssertionError):
            assert_breaker_trips_exactly_when_expected(world, batch)

    def test_breaker_expectation_trips_when_non_consecutive_failures_trip_it(
        self,
    ) -> None:
        """A non-consecutive failure pattern (exhaust, success, exhaust,
        success, exhaust -- 3 total but never 3 IN A ROW) must NOT trip
        the breaker; a planted batch claiming it tripped is a violation."""
        world = BatchWorld(candidates=(
            CandidateWorld(submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
            CandidateWorld(submit_burst_len=0),
            CandidateWorld(submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
            CandidateWorld(submit_burst_len=0),
            CandidateWorld(submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
        ))
        batch = UnfindableBatchResult(
            results=[], candidates_considered=5, breaker_tripped=True,
        )
        with self.assertRaises(AssertionError):
            assert_breaker_trips_exactly_when_expected(world, batch)


if __name__ == "__main__":
    unittest.main()
