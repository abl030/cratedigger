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
  ``unfindable_category``. A ``RESULT_PROBE_FAILED`` outcome writes
  nothing, regardless of WHICH failure kind produced it.
* **I-C** — a run that did not classify its whole batch (the circuit
  breaker tripped) is distinguishable, by process exit code, from a
  fully classified run — without parking anything on any request row.

Review round 1 (BLOCKING-1) sharpened I-A/I-B: the circuit breaker must
count ONLY a submit failure whose typed ``submit_retry_exhausted``
discriminator is True (a retryable 409 that persisted through the FULL
retry budget — a genuine transient slskd-connectivity signal), never a
DETERMINISTIC per-row rejection (a 429 rate limit, or the empty-
``artist_name`` guard) that would otherwise recur identically on every
future run and permanently sit at the head of the cohort query
(``ORDER BY last_artist_probe_at NULLS FIRST`` — a failed probe never
advances that column). The world model below generates ALL THREE
candidate kinds so this composition is patrolled directly, not just
argued about.

Two properties drive the REAL production code:

1. ``test_batch_write_invariants_hold_over_submit_failure_patterns`` runs
   the REAL ``UnfindableDetectionService.categorise_due_batch`` over the
   REAL production ``run_artist_probe`` (``_fast_probe_runner`` only
   injects a no-op sleep so a generated example doesn't really wait out
   the 2s/5s/10s backoff schedule — the retry/backoff/settle LOGIC in
   ``lib.search_exec.execute_search`` is unmodified and fully exercised)
   against generated per-candidate submit-failure patterns (409 bursts of
   varying length, deterministic 429s, empty-artist_name guard fires,
   and a varying server-readiness state), checking I-A and I-B.
2. ``test_exit_code_matches_batch_completeness`` runs the REAL
   ``scripts.run_unfindable_detection._process_batch`` over the same
   world shapes (one real run; the expected outcome is independently
   derived from the world, never a second live batch), checking I-C.

Fakes are leaf-seam only: ``FakePipelineDB`` (the DB) and ``FakeSlskdAPI``
(the slskd HTTP boundary, seeded with real ``requests.HTTPError`` instances
per test-fidelity Rule B, keyed per-``searchText`` so the mapping stays
correct regardless of how many submits a candidate actually makes — issue
#1090 NIT-9). Deterministic pins for the same invariants live in
``tests/test_unfindable_detection_service.py`` (pins (a)/(b)/(c) plus the
BLOCKING-1 regression pins) and ``tests/test_run_unfindable_detection.py``
(pin (d)).
"""

from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass
from typing import Literal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.unfindable_detection_service import (
    CIRCUIT_BREAKER_CONSECUTIVE_SUBMIT_FAILURES,
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

CandidateKind = Literal["retryable_409", "deterministic_429", "empty_artist_name"]
_CANDIDATE_KINDS: tuple[CandidateKind, ...] = (
    "retryable_409", "deterministic_429", "empty_artist_name",
)


@dataclass(frozen=True)
class CandidateWorld:
    """One cohort member's submit behaviour.

    * ``retryable_409`` — the transient shape the retry policy targets.
      ``submit_burst_len`` leading 409s before either succeeding (burst <
      the retry budget) or exhausting the budget (burst >= it) --
      ``submit_retry_exhausted=True`` ONLY in the latter case.
    * ``deterministic_429`` — slskd's real rate limiter. Never retried,
      never "exhausted" (there was no retryable failure to exhaust).
    * ``empty_artist_name`` — the request row has a NULL/blank
      ``artist_name``; ``run_artist_probe``'s guard fires before any POST.

    Both non-``retryable_409`` kinds are DETERMINISTIC: identical every
    day forever for the same row. Neither may ever count toward the
    circuit breaker (issue #1090 BLOCKING-1).
    """

    kind: CandidateKind
    submit_burst_len: int = 0  # only meaningful for kind="retryable_409"


@dataclass(frozen=True)
class BatchWorld:
    candidates: tuple[CandidateWorld, ...]
    # Whole-batch slskd server-readiness state (issue #1090 NON-BLOCKING-5):
    # True mirrors the fake's own default (isConnected=True,
    # isLoggedIn=True); False mirrors the incident's exact mid-reconnect
    # window (isConnected=True, isLoggedIn=False) for every readiness
    # check during this run.
    server_ready: bool = True


_MAX_BURST = PROBE_SUBMIT_RETRY_MAX_ATTEMPTS + 2  # covers both sides of the budget


@st.composite
def candidate_worlds(draw, *, count: int) -> tuple[CandidateWorld, ...]:
    worlds = []
    for _ in range(count):
        kind = draw(st.sampled_from(_CANDIDATE_KINDS))
        burst = (
            draw(st.integers(min_value=0, max_value=_MAX_BURST))
            if kind == "retryable_409" else 0
        )
        worlds.append(CandidateWorld(kind=kind, submit_burst_len=burst))
    return tuple(worlds)


@st.composite
def batch_worlds(draw) -> BatchWorld:
    n = draw(st.integers(min_value=1, max_value=6))
    return BatchWorld(
        candidates=draw(candidate_worlds(count=n)),
        server_ready=draw(st.booleans()),
    )


def _causes_retry_exhausted_failure(cand: CandidateWorld) -> bool:
    """True when ``cand`` produces a ``SearchSubmitError`` with
    ``retry_exhausted=True`` -- the ONLY candidate shape that may ever
    count toward the circuit breaker (issue #1090 BLOCKING-1)."""
    return (
        cand.kind == "retryable_409"
        and cand.submit_burst_len >= PROBE_SUBMIT_RETRY_MAX_ATTEMPTS
    )


def _expected_trip_index(world: BatchWorld) -> int | None:
    """Index of the candidate whose consecutive retry-exhausted-409 run
    trips the breaker, or None if no such run exists in ``world``.
    Independently derived from the world -- never reads the production
    breaker's own counter."""
    consecutive = 0
    for i, cand in enumerate(world.candidates):
        consecutive = (
            consecutive + 1 if _causes_retry_exhausted_failure(cand) else 0
        )
        if consecutive >= CIRCUIT_BREAKER_CONSECUTIVE_SUBMIT_FAILURES:
            return i
    return None


def _build_batch(
    world: BatchWorld,
) -> tuple[FakePipelineDB, FakeSlskdAPI, list[int]]:
    """Seed a fresh ``FakePipelineDB`` + ``FakeSlskdAPI`` for ``world``.

    Errors are injected per-``searchText`` (issue #1090 NIT-9) rather than
    as one flat FIFO sequence: an ``empty_artist_name`` candidate makes
    ZERO submit calls (the guard fires first), which would silently
    desynchronise a position-based queue from every candidate after it.
    Keying by the candidate's own (unique) artist name is self-correcting
    regardless of call count or the circuit breaker cutting the batch
    short.
    """
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    # NON-BLOCKING-5: compose the incident's exact readiness state
    # (isConnected=True, isLoggedIn=<server_ready>) so the retry's
    # backoff-floor branch is exercised, not just the always-ready default.
    slskd.server.set_ready(is_connected=True, is_logged_in=world.server_ready)
    rids: list[int] = []
    for i, cand in enumerate(world.candidates):
        artist = "" if cand.kind == "empty_artist_name" else f"Artist{i}"
        rid = db.add_request(
            artist_name=artist,
            album_title=f"Album{i}",
            source="request",
            mb_release_id=f"mb-gen-{i}",
        )
        db.set_tracks(rid, [
            {"disc_number": 1, "track_number": 1, "title": "T1"},
            {"disc_number": 1, "track_number": 2, "title": "T2"},
        ])
        rids.append(rid)
        if cand.kind == "retryable_409":
            attempts_that_fail = min(
                cand.submit_burst_len, PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
            queue: list[Exception | None] = [
                make_requests_http_error("conflict", status_code=409)
                for _ in range(attempts_that_fail)
            ]
            if not _causes_retry_exhausted_failure(cand):
                queue.append(None)  # the attempt after the burst succeeds
            if queue:
                slskd.searches.search_text_error_by_query[artist] = queue
        elif cand.kind == "deterministic_429":
            slskd.searches.search_text_error_by_query[artist] = [
                make_requests_http_error("rate limited", status_code=429),
            ]
        # empty_artist_name: the guard fires before any POST -- nothing to seed.
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
    candidate, whichever failure kind produced it."""
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


def assert_submit_retry_exhausted_matches_world(
    world: BatchWorld, rids: list[int], batch: UnfindableBatchResult,
) -> None:
    """I-B (precise form, issue #1090 BLOCKING-1): a RESULT_PROBE_FAILED
    outcome's typed ``submit_retry_exhausted`` field is True if and ONLY
    if the candidate's world is a ``retryable_409`` that exhausted its
    budget -- never for a deterministic failure (429, empty artist_name),
    however many of those occur."""
    results_by_rid = {r.request_id: r for r in batch.results}
    for i, rid in enumerate(rids):
        result = results_by_rid.get(rid)
        if result is None or result.outcome != RESULT_PROBE_FAILED:
            continue
        expected = _causes_retry_exhausted_failure(world.candidates[i])
        if result.submit_retry_exhausted != expected:
            raise AssertionError(
                f"candidate index {i} (request {rid}, world="
                f"{world.candidates[i]}): submit_retry_exhausted="
                f"{result.submit_retry_exhausted}, expected {expected}"
            )


def assert_breaker_trips_exactly_when_expected(
    world: BatchWorld, batch: UnfindableBatchResult,
) -> None:
    """I-A (precise form): the circuit breaker trips if and only if the
    cohort-processing order contains a run of
    ``CIRCUIT_BREAKER_CONSECUTIVE_SUBMIT_FAILURES`` CONSECUTIVE candidates
    that each individually exhaust their own submit-retry budget on a
    RETRYABLE 409 -- a deterministic failure (429, empty artist_name)
    never counts, however many occur or however they're interleaved, and
    a non-consecutive run (a blip that recovers) must never accumulate
    toward tripping it. Independently derives the expected trip point
    from ``world`` (never reads the production breaker's own counter) so
    a mutant that drops the breaker, miscounts its threshold, widens its
    counted-failure-kind, or forgets to reset on a non-qualifying outcome
    is caught."""
    expected_trip_at = _expected_trip_index(world)

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
                f"breaker tripped but no "
                f"{CIRCUIT_BREAKER_CONSECUTIVE_SUBMIT_FAILURES}-consecutive"
                f"-retry-exhausted-409 run exists in world={world}"
            )
        if len(batch.results) != len(world.candidates):
            raise AssertionError(
                f"breaker did not trip but only {len(batch.results)}/"
                f"{len(world.candidates)} candidates were attempted "
                f"(world={world})"
            )


def assert_exit_code_matches_completeness(
    breaker_tripped: bool, exit_code: int,
) -> None:
    """I-C: exit code distinguishes an incomplete (breaker-tripped) run
    from a fully classified one -- and only those two values ever occur."""
    if breaker_tripped:
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
        CandidateWorld(kind="retryable_409",
                        submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
        for _ in range(5)
    )))
    # No failures at all: a fully clean run never trips the breaker.
    @example(world=BatchWorld(candidates=(
        CandidateWorld(kind="retryable_409", submit_burst_len=0),
        CandidateWorld(kind="retryable_409", submit_burst_len=1),
        CandidateWorld(kind="retryable_409", submit_burst_len=2),
        CandidateWorld(kind="retryable_409", submit_burst_len=0),
    )))
    # A transient blip that recovers inside the retry budget must never
    # trip the breaker on its own.
    @example(world=BatchWorld(candidates=(
        CandidateWorld(kind="retryable_409",
                        submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS - 1),
        CandidateWorld(kind="retryable_409", submit_burst_len=0),
    )))
    # BLOCKING-1 regression: three consecutive DETERMINISTIC 429s must
    # never trip the breaker -- the whole batch is still attempted.
    @example(world=BatchWorld(candidates=tuple(
        CandidateWorld(kind="deterministic_429") for _ in range(3)
    )))
    # BLOCKING-1 regression: three consecutive empty-artist_name guard
    # fires must never trip the breaker either.
    @example(world=BatchWorld(candidates=tuple(
        CandidateWorld(kind="empty_artist_name") for _ in range(3)
    )))
    # BLOCKING-1 regression: retry-exhausted 409s INTERLEAVED with
    # deterministic 429s are never 3 CONSECUTIVE retry-exhausted
    # failures -- must not trip despite 3 total exhausted candidates.
    @example(world=BatchWorld(candidates=(
        CandidateWorld(kind="retryable_409",
                        submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
        CandidateWorld(kind="deterministic_429"),
        CandidateWorld(kind="retryable_409",
                        submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
        CandidateWorld(kind="deterministic_429"),
        CandidateWorld(kind="retryable_409",
                        submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
    )))
    # Not-ready server state (the incident's exact isLoggedIn=False
    # window) composed with a sustained retry-exhausted-409 outage.
    @example(world=BatchWorld(
        candidates=tuple(
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
            for _ in range(3)
        ),
        server_ready=False,
    ))
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
        assert_submit_retry_exhausted_matches_world(world, rids, batch)
        assert_breaker_trips_exactly_when_expected(world, batch)

    @given(world=batch_worlds())
    def test_exit_code_matches_batch_completeness(self, world: BatchWorld) -> None:
        db, slskd, _rids = _build_batch(world)
        svc = UnfindableDetectionService(db, slskd, probe_runner=_fast_probe_runner)
        # ONE real run (issue #1090 NIT-10) -- the expected outcome is
        # independently derived from the world, not a second live batch.
        exit_code = _process_batch(svc, limit=100)
        expected_tripped = _expected_trip_index(world) is not None
        assert_exit_code_matches_completeness(expected_tripped, exit_code)


# ---------------------------------------------------------------------------
# Known-bad self-tests: each checker must trip on a planted violation.
# ---------------------------------------------------------------------------


class TestInvariantCheckersTripOnViolations(unittest.TestCase):

    def test_no_batch_slot_lost_trips_when_untouched_row_changed(self) -> None:
        world = BatchWorld(candidates=(
            CandidateWorld(kind="retryable_409", submit_burst_len=0),))
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
        world = BatchWorld(candidates=(
            CandidateWorld(kind="retryable_409", submit_burst_len=0),))
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
                error_message="SearchSubmitError: exhausted",
                submit_retry_exhausted=True)],
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

    def test_submit_retry_exhausted_checker_trips_on_deterministic_mismatch(
        self,
    ) -> None:
        """A 429 (deterministic) result planted with
        submit_retry_exhausted=True must trip the checker."""
        from lib.unfindable_detection_service import UnfindableServiceResult
        world = BatchWorld(candidates=(
            CandidateWorld(kind="deterministic_429"),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_PROBE_FAILED, request_id=7,
                submit_retry_exhausted=True)],  # planted bug
            candidates_considered=1,
        )
        with self.assertRaises(AssertionError):
            assert_submit_retry_exhausted_matches_world(world, [7], batch)

    def test_submit_retry_exhausted_checker_trips_on_exhausted_mismatch(
        self,
    ) -> None:
        """A budget-exhausted retryable_409 result planted with
        submit_retry_exhausted=False must trip the checker."""
        from lib.unfindable_detection_service import UnfindableServiceResult
        world = BatchWorld(candidates=(
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_PROBE_FAILED, request_id=7,
                submit_retry_exhausted=False)],  # planted bug
            candidates_considered=1,
        )
        with self.assertRaises(AssertionError):
            assert_submit_retry_exhausted_matches_world(world, [7], batch)

    def test_exit_code_checker_trips_when_incomplete_run_reports_zero(self) -> None:
        with self.assertRaises(AssertionError):
            assert_exit_code_matches_completeness(True, 0)

    def test_exit_code_checker_trips_when_complete_run_reports_nonzero(self) -> None:
        with self.assertRaises(AssertionError):
            assert_exit_code_matches_completeness(False, EXIT_INCOMPLETE_RUN)

    def test_breaker_expectation_trips_when_consecutive_run_did_not_trip(
        self,
    ) -> None:
        """3 consecutive exhausted candidates, but the batch (planted)
        never tripped -- the checker must catch a disabled/broken
        breaker even though no row was mutated."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
            for _ in range(4)
        ))
        from lib.unfindable_detection_service import UnfindableServiceResult
        batch = UnfindableBatchResult(
            results=[
                UnfindableServiceResult(
                    outcome=RESULT_PROBE_FAILED, request_id=i,
                    error_message="SearchSubmitError: exhausted",
                    submit_retry_exhausted=True)
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
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
            CandidateWorld(kind="retryable_409", submit_burst_len=0),
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
            CandidateWorld(kind="retryable_409", submit_burst_len=0),
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),
        ))
        batch = UnfindableBatchResult(
            results=[], candidates_considered=5, breaker_tripped=True,
        )
        with self.assertRaises(AssertionError):
            assert_breaker_trips_exactly_when_expected(world, batch)

    def test_breaker_expectation_trips_when_deterministic_failures_trip_it(
        self,
    ) -> None:
        """Issue #1090 BLOCKING-1 known-bad case: 3 consecutive
        DETERMINISTIC 429s planted as having tripped the breaker is a
        violation -- only retry-exhausted 409s may ever trip it."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(kind="deterministic_429") for _ in range(3)
        ))
        batch = UnfindableBatchResult(
            results=[], candidates_considered=3, breaker_tripped=True,
        )
        with self.assertRaises(AssertionError):
            assert_breaker_trips_exactly_when_expected(world, batch)


if __name__ == "__main__":
    unittest.main()
