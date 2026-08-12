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
  nothing, regardless of WHICH failure kind produced it -- and a
  DETERMINISTIC failure kind (429, empty artist_name) must always END
  UP as ``RESULT_PROBE_FAILED`` when attempted, never a silent success.
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

Review round 2 widened this further, per issue #1090's per-clause audit
(``.claude/rules/code-quality.md`` § "known-bad self-test... per CLAUSE"):

* **F1** — ``CandidateWorld`` gained ``trailing_kind`` so a
  ``retryable_409`` candidate's FINAL consumed attempt can independently
  be a success, another 409, or a non-retryable 429 -- the mixed
  "409, 409, then 429 on the last attempt" world is the ONLY shape that
  falsifies the mutant that drops the ``is_retryable_409`` term from
  ``retry_exhausted``'s computation (a uniform all-409 burst can't tell
  the two formulas apart, since both agree once the final attempt really
  is a 409).
* **F4** — ``deterministic_candidate_silent_success_violations`` is
  a new checker that closes a blind spot the original checkers left:
  removing the empty-``artist_name`` guard makes the candidate submit
  ``searchText=""`` successfully against the fake and WRITE a real row
  from a blank search -- attempted, so I-A's checker is silent; not
  ``RESULT_PROBE_FAILED``, so the write-conservation checker is silent
  too.

Two properties drive the REAL production code:

1. ``test_batch_write_invariants_hold_over_submit_failure_patterns`` runs
   the REAL ``UnfindableDetectionService.categorise_due_batch`` over the
   REAL production ``run_artist_probe`` (``_fast_probe_runner`` only
   injects a no-op sleep so a generated example doesn't really wait out
   the 2s/5s backoff schedule — the retry/backoff/settle LOGIC in
   ``lib.search_exec.execute_search`` is unmodified and fully exercised)
   against generated per-candidate submit-failure patterns (409 bursts of
   varying length and varying final-attempt kind, deterministic 429s,
   empty-artist_name guard fires, and a varying server-readiness state),
   checking I-A and I-B.
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
    RESULT_CATEGORISED,
    RESULT_NO_CHANGE,
    RESULT_PROBE_FAILED,
    ArtistProbeResult,
    UnfindableBatchResult,
    UnfindableDetectionService,
    UnfindableServiceResult,
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
    2s/5s real wall time per retried candidate otherwise."""
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
TrailingKind = Literal["success", "429"]


@dataclass(frozen=True)
class CandidateWorld:
    """One cohort member's submit behaviour.

    * ``retryable_409`` — the transient shape the retry policy targets.
      ``submit_burst_len`` leading 409s, then:

      - if ``submit_burst_len >= PROBE_SUBMIT_RETRY_MAX_ATTEMPTS``, the
        burst alone exhausts the budget (every attempt was a 409) --
        ``submit_retry_exhausted=True``, ``trailing_kind`` is moot (no
        attempts remain to carry it).
      - otherwise, the NEXT (final consumed) attempt is ``trailing_kind``:
        ``"success"`` (the pre-existing "burst then succeeds" shape) or
        ``"429"`` (issue #1090 F1 -- a DETERMINISTIC failure lands on
        what may be the last attempt in the budget; this is the world
        that falsifies a mutant computing ``retry_exhausted`` from
        ``is_last_attempt`` alone, ignoring whether that final attempt
        was actually a retryable 409).
    * ``deterministic_429`` — slskd's real rate limiter. Never retried,
      never "exhausted" (there was no retryable failure to exhaust).
    * ``empty_artist_name`` — the request row has a NULL/blank
      ``artist_name``; ``run_artist_probe``'s guard fires before any POST.

    Both non-``retryable_409`` kinds are DETERMINISTIC: identical every
    day forever for the same row. Neither may ever count toward the
    circuit breaker (issue #1090 BLOCKING-1), and both must always END
    UP ``RESULT_PROBE_FAILED`` when attempted (issue #1090 F4).
    """

    kind: CandidateKind
    submit_burst_len: int = 0  # only meaningful for kind="retryable_409"
    trailing_kind: TrailingKind = "success"  # only meaningful for kind="retryable_409"


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
        if kind == "retryable_409":
            burst = draw(st.integers(min_value=0, max_value=_MAX_BURST))
            trailing = draw(st.sampled_from(("success", "429")))
        else:
            burst = 0
            trailing = "success"
        worlds.append(CandidateWorld(
            kind=kind, submit_burst_len=burst, trailing_kind=trailing))
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
    count toward the circuit breaker (issue #1090 BLOCKING-1). A
    ``trailing_kind="429"`` NEVER exhausts the budget in the
    retry-exhausted sense, however late it lands -- the final attempt
    that actually failed was not a retryable 409 (issue #1090 F1)."""
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
            attempts_with_409 = min(
                cand.submit_burst_len, PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
            queue: list[Exception | None] = [
                make_requests_http_error("conflict", status_code=409)
                for _ in range(attempts_with_409)
            ]
            if attempts_with_409 < PROBE_SUBMIT_RETRY_MAX_ATTEMPTS:
                # F1: the final consumed attempt independently varies.
                if cand.trailing_kind == "success":
                    queue.append(None)
                else:  # "429"
                    queue.append(
                        make_requests_http_error("rate limited", status_code=429))
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


def deterministic_candidate_silent_success_violations(
    world: BatchWorld, rids: list[int], batch: UnfindableBatchResult,
) -> list[str]:
    """I-B (issue #1090 review round 2, F4): an ATTEMPTED deterministic-
    failure candidate (``deterministic_429``, ``empty_artist_name``) must
    yield ``RESULT_PROBE_FAILED`` -- never a real "success" outcome.

    This is the clause that catches removal of the empty-``artist_name``
    guard itself: with the guard gone, an empty ``searchText=""`` submits
    SUCCESSFULLY against the fake, and ``categorise_request``'s
    ``record_artist_probe`` would write a REAL row from a blank search --
    a write no other checker in this module catches, because the
    candidate WAS attempted (so ``assert_no_batch_slot_lost`` is silent)
    and its outcome is not ``RESULT_PROBE_FAILED`` (so
    ``assert_probe_failed_writes_nothing`` is silent too, since it only
    ever checks the ``RESULT_PROBE_FAILED`` branch).

    Accumulates every violation (new checker; prefers the list[str] shape
    per the per-clause rule) rather than raising on the first.
    """
    violations: list[str] = []
    results_by_rid = {r.request_id: r for r in batch.results}
    for i, rid in enumerate(rids):
        cand = world.candidates[i]
        if cand.kind not in ("deterministic_429", "empty_artist_name"):
            continue
        result = results_by_rid.get(rid)
        if result is None:
            continue  # never attempted (breaker cut it short) -- I-A's job
        if result.outcome != RESULT_PROBE_FAILED:
            violations.append(
                f"candidate index {i} (request {rid}, kind={cand.kind}) "
                f"was attempted but outcome={result.outcome!r}, expected "
                f"RESULT_PROBE_FAILED"
            )
    return violations


def breaker_trip_expectation_violations(
    world: BatchWorld, batch: UnfindableBatchResult,
) -> list[str]:
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
    is caught.

    Returns every way ``batch`` breaks the invariant as an ACCUMULATING
    list (issue #1090 review round 2, F3) -- every clause evaluates
    regardless of the others, so a world violating more than one clause
    can never mask one from a self-test asserting a specific clause's
    message. Four clauses, named C1-C4 in their own message so a
    self-test can pin exactly one:

    * C1 -- a qualifying run exists but the breaker did not trip.
    * C2 -- the breaker tripped (C1 satisfied) but attempted the wrong
      candidate count for the derived trip point.
    * C3 -- no qualifying run exists but the breaker tripped anyway.
    * C4 -- the breaker correctly did not trip (C3 satisfied) but did not
      attempt every candidate.

    C4 has a Q1 self-test (a minimal hand-built world proves the clause
    fires) but NO single-point production mutant reaches it today: the real
    loop only stops short of the full candidate list by tripping the
    breaker (which C3 already patrols) or by an early exception, and both
    ``categorise_due_batch`` and ``run_artist_probe`` were re-derived
    (review round 3) to have no other early-exit branch a one-line mutant
    can open. C4 is therefore FAIL-CLOSED LEGISLATION (per #859's "a guard
    over a shared namespace ships with a patrolling property") against a
    FUTURE early-exit the breaker loop might grow, not a mutant-qualified
    clause today -- record this status wherever the kill matrix for this
    checker is reported so an absent single-point kill doesn't read as an
    oversight.
    """
    violations: list[str] = []
    expected_trip_at = _expected_trip_index(world)

    if expected_trip_at is not None:
        if not batch.breaker_tripped:
            violations.append(
                f"C1 breaker-did-not-trip: expected the breaker to trip "
                f"at candidate index {expected_trip_at} (world={world}) "
                f"but batch.breaker_tripped=False"
            )
        if len(batch.results) != expected_trip_at + 1:
            violations.append(
                f"C2 wrong-attempted-count-on-trip: breaker tripped but "
                f"attempted {len(batch.results)} candidates, expected "
                f"exactly {expected_trip_at + 1} (world={world})"
            )
    else:
        if batch.breaker_tripped:
            violations.append(
                f"C3 breaker-tripped-without-cause: no "
                f"{CIRCUIT_BREAKER_CONSECUTIVE_SUBMIT_FAILURES}-consecutive"
                f"-retry-exhausted-409 run exists in world={world}"
            )
        if len(batch.results) != len(world.candidates):
            violations.append(
                f"C4 wrong-attempted-count-without-trip: breaker did not "
                f"trip but only {len(batch.results)}/"
                f"{len(world.candidates)} candidates were attempted "
                f"(world={world})"
            )
    return violations


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
    # F1: 409, then a 429 on the FINAL consumed attempt (burst ==
    # MAX_ATTEMPTS - 1). This is the ONLY world shape that falsifies the
    # mutant dropping is_retryable_409 from retry_exhausted's formula --
    # a uniform all-409 burst can't tell the two formulas apart. Pinned
    # as an @example rather than left to chance because the combination
    # (burst == MAX_ATTEMPTS - 1 AND trailing_kind == "429") is a single
    # point in the joint domain.
    @example(world=BatchWorld(candidates=(
        CandidateWorld(kind="retryable_409",
                        submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS - 1,
                        trailing_kind="429"),
    )))
    # F4: an empty-artist_name candidate must resolve to RESULT_PROBE_FAILED.
    @example(world=BatchWorld(candidates=(
        CandidateWorld(kind="empty_artist_name"),
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
        assert_submit_retry_exhausted_matches_world(world, rids, batch)
        self.assertEqual(
            deterministic_candidate_silent_success_violations(
                world, rids, batch),
            [], (world, batch),
        )
        self.assertEqual(
            breaker_trip_expectation_violations(world, batch),
            [], (world, batch),
        )

    @given(world=batch_worlds())
    def test_exit_code_matches_batch_completeness(self, world: BatchWorld) -> None:
        db, slskd, rids = _build_batch(world)
        svc = UnfindableDetectionService(db, slskd, probe_runner=_fast_probe_runner)
        # ONE real run (issue #1090 NIT-10) -- the expected outcome is
        # independently derived from the world, not a second live batch.
        # cohort_total / due_backlog_at_start (#1112) are run-telemetry
        # inputs the completeness invariant doesn't depend on -- the
        # seeded cohort size is a faithful stand-in for both.
        exit_code = _process_batch(
            svc, db, limit=100,
            cohort_total=len(rids), due_backlog_at_start=len(rids),
        )
        expected_tripped = _expected_trip_index(world) is not None
        assert_exit_code_matches_completeness(expected_tripped, exit_code)


# ---------------------------------------------------------------------------
# Known-bad self-tests: each checker CLAUSE must trip on a planted
# violation (per-clause proof, issue #1090 review round 2 / #1094).
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
        with self.assertRaisesRegex(
            AssertionError, "was never attempted this run but its row changed",
        ):
            assert_no_batch_slot_lost(world, [7], batch, before, after)

    def test_no_batch_slot_lost_passes_when_attempted_row_changes(self) -> None:
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
        with self.assertRaisesRegex(
            AssertionError, r"RESULT_PROBE_FAILED but \S+ changed",
        ):
            assert_probe_failed_writes_nothing([7], batch, before, after)

    def test_submit_retry_exhausted_checker_trips_on_deterministic_mismatch(
        self,
    ) -> None:
        """A 429 (deterministic) result planted with
        submit_retry_exhausted=True must trip the checker."""
        world = BatchWorld(candidates=(
            CandidateWorld(kind="deterministic_429"),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_PROBE_FAILED, request_id=7,
                submit_retry_exhausted=True)],  # planted bug
            candidates_considered=1,
        )
        with self.assertRaisesRegex(
            AssertionError, r"submit_retry_exhausted=True, expected False",
        ):
            assert_submit_retry_exhausted_matches_world(world, [7], batch)

    def test_submit_retry_exhausted_checker_trips_on_exhausted_mismatch(
        self,
    ) -> None:
        """A budget-exhausted retryable_409 result planted with
        submit_retry_exhausted=False must trip the checker."""
        world = BatchWorld(candidates=(
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_PROBE_FAILED, request_id=7,
                submit_retry_exhausted=False)],  # planted bug
            candidates_considered=1,
        )
        with self.assertRaisesRegex(
            AssertionError, r"submit_retry_exhausted=False, expected True",
        ):
            assert_submit_retry_exhausted_matches_world(world, [7], batch)

    def test_exit_code_checker_trips_when_incomplete_run_reports_zero(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, r"breaker tripped but exit_code=0",
        ):
            assert_exit_code_matches_completeness(True, 0)

    def test_exit_code_checker_trips_when_complete_run_reports_nonzero(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, r"breaker NOT tripped but exit_code=\d+",
        ):
            assert_exit_code_matches_completeness(False, EXIT_INCOMPLETE_RUN)

    # -- deterministic_candidate_silent_success_violations (F4) --

    def test_deterministic_429_silently_succeeding_trips_the_checker(self) -> None:
        world = BatchWorld(candidates=(
            CandidateWorld(kind="deterministic_429"),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_CATEGORISED, request_id=7)],  # planted: NOT probe_failed
            candidates_considered=1,
        )
        violations = deterministic_candidate_silent_success_violations(
            world, [7], batch)
        self.assertTrue(
            any("expected RESULT_PROBE_FAILED" in v for v in violations),
            violations,
        )

    def test_empty_artist_name_silently_succeeding_trips_the_checker(self) -> None:
        """The exact regression this checker exists for: with the guard
        removed, an empty-artist_name candidate would resolve to a real
        (non-probe_failed) outcome."""
        world = BatchWorld(candidates=(
            CandidateWorld(kind="empty_artist_name"),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_NO_CHANGE, request_id=7)],  # planted: guard-removed shape
            candidates_considered=1,
        )
        violations = deterministic_candidate_silent_success_violations(
            world, [7], batch)
        self.assertTrue(
            any("expected RESULT_PROBE_FAILED" in v for v in violations),
            violations,
        )

    def test_deterministic_candidate_failing_cleanly_does_not_trip(self) -> None:
        world = BatchWorld(candidates=(
            CandidateWorld(kind="deterministic_429"),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_PROBE_FAILED, request_id=7)],
            candidates_considered=1,
        )
        self.assertEqual(
            deterministic_candidate_silent_success_violations(
                world, [7], batch),
            [],
        )

    def test_retryable_409_candidate_succeeding_is_out_of_scope(self) -> None:
        """A retryable_409 candidate is not one of the two deterministic
        kinds this checker patrols -- a real success outcome for it must
        never trip this checker (that's the normal, expected shape)."""
        world = BatchWorld(candidates=(
            CandidateWorld(kind="retryable_409", submit_burst_len=0),))
        batch = UnfindableBatchResult(
            results=[UnfindableServiceResult(
                outcome=RESULT_CATEGORISED, request_id=7)],
            candidates_considered=1,
        )
        self.assertEqual(
            deterministic_candidate_silent_success_violations(
                world, [7], batch),
            [],
        )

    def test_never_attempted_deterministic_candidate_is_out_of_scope(self) -> None:
        """A deterministic-kind candidate the breaker cut off before
        attempting is I-A's concern (assert_no_batch_slot_lost), not
        this checker's."""
        world = BatchWorld(candidates=(
            CandidateWorld(kind="deterministic_429"),))
        batch = UnfindableBatchResult(results=[], candidates_considered=1)
        self.assertEqual(
            deterministic_candidate_silent_success_violations(
                world, [7], batch),
            [],
        )

    # -- breaker_trip_expectation_violations (C1-C4) --

    def test_breaker_c1_trips_when_expected_trip_did_not_happen(self) -> None:
        """C1 in isolation: a real 3-consecutive-retry-exhausted run
        exists, but the batch claims the breaker never tripped -- with
        the attempted count matching what C2 would ALSO want, so only C1
        fires."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
            for _ in range(3)
        ))
        batch = UnfindableBatchResult(
            results=[
                UnfindableServiceResult(
                    outcome=RESULT_PROBE_FAILED, request_id=i,
                    submit_retry_exhausted=True)
                for i in range(3)
            ],
            candidates_considered=3,
            breaker_tripped=False,  # C1 violation
        )
        violations = breaker_trip_expectation_violations(world, batch)
        self.assertTrue(
            any(v.startswith("C1 breaker-did-not-trip") for v in violations),
            violations,
        )
        self.assertFalse(
            any(v.startswith("C2") for v in violations), violations,
        )

    def test_breaker_c2_trips_when_attempted_count_wrong_on_trip(self) -> None:
        """C2 in isolation: the breaker DID trip (C1 satisfied) but
        attempted a candidate count that doesn't match the derived trip
        point."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(kind="retryable_409",
                            submit_burst_len=PROBE_SUBMIT_RETRY_MAX_ATTEMPTS)
            for _ in range(3)
        ))
        batch = UnfindableBatchResult(
            # Expected trip point is index 2 -> exactly 3 results. Plant 2.
            results=[
                UnfindableServiceResult(
                    outcome=RESULT_PROBE_FAILED, request_id=i,
                    submit_retry_exhausted=True)
                for i in range(2)
            ],
            candidates_considered=3,
            breaker_tripped=True,  # C1 satisfied
        )
        violations = breaker_trip_expectation_violations(world, batch)
        self.assertFalse(
            any(v.startswith("C1") for v in violations), violations,
        )
        self.assertTrue(
            any(v.startswith("C2 wrong-attempted-count-on-trip")
                for v in violations),
            violations,
        )

    def test_breaker_c3_trips_when_no_run_but_tripped(self) -> None:
        """C3 in isolation: no qualifying consecutive run exists, but the
        batch claims the breaker tripped -- with the attempted count
        matching len(candidates) so C4 does not also fire."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(kind="deterministic_429") for _ in range(3)
        ))
        batch = UnfindableBatchResult(
            results=[
                UnfindableServiceResult(outcome=RESULT_PROBE_FAILED, request_id=i)
                for i in range(3)
            ],
            candidates_considered=3,
            breaker_tripped=True,  # C3 violation
        )
        violations = breaker_trip_expectation_violations(world, batch)
        self.assertTrue(
            any(v.startswith("C3 breaker-tripped-without-cause")
                for v in violations),
            violations,
        )
        self.assertFalse(
            any(v.startswith("C4") for v in violations), violations,
        )

    def test_breaker_c4_trips_when_not_all_attempted_without_trip(self) -> None:
        """C4 in isolation: no qualifying run exists and the breaker
        correctly reports NOT tripped (C3 satisfied), but fewer than all
        candidates were attempted."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(kind="deterministic_429") for _ in range(3)
        ))
        batch = UnfindableBatchResult(
            results=[
                UnfindableServiceResult(outcome=RESULT_PROBE_FAILED, request_id=0),
            ],  # only 1 of 3
            candidates_considered=3,
            breaker_tripped=False,  # C3 satisfied
        )
        violations = breaker_trip_expectation_violations(world, batch)
        self.assertFalse(
            any(v.startswith("C3") for v in violations), violations,
        )
        self.assertTrue(
            any(v.startswith("C4 wrong-attempted-count-without-trip")
                for v in violations),
            violations,
        )

    def test_breaker_clean_world_produces_no_violations(self) -> None:
        """Sanity: a batch that matches the world exactly reports zero
        violations from every clause."""
        world = BatchWorld(candidates=tuple(
            CandidateWorld(kind="retryable_409", submit_burst_len=0)
            for _ in range(3)
        ))
        batch = UnfindableBatchResult(
            results=[
                UnfindableServiceResult(outcome=RESULT_CATEGORISED, request_id=i)
                for i in range(3)
            ],
            candidates_considered=3,
            breaker_tripped=False,
        )
        self.assertEqual(
            breaker_trip_expectation_violations(world, batch), [],
        )


if __name__ == "__main__":
    unittest.main()
