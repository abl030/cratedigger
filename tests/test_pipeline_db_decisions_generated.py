"""Generated properties for the shared pipeline-DB decisions.

``lib/pipeline_db/decisions.py`` holds four rules that production and its
in-memory twin used to spell separately. Extracting them makes the rules
directly patrollable, so each ships a property here alongside its
deterministic table in ``tests/test_pipeline_db_decisions.py``.

Every checker accumulates a ``list[str]`` of violations rather than raising,
so a failure names every clause it can rather than only the first. That is
NOT the same as "clause ordering cannot mask a defect", and two of the four
checkers do not accumulate unconditionally:

* ``backoff_violations`` and ``saturation_violations`` evaluate every clause
  on every call — nothing there can be masked by ordering.
* ``cursor_violations`` and ``readiness_violations`` have GUARDED early
  returns plus ``if``/``elif`` chains. An unrecognised status or an
  unregistered bucket returns immediately, because no later clause has a
  defined meaning for such a value; the stale arm returns before the live
  arm; and the wrap/advance and deterministic/transient/no-plan ladders are
  mutually exclusive by construction. So a world CAN fall through a
  disabled clause and be reported by a different clause instead. The
  message-asserting self-tests below are what closes that: each names the
  exact clause it exercises, so a fallthrough fails the assertion rather
  than passing as a generic error.

Some clauses are also genuinely coupled — a run whose values disagree with
the formula usually breaks the range or monotonicity clause too — so each
self-test states the exact number of clauses its world trips rather than
pretending every clause can fire alone.
"""

from __future__ import annotations

import re
import unittest
from dataclasses import replace
from itertools import pairwise

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.pipeline_db._shared import (
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    CURSOR_UPDATE_ADVANCED,
    CURSOR_UPDATE_STALE,
    CURSOR_UPDATE_WRAPPED,
    SEARCH_LOG_STAGE_ACCEPTED,
    SEARCH_LOG_STAGE_STALE_COMPLETION,
    SaturationSummary,
)
from lib.pipeline_db.decisions import (
    PLAN_READINESS_BUCKETS,
    PLAN_READINESS_FAILED_DETERMINISTIC,
    PLAN_READINESS_FAILED_TRANSIENT,
    PLAN_READINESS_LEGACY,
    PLAN_READINESS_NO_PLAN,
    PLAN_READINESS_SEARCHABLE,
    CursorAdvanceDecision,
    search_backoff_minutes,
)
from tests.test_pipeline_db_decisions import (
    CursorWorld,
    ReadinessWorld,
    SaturationCounts,
)

# ---------------------------------------------------------------------------
# Retry pacing
# ---------------------------------------------------------------------------


def backoff_violations(samples: list[tuple[int, int]]) -> list[str]:
    """Check one ascending run of ``(prior_attempts, minutes)`` samples."""
    violations: list[str] = []
    for prior, minutes in samples:
        if minutes < BACKOFF_BASE_MINUTES:
            violations.append(
                f"backoff below the base interval: {prior} -> {minutes}"
            )
        if minutes > BACKOFF_MAX_MINUTES:
            violations.append(f"backoff above the cap: {prior} -> {minutes}")
        if prior == 0 and minutes != BACKOFF_BASE_MINUTES:
            violations.append(
                f"a first attempt must wait the base interval, got {minutes}"
            )
        unclamped = min(
            BACKOFF_BASE_MINUTES * (2 ** prior), BACKOFF_MAX_MINUTES,
        )
        if minutes != unclamped:
            violations.append(
                f"the exponent clamp changed a value: {prior} -> {minutes}, "
                f"want {unclamped}"
            )
    for (low_prior, low), (high_prior, high) in pairwise(samples):
        if high < low:
            violations.append(
                "backoff must never shrink as attempts accumulate: "
                f"{low_prior} -> {low} then {high_prior} -> {high}"
            )
    return violations


# ---------------------------------------------------------------------------
# Consumed-search-attempt cursor
# ---------------------------------------------------------------------------


def cursor_violations(
    world: CursorWorld, decision: CursorAdvanceDecision,
) -> list[str]:
    """Check one cursor decision against the world that produced it."""
    violations: list[str] = []
    status = decision.cursor_update_status
    if status not in {
        CURSOR_UPDATE_STALE, CURSOR_UPDATE_ADVANCED, CURSOR_UPDATE_WRAPPED,
    }:
        violations.append(f"unknown cursor update status: {status!r}")
        return violations

    should_be_stale = (
        world.current_status == "replaced"
        or world.active_plan_id != world.attempt_plan_id
        or world.next_ordinal != world.attempt_plan_ordinal
        or world.cycle_count != world.attempt_cycle_count_snapshot
    )
    if should_be_stale != (status == CURSOR_UPDATE_STALE):
        violations.append(
            "staleness must track the world exactly: want "
            f"stale={should_be_stale}, got status={status!r}"
        )

    if status == CURSOR_UPDATE_STALE:
        if (
            decision.new_next_ordinal != world.next_ordinal
            or decision.new_cycle_count != world.cycle_count
        ):
            violations.append(
                "a stale attempt must freeze the cursor, got ordinal "
                f"{decision.new_next_ordinal} cycle "
                f"{decision.new_cycle_count}"
            )
        want_reason = (
            "request_replaced" if world.current_status == "replaced"
            else "regenerated"
        )
        if decision.stale_reason != want_reason:
            violations.append(
                f"stale reason must be {want_reason!r}, got "
                f"{decision.stale_reason!r}"
            )
        if decision.execution_stage != SEARCH_LOG_STAGE_STALE_COMPLETION:
            violations.append(
                "a stale attempt must be logged as a stale completion, got "
                f"{decision.execution_stage!r}"
            )
        return violations

    if decision.stale_reason is not None:
        violations.append(
            "a live attempt must carry no stale reason, got "
            f"{decision.stale_reason!r}"
        )
    if decision.execution_stage != SEARCH_LOG_STAGE_ACCEPTED:
        violations.append(
            "a live attempt must be logged as accepted, got "
            f"{decision.execution_stage!r}"
        )
    item_count = max(world.attempt_plan_item_count, 0)
    should_wrap = (
        bool(item_count) and world.attempt_plan_ordinal >= item_count - 1
    )
    if should_wrap != (status == CURSOR_UPDATE_WRAPPED):
        violations.append(
            "wrapping must track the plan length exactly: want "
            f"wrapped={should_wrap}, got status={status!r}"
        )
    elif status == CURSOR_UPDATE_WRAPPED:
        if (
            decision.new_next_ordinal != 0
            or decision.new_cycle_count != world.cycle_count + 1
        ):
            violations.append(
                "a wrap must reset to ordinal 0 and open exactly one cycle, "
                f"got ordinal {decision.new_next_ordinal} cycle "
                f"{decision.new_cycle_count}"
            )
    elif (
        decision.new_next_ordinal != world.next_ordinal + 1
        or decision.new_cycle_count != world.cycle_count
    ):
        violations.append(
            "an advance must add exactly one ordinal and hold the cycle, "
            f"got ordinal {decision.new_next_ordinal} cycle "
            f"{decision.new_cycle_count}"
        )
    return violations


# ---------------------------------------------------------------------------
# Plan-readiness buckets
# ---------------------------------------------------------------------------


def readiness_violations(world: ReadinessWorld, bucket: str) -> list[str]:
    """Check one readiness bucket against the facts that produced it."""
    violations: list[str] = []
    if bucket not in PLAN_READINESS_BUCKETS:
        violations.append(f"unregistered readiness bucket: {bucket!r}")
        return violations
    if world.active_plan_generator_id is not None:
        want = (
            PLAN_READINESS_SEARCHABLE
            if world.active_plan_generator_id == world.current_generator_id
            else PLAN_READINESS_LEGACY
        )
        if bucket != want:
            violations.append(
                "a resolved active generator must decide the bucket: want "
                f"{want!r}, got {bucket!r}"
            )
        return violations
    if bucket in {PLAN_READINESS_SEARCHABLE, PLAN_READINESS_LEGACY}:
        violations.append(
            "an unresolved active-plan pointer must not claim an "
            f"active-plan bucket, got {bucket!r}"
        )
        return violations
    if world.has_failed_deterministic:
        if bucket != PLAN_READINESS_FAILED_DETERMINISTIC:
            violations.append(
                "a sticky deterministic failure outranks every remaining "
                f"bucket, got {bucket!r}"
            )
    elif world.has_failed_transient:
        if bucket != PLAN_READINESS_FAILED_TRANSIENT:
            violations.append(
                f"a transient failure must be retryable, got {bucket!r}"
            )
    elif bucket != PLAN_READINESS_NO_PLAN:
        violations.append(
            f"a request with no plan facts must stop the deploy, got {bucket!r}"
        )
    return violations


# ---------------------------------------------------------------------------
# Saturation summary
# ---------------------------------------------------------------------------


def saturation_violations(
    counts: SaturationCounts, summary: SaturationSummary,
) -> list[str]:
    """Check one saturation payload against the counts that produced it."""
    violations: list[str] = []
    rate = summary.saturation_rate
    if not 0.0 <= rate <= 1.0:
        violations.append(f"saturation rate outside [0, 1]: {rate!r}")
    if counts.total_searches == 0 and rate != 0.0:
        violations.append(
            f"an empty window must report exactly 0.0, got {rate!r}"
        )
    if (
        counts.total_searches > 0
        and rate != counts.saturated_searches / counts.total_searches
    ):
        violations.append(
            f"saturation rate must be {counts.saturated_searches}/"
            f"{counts.total_searches}, got {rate!r}"
        )
    carried = (
        summary.total_searches,
        summary.saturated_searches,
        summary.total_pre_filter_skips,
        summary.window_days,
    )
    want = (
        counts.total_searches,
        counts.saturated_searches,
        counts.total_pre_filter_skips,
        counts.window_days,
    )
    if carried != want:
        violations.append(
            f"the summary must carry its counts unchanged: want {want}, "
            f"got {carried}"
        )
    return violations


# ---------------------------------------------------------------------------
# Known-bad self-tests — one per clause
# ---------------------------------------------------------------------------


#: The self-test worlds below are pinned here rather than taken from
#: ``CursorWorld()``/``ReadinessWorld()``'s own defaults, because each
#: assertion names the exact cursor numbers and generator id it expects.
_SELFTEST_CURSOR = CursorWorld(
    active_plan_id=4, next_ordinal=1, cycle_count=2,
    attempt_plan_id=4, attempt_plan_ordinal=1,
    attempt_cycle_count_snapshot=2, attempt_plan_item_count=5,
)
_SELFTEST_READINESS = ReadinessWorld(current_generator_id="gen")


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every clause of every checker trips on a world that violates it."""

    def assertClauseTrips(
        self, violations: list[str], pattern: str, *, alongside: int = 0,
    ) -> None:
        """Assert the named clause tripped, and nothing unexpected did.

        ``alongside`` is the number of OTHER clauses the same world
        necessarily breaks — stated per call rather than assumed zero, so a
        coupled clause is proven honestly instead of being asserted to fire
        alone in a world where it cannot.
        """
        self.assertEqual(
            len(violations),
            alongside + 1,
            f"want {alongside + 1} violation(s), got {violations}",
        )
        self.assertTrue(
            any(re.search(pattern, violation) for violation in violations),
            f"no violation matched {pattern!r}: {violations}",
        )

    # -- backoff ---------------------------------------------------------
    #
    # The exact-value clause subsumes the other three: any wrong minutes
    # value trips it, so each world below trips that clause too. That is
    # stated with ``alongside=1`` rather than papered over.

    def test_backoff_below_base_trips(self):
        self.assertClauseTrips(
            backoff_violations([(1, 29)]),
            r"backoff below the base interval: 1 -> 29",
            alongside=1,
        )

    def test_backoff_above_cap_trips(self):
        self.assertClauseTrips(
            backoff_violations([(9, 9999)]),
            r"backoff above the cap: 9 -> 9999",
            alongside=1,
        )

    def test_backoff_first_attempt_not_base_trips(self):
        self.assertClauseTrips(
            backoff_violations([(0, 60)]),
            r"a first attempt must wait the base interval, got 60",
            alongside=1,
        )

    def test_backoff_clamp_changing_a_value_trips(self):
        self.assertClauseTrips(
            backoff_violations([(2, 240)]),
            r"the exponent clamp changed a value: 2 -> 240, want 120",
        )

    def test_backoff_shrinking_run_trips(self):
        self.assertClauseTrips(
            backoff_violations([(4, 240), (5, 120)]),
            r"backoff must never shrink as attempts accumulate: "
            r"4 -> 240 then 5 -> 120",
            alongside=1,
        )

    # -- cursor ----------------------------------------------------------

    def test_cursor_unknown_status_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                _SELFTEST_CURSOR,
                CursorAdvanceDecision(
                    cursor_update_status="teleported",
                    execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
                    stale_reason=None,
                    new_next_ordinal=2,
                    new_cycle_count=2,
                ),
            ),
            r"unknown cursor update status: 'teleported'",
        )

    def test_cursor_staleness_not_tracking_the_world_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                replace(_SELFTEST_CURSOR, current_status="replaced"),
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_ADVANCED,
                    execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
                    stale_reason=None,
                    new_next_ordinal=2,
                    new_cycle_count=2,
                ),
            ),
            r"staleness must track the world exactly: want stale=True, "
            r"got status='advanced'",
        )

    def test_cursor_stale_moving_the_cursor_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                replace(_SELFTEST_CURSOR, current_status="replaced"),
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_STALE,
                    execution_stage=SEARCH_LOG_STAGE_STALE_COMPLETION,
                    stale_reason="request_replaced",
                    new_next_ordinal=2,
                    new_cycle_count=2,
                ),
            ),
            r"a stale attempt must freeze the cursor, got ordinal 2 cycle 2",
        )

    def test_cursor_wrong_stale_reason_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                replace(_SELFTEST_CURSOR, current_status="replaced"),
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_STALE,
                    execution_stage=SEARCH_LOG_STAGE_STALE_COMPLETION,
                    stale_reason="regenerated",
                    new_next_ordinal=1,
                    new_cycle_count=2,
                ),
            ),
            r"stale reason must be 'request_replaced', got 'regenerated'",
        )

    def test_cursor_stale_wrong_stage_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                replace(_SELFTEST_CURSOR, active_plan_id=99),
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_STALE,
                    execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
                    stale_reason="regenerated",
                    new_next_ordinal=1,
                    new_cycle_count=2,
                ),
            ),
            r"a stale attempt must be logged as a stale completion, got "
            r"'accepted'",
        )

    def test_cursor_live_carrying_a_stale_reason_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                _SELFTEST_CURSOR,
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_ADVANCED,
                    execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
                    stale_reason="regenerated",
                    new_next_ordinal=2,
                    new_cycle_count=2,
                ),
            ),
            r"a live attempt must carry no stale reason, got 'regenerated'",
        )

    def test_cursor_live_wrong_stage_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                _SELFTEST_CURSOR,
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_ADVANCED,
                    execution_stage=SEARCH_LOG_STAGE_STALE_COMPLETION,
                    stale_reason=None,
                    new_next_ordinal=2,
                    new_cycle_count=2,
                ),
            ),
            r"a live attempt must be logged as accepted, got "
            r"'stale_completion'",
        )

    def test_cursor_wrapping_off_the_plan_length_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                _SELFTEST_CURSOR,
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_WRAPPED,
                    execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
                    stale_reason=None,
                    new_next_ordinal=0,
                    new_cycle_count=3,
                ),
            ),
            r"wrapping must track the plan length exactly: want "
            r"wrapped=False, got status='wrapped'",
        )

    def test_cursor_wrap_not_resetting_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                replace(
                    _SELFTEST_CURSOR, next_ordinal=4,
                    attempt_plan_ordinal=4,
                ),
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_WRAPPED,
                    execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
                    stale_reason=None,
                    new_next_ordinal=5,
                    new_cycle_count=3,
                ),
            ),
            r"a wrap must reset to ordinal 0 and open exactly one cycle, "
            r"got ordinal 5 cycle 3",
        )

    def test_cursor_advance_not_stepping_one_trips(self):
        self.assertClauseTrips(
            cursor_violations(
                _SELFTEST_CURSOR,
                CursorAdvanceDecision(
                    cursor_update_status=CURSOR_UPDATE_ADVANCED,
                    execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
                    stale_reason=None,
                    new_next_ordinal=3,
                    new_cycle_count=2,
                ),
            ),
            r"an advance must add exactly one ordinal and hold the cycle, "
            r"got ordinal 3 cycle 2",
        )

    # -- readiness -------------------------------------------------------

    def test_readiness_unregistered_bucket_trips(self):
        self.assertClauseTrips(
            readiness_violations(_SELFTEST_READINESS, "wanted_vibes"),
            r"unregistered readiness bucket: 'wanted_vibes'",
        )

    def test_readiness_ignoring_a_resolved_generator_trips(self):
        self.assertClauseTrips(
            readiness_violations(
                replace(_SELFTEST_READINESS, active_plan_generator_id="gen"),
                PLAN_READINESS_LEGACY,
            ),
            r"a resolved active generator must decide the bucket: want "
            r"'wanted_searchable', got 'wanted_legacy'",
        )

    def test_readiness_unresolved_pointer_claiming_legacy_trips(self):
        self.assertClauseTrips(
            readiness_violations(_SELFTEST_READINESS, PLAN_READINESS_LEGACY),
            r"an unresolved active-plan pointer must not claim an "
            r"active-plan bucket, got 'wanted_legacy'",
        )

    def test_readiness_transient_shadowing_deterministic_trips(self):
        self.assertClauseTrips(
            readiness_violations(
                replace(
                    _SELFTEST_READINESS, has_failed_deterministic=True,
                    has_failed_transient=True,
                ),
                PLAN_READINESS_FAILED_TRANSIENT,
            ),
            r"a sticky deterministic failure outranks every remaining "
            r"bucket, got 'wanted_failed_transient'",
        )

    def test_readiness_losing_a_transient_failure_trips(self):
        self.assertClauseTrips(
            readiness_violations(
                replace(_SELFTEST_READINESS, has_failed_transient=True),
                PLAN_READINESS_NO_PLAN,
            ),
            r"a transient failure must be retryable, got 'wanted_no_plan'",
        )

    def test_readiness_hiding_an_empty_request_trips(self):
        self.assertClauseTrips(
            readiness_violations(
                _SELFTEST_READINESS, PLAN_READINESS_FAILED_TRANSIENT,
            ),
            r"a request with no plan facts must stop the deploy, got "
            r"'wanted_failed_transient'",
        )

    # -- saturation ------------------------------------------------------
    #
    # A wrong rate necessarily breaks the exact-ratio clause too, so the
    # range and empty-window worlds below are stated with ``alongside=1``.

    def test_saturation_rate_outside_unit_interval_trips(self):
        self.assertClauseTrips(
            saturation_violations(
                SaturationCounts(),
                SaturationSummary(
                    total_searches=4,
                    saturated_searches=1,
                    saturation_rate=1.5,
                    total_pre_filter_skips=7,
                    window_days=14,
                ),
            ),
            r"saturation rate outside \[0, 1\]: 1\.5",
            alongside=1,
        )

    def test_saturation_empty_window_reporting_nonzero_trips(self):
        self.assertClauseTrips(
            saturation_violations(
                SaturationCounts(
                    total_searches=0,
                    saturated_searches=0,
                    total_pre_filter_skips=0,
                ),
                SaturationSummary(
                    total_searches=0,
                    saturated_searches=0,
                    saturation_rate=0.5,
                    total_pre_filter_skips=0,
                    window_days=14,
                ),
            ),
            r"an empty window must report exactly 0\.0, got 0\.5",
        )

    def test_saturation_wrong_ratio_trips(self):
        self.assertClauseTrips(
            saturation_violations(
                SaturationCounts(),
                SaturationSummary(
                    total_searches=4,
                    saturated_searches=1,
                    saturation_rate=0.5,
                    total_pre_filter_skips=7,
                    window_days=14,
                ),
            ),
            r"saturation rate must be 1/4, got 0\.5",
        )

    def test_saturation_dropping_a_count_trips(self):
        self.assertClauseTrips(
            saturation_violations(
                SaturationCounts(),
                SaturationSummary(
                    total_searches=4,
                    saturated_searches=1,
                    saturation_rate=0.25,
                    total_pre_filter_skips=0,
                    window_days=14,
                ),
            ),
            r"the summary must carry its counts unchanged",
        )


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


class TestSearchBackoffGenerated(unittest.TestCase):
    @given(
        priors=st.lists(
            st.integers(min_value=0, max_value=4096),
            min_size=1,
            max_size=12,
        ),
    )
    @example(priors=[0])
    @example(priors=[0, 1, 2, 3, 4])
    # The double-precision overflow point the SQL twin used to raise on.
    @example(priors=[1023, 1024, 1025])
    @example(priors=[4096])
    def test_backoff_is_a_capped_monotone_doubling(self, priors: list[int]):
        samples = [
            (prior, search_backoff_minutes(prior))
            for prior in sorted(priors)
        ]
        self.assertEqual(backoff_violations(samples), [])


class TestCursorAdvanceGenerated(unittest.TestCase):
    @given(
        current_status=st.sampled_from(
            ("wanted", "downloading", "processing", "replaced", "imported"),
        ),
        active_plan_id=st.one_of(
            st.none(), st.integers(min_value=1, max_value=4),
        ),
        next_ordinal=st.integers(min_value=0, max_value=6),
        cycle_count=st.integers(min_value=0, max_value=4),
        attempt_plan_id=st.integers(min_value=1, max_value=4),
        attempt_plan_ordinal=st.integers(min_value=0, max_value=6),
        attempt_cycle_count_snapshot=st.integers(min_value=0, max_value=4),
        attempt_plan_item_count=st.integers(min_value=-2, max_value=7),
    )
    # The pathological zero-item plan, and a replaced request whose plan and
    # cursor otherwise line up perfectly.
    @example(
        current_status="wanted",
        active_plan_id=1,
        next_ordinal=0,
        cycle_count=0,
        attempt_plan_id=1,
        attempt_plan_ordinal=0,
        attempt_cycle_count_snapshot=0,
        attempt_plan_item_count=0,
    )
    @example(
        current_status="replaced",
        active_plan_id=1,
        next_ordinal=0,
        cycle_count=0,
        attempt_plan_id=1,
        attempt_plan_ordinal=0,
        attempt_cycle_count_snapshot=0,
        attempt_plan_item_count=1,
    )
    def test_cursor_decision_matches_the_world(
        self,
        current_status: str,
        active_plan_id: int | None,
        next_ordinal: int,
        cycle_count: int,
        attempt_plan_id: int,
        attempt_plan_ordinal: int,
        attempt_cycle_count_snapshot: int,
        attempt_plan_item_count: int,
    ):
        world = CursorWorld(
            current_status=current_status,
            active_plan_id=active_plan_id,
            next_ordinal=next_ordinal,
            cycle_count=cycle_count,
            attempt_plan_id=attempt_plan_id,
            attempt_plan_ordinal=attempt_plan_ordinal,
            attempt_cycle_count_snapshot=attempt_cycle_count_snapshot,
            attempt_plan_item_count=attempt_plan_item_count,
        )
        self.assertEqual(cursor_violations(world, world.decide()), [])


class TestPlanReadinessGenerated(unittest.TestCase):
    @given(
        active_plan_generator_id=st.one_of(
            st.none(), st.sampled_from(("gen-1", "gen-2", "gen-3")),
        ),
        current_generator_id=st.sampled_from(("gen-1", "gen-2")),
        has_failed_deterministic=st.booleans(),
        has_failed_transient=st.booleans(),
    )
    # An unresolved active-plan pointer with no failed plans at all, and a
    # current-generator plan that also carries both failure classes.
    @example(
        active_plan_generator_id=None,
        current_generator_id="gen-1",
        has_failed_deterministic=False,
        has_failed_transient=False,
    )
    @example(
        active_plan_generator_id="gen-1",
        current_generator_id="gen-1",
        has_failed_deterministic=True,
        has_failed_transient=True,
    )
    def test_bucket_follows_the_precedence(
        self,
        active_plan_generator_id: str | None,
        current_generator_id: str,
        has_failed_deterministic: bool,
        has_failed_transient: bool,
    ):
        world = ReadinessWorld(
            active_plan_generator_id=active_plan_generator_id,
            current_generator_id=current_generator_id,
            has_failed_deterministic=has_failed_deterministic,
            has_failed_transient=has_failed_transient,
        )
        self.assertEqual(readiness_violations(world, world.classify()), [])


class TestSaturationSummaryGenerated(unittest.TestCase):
    @given(data=st.data())
    def test_summary_carries_its_counts_and_a_unit_rate(
        self, data: st.DataObject,
    ):
        total = data.draw(st.integers(min_value=0, max_value=5000))
        counts = SaturationCounts(
            total_searches=total,
            # ``COUNT(*) FILTER (...)`` can never exceed ``COUNT(*)``; that
            # is structural, not a plausibility filter.
            saturated_searches=data.draw(
                st.integers(min_value=0, max_value=total),
            ),
            total_pre_filter_skips=data.draw(
                st.integers(min_value=0, max_value=100_000),
            ),
            window_days=data.draw(st.integers(min_value=0, max_value=365)),
        )
        self.assertEqual(
            saturation_violations(counts, counts.assemble()), [],
        )


if __name__ == "__main__":
    unittest.main()
