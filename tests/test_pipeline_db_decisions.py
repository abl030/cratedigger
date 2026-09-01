"""Deterministic table tests for the shared pipeline-DB decisions.

These four rules used to be written twice — once by production inside a SQL
transaction (or the Python tail of one) and once by hand in
``tests/fakes/pipeline_db/``. ``lib/pipeline_db/decisions.py`` is now the
one spelling; this module pins every arm of every rule directly, and
``tests/test_pipeline_db_decisions_generated.py`` patrols the world space
around them through the same typed worlds defined here.
"""

from __future__ import annotations

import sys
import unittest
from dataclasses import dataclass, replace
from typing import ClassVar

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
    SEARCH_BACKOFF_MAX_EXPONENT,
    CursorAdvanceDecision,
    classify_plan_readiness_bucket,
    cursor_advance_decision,
    saturation_summary_from_counts,
    search_backoff_minutes,
)

#: Largest exponent the retry-pacing SQL can evaluate. The expression that
#: has to stay representable is the whole ``BACKOFF_BASE_MINUTES * POWER(2,
#: n)`` product, NOT ``POWER(2, n)`` alone — with a base of 30 the product
#: overflows ``double precision`` four exponents before the power does, so
#: guarding the power would be fail-open by exactly those four. Measured
#: against the live database on 2026-08-31: ``LEAST(30 * POWER(2, 1019),
#: 240)`` returns 240.0 and ``LEAST(30 * POWER(2, 1020), 240)`` raises
#: ``value out of range: overflow``. (Bare ``POWER(2, n)`` survives to
#: 1023; that is the number this constant used to hold, and the reason the
#: assertion below is written against the product instead.)
PG_RETRY_PACING_MAX_EXPONENT = 1019


# ---------------------------------------------------------------------------
# Typed worlds, shared with the generated siblings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CursorWorld:
    """One request cursor state plus the attempt that just finished."""

    current_status: str = "wanted"
    active_plan_id: int | None = 11
    next_ordinal: int = 2
    cycle_count: int = 7
    attempt_plan_id: int = 11
    attempt_plan_ordinal: int = 2
    attempt_cycle_count_snapshot: int = 7
    attempt_plan_item_count: int = 5

    def decide(self) -> CursorAdvanceDecision:
        """Run the REAL production decision over this world."""
        return cursor_advance_decision(
            current_status=self.current_status,
            active_plan_id=self.active_plan_id,
            next_ordinal=self.next_ordinal,
            cycle_count=self.cycle_count,
            attempt_plan_id=self.attempt_plan_id,
            attempt_plan_ordinal=self.attempt_plan_ordinal,
            attempt_cycle_count_snapshot=self.attempt_cycle_count_snapshot,
            attempt_plan_item_count=self.attempt_plan_item_count,
        )


@dataclass(frozen=True)
class ReadinessWorld:
    """The three facts one wanted request's readiness bucket comes from."""

    active_plan_generator_id: str | None = None
    current_generator_id: str = "gen-2"
    has_failed_deterministic: bool = False
    has_failed_transient: bool = False

    def classify(self) -> str:
        """Run the REAL production bucket rule over this world."""
        return classify_plan_readiness_bucket(
            active_plan_generator_id=self.active_plan_generator_id,
            current_generator_id=self.current_generator_id,
            has_failed_deterministic=self.has_failed_deterministic,
            has_failed_transient=self.has_failed_transient,
        )


@dataclass(frozen=True)
class SaturationCounts:
    """One window's aggregate counts, before assembly."""

    total_searches: int = 4
    saturated_searches: int = 1
    total_pre_filter_skips: int = 7
    window_days: int = 14

    def assemble(self) -> SaturationSummary:
        """Run the REAL production assembly over these counts."""
        return saturation_summary_from_counts(
            total_searches=self.total_searches,
            saturated_searches=self.saturated_searches,
            total_pre_filter_skips=self.total_pre_filter_skips,
            window_days=self.window_days,
        )


@dataclass(frozen=True)
class _CursorCase:
    """One row of the cursor decision table."""

    desc: str
    world: CursorWorld
    status: str
    new_next_ordinal: int
    new_cycle_count: int
    stale_reason: str | None


@dataclass(frozen=True)
class _ReadinessCase:
    """One row of the readiness bucket table."""

    desc: str
    world: ReadinessWorld
    bucket: str


@dataclass(frozen=True)
class _BackoffCase:
    """One row of the retry-pacing table."""

    desc: str
    prior_attempts: int
    minutes: int


@dataclass(frozen=True)
class _SaturationCase:
    """One row of the saturation-rate table."""

    desc: str
    counts: SaturationCounts
    rate: float


class TestSearchBackoffMinutes(unittest.TestCase):
    """One retry-pacing formula, keyed on the PRIOR attempt count."""

    CASES: ClassVar[tuple[_BackoffCase, ...]] = (
        _BackoffCase("never attempted waits the base interval", 0, 30),
        _BackoffCase("one prior attempt doubles", 1, 60),
        _BackoffCase("two prior attempts double again", 2, 120),
        _BackoffCase("three prior attempts reach the cap exactly", 3, 240),
        _BackoffCase("four prior attempts stay capped", 4, 240),
        _BackoffCase("the live 2026-08-31 worst counter stays capped", 407, 240),
        # 1020 is where the SQL twin's ``30 * POWER(2, n)`` first overflows
        # double precision (base-dependent: it is the smallest n with
        # BACKOFF_BASE_MINUTES * 2**n > DBL_MAX, not a property of POWER
        # alone, which survives to 1023).
        _BackoffCase("the SQL product's overflow point stays capped", 1020, 240),
        _BackoffCase("an absurd counter stays capped", 100_000, 240),
    )

    def test_backoff_table(self):
        for case in self.CASES:
            with self.subTest(desc=case.desc):
                self.assertEqual(
                    search_backoff_minutes(case.prior_attempts), case.minutes,
                )

    def test_negative_prior_attempts_is_refused(self):
        with self.assertRaisesRegex(
            ValueError, r"prior_attempts must be non-negative, got -1",
        ):
            search_backoff_minutes(-1)

    def test_clamping_the_exponent_cannot_change_a_value(self):
        """The cap only exists past the point ``min()`` already decides."""
        self.assertGreaterEqual(
            BACKOFF_BASE_MINUTES * (2 ** SEARCH_BACKOFF_MAX_EXPONENT),
            BACKOFF_MAX_MINUTES,
        )

    def test_clamped_product_stays_inside_double_precision(self):
        """The SQL evaluates ``base * 2**exponent``, so guard the product.

        Guarding ``2 ** SEARCH_BACKOFF_MAX_EXPONENT`` alone would be
        fail-open: with ``BACKOFF_BASE_MINUTES = 30`` the product overflows
        at exponent 1020 while the bare power survives to 1023.
        """
        self.assertLess(
            float(BACKOFF_BASE_MINUTES) * (2.0 ** SEARCH_BACKOFF_MAX_EXPONENT),
            sys.float_info.max,
        )
        # ...and the live-measured ceiling agrees with that arithmetic.
        self.assertLessEqual(
            SEARCH_BACKOFF_MAX_EXPONENT, PG_RETRY_PACING_MAX_EXPONENT,
        )
        self.assertLess(
            float(BACKOFF_BASE_MINUTES)
            * (2.0 ** PG_RETRY_PACING_MAX_EXPONENT),
            sys.float_info.max,
        )

    def test_clamped_and_unclamped_agree_below_the_overflow_point(self):
        """The clamp is value-identical to the formula it replaced."""
        for prior in range(64):
            with self.subTest(prior=prior):
                self.assertEqual(
                    search_backoff_minutes(prior),
                    min(
                        BACKOFF_BASE_MINUTES * (2 ** prior),
                        BACKOFF_MAX_MINUTES,
                    ),
                )


_CURSOR_BASE = CursorWorld()


class TestCursorAdvanceDecision(unittest.TestCase):
    """Every arm of the consumed-search-attempt cursor rule."""

    CASES: ClassVar[tuple[_CursorCase, ...]] = (
        _CursorCase(
            "a live attempt on a middle item advances one ordinal",
            _CURSOR_BASE, CURSOR_UPDATE_ADVANCED, 3, 7, None,
        ),
        _CursorCase(
            "the last item wraps to ordinal 0 and opens the next cycle",
            replace(_CURSOR_BASE, next_ordinal=4, attempt_plan_ordinal=4),
            CURSOR_UPDATE_WRAPPED, 0, 8, None,
        ),
        _CursorCase(
            "an ordinal past the last item still wraps",
            replace(_CURSOR_BASE, next_ordinal=9, attempt_plan_ordinal=9),
            CURSOR_UPDATE_WRAPPED, 0, 8, None,
        ),
        _CursorCase(
            "a zero-item plan advances rather than wrapping on no items",
            replace(_CURSOR_BASE, attempt_plan_item_count=0),
            CURSOR_UPDATE_ADVANCED, 3, 7, None,
        ),
        _CursorCase(
            "a negative item count is floored to the zero-item guard",
            replace(_CURSOR_BASE, attempt_plan_item_count=-5),
            CURSOR_UPDATE_ADVANCED, 3, 7, None,
        ),
        _CursorCase(
            "a one-item plan wraps on its only ordinal",
            replace(
                _CURSOR_BASE, next_ordinal=0, attempt_plan_ordinal=0,
                attempt_plan_item_count=1,
            ),
            CURSOR_UPDATE_WRAPPED, 0, 8, None,
        ),
        _CursorCase(
            "a replaced request is stale and freezes the cursor",
            replace(_CURSOR_BASE, current_status="replaced"),
            CURSOR_UPDATE_STALE, 2, 7, "request_replaced",
        ),
        _CursorCase(
            "a regenerated plan is stale and freezes the cursor",
            replace(_CURSOR_BASE, active_plan_id=99),
            CURSOR_UPDATE_STALE, 2, 7, "regenerated",
        ),
        _CursorCase(
            "a moved ordinal is stale and freezes the cursor",
            replace(_CURSOR_BASE, next_ordinal=5),
            CURSOR_UPDATE_STALE, 5, 7, "regenerated",
        ),
        _CursorCase(
            "a moved cycle is stale and freezes the cursor",
            replace(_CURSOR_BASE, cycle_count=8),
            CURSOR_UPDATE_STALE, 2, 8, "regenerated",
        ),
        _CursorCase(
            "a request with no active plan at all is stale",
            replace(_CURSOR_BASE, active_plan_id=None),
            CURSOR_UPDATE_STALE, 2, 7, "regenerated",
        ),
        _CursorCase(
            "replaced wins the reason even when the plan also moved",
            replace(
                _CURSOR_BASE, current_status="replaced", active_plan_id=99,
            ),
            CURSOR_UPDATE_STALE, 2, 7, "request_replaced",
        ),
    )

    def test_cursor_table(self):
        for case in self.CASES:
            with self.subTest(desc=case.desc):
                decision = case.world.decide()
                self.assertEqual(
                    decision.cursor_update_status, case.status,
                )
                self.assertEqual(
                    decision.new_next_ordinal, case.new_next_ordinal,
                )
                self.assertEqual(
                    decision.new_cycle_count, case.new_cycle_count,
                )
                self.assertEqual(decision.stale_reason, case.stale_reason)
                self.assertEqual(
                    decision.execution_stage,
                    SEARCH_LOG_STAGE_STALE_COMPLETION
                    if case.status == CURSOR_UPDATE_STALE
                    else SEARCH_LOG_STAGE_ACCEPTED,
                )
                self.assertEqual(
                    decision.is_stale, case.status == CURSOR_UPDATE_STALE,
                )


_READINESS_BASE = ReadinessWorld()


class TestClassifyPlanReadinessBucket(unittest.TestCase):
    """Every arm of the five-bucket readiness precedence."""

    CASES: ClassVar[tuple[_ReadinessCase, ...]] = (
        _ReadinessCase(
            "an active plan on the current generator is searchable",
            replace(_READINESS_BASE, active_plan_generator_id="gen-2"),
            PLAN_READINESS_SEARCHABLE,
        ),
        _ReadinessCase(
            "an active plan on an older generator is legacy carryover",
            replace(_READINESS_BASE, active_plan_generator_id="gen-1"),
            PLAN_READINESS_LEGACY,
        ),
        _ReadinessCase(
            "an active plan outranks a deterministic failure",
            replace(
                _READINESS_BASE, active_plan_generator_id="gen-2",
                has_failed_deterministic=True, has_failed_transient=True,
            ),
            PLAN_READINESS_SEARCHABLE,
        ),
        _ReadinessCase(
            "legacy carryover outranks a deterministic failure",
            replace(
                _READINESS_BASE, active_plan_generator_id="gen-1",
                has_failed_deterministic=True,
            ),
            PLAN_READINESS_LEGACY,
        ),
        _ReadinessCase(
            "no active generator plus a sticky failure is deterministic",
            replace(_READINESS_BASE, has_failed_deterministic=True),
            PLAN_READINESS_FAILED_DETERMINISTIC,
        ),
        _ReadinessCase(
            "deterministic outranks transient",
            replace(
                _READINESS_BASE, has_failed_deterministic=True,
                has_failed_transient=True,
            ),
            PLAN_READINESS_FAILED_DETERMINISTIC,
        ),
        _ReadinessCase(
            "a transient failure alone is retryable next cycle",
            replace(_READINESS_BASE, has_failed_transient=True),
            PLAN_READINESS_FAILED_TRANSIENT,
        ),
        _ReadinessCase(
            "nothing at all is the stop-the-deploy bucket",
            _READINESS_BASE,
            PLAN_READINESS_NO_PLAN,
        ),
        _ReadinessCase(
            # ``search_plans.generator_id`` is NOT NULL (migration 014), so
            # this world reaches production only through the LEFT JOIN: an
            # ``active_plan_id`` that resolves to no plan row. The SQL's
            # ``generator_id IS NOT NULL`` guard drops it past both
            # active-plan arms, which is exactly what an unqualified ``!=``
            # in the twin would have got wrong.
            "an unresolved active-plan pointer is not legacy",
            replace(
                _READINESS_BASE, active_plan_generator_id=None,
                has_failed_transient=True,
            ),
            PLAN_READINESS_FAILED_TRANSIENT,
        ),
    )

    def test_bucket_table(self):
        for case in self.CASES:
            with self.subTest(desc=case.desc):
                self.assertEqual(case.world.classify(), case.bucket)

    def test_every_bucket_name_is_registered(self):
        self.assertEqual(
            set(PLAN_READINESS_BUCKETS),
            {
                PLAN_READINESS_SEARCHABLE,
                PLAN_READINESS_LEGACY,
                PLAN_READINESS_FAILED_DETERMINISTIC,
                PLAN_READINESS_FAILED_TRANSIENT,
                PLAN_READINESS_NO_PLAN,
            },
        )
        self.assertEqual(
            len(PLAN_READINESS_BUCKETS), len(set(PLAN_READINESS_BUCKETS)),
        )


class TestSaturationSummaryFromCounts(unittest.TestCase):
    """Rate arithmetic plus the explicit empty-window fallback."""

    CASES: ClassVar[tuple[_SaturationCase, ...]] = (
        _SaturationCase(
            "an empty window reports 0.0, never NaN",
            SaturationCounts(total_searches=0, saturated_searches=0), 0.0,
        ),
        _SaturationCase(
            "no saturated searches is a zero rate",
            SaturationCounts(total_searches=10, saturated_searches=0), 0.0,
        ),
        _SaturationCase(
            "every search saturated is a rate of one",
            SaturationCounts(total_searches=10, saturated_searches=10), 1.0,
        ),
        _SaturationCase(
            "a partial window reports the ratio",
            SaturationCounts(total_searches=8, saturated_searches=2), 0.25,
        ),
    )

    def test_rate_table(self):
        for case in self.CASES:
            with self.subTest(desc=case.desc):
                summary = case.counts.assemble()
                self.assertEqual(summary.saturation_rate, case.rate)
                self.assertEqual(
                    summary.total_searches, case.counts.total_searches,
                )
                self.assertEqual(
                    summary.saturated_searches,
                    case.counts.saturated_searches,
                )
                self.assertEqual(
                    summary.total_pre_filter_skips,
                    case.counts.total_pre_filter_skips,
                )
                self.assertEqual(
                    summary.window_days, case.counts.window_days,
                )


if __name__ == "__main__":
    unittest.main()
