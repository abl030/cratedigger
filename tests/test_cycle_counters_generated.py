"""Generated proof that a cycle's counters say the same thing everywhere.

The invariant: for any counters value, the number an operator reads in
the summary line and the number carried back on ``FindDownloadMetrics``
are that counter's own number. Both consumers read one declaration
(``lib.cycle_counters``), and that is what their agreement rests on --
not on hand-kept lists agreeing, which is how ``search_time_s=`` came to
be logged but asserted nowhere (issue #1348).

Scope, stated plainly. The third consumer, the ``cycle_metrics`` row, is
deliberately NOT patrolled here. Writing it through ``FakePipelineDB``
would make the subject the fake's own mirror rather than production
(CLAUDE.md forbids property-testing test machinery), and the checker
would re-derive the very expression the fake builds the row with, so no
world could fail it (review F6). The production write is pinned instead
against real PostgreSQL, with a distinct value per counter read back
from its own column, in
``tests/test_pipeline_db.py::TestPipelineDashboardMetrics``.

What this adds over those pins is value space: negatives, negative zero,
half-way roundings and large magnitudes no hand-written world would
think to try.
"""
from __future__ import annotations

import math
import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.cycle_counters import (
    COUNTER_NAMES,
    FLOAT_COUNTER_NAMES,
    CycleCounters,
)
from lib.cycle_summary import CYCLE_COMPLETE_PREFIX, format_cycle_summary
from lib.enqueue import FindDownloadMetrics

#: Whole-number floats and half-way values included on purpose: ``.1f``
#: rounds half to even, and a renderer that reached for ``round()`` or
#: ``repr()`` instead would diverge exactly there.
_FLOATS = st.one_of(
    st.floats(min_value=-1e6, max_value=1e6, allow_nan=False,
              allow_infinity=False),
    st.sampled_from([0.0, -0.0, 0.05, 0.15, 0.25, 1.25, 6.25, 99.95]),
)
_INTS = st.integers(min_value=-(2**31), max_value=2**31 - 1)


@st.composite
def counter_worlds(draw: st.DrawFn) -> CycleCounters:
    """One counters value, every counter drawn independently.

    Filled by assignment, the way a cycle fills it, so a counter added to
    the declaration is drawn here without this strategy being edited.
    """
    counters = CycleCounters()
    for name in COUNTER_NAMES:
        setattr(counters, name,
                draw(_FLOATS if name in FLOAT_COUNTER_NAMES else _INTS))
    return counters


def line_token_violations(counters: CycleCounters, line: str) -> list[str]:
    """Every counter whose own token is missing or carries another value.

    Accumulating rather than raising at the first violation: with sixteen
    counters, a short-circuit would let one masked failure hide fifteen.

    One direction this cannot patrol, stated so nobody credits it with
    more than it does: the expected format is computed from the same
    ``FLOAT_COUNTER_NAMES`` the renderer reads, so a change to WHICH
    counters are floats moves both sides together and this stays quiet
    (review mutant M4b). That split is pinned deterministically instead,
    by ``test_float_counters_are_exactly_the_four_durations`` and by both
    ``EXPECTED_LINE`` pins in ``tests/test_cycle_summary.py``.
    """
    violations: list[str] = []
    tokens = dict(
        token.split("=", 1) for token in line.split(" ") if "=" in token)
    for name in COUNTER_NAMES:
        value = getattr(counters, name)
        expected = (f"{value:.1f}" if name in FLOAT_COUNTER_NAMES
                    else f"{value}")
        if name not in tokens:
            violations.append(f"{name}: no token in the summary line")
        elif tokens[name] != expected:
            violations.append(
                f"{name}: line says {tokens[name]!r}, value is {expected!r}")
    return violations


def projection_violations(counters: CycleCounters,
                          metrics: FindDownloadMetrics) -> list[str]:
    """Every projected field not equal to its same-named counter."""
    return [
        f"{name}: projection holds {getattr(metrics, name)!r}, value is "
        f"{getattr(counters, name)!r}"
        for name in vars(metrics)
        if getattr(metrics, name) != getattr(counters, name)
    ]


class TestCountersAgreeAcrossConsumers(unittest.TestCase):

    @example(counters=CycleCounters())
    @example(counters=CycleCounters(search_time_s=6.25))
    @example(counters=CycleCounters(browse_time_s=-0.0, peers_browsed=-1))
    @given(counters=counter_worlds())
    def test_line_and_projection_carry_the_same_numbers(
        self, counters: CycleCounters,
    ) -> None:
        line = format_cycle_summary(counters, elapsed_s=12.5)
        metrics = FindDownloadMetrics.from_counters(counters)

        violations = (
            line_token_violations(counters, line)
            + projection_violations(counters, metrics)
        )
        self.assertEqual(violations, [], "\n".join(violations))

    @given(counters=counter_worlds())
    def test_the_line_stays_one_greppable_line(
        self, counters: CycleCounters,
    ) -> None:
        line = format_cycle_summary(counters, elapsed_s=0.05)
        self.assertNotIn("\n", line)
        self.assertTrue(line.startswith("Cratedigger cycle complete in"))
        # One token per counter, plus the prefix's words, its elapsed
        # figure, and cycle_total_s. A stray space inside a rendered
        # value would show up here as an extra token.
        self.assertEqual(
            len(line.split(" ")),
            len(CYCLE_COMPLETE_PREFIX.split(" ")) + 1 + len(COUNTER_NAMES) + 1)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker clause, with its own message.

    A checker that has never accused anything is unfalsifiable until
    proven otherwise, and both of these accumulate rather than raise, so
    every clause has to be reached on its own.
    """

    def test_line_checker_names_a_missing_token(self):
        violations = line_token_violations(
            CycleCounters(), "Cratedigger cycle complete in 1.0s")
        self.assertIn("browse_time_s: no token in the summary line", violations)

    def test_line_checker_names_a_wrong_value(self):
        counters = CycleCounters(peers_browsed=7)
        line = format_cycle_summary(counters, elapsed_s=1.0).replace(
            "peers_browsed=7", "peers_browsed=8")
        self.assertIn(
            "peers_browsed: line says '8', value is '7'",
            line_token_violations(counters, line))

    def test_line_checker_is_quiet_on_a_correct_line(self):
        counters = CycleCounters(peers_browsed=7, browse_time_s=1.25)
        self.assertEqual(
            line_token_violations(
                counters, format_cycle_summary(counters, elapsed_s=1.0)),
            [])

    def test_projection_checker_names_a_swapped_field(self):
        counters = CycleCounters(browse_time_s=1.0, match_time_s=2.0)
        swapped = FindDownloadMetrics(browse_time_s=2.0, match_time_s=2.0)
        self.assertIn(
            "browse_time_s: projection holds 2.0, value is 1.0",
            projection_violations(counters, swapped))

    def test_projection_checker_is_quiet_on_a_faithful_projection(self):
        counters = CycleCounters(browse_time_s=1.0, match_time_s=2.0)
        self.assertEqual(
            projection_violations(
                counters, FindDownloadMetrics.from_counters(counters)),
            [])

    def test_negative_zero_is_not_a_hidden_pass(self):
        """``-0.0 == 0.0`` in Python, so the checkers cannot tell them
        apart by value. The rendered token can, and does."""
        self.assertTrue(math.copysign(1.0, -0.0) < 0)
        self.assertIn(
            "browse_time_s=-0.0",
            format_cycle_summary(CycleCounters(browse_time_s=-0.0),
                                 elapsed_s=1.0))


if __name__ == "__main__":
    unittest.main()
