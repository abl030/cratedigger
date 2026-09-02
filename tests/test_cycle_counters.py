"""The cycle-counter value type's own contract (issue #1348).

``lib.cycle_counters`` is the single declaration three consumers derive
from: the operator-facing summary line, the ``cycle_metrics`` INSERT, and
``FindDownloadMetrics``. Everything derived is only as good as what it
derives from, so the order, the names, and the int/float split are pinned
here rather than inferred anywhere else.
"""
from __future__ import annotations

import dataclasses
import unittest

from lib.cycle_counters import (
    COUNTER_NAMES,
    FLOAT_COUNTER_NAMES,
    CycleCounters,
    _float_counter_names,
    counter_values,
)


class TestCounterDeclaration(unittest.TestCase):
    """The names and their order, written out once.

    Deriving this expectation from ``CycleCounters`` would agree with any
    declaration by construction. The literal is what makes a rename, a
    removal, or a reorder show up as a failing test rather than as a
    changed log line and an unwritten column in production.
    """

    EXPECTED_ORDER = (
        "browse_time_s",
        "match_time_s",
        "search_time_s",
        "cache_pos_hits",
        "cache_neg_hits",
        "cache_misses",
        "cache_errors",
        "cache_fuse_tripped",
        "cache_write_errors",
        "peers_browsed",
        "peers_browsed_lazy",
        "fanout_waves",
        "cycle_searches_watchdog_killed",
        "find_download_queued",
        "find_download_completed",
        "find_download_drain_time_s",
    )

    def test_counter_names_are_exactly_these_in_this_order(self):
        self.assertEqual(COUNTER_NAMES, self.EXPECTED_ORDER)

    def test_float_counters_are_exactly_the_four_durations(self):
        self.assertEqual(FLOAT_COUNTER_NAMES, frozenset({
            "browse_time_s", "match_time_s", "search_time_s",
            "find_download_drain_time_s",
        }))

    def test_every_counter_defaults_to_zero(self):
        self.assertEqual(counter_values(CycleCounters()), [0] * 16)

    def test_counters_are_keyword_only(self):
        """Sixteen same-typed numbers make positional construction a swap
        waiting to happen, and a swap is invisible to a shape assertion."""
        with self.assertRaises(TypeError):
            CycleCounters(1.0)  # pyright: ignore[reportCallIssue]

    def test_counter_values_follow_counter_names(self):
        counters = CycleCounters(**{
            name: 2 + offset for offset, name in enumerate(COUNTER_NAMES)})
        self.assertEqual(
            counter_values(counters),
            [2 + offset for offset in range(len(COUNTER_NAMES))])


class TestFloatCounterNamesFailsClosed(unittest.TestCase):
    """``_float_counter_names`` decides how every counter is rendered and
    stored. A type it does not understand must stop the import, not pick
    a format on the operator's behalf."""

    def _hints(self, **overrides: type) -> dict[str, type]:
        hints = {name: int for name in COUNTER_NAMES}
        hints.update(overrides)
        return hints

    def test_a_non_numeric_counter_is_refused_by_name(self):
        with self.assertRaisesRegex(
            TypeError,
            "^cycle counters must be declared int or float; .*peers_browsed",
        ):
            _float_counter_names(self._hints(peers_browsed=str))

    def test_every_offending_counter_is_named_not_just_the_first(self):
        with self.assertRaisesRegex(TypeError, "cache_misses, peers_browsed"):
            _float_counter_names(
                self._hints(peers_browsed=bytes, cache_misses=str))

    def test_an_all_numeric_declaration_is_accepted(self):
        """The must-still-work half: refusing everything would also pass
        the two tests above."""
        self.assertEqual(
            _float_counter_names(self._hints(browse_time_s=float)),
            frozenset({"browse_time_s"}),
        )

    def test_the_live_declaration_is_all_numeric(self):
        declared = {
            str(field.type) for field in dataclasses.fields(CycleCounters)
        }
        self.assertEqual(
            sorted(declared), ["float", "int"],
            "a counter declared as something else would change how the "
            "summary line and the cycle_metrics row render it",
        )


if __name__ == "__main__":
    unittest.main()
