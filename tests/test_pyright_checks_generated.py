"""Generated allocation and outcome contracts for the Pyright runner."""

from __future__ import annotations

import unittest

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from scripts.run_pyright_checks import (
    PyrightCheck,
    PyrightOutcome,
    combined_exit_code,
    recommended_thread_counts,
)


class TestGeneratedPyrightThreadAllocation(unittest.TestCase):
    @given(cpu_count=st.integers(min_value=1, max_value=512))
    def test_allocation_is_positive_bounded_and_uses_the_available_budget(
        self,
        cpu_count: int,
    ) -> None:
        whole, strict = recommended_thread_counts(cpu_count)

        self.assertGreaterEqual(whole, 1)
        self.assertGreaterEqual(strict, 1)
        if cpu_count == 1:
            self.assertEqual((whole, strict), (1, 1))
        else:
            self.assertEqual(whole + strict, min(cpu_count, 12))
        self.assertLessEqual(whole, 8)
        self.assertLessEqual(strict, 4)


class TestGeneratedCombinedPyrightOutcome(unittest.TestCase):
    @given(
        whole_status=st.integers(min_value=0, max_value=127),
        strict_status=st.integers(min_value=0, max_value=127),
    )
    def test_every_child_status_contributes_to_the_combined_result(
        self,
        whole_status: int,
        strict_status: int,
    ) -> None:
        outcomes = (
            PyrightOutcome(
                PyrightCheck("whole-tree", "pyrightconfig.json", 8),
                whole_status,
                "",
            ),
            PyrightOutcome(
                PyrightCheck(
                    "production-strict",
                    "pyrightconfig.production.json",
                    4,
                ),
                strict_status,
                "",
            ),
        )
        if whole_status == strict_status == 0:
            expected = 0
        elif whole_status in {0, 1} and strict_status in {0, 1}:
            expected = 1
        else:
            expected = 2

        self.assertEqual(combined_exit_code(outcomes), expected)


class TestCombinedOutcomeCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_false_green_when_strict_failed(self) -> None:
        outcomes = (
            PyrightOutcome(
                PyrightCheck("whole-tree", "pyrightconfig.json", 8),
                0,
                "",
            ),
            PyrightOutcome(
                PyrightCheck(
                    "production-strict",
                    "pyrightconfig.production.json",
                    4,
                ),
                1,
                "",
            ),
        )

        self.assertNotEqual(combined_exit_code(outcomes), 0)


if __name__ == "__main__":
    unittest.main()
