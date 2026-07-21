"""Generated exact-coverage patrol for parallel test partitioning."""

from __future__ import annotations

import unittest
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from scripts.run_python_tests import (
    TestModule,
    assert_exact_schedule,
    schedule_modules,
)


class TestGeneratedParallelSchedule(unittest.TestCase):
    @given(
        weights=st.lists(
            st.integers(min_value=1, max_value=100_000),
            min_size=1,
            max_size=80,
        ),
    )
    def test_every_generated_module_runs_exactly_once(
        self,
        weights: list[int],
    ) -> None:
        modules = tuple(
            TestModule(f"test_{index}", Path(f"/test_{index}.py"), weight)
            for index, weight in enumerate(weights)
        )

        schedule = schedule_modules(modules)

        assert_exact_schedule(modules, schedule)
        self.assertEqual(len(schedule), len(modules))
        self.assertEqual(len({module.name for module in schedule}), len(modules))


class TestParallelScheduleCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_generated_style_omission(self) -> None:
        first = TestModule("first", Path("/first.py"), 1)
        second = TestModule("second", Path("/second.py"), 1)

        with self.assertRaisesRegex(ValueError, "missing"):
            assert_exact_schedule((first, second), (first,))


if __name__ == "__main__":
    unittest.main()
