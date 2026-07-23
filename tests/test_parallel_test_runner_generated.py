"""Generated exact-coverage patrol for parallel test partitioning."""

from __future__ import annotations

import unittest
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from scripts.run_python_tests import (
    DEFAULT_MAX_WORKERS,
    TestModule,
    assert_exact_target_coverage,
    assert_exact_schedule,
    recommended_worker_count,
    schedule_modules,
    shard_test_ids,
)


def assert_recommended_worker_policy(cpu_count: int, worker_count: int) -> None:
    """Check the bounded half-host worker policy."""
    expected = min(DEFAULT_MAX_WORKERS, max(1, cpu_count // 2))
    if worker_count != expected:
        raise AssertionError(
            f"{cpu_count} CPUs require {expected} workers, got {worker_count}"
        )
    if worker_count > cpu_count:
        raise AssertionError(f"{worker_count} workers oversubscribe {cpu_count} CPUs")


class TestGeneratedParallelSchedule(unittest.TestCase):
    @given(cpu_count=st.integers(min_value=1, max_value=512))
    def test_default_workers_follow_bounded_half_host_policy(
        self,
        cpu_count: int,
    ) -> None:
        assert_recommended_worker_policy(
            cpu_count,
            recommended_worker_count(cpu_count),
        )

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
    def test_worker_policy_checker_rejects_oversized_default(self) -> None:
        with self.assertRaisesRegex(AssertionError, "require 12 workers"):
            assert_recommended_worker_policy(30, 16)

    def test_checker_rejects_generated_style_omission(self) -> None:
        first = TestModule("first", Path("/first.py"), 1)
        second = TestModule("second", Path("/second.py"), 1)

        with self.assertRaisesRegex(ValueError, "missing"):
            assert_exact_schedule((first, second), (first,))


class TestGeneratedTargetSharding(unittest.TestCase):
    @given(
        class_sizes=st.lists(
            st.integers(min_value=1, max_value=20),
            min_size=1,
            max_size=30,
        ),
        granularity=st.sampled_from(("class", "class_batch", "method", "method_batch")),
    )
    def test_every_hotspot_test_id_is_scheduled_exactly_once(
        self,
        class_sizes: list[int],
        granularity: str,
    ) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 1)
        test_ids = tuple(
            f"{module.name}.Test{class_index}.test_{test_index}"
            for class_index, class_size in enumerate(class_sizes)
            for test_index in range(class_size)
        )

        targets = shard_test_ids(module, test_ids, granularity=granularity)

        assert_exact_target_coverage(module, test_ids, targets)
        scheduled_ids = tuple(
            test_id for target in targets for test_id in target.expected_test_ids
        )
        self.assertEqual(set(scheduled_ids), set(test_ids))
        self.assertEqual(len(scheduled_ids), len(test_ids))


if __name__ == "__main__":
    unittest.main()
