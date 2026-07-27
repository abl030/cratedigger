"""Generated exact-coverage patrol for parallel test partitioning."""

from __future__ import annotations

import unittest
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from scripts.run_python_tests import (
    DEFAULT_MAX_WORKERS,
    TestModule,
    _iter_test_cases,
    assert_exact_schedule,
    assert_exact_target_coverage,
    assert_hypothesis_deadlines_disabled,
    recommended_worker_count,
    resolve_hypothesis_settings,
    schedule_modules,
    shard_test_ids,
)
from tests.test_parallel_test_runner import (
    DEADLINE_BEARING_KINDS,
    HYPOTHESIS_KINDS,
    PLANTED_CASES,
    planted_suite,
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


def assert_deadline_contract(
    *,
    accepted: bool,
    kinds: tuple[str, ...],
) -> None:
    """A suite is admissible iff no Hypothesis test carries a deadline.

    Stated from the planted construction rather than by re-reading the
    settings the checker itself read (#882 B1b).
    """
    expected = not any(kind in DEADLINE_BEARING_KINDS for kind in kinds)
    if accepted != expected:
        verdict = "accepted" if accepted else "rejected"
        raise AssertionError(
            f"suite of {kinds!r} was {verdict}; deadlines present="
            f"{not expected}",
        )


class TestGeneratedSuiteDeadlineContract(unittest.TestCase):
    """#882 B1b: the runtime half of the profile-tier invariant.

    Ranges over mixed suites of Hypothesis and ordinary tests with assorted
    deadlines, driving the real ``assert_hypothesis_deadlines_disabled``.
    """

    @given(
        kinds=st.lists(
            st.sampled_from(sorted(PLANTED_CASES)),
            min_size=1,
            max_size=6,
        ),
    )
    @example(kinds=["none"])
    @example(kinds=["ms_200"])
    @example(kinds=["plain"])
    @example(kinds=["none", "plain", "ms_200"])
    @example(kinds=["timedelta"])
    def test_only_a_deadline_free_suite_is_admissible(
        self,
        kinds: list[str],
    ) -> None:
        planted = tuple(kinds)
        suite = planted_suite(planted)
        try:
            assert_hypothesis_deadlines_disabled(suite)
        except RuntimeError:
            accepted = False
        else:
            accepted = True

        assert_deadline_contract(accepted=accepted, kinds=planted)

    @given(
        kinds=st.lists(
            st.sampled_from(sorted(PLANTED_CASES)),
            min_size=1,
            max_size=6,
        ),
    )
    def test_every_planted_property_resolves_its_own_settings(
        self,
        kinds: list[str],
    ) -> None:
        """The resolver never silently returns ``None`` for a real property —
        that would let a deadline slip past the contract unexamined."""
        planted = tuple(kinds)
        resolved = [
            resolve_hypothesis_settings(test)
            for test in _iter_test_cases(planted_suite(planted))
        ]

        self.assertEqual(
            sum(item is not None for item in resolved),
            sum(kind in HYPOTHESIS_KINDS for kind in planted),
        )


class TestSuiteDeadlineCheckerKnownBad(unittest.TestCase):
    def test_contract_trips_when_a_deadline_suite_is_accepted(self) -> None:
        with self.assertRaises(AssertionError):
            assert_deadline_contract(accepted=True, kinds=("ms_200",))

    def test_contract_trips_when_a_clean_suite_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            assert_deadline_contract(accepted=False, kinds=("none", "plain"))

    def test_planted_cases_are_never_collected_from_this_module(self) -> None:
        """The deadline-bearing plants must stay out of unittest discovery —
        otherwise this module fails the very contract it patrols."""
        suite = unittest.defaultTestLoader.loadTestsFromName(__name__)
        collected = {type(test).__name__ for test in _iter_test_cases(suite)}

        self.assertNotIn("PlantedWorld", collected)
        self.assertNotIn("PlainWorld", collected)
        assert_hypothesis_deadlines_disabled(suite)


if __name__ == "__main__":
    unittest.main()
