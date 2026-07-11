"""Generated provenance invariant for citeable full-suite artifacts.

Invariant: a summary is citeable for an exact commit iff the run completed
green, started and ended clean at that commit, and executed every test in the
single discovered suite.  The deterministic concurrency and wrong-HEAD pins
live in ``tests/test_suite_artifact.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from scripts.test_artifact import TestRunSummary, summary_rejection_reasons


_HEADS = st.text(alphabet="0123456789abcdef", min_size=40, max_size=40)


def assert_provenance_invariant(
    summary: TestRunSummary,
    expected_head: str,
    expected_valid: bool,
) -> None:
    """A planted valid/invalid world must match the production checker."""
    actual_valid = not summary_rejection_reasons(summary, expected_head)
    if actual_valid != expected_valid:
        raise AssertionError(
            f"provenance verdict mismatch: expected_valid={expected_valid} "
            f"actual_valid={actual_valid} summary={summary!r}"
        )


@st.composite
def _provenance_worlds(draw):
    expected_head = draw(_HEADS)
    start_matches = draw(st.booleans())
    end_matches = draw(st.booleans())
    start_dirty = draw(st.booleans())
    end_dirty = draw(st.booleans())
    passed = draw(st.booleans())
    discovered = draw(st.integers(min_value=0, max_value=100_000))
    counts_match = draw(st.booleans())
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ended_at = (start + timedelta(seconds=1)).isoformat()
    wrong_start = "a" * 40 if expected_head != "a" * 40 else "c" * 40
    wrong_end = "b" * 40 if expected_head != "b" * 40 else "d" * 40
    summary = TestRunSummary(
        schema_version=1,
        artifact_path="/tmp/artifact",
        output_path="/tmp/artifact/output.log",
        worktree_path="/tmp/worktree",
        started_at=start.isoformat(),
        ended_at=ended_at,
        start_head=expected_head if start_matches else wrong_start,
        end_head=expected_head if end_matches else wrong_end,
        start_dirty=start_dirty,
        end_dirty=end_dirty,
        status="passed" if passed else "failed",
        exit_code=0 if passed else 1,
        discovered_tests=discovered,
        run_tests=discovered if counts_match else discovered + 1,
    )
    expected_valid = (
        start_matches
        and end_matches
        and not start_dirty
        and not end_dirty
        and passed
        and discovered > 0
        and counts_match
    )
    return summary, expected_head, expected_valid


class TestGeneratedSuiteArtifactProvenance(unittest.TestCase):
    @given(world=_provenance_worlds())
    @example(
        world=(
            TestRunSummary(
                schema_version=1,
                artifact_path="/tmp/artifact",
                output_path="/tmp/artifact/output.log",
                worktree_path="/tmp/worktree",
                started_at="2026-01-01T00:00:00+00:00",
                ended_at="2026-01-01T00:00:01+00:00",
                start_head="1" * 40,
                end_head="1" * 40,
                start_dirty=True,
                end_dirty=False,
                status="passed",
                exit_code=0,
                discovered_tests=1,
                run_tests=1,
            ),
            "1" * 40,
            False,
        )
    )
    def test_only_exact_clean_complete_worlds_are_citeable(self, world) -> None:
        assert_provenance_invariant(*world)


class TestProvenanceCheckerTripsOnViolations(unittest.TestCase):
    def test_known_bad_checker_rejects_a_planted_wrong_head(self) -> None:
        summary = TestRunSummary(
            schema_version=1,
            artifact_path="/tmp/artifact",
            output_path="/tmp/artifact/output.log",
            worktree_path="/tmp/worktree",
            started_at="2026-01-01T00:00:00+00:00",
            ended_at="2026-01-01T00:00:01+00:00",
            start_head="1" * 40,
            end_head="1" * 40,
            start_dirty=False,
            end_dirty=False,
            status="passed",
            exit_code=0,
            discovered_tests=1,
            run_tests=1,
        )

        with self.assertRaises(AssertionError):
            assert_provenance_invariant(summary, "2" * 40, True)


if __name__ == "__main__":
    unittest.main()
