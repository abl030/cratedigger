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
    status_passed = draw(st.booleans())
    gate_passed = draw(st.booleans())
    capture_passed = draw(st.booleans())
    discovered = draw(st.integers(min_value=0, max_value=100_000))
    counts_match = draw(st.booleans())
    integrity_recorded = draw(st.booleans())
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ended_at = (start + timedelta(seconds=1)).isoformat()
    wrong_start = "a" * 40 if expected_head != "a" * 40 else "c" * 40
    wrong_end = "b" * 40 if expected_head != "b" * 40 else "d" * 40
    summary = TestRunSummary(
        schema_version=2,
        artifact_path="/tmp/artifact",
        output_path="/tmp/artifact/output.log",
        worktree_path="/tmp/worktree",
        started_at=start.isoformat(),
        ended_at=ended_at,
        start_head=expected_head if start_matches else wrong_start,
        end_head=expected_head if end_matches else wrong_end,
        start_dirty=start_dirty,
        end_dirty=end_dirty,
        status="passed" if status_passed else "failed",
        exit_code=0 if status_passed else 1,
        gate_exit_code=0 if gate_passed else 17,
        capture_exit_code=0 if capture_passed else 23,
        discovered_tests=discovered,
        run_tests=discovered if counts_match else discovered + 1,
        output_bytes=123 if integrity_recorded else None,
        output_sha256="0" * 64 if integrity_recorded else None,
    )
    expected_valid = (
        start_matches
        and end_matches
        and not start_dirty
        and not end_dirty
        and status_passed
        and gate_passed
        and capture_passed
        and discovered > 0
        and counts_match
        and integrity_recorded
    )
    return summary, expected_head, expected_valid


class TestGeneratedSuiteArtifactProvenance(unittest.TestCase):
    @given(world=_provenance_worlds())
    @example(
        world=(
            TestRunSummary(
                schema_version=2,
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
                gate_exit_code=0,
                capture_exit_code=0,
                discovered_tests=1,
                run_tests=1,
                output_bytes=123,
                output_sha256="0" * 64,
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
            schema_version=2,
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
            gate_exit_code=0,
            capture_exit_code=0,
            discovered_tests=1,
            run_tests=1,
            output_bytes=123,
            output_sha256="0" * 64,
        )

        with self.assertRaises(AssertionError):
            assert_provenance_invariant(summary, "2" * 40, True)

    def test_known_bad_checker_rejects_missing_output_integrity(self) -> None:
        summary = TestRunSummary(
            schema_version=2,
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
            gate_exit_code=0,
            capture_exit_code=0,
            discovered_tests=1,
            run_tests=1,
            output_bytes=None,
            output_sha256=None,
        )

        with self.assertRaises(AssertionError):
            assert_provenance_invariant(summary, "1" * 40, True)


if __name__ == "__main__":
    unittest.main()
