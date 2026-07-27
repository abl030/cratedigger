"""Generated patrol for truthful terminal ``run_import`` failure reasons."""

from __future__ import annotations

import string
import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from harness.import_one import RunImportOutcome, _harness_failure_error
from tests.test_import_one_stages import run_import_with_fake_harness

_DIAGNOSTIC_TEXT = st.text(
    alphabet=string.ascii_letters + string.digits + " .,:;_-",
    max_size=40,
).map(lambda suffix: f"observed diagnostic: {suffix}")
_STDERR_LINE = st.one_of(
    _DIAGNOSTIC_TEXT,
    st.sampled_from([
        "",
        " ",
        "\t",
        "Disabled fetchart: no sources configured",
    ]),
)
_PROCESS_RETURNCODE = st.one_of(
    st.integers(min_value=0, max_value=255),
    # SIGINT/SIGQUIT can be inherited as ignored by a noninteractive shell;
    # these signals reliably terminate the executable fake harness.
    st.sampled_from([-1, -6, -9, -15]),
)


def assert_terminal_failure_has_a_human_reason(
    outcome: RunImportOutcome,
) -> None:
    """A terminal producer result never falls through to the rc-only token."""
    assert outcome.exit_code != 0
    error = _harness_failure_error(outcome, outcome.exit_code)
    assert error
    assert error != f"Harness returned rc={outcome.exit_code}", (
        f"terminal producer result had no observed reason: {outcome!r}"
    )


class TestRunImportFailureReasonProperties(unittest.TestCase):

    @settings(max_examples=40)
    @given(
        process_status=_PROCESS_RETURNCODE,
        stderr_lines=st.lists(_STDERR_LINE, max_size=4),
    )
    def test_terminal_process_worlds_keep_the_most_specific_observed_reason(
        self,
        process_status: int,
        stderr_lines: list[str],
    ) -> None:
        outcome = run_import_with_fake_harness(
            process_status=process_status,
            stderr_lines=stderr_lines,
        )

        self.assertEqual(outcome.exit_code, 2)
        assert_terminal_failure_has_a_human_reason(outcome)
        error = _harness_failure_error(outcome, outcome.exit_code)
        retained_lines = [
            line.strip()
            for line in stderr_lines
            if line.strip() and "Disabled fetchart" not in line
        ]
        if process_status == 0:
            self.assertEqual(
                error,
                "beets harness ended without applying requested release "
                "release-under-test",
            )
        elif retained_lines:
            self.assertEqual(error, retained_lines[-1])
        elif process_status < 0:
            self.assertEqual(
                error,
                f"beets harness terminated by signal {-process_status}",
            )
        else:
            self.assertEqual(
                error,
                f"beets harness exited with status {process_status}",
            )


class TestInvariantCheckerTripsOnKnownBad(unittest.TestCase):

    def test_rc_only_fallback_is_rejected(self) -> None:
        known_bad = RunImportOutcome(2, [])
        with self.assertRaises(AssertionError):
            assert_terminal_failure_has_a_human_reason(known_bad)


if __name__ == "__main__":
    unittest.main()
