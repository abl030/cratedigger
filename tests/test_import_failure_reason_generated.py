"""Generated patrol for truthful terminal ``run_import`` failure reasons."""

from __future__ import annotations

import string
import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from harness.import_one import (
    RunImportOutcome,
    _harness_failure_error,
    _terminal_process_reason,
)
from tests.test_import_one_stages import (
    FAKE_HARNESS_FATAL_SIGNAL,
    run_import_with_fake_harness,
)

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
    # Only an unignorable, unblockable signal makes the REQUESTED world the
    # OBSERVED one: SIG_IGN and the blocked mask are both inherited across
    # exec, so any other signal silently degrades to "exited 0" under an
    # ancestor holding it. See FAKE_HARNESS_FATAL_SIGNAL for the full
    # account; every other signal NUMBER is exercised process-free below.
    st.just(-FAKE_HARNESS_FATAL_SIGNAL),
)
#: Signal numbers a real harness death can carry, plus the whole ordinary
#: exit-status domain. Drives the pure producer, so nothing here needs a
#: process to observe it.
_OBSERVED_TERMINAL_STATUS = st.one_of(
    st.integers(min_value=-64, max_value=-1),
    st.integers(min_value=0, max_value=255),
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


def terminal_reason_violations(proc_rc: int, reason: str) -> list[str]:
    """Every way a terminal-status reason can misdescribe what was observed.

    Accumulating, not short-circuiting: a world that violates several
    clauses reports all of them, so clause order can never mask one.
    """
    violations: list[str] = []
    if not reason.strip():
        violations.append("terminal reason was blank")
    if str(abs(proc_rc)) not in reason:
        violations.append("terminal reason omits the observed status number")
    if f"-{abs(proc_rc)}" in reason:
        violations.append("terminal reason rendered a negative number")
    if proc_rc < 0 and "signal" not in reason:
        violations.append("signal death was not named as a signal")
    if proc_rc >= 0 and "signal" in reason:
        violations.append("ordinary exit status was named as a signal")
    return violations


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
        else:
            # ROUTING, not wording: this proves run_import reaches the
            # producer at all. Comparing production against the producer
            # that generates it is a tautology about the sentence itself,
            # so the operator-facing wording stays pinned by the
            # hand-typed literals in TestRunImportFailureReasons — do not
            # "simplify" those into producer calls.
            self.assertEqual(error, _terminal_process_reason(process_status))

    @settings(max_examples=200)
    @given(proc_rc=_OBSERVED_TERMINAL_STATUS)
    def test_every_observed_terminal_status_is_named_specifically(
        self, proc_rc: int,
    ) -> None:
        reason = _terminal_process_reason(proc_rc)

        self.assertEqual(terminal_reason_violations(proc_rc, reason), [])
        # A signal death and an exit status sharing digits must never read
        # the same: the sign is the whole diagnosis for an operator.
        if proc_rc != 0:
            self.assertNotEqual(reason, _terminal_process_reason(-proc_rc))


class TestInvariantCheckerTripsOnKnownBad(unittest.TestCase):

    def test_rc_only_fallback_is_rejected(self) -> None:
        known_bad = RunImportOutcome(2, [])
        with self.assertRaises(AssertionError):
            assert_terminal_failure_has_a_human_reason(known_bad)

    def test_blank_reason_is_rejected(self) -> None:
        self.assertIn(
            "terminal reason was blank",
            terminal_reason_violations(9, "   "),
        )

    def test_omitted_status_number_is_rejected(self) -> None:
        self.assertIn(
            "terminal reason omits the observed status number",
            terminal_reason_violations(23, "beets harness exited with status"),
        )

    def test_negative_rendering_is_rejected(self) -> None:
        self.assertIn(
            "terminal reason rendered a negative number",
            terminal_reason_violations(
                -9, "beets harness terminated by signal -9"),
        )

    def test_unnamed_signal_death_is_rejected(self) -> None:
        self.assertIn(
            "signal death was not named as a signal",
            terminal_reason_violations(
                -9, "beets harness exited with status 9"),
        )

    def test_status_named_as_a_signal_is_rejected(self) -> None:
        self.assertIn(
            "ordinary exit status was named as a signal",
            terminal_reason_violations(
                9, "beets harness terminated by signal 9"),
        )


if __name__ == "__main__":
    unittest.main()
