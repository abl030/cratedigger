"""Generated failure-contract tests for ``scripts/test_tmpfs.sh`` (issue #873).

The deterministic low-headroom pin lives in ``tests/test_test_tmpfs.py``.
This property varies a safe inherited ``TMPDIR`` while driving that same real
shell-helper subprocess: a setup failure must stay nonzero, emit no selected
directory, and retain the helper's headroom diagnostic. The checker separately
qualifies known-bad status, inherited-stdout, and diagnostic-loss shapes.
"""

from __future__ import annotations

import subprocess
import unittest

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given, settings
from hypothesis import strategies as st

from tests.test_test_tmpfs import (
    LOW_HEADROOM_MINIMUM_BYTES,
    assert_tmpfs_setup_failure_contract,
    low_headroom_environment,
    run_tmpfs_setup_and_print_tmpdir,
    tmpfs_runtime_root,
)


_INHERITED_TMPDIR_NAMES = st.from_regex(
    r"[A-Za-z0-9_-]{1,24}",
    fullmatch=True,
)


class TestGeneratedTmpfsSetupFailure(unittest.TestCase):
    """I1: setup failure is never mistaken for a TMPDIR allocation."""

    @settings(max_examples=24)
    @given(name=_INHERITED_TMPDIR_NAMES)
    @example(name="historical-inherited-tmpdir")
    def test_setup_failure_never_reports_inherited_tmpdir(self, name: str) -> None:
        inherited_tmpdir = f"/tmp/cratedigger-inherited-{name}"
        runtime_dir = tmpfs_runtime_root()
        completed = run_tmpfs_setup_and_print_tmpdir(
            env=low_headroom_environment(
                inherited_tmpdir=inherited_tmpdir,
                minimum_bytes=LOW_HEADROOM_MINIMUM_BYTES,
            ),
        )

        assert_tmpfs_setup_failure_contract(
            completed,
            inherited_tmpdir=inherited_tmpdir,
            runtime_dir=runtime_dir,
            minimum_bytes=LOW_HEADROOM_MINIMUM_BYTES,
        )


class TestTmpfsSetupFailureCheckersTripOnViolations(unittest.TestCase):
    """Known-bad qualification for the semicolon-sequencing regression."""

    def _valid_failure(
        self,
        *,
        returncode: int = 1,
        stdout: str = "",
        stderr: str = (
            "Test RAM root lacks headroom: /run/user/1000 has 1 bytes, needs 2\n"
        ),
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=("bash",),
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )

    def _assert_checker_rejects(
        self, completed: subprocess.CompletedProcess[str]
    ) -> None:
        with self.assertRaises(AssertionError):
            assert_tmpfs_setup_failure_contract(
                completed,
                inherited_tmpdir="/tmp/cratedigger-inherited-historical",
                runtime_dir="/run/user/1000",
                minimum_bytes=2,
            )

    def test_checker_rejects_zero_status_with_valid_output_and_diagnostic(self) -> None:
        self._assert_checker_rejects(self._valid_failure(returncode=0))

    def test_checker_rejects_inherited_stdout_with_nonzero_status(self) -> None:
        self._assert_checker_rejects(
            self._valid_failure(stdout="/tmp/cratedigger-inherited-historical")
        )

    def test_checker_rejects_missing_diagnostic_with_nonzero_empty_output(self) -> None:
        self._assert_checker_rejects(self._valid_failure(stderr=""))


if __name__ == "__main__":
    unittest.main()
