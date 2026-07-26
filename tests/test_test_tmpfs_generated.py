"""Generated failure-contract tests for ``scripts/test_tmpfs.sh`` (issue #873).

The deterministic low-headroom pin lives in ``tests/test_test_tmpfs.py``.
This property varies a safe inherited ``TMPDIR`` while driving that same real
shell-helper subprocess: a setup failure must stay nonzero, emit no selected
directory, and retain the helper's headroom diagnostic.  The checker has a
known-bad self-test for the historical ``returncode=0`` plus inherited-stdout
shape.
"""

from __future__ import annotations

import os
import re
import subprocess
import unittest

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given, settings
from hypothesis import strategies as st

from tests.test_test_tmpfs import run_tmpfs_setup_and_print_tmpdir


_INHERITED_TMPDIR_NAMES = st.from_regex(
    r"[A-Za-z0-9_-]{1,24}", fullmatch=True,
)
_IMPOSSIBLE_HEADROOM_BYTES = 1 << 50


def assert_tmpfs_setup_failure_contract(
    completed: subprocess.CompletedProcess[str],
    *,
    inherited_tmpdir: str,
    runtime_dir: str,
    minimum_bytes: int,
) -> None:
    """Reject masked setup failure or any inherited TMPDIR success shape."""
    if completed.returncode == 0:
        raise AssertionError("tmpfs setup failure was reported as success")
    if completed.stdout != "":
        raise AssertionError(
            "tmpfs setup failure exposed a selected TMPDIR: "
            f"{completed.stdout!r} (inherited {inherited_tmpdir!r})"
        )
    expected_diagnostic = re.compile(
        rf"^Test RAM root lacks headroom: {re.escape(runtime_dir)} has \d+ "
        rf"bytes, needs {minimum_bytes}\n$",
    )
    if not expected_diagnostic.fullmatch(completed.stderr):
        raise AssertionError(
            "tmpfs setup failure lost its headroom diagnostic: "
            f"{completed.stderr!r}"
        )


class TestGeneratedTmpfsSetupFailure(unittest.TestCase):
    """I1: setup failure is never mistaken for a TMPDIR allocation."""

    @settings(max_examples=24)
    @given(name=_INHERITED_TMPDIR_NAMES)
    @example(name="historical-inherited-tmpdir")
    def test_setup_failure_never_reports_inherited_tmpdir(self, name: str) -> None:
        inherited_tmpdir = f"/tmp/cratedigger-inherited-{name}"
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        completed = run_tmpfs_setup_and_print_tmpdir(
            env={
                **os.environ,
                "TMPDIR": inherited_tmpdir,
                "CRATEDIGGER_TEST_RAM_MIN_BYTES": str(_IMPOSSIBLE_HEADROOM_BYTES),
            },
        )

        assert_tmpfs_setup_failure_contract(
            completed,
            inherited_tmpdir=inherited_tmpdir,
            runtime_dir=runtime_dir,
            minimum_bytes=_IMPOSSIBLE_HEADROOM_BYTES,
        )


class TestTmpfsSetupFailureCheckersTripOnViolations(unittest.TestCase):
    """Known-bad qualification for the semicolon-sequencing regression."""

    def test_checker_rejects_historical_success_and_inherited_stdout(self) -> None:
        inherited_tmpdir = "/tmp/cratedigger-inherited-historical"
        completed = subprocess.CompletedProcess(
            args=("bash",),
            returncode=0,
            stdout=inherited_tmpdir,
            stderr="Test RAM root lacks headroom: /run/user/1000 has 1 bytes, needs 2\n",
        )

        with self.assertRaises(AssertionError):
            assert_tmpfs_setup_failure_contract(
                completed,
                inherited_tmpdir=inherited_tmpdir,
                runtime_dir="/run/user/1000",
                minimum_bytes=2,
            )


if __name__ == "__main__":
    unittest.main()
