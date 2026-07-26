"""Generated command-result coverage for the final-gate receipt launcher."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from tests.test_final_gate_receipt import HELPER, _FAKE_NIX_SHELL


def assert_receipt_contract(
    *, command: str, exit_code: int, terminal: str | None,
    helper_live: bool, gate_live: bool, tree_matches: bool,
) -> str:
    if command != "pyright --threads 4":
        raise AssertionError("receipt command is not canonical")
    if terminal is not None:
        if exit_code >= 128:
            raise AssertionError("signal-shaped exit has a terminal receipt")
        if not tree_matches:
            raise AssertionError("changed tree has a terminal receipt")
        expected = "pass 0" if exit_code == 0 else f"fail {exit_code}"
        if terminal != expected:
            raise AssertionError(f"expected {expected}, got {terminal}")
        return "pass" if exit_code == 0 else "fail"
    return "exact-active" if helper_live and gate_live else "incomplete"


class TestFinalGateReceiptGenerated(unittest.TestCase):
    def test_invariant_checker_trips_on_known_bad_receipts(self) -> None:
        bad_cases: tuple[tuple[str, int, str, bool, bool, bool], ...] = (
            ("echo substituted", 0, "pass 0", False, False, True),
            ("pyright --threads 4", 143, "fail 143", False, False, True),
            ("pyright --threads 4", 0, "pass 0", False, False, False),
        )
        for command, exit_code, terminal, helper_live, gate_live, tree_matches in bad_cases:
            with self.subTest(command=command, exit_code=exit_code), self.assertRaises(AssertionError):
                assert_receipt_contract(
                    command=command, exit_code=exit_code, terminal=terminal,
                    helper_live=helper_live, gate_live=gate_live, tree_matches=tree_matches,
                )
        self.assertEqual(assert_receipt_contract(
            command="pyright --threads 4", exit_code=0, terminal=None,
            helper_live=False, gate_live=False, tree_matches=True,
        ), "incomplete")
        self.assertNotEqual(
            assert_receipt_contract(
                command="pyright --threads 4", exit_code=0, terminal=None,
                helper_live=False, gate_live=False, tree_matches=True,
            ), "pass",
            "missing terminal must never be green",
        )
        self.assertNotEqual(
            assert_receipt_contract(
                command="pyright --threads 4", exit_code=0, terminal=None,
                helper_live=False, gate_live=True, tree_matches=True,
            ),
            "exact-active",
            "stale helper identity must never be active",
        )

    @settings(max_examples=12, deadline=None)
    @given(exit_code=st.integers(min_value=0, max_value=125))
    def test_canonical_fake_nix_shell_preserves_every_ordinary_exit_code(
        self, exit_code: int,
    ) -> None:
        runtime = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"))
        self.assertTrue(runtime.is_dir(), "private runtime tmpfs is required for this test")
        with tempfile.TemporaryDirectory(prefix="cratedigger-final-gate-repo-") as repo, \
             tempfile.TemporaryDirectory(prefix="cratedigger-final-gate-bin-") as fake_bin:
            fake_nix_shell = Path(fake_bin) / "nix-shell"
            fake_nix_shell.write_text(_FAKE_NIX_SHELL)
            fake_nix_shell.chmod(0o755)
            record = Path(fake_bin) / "nix-shell.argv"
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "tests@example.invalid"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Final gate tests"], cwd=repo, check=True)
            Path(repo, "README").write_text("receipt fixture\n")
            subprocess.run(["git", "add", "README"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "fixture"], cwd=repo, check=True)
            env = os.environ | {
                "XDG_RUNTIME_DIR": str(runtime),
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "FAKE_NIX_SHELL_RECORD": str(record),
                "FAKE_NIX_SHELL_MODE": "exit",
                "FAKE_NIX_SHELL_EXIT": str(exit_code),
                "FAKE_NIX_SHELL_REPO": repo,
            }
            result = subprocess.run(
                [str(HELPER), "pyright"], cwd=repo, text=True, capture_output=True, env=env,
            )
            receipt = Path(result.stdout.splitlines()[0].removeprefix("receipt: "))
            self.addCleanup(shutil.rmtree, receipt, True)

            terminal = (receipt / "terminal").read_text().strip()
            self.assertEqual(result.returncode, exit_code, result.stderr)
            self.assertEqual(record.read_text(), "--run\npyright --threads 4\n")
            self.assertEqual(
                assert_receipt_contract(
                    command=(receipt / "command").read_text().strip(), exit_code=exit_code,
                    terminal=terminal, helper_live=False, gate_live=False, tree_matches=True,
                ),
                "pass" if exit_code == 0 else "fail",
            )
