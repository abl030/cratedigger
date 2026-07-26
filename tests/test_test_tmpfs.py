"""Contracts for RAM-backed test scratch storage."""

from __future__ import annotations

import ast
import os
import re
import stat
import subprocess
import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
TMPFS_SETUP = REPO_ROOT / "scripts" / "test_tmpfs.sh"
NIX_SHELL = REPO_ROOT / "nix" / "shell.nix"
TMPFS_SETUP_AND_PRINT_TMPDIR = (
    'source "$1" && setup_cratedigger_test_tmpfs && printf "%s" "$TMPDIR"'
)
LOW_HEADROOM_MINIMUM_BYTES = 1 << 50


def run_tmpfs_setup_and_print_tmpdir(
    *,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Drive the real shell helper and report the TMPDIR it selected."""
    return subprocess.run(
        [
            "bash",
            "-c",
            TMPFS_SETUP_AND_PRINT_TMPDIR,
            "bash",
            str(TMPFS_SETUP),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def tmpfs_runtime_root() -> str:
    """Return the helper's default root when no explicit override is set."""
    return os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")


def allocation_environment() -> dict[str, str]:
    """Make allocation seam tests independent of shared tmpfs headroom."""
    return {**os.environ, "CRATEDIGGER_TEST_RAM_MIN_BYTES": "0"}


def low_headroom_environment(
    *,
    inherited_tmpdir: str,
    minimum_bytes: int,
) -> dict[str, str]:
    """Force the helper's real default tmpfs root below the headroom gate."""
    env = dict(os.environ)
    env.pop("CRATEDIGGER_TEST_RAM_ROOT", None)
    env["TMPDIR"] = inherited_tmpdir
    env["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = str(minimum_bytes)
    return env


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
            f"tmpfs setup failure lost its headroom diagnostic: {completed.stderr!r}"
        )


class TestTmpfsSetup(unittest.TestCase):
    def test_allocates_isolated_tmpfs_directory_and_cleans_it_on_exit(self) -> None:
        completed = run_tmpfs_setup_and_print_tmpdir(env=allocation_environment())

        self.assertEqual(completed.returncode, 0, completed.stderr)
        selected = Path(completed.stdout)
        runtime_dir = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertEqual(selected.parent, runtime_dir)
        self.assertTrue(selected.name.startswith("cratedigger-tests."))
        self.assertFalse(selected.exists())

    def test_low_headroom_does_not_report_inherited_tmpdir_as_allocation(self) -> None:
        runtime_dir = tmpfs_runtime_root()
        inherited_tmpdir = "/tmp/cratedigger-inherited-tmpdir"

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

    def test_active_tmpdir_has_private_ancestry(self) -> None:
        current = Path(tempfile.gettempdir()).resolve()
        while True:
            mode = stat.S_IMODE(current.stat().st_mode)
            self.assertEqual(
                mode & 0o022,
                0,
                f"test TMPDIR has replaceable ancestor: {current}",
            )
            if current.parent == current:
                break
            current = current.parent

    def test_test_fixtures_do_not_bypass_tmpdir_for_repository_scratch(self) -> None:
        forbidden_dir_expressions = {
            "os.getcwd()",
            "Path.cwd()",
            "REPO_ROOT",
            "self._repo_root",
        }
        offenders: list[str] = []

        for path in sorted((REPO_ROOT / "tests").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                for keyword in node.keywords:
                    if keyword.arg != "dir":
                        continue
                    rendered = ast.unparse(keyword.value)
                    if rendered in forbidden_dir_expressions:
                        offenders.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno}: {rendered}"
                        )

        self.assertEqual(offenders, [])

    def test_rejects_world_writable_tmpfs_ancestry(self) -> None:
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; setup_cratedigger_test_tmpfs',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env={
                **os.environ,
                "CRATEDIGGER_TEST_RAM_ROOT": "/dev/shm",
                "CRATEDIGGER_TEST_RAM_MIN_BYTES": "0",
            },
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("replaceable ancestor", completed.stderr)

    def test_rejects_disk_backed_override_instead_of_falling_back(self) -> None:
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; setup_cratedigger_test_tmpfs',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env={
                **os.environ,
                "CRATEDIGGER_TEST_RAM_ROOT": str(REPO_ROOT),
            },
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("is not tmpfs", completed.stderr)

    def test_cleanup_preserves_the_command_exit_status(self) -> None:
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1" && setup_cratedigger_test_tmpfs && exit 7',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env=allocation_environment(),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 7, completed.stderr)

    def test_nix_shell_activates_tmpfs_before_dev_commands(self) -> None:
        source = NIX_SHELL.read_text(encoding="utf-8")

        self.assertIn("scripts/test_tmpfs.sh", source)
        self.assertIn("setup_cratedigger_test_tmpfs", source)


if __name__ == "__main__":
    unittest.main()
