"""Contracts for RAM-backed test scratch storage."""

from __future__ import annotations

import ast
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
TMPFS_SETUP = REPO_ROOT / "scripts" / "test_tmpfs.sh"
NIX_SHELL = REPO_ROOT / "nix" / "shell.nix"


class TestTmpfsSetup(unittest.TestCase):
    def test_allocates_isolated_tmpfs_directory_and_cleans_it_on_exit(self) -> None:
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; setup_cratedigger_test_tmpfs; printf "%s" "$TMPDIR"',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        selected = Path(completed.stdout)
        runtime_dir = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertEqual(selected.parent, runtime_dir)
        self.assertTrue(selected.name.startswith("cratedigger-tests."))
        self.assertFalse(selected.exists())

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
                'source "$1"; setup_cratedigger_test_tmpfs; exit 7',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
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
