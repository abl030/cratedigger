"""Contracts for RAM-backed test scratch storage."""

from __future__ import annotations

import os
import subprocess
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
        self.assertEqual(selected.parent, Path("/dev/shm"))
        self.assertTrue(selected.name.startswith("cratedigger-tests."))
        self.assertFalse(selected.exists())

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
