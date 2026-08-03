"""Contract tests for the unattended Beets tip canary updater."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.fakes.daily_flake_update import FakeDailyFlakeUpdateCommands

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "daily_beets_tip_update.sh"


class TestDailyBeetsTipUpdateScript(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDailyFlakeUpdateCommands(Path(self.tempdir.name))

    def test_green_tip_canary_updates_only_its_lock_and_pushes(self) -> None:
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(["nix", "flake", "update", "beets-tip"], state["events"])
        self.assertEqual(
            state["stages"], ["tip-build", "tip-contract", "tip-pyright"]
        )
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["push_count"], 1)
        self.assertEqual(state["push_ref"], "HEAD:refs/heads/main")
        self.assertEqual(state["commit_args"][-2:], ["--", "flake.lock"])
        self.assertIn("Refs #992", state["commit_args"])
        self.assertIsNone(state["stage_env"]["tip-contract"]["TEST_DB_DSN"])

    def test_failed_canary_never_commits_or_pushes(self) -> None:
        self.fake.update_state(fault="tip-contract")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["stages"], ["tip-build", "tip-contract"])
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)

    def test_unchanged_tip_lock_still_proves_the_canary(self) -> None:
        self.fake.update_state(lock_changed=False)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            state["stages"], ["tip-build", "tip-contract", "tip-pyright"]
        )
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("flake.lock already current", proc.stdout)


if __name__ == "__main__":
    unittest.main()
