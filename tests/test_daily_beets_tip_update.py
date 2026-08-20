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
        self.addCleanup(self.fake.close)

    def test_green_tip_canary_proves_the_suite_and_publishes_nothing(self) -> None:
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(
            [
                "nix", "flake", "update",
                "beets-tip", "mutagen-tip", "mediafile-tip",
            ],
            state["events"],
        )
        # One run of the whole deterministic suite, against a shell whose
        # Beets, mutagen and mediafile are all at tip. The hand-picked
        # target list it replaced could not fail on a test nobody had
        # remembered to name.
        self.assertEqual(state["stages"], ["tip-suite"])
        self.assertIsNone(state["stage_env"]["tip-suite"]["TEST_DB_DSN"])
        for node in ("beets-tip", "mutagen-tip", "mediafile-tip"):
            self.assertEqual(
                state["lock_after_update"]["nodes"][node]["locked"]["rev"],
                f"new-{node}",
                f"{node} must advance for the run",
            )
        self.assertEqual(
            state["lock_after_update"]["nodes"]["nixpkgs"],
            state["lock_before"]["nodes"]["nixpkgs"],
        )
        # The canary's product is the signal, not a stored revision. It
        # re-resolves every tip input at the START of each run, so a
        # published value would be overwritten before anything read it —
        # and publishing was the sole reason an unrelated merge landing
        # mid-run could reject the push and report someone else's merge as
        # a red canary (observed live, 2026-08-15).
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("nothing published", proc.stdout)

    def test_a_moved_branch_cannot_reach_the_canary_at_all(self) -> None:
        """The fake refuses a push whenever the branch has moved. A canary
        that publishes nothing never reaches that refusal, so this world is
        indistinguishable from the ordinary one — which is the whole point
        of not publishing."""
        self.fake.update_state(remote_moved=True)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["stages"], ["tip-suite"])
        self.assertEqual(state["push_count"], 0)

    def test_failed_canary_reports_the_failure(self) -> None:
        self.fake.update_state(fault="tip-suite")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["stages"], ["tip-suite"])
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertNotIn("nothing published", proc.stdout)

    def test_unmoved_upstream_still_gets_a_full_suite_run(self) -> None:
        """The canary never short-circuits on the lock — it does not
        consult it. An upstream that has not moved since yesterday is still
        proved today, because the suite around it changed."""
        self.fake.update_state(lock_changed=False)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["stages"], ["tip-suite"])
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)


if __name__ == "__main__":
    unittest.main()
