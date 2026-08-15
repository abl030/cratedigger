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
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["push_count"], 1)
        self.assertEqual(state["push_ref"], "HEAD:refs/heads/main")
        self.assertEqual(state["commit_args"][-2:], ["--", "flake.lock"])
        self.assertIn("Refs #992", state["commit_args"])
        self.assertIsNone(state["stage_env"]["tip-suite"]["TEST_DB_DSN"])
        for node in ("beets-tip", "mutagen-tip", "mediafile-tip"):
            self.assertEqual(
                state["lock_after_update"]["nodes"][node]["locked"]["rev"],
                f"new-{node}",
                f"{node} must advance with the canary",
            )
        self.assertEqual(
            state["lock_after_update"]["nodes"]["nixpkgs"],
            state["lock_before"]["nodes"]["nixpkgs"],
        )
        self.assertEqual(state["lock_at_commit"], state["lock_after_update"])

    def test_concurrent_branch_push_is_rebased_onto_not_reported_as_failure(
        self,
    ) -> None:
        """A merge landing on main while the canary runs its suite is
        ordinary. Before the rebase the runner pushed unconditionally and
        Git rejected it non-fast-forward, turning someone else's merge into
        a red canary and an RCA alert (observed live, 2026-08-15)."""
        self.fake.update_state(remote_moved=True)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["pull_count"], 1)
        self.assertEqual(state["push_count"], 1)
        events = [event[:2] for event in state["events"]]
        self.assertLess(
            events.index(["git", "pull"]),
            events.index(["git", "push"]),
            "the rebase must precede the push",
        )

    def test_failed_canary_never_commits_or_pushes(self) -> None:
        self.fake.update_state(fault="tip-suite")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["stages"], ["tip-suite"])
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)

    def test_unchanged_tip_lock_still_proves_the_canary(self) -> None:
        self.fake.update_state(lock_changed=False)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["stages"], ["tip-suite"])
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("flake.lock already current", proc.stdout)


if __name__ == "__main__":
    unittest.main()
