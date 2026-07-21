"""Contract tests for the unattended unstable lock-update runner."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.fakes.daily_flake_update import FakeDailyFlakeUpdateCommands


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "daily_flake_update.sh"


class TestDailyFlakeUpdateScript(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDailyFlakeUpdateCommands(Path(self.tempdir.name))

    def test_green_candidate_runs_every_gate_and_pushes_only_lock(self) -> None:
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            state["stages"],
            ["pyright", "suite", "flake-check", "world", "fuzz", "mirror"],
        )
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["push_count"], 1)
        self.assertEqual(state["push_ref"], "HEAD:refs/heads/main")
        self.assertIn("--only", state["commit_args"])
        self.assertEqual(state["commit_args"][-2:], ["--", "flake.lock"])
        self.assertIn("Refs #498", state["commit_args"])
        self.assertIn("ALL CANDIDATE GATES GREEN", proc.stdout)
        self.assertIn("pushed updated flake.lock", proc.stdout)

        clone_path = Path(state["clone_path"])
        self.assertFalse(clone_path.exists())
        for stage, stage_env in state["stage_env"].items():
            self.assertIsNone(stage_env["TEST_DB_DSN"], stage)

    def test_failed_gate_runs_later_gates_and_pushes_nothing(self) -> None:
        self.fake.update_state(fault="world")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(
            state["stages"],
            ["pyright", "suite", "flake-check", "world", "fuzz", "mirror"],
        )
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("FAIL world-model burst", proc.stdout)
        self.assertIn("PASS mirror-harness smoke", proc.stdout)
        self.assertIn("candidate failed; flake.lock was not committed", proc.stderr)

    def test_unchanged_lock_still_runs_gates_without_commit(self) -> None:
        self.fake.update_state(lock_changed=False)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(len(state["stages"]), 6)
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("flake.lock already current", proc.stdout)

    def test_update_failure_stops_before_candidate_gates_or_push(self) -> None:
        self.fake.update_state(fault="update")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["stages"], [])
        self.assertEqual(state["push_count"], 0)
        self.assertIn("flake update failed", proc.stderr)

    def test_push_failure_is_reported_as_the_single_run_failure(self) -> None:
        self.fake.update_state(fault="push")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("push failed", proc.stderr)

    def test_commit_failure_never_attempts_a_push(self) -> None:
        self.fake.update_state(fault="commit")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("lock commit failed", proc.stderr)

    def test_state_paths_and_mirror_budget_are_explicit(self) -> None:
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        world = state["stage_env"]["world"]
        fuzz = state["stage_env"]["fuzz"]
        mirror = state["stage_env"]["mirror"]
        self.assertEqual(
            world["CRATEDIGGER_WORLD_DATABASE"],
            str(self.fake.automation_state / "hypothesis" / "world-model"),
        )
        self.assertEqual(
            fuzz["HYPOTHESIS_STORAGE_DIRECTORY"],
            str(self.fake.automation_state / "hypothesis" / "fuzz"),
        )
        self.assertEqual(
            fuzz["CRATEDIGGER_FUZZ_OUTPUT_DIR"],
            str(self.fake.automation_state / "fuzz-failures"),
        )
        self.assertEqual(mirror["CRATEDIGGER_WORLD_ENGINE"], "mirror-harness")
        self.assertEqual(
            mirror["CRATEDIGGER_WORLD_MIRROR_URL"],
            "http://mirror.example.test/ws/2",
        )
        self.assertEqual(mirror["CRATEDIGGER_WORLD_EXAMPLES"], "2")
        self.assertEqual(mirror["CRATEDIGGER_WORLD_STEPS"], "5")

    def test_missing_required_configuration_fails_before_clone(self) -> None:
        proc = self.fake.run(
            SCRIPT,
            extra_env={"CRATEDIGGER_MIRROR_URL": ""},
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIsNone(self.fake.state["clone_path"])
        self.assertIn("CRATEDIGGER_MIRROR_URL", proc.stderr)


if __name__ == "__main__":
    unittest.main()
