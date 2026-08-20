"""Contract tests for the unattended unstable lock-update runner."""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests._source_pins import pinned_source
from tests.fakes.daily_flake_update import FakeDailyFlakeUpdateCommands

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "daily_flake_update.sh"
TIP_SCRIPT = REPO_ROOT / "scripts" / "daily_beets_tip_update.sh"


class TestDailyFlakeUpdateScript(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDailyFlakeUpdateCommands(Path(self.tempdir.name))
        self.addCleanup(self.fake.close)

    def fake_environment(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.fake.fake_bin}:{env['PATH']}",
                "DAILY_UPDATE_FAKE_STATE": str(self.fake.state_path),
                "CRATEDIGGER_AUTOMATION_STATE_DIR": str(
                    self.fake.automation_state
                ),
                "CRATEDIGGER_MIRROR_URL": "http://mirror.example.test/ws/2",
                "CRATEDIGGER_UPDATE_REPOSITORY": (
                    "https://github.com/abl030/cratedigger.git"
                ),
                "CRATEDIGGER_UPDATE_BRANCH": "main",
                "TMPDIR": str(self.fake.tmpdir),
                "TEST_DB_DSN": "postgresql://production-must-not-leak",
            }
        )
        return env

    def test_green_candidate_runs_every_gate_and_pushes_only_lock(self) -> None:
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(["nix", "flake", "update", "nixpkgs"], state["events"])
        self.assertEqual(
            state["stages"],
            ["suite", "stable-candidate", "world", "fuzz", "mirror"],
        )
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["push_count"], 1)
        self.assertEqual(state["push_ref"], "HEAD:refs/heads/main")
        self.assertIn("--only", state["commit_args"])
        self.assertEqual(state["commit_args"][-2:], ["--", "flake.lock"])
        self.assertIn("Refs #498", state["commit_args"])
        self.assertIn("ALL CANDIDATE GATES GREEN", proc.stdout)
        self.assertIn("pushed updated flake.lock", proc.stdout)
        self.assertEqual(
            proc.stdout.count("CRATEDIGGER_DAILY_RESOURCE_RECEIPT "), 1
        )
        self.assertIn(
            "CRATEDIGGER_DAILY_RESOURCE_RECEIPT schema=1 status=valid",
            proc.stdout,
        )
        self.assertIn("dropped_samples=0", proc.stdout)
        self.assertNotIn("resource receipt invalid", proc.stderr)
        for phase in (
            "deterministic_suite",
            "stable_nix",
            "world_model",
            "generated_fuzz",
            "mirror_harness",
            "cleanup",
        ):
            self.assertIn(
                f"CRATEDIGGER_DAILY_RESOURCE_PHASE schema=1 phase={phase} ",
                proc.stdout,
            )
        self.assertEqual(
            state["lock_after_update"]["nodes"]["nixpkgs"]["locked"]["rev"],
            "new-nixpkgs",
        )
        for node in ("beets-tip", "mutagen-tip", "mediafile-tip"):
            self.assertEqual(
                state["lock_after_update"]["nodes"][node],
                state["lock_before"]["nodes"][node],
                f"the nixpkgs candidate must not advance {node}",
            )
        self.assertEqual(state["lock_at_commit"], state["lock_after_update"])

        clone_path = Path(state["clone_path"])
        self.assertFalse(clone_path.exists())
        for stage, stage_env in state["stage_env"].items():
            self.assertIsNone(stage_env["TEST_DB_DSN"], stage)

    def test_concurrent_branch_push_is_rebased_onto_not_reported_as_failure(
        self,
    ) -> None:
        """This runner's clone-to-push window is the whole candidate gate,
        so an unrelated merge landing meanwhile is likelier here than in the
        tip canary — and used to lose the whole night's green lock to a
        non-fast-forward rejection."""
        self.fake.update_state(remote_moved=True)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["pull_count"], 1)
        self.assertEqual(state["push_count"], 1)
        self.assertIn("pushed updated flake.lock", proc.stdout)

    def test_failed_gate_runs_later_gates_and_pushes_nothing(self) -> None:
        self.fake.update_state(fault="world")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(
            state["stages"],
            ["suite", "stable-candidate", "world", "fuzz", "mirror"],
        )
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("FAIL world-model burst", proc.stdout)
        self.assertIn("PASS mirror-harness smoke", proc.stdout)
        self.assertIn("candidate failed; flake.lock was not committed", proc.stderr)
        self.assertEqual(
            proc.stdout.count("CRATEDIGGER_DAILY_RESOURCE_RECEIPT "), 1
        )
        self.assertIn(
            "CRATEDIGGER_DAILY_RESOURCE_RECEIPT schema=1 status=valid",
            proc.stdout,
        )
        self.assertIn("dropped_samples=0", proc.stdout)
        # A healthy monitor on an ordinary gate failure gets no invalid-
        # receipt diagnostic -- issue #1214 gap 4 is about an INVALID
        # receipt surfacing, not every failing run growing new output.
        self.assertNotIn("resource receipt invalid", proc.stderr)

    def test_unchanged_lock_still_runs_gates_without_commit(self) -> None:
        self.fake.update_state(lock_changed=False)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(len(state["stages"]), 5)
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

    def test_state_paths_and_unattended_budgets_are_explicit(self) -> None:
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
        self.assertEqual(fuzz["CRATEDIGGER_FUZZ_MAX_EXAMPLES"], "20000")
        self.assertEqual(mirror["CRATEDIGGER_WORLD_ENGINE"], "mirror-harness")
        self.assertEqual(
            mirror["CRATEDIGGER_WORLD_MIRROR_URL"],
            "http://mirror.example.test/ws/2",
        )
        self.assertEqual(mirror["CRATEDIGGER_WORLD_EXAMPLES"], "2")
        self.assertEqual(mirror["CRATEDIGGER_WORLD_STEPS"], "5")

    def test_deterministic_suite_stage_sets_the_suite_owns_headroom_env_var(
        self,
    ) -> None:
        """Issue #1111 review MAJOR-1/MAJOR-3: the nightly deterministic_suite
        stage's own nix-shell invocation must set
        CRATEDIGGER_SUITE_OWNS_HEADROOM=1 — without it the unattended
        launcher dies at shell entry under contention with the old unnamed
        message, and run_stage records that as an indistinguishable "FAIL
        deterministic full suite" rather than the named exhaustion. Pinned
        as an exact block, the same grep-the-source shape as
        tests/test_targeted_test_selection.py's scripts/test.sh pin, so
        deleting just this var (not some other CRATEDIGGER_SUITE_OWNS_
        HEADROOM occurrence) fails this test."""
        source = pinned_source(SCRIPT)

        self.assertIn(
            'run_stage deterministic_suite "deterministic full suite" \\\n'
            "    env CRATEDIGGER_SUITE_OWNS_HEADROOM=1 \\\n"
            '    nix-shell --run "bash scripts/run_tests.sh"',
            source,
        )

    def test_missing_required_configuration_fails_before_clone(self) -> None:
        proc = self.fake.run(
            SCRIPT,
            extra_env={"CRATEDIGGER_MIRROR_URL": ""},
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIsNone(self.fake.state["clone_path"])
        self.assertIn("CRATEDIGGER_MIRROR_URL", proc.stderr)
        self.assertEqual(
            proc.stdout.count("CRATEDIGGER_DAILY_RESOURCE_RECEIPT "), 1
        )
        self.assertIn("status=invalid reason=monitor_not_started", proc.stdout)

    def test_invalid_resource_namespace_fails_before_clone_without_zero_metrics(
        self,
    ) -> None:
        proc = self.fake.run(
            SCRIPT,
            extra_env={"XDG_RUNTIME_DIR": str(REPO_ROOT)},
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIsNone(self.fake.state["clone_path"])
        self.assertEqual(
            proc.stdout.count("CRATEDIGGER_DAILY_RESOURCE_RECEIPT "), 1
        )
        self.assertIn("status=invalid reason=scratch_not_tmpfs", proc.stdout)
        self.assertNotIn("scratch_byte_peak=0", proc.stdout)

    def test_invalid_receipt_surfaces_even_when_the_command_already_failed(
        self,
    ) -> None:
        """Regression pin for issue #1214 gap 4: an invalid resource receipt
        must surface on its own, not be silently absorbed into whatever
        exit code the run already had. finalize()'s union logic used to
        promote an invalid receipt into the process's own exit code only
        when the command had otherwise succeeded (command_status == 0) --
        when the command was already failing, nothing distinguished
        'ordinary red' from 'red AND we lost telemetry for it'.
        XDG_RUNTIME_DIR pointed outside a tmpfs fails the monitor before
        any candidate gate runs at all, so command_status is already
        non-zero (the top-level `exit 1`) by the time finalize() sees it
        -- exactly the branch that used to go unremarked.

        Mutant proof (both directions; run manually during review, not
        committed): reverting finalize()'s new unconditional
        `if ((resource_status != 0))` diagnostic back to only firing
        inside the `if ((command_status == 0 ...))` branch (the pre-#1214
        shape) makes this test's stderr assertion fail -- the invalid
        receipt still prints to stdout (unchanged), but nothing on stderr
        calls it out when the command was already failing."""
        proc = self.fake.run(
            SCRIPT,
            extra_env={"XDG_RUNTIME_DIR": str(REPO_ROOT)},
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("status=invalid reason=scratch_not_tmpfs", proc.stdout)
        self.assertIn("resource receipt invalid", proc.stderr)

    def test_process_group_term_emits_one_terminal_receipt_without_deadlock(
        self,
    ) -> None:
        self.fake.update_state(hold_stage="suite", hold_seconds=30)
        process = subprocess.Popen(
            ["bash", str(SCRIPT)],
            cwd=self.fake.root,
            env=self.fake_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        deadline = time.monotonic() + 5
        while "suite" not in self.fake.state["stage_started"]:
            self.assertIsNone(process.poll(), "daily runner exited before suite")
            self.assertLess(time.monotonic(), deadline, "daily runner never reached suite")
            time.sleep(0.02)

        os.killpg(process.pid, signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)

        self.assertEqual(process.returncode, 143, stderr)
        self.assertEqual(
            stdout.count("CRATEDIGGER_DAILY_RESOURCE_RECEIPT "), 1,
            stdout,
        )
        # A signal can race a boundary sample write; issue #1214 gap 2 means
        # that can now legitimately degrade rather than invalidate.
        self.assertRegex(stdout, r"status=(?:valid|degraded|invalid) ")

    def test_red_tip_canary_cannot_block_green_nixpkgs_candidate(self) -> None:
        # The fault must name the canary's CURRENT stage, or this test
        # passes for the wrong reason: a fault nothing can trigger proves
        # nothing about which runner ran what.
        self.fake.update_state(fault="tip-suite")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("stable-candidate", state["stages"])
        self.assertNotIn("tip-suite", state["stages"])
        self.assertEqual(state["commit_count"], 1)

    def test_shared_flock_serializes_nixpkgs_and_tip_processes(self) -> None:
        self.fake.update_state(hold_stage="suite", hold_seconds=0.4)
        with ThreadPoolExecutor(max_workers=2) as executor:
            daily = executor.submit(self.fake.run, SCRIPT)
            deadline = time.monotonic() + 3
            while "suite" not in self.fake.state["stage_started"]:
                self.assertLess(time.monotonic(), deadline, "daily runner never reached gate")
                time.sleep(0.02)
            tip = executor.submit(self.fake.run, TIP_SCRIPT)
            daily_proc = daily.result(timeout=10)
            tip_proc = tip.result(timeout=10)

        state = self.fake.state
        self.assertEqual(daily_proc.returncode, 0, daily_proc.stderr)
        self.assertEqual(tip_proc.returncode, 0, tip_proc.stderr)
        update_nixpkgs = state["events"].index(["nix", "flake", "update", "nixpkgs"])
        daily_push = next(
            index for index, event in enumerate(state["events"])
            if event[:2] == ["git", "push"]
        )
        update_tip = state["events"].index(
            ["nix", "flake", "update", "beets-tip", "mutagen-tip", "mediafile-tip"]
        )
        self.assertLess(update_nixpkgs, daily_push)
        self.assertLess(daily_push, update_tip)


if __name__ == "__main__":
    unittest.main()
