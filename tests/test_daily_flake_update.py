"""Contract tests for the unattended unstable lock-update runner."""

from __future__ import annotations

import fcntl
import os
import signal
import subprocess
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
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
        # A healthy, valid monitor on an ordinary gate failure gets no
        # invalid-receipt diagnostic -- issue #1214 gap 4 is about a
        # NON-CLEAN receipt surfacing, not every failing run growing new
        # output.
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

    def test_invalid_receipt_from_a_write_failure_surfaces_and_flips_an_otherwise_green_exit(
        self,
    ) -> None:
        """Regression pin for the gap-4 invariant applied to a REAL,
        mid-run write failure rather than a startup refusal: a failed
        sample write during an otherwise-green candidate run must still
        flip the gate's own exit code and print a stderr call-out, not
        just an invalid receipt nobody's exit code reflects. issue #1214's
        round-6 strip-back removed the quantified `status=degraded`
        status this test used to pin (a single failed write no longer
        gets a separate "partial" outcome -- it is invalid, the same as
        any other lost write, per the round-6 design). This test forces a
        REAL boundary sample write to fail (chmod 400, real EACCES)
        during an otherwise-green run and asserts status=invalid, without
        pinning the exact reason token: depending on timing, the
        real (unstubbed) periodic loop may ALSO hit the same chmod'd
        file and die first (`monitor_process_died`), or the in-flight
        set_phase boundary write may lose the race
        (`sample_write_failed`) -- both are legitimate, and this
        integration-level test cannot control that race the way the
        unit-level pins in tests/test_daily_resource_monitor.py do.

        Mutant proof (empirically run during review, not committed):
        reverting daily_resource_monitor_finish so a failure reason never
        forces an invalid summarize call (i.e. always passing an empty
        reason to daily_resource_summarize_samples) makes this test fail
        -- the receipt still shows the surviving phase breakdown, but
        prints status=valid and the process exits 0 with no stderr
        diagnostic at all."""
        # issue #1214 review C2: globbing shared /tmp for the monitor's
        # state directory is unsound -- another test's timed-out/SIGKILLed
        # monitor run leaks its mktemp'd directory there permanently (it is
        # removed only by daily_resource_monitor_finish, which a kill or a
        # timeout never reaches), so a stray leftover makes this assertion
        # fail on that host forever, not just flake. Reproduced both ways:
        # concurrently with this module's own other tests, and
        # deterministically with pre-planted leftover directories and zero
        # concurrency. Fix: give this run its OWN isolated TMPDIR, on a
        # filesystem distinct from $XDG_RUNTIME_DIR (real disk, not the
        # fake's ambient tmpfs-backed one -- in the ordinary dev shell that
        # ambient TMPDIR shares a filesystem with $XDG_RUNTIME_DIR, so F9's
        # /tmp fallback is this test's live path, not an edge case), so the
        # real monitor's own candidate-list logic (F9) resolves the state
        # root to exactly this directory -- never the shared fallback -- and
        # glob only inside it.
        with tempfile.TemporaryDirectory(
            dir="/tmp", prefix="cratedigger-isolated-tmpdir-"
        ) as isolated_tmpdir:
            env = self.fake_environment()
            env["TMPDIR"] = isolated_tmpdir
            process = subprocess.Popen(
                ["bash", str(SCRIPT)],
                cwd=self.fake.root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            self.addCleanup(lambda: process.poll() is None and process.kill())
            self.fake.update_state(hold_stage="suite", hold_seconds=0.4)
            deadline = time.monotonic() + 5
            while "suite" not in self.fake.state["stage_started"]:
                self.assertIsNone(process.poll(), "daily runner exited before suite")
                self.assertLess(time.monotonic(), deadline, "daily runner never reached suite")
                time.sleep(0.02)

            candidates = list(
                Path(isolated_tmpdir).glob("cratedigger-daily-resource.*/samples.tsv")
            )
            self.assertEqual(len(candidates), 1, candidates)
            candidates[0].chmod(0o400)

            # The subprocess -- and its use of isolated_tmpdir as the
            # monitor's own state root -- must finish before the `with`
            # block above tears that directory down.
            stdout, stderr = process.communicate(timeout=15)

        state = self.fake.state

        # Resource monitoring is purely observational and never gates the
        # candidate logic (the commit/push already happened, inside the
        # main script body, before finalize() ever runs) -- only the
        # PROCESS'S OWN exit code changes, exactly the original gap-4
        # promotion path (command_status == 0, resource_status != 0).
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["push_count"], 1)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("status=invalid", stdout)
        self.assertIn("resource receipt invalid", stderr)

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
        # A signal can race a boundary sample write, which can legitimately
        # make the receipt invalid rather than valid (issue #1214 gap 2 /
        # round-6 strip-back: no separate "degraded" status any more).
        self.assertRegex(stdout, r"status=(?:valid|invalid) ")

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


class TestDailyFlakeUpdateFakeShimCaching(unittest.TestCase):
    """Pins for the shared-module fake-command shape (issue #1156 item 5):
    git/nix/nix-shell remain symlinks to one tiny stub that imports a shared
    ``_shim.py``, so CPython caches its compiled bytecode across every fake
    command invocation instead of recompiling on each one."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDailyFlakeUpdateCommands(Path(self.tempdir.name))

    def test_command_stub_is_tiny_and_shares_one_cached_shim_module(self) -> None:
        shim_path = self.fake.fake_bin / "_shim.py"
        self.assertTrue(shim_path.exists())
        shim_size = shim_path.stat().st_size
        stub_path = self.fake.fake_bin / "command"
        stub_size = stub_path.stat().st_size
        # A regression back to writing the full body into the shared
        # "command" file (the pre-#1156-item-5 shape) would make the stub
        # as large as the shim itself.
        self.assertLess(stub_size, 300, "command stub is not tiny")
        self.assertLess(stub_size * 5, shim_size,
                         "command stub looks like a full shim copy")
        for name in ("git", "nix", "nix-shell"):
            self.assertTrue((self.fake.fake_bin / name).is_symlink())

        pycache = self.fake.fake_bin / "__pycache__"
        self.assertFalse(pycache.exists())

        # Default seed state is lock_changed=True (`_write_state` above), so
        # this world exits 1 -- an exit-0 world here would be indistinguishable
        # from a stub that never calls main() at all (P2-F1 review finding on
        # #1156 items 4/5). The `events` assertion below is the direct kill:
        # `main()` appends to `state["events"]` before any branch dispatch, so
        # a stub that imports `_shim` but never calls `main()` leaves it empty
        # regardless of exit code.
        proc = subprocess.run(
            [str(self.fake.fake_bin / "git"), "diff", "--quiet", "--", "flake.lock"],
            env=self.fake.environment(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 1, proc.stderr)
        self.assertIn(
            ["git", "diff", "--quiet", "--", "flake.lock"],
            self.fake.state["events"],
        )

        cached = list(pycache.glob("_shim.*.pyc"))
        self.assertEqual(
            len(cached), 1,
            "expected the shim's bytecode to be cached in __pycache__ "
            f"after one call, found {cached} -- check for an ambient "
            "PYTHONDONTWRITEBYTECODE or PYTHONPYCACHEPREFIX in your "
            "environment, either of which silently defeats this caching",
        )

    def test_command_stub_fails_loudly_without_the_shared_shim_module(self) -> None:
        (self.fake.fake_bin / "_shim.py").unlink()

        proc = subprocess.run(
            [str(self.fake.fake_bin / "git"), "diff", "--quiet", "--", "flake.lock"],
            env=self.fake.environment(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("ModuleNotFoundError", proc.stderr)
        self.assertIn("_shim", proc.stderr)


class TestFakeStatePublication(unittest.TestCase):
    """The fixture's state file, which three poll loops read unlocked.

    `test_process_group_term_emits_one_terminal_receipt_without_deadlock`,
    `test_invalid_receipt_from_a_write_failure_...` and
    `test_shared_flock_serializes_nixpkgs_and_tip_processes` all spin on
    `self.fake.state` while a fake command is writing the same file. Under a
    loaded parallel suite that read caught the file mid-truncation and raised
    `JSONDecodeError`; standalone it passed. These are that contract, made
    explicit.

    Test infrastructure, so deterministic only -- an exact mechanism pin plus
    one end-to-end contract, never a generated property
    (`.claude/rules/code-quality.md` § "Never property-test the test
    machinery").
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDailyFlakeUpdateCommands(Path(self.tempdir.name))

    def test_fixture_publishes_state_by_rename_never_by_truncation(self) -> None:
        """The mechanism itself: a fresh file replaces the old one.

        `Path.write_text` opens the live path with mode "w", which truncates
        before a single byte is written and keeps the inode; `os.replace`
        publishes a different inode and never makes the path unreadable. The
        inode changing IS the atomicity here, so it is what this asserts.
        """
        before = self.fake.state_path.stat().st_ino

        self.fake.update_state(hold_seconds=1.5)

        self.assertNotEqual(
            self.fake.state_path.stat().st_ino,
            before,
            "state.json kept its inode, so it was written in place -- a "
            "concurrent reader can observe it truncated",
        )
        self.assertEqual(self.fake.state["hold_seconds"], 1.5)

    def test_shim_publishes_state_by_rename_never_by_truncation(self) -> None:
        """The same mechanism on the other writer, driven as a real process.

        The shim is a separate interpreter running out of the fixture
        directory and carries its own copy of the publish helper, so proving
        the fixture side says nothing about it.
        """
        before = self.fake.state_path.stat().st_ino

        proc = subprocess.run(
            [str(self.fake.fake_bin / "git"), "diff", "--quiet", "--", "flake.lock"],
            env=self.fake.environment(),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 1, proc.stderr)
        self.assertIn(
            ["git", "diff", "--quiet", "--", "flake.lock"],
            self.fake.state["events"],
        )
        self.assertNotEqual(self.fake.state_path.stat().st_ino, before)

    def test_unlocked_reads_stay_valid_while_a_writer_runs(self) -> None:
        """End-to-end: the poll loops' own read shape, under real contention.

        A background thread republishes the state as fast as it can while
        this thread reads it exactly the way every poll loop does. On the
        pre-fix fixture this raised `JSONDecodeError` within milliseconds
        (measured: 9,086 empty reads in 15,769 attempts over two seconds).
        Bounded by the writer's iteration count, so it cannot hang, and it
        can only fail in the direction of a real defect.
        """
        writes = 300
        errors: list[str] = []

        def publish() -> None:
            for index in range(writes):
                self.fake.update_state(hold_seconds=float(index))

        with ThreadPoolExecutor(max_workers=1) as executor:
            writer = executor.submit(publish)
            reads = 0
            while not writer.done():
                try:
                    self.fake.state["stage_started"]
                except (OSError, ValueError, KeyError) as exc:
                    errors.append(f"{type(exc).__name__}: {exc}")
                    break
                reads += 1
            writer.result(timeout=30)

        self.assertEqual(errors, [], f"after {reads} reads")
        self.assertGreater(reads, 0, "the reader never got to run")
        self.assertEqual(self.fake.state["hold_seconds"], float(writes - 1))

    def test_update_state_takes_an_exclusive_lock(self) -> None:
        """`update_state` is a read-modify-write and must hold the lock
        EXCLUSIVELY.

        A shared lock is held here rather than an exclusive one, which is
        what makes the strength of `update_state`'s own lock observable: a
        held `LOCK_SH` blocks a `LOCK_EX` request and does not block another
        `LOCK_SH`, so degrading `update_state` to a shared lock stops this
        from blocking at all. With an exclusive lock on both sides the test
        cannot tell the two apart, and a `LOCK_SH` mutant survived it.

        The unlock is in a `finally`: without it, an assertion failing while
        the lock is held unwinds into `ThreadPoolExecutor.__exit__`, which
        waits for a worker blocked on the lock this dying thread still owns.
        Measured — the test hung indefinitely instead of reporting, so a
        real defect would have shown up as a stuck suite worker rather than
        a red test.
        """
        lock_path = self.fake.state_path.with_suffix(".lock")
        with lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            try:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    blocked = executor.submit(
                        self.fake.update_state, hold_stage="suite"
                    )
                    try:
                        with self.assertRaises(FuturesTimeoutError):
                            blocked.result(timeout=0.5)
                        self.assertIsNone(self.fake.state["hold_stage"])
                    finally:
                        fcntl.flock(lock, fcntl.LOCK_UN)
                    blocked.result(timeout=10)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

        self.assertEqual(self.fake.state["hold_stage"], "suite")

    def test_update_state_waits_behind_a_real_shim_holding_the_lock(
        self,
    ) -> None:
        """The composed contract: the REAL shim writer, not a hand-taken lock.

        The test above proves `update_state` respects a lock; it says nothing
        about whether the shim still TAKES one, because the lock it contends
        with is this test's own. Deleting the shim's `flock` left every other
        test in this module green. So this drives a real fake command into
        its hold, then updates the state while it sleeps.

        The assertion is the lost update itself, not a duration, so nothing
        here depends on how long anything takes. Unlocked, the shim read the
        state before this update and republishes its own copy afterwards,
        dropping `probe_marker` entirely. Locked, `update_state` runs after
        the shim's final publish and the marker survives. A poll that slips
        past the hold makes the test pass without proving anything, never
        fail — the failure direction is always a real defect.
        """
        self.fake.update_state(hold_stage="suite", hold_seconds=2.0)
        shim = subprocess.Popen(
            [
                str(self.fake.fake_bin / "nix-shell"),
                "--run",
                "bash scripts/run_tests.sh",
            ],
            cwd=self.fake.root,
            env=self.fake.environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.addCleanup(lambda: shim.poll() is None and shim.kill())

        deadline = time.monotonic() + 15
        while "suite" not in self.fake.state["stage_started"]:
            self.assertIsNone(shim.poll(), "the fake shim exited before its hold")
            self.assertLess(time.monotonic(), deadline, "shim never reached suite")
            time.sleep(0.01)

        self.fake.update_state(probe_marker="kept")
        _stdout, stderr = shim.communicate(timeout=30)
        self.assertEqual(shim.returncode, 0, stderr)

        state = self.fake.state
        self.assertIn("suite", state["stages"])
        self.assertEqual(
            state.get("probe_marker"),
            "kept",
            "the shim republished its stale copy over this update",
        )


if __name__ == "__main__":
    unittest.main()
