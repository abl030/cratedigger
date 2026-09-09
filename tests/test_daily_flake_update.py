"""Contract tests for the unattended unstable lock-update runner."""

from __future__ import annotations

import fcntl
import os
import signal
import subprocess
import tempfile
import time
import unittest
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from pathlib import Path
from unittest.mock import patch

from tests._source_pins import pinned_source
from tests.fakes.daily_flake_update import FakeDailyFlakeUpdateCommands
from tests.fakes.subprocess_env import BYTECODE_CACHE_OPT_OUT_VARS

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "daily_flake_update.sh"
TIP_SCRIPT = REPO_ROOT / "scripts" / "daily_beets_tip_update.sh"

# Issue #1392: every wait below is a liveness gate, never a performance
# assertion -- the tests need "the runner reached its first gate stage", and
# nothing about them depends on how long that took. The old per-site caps (3,
# 5 and 15 seconds) were performance assertions by accident, and on one of
# doc1's crowded review nights (load average 25 to 40) the 3-second one failed
# a run that passed alone immediately afterwards, with the same code. Measured
# on doc1 with this fake runner: the preamble reaches the first gate stage in
# 0.16s median (max 0.18s) on a near-idle host, and 0.67s median (max 1.84s)
# under a deliberate load average of 138 to 164, so a cap at all is only there
# to stop a genuinely wedged runner from hanging a suite worker forever. A
# runner that DIES instead is caught by the exit probe below, immediately and
# with its own output, rather than by burning the whole cap and then saying
# only "never reached".
STAGE_START_TIMEOUT_SECONDS = 120

# The same reasoning for the other half of those tests: waiting for the thing
# they started to finish, whether that is a runner subprocess or a worker
# thread. Those bounds are hang guards too, and the tightest of them (10s, in
# the flock test the issue names) sat 1.4x above the slowest run measured
# here -- 7.1s for a complete fake candidate gate under a load average of 161
# to 170, against 0.55s median on a near-idle host. A hang still fails, just
# later; a merely slow host no longer does. The deliberate SHORT bound in
# `test_update_state_takes_an_exclusive_lock` is not one of these: it asserts
# a call has NOT finished, so load pushes it away from a false red.
RUNNER_EXIT_TIMEOUT_SECONDS = 120

# Draining a dead runner's pipes is bounded too, and for a sharper reason:
# scripts/daily_flake_update.sh forks the resource monitor's periodic loop
# with the parent's stdout and stderr inherited, and that loop exits only when
# daily_resource_monitor_finish writes its stop file from the parent's EXIT
# trap. Kill the parent without running that trap -- SIGKILL, doc1's OOM
# killer -- and the orphan holds the pipe open, so an unbounded
# `communicate()` never returns, inside the very wait whose job is to be
# bounded (measured: still blocked after 10s with the parent already reaped).
# Exceeding this loses the runner's output, never the diagnosis, so it cannot
# fail a test in either direction.
PROBE_DRAIN_TIMEOUT_SECONDS = 5


def process_exit_diagnosis(
    process: subprocess.Popen[str],
) -> Callable[[], str | None]:
    """Exit probe for a runner launched as a raw ``Popen``."""

    def probe() -> str | None:
        if process.poll() is None:
            return None
        try:
            stdout, stderr = process.communicate(
                timeout=PROBE_DRAIN_TIMEOUT_SECONDS
            )
        except subprocess.TimeoutExpired:
            return (
                f"exit {process.returncode}; its output is still held by a "
                "surviving child, so there is none to show"
            )
        return f"exit {process.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"

    return probe


def future_exit_diagnosis(
    runner: Future[subprocess.CompletedProcess[str]],
) -> Callable[[], str | None]:
    """Exit probe for a runner submitted to an executor.

    ``result()`` re-raises whatever the submitted callable raised, which
    surfaces as a test error rather than a failure. `FakeDailyFlakeUpdateCommands.run`
    passes ``check=False``, so today that cannot happen; if it ever does, the
    real exception is the diagnosis.
    """

    def probe() -> str | None:
        if not runner.done():
            return None
        proc = runner.result()
        return (
            f"exit {proc.returncode}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )

    return probe


def await_stage_start(
    test: unittest.TestCase,
    fake: FakeDailyFlakeUpdateCommands,
    stage: str,
    *,
    exited: Callable[[], str | None],
    timeout: float = STAGE_START_TIMEOUT_SECONDS,
) -> None:
    """Block until the fake records ``stage`` in ``stage_started``.

    ``exited`` reports the runner's own output once it is gone, so an early
    exit fails here with the reason it printed instead of a bare "never
    reached" after the whole cap has elapsed.
    """
    deadline = time.monotonic() + timeout
    while stage not in fake.state["stage_started"]:
        diagnosis = exited()
        # Re-read before accusing: the shim publishes ``stage_started`` and
        # only then exits, so a state read taken microseconds before that
        # publish, paired with an exit observed after it, is an ordinary
        # race and not an early exit at all.
        if diagnosis is not None and stage not in fake.state["stage_started"]:
            test.fail(f"the runner exited before reaching {stage}: {diagnosis}")
        test.assertLess(
            time.monotonic(),
            deadline,
            f"the runner is still alive but never reached {stage} within "
            f"{timeout}s",
        )
        time.sleep(0.02)


def _kill_group(group: int) -> None:
    """Reap a whole process group, orphans included, ignoring one already gone."""
    try:
        os.killpg(group, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _close_pipes(process: subprocess.Popen[str]) -> None:
    """Close a Popen's pipes that a timed-out `communicate()` left open."""
    for pipe in (process.stdout, process.stderr):
        if pipe is not None:
            pipe.close()


def gate_run_diagnosis(proc: subprocess.CompletedProcess[str]) -> str:
    """The runner's stderr plus its resource receipt, for an assertion message.

    Issue #1392: ``assertEqual(proc.returncode, 0, proc.stderr)`` prints
    ``daily unstable gate: resource receipt invalid (command exit 0)`` and
    stops there. Which monitor step failed is on stdout, in the receipt's own
    ``reason=`` token, so the two crowded-night failures of #1391 that ended
    this way said the receipt was invalid and never said which step lost its
    write. The receipt travels with the stderr from here on.
    """
    receipts = [
        line
        for line in proc.stdout.splitlines()
        if line.startswith("CRATEDIGGER_DAILY_RESOURCE_RECEIPT ")
    ]
    return "\n".join([proc.stderr.rstrip(), *receipts])


class FakeRunnerCase(unittest.TestCase):
    """One fixture directory per test, shared by every class in this module."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDailyFlakeUpdateCommands(Path(self.tempdir.name))


class TestDailyFlakeUpdateScript(FakeRunnerCase):
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

        self.assertEqual(proc.returncode, 0, gate_run_diagnosis(proc))
        self.assertIn(["nix", "flake", "update", "nixpkgs"], state["events"])
        self.assertEqual(
            state["stages"],
            ["suite", "shuffled-suite", "stable-candidate", "world", "fuzz", "mirror"],
        )
        # Issue #1322: the shuffled stage is the ONLY one carrying a seed, and
        # the seed it carries is the one the runner announced, so a red night
        # is replayable from the log alone.
        self.assertIsNone(state["stage_env"]["suite"]["CRATEDIGGER_SHUFFLE_SEED"])
        shuffle_seed = state["stage_env"]["shuffled-suite"]["CRATEDIGGER_SHUFFLE_SEED"]
        self.assertTrue(shuffle_seed and shuffle_seed.isdigit(), shuffle_seed)
        self.assertIn(
            f"daily unstable gate: shuffled-order suite seed {shuffle_seed}",
            proc.stdout,
        )
        self.assertIn(
            f"PASS shuffled-order deterministic suite (seed {shuffle_seed})",
            proc.stdout,
        )
        for stage in ("suite", "shuffled-suite"):
            self.assertEqual(
                state["stage_env"][stage]["CRATEDIGGER_SUITE_OWNS_HEADROOM"],
                "1",
                stage,
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
            gate_run_diagnosis(proc),
        )
        self.assertNotIn(
            "resource receipt invalid", proc.stderr, gate_run_diagnosis(proc)
        )
        for phase in (
            "deterministic_suite",
            "shuffled_suite",
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

        self.assertEqual(proc.returncode, 0, gate_run_diagnosis(proc))
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
            ["suite", "shuffled-suite", "stable-candidate", "world", "fuzz", "mirror"],
        )
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        self.assertIn("FAIL world-model burst", proc.stdout)
        self.assertIn("PASS mirror-harness smoke", proc.stdout)

    def test_inherited_shuffle_seed_never_reaches_the_fixed_order_stage(
        self,
    ) -> None:
        """Issue #1322: the runner honours CRATEDIGGER_SHUFFLE_SEED wherever it
        finds it, and the nightly shuffled stage exports one to every child,
        this suite included. Found by the first shuffled rehearsal: without
        the script's own scrub, an inherited seed turned the fixed-order
        stage into a second shuffled one. The gate scrubs it and mints its
        own for the shuffled stage only."""
        proc = self.fake.run(
            SCRIPT, extra_env={"CRATEDIGGER_SHUFFLE_SEED": "999"}
        )
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, gate_run_diagnosis(proc))
        self.assertEqual(state["stages"][:2], ["suite", "shuffled-suite"])
        self.assertIsNone(state["stage_env"]["suite"]["CRATEDIGGER_SHUFFLE_SEED"])
        minted = state["stage_env"]["shuffled-suite"]["CRATEDIGGER_SHUFFLE_SEED"]
        self.assertTrue(minted and minted.isdigit(), minted)
        self.assertNotEqual(minted, "999", "the gate mints its own seed")

    def test_shuffled_suite_failure_reads_beside_a_green_fixed_order_suite(
        self,
    ) -> None:
        """Issue #1322 triage rule: a red shuffled stage next to a green
        fixed-order stage is a test-isolation defect. The summary must put
        both verdicts, and the seed, in front of the reader."""
        self.fake.update_state(fault="shuffled-suite")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["commit_count"], 0)
        self.assertEqual(state["push_count"], 0)
        shuffle_seed = state["stage_env"]["shuffled-suite"]["CRATEDIGGER_SHUFFLE_SEED"]
        self.assertIn("PASS deterministic full suite", proc.stdout)
        self.assertIn(
            f"FAIL shuffled-order deterministic suite (seed {shuffle_seed})",
            proc.stdout,
        )
        self.assertIn("PASS mirror-harness smoke", proc.stdout)
        self.assertIn("candidate failed; flake.lock was not committed", proc.stderr)
        self.assertEqual(
            proc.stdout.count("CRATEDIGGER_DAILY_RESOURCE_RECEIPT "), 1
        )
        self.assertIn(
            "CRATEDIGGER_DAILY_RESOURCE_RECEIPT schema=1 status=valid",
            proc.stdout,
            gate_run_diagnosis(proc),
        )
        # A healthy, valid monitor on an ordinary gate failure gets no
        # invalid-receipt diagnostic -- issue #1214 gap 4 is about a
        # NON-CLEAN receipt surfacing, not every failing run growing new
        # output.
        self.assertNotIn(
            "resource receipt invalid", proc.stderr, gate_run_diagnosis(proc)
        )

    def test_unchanged_lock_still_runs_gates_without_commit(self) -> None:
        self.fake.update_state(lock_changed=False)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, gate_run_diagnosis(proc))
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

    def test_state_paths_and_unattended_budgets_are_explicit(self) -> None:
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, gate_run_diagnosis(proc))
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

    def test_shuffled_suite_stage_sets_headroom_and_a_fresh_seed(self) -> None:
        """Issue #1322: the shuffled stage is the same unattended launcher as
        the deterministic one (so it owns headroom the same way) plus the
        seed the runner reads. Pinned as an exact block, like the stage
        above, so dropping either variable fails this test by name."""
        source = pinned_source(SCRIPT)

        self.assertIn(
            'run_stage shuffled_suite "shuffled-order deterministic suite'
            ' (seed ${shuffle_seed})" \\\n'
            "    env CRATEDIGGER_SUITE_OWNS_HEADROOM=1 \\\n"
            '        CRATEDIGGER_SHUFFLE_SEED="${shuffle_seed}" \\\n'
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
            await_stage_start(
                self, self.fake, "suite", exited=process_exit_diagnosis(process)
            )

            candidates = list(
                Path(isolated_tmpdir).glob("cratedigger-daily-resource.*/samples.tsv")
            )
            self.assertEqual(len(candidates), 1, candidates)
            candidates[0].chmod(0o400)

            # The subprocess -- and its use of isolated_tmpdir as the
            # monitor's own state root -- must finish before the `with`
            # block above tears that directory down.
            stdout, stderr = process.communicate(
                timeout=RUNNER_EXIT_TIMEOUT_SECONDS
            )

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
        await_stage_start(
            self, self.fake, "suite", exited=process_exit_diagnosis(process)
        )

        os.killpg(process.pid, signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)

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

        self.assertEqual(proc.returncode, 0, gate_run_diagnosis(proc))
        self.assertIn("stable-candidate", state["stages"])
        self.assertNotIn("tip-suite", state["stages"])
        self.assertEqual(state["commit_count"], 1)

    def test_shared_flock_serializes_nixpkgs_and_tip_processes(self) -> None:
        self.fake.update_state(hold_stage="suite", hold_seconds=0.4)
        with ThreadPoolExecutor(max_workers=2) as executor:
            daily = executor.submit(self.fake.run, SCRIPT)
            await_stage_start(
                self, self.fake, "suite", exited=future_exit_diagnosis(daily)
            )
            tip = executor.submit(self.fake.run, TIP_SCRIPT)
            daily_proc = daily.result(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)
            tip_proc = tip.result(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)

        state = self.fake.state
        self.assertEqual(daily_proc.returncode, 0, gate_run_diagnosis(daily_proc))
        self.assertEqual(tip_proc.returncode, 0, gate_run_diagnosis(tip_proc))
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


class TestDailyFlakeUpdateFakeShimCaching(FakeRunnerCase):
    """Pins for the shared-module fake-command shape (issue #1156 item 5):
    git/nix/nix-shell remain symlinks to one tiny stub that imports a shared
    ``_shim.py``, so CPython caches its compiled bytecode across every fake
    command invocation instead of recompiling on each one."""

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
        # Run with the opt-outs SET, not merely with whatever the reviewer
        # happened to export. A mutant runner must set
        # PYTHONDONTWRITEBYTECODE=1, so this used to be a standing collision
        # between two house rules; the fixture now drops both variables and
        # this is where that is proved (issue #1313 residual 1329-2).
        with patch.dict(
            os.environ,
            {name: "1" for name in BYTECODE_CACHE_OPT_OUT_VARS},
        ):
            proc = subprocess.run(
                [
                    str(self.fake.fake_bin / "git"),
                    "diff", "--quiet", "--", "flake.lock",
                ],
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
            f"after one call, found {cached} -- the fixture's own "
            "environment() is what must drop PYTHONDONTWRITEBYTECODE and "
            "PYTHONPYCACHEPREFIX, both of which silently defeat this caching",
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


class TestFakeStatePublication(FakeRunnerCase):
    """The fixture's state file, which `await_stage_start` reads unlocked.

    Every subprocess test that waits for a stage marker spins on
    `fake.state` while a fake command is writing the same file. Under a
    loaded parallel suite that read caught the file mid-truncation and raised
    `JSONDecodeError`; standalone it passed. These are that contract, made
    explicit.

    Test infrastructure, so deterministic only -- an exact mechanism pin plus
    one end-to-end contract, never a generated property
    (`.claude/rules/code-quality.md` § "Never property-test the test
    machinery").
    """

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
            writer.result(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)

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
                    blocked.result(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)
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

        await_stage_start(
            self, self.fake, "suite", exited=process_exit_diagnosis(shim)
        )

        self.fake.update_state(probe_marker="kept")
        _stdout, stderr = shim.communicate(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)
        self.assertEqual(shim.returncode, 0, stderr)

        state = self.fake.state
        self.assertIn("suite", state["stages"])
        self.assertEqual(
            state.get("probe_marker"),
            "kept",
            "the shim republished its stale copy over this update",
        )


class TestStageStartWait(FakeRunnerCase):
    """Issue #1392: the wait every subprocess test above starts with.

    Test infrastructure, so deterministic only -- exact contracts, never a
    generated property (`.claude/rules/code-quality.md` § "Never
    property-test the test machinery").
    """

    def test_a_marker_landing_past_the_old_window_is_still_awaited(self) -> None:
        """The load flake itself: a slow preamble is not a failure.

        The marker lands 3.2s in, past the 3-second cap the flock test used
        to impose, and the wait must still be waiting. Nothing here can land
        the marker EARLY, so a loaded host only makes the delay longer --
        this test fails in one direction only, and that direction is a cap
        too small to survive doc1's crowded nights.
        """
        def land_marker() -> None:
            time.sleep(3.2)
            self.fake.update_state(stage_started=["suite"])

        with ThreadPoolExecutor(max_workers=1) as executor:
            lander = executor.submit(land_marker)
            await_stage_start(self, self.fake, "suite", exited=lambda: None)
            # Before the join, not after: joining the lander lands the marker
            # by itself, so an assertion taken afterwards passes even for a
            # wait that returned instantly. Found by the review's mutant
            # runner, inverting this helper's own loop condition.
            self.assertIn(
                "suite",
                self.fake.state["stage_started"],
                "the wait returned before the marker landed",
            )
            lander.result(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)

    def test_a_runner_that_exits_early_fails_with_its_own_output(self) -> None:
        """A dead runner is diagnosed, not waited out.

        `fault="update"` makes the fake `nix flake update` fail, so the real
        script exits before any stage. The old loops burned their whole cap
        and then said "never reached" -- this names the runner's own stderr,
        immediately.
        """
        self.fake.update_state(fault="update")
        with ThreadPoolExecutor(max_workers=1) as executor:
            runner = executor.submit(self.fake.run, SCRIPT)
            with self.assertRaises(AssertionError) as caught:
                await_stage_start(
                    self, self.fake, "suite", exited=future_exit_diagnosis(runner)
                )
            runner.result(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)

        # Both strings together say the wait exited through the exit probe:
        # the cap's own message is a different sentence, and only the probe
        # branch carries the runner's stderr. Timing them would be the
        # performance assertion this whole change removes.
        self.assertIn("exited before reaching suite", str(caught.exception))
        self.assertIn("flake update failed", str(caught.exception))

    def test_a_dead_runners_surviving_child_cannot_block_the_exit_probe(
        self,
    ) -> None:
        """An orphan holding the runner's pipe must not hang the wait.

        `scripts/daily_flake_update.sh` forks the resource monitor's periodic
        loop with its own stdout and stderr, and that loop exits only on the
        stop file its parent's EXIT trap writes. A parent killed without
        running that trap leaves the orphan holding the pipe, and an
        unbounded `communicate()` then never returns -- measured against the
        real script during this change's review, still blocked after 10s with
        the parent already reaped. The world here is that shape at its
        smallest: a shell that outlives itself through one child, with no
        monitor involved, so nothing about the timing can drift.
        """
        process = subprocess.Popen(
            ["bash", "-c", "sleep 300 & exit 7"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        group = process.pid
        # LIFO: kill the group first, then close the pipes a timed-out
        # `communicate()` leaves open.
        self.addCleanup(_close_pipes, process)
        self.addCleanup(_kill_group, group)
        process.wait(timeout=RUNNER_EXIT_TIMEOUT_SECONDS)

        diagnosis = process_exit_diagnosis(process)()

        self.assertEqual(
            diagnosis,
            "exit 7; its output is still held by a surviving child, so there "
            "is none to show",
        )

    def test_an_exit_seen_after_the_marker_landed_is_not_called_early(self) -> None:
        """The publish-then-exit race, driven deliberately.

        The shim publishes ``stage_started`` and only then exits, so a state
        read taken just before that publish can pair with an exit observed
        just after it. This probe reproduces exactly that interleaving: it
        lands the marker and reports the exit in the same call.
        """
        def exited_after_landing_the_marker() -> str | None:
            self.fake.update_state(stage_started=["suite"])
            return "exit 0"

        await_stage_start(
            self, self.fake, "suite", exited=exited_after_landing_the_marker
        )

        self.assertIn("suite", self.fake.state["stage_started"])

    def test_a_marker_that_never_lands_gives_up_at_the_cap(self) -> None:
        """The cap is smaller here than any real one, and that is the point:
        a wait with no ceiling at all hangs a suite worker forever. The probe
        reports a live runner throughout, so the cap is the only way out."""
        started = time.monotonic()
        with self.assertRaises(AssertionError) as caught:
            await_stage_start(
                self, self.fake, "suite", exited=lambda: None, timeout=0.2
            )
        elapsed = time.monotonic() - started

        self.assertIn("never reached suite", str(caught.exception))
        self.assertIn("still alive", str(caught.exception))
        # It must give up BECAUSE the cap elapsed. Inverting the comparison
        # inside the wait produces the same message instantly, and the
        # review's mutant runner proved the message assertions alone cannot
        # tell the two apart. A slow host only pushes this further past 0.2s,
        # so it fails in one direction only.
        self.assertGreaterEqual(elapsed, 0.2)


class TestGateRunDiagnosis(FakeRunnerCase):
    """Issue #1392: what a failing green-run assertion actually prints."""

    def test_an_invalid_receipt_names_the_monitor_step_that_failed(self) -> None:
        """The reason token comes from the real monitor, not a literal.

        Pointing XDG_RUNTIME_DIR outside a tmpfs is a refusal the monitor
        itself spells; the diagnosis has to carry it, because "resource
        receipt invalid" on its own is all two of #1391's crowded-night
        failures printed, and it does not say which step lost its write.
        """
        proc = self.fake.run(SCRIPT, extra_env={"XDG_RUNTIME_DIR": str(REPO_ROOT)})

        self.assertNotEqual(proc.returncode, 0)
        diagnosis = gate_run_diagnosis(proc)
        self.assertIn("resource receipt invalid", diagnosis)
        self.assertIn("status=invalid reason=scratch_not_tmpfs", diagnosis)

    def test_a_clean_receipt_still_reports_the_gate_failure(self) -> None:
        """Must still work: a red gate under a healthy monitor reads the same
        as it always did, with the valid receipt appended."""
        self.fake.update_state(fault="world")

        proc = self.fake.run(SCRIPT)

        diagnosis = gate_run_diagnosis(proc)
        self.assertIn("candidate failed; flake.lock was not committed", diagnosis)
        self.assertIn("status=valid", diagnosis)
        # One receipt line, not the monitor's whole telemetry dump: a run
        # emits seven CRATEDIGGER_DAILY_RESOURCE_PHASE lines of a couple of
        # hundred characters each, and an assertion message carrying all of
        # them buries the one fact it exists to show. Widening the prefix to
        # CRATEDIGGER_DAILY_RESOURCE_ survived every substring assertion above
        # when the review's mutant runner tried it.
        self.assertNotIn("CRATEDIGGER_DAILY_RESOURCE_PHASE", diagnosis)
        carried = [
            line
            for line in diagnosis.splitlines()
            if line.startswith("CRATEDIGGER_DAILY_RESOURCE_")
        ]
        self.assertEqual(len(carried), 1, diagnosis)


if __name__ == "__main__":
    unittest.main()
