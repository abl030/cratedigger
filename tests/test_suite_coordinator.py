"""Process and schema contracts for the exhaustive full-suite coordinator."""

from __future__ import annotations

import fcntl
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import msgspec

from scripts.run_python_tests import TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
from scripts.run_targeted_tests import targeted_phases
from scripts.run_test_suite import (
    _HEADROOM_BASE_BYTES,
    _HEADROOM_PER_WORKER_BYTES,
    DEFAULT_MIN_HEADROOM_BYTES,
    DEFAULT_RECEIPT_RETIREMENT_MAX_AGE_SECONDS,
    FAILURE_MARKER_PREFIX,
    SCRATCH_TREE_OWNER_MARKER_NAME,
    SCRATCH_TREE_PREFIX,
    TEST_RAM_ROOT_EXHAUSTED,
    CheckFailureMarker,
    CheckSummary,
    PhaseSpec,
    RamRootExhaustedError,
    SuiteAdmissionTimeout,
    _active_processes_lock,
    _check_suite_headroom,
    _default_min_headroom_bytes,
    _default_phases,
    _proc_start_ticks,
    _proc_stat_start_ticks,
    _read_lock_holder_identity,
    _scratch_tree_owner_dead,
    acquire_suite_admission,
    admission_lock_path,
    dirty_state_fingerprint,
    headroom_floor_bytes,
    reap_stale_check_bundles,
    reap_stale_final_gate_receipts,
    run_suite,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
JS_HELPER = REPO_ROOT / "scripts" / "run_js_checks.sh"


def decode_summary(path: Path) -> CheckSummary:
    return msgspec.json.decode(path.read_bytes(), type=CheckSummary)


def _python_command(output: str, exit_code: int) -> tuple[str, ...]:
    return (
        sys.executable,
        "-c",
        f"import sys; print({output!r}); raise SystemExit({exit_code})",
    )


class SuiteCoordinatorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        shared = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertTrue(
            shared.is_dir(),
            "private runtime tmpfs is required for this test",
        )
        # A fresh subdirectory per test, not the shared XDG_RUNTIME_DIR
        # itself: run_suite() now takes an exclusive admission lock scoped to
        # its runtime_dir (issue #1111), and this module's own tests run
        # AS PART OF the real canonical suite's own "python" phase, which
        # already holds that lock on the shared root for the whole run.
        # Reusing the shared root here would deadlock every nested run_suite()
        # call in this file against its own enclosing suite invocation.
        self.runtime = Path(
            tempfile.mkdtemp(dir=shared, prefix="cratedigger-suite-test-")
        )
        self.bundles: list[Path] = []

    def tearDown(self) -> None:
        for bundle in self.bundles:
            shutil.rmtree(bundle, ignore_errors=True)
        shutil.rmtree(self.runtime, ignore_errors=True)

    def _run(
        self,
        phases: tuple[PhaseSpec, ...],
        *,
        command: str = "bash scripts/run_tests.sh",
        min_headroom_bytes: int | None = 0,
        admission_timeout_seconds: float = 5.0,
    ):
        stream = io.StringIO()
        result = run_suite(
            repo_root=REPO_ROOT,
            phases=phases,
            runtime_dir=self.runtime,
            stream=stream,
            command=command,
            min_headroom_bytes=min_headroom_bytes,
            admission_timeout_seconds=admission_timeout_seconds,
        )
        self.bundles.append(result.bundle)
        return result, stream.getvalue()

    def test_default_suite_owns_one_concurrent_complementary_pyright_phase(
        self,
    ) -> None:
        pyright_phases = tuple(
            phase for phase in _default_phases() if phase.parser == "pyright"
        )

        self.assertEqual(tuple(phase.name for phase in pyright_phases), ("pyright",))
        self.assertEqual(
            pyright_phases[0].command,
            ("python3", "scripts/run_pyright_checks.py"),
        )
        self.assertEqual(
            pyright_phases[0].rerun_command,
            "python3 scripts/run_pyright_checks.py",
        )

    def test_default_phases_end_with_python_so_the_overlap_stays_enabled(
        self,
    ) -> None:
        """Issue #1131 review N4: _execute_suite's concurrent-overlap split
        (scripts/run_test_suite.py) keys on the LAST phase being named
        literally "python". A silent rename in either producer would fall
        back to the still-correct-but-no-longer-concurrent fully-serial
        path with nothing failing — it would just show up as "the suite
        got slower" days later. Name the overlap directly instead."""
        default_phases = _default_phases()
        self.assertGreater(len(default_phases), 1)
        self.assertEqual(default_phases[-1].name, "python")

        selected_phases = targeted_phases(("tests.test_typing_ratchet",))
        self.assertGreater(len(selected_phases), 1)
        self.assertEqual(selected_phases[-1].name, "python")

    def test_summary_records_the_actual_invoked_suite_command(self) -> None:
        command = "python3 scripts/run_targeted_tests.py tests.test_alpha"

        result, _output = self._run(
            (PhaseSpec("alpha", _python_command("ok", 0), "alpha", "generic"),),
            command=command,
        )

        self.assertEqual(result.summary.command, command)

    def test_one_invocation_indexes_simultaneous_failures_from_every_phase(
        self,
    ) -> None:
        python_marker = msgspec.json.encode(
            CheckFailureMarker(
                identity="tests.test_alpha.TestAlpha.test_bad",
                owner="tests/test_alpha.py",
                detail="assertion failed",
                test_ids=("tests.test_alpha.TestAlpha.test_bad",),
            )
        ).decode()
        phases = (
            PhaseSpec(
                "js-syntax",
                _python_command(
                    "CRATEDIGGER_JS_FAILURE\tweb/js/bad.js\tSyntaxError", 1
                ),
                "bash scripts/run_js_checks.sh syntax",
                "js-syntax",
            ),
            PhaseSpec(
                "js-unit",
                _python_command(
                    "CRATEDIGGER_JS_FAILURE\ttests/test_js_bad.mjs\t2 assertions",
                    1,
                ),
                "bash scripts/run_js_checks.sh unit",
                "js-unit",
            ),
            PhaseSpec(
                "pyright",
                _python_command(
                    "lib/typed.py:7:4 - error: Argument is unknown "
                    "(reportUnknownArgumentType)",
                    1,
                ),
                "python3 scripts/run_pyright_checks.py",
                "pyright",
            ),
            PhaseSpec(
                "ruff",
                _python_command(
                    "F821 Undefined name `missing`\n"
                    " --> lib/lint.py:9:2",
                    1,
                ),
                "bash scripts/run_ruff.sh",
                "ruff",
            ),
            PhaseSpec(
                "vulture",
                _python_command(
                    "lib/dead.py:12: unused function 'orphan' (60% confidence)",
                    3,
                ),
                "bash scripts/find_dead_code.sh",
                "vulture",
                (3,),
            ),
            PhaseSpec(
                "python",
                _python_command(f"{FAILURE_MARKER_PREFIX}{python_marker}", 1),
                "python3 scripts/run_python_tests.py",
                "python",
            ),
        )

        result, terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 1)
        self.assertEqual(summary.state, "failed")
        self.assertEqual(
            tuple(phase.name for phase in summary.phases),
            tuple(phase.name for phase in phases),
        )
        self.assertTrue(all(phase.state == "failed" for phase in summary.phases))
        self.assertEqual(
            [failure.identity for phase in summary.phases for failure in phase.failures],
            [
                "web/js/bad.js",
                "tests/test_js_bad.mjs",
                "lib/typed.py:7:4",
                "lib/lint.py:9:2",
                "lib/dead.py:12",
                "tests.test_alpha.TestAlpha.test_bad",
            ],
        )
        self.assertIn("FAILED: 6 phases, 6 failures", terminal)
        self.assertIn(f"bundle: {result.bundle}", terminal)
        for phase in summary.phases:
            log = result.bundle / phase.log
            self.assertTrue(log.is_file(), phase.name)
            self.assertTrue(log.read_text(encoding="utf-8"), phase.name)
            for failure in phase.failures:
                self.assertEqual(failure.log, phase.log)
                self.assertTrue(failure.rerun_command)
                self.assertIn(failure.rerun_command, terminal)
        self.assertEqual(
            summary.phases[-1].failures[0].test_ids,
            ("tests.test_alpha.TestAlpha.test_bad",),
        )
        self.assertEqual((result.bundle.stat().st_mode & 0o777), 0o700)
        self.assertEqual(
            (result.bundle / "summary.json").stat().st_mode & 0o777,
            0o600,
        )
        self.assertTrue((result.bundle / "summary.md").is_file())

    def test_command_start_failure_is_indexed_and_does_not_stop_later_phase(
        self,
    ) -> None:
        phases = (
            PhaseSpec(
                "missing",
                ("/definitely/missing/cratedigger-check",),
                "missing-check",
                "generic",
            ),
            PhaseSpec(
                "later",
                _python_command("later phase ran", 0),
                "later-check",
                "generic",
            ),
        )

        result, terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 2)
        self.assertEqual(summary.state, "infrastructure-failure")
        self.assertEqual(
            tuple(phase.state for phase in summary.phases),
            ("infrastructure-failure", "passed"),
        )
        self.assertIn("later phase ran", (result.bundle / "later.log").read_text())
        self.assertIn("INFRASTRUCTURE FAILURE", terminal)

    def test_sigterm_publishes_interrupted_partial_state_and_stops_new_work(
        self,
    ) -> None:
        phases = (
            PhaseSpec(
                "interrupting",
                (
                    sys.executable,
                    "-c",
                    (
                        "import os, signal, time; "
                        "os.kill(os.getppid(), signal.SIGTERM); time.sleep(30)"
                    ),
                ),
                "interrupting-check",
                "generic",
            ),
            PhaseSpec(
                "must-not-run",
                _python_command("wrong", 0),
                "must-not-run",
                "generic",
            ),
        )

        result, terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 143)
        self.assertEqual(summary.state, "interrupted")
        self.assertEqual(
            tuple(phase.state for phase in summary.phases),
            ("interrupted", "not-run"),
        )
        self.assertIn("INTERRUPTED: signal 15", terminal)
        self.assertFalse((result.bundle / "must-not-run.log").exists())

    def test_leading_phases_run_concurrently_with_a_trailing_python_phase(
        self,
    ) -> None:
        """Issue #1131: every phase before a phase literally named "python"
        (the shape both _default_phases() and targeted_phases() always
        produce) must run CONCURRENTLY with it, not serially.

        Proven with a bounded rendezvous rather than a timing measurement:
        the leading "js-syntax" phase polls (up to 5s) for a marker file
        only the "python" phase creates. Under strict serial scheduling
        "python" would never even start until "js-syntax" finished, so the
        marker could never appear in time and the leading phase would fail
        closed after the full 5s wait — this is this test's own known-bad
        self-test: reverting to serial scheduling turns this from a
        near-instant pass into a ~5s failure, not a hang.
        """
        marker = self.runtime / "python-phase-started"
        poll_script = (
            "import pathlib, time, sys\n"
            f"target = pathlib.Path({str(marker)!r})\n"
            "deadline = time.monotonic() + 5.0\n"
            "while not target.exists():\n"
            "    if time.monotonic() > deadline:\n"
            "        sys.exit(1)\n"
            "    time.sleep(0.02)\n"
        )
        phases = (
            PhaseSpec(
                "js-syntax",
                (sys.executable, "-c", poll_script),
                "leading-check",
                "generic",
            ),
            PhaseSpec(
                "python",
                (
                    sys.executable,
                    "-c",
                    f"import pathlib; pathlib.Path({str(marker)!r}).touch()",
                ),
                "python3 scripts/run_python_tests.py",
                "python",
            ),
        )

        result, _terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(summary.state, "passed")
        self.assertEqual(
            tuple(phase.state for phase in summary.phases),
            ("passed", "passed"),
        )

    def test_sigterm_kills_every_concurrently_active_phase(self) -> None:
        """Issue #1131 regression pin for the single-``_active_process``
        shape ``execute_phase``/the interrupt handler replaced with a
        registry: once a leading phase and a trailing "python" phase run
        concurrently, SIGTERM must signal BOTH currently-running
        processes, not only whichever most recently registered. A third
        leading phase ("js-unit") that has not started yet when the
        signal arrives must still stay "not-run" — the same "no new phase
        starts after an interrupt" contract the single-group case already
        had, now proven independently for the leading group's own thread.

        Issue #1131 review B1: the ``"interrupted"`` STATE alone is not
        proof of an actual kill — ``run_one_phase`` labels a phase
        interrupted purely from ``interrupted_signal`` being set once its
        executor call returns, regardless of whether THAT process was ever
        signalled. A registry collapsed back to a single most-recent slot
        still kills "python" (the last to register) but leaves "js-syntax"
        to sleep its own full ``time.sleep(30)`` — and the state tuple
        alone would still read exactly
        ``("interrupted", "not-run", "interrupted")``. Only the elapsed
        time (or an unset post-sleep marker) distinguishes a genuine kill
        from a label applied after the fact; the assertions below check
        both.
        """
        leading_marker = self.runtime / "leading-phase-started"
        leading_survived_sentinel = self.runtime / "js-syntax-survived-the-sleep"
        js_unit_sentinel = self.runtime / "js-unit-ran"
        phases = (
            PhaseSpec(
                "js-syntax",
                (
                    sys.executable,
                    "-c",
                    (
                        "import pathlib, time\n"
                        f"pathlib.Path({str(leading_marker)!r}).touch()\n"
                        "time.sleep(30)\n"
                        f"pathlib.Path({str(leading_survived_sentinel)!r}).touch()\n"
                    ),
                ),
                "leading-check",
                "generic",
            ),
            PhaseSpec(
                "js-unit",
                (
                    sys.executable,
                    "-c",
                    f"import pathlib; pathlib.Path({str(js_unit_sentinel)!r}).touch()",
                ),
                "js-unit-check",
                "generic",
            ),
            PhaseSpec(
                "python",
                (
                    sys.executable,
                    "-c",
                    (
                        "import os, pathlib, signal, time, sys\n"
                        f"target = pathlib.Path({str(leading_marker)!r})\n"
                        "deadline = time.monotonic() + 5.0\n"
                        "while not target.exists():\n"
                        "    if time.monotonic() > deadline:\n"
                        "        sys.exit(1)\n"
                        "    time.sleep(0.02)\n"
                        "os.kill(os.getppid(), signal.SIGTERM)\n"
                        "time.sleep(30)\n"
                    ),
                ),
                "python3 scripts/run_python_tests.py",
                "python",
            ),
        )

        result, terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 143)
        self.assertEqual(summary.state, "interrupted")
        self.assertEqual(
            tuple(phase.state for phase in summary.phases),
            ("interrupted", "not-run", "interrupted"),
        )
        # The state tuple alone cannot distinguish "actually killed" from
        # "labelled interrupted after running to completion regardless"
        # (review B1) — these two assertions can, and are what a registry
        # collapsed to a single most-recent slot actually fails.
        self.assertLess(summary.phases[0].elapsed_seconds, 20.0)
        self.assertLess(summary.phases[2].elapsed_seconds, 20.0)
        self.assertFalse(leading_survived_sentinel.exists())
        self.assertIn("INTERRUPTED: signal 15", terminal)
        self.assertFalse(js_unit_sentinel.exists())

    def test_leading_thread_start_failure_does_not_mask_the_real_exception(
        self,
    ) -> None:
        """Issue #1131 review N5: if starting the leading-phase-group
        thread itself raises (e.g. the OS refuses to spawn a new thread),
        that original exception must propagate — not a RuntimeError from
        `finally` calling `.join()` on a thread that was never started,
        which would replace the real diagnosis with an unrelated one."""
        phases = (
            PhaseSpec(
                "js-syntax",
                _python_command("ok", 0),
                "leading-check",
                "generic",
            ),
            PhaseSpec(
                "python",
                _python_command("ok", 0),
                "python3 scripts/run_python_tests.py",
                "python",
            ),
        )
        with mock.patch.object(
            threading.Thread,
            "start",
            side_effect=RuntimeError("simulated thread start failure"),
        ), self.assertRaisesRegex(
            RuntimeError, "simulated thread start failure"
        ):
            self._run(phases)

    def test_active_processes_lock_is_reentrant(self) -> None:
        """Issue #1131 review B2: ``_active_processes_lock`` MUST be
        reentrant. The SIGINT/SIGTERM/SIGHUP handler always runs on the
        main thread (a Python guarantee), and the main thread also runs
        ``execute_phase`` for the trailing "python" phase — so a signal
        landing while that thread holds this lock inside
        ``execute_phase`` (registering or de-registering its own
        process) makes the handler RE-ENTER the same thread's lock. A
        plain ``threading.Lock`` deadlocks there — unkillable, since
        SIGTERM cannot interrupt a thread blocked acquiring its own
        already-held lock — strictly worse than the pre-#1131 handler,
        which took no lock at all.

        Proven directly rather than via a real signal: that race's
        window is microseconds, so reliably HITTING it through genuine
        signal timing would make this test slow and flaky. Re-entering
        from the same thread must succeed immediately for an RLock; a
        plain Lock's ``acquire(blocking=False)`` returns False for that
        same re-entry attempt — the exact call the signal handler makes
        with ``blocking=True``, which is what would hang forever.
        """
        acquired_outer = _active_processes_lock.acquire(blocking=False)
        self.assertTrue(acquired_outer, "outer acquire should never contend here")
        try:
            reentered = _active_processes_lock.acquire(blocking=False)
            self.assertTrue(
                reentered,
                "re-entering _active_processes_lock from the same thread "
                "must succeed immediately (RLock) — a plain Lock returns "
                "False here, and would block forever under the signal "
                "handler's real blocking=True acquire",
            )
            if reentered:
                _active_processes_lock.release()
        finally:
            _active_processes_lock.release()

    def test_summary_schema_rejects_wrong_wire_types(self) -> None:
        result, _terminal = self._run(
            (
                PhaseSpec(
                    "pass",
                    _python_command("ok", 0),
                    "pass-check",
                    "generic",
                ),
            )
        )
        payload = json.loads((result.bundle / "summary.json").read_text())
        payload["schema_version"] = "1"

        with self.assertRaises(msgspec.ValidationError):
            msgspec.json.decode(
                json.dumps(payload).encode(),
                type=CheckSummary,
            )

    def test_wrong_python_marker_type_becomes_infrastructure_failure(self) -> None:
        phases = (
            PhaseSpec(
                "python",
                _python_command(
                    FAILURE_MARKER_PREFIX
                    + '{"identity":7,"owner":"tests/test_bad.py",'
                    '"detail":"bad marker"}',
                    1,
                ),
                "python3 scripts/run_python_tests.py",
                "python",
            ),
        )

        result, _terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 2)
        self.assertEqual(summary.state, "infrastructure-failure")
        self.assertEqual(summary.phases[0].state, "infrastructure-failure")
        self.assertIn("ValidationError", summary.phases[0].failures[0].detail)

    def test_bundle_creation_rejects_a_non_private_runtime_before_work(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix="cratedigger-public-runtime-"
        ) as tempdir:
            runtime = Path(tempdir)
            runtime.chmod(0o755)

            with self.assertRaisesRegex(RuntimeError, "mode 0700"):
                run_suite(
                    repo_root=REPO_ROOT,
                    phases=(
                        PhaseSpec(
                            "must-not-run",
                            _python_command("wrong", 0),
                            "must-not-run",
                            "generic",
                        ),
                    ),
                    runtime_dir=runtime,
                    stream=io.StringIO(),
                )

    def test_dirty_fingerprint_changes_with_content_not_only_path(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix="cratedigger-fingerprint-"
        ) as tempdir:
            repo = Path(tempdir)
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(
                ["git", "config", "user.email", "tests@example.invalid"],
                cwd=repo,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Suite tests"],
                cwd=repo,
                check=True,
            )
            tracked = repo / "tracked"
            tracked.write_text("clean\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked"], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-qm", "fixture"],
                cwd=repo,
                check=True,
            )
            clean_dirty, clean_fingerprint = dirty_state_fingerprint(repo)
            tracked.write_text("first\n", encoding="utf-8")
            first_dirty, first_fingerprint = dirty_state_fingerprint(repo)
            tracked.write_text("second\n", encoding="utf-8")
            second_dirty, second_fingerprint = dirty_state_fingerprint(repo)

        self.assertFalse(clean_dirty)
        self.assertTrue(first_dirty)
        self.assertTrue(second_dirty)
        self.assertEqual(
            len({clean_fingerprint, first_fingerprint, second_fingerprint}),
            3,
        )

    def test_vulture_freshness_diff_indexes_every_new_candidate(self) -> None:
        phases = (
            PhaseSpec(
                "vulture",
                _python_command(
                    "+first  # unused variable (lib/one.py:7)\n"
                    "+second  # unused function (lib/two.py:11)",
                    3,
                ),
                "bash scripts/find_dead_code.sh",
                "vulture",
                (3,),
            ),
        )

        result, terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 1)
        self.assertEqual(
            tuple(failure.identity for failure in summary.phases[0].failures),
            ("lib/one.py:7", "lib/two.py:11"),
        )
        self.assertIn("first: unused variable", terminal)
        self.assertIn("second: unused function", terminal)

    def test_ram_root_exhausted_exit_code_is_not_an_ordinary_python_failure_code(
        self,
    ) -> None:
        """Issue #1111 review M4(a)/m14: TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
        must stay outside the "python" phase's declared failure_exit_codes
        in BOTH the canonical suite's _default_phases() and the targeted
        runner's targeted_phases() — either one silently regaining it would
        let the infrastructure-failure promotion revert with no test
        noticing, exactly the shape M4 closed for the plain exit-code
        contract."""
        canonical_python = next(
            phase for phase in _default_phases() if phase.name == "python"
        )
        targeted_python = next(
            phase
            for phase in targeted_phases(("tests.test_typing_ratchet",))
            if phase.name == "python"
        )
        for desc, phase in (
            ("canonical _default_phases", canonical_python),
            ("targeted targeted_phases", targeted_python),
        ):
            with self.subTest(desc=desc):
                self.assertNotIn(
                    TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE, phase.failure_exit_codes
                )

    def test_pure_ram_root_exhaustion_promotes_the_suite_to_infrastructure_failure(
        self,
    ) -> None:
        """Issue #1111 review M4(b): a "python" phase that emits the
        collapsed ram-root marker and exits TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
        must promote the whole suite to infrastructure-failure (exit 2), not
        an ordinary failed (exit 1) — made permanent from the review's own
        probe."""
        marker = msgspec.json.encode(
            CheckFailureMarker(
                identity=TEST_RAM_ROOT_EXHAUSTED,
                owner="",
                detail=(
                    "1 target(s)/test(s) failed while the shared test RAM "
                    "root was exhausted"
                ),
                test_ids=("tests.test_alpha",),
            )
        ).decode()
        phases = (
            PhaseSpec(
                "python",
                _python_command(
                    f"{FAILURE_MARKER_PREFIX}{marker}",
                    TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
                ),
                "python3 scripts/run_python_tests.py",
                "python",
            ),
        )

        result, _terminal = self._run(phases)
        summary = decode_summary(result.bundle / "summary.json")

        self.assertEqual(result.exit_code, 2)
        self.assertEqual(summary.state, "infrastructure-failure")
        self.assertEqual(summary.phases[0].state, "infrastructure-failure")
        self.assertEqual(
            summary.phases[0].failures[0].identity, TEST_RAM_ROOT_EXHAUSTED
        )


class SuiteAdmissionTestCase(unittest.TestCase):
    """Direct contracts for the exclusive suite-runner admission lock (#1111)."""

    def setUp(self) -> None:
        shared = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertTrue(
            shared.is_dir(),
            "private runtime tmpfs is required for this test",
        )
        self.runtime = Path(
            tempfile.mkdtemp(dir=shared, prefix="cratedigger-admission-test-")
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.runtime, ignore_errors=True)

    def test_lock_path_is_scoped_to_the_given_runtime_root(self) -> None:
        self.assertEqual(
            admission_lock_path(self.runtime),
            self.runtime / ".cratedigger-test-admission.lock",
        )

    def test_wait_times_out_and_names_the_contended_lock_path(self) -> None:
        lock_path = admission_lock_path(self.runtime)
        held = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(held, fcntl.LOCK_EX)
        try:
            stream = io.StringIO()
            with (
                self.assertRaises(SuiteAdmissionTimeout) as caught,
                acquire_suite_admission(
                    self.runtime,
                    stream=stream,
                    timeout_seconds=0.15,
                    poll_seconds=0.02,
                    progress_interval_seconds=0.05,
                ),
            ):
                raise AssertionError("must not run while contended")
            self.assertIn(str(lock_path), str(caught.exception))
        finally:
            fcntl.flock(held, fcntl.LOCK_UN)
            os.close(held)

        # A progress message names what it is waiting for BEFORE timing out.
        self.assertIn("waiting for admission", stream.getvalue())
        self.assertIn(str(lock_path), stream.getvalue())

    def test_wait_succeeds_once_the_holder_releases(self) -> None:
        lock_path = admission_lock_path(self.runtime)
        held = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(held, fcntl.LOCK_EX)

        def _release_shortly() -> None:
            time.sleep(0.05)
            fcntl.flock(held, fcntl.LOCK_UN)
            os.close(held)

        releaser = threading.Thread(target=_release_shortly)
        releaser.start()
        try:
            stream = io.StringIO()
            entered = False
            with acquire_suite_admission(
                self.runtime,
                stream=stream,
                timeout_seconds=5.0,
                poll_seconds=0.02,
                progress_interval_seconds=0.05,
            ):
                entered = True
            self.assertTrue(entered)
            self.assertIn("waiting for admission", stream.getvalue())
            self.assertIn("admission acquired", stream.getvalue())
        finally:
            releaser.join(timeout=5.0)

    def test_an_uncontended_lock_is_acquired_without_any_progress_message(
        self,
    ) -> None:
        stream = io.StringIO()

        with acquire_suite_admission(self.runtime, stream=stream):
            pass

        self.assertEqual(stream.getvalue(), "")

    def test_waiter_reports_the_real_holders_identity(self) -> None:
        """Issue #1111 review m9: a waiter's progress message names the
        actual current holder (pid + start ticks), read from what
        acquire_suite_admission itself writes on acquisition — not just a
        generic "another canonical suite" with no identity, matching
        run_final_gate.sh's own helper/gate pid+start-ticks precedent."""
        holder_ready = threading.Event()
        release_holder = threading.Event()

        def _hold() -> None:
            with acquire_suite_admission(self.runtime, stream=io.StringIO()):
                holder_ready.set()
                release_holder.wait(5.0)

        holder_thread = threading.Thread(target=_hold)
        holder_thread.start()
        try:
            self.assertTrue(holder_ready.wait(5.0), "holder never acquired")
            stream = io.StringIO()
            with (
                self.assertRaises(SuiteAdmissionTimeout),
                acquire_suite_admission(
                    self.runtime,
                    stream=stream,
                    timeout_seconds=0.2,
                    poll_seconds=0.02,
                    progress_interval_seconds=0.05,
                ),
            ):
                raise AssertionError("must not run while contended")
            # threading shares one process pid; this proves the wiring reads
            # what the real holder wrote, not a hand-typed placeholder.
            self.assertIn(f"pid {os.getpid()}", stream.getvalue())
            self.assertIn("start ticks", stream.getvalue())
        finally:
            release_holder.set()
            holder_thread.join(timeout=5.0)

    def test_read_lock_holder_identity_rejects_malformed_content(self) -> None:
        lock_path = admission_lock_path(self.runtime)
        CASES = (
            ("missing file", None),
            ("empty", ""),
            ("only a pid", "12345"),
            ("three fields", "12345 6789 extra"),
            ("non-digit pid", "abc 6789"),
            ("non-digit ticks", "12345 abc"),
        )
        for desc, content in CASES:
            with self.subTest(desc=desc):
                if content is not None:
                    lock_path.write_text(content)
                elif lock_path.exists():
                    lock_path.unlink()

                self.assertIsNone(_read_lock_holder_identity(lock_path))

    def test_read_lock_holder_identity_parses_well_formed_live_content(
        self,
    ) -> None:
        """A syntactically well-formed pid+ticks pair is only trusted once
        verified live (issue #1111 review MAJOR-2) — this uses our own real,
        currently-running process so the liveness check genuinely passes."""
        lock_path = admission_lock_path(self.runtime)
        ticks = _proc_start_ticks(os.getpid())
        assert ticks is not None
        lock_path.write_text(f"{os.getpid()} {ticks}\n")

        self.assertEqual(
            _read_lock_holder_identity(lock_path),
            f"pid {os.getpid()}, start ticks {ticks}",
        )

    def test_read_lock_holder_identity_rejects_a_stale_dead_pid(self) -> None:
        """A syntactically well-formed but DEAD pid+ticks pair — e.g. a
        lingering write from a holder that has since released — must never
        be reported as live (issue #1111 review MAJOR-2)."""
        lock_path = admission_lock_path(self.runtime)
        # A PID far past any real process on this host; guaranteed dead.
        lock_path.write_text("999999999 123456\n")

        self.assertIsNone(_read_lock_holder_identity(lock_path))

    def test_waiter_falls_back_to_pathname_only_message_for_a_stale_identity(
        self,
    ) -> None:
        """The waiter must not confidently name a dead process (issue #1111
        review MAJOR-2): a lingering/bogus identity in the lockfile degrades
        to the plain lockfile-path message, exercised through the real
        contended-wait path, not just the pure reader in isolation."""
        lock_path = admission_lock_path(self.runtime)
        held = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(held, fcntl.LOCK_EX)
        lock_path.write_text("999999999 123456\n")
        try:
            stream = io.StringIO()
            with (
                self.assertRaises(SuiteAdmissionTimeout),
                acquire_suite_admission(
                    self.runtime,
                    stream=stream,
                    timeout_seconds=0.15,
                    poll_seconds=0.02,
                    progress_interval_seconds=0.05,
                ),
            ):
                raise AssertionError("must not run while contended")
            output = stream.getvalue()
            self.assertIn("waiting for admission", output)
            self.assertNotIn("999999999", output)
            self.assertNotIn("pid ", output)
        finally:
            fcntl.flock(held, fcntl.LOCK_UN)
            os.close(held)

    def test_release_clears_the_holder_identity(self) -> None:
        """Issue #1111 review MAJOR-2: releasing must not leave a stale
        identity for the next waiter to mis-attribute."""
        lock_path = admission_lock_path(self.runtime)

        with acquire_suite_admission(self.runtime, stream=io.StringIO()):
            self.assertNotEqual(lock_path.read_text().strip(), "")

        self.assertEqual(lock_path.read_text().strip(), "")


def _chown_via_user_namespace(
    path: Path, owner: str, *, recursive: bool = False
) -> None:
    """Remap ``path``'s ownership through an unprivileged user namespace.

    Issue #1208 review D-F1: an earlier version of the foreign-uid guard
    tests here (and this commit's own message) claimed constructing a
    genuinely foreign-uid directory "requires root or a user-namespace
    remap, neither available to this test user" — false. The remap IS
    available: this is the exact ``unshare --map-root-user --map-auto
    chown`` technique ``tests/fakes/beets_contract.py``'s ``_chown_path``
    already uses, load-bearing, on every run of this suite. ``owner``
    values are uid:gid pairs INSIDE the namespace — ``--map-root-user``
    maps namespace uid 0 back to this process's own real uid, so
    ``"0:0"`` restores real ownership (the unseal direction) and any
    other pair (e.g. ``"1:1"``) lands on a genuinely different,
    auto-allocated subordinate uid outside the namespace — no root
    required, matching the review's own reproduction (``st_uid=100000``
    from ``chown -R 1:1``).
    """
    recursive_flag = ["-R"] if recursive else []
    subprocess.run(
        [
            "unshare", "--map-root-user", "--map-auto", "chown",
            *recursive_flag, owner, str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _run_in_uid_remapped_namespace(script: str) -> subprocess.CompletedProcess[str]:
    """Run ``script`` (a self-contained Python source string) as fake-root
    inside a fresh, unprivileged user namespace.

    Issue #1208 review D-F1, second correction: a delete attempt run from
    a NORMAL (non-namespaced) process against a directory chowned via
    ``_chown_via_user_namespace`` above is invisible to the foreign-uid
    guard clause, because ordinary POSIX permission bits (mode 0700,
    foreign owner) already block ``shutil.rmtree`` with a plain
    ``PermissionError`` before the software guard is ever reached —
    empirically verified: this repository's own reap functions catch
    that ``OSError`` and silently continue, so the guard-removed mutant
    is unobservable that way, the same redundancy D6 already documented
    honestly for the symlink/non-directory clauses. That is NOT the
    world the review's own reproduction table describes.

    The actual discriminating world: `unshare --map-root-user` makes the
    CALLING process uid 0 (fake-root) WITHIN the new namespace, and
    Linux root has ``DAC_OVERRIDE`` for any file owned by a uid the
    namespace's own mapping covers — so a directory chowned to a
    DIFFERENT in-namespace uid (``os.chown(path, 1, 1)``, run from
    inside this same namespace) is fully readable, writable, and
    deletable by that fake-root process. There, POSIX permissions grant
    NOTHING — the software guard (``info.st_uid != os.getuid()``) is the
    ONLY thing between it and deletion. Verified directly: inside such a
    namespace, ``os.getuid()`` reads 0, a directory chowned to uid 1
    reports ``st_uid=1``, and ``shutil.rmtree`` on it succeeds outright
    (no exception at all) — the exact shape a caller of this reaper
    could produce by never having remapped uid 1 to anything, or a
    future refactor that lost the guard.

    This runs the real reap function inside exactly that world and
    reports a ``RESULT:PASS``/``RESULT:FAIL`` marker on stdout so the
    caller can assert on it without itself needing namespace access.
    """
    return subprocess.run(
        ["unshare", "--map-root-user", "--map-auto", sys.executable, "-c", script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


class StaleCheckBundleReapTestCase(unittest.TestCase):
    """Contracts for admission-time cleanup of stale check bundles (#1111)."""

    def setUp(self) -> None:
        shared = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertTrue(
            shared.is_dir(),
            "private runtime tmpfs is required for this test",
        )
        self.runtime = Path(
            tempfile.mkdtemp(dir=shared, prefix="cratedigger-reap-test-")
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.runtime, ignore_errors=True)

    def _bundle(self, name: str, *, age_seconds: float, now: float) -> Path:
        bundle = self.runtime / name
        bundle.mkdir(mode=0o700)
        summary = bundle / "summary.json"
        summary.write_text("{}\n", encoding="utf-8")
        stamp = now - age_seconds
        os.utime(summary, (stamp, stamp))
        return bundle

    def test_reaps_bundles_older_than_the_threshold(self) -> None:
        now = time.time()
        stale = self._bundle("cratedigger-checks.stale", age_seconds=999, now=now)

        reaped = reap_stale_check_bundles(
            self.runtime,
            max_age_seconds=100,
            reference_time=now,
        )

        self.assertEqual(reaped, (stale,))
        self.assertFalse(stale.exists())

    def test_keeps_bundles_within_the_threshold(self) -> None:
        now = time.time()
        fresh = self._bundle("cratedigger-checks.fresh", age_seconds=10, now=now)

        reaped = reap_stale_check_bundles(
            self.runtime,
            max_age_seconds=100,
            reference_time=now,
        )

        self.assertEqual(reaped, ())
        self.assertTrue(fresh.exists())

    def test_ignores_directories_matching_no_reapable_prefix(self) -> None:
        # NOTE: this must NOT be a "cratedigger-tests." prefix — issue #1208
        # item 1 registered that prefix precisely so it IS considered for
        # reaping (see the ScratchTreeOwnershipTestCase-style methods below
        # in this same class). This test instead proves a directory
        # matching no registered prefix at all is left untouched.
        now = time.time()
        unrelated = self.runtime / "some-other-tool.unrelated"
        unrelated.mkdir(mode=0o700)
        stamp = now - 999
        os.utime(unrelated, (stamp, stamp))

        reaped = reap_stale_check_bundles(
            self.runtime,
            max_age_seconds=100,
            reference_time=now,
        )

        self.assertEqual(reaped, ())
        self.assertTrue(unrelated.exists())

    def _leaked_test_scaffold(
        self, name: str, *, age_seconds: float, now: float
    ) -> Path:
        """A directory shaped like a killed test process's own leaked
        tempfile.mkdtemp fixture — no summary.json, just the bare dir."""
        scaffold = self.runtime / name
        scaffold.mkdir(mode=0o700)
        stamp = now - age_seconds
        os.utime(scaffold, (stamp, stamp))
        return scaffold

    def test_reaps_stale_test_scaffolding_prefixes_too(self) -> None:
        """Issue #1111 review m8: a killed test process (SIGKILL, OOM)
        leaks its tests/test_suite_coordinator.py fixture directory before
        tearDown can remove it — those prefixes must also age out."""
        now = time.time()
        stale = tuple(
            self._leaked_test_scaffold(name, age_seconds=999, now=now)
            for name in (
                "cratedigger-suite-test-leaked",
                "cratedigger-admission-test-leaked",
                "cratedigger-reap-test-leaked",
                "cratedigger-headroom-test-leaked",
            )
        )
        fresh = self._leaked_test_scaffold(
            "cratedigger-suite-test-current", age_seconds=1, now=now
        )

        reaped = reap_stale_check_bundles(
            self.runtime,
            max_age_seconds=100,
            reference_time=now,
        )

        self.assertEqual(set(reaped), set(stale))
        for path in stale:
            self.assertFalse(path.exists())
        self.assertTrue(fresh.exists())

    def _receipt(self, name: str, *, bundle: Path) -> Path:
        receipt = self.runtime / name
        receipt.mkdir(mode=0o700)
        (receipt / "bundle").write_text(f"{bundle}\n", encoding="utf-8")
        return receipt

    def test_never_reaps_a_bundle_a_live_receipt_still_references(self) -> None:
        """Issue #1111 review m13: a run_final_gate.sh receipt's own bundle
        survives regardless of age, preserving both verdict and evidence
        for a long-running review."""
        now = time.time()
        referenced = self._bundle(
            "cratedigger-checks.referenced", age_seconds=999, now=now
        )
        self._receipt("cratedigger-final-gate.live", bundle=referenced)

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, ())
        self.assertTrue(referenced.exists())

    def test_still_reaps_an_unreferenced_bundle_alongside_a_protected_one(
        self,
    ) -> None:
        now = time.time()
        referenced = self._bundle(
            "cratedigger-checks.referenced", age_seconds=999, now=now
        )
        unreferenced = self._bundle(
            "cratedigger-checks.unreferenced", age_seconds=999, now=now
        )
        self._receipt("cratedigger-final-gate.live", bundle=referenced)

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, (unreferenced,))
        self.assertTrue(referenced.exists())
        self.assertFalse(unreferenced.exists())

    def test_never_reaps_a_bundle_owned_by_a_foreign_uid(self) -> None:
        """Issue #1208 review D-F1: the foreign-uid guard
        (``info.st_uid != os.getuid()``) here — see
        ``_run_in_uid_remapped_namespace`` for why the delete attempt
        must run INSIDE the same user namespace that performed the
        chown to actually exercise this clause (a normal-process delete
        attempt is already blocked by ordinary POSIX permissions,
        vacuously)."""
        bundle = self.runtime / "cratedigger-checks.foreignowner"
        script = (
            "import os\n"
            "import time\n"
            "from pathlib import Path\n"
            "from scripts.run_test_suite import reap_stale_check_bundles\n"
            "\n"
            f"runtime = Path({str(self.runtime)!r})\n"
            f"bundle = Path({str(bundle)!r})\n"
            "bundle.mkdir(mode=0o700)\n"
            "summary = bundle / 'summary.json'\n"
            "summary.write_text('{}\\n')\n"
            "now = time.time()\n"
            "stamp = now - 999\n"
            "os.utime(summary, (stamp, stamp))\n"
            "os.chown(bundle, 1, 1)\n"
            "\n"
            "reaped = reap_stale_check_bundles(\n"
            "    runtime, max_age_seconds=100, reference_time=now\n"
            ")\n"
            "ok = reaped == () and bundle.exists()\n"
            "print('RESULT:' + ('PASS' if ok else 'FAIL'))\n"
        )
        try:
            completed = _run_in_uid_remapped_namespace(script)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn(
                "RESULT:PASS",
                completed.stdout,
                f"stdout={completed.stdout!r} stderr={completed.stderr!r}",
            )
        finally:
            if bundle.exists():
                _chown_via_user_namespace(bundle, "0:0", recursive=True)

    # --- Issue #1208 item 1: ownership-marker liveness for
    # SCRATCH_TREE_PREFIX ("cratedigger-tests.*"), the dev shell's own main
    # scratch TMPDIR. A prior attempt reaped these on mtime alone and was
    # reverted after review found it reaps LIVE trees (a busy suite's
    # scratch root can go quiet for hours while very much in use). These
    # tests prove the replacement ownership-marker design instead.

    def test_scratch_tree_prefix_matches_test_tmpfs_shs_own_mktemp_template(
        self,
    ) -> None:
        """Issue #1208 review D2: an earlier version of this docstring
        falsely claimed this pin "is the only thing that would catch the
        constant here drifting" and, implicitly, that it binds this
        constant to the producer. Neither is true — both sides of this
        equality are hand-typed literals (this one, and
        scripts/test_tmpfs.sh's own `mktemp -d
        "$parent/cratedigger-tests.XXXXXX"` template), so this is a
        same-repo consistency check between two independently-written
        strings, not a producer binding. Real producer-side drift is
        caught elsewhere, against the REAL shell function:
        `tests.test_test_tmpfs.TestTmpfsSetup.test_allocates_isolated_
        tmpfs_directory_and_cleans_it_on_exit` (asserts the real created
        directory's name against its own hand-typed "cratedigger-tests."
        literal) and
        `tests.test_test_tmpfs.ScratchTreeOwnershipMarkerTestCase`'s real
        SIGKILL round trip (drives the real setup function end to end).
        This pin is kept anyway because it fails fast and names the exact
        drifted value if the two literals here and in run_test_suite.py
        ever disagree with each other — it does not replace those."""
        self.assertEqual(SCRATCH_TREE_PREFIX, "cratedigger-tests.")

    def _scratch_tree(self, name: str, *, age_seconds: float, now: float) -> Path:
        assert name.startswith(SCRATCH_TREE_PREFIX)
        tree = self.runtime / name
        tree.mkdir(mode=0o700)
        stamp = now - age_seconds
        os.utime(tree, (stamp, stamp))
        return tree

    @staticmethod
    def _write_owner_marker(tree: Path, pid: int, ticks: str) -> None:
        (tree / SCRATCH_TREE_OWNER_MARKER_NAME).write_text(f"{pid} {ticks}\n")

    def test_scratch_tree_owner_dead_is_false_for_a_missing_marker(self) -> None:
        """Fail closed: no marker at all is "unknown", never "dead" — the
        tiny post-mktemp race window, or any other cause, must not become
        reap-eligible."""
        tree = self._scratch_tree(
            "cratedigger-tests.nomarker", age_seconds=1, now=time.time()
        )

        self.assertFalse(_scratch_tree_owner_dead(tree))

    def test_scratch_tree_owner_dead_is_false_for_malformed_marker_content(
        self,
    ) -> None:
        now = time.time()
        CASES = (
            ("empty", ""),
            ("only a pid", "12345"),
            ("three fields", "12345 6789 extra"),
            ("non-digit pid", "abc 6789"),
            ("non-digit ticks", "12345 abc"),
            # issue #1208 review D-F7: a superscript "\u00b9" IS
            # str.isdigit() (True) but is NOT str.isdecimal() (False) and
            # int("\u00b9") raises ValueError — the guard must use
            # isdecimal(), or this case crashes the whole suite instead
            # of degrading to "not dead".
            ("unicode digit that int() cannot parse", "\u00b9 6789"),
        )
        for index, (desc, content) in enumerate(CASES):
            with self.subTest(desc=desc):
                tree = self._scratch_tree(
                    f"cratedigger-tests.malformed{index}", age_seconds=1, now=now
                )
                (tree / SCRATCH_TREE_OWNER_MARKER_NAME).write_text(content)

                self.assertFalse(_scratch_tree_owner_dead(tree))

    def test_scratch_tree_owner_dead_is_false_for_a_live_process(self) -> None:
        """Uses our own real, currently-running process — same precedent
        as test_read_lock_holder_identity_parses_well_formed_live_content."""
        tree = self._scratch_tree(
            "cratedigger-tests.livepid", age_seconds=1, now=time.time()
        )
        ticks = _proc_start_ticks(os.getpid())
        assert ticks is not None
        self._write_owner_marker(tree, os.getpid(), ticks)

        self.assertFalse(_scratch_tree_owner_dead(tree))

    def test_scratch_tree_owner_dead_is_true_for_a_guaranteed_dead_pid(self) -> None:
        tree = self._scratch_tree(
            "cratedigger-tests.deadpid", age_seconds=1, now=time.time()
        )
        # A PID far past any real process on this host; guaranteed dead —
        # same precedent as test_read_lock_holder_identity_rejects_a_stale_dead_pid.
        self._write_owner_marker(tree, 999999999, "123456")

        self.assertTrue(_scratch_tree_owner_dead(tree))

    # --- Issue #1208 review D4: "cannot verify" must never collapse into
    # "provably dead". A permission boundary (e.g. a restrictive `hidepid`
    # mount) or a malformed /proc/<pid>/stat line is not constructible
    # against a REAL /proc entry without root or a user-namespace remap,
    # so these use the kwarg-DI seam _proc_stat_start_ticks and
    # _scratch_tree_owner_dead both accept (`read_stat=` / `proc_stat=`) —
    # production always uses the real default; only tests inject.

    def test_proc_stat_start_ticks_confirms_absence_only_on_file_not_found(
        self,
    ) -> None:
        def _raise(exc: BaseException) -> str:
            raise exc

        CASES = (
            (
                "process confirmed gone (ENOENT)",
                lambda pid: _raise(FileNotFoundError()),
                (True, None),
            ),
            (
                "permission boundary (a real, non-ENOENT OSError)",
                lambda pid: _raise(PermissionError()),
                (False, None),
            ),
            (
                "malformed content: no comm-field close paren",
                lambda pid: "not a real stat line",
                (False, None),
            ),
            (
                "malformed content: too few fields after comm",
                lambda pid: "12345 (bash) R 1 2 3",
                (False, None),
            ),
        )
        for desc, read_stat, expected in CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    _proc_stat_start_ticks(12345, read_stat=read_stat),
                    expected,
                )

    def test_proc_stat_start_ticks_reads_real_ticks_on_success(self) -> None:
        """The happy path, through the real default reader. Issue #1208
        review D-F2: an earlier version of this docstring claimed this
        "proves the DI seam's default is wired to the real function" —
        false. It compared against ``_proc_start_ticks``, which shares
        the exact SAME ``_read_proc_stat`` default, so a fake reader
        returning a constant stat line would pass both sides identically
        in isolation — agree-by-construction, not proof. That real
        producer-vs-reader binding is already proven by
        ``test_real_marker_reports_alive_then_dead_across_a_real_sigkill``
        in ``tests/test_test_tmpfs.py`` (an independent awk-based
        producer). This test instead compares against a genuinely
        independent oracle — bash + awk reading the same ``/proc`` entry,
        not Python, and not this module's own parsing code."""
        independent = subprocess.run(
            [
                "bash",
                "-c",
                (
                    f'stat=$(cat /proc/{os.getpid()}/stat); '
                    'stat=${stat##*) }; '
                    "awk '{print $20}' <<<\"$stat\""
                ),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        expected = independent.stdout.strip()

        confirmed_absent, ticks = _proc_stat_start_ticks(os.getpid())

        self.assertFalse(confirmed_absent)
        self.assertEqual(ticks, expected)

    def test_scratch_tree_owner_dead_is_false_when_liveness_cannot_be_verified(
        self,
    ) -> None:
        """THE outermost real adapter for this decision: a marker naming
        a pid whose /proc read fails for a reason OTHER than confirmed
        absence must never be treated as dead — "unknown" degrades to
        "alive", the fail-closed half of the ownership-marker design, now
        proven for the /proc-read failure mode specifically (not just the
        marker-file failure modes the earlier tests in this class cover)."""
        tree = self._scratch_tree(
            "cratedigger-tests.unverifiable", age_seconds=1, now=time.time()
        )
        self._write_owner_marker(tree, 999999999, "123456")

        self.assertFalse(
            _scratch_tree_owner_dead(
                tree, proc_stat=lambda pid: (False, None)
            )
        )

    @staticmethod
    def _observed_start_ticks(pid: int) -> str:
        ticks = _proc_start_ticks(pid)
        deadline = time.monotonic() + 5.0
        while ticks is None and time.monotonic() < deadline:
            time.sleep(0.02)
            ticks = _proc_start_ticks(pid)
        assert ticks is not None, f"could not observe start ticks for pid {pid}"
        return ticks

    def test_reap_never_touches_a_live_owned_scratch_tree(self) -> None:
        """THE most important contract in this PR (issue #1208): a live
        owner is never reaped, however stale its directory mtime looks.
        Proven against a REAL subprocess, not a mock or our own test
        process — this is exactly the scenario the reverted mtime-based
        attempt got wrong."""
        proc = subprocess.Popen(["sleep", "30"])
        try:
            ticks = self._observed_start_ticks(proc.pid)
            now = time.time()
            stamp = now - 999
            tree = self._scratch_tree(
                "cratedigger-tests.livechild", age_seconds=999, now=now
            )
            self._write_owner_marker(tree, proc.pid, ticks)
            # Writing the marker just now bumped the directory's own mtime
            # AND gave the .owner file itself a fresh mtime —
            # _scratch_tree_last_activity (issue #1208 review D3) walks
            # the WHOLE tree, so restamping only the top-level dir is not
            # enough: without restamping the marker file too, the age
            # gate alone (not the live-owner check) would be what keeps
            # this test green, defeating the point of the test.
            self._restamp_recursive(tree, stamp)

            reaped = reap_stale_check_bundles(
                self.runtime, max_age_seconds=100, reference_time=now
            )

            self.assertEqual(reaped, ())
            self.assertTrue(tree.exists())
        finally:
            proc.kill()
            proc.wait(timeout=5)

    def test_reap_removes_a_dead_owned_stale_scratch_tree(self) -> None:
        """Converse of the above: once the real owning process is
        provably gone AND the tree is stale, it is reaped."""
        proc = subprocess.Popen(["sleep", "30"])
        ticks = self._observed_start_ticks(proc.pid)
        pid = proc.pid
        proc.kill()
        proc.wait(timeout=5)

        now = time.time()
        stamp = now - 999
        tree = self._scratch_tree(
            "cratedigger-tests.deadchild", age_seconds=999, now=now
        )
        self._write_owner_marker(tree, pid, ticks)
        # Writing the marker just now bumped the directory's own mtime AND
        # gave the .owner file itself a fresh mtime — _scratch_tree_last_
        # activity (issue #1208 review D3) walks the WHOLE tree, so both
        # need restoring to genuine staleness, not just the top-level dir.
        self._restamp_recursive(tree, stamp)

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, (tree,))
        self.assertFalse(tree.exists())

    def test_reap_keeps_a_dead_owned_scratch_tree_within_the_age_floor(
        self,
    ) -> None:
        """The age gate still applies on top of provable death — a
        process dying seconds ago does not make its tree fair game
        immediately; both conditions are required."""
        now = time.time()
        tree = self._scratch_tree(
            "cratedigger-tests.freshdead", age_seconds=1, now=now
        )
        self._write_owner_marker(tree, 999999999, "123456")

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, ())
        self.assertTrue(tree.exists())

    def test_reap_keeps_a_stale_scratch_tree_with_no_marker_at_all(self) -> None:
        """Fail-closed exercised through the real reap entry point: a
        directory with no ownership marker (the tiny post-mktemp race, or
        anything else) is never reaped through this mechanism, however
        old it looks — the reverted design's own failure mode inverted."""
        now = time.time()
        tree = self._scratch_tree(
            "cratedigger-tests.unmarked", age_seconds=999, now=now
        )

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, ())
        self.assertTrue(tree.exists())

    # --- Issue #1208 review D3: the age gate must walk the WHOLE tree,
    # not just its top-level directory mtime. A write into an EXISTING
    # nested subdirectory never bumps the top-level dir's own mtime, so
    # an orphaned descendant of the (now dead) owning shell — e.g. issue
    # #1214's fuzz burst, which takes neither the admission lock nor a
    # headroom guard — could keep a tree genuinely in use while its
    # top-level dirent looked stale for hours.

    @staticmethod
    def _restamp_recursive(path: Path, stamp: float) -> None:
        for root, dirs, files in os.walk(path):
            for name in (*dirs, *files):
                os.utime(os.path.join(root, name), (stamp, stamp))
        os.utime(path, (stamp, stamp))

    def test_reap_keeps_a_dead_owned_tree_with_recent_nested_activity(
        self,
    ) -> None:
        now = time.time()
        old = now - 999
        tree = self._scratch_tree(
            "cratedigger-tests.orphanwriting", age_seconds=999, now=now
        )
        self._write_owner_marker(tree, 999999999, "123456")
        nested = tree / "nested"
        nested.mkdir()
        self._restamp_recursive(tree, old)
        # Created AFTER the restamp above, so its mtime (and its parent
        # nested/'s) is genuinely recent — an orphaned descendant still
        # writing, exactly the scenario the top-level-only check misses.
        (nested / "still-writing.tmp").write_text("x")

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, ())
        self.assertTrue(tree.exists())

    def test_reap_removes_a_dead_owned_tree_once_all_nested_activity_is_stale(
        self,
    ) -> None:
        """Converse of the above: once every entry in the tree — not just
        the top-level dir — is genuinely stale, the dead-owned tree is
        reaped."""
        now = time.time()
        old = now - 999
        tree = self._scratch_tree(
            "cratedigger-tests.trulystale", age_seconds=999, now=now
        )
        self._write_owner_marker(tree, 999999999, "123456")
        nested = tree / "nested"
        nested.mkdir()
        (nested / "old.tmp").write_text("x")
        self._restamp_recursive(tree, old)

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, (tree,))
        self.assertFalse(tree.exists())

    def test_reap_keeps_a_dead_owned_tree_when_a_recent_entry_precedes_stale_ones_in_walk_order(
        self,
    ) -> None:
        """Issue #1208 review D-F3: a ``latest = entry_mtime`` mutant
        (last-entry-wins, not ``max()``) is invisible if the fixture's
        only recent entry happens to be visited LAST in ``os.walk``'s
        traversal order — the earlier two D3 tests both had that shape
        by accident. ``os.walk`` in topdown mode (the default, used here)
        guarantees a directory's own direct children are yielded in ONE
        tuple BEFORE any subdirectory's contents are yielded in a LATER
        tuple — so a recent entry placed directly in ``tree`` (yielded
        first) followed by a stale entry nested one level deeper (yielded
        later) deterministically orders "recent, then stale" regardless
        of within-directory listing order, which "last wins" gets
        backwards."""
        now = time.time()
        old = now - 999
        tree = self._scratch_tree(
            "cratedigger-tests.recentfirst", age_seconds=999, now=now
        )
        self._write_owner_marker(tree, 999999999, "123456")
        nested = tree / "nested"
        nested.mkdir()
        (nested / "old.tmp").write_text("x")
        self._restamp_recursive(tree, old)
        # Created AFTER the restamp, directly inside `tree` — visited in
        # os.walk's FIRST yield, strictly before `nested`'s own contents
        # in a later yield. Genuinely recent.
        (tree / "recent.tmp").write_text("x")

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, ())
        self.assertTrue(tree.exists())

    def test_reap_keeps_a_dead_owned_tree_whose_only_recent_entry_is_a_directory(
        self,
    ) -> None:
        """Issue #1208 review D-F3: a ``for name in files`` mutant
        (dropping directory names from the walk targets) is invisible if
        the recent activity happens to also touch a surviving FILE. An
        orphaned descendant that creates and deletes scratch files inside
        a nested directory bumps only that DIRECTORY's own mtime — no
        file with a recent mtime survives to be walked, so the recent
        entry must be found among ``dirs``, not only ``files``."""
        now = time.time()
        old = now - 999
        tree = self._scratch_tree(
            "cratedigger-tests.dirchurn", age_seconds=999, now=now
        )
        self._write_owner_marker(tree, 999999999, "123456")
        nested = tree / "nested"
        nested.mkdir()
        self._restamp_recursive(tree, old)
        # Create then delete a scratch file inside `nested` — bumps ONLY
        # `nested`'s own directory mtime to "now"; nothing recent
        # survives as a walkable file.
        churn_file = nested / "scratch.tmp"
        churn_file.write_text("x")
        churn_file.unlink()

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, ())
        self.assertTrue(tree.exists())


class FinalGateReceiptRetirementTestCase(unittest.TestCase):
    """Contracts for age-gated run_final_gate.sh receipt retirement (#1208 item 4)."""

    def setUp(self) -> None:
        shared = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertTrue(
            shared.is_dir(),
            "private runtime tmpfs is required for this test",
        )
        self.runtime = Path(
            tempfile.mkdtemp(dir=shared, prefix="cratedigger-reap-test-")
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.runtime, ignore_errors=True)

    @staticmethod
    def _live_identity() -> tuple[str, str]:
        """This test process's own real pid+ticks pair — genuinely alive,
        same precedent as test_read_lock_holder_identity_parses_well_
        formed_live_content above."""
        ticks = _proc_start_ticks(os.getpid())
        assert ticks is not None
        return str(os.getpid()), ticks

    @staticmethod
    def _dead_identity() -> tuple[str, str]:
        """A syntactically well-formed but guaranteed-dead pid+ticks pair —
        same precedent as test_read_lock_holder_identity_rejects_a_stale_
        dead_pid above (a PID far past any real process on this host)."""
        return "999999999", "123456"

    def _receipt(
        self,
        name: str,
        *,
        age_seconds: float,
        now: float,
        terminal: bool,
        helper_identity: tuple[str, str] | None = None,
        gate_identity: tuple[str, str] | None = None,
    ) -> Path:
        receipt = self.runtime / name
        receipt.mkdir(mode=0o700)
        if helper_identity is not None:
            (receipt / "helper_pid").write_text(f"{helper_identity[0]}\n")
            (receipt / "helper_start_ticks").write_text(f"{helper_identity[1]}\n")
        if gate_identity is not None:
            (receipt / "gate_pid").write_text(f"{gate_identity[0]}\n")
            (receipt / "gate_start_ticks").write_text(f"{gate_identity[1]}\n")
        stamp = now - age_seconds
        if terminal:
            terminal_path = receipt / "terminal"
            terminal_path.write_text("pass 0\n")
            os.utime(terminal_path, (stamp, stamp))
            # Deliberately leave the receipt directory's own mtime alone
            # (fresh, "now" — it was just mkdir'd above): production
            # creates the receipt directory BEFORE the run and writes
            # `terminal` only at the end, so age_seconds should always be
            # measured from the terminal file, never the directory.
            # Backdating only the terminal file here proves
            # _receipt_last_activity actually reads it, rather than
            # accidentally passing because both timestamps happen to agree.
        else:
            os.utime(receipt, (stamp, stamp))
        return receipt

    def test_retires_a_terminal_receipt_older_than_the_floor(self) -> None:
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.done", age_seconds=999, now=now, terminal=True
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, (receipt,))
        self.assertFalse(receipt.exists())

    def test_keeps_a_terminal_receipt_within_the_floor(self) -> None:
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.recent", age_seconds=10, now=now, terminal=True
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, ())
        self.assertTrue(receipt.exists())

    def test_keeps_an_old_receipt_whose_gate_process_is_still_live(self) -> None:
        """Never retire a receipt whose recorded gate identity is still
        live, however old — an in-progress or genuinely long final gate."""
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.running",
            age_seconds=999,
            now=now,
            terminal=False,
            helper_identity=self._dead_identity(),
            gate_identity=self._live_identity(),
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, ())
        self.assertTrue(receipt.exists())

    def test_keeps_an_old_receipt_whose_helper_process_is_still_live(self) -> None:
        """Symmetric to the gate check: EITHER recorded identity being live
        blocks retirement, not only the gate (asymmetric-mutant guard)."""
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.wrapper-alive",
            age_seconds=999,
            now=now,
            terminal=False,
            helper_identity=self._live_identity(),
            gate_identity=self._dead_identity(),
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, ())
        self.assertTrue(receipt.exists())

    def test_retires_an_old_receipt_with_no_terminal_and_dead_processes(self) -> None:
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.crashed",
            age_seconds=999,
            now=now,
            terminal=False,
            helper_identity=self._dead_identity(),
            gate_identity=self._dead_identity(),
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, (receipt,))
        self.assertFalse(receipt.exists())

    def test_keeps_a_fresh_receipt_with_no_terminal_and_dead_processes(self) -> None:
        """The age gate applies independently of liveness: dead recorded
        processes alone are not sufficient — it must ALSO be old."""
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.fresh-crash",
            age_seconds=1,
            now=now,
            terminal=False,
            helper_identity=self._dead_identity(),
            gate_identity=self._dead_identity(),
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, ())
        self.assertTrue(receipt.exists())

    def test_retires_an_old_receipt_with_no_process_identity_recorded(self) -> None:
        """A missing pid/ticks file (interrupted before the first write) is
        "not live", never assumed live — same posture as
        _read_lock_holder_identity's own unreadable-content fallback."""
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.no-identity",
            age_seconds=999,
            now=now,
            terminal=False,
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, (receipt,))
        self.assertFalse(receipt.exists())

    def test_retirement_does_not_crash_on_a_non_decimal_unicode_digit_identity(
        self,
    ) -> None:
        """Issue #1208 review D-F7: the identical isdigit()-vs-isdecimal()
        bug in ``_receipt_process_field_live`` — a superscript "\u00b9"
        passes ``str.isdigit()`` but ``int("\u00b9")`` raises
        ``ValueError``, which would abort the whole suite instead of
        degrading to "not live", same as the fix for
        ``_scratch_tree_owner_dead``. A malformed identity degrades
        gracefully to "not live" (retirement proceeds), never crashes."""
        now = time.time()
        receipt = self._receipt(
            "cratedigger-final-gate.baddigit",
            age_seconds=999,
            now=now,
            terminal=False,
            helper_identity=("\u00b9", "123456"),
            gate_identity=self._dead_identity(),
        )

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, (receipt,))
        self.assertFalse(receipt.exists())

    def test_retiring_a_receipt_releases_its_bundle_protection(self) -> None:
        """Retirement and bundle reaping run in the SAME admitted pass —
        run_suite calls reap_stale_final_gate_receipts before
        reap_stale_check_bundles — so a retired receipt's bundle is no
        longer protected on the very next reap call. One lifecycle owner,
        no second cleanup path."""
        now = time.time()
        bundle = self.runtime / "cratedigger-checks.orphaned"
        bundle.mkdir(mode=0o700)
        summary = bundle / "summary.json"
        summary.write_text("{}\n")
        stamp = now - 999
        os.utime(summary, (stamp, stamp))
        receipt = self._receipt(
            "cratedigger-final-gate.protecting",
            age_seconds=999,
            now=now,
            terminal=True,
        )
        (receipt / "bundle").write_text(f"{bundle}\n")

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )
        self.assertEqual(retired, (receipt,))

        reaped = reap_stale_check_bundles(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(reaped, (bundle,))
        self.assertFalse(bundle.exists())

    # --- Issue #1208 review D6: per-clause coverage for
    # reap_stale_final_gate_receipts's non-dir/symlink and foreign-uid
    # guards — untested gaps review found by planting mutants that
    # survived. Recorded honestly: for THESE two clauses specifically,
    # shutil.rmtree already refuses to operate on a symlink or a
    # non-directory (raises OSError, caught by this function's own
    # `except OSError: continue`), so removing either guard clause does
    # NOT change the observable `retired` return value or which entities
    # survive on disk — the tests below pin that real end-to-end safety
    # property (proven empirically: shutil.rmtree(symlink) raises
    # "Cannot call rmtree on a symbolic link", shutil.rmtree(regular_file)
    # raises NotADirectoryError), not the specific guard-clause mutant,
    # which this composition genuinely cannot distinguish from "no guard
    # at all". They still matter as regression coverage: if a future
    # change ever replaced shutil.rmtree with something that lacks its
    # built-in refusal, these would be the tests that catch it.

    def test_never_retires_a_symlink_named_like_a_receipt(self) -> None:
        now = time.time()
        real_target = self.runtime / "not-actually-a-receipt"
        real_target.mkdir(mode=0o700)
        (real_target / "terminal").write_text("pass 0\n")
        stamp = now - 999
        os.utime(real_target / "terminal", (stamp, stamp))
        link = self.runtime / "cratedigger-final-gate.symlink"
        link.symlink_to(real_target)

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, ())
        self.assertTrue(link.exists())
        self.assertTrue(real_target.exists())

    def test_never_retires_a_non_directory_named_like_a_receipt(self) -> None:
        now = time.time()
        stray_file = self.runtime / "cratedigger-final-gate.strayfile"
        stray_file.write_text("not a receipt directory\n")
        stamp = now - 999
        os.utime(stray_file, (stamp, stamp))

        retired = reap_stale_final_gate_receipts(
            self.runtime, max_age_seconds=100, reference_time=now
        )

        self.assertEqual(retired, ())
        self.assertTrue(stray_file.exists())

    def test_never_retires_a_receipt_owned_by_a_foreign_uid(self) -> None:
        """Issue #1208 review D-F1: the third guard clause
        (``info.st_uid != os.getuid()``) — D6 originally recorded this as
        untestable without root; that claim was false, closed via
        ``_chown_via_user_namespace``. A second correction: a delete
        attempt run from a normal process against that chowned directory
        is ALSO invisible to this specific clause, because ordinary
        POSIX permissions already block it — see
        ``_run_in_uid_remapped_namespace`` for the actual discriminating
        world (the delete attempt run AS fake-root INSIDE the same
        namespace that performed the chown, where only the software
        guard stands between it and deletion)."""
        receipt = self.runtime / "cratedigger-final-gate.foreignowner"
        script = (
            "import os\n"
            "import time\n"
            "from pathlib import Path\n"
            "from scripts.run_test_suite import reap_stale_final_gate_receipts\n"
            "\n"
            f"runtime = Path({str(self.runtime)!r})\n"
            f"receipt = Path({str(receipt)!r})\n"
            "receipt.mkdir(mode=0o700)\n"
            "terminal = receipt / 'terminal'\n"
            "terminal.write_text('pass 0\\n')\n"
            "now = time.time()\n"
            "stamp = now - 999\n"
            "os.utime(terminal, (stamp, stamp))\n"
            "os.chown(receipt, 1, 1)\n"
            "\n"
            "retired = reap_stale_final_gate_receipts(\n"
            "    runtime, max_age_seconds=100, reference_time=now\n"
            ")\n"
            "ok = retired == () and receipt.exists()\n"
            "print('RESULT:' + ('PASS' if ok else 'FAIL'))\n"
        )
        try:
            completed = _run_in_uid_remapped_namespace(script)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn(
                "RESULT:PASS",
                completed.stdout,
                f"stdout={completed.stdout!r} stderr={completed.stderr!r}",
            )
        finally:
            if receipt.exists():
                _chown_via_user_namespace(receipt, "0:0", recursive=True)

    def test_default_retirement_floor_is_seven_days(self) -> None:
        """Pins the documented constant directly — a change here is a
        deliberate policy change, not an accident."""
        self.assertEqual(
            DEFAULT_RECEIPT_RETIREMENT_MAX_AGE_SECONDS, 7 * 24 * 60 * 60
        )

    def test_run_suite_retires_eligible_receipts_before_reaping_bundles(
        self,
    ) -> None:
        """Wiring pin: run_suite's own admitted pass must actually call
        reap_stale_final_gate_receipts, and BEFORE reap_stale_check_bundles
        — proven end-to-end through the real run_suite entry point (a
        headroom failure short-circuits before any phase runs, so this
        never executes the real suite)."""
        now = time.time()
        bundle = self.runtime / "cratedigger-checks.orphaned"
        bundle.mkdir(mode=0o700)
        summary = bundle / "summary.json"
        summary.write_text("{}\n")
        stale = now - (5 * 60 * 60)
        os.utime(summary, (stale, stale))
        receipt = self._receipt(
            "cratedigger-final-gate.old",
            age_seconds=8 * 24 * 60 * 60,
            now=now,
            terminal=True,
        )
        (receipt / "bundle").write_text(f"{bundle}\n")

        phases = (
            PhaseSpec(
                "must-not-run", (sys.executable, "-c", "pass"), "n", "generic"
            ),
        )
        stream = io.StringIO()

        with self.assertRaises(RamRootExhaustedError):
            run_suite(
                repo_root=REPO_ROOT,
                phases=phases,
                runtime_dir=self.runtime,
                stream=stream,
                min_headroom_bytes=1 << 62,
            )

        self.assertFalse(receipt.exists(), "an eligible receipt must retire")
        self.assertFalse(
            bundle.exists(),
            "its now-unprotected bundle must reap in the same admitted pass",
        )
        self.assertIn("retired 1 stale final-gate receipt(s)", stream.getvalue())


class SuiteHeadroomPreconditionTestCase(unittest.TestCase):
    """The whole suite fails once, immediately, before any phase runs (#1111)."""

    def setUp(self) -> None:
        shared = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertTrue(
            shared.is_dir(),
            "private runtime tmpfs is required for this test",
        )
        self.runtime = Path(
            tempfile.mkdtemp(dir=shared, prefix="cratedigger-headroom-test-")
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.runtime, ignore_errors=True)

    def test_insufficient_headroom_fails_before_any_phase_runs(self) -> None:
        sentinel = self.runtime / "must-not-run"
        phases = (
            PhaseSpec(
                "must-not-run",
                (
                    sys.executable,
                    "-c",
                    f"open({str(sentinel)!r}, 'w').close()",
                ),
                "must-not-run",
                "generic",
            ),
        )
        stream = io.StringIO()

        with self.assertRaises(RamRootExhaustedError) as caught:
            run_suite(
                repo_root=REPO_ROOT,
                phases=phases,
                runtime_dir=self.runtime,
                stream=stream,
                min_headroom_bytes=1 << 62,
            )

        self.assertIn(TEST_RAM_ROOT_EXHAUSTED, str(caught.exception))
        self.assertFalse(sentinel.exists(), "the whole suite must not run")
        self.assertEqual(
            list(self.runtime.glob("cratedigger-checks.*")),
            [],
            "a headroom failure must not create a bundle",
        )

    def test_default_headroom_minimum_reads_the_shared_shell_env_var(
        self,
    ) -> None:
        original = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        try:
            os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = "123456"
            self.assertEqual(_default_min_headroom_bytes(), 123456)
        finally:
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original

    def test_default_headroom_minimum_scales_with_the_expected_worker_count(
        self,
    ) -> None:
        """Issue #1131 review N1: raising the default worker count means
        more concurrent ephemeral PostgreSQL clusters, so an UNSET
        CRATEDIGGER_TEST_RAM_MIN_BYTES floor must scale with the Python
        phase's own expected worker count, not stay a flat 1 GiB
        regardless of how many workers that phase will actually spawn."""
        original_floor = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        original_jobs = os.environ.pop("CRATEDIGGER_TEST_JOBS", None)
        try:
            os.environ["CRATEDIGGER_TEST_JOBS"] = "40"
            floor = _default_min_headroom_bytes()
        finally:
            if original_floor is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original_floor
            if original_jobs is None:
                os.environ.pop("CRATEDIGGER_TEST_JOBS", None)
            else:
                os.environ["CRATEDIGGER_TEST_JOBS"] = original_jobs

        self.assertGreater(floor, DEFAULT_MIN_HEADROOM_BYTES)
        self.assertEqual(
            floor, _HEADROOM_BASE_BYTES + _HEADROOM_PER_WORKER_BYTES * 40
        )

    def test_default_headroom_minimum_never_drops_below_the_flat_floor(
        self,
    ) -> None:
        """A tiny expected worker count must not pull the floor BELOW the
        original flat 1 GiB — the per-worker term only ever raises it."""
        original_floor = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        original_jobs = os.environ.pop("CRATEDIGGER_TEST_JOBS", None)
        try:
            os.environ["CRATEDIGGER_TEST_JOBS"] = "1"
            floor = _default_min_headroom_bytes()
        finally:
            if original_floor is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original_floor
            if original_jobs is None:
                os.environ.pop("CRATEDIGGER_TEST_JOBS", None)
            else:
                os.environ["CRATEDIGGER_TEST_JOBS"] = original_jobs

        self.assertEqual(floor, DEFAULT_MIN_HEADROOM_BYTES)

    def test_explicit_headroom_override_is_not_worker_scaled(self) -> None:
        """An EXPLICIT CRATEDIGGER_TEST_RAM_MIN_BYTES override is honored
        exactly as given, even under a large expected worker count that
        would otherwise raise the floor well above it — the worker-aware
        scaling only fills in the UNSET default (issue #1131 review N1)."""
        original_floor = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        original_jobs = os.environ.pop("CRATEDIGGER_TEST_JOBS", None)
        try:
            os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = "5000000"
            os.environ["CRATEDIGGER_TEST_JOBS"] = "200"
            floor = _default_min_headroom_bytes()
        finally:
            if original_floor is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original_floor
            if original_jobs is None:
                os.environ.pop("CRATEDIGGER_TEST_JOBS", None)
            else:
                os.environ["CRATEDIGGER_TEST_JOBS"] = original_jobs

        self.assertEqual(floor, 5000000)

    def test_malformed_headroom_env_var_fails_closed(self) -> None:
        original = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        try:
            os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = "not-a-number"
            with self.assertRaises(ValueError):
                _default_min_headroom_bytes()
        finally:
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original

    def test_headroom_floor_bytes_rejects_a_worker_count_below_one(self) -> None:
        """Known-bad self-test for headroom_floor_bytes's own guard clause
        (issue #1156 item 3): every OTHER clause in this function is
        already covered indirectly via _default_min_headroom_bytes's own
        tests below, but THIS clause only trips when a caller passes a
        non-positive worker_count directly -- _default_min_headroom_bytes
        can never reach it, since _expected_worker_count always returns at
        least 1."""
        original = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        try:
            with self.assertRaisesRegex(
                ValueError, "worker_count must be at least 1"
            ):
                headroom_floor_bytes(0)
        finally:
            if original is not None:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original

    def test_headroom_floor_bytes_scales_with_the_given_worker_count(self) -> None:
        """Issue #1156 item 3: headroom_floor_bytes is the SAME formula
        _default_min_headroom_bytes wraps, but callable directly with an
        explicit worker_count -- the fuzz and world-model bursts each size
        their own floor this way rather than through the deterministic
        suite's own worker-count prediction."""
        original = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        try:
            self.assertEqual(
                headroom_floor_bytes(40),
                _HEADROOM_BASE_BYTES + _HEADROOM_PER_WORKER_BYTES * 40,
            )
            self.assertEqual(headroom_floor_bytes(1), DEFAULT_MIN_HEADROOM_BYTES)
        finally:
            if original is not None:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original

    def test_headroom_floor_bytes_honors_explicit_override_regardless_of_worker_count(
        self,
    ) -> None:
        original = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        try:
            os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = "7000000"
            self.assertEqual(headroom_floor_bytes(200), 7000000)
        finally:
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original

    def test_headroom_check_honors_the_ram_root_override(self) -> None:
        """Issue #1111 review m10: CRATEDIGGER_TEST_RAM_ROOT must be measured
        the same way scripts/test_tmpfs.sh's own shell-entry guard measures
        it — not silently ignored in favour of the passed-in runtime_dir."""
        # Matches _REAPABLE_PREFIXES' "cratedigger-headroom-test-" entry — a
        # differently-worded prefix here would silently escape reaping
        # (issue #1111 review m12).
        other = Path(
            tempfile.mkdtemp(dir=self.runtime.parent, prefix="cratedigger-headroom-test-")
        )
        original = os.environ.pop("CRATEDIGGER_TEST_RAM_ROOT", None)
        try:
            os.environ["CRATEDIGGER_TEST_RAM_ROOT"] = str(self.runtime)
            with self.assertRaises(RamRootExhaustedError) as caught:
                _check_suite_headroom(other, minimum_bytes=1 << 62)
            self.assertIn(str(self.runtime), str(caught.exception))
            self.assertNotIn(str(other), str(caught.exception))
        finally:
            shutil.rmtree(other, ignore_errors=True)
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_ROOT", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_ROOT"] = original


class JsCheckHelperTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.TemporaryDirectory(prefix="cratedigger-js-checks-")
        self.fake_bin = tempfile.TemporaryDirectory(prefix="cratedigger-js-bin-")
        self.record = Path(self.root.name) / "node.argv"
        fake_node = Path(self.fake_bin.name) / "node"
        fake_node.write_text(
            "#!/usr/bin/env bash\n"
            "printf '%s\\n' \"$*\" >> \"$CRATEDIGGER_NODE_RECORD\"\n"
            "printf 'bad: %s\\n' \"$*\" >&2\n"
            "exit 1\n",
            encoding="utf-8",
        )
        fake_node.chmod(0o755)
        (Path(self.root.name) / "web" / "js").mkdir(parents=True)
        (Path(self.root.name) / "tests").mkdir()

    def tearDown(self) -> None:
        self.fake_bin.cleanup()
        self.root.cleanup()

    def _run(self, mode: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(JS_HELPER), mode],
            cwd=self.root.name,
            env=os.environ
            | {
                "PATH": f"{self.fake_bin.name}:{os.environ['PATH']}",
                "CRATEDIGGER_REPO_ROOT": self.root.name,
                "CRATEDIGGER_NODE_RECORD": str(self.record),
            },
            text=True,
            capture_output=True,
            check=False,
        )

    def test_syntax_phase_checks_every_javascript_file_before_failing(self) -> None:
        for name in ("a.js", "b.js", "c.js"):
            (Path(self.root.name) / "web" / "js" / name).write_text(
                "bad",
                encoding="utf-8",
            )

        result = self._run("syntax")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.record.read_text().splitlines()), 3)
        self.assertEqual(result.stdout.count("CRATEDIGGER_JS_FAILURE"), 3)

    def test_real_node_rejects_invalid_es_module_syntax(self) -> None:
        (Path(self.root.name) / "web" / "js" / "bad.js").write_text(
            "export const broken = ;\n",
            encoding="utf-8",
        )

        result = subprocess.run(
            [str(JS_HELPER), "syntax"],
            cwd=self.root.name,
            env=os.environ
            | {
                "CRATEDIGGER_REPO_ROOT": self.root.name,
            },
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("CRATEDIGGER_JS_FAILURE\tweb/js/bad.js", result.stdout)
        self.assertIn("SyntaxError", result.stderr)

    def test_unit_phase_runs_every_javascript_test_before_failing(self) -> None:
        for name in ("test_js_a.mjs", "test_js_b.mjs"):
            (Path(self.root.name) / "tests" / name).write_text(
                "bad",
                encoding="utf-8",
            )

        result = self._run("unit")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.record.read_text().splitlines()), 2)
        self.assertEqual(result.stdout.count("CRATEDIGGER_JS_FAILURE"), 2)


if __name__ == "__main__":
    unittest.main()
