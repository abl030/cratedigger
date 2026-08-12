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

import msgspec

from scripts.run_python_tests import TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
from scripts.run_test_suite import (
    FAILURE_MARKER_PREFIX,
    TEST_RAM_ROOT_EXHAUSTED,
    CheckFailureMarker,
    CheckSummary,
    PhaseSpec,
    RamRootExhaustedError,
    SuiteAdmissionTimeout,
    _check_suite_headroom,
    _default_min_headroom_bytes,
    _default_phases,
    _read_lock_holder_identity,
    acquire_suite_admission,
    admission_lock_path,
    dirty_state_fingerprint,
    reap_stale_check_bundles,
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
        """Issue #1111 review M4(a): TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE must
        stay outside the "python" phase's declared failure_exit_codes, or
        run_test_suite.py's generic phase-state derivation would route it to
        an ordinary "failed" instead of "infrastructure-failure"."""
        python_phase = next(
            phase for phase in _default_phases() if phase.name == "python"
        )

        self.assertNotIn(
            TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE, python_phase.failure_exit_codes
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

    def test_read_lock_holder_identity_parses_well_formed_content(self) -> None:
        lock_path = admission_lock_path(self.runtime)
        lock_path.write_text("12345 6789\n")

        self.assertEqual(
            _read_lock_holder_identity(lock_path), "pid 12345, start ticks 6789"
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
        now = time.time()
        unrelated = self.runtime / "cratedigger-tests.unrelated"
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

    def test_headroom_check_honors_the_ram_root_override(self) -> None:
        """Issue #1111 review m10: CRATEDIGGER_TEST_RAM_ROOT must be measured
        the same way scripts/test_tmpfs.sh's own shell-entry guard measures
        it — not silently ignored in favour of the passed-in runtime_dir."""
        other = Path(
            tempfile.mkdtemp(dir=self.runtime.parent, prefix="cratedigger-other-root-")
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
