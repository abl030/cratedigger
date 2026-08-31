"""Contracts for the parallel full-suite Python runner."""

from __future__ import annotations

import contextlib
import datetime
import errno
import io
import os
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Callable, Sequence
from concurrent.futures.process import BrokenProcessPool
from dataclasses import replace
from pathlib import Path

import msgspec
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st
from hypothesis.internal.conjecture.data import Status
from hypothesis.statistics import collector

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from scripts.run_fuzz_tests import (
    aggregate_property_depth,
    is_structurally_shallow,
)
from scripts.run_python_tests import (
    AUDITED_FRONTLOAD_MODULES,
    HOTSPOT_ISOLATED_METHODS,
    HOTSPOT_SHARD_POLICIES,
    HYPOTHESIS_CASE_STATUSES,
    STRATEGY_SPACE_EXHAUSTED,
    TARGET_DURATION_CACHE_NAME,
    TEST_HOST_MEMORY_EXHAUSTED,
    TEST_HOST_MEMORY_EXHAUSTED_EXIT_CODE,
    TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
    WORLD_MODEL_MODULE,
    ChildInfrastructureError,
    ChildTargetResult,
    HypothesisPropertyStats,
    HypothesisStatsRecorder,
    RecordingTextTestResult,
    TargetInfrastructureFailure,
    TargetRunResult,
    TestModule,
    TestTarget,
    _classify_target_infrastructure_failure,
    _classify_test_infrastructure_error,
    _collapse_disk_full_failures,
    _collapse_memory_exhausted_failures,
    _default_min_memory_headroom_bytes,
    _iter_test_cases,
    _measure_available_memory_bytes,
    _measure_tempdir_available_bytes,
    _run_targets,
    assert_exact_schedule,
    assert_exact_target_coverage,
    assert_hypothesis_deadlines_disabled,
    build_test_targets,
    complete_test_modules,
    discover_test_modules,
    hotspot_targets,
    hypothesis_example_budgets,
    list_module_test_ids,
    load_target_durations,
    main,
    order_targets_by_measured_cost,
    recommended_worker_count,
    resolve_hypothesis_settings,
    schedule_modules,
    select_test_targets,
    shard_test_ids,
    store_target_durations,
    test_subprocess_environment,
    worker_environment,
)
from scripts.run_test_suite import FAILURE_MARKER_PREFIX, CheckFailureMarker
from scripts.test_substrate import TEST_RAM_ROOT_EXHAUSTED
from tests._source_pins import pinned_source

REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER = REPO_ROOT / "scripts" / "run_python_tests.py"
RUN_TESTS_SH = REPO_ROOT / "scripts" / "run_tests.sh"
RUN_SUITE = REPO_ROOT / "scripts" / "run_test_suite.py"


def _target(name: str) -> TestTarget:
    return TestTarget(
        module=TestModule(name=name, path=Path(f"tests/{name}.py"), weight=1),
        test_name=name,
    )


class TestInfrastructureFailureClassification(unittest.TestCase):
    def test_low_headroom_invalidates_a_failure_after_enospc_was_swallowed(
        self,
    ) -> None:
        classified = _classify_test_infrastructure_error(
            AssertionError("preview manifest was incomplete"),
            available_temp_bytes=1,
        )

        self.assertIsNotNone(classified)
        assert classified is not None
        self.assertEqual(classified[0], "disk_full")
        self.assertIn("1 bytes free", classified[1])
        self.assertIn("AssertionError", classified[1])

    def test_ordinary_failure_with_headroom_stays_a_property_failure(
        self,
    ) -> None:
        classified = _classify_test_infrastructure_error(
            AssertionError("real counterexample"),
            available_temp_bytes=1024 * 1024 * 1024,
        )

        self.assertIsNone(classified)

    def test_subtest_enospc_uses_the_infrastructure_channel(self) -> None:
        class CapacitySubtest(unittest.TestCase):
            def runTest(self) -> None:
                with self.subTest(stage="snapshot"):
                    raise OSError(errno.ENOSPC, "No space left on device")

        result = unittest.TextTestRunner(
            stream=io.StringIO(),
            resultclass=RecordingTextTestResult,  # pyright: ignore[reportArgumentType]
        ).run(CapacitySubtest())

        self.assertIsInstance(result, RecordingTextTestResult)
        assert isinstance(result, RecordingTextTestResult)
        self.assertEqual(len(result.infrastructure_errors or ()), 1)
        error = (result.infrastructure_errors or [])[0]
        self.assertEqual(error.kind, "disk_full")
        self.assertIn("stage='snapshot'", error.test_id)


class TestWorkerCrashClassification(unittest.TestCase):
    """A worker's own crash (issue #1111's dominant symptom) is classified
    by measuring free bytes at the moment it is caught — never by parsing
    the exception's text."""

    def test_low_measured_headroom_marks_the_failure_disk_full(self) -> None:
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            FileNotFoundError(2, "No such file or directory", "raw-output.log"),
            available_bytes=lambda: 1,
        )

        self.assertTrue(failure.disk_full)
        self.assertIn("1 bytes free", failure.detail)
        self.assertIn("FileNotFoundError", failure.detail)

    def test_ample_measured_headroom_is_an_ordinary_worker_failure(self) -> None:
        # Independent review B-6 (third round): pins minimum_bytes=0 --
        # the identical ambient-CRATEDIGGER_TEST_RAM_MIN_BYTES-coupling
        # shape B2 fixed in TestWorkerMemoryExhaustionClassification next
        # door. Without it, a large-enough ambient floor makes this
        # available_bytes=10 GiB fixture read as disk_full too.
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            RuntimeError("target subprocess exited 1: traceback"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
        )

        self.assertFalse(failure.disk_full)
        self.assertEqual(failure.detail, "RuntimeError: target subprocess exited 1: traceback")

    def test_unmeasurable_headroom_never_claims_exhaustion(self) -> None:
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            RuntimeError("boom"),
            available_bytes=lambda: None,
        )

        self.assertFalse(failure.disk_full)

    def test_default_floor_is_the_configured_one_gib_minimum_not_64mib(
        self,
    ) -> None:
        """Issue #1111 review M3: 100 MiB free is BELOW run_suite's own
        configured 1 GiB startup floor (CRATEDIGGER_TEST_RAM_MIN_BYTES) —
        real exhaustion the suite would refuse to even start under — but
        was ABOVE the old internal _MIN_VALID_TEMP_HEADROOM_BYTES (64 MiB),
        so it used to read as an ordinary worker failure.
        tests/test_decision_corpus_export.py's now-removed nested nix-shell
        subprocesses (issue #1131) used to genuinely fail in exactly this
        band mid-run.
        """
        original = os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        try:
            os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            failure = _classify_target_infrastructure_failure(
                _target("tests.test_alpha"),
                RuntimeError("target subprocess exited 1: traceback"),
                available_bytes=lambda: 100 * 1024 * 1024,
            )
        finally:
            if original is not None:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original

        self.assertTrue(failure.disk_full)
        self.assertIn("104857600 bytes free", failure.detail)

    def test_explicit_minimum_bytes_overrides_the_configured_default(
        self,
    ) -> None:
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            RuntimeError("boom"),
            available_bytes=lambda: 100 * 1024 * 1024,
            minimum_bytes=10 * 1024 * 1024,
        )

        self.assertFalse(failure.disk_full)


class TestWorkerMemoryExhaustionClassification(unittest.TestCase):
    """A worker killed by the OOM killer (issue #1156 item 2) is classified
    by exception SHAPE (BrokenProcessPool) narrowed by MEASURED system
    memory -- never a scan of the exception's text, and never triggered by
    measured state alone (unlike disk_full, which does not care what the
    exception is).

    Independent review B2 (discovered while verifying the named fix):
    every case below pins `minimum_bytes=0` explicitly. `_classify_target_
    infrastructure_failure` checks disk_full BEFORE memory, and disk_full's
    own floor falls back to the ambient CRATEDIGGER_TEST_RAM_MIN_BYTES when
    no `minimum_bytes` is given -- so without the pin, a large-enough
    ambient floor (e.g. a suite run forcing it to prove B2's OWN fix) makes
    every `available_bytes=10 GiB` fixture here read as disk_full and
    short-circuit the memory branch these tests exist to exercise. Pinning
    the disk floor to 0 makes disk_full unreachable at 10 GiB regardless of
    the ambient value, so only the memory branch under test is live."""

    def test_broken_process_pool_with_low_memory_is_memory_exhausted(
        self,
    ) -> None:
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            BrokenProcessPool("a worker was terminated abruptly"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
            available_memory_bytes=lambda: 1,
        )

        self.assertTrue(failure.memory_exhausted)
        self.assertFalse(failure.disk_full)
        self.assertIn("1 bytes available", failure.detail)
        self.assertIn("BrokenProcessPool", failure.detail)

    def test_broken_process_pool_with_ample_memory_is_ordinary(self) -> None:
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            BrokenProcessPool("a worker was terminated abruptly"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
            available_memory_bytes=lambda: 10 * 1024 * 1024 * 1024,
        )

        self.assertFalse(failure.memory_exhausted)

    def test_non_broken_process_pool_exception_is_never_memory_exhausted(
        self,
    ) -> None:
        """Known-bad self-test: proves the classifier is gated by
        exception SHAPE, not measured state alone -- a low memory reading
        must never be enough by itself."""
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            RuntimeError("ordinary worker crash, unrelated to memory"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
            available_memory_bytes=lambda: 1,
        )

        self.assertFalse(failure.memory_exhausted)

    def test_disk_full_takes_priority_over_memory_exhaustion(self) -> None:
        """The disk check short-circuits the memory one by construction
        (a deliberate ORDERING choice in the classifier, not a claim the
        two are physically distinct -- independent review B2: on this
        host's own topology the tmpfs RAM root often IS memory pressure,
        per issue #1214's own cgroup measurement)."""
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            BrokenProcessPool("a worker was terminated abruptly"),
            available_bytes=lambda: 1,
            available_memory_bytes=lambda: 1,
        )

        self.assertTrue(failure.disk_full)
        self.assertFalse(failure.memory_exhausted)

    def test_unmeasurable_memory_never_claims_exhaustion(self) -> None:
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            BrokenProcessPool("a worker was terminated abruptly"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
            available_memory_bytes=lambda: None,
        )

        self.assertFalse(failure.memory_exhausted)

    def test_default_memory_floor_reads_the_configured_env_var(self) -> None:
        original = os.environ.pop("CRATEDIGGER_TEST_MEMORY_MIN_BYTES", None)
        try:
            os.environ["CRATEDIGGER_TEST_MEMORY_MIN_BYTES"] = "123456"
            failure = _classify_target_infrastructure_failure(
                _target("tests.test_alpha"),
                BrokenProcessPool("a worker was terminated abruptly"),
                available_bytes=lambda: 10 * 1024 * 1024 * 1024,
                minimum_bytes=0,
                available_memory_bytes=lambda: 123455,
            )
        finally:
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_MEMORY_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_MEMORY_MIN_BYTES"] = original

        self.assertTrue(failure.memory_exhausted)

    def test_explicit_minimum_memory_bytes_overrides_the_configured_default(
        self,
    ) -> None:
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            BrokenProcessPool("a worker was terminated abruptly"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
            available_memory_bytes=lambda: 100 * 1024 * 1024,
            minimum_memory_bytes=10 * 1024 * 1024,
        )

        self.assertFalse(failure.memory_exhausted)

    def test_default_floor_uses_the_real_256mib_constant_not_env(self) -> None:
        """Independent review F4 (MEDIUM): every other "low memory" test in
        this class uses available_memory_bytes=lambda: 1 -- degenerate,
        since 1 is below ANY plausible floor including a mutant of
        _MIN_VALID_MEMORY_HEADROOM_BYTES collapsed to 2 (1 < 2 is still
        True, so that mutant survives every other test here). A reading
        comfortably under the real 256 MiB constant but far above a
        near-zero mutant proves the CONSTANT itself, not just the env
        override path (test_default_memory_floor_reads_the_configured_
        env_var above), gates real classification."""
        original = os.environ.pop("CRATEDIGGER_TEST_MEMORY_MIN_BYTES", None)
        try:
            failure = _classify_target_infrastructure_failure(
                _target("tests.test_alpha"),
                BrokenProcessPool("a worker was terminated abruptly"),
                available_bytes=lambda: 10 * 1024 * 1024 * 1024,
                minimum_bytes=0,
                available_memory_bytes=lambda: 200 * 1024 * 1024,
            )
        finally:
            if original is not None:
                os.environ["CRATEDIGGER_TEST_MEMORY_MIN_BYTES"] = original

        self.assertTrue(failure.memory_exhausted)

    def test_memory_exactly_at_the_floor_is_not_exhausted(self) -> None:
        """Independent review F8: boundary. The classifier uses a strict
        `<` comparison against the floor, matching the disk-full
        classifier's own convention -- `<=` would misclassify a host
        sitting exactly at the configured floor."""
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            BrokenProcessPool("a worker was terminated abruptly"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
            available_memory_bytes=lambda: 10 * 1024 * 1024,
            minimum_memory_bytes=10 * 1024 * 1024,
        )

        self.assertFalse(failure.memory_exhausted)

    def test_zero_available_memory_is_still_exhausted(self) -> None:
        """Independent review F8: boundary. 0 is a legitimate (falsy)
        MemAvailable reading -- a genuinely exhausted host -- not the
        unmeasurable (None) case; the classifier's `is not None` check
        must not be weakened to a truthiness check that would silently
        treat 0 the same as "cannot classify"."""
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            BrokenProcessPool("a worker was terminated abruptly"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
            minimum_bytes=0,
            available_memory_bytes=lambda: 0,
        )

        self.assertTrue(failure.memory_exhausted)

    def test_measure_available_memory_converts_kib_to_bytes(self) -> None:
        """Independent review F4 (MEDIUM): MemAvailable in /proc/meminfo is
        reported in KiB. Dropping the `* 1024` conversion would silently
        read kB as bytes, misclassifying every real BrokenProcessPool as
        memory-exhausted on a healthy multi-GB host -- worse than the
        disguise this whole change exists to remove. A fake fixture file
        proves the conversion since /proc/meminfo cannot be forced to a
        known value."""
        with tempfile.TemporaryDirectory() as tempdir:
            fake_meminfo = Path(tempdir) / "meminfo"
            fake_meminfo.write_text(
                "MemTotal:       32865536 kB\n"
                "MemFree:        10000000 kB\n"
                "MemAvailable:   20000000 kB\n",
                encoding="utf-8",
            )
            result = _measure_available_memory_bytes(fake_meminfo)

        self.assertEqual(result, 20000000 * 1024)

    def test_measure_available_memory_reads_memavailable_not_memfree(
        self,
    ) -> None:
        """Independent review F4 (MEDIUM): must read MemAvailable, not
        MemFree -- MemFree undercounts reclaimable page cache and would
        report a healthy host as critically low."""
        with tempfile.TemporaryDirectory() as tempdir:
            fake_meminfo = Path(tempdir) / "meminfo"
            fake_meminfo.write_text(
                "MemTotal:       32865536 kB\n"
                "MemFree:            1000 kB\n"
                "MemAvailable:   20000000 kB\n",
                encoding="utf-8",
            )
            result = _measure_available_memory_bytes(fake_meminfo)

        self.assertEqual(result, 20000000 * 1024)

    def test_measure_available_memory_reads_the_real_proc_meminfo(self) -> None:
        """The default parameter value is wired to the REAL file on this
        Linux host, not just the fixture-driven tests above."""
        result = _measure_available_memory_bytes()

        self.assertIsNotNone(result)
        assert result is not None
        self.assertGreater(result, 0)

    def test_negative_memory_floor_override_fails_closed(self) -> None:
        """Independent review B5 (MEDIUM): known-bad self-test for
        `_default_min_memory_headroom_bytes`'s negative-override guard
        clause, which shipped with no test at all -- mirroring the
        sibling disk-floor guard's own message-asserting shape."""
        original = os.environ.get("CRATEDIGGER_TEST_MEMORY_MIN_BYTES")
        try:
            os.environ["CRATEDIGGER_TEST_MEMORY_MIN_BYTES"] = "-1"
            with self.assertRaisesRegex(
                ValueError,
                "CRATEDIGGER_TEST_MEMORY_MIN_BYTES must be a "
                "non-negative integer",
            ):
                _default_min_memory_headroom_bytes()
        finally:
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_MEMORY_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_MEMORY_MIN_BYTES"] = original

    def test_non_integer_memory_floor_override_fails_closed(self) -> None:
        """A malformed (non-integer) override is coerced to the same
        fail-closed ValueError as an explicit negative one -- proving the
        `except ValueError: value = -1` fallback actually reaches the
        guard clause rather than silently returning a bogus floor."""
        original = os.environ.get("CRATEDIGGER_TEST_MEMORY_MIN_BYTES")
        try:
            os.environ["CRATEDIGGER_TEST_MEMORY_MIN_BYTES"] = "not-a-number"
            with self.assertRaisesRegex(
                ValueError,
                "CRATEDIGGER_TEST_MEMORY_MIN_BYTES must be a "
                "non-negative integer",
            ):
                _default_min_memory_headroom_bytes()
        finally:
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_MEMORY_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_MEMORY_MIN_BYTES"] = original


class TestCollapseDiskFullFailures(unittest.TestCase):
    """Issue #1111 item 2: N disk-full-classified failures fold into ONE
    named failure-index entry; anything else is reported unchanged."""

    def test_no_disk_full_signal_passes_everything_through_unchanged(self) -> None:
        result = TargetRunResult(
            target=_target("tests.test_alpha"),
            worker_pid=1,
            successful=False,
            tests_run=3,
            elapsed_seconds=0.1,
            output="",
            failed_test_ids=("tests.test_alpha.Alpha.test_real_bug",),
        )
        infra = TargetInfrastructureFailure(
            target=_target("tests.test_beta"), detail="segfault"
        )

        remaining_results, remaining_infra, marker = _collapse_disk_full_failures(
            [result], [infra]
        )

        self.assertEqual(remaining_results, (result,))
        self.assertEqual(remaining_infra, (infra,))
        self.assertIsNone(marker)

    def test_whole_target_worker_crash_is_dropped_and_folded_into_one_marker(
        self,
    ) -> None:
        crashed = TargetInfrastructureFailure(
            target=_target("tests.test_alpha"),
            detail="FileNotFoundError: raw-output.log",
            disk_full=True,
        )
        also_crashed = TargetInfrastructureFailure(
            target=_target("tests.test_beta"),
            detail="temporary filesystem has 1 bytes free; RuntimeError: exited 1",
            disk_full=True,
        )

        remaining_results, remaining_infra, marker = _collapse_disk_full_failures(
            [], [crashed, also_crashed]
        )

        self.assertEqual(remaining_results, ())
        self.assertEqual(remaining_infra, ())
        self.assertIsNotNone(marker)
        assert marker is not None
        self.assertEqual(marker.identity, TEST_RAM_ROOT_EXHAUSTED)
        self.assertEqual(
            set(marker.test_ids),
            {"tests.test_alpha", "tests.test_beta"},
        )

    def test_a_mixed_target_keeps_its_real_failures_and_strips_only_disk_full_ones(
        self,
    ) -> None:
        result = TargetRunResult(
            target=_target("tests.test_alpha"),
            worker_pid=1,
            successful=False,
            tests_run=2,
            elapsed_seconds=0.1,
            output="",
            failed_test_ids=(
                "tests.test_alpha.Alpha.test_real_bug",
                "tests.test_alpha.Alpha.test_hit_enospc",
            ),
            infrastructure_errors=(
                ChildInfrastructureError(
                    test_id="tests.test_alpha.Alpha.test_hit_enospc",
                    kind="disk_full",
                    detail="OSError: No space left on device",
                ),
            ),
        )

        remaining_results, remaining_infra, marker = _collapse_disk_full_failures(
            [result], []
        )

        self.assertEqual(len(remaining_results), 1)
        self.assertEqual(
            remaining_results[0].failed_test_ids,
            ("tests.test_alpha.Alpha.test_real_bug",),
        )
        self.assertEqual(remaining_infra, ())
        self.assertIsNotNone(marker)
        assert marker is not None
        self.assertEqual(
            marker.test_ids, ("tests.test_alpha.Alpha.test_hit_enospc",)
        )

    def test_a_target_whose_only_failures_are_disk_full_is_dropped_entirely(
        self,
    ) -> None:
        result = TargetRunResult(
            target=_target("tests.test_alpha"),
            worker_pid=1,
            successful=False,
            tests_run=1,
            elapsed_seconds=0.1,
            output="",
            failed_test_ids=("tests.test_alpha.Alpha.test_hit_enospc",),
            infrastructure_errors=(
                ChildInfrastructureError(
                    test_id="tests.test_alpha.Alpha.test_hit_enospc",
                    kind="disk_full",
                    detail="OSError: No space left on device",
                ),
            ),
        )

        remaining_results, _remaining_infra, marker = _collapse_disk_full_failures(
            [result], []
        )

        self.assertEqual(remaining_results, ())
        self.assertIsNotNone(marker)

    def test_database_unavailable_is_never_folded_into_the_ram_root_bucket(
        self,
    ) -> None:
        result = TargetRunResult(
            target=_target("tests.test_alpha"),
            worker_pid=1,
            successful=False,
            tests_run=1,
            elapsed_seconds=0.1,
            output="",
            failed_test_ids=("tests.test_alpha.Alpha.test_flaky_db",),
            infrastructure_errors=(
                ChildInfrastructureError(
                    test_id="tests.test_alpha.Alpha.test_flaky_db",
                    kind="database_unavailable",
                    detail="OperationalError: could not connect",
                ),
            ),
        )

        remaining_results, _remaining_infra, marker = _collapse_disk_full_failures(
            [result], []
        )

        self.assertEqual(remaining_results, (result,))
        self.assertIsNone(marker)


class TestCollapseMemoryExhaustedFailures(unittest.TestCase):
    """Issue #1156 item 2: N OOM-classified worker deaths fold into ONE
    named failure-index entry, mirroring TestCollapseDiskFullFailures for
    a different resource; anything else passes through unchanged."""

    def test_no_memory_signal_passes_everything_through_unchanged(self) -> None:
        infra = TargetInfrastructureFailure(
            target=_target("tests.test_beta"), detail="segfault"
        )

        remaining, marker = _collapse_memory_exhausted_failures([infra])

        self.assertEqual(remaining, (infra,))
        self.assertIsNone(marker)

    def test_memory_exhausted_worker_deaths_fold_into_one_marker(self) -> None:
        crashed = TargetInfrastructureFailure(
            target=_target("tests.test_gamma"),
            detail="BrokenProcessPool: a worker was terminated abruptly",
            memory_exhausted=True,
        )
        also_crashed = TargetInfrastructureFailure(
            target=_target("tests.test_delta"),
            detail="BrokenProcessPool: a worker was terminated abruptly",
            memory_exhausted=True,
        )

        remaining, marker = _collapse_memory_exhausted_failures(
            [crashed, also_crashed]
        )

        self.assertEqual(remaining, ())
        self.assertIsNotNone(marker)
        assert marker is not None
        self.assertEqual(marker.identity, TEST_HOST_MEMORY_EXHAUSTED)
        self.assertEqual(
            set(marker.test_ids), {"tests.test_gamma", "tests.test_delta"}
        )

    def test_disk_full_failures_are_not_folded_by_this_function(self) -> None:
        disk_full_failure = TargetInfrastructureFailure(
            target=_target("tests.test_epsilon"), detail="disk", disk_full=True
        )

        remaining, marker = _collapse_memory_exhausted_failures(
            [disk_full_failure]
        )

        self.assertEqual(remaining, (disk_full_failure,))
        self.assertIsNone(marker)

    def test_disk_full_and_memory_exhausted_are_each_folded_exactly_once(
        self,
    ) -> None:
        """Independent review F9: main()'s wiring MUST feed
        _collapse_memory_exhausted_failures the DISK-FULL-FILTERED
        remainder from _collapse_disk_full_failures, never the raw
        infrastructure_failures list. Feeding the raw list would leave a
        disk_full failure in BOTH ram_root_marker's folded set AND the
        "remaining" list this composition proves must be empty --
        double-reporting the same failure under two different markers
        while ALSO printing it a third time as an ordinary per-target
        detail."""
        disk_full_failure = TargetInfrastructureFailure(
            target=_target("tests.test_disk"), detail="disk", disk_full=True
        )
        memory_failure = TargetInfrastructureFailure(
            target=_target("tests.test_memory"),
            detail="BrokenProcessPool: a worker was terminated abruptly",
            memory_exhausted=True,
        )

        _remaining_results, remaining_infra, ram_root_marker = (
            _collapse_disk_full_failures([], [disk_full_failure, memory_failure])
        )
        remaining_infra, memory_marker = _collapse_memory_exhausted_failures(
            remaining_infra
        )

        self.assertEqual(remaining_infra, ())
        self.assertIsNotNone(ram_root_marker)
        self.assertIsNotNone(memory_marker)
        assert ram_root_marker is not None and memory_marker is not None
        self.assertEqual(ram_root_marker.test_ids, ("tests.test_disk",))
        self.assertEqual(memory_marker.test_ids, ("tests.test_memory",))


class TestRunTargetsWorkerExceptionWiring(unittest.TestCase):
    """`_run_targets` really delegates to the measured classifier (#1111),
    with the floor controlled through the SAME configured
    ``CRATEDIGGER_TEST_RAM_MIN_BYTES`` override production honors
    (`scripts/test_substrate.py::default_min_headroom_bytes`, read
    verbatim including ``0``) — the identical env-var idiom
    `tests/test_test_tmpfs.py` already uses, set for the duration and
    restored in a ``finally`` (#1208 item 2).

    The prior shape drove `_run_targets` unmodified but asserted
    ``disk_full`` against a LIVE measurement of the ambient, shared
    test-RAM root at the default (unset) floor — its own comment admitted
    the assumption ("ample real headroom on the host running this test").
    On 2026-08-19, 1.25G of leaked sibling scratch from OOM-killed suite
    runs (#1208 item 1) made that assumption false and failed this test on
    three consecutive otherwise-green gate runs, while it passed in
    isolation with no code changed in between. A first fix injected a
    ``classify_infrastructure_failure`` kwarg-DI seam into `_run_targets`
    with the production classifier as its default — but review found that
    seam left the module's own default binding completely UNPINNED: a
    planted always-``disk_full=True`` default classifier survived the
    whole module undetected, since no test exercised `_run_targets`
    without overriding the seam.

    This design needs no production change at all. Instead of injecting a
    replacement classifier, both tests below set
    ``CRATEDIGGER_TEST_RAM_MIN_BYTES`` to a floor chosen so that ANY real
    measurement of the host's actual free bytes falls on the same side of
    the comparison — deterministic under arbitrary shared-root pressure —
    while `_run_targets`, the real classifier, the real live measurement,
    and the real default `available_bytes` binding all run completely
    unmodified and genuinely under test. The headroom really is
    live-measured here; the test name stands.
    """

    def _classify_worker_mismatch(self) -> TargetInfrastructureFailure:
        """Run one real worker through `_run_targets`'s exception path and
        return the sole resulting `TargetInfrastructureFailure`.

        The trigger is a real, deterministic parent-side exception (no
        disk pressure needed): the child legitimately reports its own real
        test IDs, which differ from the fabricated expectation below, so
        the parent's own mismatch guard raises before returning.
        """
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "fixture_tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            (tests_dir / "test_alpha.py").write_text(
                "import unittest\n\n"
                "class Alpha(unittest.TestCase):\n"
                "    def test_ok(self):\n"
                "        pass\n",
                encoding="utf-8",
            )
            module = TestModule(
                name="fixture_tests.test_alpha",
                path=tests_dir / "test_alpha.py",
                weight=1,
            )
            target = TestTarget(
                module=module,
                test_name="fixture_tests.test_alpha",
                expected_test_ids=(
                    "fixture_tests.test_alpha.Alpha.test_bogus",
                ),
            )

            results, infrastructure_failures = _run_targets(
                (target,),
                worker_count=1,
                top_level_directory=root,
                durations=0,
            )

        self.assertEqual(results, ())
        self.assertEqual(len(infrastructure_failures), 1)
        return infrastructure_failures[0]

    def _with_configured_floor(self, raw_floor: str) -> TargetInfrastructureFailure:
        original = os.environ.get("CRATEDIGGER_TEST_RAM_MIN_BYTES")
        os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = raw_floor
        try:
            return self._classify_worker_mismatch()
        finally:
            if original is None:
                os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
            else:
                os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = original

    def test_a_real_worker_exception_is_classified_by_live_measured_headroom(
        self,
    ) -> None:
        """Case A: floor "0" -- ``available < 0`` is False for every real
        measurement, and for an unmeasurable ``None`` (`available is not
        None and available < floor` short-circuits) -- so `disk_full` is
        deterministically False."""
        failure = self._with_configured_floor("0")

        self.assertIn("unexpected test IDs", failure.detail)
        self.assertFalse(failure.disk_full)

    def test_a_floor_far_above_any_real_measurement_marks_it_disk_full(
        self,
    ) -> None:
        """Case B: floor huge (10**18 bytes, ~888 PB) -- any real
        measurement of an actual host's free bytes is below it, so
        `disk_full` is deterministically True and the detail carries the
        measured-bytes prefix. This pins the PROPAGATION direction: a
        mutant that resets `disk_full` back to False before the failure is
        appended in `_run_targets` survives Case A (which already expects
        False) but flips this one."""
        failure = self._with_configured_floor("1000000000000000000")

        self.assertIn("unexpected test IDs", failure.detail)
        self.assertTrue(failure.disk_full)
        self.assertIn("bytes free", failure.detail)


class TestMeasureTempdirAvailableBytesTracksDiskUsage(unittest.TestCase):
    """The module's only real-input coverage of
    `_measure_tempdir_available_bytes` was through classifier tests that
    inject a fake `available_bytes` callable, so the measurer's own body
    (`shutil.disk_usage(tempfile.gettempdir()).free`) had no direct guard
    -- near-vacuous against a `.free` -> `.total` mutant (#1208 item 2
    review, F3).

    A static tolerance-window comparison against one live
    `shutil.disk_usage(...).free` read is NOT reliable evidence here:
    measured on this repository's own shared test-RAM root, `total -
    free` (bytes actually in use) was ~5.5 MiB at authoring time --
    comfortably inside any "generous" tolerance such as 64 MiB, so a
    `.total` mutant reports a value indistinguishable from `.free` within
    that window and the mutant SURVIVES (confirmed empirically: planted,
    ran, green). This test instead forces a real, deterministic drop in
    free space by writing real data, and asserts the measurer tracks that
    drop -- `.total` is a static filesystem property that never moves when
    disk CONTENTS change, so the mutant cannot survive regardless of
    ambient occupancy.
    """

    def test_measured_value_tracks_a_real_consumption_delta(self) -> None:
        """Issue #1229: retried, because the quantity under test is SHARED.

        The original single-shot form raced the suite's other 21 workers on
        the one test RAM root and failed intermittently in a real gate
        (`3235840 not greater than or equal to 16777216`): a sibling
        RELEASING space inside the measurement window offsets this test's
        own payload, and no fixed threshold can absorb that, since a
        sibling freeing a whole scratch tree can dwarf any payload this
        test is willing to write. Widening the tolerance would have been
        the wrong fix -- it trades the mutant kill away to buy quiet.

        Retrying keeps the kill exactly as strong while removing the race.
        The `.total` mutant is a STATIC filesystem property: it never moves
        when contents change, so it fails every attempt, however many are
        allowed. Only the correct `.free` field can ever produce the drop,
        and it needs just one window that no sibling happens to free space
        inside.
        """
        payload_bytes = 32 * 1024 * 1024
        observed: list[int] = []

        for _ in range(8):
            before = _measure_tempdir_available_bytes()
            self.assertIsNotNone(before)
            assert before is not None

            with tempfile.NamedTemporaryFile(dir=tempfile.gettempdir()) as scratch:
                scratch.write(b"\0" * payload_bytes)
                scratch.flush()
                os.fsync(scratch.fileno())

                after = _measure_tempdir_available_bytes()

            self.assertIsNotNone(after)
            assert after is not None
            drop = before - after
            observed.append(drop)
            if drop >= payload_bytes // 2:
                return

        self.fail(
            "free bytes never tracked a real 32 MiB write across 8 attempts; "
            f"observed drops: {observed}"
        )


class TestMeasuredCostOrdering(unittest.TestCase):
    """Issue #1229: the duration cache is a scheduling HINT.

    Every clause below is about ordering or about degrading safely. None of
    them can change WHICH targets run — `assert_exact_target_schedule`
    compares by name as a set, so coverage is order-insensitive by
    construction and is covered by its own tests.
    """

    @staticmethod
    def _target(name: str) -> TestTarget:
        return TestTarget(TestModule(name, Path(f"/{name}.py"), 1), name)

    def _schedule(self, *names: str) -> tuple[TestTarget, ...]:
        return tuple(self._target(name) for name in names)

    def test_longest_measured_target_is_admitted_first(self) -> None:
        schedule = self._schedule("cheap", "dear", "middling")
        ordered = order_targets_by_measured_cost(
            schedule, {"cheap": 0.5, "dear": 40.0, "middling": 4.0}
        )

        self.assertEqual(
            [target.test_name for target in ordered],
            ["dear", "middling", "cheap"],
        )

    def test_an_unknown_target_is_admitted_before_every_known_one(self) -> None:
        """The fail-safe direction. An unknown target is new, renamed, or
        resharded; treating it as cheap would risk re-creating exactly the
        late-admitted tail this ordering exists to remove."""
        schedule = self._schedule("dear", "brand-new")
        ordered = order_targets_by_measured_cost(schedule, {"dear": 40.0})

        self.assertEqual(
            [target.test_name for target in ordered], ["brand-new", "dear"]
        )

    def test_an_empty_cache_leaves_the_heuristic_order_untouched(self) -> None:
        """Cold start must be exactly the pre-#1229 behaviour, not a
        reshuffle. There is no short-circuit for this case: every target is
        unknown, so every sort key is equal and the stable sort returns the
        incoming order. This drives the real sort, not a guard around it."""
        schedule = self._schedule("first", "second", "third")

        self.assertEqual(order_targets_by_measured_cost(schedule, {}), schedule)

    def test_equal_costs_preserve_the_incoming_order(self) -> None:
        schedule = self._schedule("alpha", "beta", "gamma")
        ordered = order_targets_by_measured_cost(
            schedule, {"alpha": 2.0, "beta": 2.0, "gamma": 2.0}
        )

        self.assertEqual(ordered, schedule)

    def test_durations_round_trip_through_the_cache_file(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            runtime = Path(raw)
            store_target_durations(
                runtime,
                (
                    TargetRunResult(
                        target=self._target("dear"),
                        worker_pid=1,
                        successful=True,
                        tests_run=1,
                        elapsed_seconds=40.25,
                        output="",
                        failed_test_ids=(),
                    ),
                ),
            )

            self.assertEqual(load_target_durations(runtime), {"dear": 40.25})

    def test_a_partial_run_refines_rather_than_discards_the_cache(self) -> None:
        """A `--test`-selected run measures a handful of targets. Replacing
        the file would throw away every other target's timing and silently
        return the next full run to cold-start ordering."""
        with tempfile.TemporaryDirectory() as raw:
            runtime = Path(raw)
            (runtime / TARGET_DURATION_CACHE_NAME).write_bytes(
                b'{"untouched": 12.0, "dear": 1.0}'
            )
            store_target_durations(
                runtime,
                (
                    TargetRunResult(
                        target=self._target("dear"),
                        worker_pid=1,
                        successful=True,
                        tests_run=1,
                        elapsed_seconds=40.0,
                        output="",
                        failed_test_ids=(),
                    ),
                ),
            )

            self.assertEqual(
                load_target_durations(runtime),
                {"untouched": 12.0, "dear": 40.0},
            )

    def test_an_unreadable_or_corrupt_cache_degrades_to_no_ordering(self) -> None:
        """Known-bad self-test, one world per degradation clause: this file
        is written by a previous process, so nothing in it may ever be able
        to fail a run."""
        with tempfile.TemporaryDirectory() as raw:
            runtime = Path(raw)

            # 1. absent
            self.assertEqual(load_target_durations(runtime), {})

            # 2. not JSON at all (e.g. a truncated or clobbered write)
            path = runtime / TARGET_DURATION_CACHE_NAME
            path.write_bytes(b"{not json")
            self.assertEqual(load_target_durations(runtime), {})

            # 3. valid JSON of the wrong shape
            path.write_bytes(b'{"dear": "forty"}')
            self.assertEqual(load_target_durations(runtime), {})

            # 4. a negative duration is not a measurement
            path.write_bytes(b'{"dear": -1.0, "real": 2.0}')
            self.assertEqual(load_target_durations(runtime), {"real": 2.0})

    def test_storing_nothing_never_creates_a_cache(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            runtime = Path(raw)
            store_target_durations(runtime, ())

            self.assertFalse((runtime / TARGET_DURATION_CACHE_NAME).exists())


class TestModuleDiscovery(unittest.TestCase):
    def test_discovers_recursive_test_modules_in_stable_order(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "fixture_tests"
            web_dir = tests_dir / "web"
            hidden_dir = tests_dir / "__pycache__"
            web_dir.mkdir(parents=True)
            hidden_dir.mkdir()
            for package in (tests_dir, web_dir):
                (package / "__init__.py").write_text("", encoding="utf-8")
            (tests_dir / "test_zed.py").write_text("# zed\n", encoding="utf-8")
            (web_dir / "test_alpha.py").write_text(
                "# alpha\n# second line\n", encoding="utf-8"
            )
            (tests_dir / "helper.py").write_text("# helper\n", encoding="utf-8")
            (hidden_dir / "test_stale.py").write_text("# stale\n", encoding="utf-8")

            modules = discover_test_modules(tests_dir, root, "test*.py")

        self.assertEqual(
            [(module.name, module.weight) for module in modules],
            [
                ("fixture_tests.test_zed", 1),
                ("fixture_tests.web.test_alpha", 2),
            ],
        )

    def test_only_the_audited_nix_straggler_is_frontloaded(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            (tests_dir / "test_nix_module.py").write_text("# nix\n", encoding="utf-8")
            (tests_dir / "test_ordinary.py").write_text("# ordinary\n", encoding="utf-8")

            modules = discover_test_modules(tests_dir, root, "test*.py")

        self.assertEqual(
            {module.name: module.frontload for module in modules},
            {
                "tests.test_nix_module": True,
                "tests.test_ordinary": False,
            },
        )


class TestModuleScheduling(unittest.TestCase):
    def test_worker_policy_scales_with_host_without_chasing_diminishing_returns(
        self,
    ) -> None:
        self.assertEqual(recommended_worker_count(1), 1)
        self.assertEqual(recommended_worker_count(8), 6)
        self.assertEqual(recommended_worker_count(16), 12)
        self.assertEqual(recommended_worker_count(30), 22)
        self.assertEqual(recommended_worker_count(64), 48)

    def test_worker_policy_has_no_fixed_ceiling(self) -> None:
        """Issue #1131: the prior flat DEFAULT_MAX_WORKERS=12 ceiling
        predates issue #1111's admission control + headroom precondition
        and this issue's own ephemeral-PostgreSQL RAM/tmpfs diet, both of
        which make an unbounded proportional formula safe. A much larger
        host must not be silently clamped back down to a small constant —
        a known-bad mutant reintroducing ``min(12, ...)`` must fail this."""
        self.assertEqual(recommended_worker_count(128), 96)

    def test_generated_first_schedule_is_exact_and_deterministic(self) -> None:
        modules = tuple(
            TestModule(name, Path(f"/{name}.py"), weight)
            for name, weight in (
                ("a", 10),
                ("b_generated", 1),
                ("c", 8),
                ("d_generated", 2),
            )
        )

        schedule = schedule_modules(modules)

        assert_exact_schedule(modules, schedule)
        self.assertEqual(
            tuple(module.name for module in schedule),
            ("d_generated", "b_generated", "a", "c"),
        )
        self.assertEqual(schedule_modules(modules), schedule)

    def test_exact_schedule_checker_rejects_duplicate_module(self) -> None:
        module = TestModule("a", Path("/a.py"), 1)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            assert_exact_schedule((module,), (module, module))

    def test_exact_schedule_checker_rejects_missing_module(self) -> None:
        module = TestModule("a", Path("/a.py"), 1)
        with self.assertRaisesRegex(ValueError, "missing"):
            assert_exact_schedule((module,), ())

    def test_worker_environment_forces_private_database_bootstrap(self) -> None:
        env = worker_environment(
            {
                "PATH": "/bin",
                "TEST_DB_DSN": "postgresql://shared",
                "CRATEDIGGER_TEST_SCHEMA_READY": "1",
            },
            worker_index=3,
        )

        self.assertNotIn("TEST_DB_DSN", env)
        self.assertNotIn("CRATEDIGGER_TEST_SCHEMA_READY", env)
        self.assertEqual(env["CRATEDIGGER_TEST_WORKER"], "3")
        self.assertEqual(env["PATH"], "/bin")

    def test_audited_hotspots_split_at_the_narrowest_safe_boundary(self) -> None:
        # Issue #1226 added the four measured-heavy, late-admitted modules
        # below. They are frontloaded for SCHEDULING ONLY — unlike
        # tests.test_nix_module they carry no sharding policy at all, so
        # nothing about how they are split changes; only when they enter
        # the queue does. See AUDITED_FRONTLOAD_MODULES' own comment in
        # scripts/run_python_tests.py for the measured ordering loss.
        self.assertEqual(
            AUDITED_FRONTLOAD_MODULES,
            frozenset({
                "tests.test_nix_module",
                "tests.test_world_model_coordinator",
                "tests.test_fuzz_burst",
                "tests.test_targeted_test_selection",
                "tests.test_aac_lattice",
            }),
        )
        # The scheduling-only additions must stay scheduling-only: a
        # sharding policy or isolated-method carve-out for one of them
        # would be a different, unaudited change wearing this one's name.
        for module_name in AUDITED_FRONTLOAD_MODULES - {"tests.test_nix_module"}:
            self.assertNotIn(module_name, HOTSPOT_SHARD_POLICIES)
            self.assertNotIn(module_name, HOTSPOT_ISOLATED_METHODS)
        # tests.test_nix_module is frontloaded but deliberately NOT
        # method_batch-sharded (issue #1131 review round 2): its nix-eval
        # tests are cost-grouped into three exception-memoizing cached
        # helpers, which only pay off when every one of a group's
        # consumers runs in the same worker process. A blind method_batch
        # split could land more than one of the module's heavy nix-eval
        # methods in one batch (up to 5, main's own scheme); a naive full
        # unshard (this issue's own round 1) serializes every merged world
        # onto one target instead (measured: main's own worst-case
        # bin-packed batch is 61.6s, a full unshard is 118.3s).
        # HOTSPOT_ISOLATED_METHODS below is the narrower fix: originally
        # (issue #1131) carved the single heaviest consumer into its own
        # target, bundling everything else into one remainder target — at
        # most TWO concurrent heavy nix-eval subprocesses instead of up to
        # five, which is what made raising the suite's worker count
        # affordable in the first place (main's own worker-count sweep
        # shows this module's pole inflating hard with concurrency: 88.0s
        # at 8 workers, 122.7s at 12, 147.7s at 16, 152.3s at 20). Issue
        # #1156 item 1 split that single heaviest consumer's own world
        # matrix into two roughly-balanced halves (below), raising the count
        # to THREE — measured (three interleaved baseline/candidate full-
        # suite pairs) as a real, if modest, net win: the module's worst
        # single target dropped from a mean of 105.9s to 92.4s (-13%,
        # every pair individually improved, no runaway blowup), and the
        # suite's python-phase wall time moved from a mean of 118.6s to
        # 113.8s (-4%, noisier — baseline alone spanned 107.5-137.0s from
        # ambient host contention). See
        # ``_shared_module_worlds_web_auth_matrix_part1``'s docstring in
        # ``tests/test_nix_module.py`` for the full numbers.
        self.assertEqual(
            HOTSPOT_SHARD_POLICIES,
            {
                "tests.test_beets_destructive_configs_generated": "method_batch",
                "tests.test_deploy_pin_generated": "method_batch",
                "tests.test_deploy_pin_script": "method_batch",
                "tests.test_pipeline_db": "class_batch",
            },
        )
        self.assertEqual(
            HOTSPOT_ISOLATED_METHODS,
            {
                "tests.test_nix_module": frozenset({
                    (
                        "tests.test_nix_module.TestWebAuthenticationModuleContract."
                        "test_basic_and_insecure_mode_matrix_is_evaluated_part1"
                    ),
                    (
                        "tests.test_nix_module.TestWebAuthenticationModuleContract."
                        "test_basic_and_insecure_mode_matrix_is_evaluated_part2"
                    ),
                }),
            },
        )

    def test_isolated_method_ids_are_real_discovered_tests(self) -> None:
        """Dev-loop drift signal, not a suite-level guard: hotspot_targets'
        own runtime check (an isolated ID missing from the real discovery
        manifest) already fails the suite closed during scheduling, before
        this test would ever run in a normal `scripts/test.sh` or
        `run_tests.sh` invocation. This pin exists so a direct
        `unittest tests.test_parallel_test_runner` run (or an isolated
        rerun of just this test) also catches a renamed/removed isolated
        test id quickly, without needing a full scheduling pass.
        """
        for module_name, isolated in HOTSPOT_ISOLATED_METHODS.items():
            discovered = {
                test.id()
                for test in _iter_test_cases(
                    unittest.defaultTestLoader.loadTestsFromName(module_name)
                )
            }
            missing = isolated - discovered
            self.assertEqual(
                missing,
                set(),
                f"{module_name}: isolated test id(s) no longer exist: {missing}",
            )

    def test_class_batching_is_exact_and_bounds_repeated_imports(self) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = tuple(
            f"{module.name}.Test{class_index}.test_{test_index}"
            for class_index in range(12)
            for test_index in range((class_index % 4) + 1)
        )

        targets = shard_test_ids(module, test_ids, granularity="class_batch")

        assert_exact_target_coverage(module, test_ids, targets)
        self.assertEqual(len(targets), 8)
        self.assertLessEqual(
            max(len(target.expected_test_ids) for target in targets)
            - min(len(target.expected_test_ids) for target in targets),
            1,
        )

    def test_method_sharding_is_exact(self) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = (
            "tests.test_hotspot.TestCases.test_one",
            "tests.test_hotspot.TestCases.test_two",
        )

        targets = shard_test_ids(module, test_ids, granularity="method")

        assert_exact_target_coverage(module, test_ids, targets)
        self.assertEqual(
            tuple(target.test_name for target in targets),
            test_ids,
        )

    def test_isolated_method_gets_its_own_target_and_bundles_the_remainder(
        self,
    ) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = (
            "tests.test_hotspot.TestCases.test_cheap_one",
            "tests.test_hotspot.TestCases.test_cheap_two",
            "tests.test_hotspot.TestCases.test_expensive",
        )
        isolated = frozenset({"tests.test_hotspot.TestCases.test_expensive"})

        targets = hotspot_targets(
            module, test_ids, granularity=None, isolated=isolated
        )

        assert_exact_target_coverage(module, test_ids, targets)
        self.assertEqual(len(targets), 2)
        isolated_target = next(
            t for t in targets if t.expected_test_ids == (
                "tests.test_hotspot.TestCases.test_expensive",
            )
        )
        self.assertEqual(
            isolated_target.test_name,
            "tests.test_hotspot.TestCases.test_expensive",
        )
        self.assertEqual(
            isolated_target.load_names,
            ("tests.test_hotspot.TestCases.test_expensive",),
        )
        remainder_target = next(t for t in targets if t is not isolated_target)
        self.assertEqual(
            set(remainder_target.expected_test_ids),
            {
                "tests.test_hotspot.TestCases.test_cheap_one",
                "tests.test_hotspot.TestCases.test_cheap_two",
            },
        )
        self.assertEqual(remainder_target.load_names, remainder_target.expected_test_ids)

    def test_isolation_composes_with_granularity_on_the_remainder(self) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = (
            "tests.test_hotspot.TestCases.test_cheap_one",
            "tests.test_hotspot.TestCases.test_cheap_two",
            "tests.test_hotspot.TestCases.test_expensive",
        )
        isolated = frozenset({"tests.test_hotspot.TestCases.test_expensive"})

        targets = hotspot_targets(
            module, test_ids, granularity="method", isolated=isolated
        )

        assert_exact_target_coverage(module, test_ids, targets)
        self.assertEqual(len(targets), 3)
        # A bare target COUNT can't distinguish this from isolation being
        # disabled entirely: "method" granularity already puts every test
        # in its own target, so 1 isolated + 2 method shards and 3 plain
        # method shards both total 3 targets with the same expected_test_ids.
        # The real discriminator is load_names: hotspot_targets sets it
        # explicitly for the isolated singleton, while shard_test_ids's
        # own plain (non-batch) branch leaves it empty (falls back to
        # test_name at load time) — so under isolation-disabled, the
        # target covering the "expensive" ID would have load_names == ().
        isolated_target = next(
            t for t in targets
            if t.expected_test_ids == ("tests.test_hotspot.TestCases.test_expensive",)
        )
        self.assertEqual(isolated_target.load_names, isolated_target.expected_test_ids)
        for other in targets:
            if other is not isolated_target:
                self.assertEqual(other.load_names, ())

    def test_hotspot_targets_rejects_an_unknown_isolated_id(self) -> None:
        """Known-bad self-test: an isolated ID that drifted out of the
        discovered set must fail closed, never silently drop coverage.
        """
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = ("tests.test_hotspot.TestCases.test_one",)
        isolated = frozenset({"tests.test_hotspot.TestCases.test_renamed"})

        with self.assertRaisesRegex(ValueError, "unknown isolated test id"):
            hotspot_targets(module, test_ids, granularity=None, isolated=isolated)

    def test_build_test_targets_applies_isolated_methods(self) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = (
            "tests.test_hotspot.TestCases.test_cheap",
            "tests.test_hotspot.TestCases.test_expensive",
        )
        isolated = frozenset({"tests.test_hotspot.TestCases.test_expensive"})

        targets = build_test_targets(
            (module,),
            {module.name: test_ids},
            isolated_methods={module.name: isolated},
        )

        assert_exact_target_coverage(module, test_ids, targets)
        self.assertEqual(
            {target.test_name for target in targets},
            {
                "tests.test_hotspot.TestCases.test_expensive",
                "tests.test_hotspot::remainder",
            },
        )

    def test_selected_module_keeps_the_canonical_hotspot_sharding(self) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = (
            "tests.test_hotspot.TestCases.test_one",
            "tests.test_hotspot.TestCases.test_two",
        )

        targets = select_test_targets(
            (module,),
            (module.name,),
            listed_test_ids={module.name: test_ids},
            hotspot_policies={module.name: "method"},
        )

        self.assertEqual(tuple(target.test_name for target in targets), test_ids)

    def test_selected_module_applies_isolated_methods_with_no_shard_policy(
        self,
    ) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 90)
        test_ids = (
            "tests.test_hotspot.TestCases.test_cheap",
            "tests.test_hotspot.TestCases.test_expensive",
        )
        isolated = frozenset({"tests.test_hotspot.TestCases.test_expensive"})

        targets = select_test_targets(
            (module,),
            (module.name,),
            listed_test_ids={module.name: test_ids},
            hotspot_policies={},
            hotspot_isolated_methods={module.name: isolated},
        )

        self.assertEqual(
            {target.test_name for target in targets},
            {
                "tests.test_hotspot.TestCases.test_expensive",
                "tests.test_hotspot::remainder",
            },
        )

    def test_selected_method_runs_only_that_exact_unittest_name(self) -> None:
        module = TestModule("tests.test_alpha", Path("/test_alpha.py"), 10)
        selector = "tests.test_alpha.TestCases.test_one"

        targets = select_test_targets((module,), (selector,))

        self.assertEqual(
            targets,
            (
                TestTarget(
                    module=module,
                    test_name=selector,
                    load_names=(selector,),
                ),
            ),
        )

    def test_selected_module_subsumes_duplicate_and_method_selectors(self) -> None:
        module = TestModule("tests.test_alpha", Path("/test_alpha.py"), 10)

        targets = select_test_targets(
            (module,),
            (
                "tests.test_alpha.TestCases.test_one",
                "tests.test_alpha",
                "tests.test_alpha",
            ),
        )

        self.assertEqual(targets, (TestTarget(module, module.name),))

    def test_unknown_selected_test_fails_before_workers_start(self) -> None:
        module = TestModule("tests.test_alpha", Path("/test_alpha.py"), 10)

        with self.assertRaisesRegex(ValueError, "unknown test selector"):
            select_test_targets((module,), ("tests.test_missing",))

    def test_target_coverage_rejects_an_omitted_test(self) -> None:
        module = TestModule("tests.test_hotspot", Path("/test_hotspot.py"), 1)
        test_ids = (
            "tests.test_hotspot.TestCases.test_one",
            "tests.test_hotspot.TestCases.test_two",
        )
        targets = shard_test_ids(module, test_ids[:1], granularity="method")

        with self.assertRaisesRegex(ValueError, "missing test target"):
            assert_exact_target_coverage(module, test_ids, targets)

    def test_world_model_is_frontloaded_with_its_isolated_budget(self) -> None:
        modules = complete_test_modules((), REPO_ROOT)
        world = next(module for module in modules if module.name == WORLD_MODEL_MODULE)
        env = test_subprocess_environment(
            {
                "TEST_DB_DSN": "postgresql://worker",
                "CRATEDIGGER_TEST_SCHEMA_READY": "1",
                "CRATEDIGGER_WORLD_RANDOMIZED": "1",
            },
            world,
        )

        self.assertEqual(schedule_modules(modules)[0], world)
        self.assertNotIn("TEST_DB_DSN", env)
        self.assertNotIn("CRATEDIGGER_TEST_SCHEMA_READY", env)
        self.assertEqual(env["CRATEDIGGER_WORLD_RANDOMIZED"], "0")
        self.assertEqual(env["CRATEDIGGER_WORLD_EXAMPLES"], "6")
        self.assertEqual(env["CRATEDIGGER_WORLD_STEPS"], "8")

    def test_real_beets_matrix_exposes_every_cell_as_a_queue_target(self) -> None:
        test_ids = list_module_test_ids(
            "tests.test_beets_destructive_configs_generated",
            REPO_ROOT,
        )
        matrix_ids = tuple(
            test_id for test_id in test_ids if ".test_common_config_" in test_id
        )

        self.assertEqual(len(matrix_ids), 60)
        self.assertNotIn(
            "tests.test_beets_destructive_configs_generated."
            "TestGeneratedRealBeetsConfigMatrix."
            "test_every_declared_common_config_cell",
            test_ids,
        )
        module = TestModule(
            "tests.test_beets_destructive_configs_generated",
            REPO_ROOT / "tests" / "test_beets_destructive_configs_generated.py",
            1,
        )
        targets = shard_test_ids(module, test_ids, granularity="method_batch")
        assert_exact_target_coverage(module, test_ids, targets)
        self.assertEqual(len(targets), 12)


class TestRunnerProcessContract(unittest.TestCase):
    def _write_fixture_suite(self, root: Path, *, failing: bool = False) -> Path:
        tests_dir = root / "fixture_tests"
        nested_dir = tests_dir / "nested"
        nested_dir.mkdir(parents=True)
        for package in (tests_dir, nested_dir):
            (package / "__init__.py").write_text("", encoding="utf-8")
        (tests_dir / "test_alpha.py").write_text(
            "import os\n"
            "import unittest\n\n"
            "class Alpha(unittest.TestCase):\n"
            "    def test_private_database(self):\n"
            "        self.assertIsNone(os.environ.get('TEST_DB_DSN'))\n"
            "    def test_second(self):\n"
            f"        self.assertEqual({1 if not failing else 0}, 1, "
            "'alpha second failure sentinel')\n"
            "    def test_third(self):\n"
            f"        self.assertEqual({1 if not failing else 0}, 1, "
            "'alpha third failure sentinel')\n",
            encoding="utf-8",
        )
        (nested_dir / "test_beta.py").write_text(
            "import time\n"
            "import unittest\n\n"
            "class Beta(unittest.TestCase):\n"
            "    def test_beta(self):\n"
            "        time.sleep(0.2)\n"
            f"        self.assertTrue({not failing}, "
            "'beta delayed failure sentinel')\n",
            encoding="utf-8",
        )
        return tests_dir

    def _run_fixture(
        self, *, failing: bool = False
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = self._write_fixture_suite(root, failing=failing)
            env = {**os.environ, "TEST_DB_DSN": "postgresql://must-not-leak"}
            return subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--start-directory",
                    str(tests_dir),
                    "--top-level-directory",
                    str(root),
                    "--jobs",
                    "2",
                    "--durations",
                    "2",
                ],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

    def test_runner_aggregates_every_module_and_test(self) -> None:
        result = self._run_fixture()

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("2 modules across 2 workers", result.stdout)
        self.assertIn("Ran 4 tests across", result.stdout)
        self.assertIn("OK", result.stdout)

    def test_runner_collects_all_failures_before_returning_nonzero(self) -> None:
        result = self._run_fixture(failing=True)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FAIL: worker", result.stdout)
        self.assertIn("alpha second failure sentinel", result.stdout)
        self.assertIn("alpha third failure sentinel", result.stdout)
        self.assertIn("beta delayed failure sentinel", result.stdout)
        self.assertIn("FAILED", result.stdout)

    def _run_enospc_fixture(
        self, *, mixed: bool
    ) -> subprocess.CompletedProcess[str]:
        """Deterministically trip the ENOSPC classifier — no real disk pressure.

        `_classify_test_infrastructure_error` recognises a bare
        `OSError(errno.ENOSPC, ...)` regardless of the host's actual free
        space (`_find_disk_full_exception`), so this reproduces issue
        #1111's "N disguised failures" shape without ever touching the
        shared tmpfs.
        """
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "fixture_tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            real_bug = (
                "    def test_real_bug(self):\n"
                "        self.assertEqual(0, 1, 'genuine alpha bug')\n"
                if mixed
                else ""
            )
            (tests_dir / "test_alpha.py").write_text(
                "import errno\n"
                "import unittest\n\n"
                "class Alpha(unittest.TestCase):\n"
                f"{real_bug}"
                "    def test_hit_enospc(self):\n"
                "        raise OSError(errno.ENOSPC, "
                "'No space left on device')\n",
                encoding="utf-8",
            )
            (tests_dir / "test_beta.py").write_text(
                "import unittest\n\n"
                "class Beta(unittest.TestCase):\n"
                "    def test_ok(self):\n"
                "        pass\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--start-directory",
                    str(tests_dir),
                    "--top-level-directory",
                    str(root),
                    "--jobs",
                    "2",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

    def test_pure_ram_root_exhaustion_collapses_to_one_named_failure(
        self,
    ) -> None:
        result = self._run_enospc_fixture(mixed=False)

        self.assertEqual(
            result.returncode,
            TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
            result.stdout + result.stderr,
        )
        self.assertEqual(
            result.stdout.count(FAILURE_MARKER_PREFIX),
            1,
            "every disk-full-classified failure must fold into ONE marker",
        )
        self.assertIn(TEST_RAM_ROOT_EXHAUSTED, result.stdout)
        self.assertIn("fixture_tests.test_alpha.Alpha.test_hit_enospc", result.stdout)

    def test_mixed_ram_root_exhaustion_still_reports_the_real_bug(self) -> None:
        result = self._run_enospc_fixture(mixed=True)

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(
            result.stdout.count(FAILURE_MARKER_PREFIX),
            2,
            "the real bug and the collapsed disk-full bucket are two "
            "SEPARATE entries — neither swallows the other",
        )
        self.assertIn(TEST_RAM_ROOT_EXHAUSTED, result.stdout)
        self.assertIn("genuine alpha bug", result.stdout)
        markers = [
            msgspec.json.decode(
                line.removeprefix(FAILURE_MARKER_PREFIX), type=CheckFailureMarker
            )
            for line in result.stdout.splitlines()
            if line.startswith(FAILURE_MARKER_PREFIX)
        ]
        identities = {marker.identity for marker in markers}
        self.assertEqual(identities, {"fixture_tests.test_alpha", TEST_RAM_ROOT_EXHAUSTED})
        alpha_marker = next(
            marker
            for marker in markers
            if marker.identity == "fixture_tests.test_alpha"
        )
        self.assertEqual(
            alpha_marker.test_ids,
            ("fixture_tests.test_alpha.Alpha.test_real_bug",),
        )

    def _run_worker_death_fixture(
        self, *, count: int = 2, env_overrides: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        """Kill `count` ProcessPoolExecutor worker processes (not the
        nested per-target subprocess `_run_test_target` spawns) so the
        parent's `future.result()` genuinely raises `BrokenProcessPool`
        for EACH ONE -- the SAME exception shape an OOM kill produces.
        `os.getppid()` inside the nested `--_run-target` child, captured
        once, at MODULE IMPORT time, via `tests.parent_signal_guard.
        capture_intended_parent_pid` (module scope in the generated
        fixture file below -- the earliest point this dynamically-loaded
        module can run any code at all), names the pool worker's PID:
        killing IT (not the child's own PID) reproduces the real
        production shape, confirmed live (issue #1156 item 2).
        Independent review F10: count=1 cannot distinguish "folded into
        one marker" from "there was only ever one failure to report" --
        count>1 is required to prove the fold actually collapses N>1 into
        ONE.

        Issue #1250 review finding F4, stated honestly: at THIS site,
        "captured early" USUALLY narrows nothing, though not never --
        method, measurements, and the exact clause attribution (review
        findings R3, then corrected again by V2/V3 after that
        correction pass itself mis-described what one clause catches
        and overstated another's durability) all live in
        `docs/solutions/testing/parent-signal-guard-worker-death-fixture.md`,
        not restated here. The short version: a `pid 1` verdict is only
        reachable AFTER the reparented-check clause already agreed the
        live parent still matched the captured value, which means
        whenever that verdict occurs the captured value was ALREADY 1 --
        this fixture instance's real pool worker had already been torn
        down, and this child had already been reparented, before its
        own module import even ran. The import-time capture narrowed
        nothing in that case; it simply recorded the post-reparenting
        PID as faithfully as the pre-reparenting one. The general
        "capture early, then re-verify" design is still correct -- the
        rare case where the parent is still alive at capture and exits
        before the live re-check is exactly what justifies capturing
        early at all -- see the doc for the measured, corrected
        attribution rather than a number restated here.

        Issue #1250 correction: each instance signals through
        `tests.parent_signal_guard.guard_and_signal_parent`, never a bare
        getppid() re-read at signal time. Re-reading it there races every
        OTHER instance's own kill --
        whichever instance's real pool worker exits first orphans the
        rest, and CPython's `_ExecutorManagerThread._terminate_broken`
        then calls `.terminate()` on every REMAINING pool worker while
        tearing the broken pool down. In a bare/CI process tree that just
        means the next instance's `os.getppid()` returns PID 1 (harmless,
        EPERM). Under a `systemd --user` session
        (`PR_SET_CHILD_SUBREAPER` makes the user manager the orphan
        reaper) it instead returns the MANAGER's own PID, and the raw
        form SIGKILLs it -- taking the whole interactive session down.
        Measured live twice on doc1 (2026-08-22, 2026-08-23); full account
        in `docs/solutions/testing/parent-signal-guard-worker-death-fixture.md`.

        With the guard active, `count` INDEPENDENT ProcessPoolExecutor
        deaths are no longer guaranteed: whichever instance's kill fires
        first may itself orphan a sibling before the sibling's own
        guarded kill runs, and the sibling then correctly REFUSES (its
        captured parent is no longer its live parent) instead of
        escalating to whatever it got reparented to. That sibling's own
        pool worker still dies -- via the SAME `_terminate_broken`
        cascade the executor already runs while tearing down the pool --
        so `future.result()` still raises `BrokenProcessPool` for it.
        This is arguably BETTER production fidelity, not worse: a real
        OOM kill hits one worker directly and the executor's own teardown
        cascades `.terminate()` to the rest, which is exactly this shape.
        What this fixture actually proves, either way, is that `count`
        targets each report `BrokenProcessPool` and fold into ONE marker
        -- not that each one's exception was raised by this fixture's own
        signal specifically.

        Independent review B2 (HIGH): CRATEDIGGER_TEST_RAM_MIN_BYTES is
        pinned to "0" by default -- disk_full is checked BEFORE memory in
        `_classify_target_infrastructure_failure`, so leaving it ambient
        left this fixture coupled to the host's own real free tmpfs bytes
        (measured on doc1: the ambient worker-scaled floor, 1,744,830,464,
        classified both deaths disk_full instead of memory_exhausted).
        `env_overrides` lets callers (e.g. a pure disk-full scenario) flip
        which resource actually trips.
        """
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "fixture_tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            for index in range(count):
                (tests_dir / f"test_alpha{index}.py").write_text(
                    "import signal\n"
                    "import unittest\n\n"
                    "import tests.parent_signal_guard as parent_signal_guard\n\n"
                    "_INTENDED_PARENT_PID = ("
                    "parent_signal_guard.capture_intended_parent_pid())\n\n"
                    "class Alpha(unittest.TestCase):\n"
                    "    def test_worker_dies(self):\n"
                    "        parent_signal_guard.guard_and_signal_parent(\n"
                    "            _INTENDED_PARENT_PID, signal.SIGKILL,\n"
                    "        )\n",
                    encoding="utf-8",
                )
            env = {
                **os.environ,
                "CRATEDIGGER_TEST_RAM_MIN_BYTES": "0",
                "CRATEDIGGER_TEST_MEMORY_MIN_BYTES": str(10**18),
                **(env_overrides or {}),
            }
            return subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--start-directory",
                    str(tests_dir),
                    "--top-level-directory",
                    str(root),
                    "--jobs",
                    str(count),
                ],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

    def test_worker_death_under_exhausted_memory_floor_collapses_to_one_named_failure(
        self,
    ) -> None:
        """End-to-end (issue #1156 item 2): TWO REAL BrokenProcessPool
        deaths, not fakes, folded into ONE marker instead of an ordinary
        per-target disguise -- the CRATEDIGGER_TEST_MEMORY_MIN_BYTES
        override is the same deterministic trick
        TestRunTargetsWorkerExceptionWiring documents for the disk-full
        floor, applied to the memory one. Independent review F10: N=2
        (not 1) so "exactly one marker" actually proves folding, not just
        that there was one failure to report. Issue #1250 correction:
        "independent" no longer describes the two deaths' CAUSE -- with
        the parent-signal guard active, one instance's own kill can
        orphan the other before its guarded kill runs, so the second
        death may come from the executor's own `_terminate_broken`
        cascade rather than a second fixture signal. Both still produce a
        genuine, distinct `BrokenProcessPool` for their own target, which
        is what this test actually asserts."""
        result = self._run_worker_death_fixture(count=2)

        self.assertEqual(
            result.returncode,
            TEST_HOST_MEMORY_EXHAUSTED_EXIT_CODE,
            result.stdout + result.stderr,
        )
        self.assertEqual(
            result.stdout.count(FAILURE_MARKER_PREFIX),
            1,
            "every memory-exhausted worker death must fold into ONE marker",
        )
        self.assertIn(TEST_HOST_MEMORY_EXHAUSTED, result.stdout)
        self.assertIn("BrokenProcessPool", result.stdout)
        marker_line = next(
            line
            for line in result.stdout.splitlines()
            if line.startswith(FAILURE_MARKER_PREFIX)
        )
        marker = msgspec.json.decode(
            marker_line.removeprefix(FAILURE_MARKER_PREFIX),
            type=CheckFailureMarker,
        )
        self.assertEqual(
            set(marker.test_ids),
            {"fixture_tests.test_alpha0", "fixture_tests.test_alpha1"},
        )

    def test_worker_deaths_classified_pure_disk_full_never_reach_the_memory_bucket(
        self,
    ) -> None:
        """Independent review B3: the composition test in
        TestCollapseMemoryExhaustedFailures (test_disk_full_and_memory_
        exhausted_are_each_folded_exactly_once) reimplements main()'s own
        wiring inline rather than driving it -- it would stay green even if
        main() itself fed `_collapse_memory_exhausted_failures` the RAW
        `infrastructure_failures` list instead of the disk-full-FILTERED
        `remaining_infrastructure_failures` (line ~1706). This test drives
        the real subprocess end-to-end instead: TWO real BrokenProcessPool
        deaths (see `_run_worker_death_fixture`'s own docstring, issue
        #1250, for why "independent" no longer describes their cause with
        the parent-signal guard active), both forced disk_full via an
        impossibly high CRATEDIGGER_TEST_RAM_MIN_BYTES (the memory floor is left at
        the fixture's own high default, but never evaluated -- disk_full
        short-circuits the memory branch in _classify_target_infrastructure_
        failure). Correct wiring: both fold into ONE ram-root marker, exit
        TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE, no memory marker at all. The
        named mutant (raw list instead of the filtered remainder) would
        instead leave both targets in `remaining_infrastructure_failures`
        (since neither has memory_exhausted=True) -- printed a SECOND time
        as ordinary per-target failures on top of the folded marker, and
        the exit code would fall through to plain `1` because
        `remaining_infrastructure_failures` is no longer empty."""
        result = self._run_worker_death_fixture(
            count=2,
            env_overrides={"CRATEDIGGER_TEST_RAM_MIN_BYTES": str(10**18)},
        )

        self.assertEqual(
            result.returncode,
            TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
            result.stdout + result.stderr,
        )
        self.assertEqual(
            result.stdout.count(FAILURE_MARKER_PREFIX),
            1,
            "both disk-full-classified deaths must fold into ONE marker, "
            "never double-reported alongside an ordinary per-target entry",
        )
        self.assertIn(TEST_RAM_ROOT_EXHAUSTED, result.stdout)
        self.assertNotIn(
            TEST_HOST_MEMORY_EXHAUSTED,
            result.stdout,
            "disk_full short-circuits the memory branch -- no memory "
            "marker should appear at all",
        )
        marker_line = next(
            line
            for line in result.stdout.splitlines()
            if line.startswith(FAILURE_MARKER_PREFIX)
        )
        marker = msgspec.json.decode(
            marker_line.removeprefix(FAILURE_MARKER_PREFIX),
            type=CheckFailureMarker,
        )
        self.assertEqual(
            set(marker.test_ids),
            {"fixture_tests.test_alpha0", "fixture_tests.test_alpha1"},
        )

    def test_both_markers_present_falls_through_to_the_ordinary_failure_code(
        self,
    ) -> None:
        """Independent review B-3 (third round): the two-clause promotion
        guard in main() (`... and ram_root_marker is not None and
        memory_marker is None` / the mirrored clause for memory) implements
        the documented F10 residual -- BOTH markers present is a
        HETEROGENEOUS failure set, so promotion is skipped and the run
        falls through to plain `return 1`. Deleting either `is None` clause
        survived every prior test, since none seeded BOTH markers in one
        run.

        A REAL subprocess reproduction of this scenario was tried first
        and abandoned: a killed ProcessPoolExecutor worker poisons the
        WHOLE pool for every future still tracked at that moment
        (concurrent.futures' own documented semantics), including a
        SEPARATE target's already-in-flight ENOSPC classification --
        reproduced deterministically (3/3 runs, both --jobs 2 racing the
        two targets and --jobs 1 sequencing them) as BOTH targets folding
        into the SAME memory_exhausted marker, never the heterogeneous
        scenario this guard exists to handle. `run_targets_fn` (mirroring
        the burst `main`s' own `check_headroom` DI seam, issue #1156 item
        3) replaces exactly that one real subprocess boundary with two
        pre-built failures -- one disk_full, one memory_exhausted, using
        the REAL schedule main() itself discovers -- while every other
        line of main() (CLI parsing, discovery, scheduling, both collapse
        helpers, printing, exit-code selection) runs unmocked."""
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "fixture_tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            (tests_dir / "test_alpha.py").write_text(
                "import unittest\n\n"
                "class Alpha(unittest.TestCase):\n"
                "    def test_ok(self):\n"
                "        pass\n",
                encoding="utf-8",
            )
            (tests_dir / "test_beta.py").write_text(
                "import unittest\n\n"
                "class Beta(unittest.TestCase):\n"
                "    def test_ok(self):\n"
                "        pass\n",
                encoding="utf-8",
            )

            def seeded_run_targets(
                schedule: Sequence[TestTarget], **_kwargs: object
            ) -> tuple[
                tuple[TargetRunResult, ...],
                tuple[TargetInfrastructureFailure, ...],
            ]:
                targets = tuple(schedule)
                self.assertEqual(len(targets), 2)
                disk_failure = TargetInfrastructureFailure(
                    target=targets[0],
                    detail="temporary filesystem has 0 bytes free",
                    disk_full=True,
                )
                memory_failure = TargetInfrastructureFailure(
                    target=targets[1],
                    detail=(
                        "system memory has 0 bytes available; "
                        "BrokenProcessPool: fake worker death"
                    ),
                    memory_exhausted=True,
                )
                return (), (disk_failure, memory_failure)

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                status = main(
                    (
                        "--start-directory", str(tests_dir),
                        "--top-level-directory", str(root),
                        "--jobs", "2",
                    ),
                    run_targets_fn=seeded_run_targets,
                )
            output = stdout.getvalue()

        self.assertEqual(status, 1, output)
        self.assertIn(TEST_RAM_ROOT_EXHAUSTED, output)
        self.assertIn(TEST_HOST_MEMORY_EXHAUSTED, output)

    def test_each_module_gets_a_fresh_python_interpreter(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "fixture_tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            (tests_dir / "test_alpha.py").write_text(
                "import builtins\n"
                "import unittest\n\n"
                "class Alpha(unittest.TestCase):\n"
                "    def test_mutates_process_global(self):\n"
                "        builtins._parallel_runner_leak = True\n",
                encoding="utf-8",
            )
            (tests_dir / "test_beta.py").write_text(
                "import builtins\n"
                "import unittest\n\n"
                "class Beta(unittest.TestCase):\n"
                "    def test_process_global_is_clean(self):\n"
                "        self.assertFalse(hasattr(builtins, "
                "'_parallel_runner_leak'))\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--start-directory",
                    str(tests_dir),
                    "--top-level-directory",
                    str(root),
                    "--jobs",
                    "1",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Ran 2 tests", result.stdout)

    def test_zero_test_contract_module_still_reports_a_result(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            tests_dir = root / "fixture_tests"
            tests_dir.mkdir()
            (tests_dir / "__init__.py").write_text("", encoding="utf-8")
            (tests_dir / "test_contract_only.py").write_text(
                "CONTRACT_SENTINEL = True\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--start-directory",
                    str(tests_dir),
                    "--top-level-directory",
                    str(root),
                    "--jobs",
                    "1",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Ran 0 tests", result.stdout)


def _planted_case(
    deadline: int | datetime.timedelta | None,
) -> type[unittest.TestCase]:
    """One real Hypothesis TestCase class carrying exactly this deadline."""

    @settings(deadline=deadline, max_examples=1)
    @given(value=st.integers())
    def test_property(self: unittest.TestCase, value: int) -> None:
        self.assertIsInstance(value, int)

    return type("PlantedWorld", (unittest.TestCase,), {
        "test_property": test_property,
    })


def _plain_case() -> type[unittest.TestCase]:
    def test_ordinary(self: unittest.TestCase) -> None:
        self.assertTrue(True)

    return type("PlainWorld", (unittest.TestCase,), {
        "test_ordinary": test_ordinary,
    })


# Held in a dict, never bound as module attributes, so unittest never collects
# them — the deadline-bearing ones would otherwise fail this very module under
# the contract they exist to test. Built here rather than loaded from a temp
# package because an arbitrary ``sys.path`` insert is the #95/#445 dual-load
# hazard the sys.path audit forbids.
PLANTED_CASES: dict[str, type[unittest.TestCase]] = {
    "none": _planted_case(None),
    "ms_1": _planted_case(1),
    "ms_200": _planted_case(200),
    "ms_60000": _planted_case(60_000),
    "timedelta": _planted_case(datetime.timedelta(milliseconds=200)),
    "plain": _plain_case(),
}

#: Planted kinds whose Hypothesis tests carry a deadline the contract rejects.
DEADLINE_BEARING_KINDS = frozenset({"ms_1", "ms_200", "ms_60000", "timedelta"})

#: Planted kinds that are Hypothesis tests at all.
HYPOTHESIS_KINDS = frozenset(set(PLANTED_CASES) - {"plain"})


def planted_suite(kinds: tuple[str, ...]) -> unittest.TestSuite:
    """Compose one suite from the planted catalogue, in order."""
    suite = unittest.TestSuite()
    for kind in kinds:
        suite.addTests(
            unittest.defaultTestLoader.loadTestsFromTestCase(PLANTED_CASES[kind]),
        )
    return suite


class TestSuiteDeadlineContract(unittest.TestCase):
    """Issue #882 B1b: the suite tier enforces what fuzz discovery already did.

    A source audit can only police one spelling in one position. This is the
    runtime fact itself — resolved settings, not source text — so a module
    that imports the profile tier below its decorators (which the static audit
    catches) and any shape it does NOT catch both fail here.
    """

    # An "unwired module" cannot be reproduced IN THIS PROCESS: something has
    # already imported tests._hypothesis_profiles, and load_profile mutates
    # settings.default globally, so a freshly loaded unwired module inherits
    # the tier anyway. That process-global inheritance IS the #882 defect. The
    # unwired case is therefore pinned end-to-end below, in a fresh child that
    # loads nothing else; the in-process pins cover the contract itself.

    def test_a_deadline_pinned_to_none_passes_the_runtime_contract(self) -> None:
        """Must-still-work guard: the ordinary shape is not rejected."""
        assert_hypothesis_deadlines_disabled(planted_suite(("none",)))

    def test_an_explicit_deadline_is_rejected_however_the_module_is_wired(
        self,
    ) -> None:
        """Spelling and position cannot buy an explicit deadline back."""
        with self.assertRaisesRegex(RuntimeError, "non-None deadline"):
            assert_hypothesis_deadlines_disabled(planted_suite(("ms_200",)))

    def test_one_deadline_condemns_an_otherwise_clean_suite(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "non-None deadline"):
            assert_hypothesis_deadlines_disabled(
                planted_suite(("none", "plain", "timedelta")),
            )

    def test_a_suite_without_hypothesis_tests_is_accepted(self) -> None:
        suite = planted_suite(("plain", "plain"))

        assert_hypothesis_deadlines_disabled(suite)
        for test in _iter_test_cases(suite):
            self.assertIsNone(resolve_hypothesis_settings(test))

    def test_an_unclassifiable_hypothesis_test_fails_closed(self) -> None:
        """A Hypothesis test whose settings cannot be read must raise, not be
        quietly skipped past the deadline contract."""
        broken = _planted_case(None)
        setattr(  # noqa: B010 - plant malformed Hypothesis runtime metadata
            getattr(broken, "test_property"),  # noqa: B009 - generated test method
            "_hypothesis_internal_use_settings",
            object(),
        )
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(broken)

        with self.assertRaisesRegex(TypeError, "invalid settings"):
            assert_hypothesis_deadlines_disabled(suite)

    def test_planted_cases_are_never_collected_from_this_module(self) -> None:
        """The deadline-bearing plants must stay out of unittest discovery —
        otherwise this module fails the very contract it defines."""
        suite = unittest.defaultTestLoader.loadTestsFromName(__name__)
        collected = {type(test).__name__ for test in _iter_test_cases(suite)}

        self.assertNotIn("PlantedWorld", collected)
        self.assertNotIn("PlainWorld", collected)
        assert_hypothesis_deadlines_disabled(suite)

    def test_the_child_runner_refuses_to_run_an_unwired_module(self) -> None:
        """End-to-end: the real ``--_run-target`` child, not just the helper.

        A fresh interpreter that loads ONLY this module has never imported the
        profile tier, so the bare ``@given`` really does resolve the stock
        200ms deadline — the exact #882 item-1 shape.
        """
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            package = root / "child_deadline_fixture"
            package.mkdir()
            (package / "__init__.py").write_text("", encoding="utf-8")
            (package / "test_unwired.py").write_text(
                "import unittest\n"
                "from hypothesis import given\n"
                "from hypothesis import strategies as st\n"
                "\n"
                "class Planted(unittest.TestCase):\n"
                "    @given(value=st.integers())\n"
                "    def test_property(self, value):\n"
                "        self.assertIsInstance(value, int)\n",
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "PYTHONPATH": os.pathsep.join(
                    part
                    for part in (
                        str(root),
                        str(REPO_ROOT),
                        os.environ.get("PYTHONPATH", ""),
                    )
                    if part
                ),
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--_run-target",
                    '["child_deadline_fixture.test_unwired"]',
                    "0",
                    str(root / "result.json"),
                ],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("non-None deadline", completed.stderr)
        self.assertFalse((root / "result.json").exists())


@settings(deadline=None, max_examples=50, database=None)
@given(first=st.booleans(), second=st.booleans())
def _tiny_space_property(first: bool, second: bool) -> None:
    """Four distinct worlds against a fifty-example budget (#888 item 1)."""
    assert isinstance(first, bool)
    assert isinstance(second, bool)


@settings(
    deadline=None,
    max_examples=50,
    database=None,
    suppress_health_check=[HealthCheck.filter_too_much],
)
@given(value=st.integers())
def _discarding_property(value: int) -> None:
    """Half of every generated world is discarded by ``assume``."""
    assume(value % 2 == 0)
    assert value % 2 == 0


@settings(deadline=None, max_examples=50, database=None, print_blob=False)
@given(value=st.integers(min_value=0, max_value=3))
def _planted_failing_property(value: int) -> None:
    """The known-bad shape every self-test has: it finds its planted bug."""
    raise AssertionError(f"planted failure for {value}")


class TestHypothesisStatsRecorder(unittest.TestCase):
    """Per-property depth measured from real Hypothesis statistics (#888).

    Every statistics mapping under test is produced by Hypothesis itself
    through the real ``collector`` seam — a hand-typed statistics literal
    would prove nothing about what the engine emits (`.claude/rules/
    test-fidelity.md` Rule C).
    """

    def _record(
        self,
        prop: Callable[[], None],
        *,
        budget: int,
        test_id: str = "planted.Property.test_property",
        expect_failure: bool = False,
    ) -> HypothesisPropertyStats:
        recorder = HypothesisStatsRecorder({test_id: budget})
        recorder.start(test_id)
        with collector.with_value(recorder.note):  # pyright: ignore[reportArgumentType]
            if expect_failure:
                with self.assertRaises(AssertionError):
                    prop()
            else:
                prop()
        self.assertEqual(len(recorder.records), 1, recorder.records)
        return recorder.records[0]

    def test_an_exhausted_strategy_space_is_measured_below_its_budget(self) -> None:
        record = self._record(_tiny_space_property, budget=50)

        self.assertEqual(record.stopped_because, STRATEGY_SPACE_EXHAUSTED)
        self.assertEqual(record.valid, 4)
        self.assertEqual(record.interesting, 0)
        self.assertEqual(record.max_examples, 50)

    def test_assume_discards_are_measured_as_invalid_worlds(self) -> None:
        record = self._record(_discarding_property, budget=50)

        self.assertEqual(record.valid, 50)
        self.assertGreater(record.invalid, 0)

    def test_a_planted_bug_is_measured_as_an_interesting_case(self) -> None:
        record = self._record(
            _planted_failing_property,
            budget=50,
            expect_failure=True,
        )

        self.assertGreater(record.interesting, 0)

    def test_a_real_interesting_run_is_never_reported_as_shallow(self) -> None:
        """The carve-out, driven by the producer that makes interesting cases.

        A property that fails on its first world stops with ``nothing left to
        do`` and zero valid worlds — indistinguishable from an exhausted tiny
        space unless the interesting cases are read. No repository property
        reaches this state today (the known-bad self-tests plant their bug in
        an inner ``@given`` whose statistics are dropped), so the carve-out is
        defensive; this pin proves the shape is producible and handled.
        """
        record = self._record(
            _planted_failing_property,
            budget=50,
            expect_failure=True,
        )
        depth = aggregate_property_depth((record,))[0]

        self.assertGreater(depth.interesting, 0)
        self.assertEqual(depth.exhausted_shards, depth.shards)
        self.assertLess(depth.distinct_world_bound, depth.budget)
        self.assertFalse(is_structurally_shallow(depth))
        self.assertTrue(is_structurally_shallow(replace(depth, interesting=0)))

    def test_statistics_arriving_with_no_started_test_are_dropped(self) -> None:
        """Impossible by construction; a report must never break its own run."""
        recorder = HypothesisStatsRecorder({})
        with collector.with_value(recorder.note):  # pyright: ignore[reportArgumentType]
            _tiny_space_property()

        self.assertEqual(recorder.records, ())

    def test_an_inner_property_is_not_filed_under_its_enclosing_test(self) -> None:
        """The known-bad self-test shape: a plain test that runs a property.

        Attributing those statistics to the enclosing test would inflate the
        property count, and a body running two inner properties would fold
        into one row claiming two entropy shards it never had.
        """
        recorder = HypothesisStatsRecorder({"module.World.test_property": 150})
        recorder.start("module.World.test_plain_pin")
        with collector.with_value(recorder.note):  # pyright: ignore[reportArgumentType]
            _tiny_space_property()
            _discarding_property()

        self.assertEqual(recorder.records, ())

    def test_counted_statuses_match_the_hypothesis_vocabulary(self) -> None:
        """A new engine status must not silently vanish from the report."""
        self.assertEqual(
            set(HYPOTHESIS_CASE_STATUSES),
            {status.name.lower() for status in Status},
        )

    def test_budget_map_covers_every_hypothesis_test_and_nothing_else(self) -> None:
        suite = planted_suite(("none", "plain"))

        budgets = hypothesis_example_budgets(suite)

        hypothesis_ids = [
            test.id()
            for test in _iter_test_cases(suite)
            if resolve_hypothesis_settings(test) is not None
        ]
        self.assertEqual(sorted(budgets), sorted(hypothesis_ids))
        self.assertEqual(set(budgets.values()), {1})

    def test_the_child_runner_reports_per_property_generated_depth(self) -> None:
        """End-to-end: the real ``--_run-target`` child, over the real wire.

        Proves the collected depth survives ``ChildTargetResult`` — the fake
        cannot show that, because the runner reads what the child encoded.
        """
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            package = root / "child_depth_fixture"
            package.mkdir()
            (package / "__init__.py").write_text("", encoding="utf-8")
            (package / "test_tiny_generated.py").write_text(
                "import unittest\n"
                "from hypothesis import given, settings\n"
                "from hypothesis import strategies as st\n"
                "\n"
                "import tests._hypothesis_profiles  # noqa: F401\n"
                "\n"
                "class Tiny(unittest.TestCase):\n"
                "    @settings(max_examples=50, database=None)\n"
                "    @given(first=st.booleans(), second=st.booleans())\n"
                "    def test_property(self, first, second):\n"
                "        self.assertIsInstance(first, bool)\n"
                "        self.assertIsInstance(second, bool)\n"
                "\n"
                "    def test_pin(self):\n"
                "        self.assertTrue(True)\n",
                encoding="utf-8",
            )
            result_path = root / "result.json"
            env = {
                **os.environ,
                "PYTHONPATH": os.pathsep.join(
                    part
                    for part in (
                        str(root),
                        str(REPO_ROOT),
                        os.environ.get("PYTHONPATH", ""),
                    )
                    if part
                ),
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--_run-target",
                    '["child_depth_fixture.test_tiny_generated"]',
                    "0",
                    str(result_path),
                ],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                completed.returncode,
                0,
                completed.stdout + completed.stderr,
            )
            child = msgspec.json.decode(
                result_path.read_bytes(),
                type=ChildTargetResult,
            )

        self.assertEqual(child.tests_run, 2)
        self.assertEqual(
            tuple(record.test_id for record in child.hypothesis_stats),
            ("child_depth_fixture.test_tiny_generated.Tiny.test_property",),
        )
        self.assertEqual(
            child.hypothesis_stats[0],
            HypothesisPropertyStats(
                test_id=(
                    "child_depth_fixture.test_tiny_generated.Tiny.test_property"
                ),
                max_examples=50,
                valid=4,
                invalid=0,
                overrun=0,
                interesting=0,
                stopped_because=STRATEGY_SPACE_EXHAUSTED,
            ),
        )

    def test_child_result_rejects_wrong_infrastructure_error_types(self) -> None:
        payload = msgspec.json.encode(
            {
                "successful": False,
                "tests_run": 1,
                "test_ids": ["fixture.World.test_property"],
                "output": "failure",
                "failed_test_ids": ["fixture.World.test_property"],
                "infrastructure_errors": [
                    {
                        "test_id": "fixture.World.test_property",
                        "kind": 53100,
                        "detail": "disk full",
                    }
                ],
            }
        )

        with self.assertRaises(msgspec.ValidationError):
            msgspec.json.decode(payload, type=ChildTargetResult)


class TestRunTestsWiring(unittest.TestCase):
    def test_full_suite_uses_parallel_python_runner(self) -> None:
        shell_source = pinned_source(RUN_TESTS_SH)
        coordinator_source = pinned_source(RUN_SUITE)
        self.assertIn("exec python3 scripts/run_test_suite.py", shell_source)
        self.assertIn('("python3", "scripts/run_python_tests.py")', coordinator_source)
        self.assertNotIn("python3 -m unittest discover", coordinator_source)
        self.assertNotIn(
            "python3 -m unittest tests.world_model.state_machine",
            coordinator_source,
        )


if __name__ == "__main__":
    unittest.main()
