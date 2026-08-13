"""Contracts for the parallel full-suite Python runner."""

from __future__ import annotations

import datetime
import errno
import io
import os
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Callable
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
    _iter_test_cases,
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
    recommended_worker_count,
    resolve_hypothesis_settings,
    schedule_modules,
    select_test_targets,
    shard_test_ids,
    test_subprocess_environment,
    worker_environment,
)
from scripts.run_test_suite import (
    FAILURE_MARKER_PREFIX,
    TEST_RAM_ROOT_EXHAUSTED,
    CheckFailureMarker,
)

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
        failure = _classify_target_infrastructure_failure(
            _target("tests.test_alpha"),
            RuntimeError("target subprocess exited 1: traceback"),
            available_bytes=lambda: 10 * 1024 * 1024 * 1024,
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


class TestRunTargetsWorkerExceptionWiring(unittest.TestCase):
    """`_run_targets` really delegates to the measured classifier (#1111)."""

    def test_a_real_worker_exception_is_classified_by_live_measured_headroom(
        self,
    ) -> None:
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
            # A real, deterministic parent-side exception (no disk pressure
            # needed): the child legitimately reports its own real test IDs,
            # and the parent's own mismatch guard raises before returning.
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
        failure = infrastructure_failures[0]
        self.assertIn("unexpected test IDs", failure.detail)
        # Ample real headroom on the host running this test — proves the
        # measurement is live, not a fake stand-in for "always disk_full".
        self.assertFalse(failure.disk_full)


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
        self.assertEqual(recommended_worker_count(8), 4)
        self.assertEqual(recommended_worker_count(30), 12)
        self.assertEqual(recommended_worker_count(64), 12)

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
        self.assertEqual(
            AUDITED_FRONTLOAD_MODULES,
            frozenset({"tests.test_nix_module"}),
        )
        # tests.test_nix_module is frontloaded but deliberately NOT
        # method_batch-sharded (issue #1131 review round 2): its nix-eval
        # tests are cost-grouped into two exception-memoizing cached
        # helpers, which only pay off when every one of a group's
        # consumers runs in the same worker process. A blind method_batch
        # split could land more than one of the module's heavy nix-eval
        # methods in one batch (up to 5, main's own scheme); a naive full
        # unshard (this issue's own round 1) serializes every merged world
        # onto one target instead (measured: main's own worst-case
        # bin-packed batch is 61.6s, a full unshard is 118.3s).
        # HOTSPOT_ISOLATED_METHODS below is the narrower fix: carve the
        # single heaviest consumer into its own target, bundle everything
        # else into one remainder target — floor level with main (not
        # better), at most TWO concurrent heavy nix-eval subprocesses
        # instead of up to five, which is what makes raising the suite's
        # worker count affordable (main's own worker-count sweep shows
        # this module's pole inflating hard with concurrency: 88.0s at 8
        # workers, 122.7s at 12, 147.7s at 16, 152.3s at 20).
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
                        "test_basic_and_insecure_mode_matrix_is_evaluated"
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
        shell_source = RUN_TESTS_SH.read_text(encoding="utf-8")
        coordinator_source = RUN_SUITE.read_text(encoding="utf-8")
        self.assertIn("exec python3 scripts/run_test_suite.py", shell_source)
        self.assertIn('("python3", "scripts/run_python_tests.py")', coordinator_source)
        self.assertNotIn("python3 -m unittest discover", coordinator_source)
        self.assertNotIn(
            "python3 -m unittest tests.world_model.state_machine",
            coordinator_source,
        )


if __name__ == "__main__":
    unittest.main()
