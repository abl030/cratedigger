"""Contracts for the parallel full-suite Python runner."""

from __future__ import annotations

import datetime
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from scripts.run_python_tests import (
    HOTSPOT_SHARD_POLICIES,
    WORLD_MODEL_MODULE,
    TestModule,
    _iter_test_cases,
    assert_exact_target_coverage,
    assert_exact_schedule,
    assert_hypothesis_deadlines_disabled,
    complete_test_modules,
    discover_test_modules,
    list_module_test_ids,
    recommended_worker_count,
    resolve_hypothesis_settings,
    schedule_modules,
    shard_test_ids,
    test_subprocess_environment,
    worker_environment,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER = REPO_ROOT / "scripts" / "run_python_tests.py"
RUN_TESTS_SH = REPO_ROOT / "scripts" / "run_tests.sh"


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
            HOTSPOT_SHARD_POLICIES,
            {
                "tests.test_beets_destructive_configs_generated": "method_batch",
                "tests.test_pipeline_db": "class_batch",
            },
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

        self.assertEqual(len(matrix_ids), 54)
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
        setattr(
            getattr(broken, "test_property"),
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


class TestRunTestsWiring(unittest.TestCase):
    def test_full_suite_uses_parallel_python_runner(self) -> None:
        source = RUN_TESTS_SH.read_text(encoding="utf-8")
        self.assertIn("python3 scripts/run_python_tests.py", source)
        self.assertNotIn("python3 -m unittest discover", source)
        self.assertNotIn("python3 -m unittest tests.world_model.state_machine", source)


if __name__ == "__main__":
    unittest.main()
