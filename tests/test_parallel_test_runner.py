"""Contracts for the parallel full-suite Python runner."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_python_tests import (
    TestModule,
    assert_exact_schedule,
    discover_test_modules,
    schedule_modules,
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


class TestRunTestsWiring(unittest.TestCase):
    def test_full_suite_uses_parallel_python_runner(self) -> None:
        source = RUN_TESTS_SH.read_text(encoding="utf-8")
        self.assertIn("python3 scripts/run_python_tests.py", source)
        self.assertNotIn("python3 -m unittest discover", source)


if __name__ == "__main__":
    unittest.main()
