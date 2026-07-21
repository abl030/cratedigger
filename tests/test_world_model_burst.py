"""Operator contract for the heavyweight #743 world-model burst runner."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "world_model_burst.sh"
RUN_TESTS_SCRIPT = REPO_ROOT / "scripts" / "run_tests.sh"
PYTHON_RUNNER = REPO_ROOT / "scripts" / "run_python_tests.py"


class TestWorldModelBurstScript(unittest.TestCase):
    def _run(
        self,
        *args: str,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(SCRIPT), *args],
            cwd=REPO_ROOT,
            env={**os.environ, **(env or {})},
            capture_output=True,
            text=True,
            timeout=30,
        )

    def test_print_config_reports_randomized_ephemeral_world(self) -> None:
        result = self._run(
            "--print-config",
            env={
                "CRATEDIGGER_WORLD_EXAMPLES": "12",
                "CRATEDIGGER_WORLD_STEPS": "34",
                "CRATEDIGGER_WORLD_DATABASE": ".hypothesis/custom-world",
                # A hammer invocation must never inherit an arbitrary DB.
                "TEST_DB_DSN": "postgresql://production.invalid/cratedigger",
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("examples=12", result.stdout)
        self.assertIn("steps=34", result.stdout)
        self.assertIn("randomized=true", result.stdout)
        self.assertIn("postgres=ephemeral", result.stdout)
        self.assertIn("database=.hypothesis/custom-world", result.stdout)

    def test_command_line_budget_overrides_environment(self) -> None:
        result = self._run(
            "--examples",
            "7",
            "--steps",
            "19",
            "--print-config",
            env={
                "CRATEDIGGER_WORLD_EXAMPLES": "2",
                "CRATEDIGGER_WORLD_STEPS": "3",
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("examples=7", result.stdout)
        self.assertIn("steps=19", result.stdout)

    def test_mirror_engine_requires_and_reports_explicit_read_only_origin(self) -> None:
        missing = self._run("--engine", "mirror-harness", "--print-config")
        self.assertEqual(missing.returncode, 2)
        self.assertIn("--mirror-url", missing.stderr)

        configured = self._run(
            "--engine",
            "mirror-harness",
            "--mirror-url",
            "http://mirror.invalid:5200",
            "--print-config",
        )
        self.assertEqual(configured.returncode, 0, configured.stderr)
        self.assertIn("engine=mirror-harness", configured.stdout)
        self.assertIn("mirror_url=http://mirror.invalid:5200", configured.stdout)

    def test_non_positive_budget_is_rejected_before_world_start(self) -> None:
        result = self._run("--examples", "0", "--print-config")

        self.assertEqual(result.returncode, 2)
        self.assertIn("positive integer", result.stderr)

    def test_help_is_side_effect_free(self) -> None:
        result = self._run("--help")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("randomized real-storage lifecycle hammer", result.stdout)

    def test_standard_suite_runs_only_the_deterministic_world_budget(self) -> None:
        script = RUN_TESTS_SCRIPT.read_text(encoding="utf-8")
        runner = PYTHON_RUNNER.read_text(encoding="utf-8")

        self.assertIn("python3 scripts/run_python_tests.py", script)
        self.assertNotIn("python3 -m unittest tests.world_model.state_machine", script)
        self.assertIn('WORLD_MODEL_MODULE = "tests.world_model.state_machine"', runner)
        self.assertIn('("CRATEDIGGER_WORLD_RANDOMIZED", "0")', runner)
        self.assertIn('("CRATEDIGGER_WORLD_EXAMPLES", "6")', runner)
        self.assertIn('("CRATEDIGGER_WORLD_STEPS", "8")', runner)
        self.assertIn('unset_environment=("TEST_DB_DSN", _SCHEMA_READY_ENV)', runner)
        self.assertNotIn("scripts/world_model_burst.sh", script)
        self.assertNotIn("scripts/fuzz_burst.sh", script)


if __name__ == "__main__":
    unittest.main()
