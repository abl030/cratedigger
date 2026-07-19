"""Operator contract for the heavyweight #743 world-model burst runner."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "world_model_burst.sh"


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

    def test_non_positive_budget_is_rejected_before_world_start(self) -> None:
        result = self._run("--examples", "0", "--print-config")

        self.assertEqual(result.returncode, 2)
        self.assertIn("positive integer", result.stderr)

    def test_help_is_side_effect_free(self) -> None:
        result = self._run("--help")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("randomized real-storage lifecycle hammer", result.stdout)


if __name__ == "__main__":
    unittest.main()
