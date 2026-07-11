"""Structural checks for the shared Claude/Codex project surfaces."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


class TestAIPortability(unittest.TestCase):
    def test_generated_adapters_are_current(self) -> None:
        result = subprocess.run(
            [sys.executable, "tools/generate-ai-adapters.py", "--check"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
