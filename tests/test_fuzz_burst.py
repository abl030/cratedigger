"""Persistence contract for unattended generated fuzz bursts."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "fuzz_burst.sh"


class TestFuzzBurstOutput(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.fake_bin = self.root / "bin"
        self.fake_bin.mkdir()
        python = self.fake_bin / "python3"
        python.write_text(
            "#!/usr/bin/env bash\n"
            "echo fuzz-log-marker\n"
            "echo 'Ran 1 test in 0.001s'\n"
            "exit \"${FAKE_FUZZ_EXIT:-0}\"\n",
            encoding="utf-8",
        )
        python.chmod(0o755)
        self.output_dir = self.root / "failures"

    def run_burst(self, status: int) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.fake_bin}:{env['PATH']}",
                "FAKE_FUZZ_EXIT": str(status),
                "FUZZ_PROFILE": "suite",
                "CRATEDIGGER_FUZZ_OUTPUT_DIR": str(self.output_dir),
            }
        )
        return subprocess.run(
            ["bash", str(SCRIPT), "tests.test_example_generated"],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_failed_module_log_is_retained_in_supplied_directory(self) -> None:
        proc = self.run_burst(1)

        self.assertNotEqual(proc.returncode, 0)
        logs = list(self.output_dir.glob("run.*/tests.test_example_generated.log"))
        self.assertEqual(len(logs), 1)
        self.assertIn("fuzz-log-marker", logs[0].read_text(encoding="utf-8"))
        self.assertIn(str(logs[0].parent), proc.stdout)

    def test_green_run_removes_transient_module_logs(self) -> None:
        proc = self.run_burst(0)

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(list(self.output_dir.glob("run.*")), [])


if __name__ == "__main__":
    unittest.main()
