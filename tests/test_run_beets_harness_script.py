"""Contract tests for harness/run_beets_harness.sh (tier-2 plan U5).

The wrapper is a two-liner now: exec ``$CRATEDIGGER_BEETS_PYTHON`` on
beets_harness.py. These tests pin the production launch shape — the
interpreter comes from the env (exported by ``beets_subprocess_env()``
from the module-rendered config), the harness resolves its beets config
via BEETSDIR, and a missing interpreter is an actionable error rather
than a silent fallback to a Home Manager profile.

The BEETSDIR tests run the REAL harness on the REAL beets (dev-shell Python).
They prove the wrapper selects the supplied configuration while deliberately
leaving contract enforcement at the already-completed top-level startup gate.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "harness" / "run_beets_harness.sh"


class TestRunBeetsHarnessScript(unittest.TestCase):
    def test_missing_interpreter_is_actionable_error(self) -> None:
        env = {k: v for k, v in os.environ.items()
               if k != "CRATEDIGGER_BEETS_PYTHON"}
        proc = subprocess.run(
            [str(SCRIPT), "--help"],
            env=env, capture_output=True, text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 1, proc.stderr)
        self.assertIn("CRATEDIGGER_BEETS_PYTHON", proc.stderr)
        self.assertIn("config.ini", proc.stderr)

    def test_execs_the_given_interpreter(self) -> None:
        """The wrapper adds nothing but the exec — argv reaches the harness
        unchanged on the interpreter we point it at."""
        proc = subprocess.run(
            [str(SCRIPT), "--help"],
            env={**os.environ, "CRATEDIGGER_BEETS_PYTHON": sys.executable},
            capture_output=True, text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("--search-id", proc.stdout)

    def test_harness_does_not_repeat_duplicate_contract_validation(self) -> None:
        """The child inherits an admitted BEETSDIR and never revalidates it."""
        with tempfile.TemporaryDirectory() as beetsdir:
            with open(os.path.join(beetsdir, "config.yaml"), "w",
                      encoding="utf-8") as f:
                f.write(f"library: {beetsdir}/lib.db\n")
            proc = subprocess.run(
                [str(SCRIPT), "--pretend", beetsdir],
                env={**os.environ,
                     "CRATEDIGGER_BEETS_PYTHON": sys.executable,
                     "BEETSDIR": beetsdir},
                capture_output=True, text=True, input="",
                check=False,
            )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertNotIn("duplicate_keys.album must be exactly", proc.stderr)

    def test_harness_accepts_module_shaped_config(self) -> None:
        """A normal inherited configuration still reaches the Beets import."""
        with tempfile.TemporaryDirectory() as beetsdir, \
                tempfile.TemporaryDirectory() as emptydir:
            with open(os.path.join(beetsdir, "config.yaml"), "w",
                      encoding="utf-8") as f:
                f.write(
                    f"library: {beetsdir}/lib.db\n"
                    f"directory: {beetsdir}/music\n"
                    "import:\n"
                    "  duplicate_keys:\n"
                    "    album: [mb_albumid, discogs_albumid]\n"
                    "    item: [artist, title]\n"
                )
            proc = subprocess.run(
                [str(SCRIPT), "--pretend", emptydir],
                env={**os.environ,
                     "CRATEDIGGER_BEETS_PYTHON": sys.executable,
                     "BEETSDIR": beetsdir},
                capture_output=True, text=True, input="",
                timeout=120,
                check=False,
            )
        self.assertNotIn("duplicate_keys.album must be exactly", proc.stderr)
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
