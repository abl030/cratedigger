"""Contract tests for harness/run_beets_harness.sh (tier-2 plan U5).

The wrapper is a two-liner now: exec ``$CRATEDIGGER_BEETS_PYTHON`` on
beets_harness.py. These tests pin the production launch shape — the
interpreter comes from the env (exported by ``beets_subprocess_env()``
from the module-rendered config), the harness resolves its beets config
via BEETSDIR, and a missing interpreter is an actionable error rather
than a silent fallback to a Home Manager profile.

The BEETSDIR test runs the REAL harness on the REAL beets (dev-shell
python) — it proves the whole chain production uses: wrapper → pinned
interpreter → beets config.read() honouring BEETSDIR → the Palo Santo
duplicate_keys guard evaluating the module-rendered config shape.
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
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("--search-id", proc.stdout)

    def test_harness_reads_beets_config_from_BEETSDIR(self) -> None:
        """Launch exactly as production does (wrapper + env) against a
        BEETSDIR whose config VIOLATES the duplicate_keys invariant: the
        harness must read THAT config (not ~/.config/beets) and die with
        the Palo Santo guard's message."""
        with tempfile.TemporaryDirectory() as beetsdir:
            with open(os.path.join(beetsdir, "config.yaml"), "w",
                      encoding="utf-8") as f:
                f.write("library: {}/lib.db\n".format(beetsdir))
            proc = subprocess.run(
                [str(SCRIPT), "--pretend", beetsdir],
                env={**os.environ,
                     "CRATEDIGGER_BEETS_PYTHON": sys.executable,
                     "BEETSDIR": beetsdir},
                capture_output=True, text=True, input="",
            )
        self.assertEqual(proc.returncode, 1, proc.stderr)
        self.assertIn("duplicate_keys.album must be exactly", proc.stderr)

    def test_harness_accepts_module_shaped_config(self) -> None:
        """Same launch, but the BEETSDIR carries the module-rendered
        duplicate_keys shape — the guard passes and the harness proceeds
        past config validation (empty dir -> clean run, no albums)."""
        with tempfile.TemporaryDirectory() as beetsdir, \
                tempfile.TemporaryDirectory() as emptydir:
            with open(os.path.join(beetsdir, "config.yaml"), "w",
                      encoding="utf-8") as f:
                f.write(
                    "library: {}/lib.db\n"
                    "directory: {}/music\n"
                    "import:\n"
                    "  duplicate_keys:\n"
                    "    album: [mb_albumid, discogs_albumid]\n"
                    "    item: [artist, title]\n".format(beetsdir, beetsdir)
                )
            proc = subprocess.run(
                [str(SCRIPT), "--pretend", emptydir],
                env={**os.environ,
                     "CRATEDIGGER_BEETS_PYTHON": sys.executable,
                     "BEETSDIR": beetsdir},
                capture_output=True, text=True, input="",
                timeout=120,
            )
        self.assertNotIn("duplicate_keys.album must be exactly", proc.stderr)
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
