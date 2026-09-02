"""Direct pins for the fake-command fixtures' inherited environment."""

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from tests.fakes.subprocess_env import (
    BYTECODE_CACHE_OPT_OUT_VARS,
    inherited_environment,
)


class BytecodeCacheOptOutNamesTestCase(unittest.TestCase):
    """Each name is proved against CPython, which is what produces the effect.

    Everything else in this file derives its expectations from the constant,
    so pointing the constant at plausible near-misses
    (``PYTHONDONTWRITEBYTECODES``, ``PYTHON_PYCACHEPREFIX``) left all ten
    tests green whenever the reviewer's own shell happened not to export the
    real variable (review round, mutant M10). Rule C's shape: the trigger's
    producer is the interpreter, so ask the interpreter.
    """

    def _interpreter_reports(self, name: str, value: str, expression: str) -> str:
        environment = {
            key: item
            for key, item in os.environ.items()
            if key not in BYTECODE_CACHE_OPT_OUT_VARS
        }
        environment[name] = value
        result = subprocess.run(
            [sys.executable, "-c", f"import sys; print({expression})"],
            env=environment,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def test_the_dont_write_name_really_disables_the_write(self) -> None:
        name = "PYTHONDONTWRITEBYTECODE"

        self.assertIn(name, BYTECODE_CACHE_OPT_OUT_VARS)
        self.assertEqual(
            self._interpreter_reports(name, "1", "sys.dont_write_bytecode"),
            "True",
        )

    def test_the_cache_prefix_name_really_moves_the_cache(self) -> None:
        name = "PYTHONPYCACHEPREFIX"

        self.assertIn(name, BYTECODE_CACHE_OPT_OUT_VARS)
        self.assertEqual(
            self._interpreter_reports(name, "/tmp/elsewhere", "sys.pycache_prefix"),
            "/tmp/elsewhere",
        )

    def test_no_other_name_is_stripped(self) -> None:
        """Must still work: this drops exactly the two proved above. A third
        entry would silently remove something the fixtures may need."""
        self.assertEqual(
            set(BYTECODE_CACHE_OPT_OUT_VARS),
            {"PYTHONDONTWRITEBYTECODE", "PYTHONPYCACHEPREFIX"},
        )


class InheritedEnvironmentTestCase(unittest.TestCase):
    def test_the_bytecode_cache_opt_outs_are_dropped(self) -> None:
        """The whole point: a mutant runner's mandatory
        PYTHONDONTWRITEBYTECODE (and its PYTHONPYCACHEPREFIX sibling) must
        not reach a fixture's fake commands, where it defeats the shared
        shim's bytecode cache (issue #1313, 1329-2)."""
        with patch.dict(
            os.environ,
            {name: "1" for name in BYTECODE_CACHE_OPT_OUT_VARS},
        ):
            environment = inherited_environment()

        for name in BYTECODE_CACHE_OPT_OUT_VARS:
            with self.subTest(name=name):
                self.assertNotIn(name, environment)

    def test_everything_else_is_inherited_unchanged(self) -> None:
        """Must still work: this drops two names, not the environment. A
        fixture that lost PATH or TMPDIR here would fail in ways nothing
        below traces back to this function."""
        marker = "CRATEDIGGER_SUBPROCESS_ENV_MARKER"
        with patch.dict(os.environ, {marker: "kept"}):
            environment = inherited_environment()
            expected = {
                name: value
                for name, value in os.environ.items()
                if name not in BYTECODE_CACHE_OPT_OUT_VARS
            }

        self.assertEqual(environment, expected)
        self.assertEqual(environment[marker], "kept")

    def test_an_absent_opt_out_is_not_an_error(self) -> None:
        """The ordinary case — nobody exported either variable."""
        stripped = {
            name: value
            for name, value in os.environ.items()
            if name not in BYTECODE_CACHE_OPT_OUT_VARS
        }
        with patch.dict(os.environ, stripped, clear=True):
            environment = inherited_environment()

        self.assertEqual(environment, stripped)

    def test_the_caller_cannot_mutate_os_environ_through_the_result(self) -> None:
        """A plain dict, not a live view of ``os.environ``: fixtures add
        their own PATH and state paths to it, and those must not leak back
        into this process."""
        environment = inherited_environment()
        environment["CRATEDIGGER_SUBPROCESS_ENV_LEAK"] = "1"

        self.assertNotIn("CRATEDIGGER_SUBPROCESS_ENV_LEAK", os.environ)


if __name__ == "__main__":
    unittest.main()
