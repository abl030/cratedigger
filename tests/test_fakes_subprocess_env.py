"""Direct pins for the fake-command fixtures' inherited environment."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from tests.fakes.subprocess_env import (
    BYTECODE_CACHE_OPT_OUT_VARS,
    inherited_environment,
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
