"""Contract tests for the shared pinned-Beets child spawner (#1278 item 4).

``lib/beets_child.py`` owns the mechanics every run-to-completion Beets
mutation child shares — interpreter/env resolution through
``lib/util.py::beets_subprocess_env``, the ``[<beets python>, *argv_tail]``
argv shape, timeout and captured output, and the decoded diagnostic record.
The lanes own their argv tails and their evidence mechanisms; nothing here
decides whether a mutation landed.
"""

from __future__ import annotations

import dataclasses
import os
import subprocess as sp
import tempfile
import unittest
from collections.abc import Generator
from contextlib import contextmanager
from unittest.mock import patch

from lib.beets_child import (
    BeetsChildRun,
    harness_session_argv,
    run_pinned_beets_child,
    spawn_harness_session,
)
from lib.util import beets_subprocess_env

FAKE_PYTHON = "/nix/store/fake-beets/bin/python3"
CONFIG_INI = (
    "[Beets]\nconfig_dir = /var/lib/cratedigger/beets\n"
    f"python = {FAKE_PYTHON}\n"
)


class _RecordingRunner:
    """Injected runner recording exactly what the spawner asked for."""

    def __init__(self, proc: sp.CompletedProcess[bytes] | None = None) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []
        self._proc = proc

    def __call__(
        self, argv: list[str], **kwargs: object,
    ) -> sp.CompletedProcess[bytes]:
        self.calls.append((argv, kwargs))
        if self._proc is None:
            return sp.CompletedProcess(argv, 0, b"", b"")
        return self._proc


@contextmanager
def runtime_config(ini_text: str = CONFIG_INI) -> Generator[None]:
    """Point the runtime config at a throwaway ini naming a fake pinned
    interpreter, so no test here depends on the dev shell's real one."""
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "config.ini")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(ini_text)
        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": path},
            clear=False,
        ):
            yield


class TestRunPinnedBeetsChild(unittest.TestCase):
    def test_argv_begins_with_the_pinned_interpreter_then_the_tail_verbatim(
        self,
    ) -> None:
        """The interpreter is the resolved ``CRATEDIGGER_BEETS_PYTHON`` —
        never a ``beet`` binary from this process's PATH — and the lane's
        tail follows it unmodified, each token a separate argv element."""
        runner = _RecordingRunner(
            sp.CompletedProcess([], 0, b"wrote 3 items\n", b""),
        )
        with runtime_config():
            expected_env = beets_subprocess_env()
            proc = run_pinned_beets_child(
                ["-m", "beets", "write", "album_id:=7", "mb_albumid:=x"],
                timeout=42,
                runner=runner,
            )

        argv, kwargs = runner.calls[0]
        self.assertEqual(argv, [
            FAKE_PYTHON, "-m", "beets", "write", "album_id:=7", "mb_albumid:=x",
        ])
        env = kwargs["env"]
        assert isinstance(env, dict)
        self.assertEqual(env["BEETSDIR"], "/var/lib/cratedigger/beets")
        self.assertEqual(env["CRATEDIGGER_BEETS_PYTHON"], FAKE_PYTHON)
        # The WHOLE resolved environment, not just the two beets keys — a
        # spawner that filters PATH/HOME/BEETS_DB out would satisfy the two
        # key pins while breaking every lane at once (review round, reader
        # finding 5).
        self.assertEqual(env, expected_env)
        self.assertEqual(kwargs["timeout"], 42)
        self.assertIs(kwargs["capture_output"], True)
        self.assertNotIn("input", kwargs)
        self.assertEqual(proc.stdout, b"wrote 3 items\n")

    def test_input_bytes_are_forwarded_to_the_child_stdin(self) -> None:
        runner = _RecordingRunner()
        with runtime_config():
            run_pinned_beets_child(
                ["/repo/harness/delete_album.py"],
                timeout=60,
                input_bytes=b'{"album_id": 7}',
                runner=runner,
            )

        _, kwargs = runner.calls[0]
        self.assertEqual(kwargs["input"], b'{"album_id": 7}')

    def test_an_unconfigured_interpreter_refuses_before_launch(self) -> None:
        def runner(
            argv: list[str], **kwargs: object,
        ) -> sp.CompletedProcess[bytes]:
            raise AssertionError("must not launch without a pinned interpreter")

        stripped = {
            key: value
            for key, value in os.environ.items()
            if key != "CRATEDIGGER_BEETS_PYTHON"
        }
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "config.ini")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "[Beets]\nconfig_dir = /var/lib/cratedigger/beets\n",
                )
            stripped["CRATEDIGGER_RUNTIME_CONFIG"] = path
            with patch.dict(os.environ, stripped, clear=True), \
                    self.assertRaises(RuntimeError) as caught:
                run_pinned_beets_child(
                    ["-m", "beets", "write"], timeout=1, runner=runner,
                )

        self.assertIn("CRATEDIGGER_BEETS_PYTHON", str(caught.exception))

    def test_an_unset_config_dir_refuses_before_launch(self) -> None:
        """``beets_subprocess_env``'s own fail-closed refusal (no silent
        ~/.config/beets fallback) reaches this spawner's callers intact."""
        def runner(
            argv: list[str], **kwargs: object,
        ) -> sp.CompletedProcess[bytes]:
            raise AssertionError("must not launch without a config dir")

        stripped = {
            key: value
            for key, value in os.environ.items()
            if key not in ("BEETSDIR", "CRATEDIGGER_BEETS_PYTHON")
        }
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "config.ini")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(f"[Beets]\npython = {FAKE_PYTHON}\n")
            stripped["CRATEDIGGER_RUNTIME_CONFIG"] = path
            with patch.dict(os.environ, stripped, clear=True), \
                    self.assertRaises(RuntimeError) as caught:
                run_pinned_beets_child(
                    ["-m", "beets", "write"], timeout=1, runner=runner,
                )

        self.assertIn("config dir", str(caught.exception))

    def test_launch_and_timeout_failures_propagate_to_the_caller(self) -> None:
        """The spawner converts nothing: each lane owns its own failure
        typing. The fakes raise the REAL exception classes ``sp.run`` can
        raise (test-fidelity Rule B)."""
        def timing_out(
            argv: list[str], **kwargs: object,
        ) -> sp.CompletedProcess[bytes]:
            raise sp.TimeoutExpired(cmd=argv, timeout=42)

        def unlaunchable(
            argv: list[str], **kwargs: object,
        ) -> sp.CompletedProcess[bytes]:
            raise OSError("No such file or directory")

        with runtime_config():
            with self.assertRaises(sp.TimeoutExpired):
                run_pinned_beets_child(
                    ["-m", "beets", "write"], timeout=42, runner=timing_out,
                )
            with self.assertRaises(OSError):
                run_pinned_beets_child(
                    ["-m", "beets", "write"], timeout=42, runner=unlaunchable,
                )


class TestBeetsChildRun(unittest.TestCase):
    def test_construction_is_frozen(self) -> None:
        run = BeetsChildRun(returncode=1, stdout="out", stderr="err")
        self.assertEqual(run.returncode, 1)
        self.assertEqual(run.stdout, "out")
        self.assertEqual(run.stderr, "err")
        self.assertRaises(
            dataclasses.FrozenInstanceError, setattr, run, "returncode", 0,
        )

    def test_from_completed_decodes_utf8_with_replacement(self) -> None:
        """Non-UTF-8 bytes in a child's streams (CP1252-tagged metadata
        echoed by beets) must never raise during capture — the lanes fold
        these strings into diagnostics, not decisions."""
        run = BeetsChildRun.from_completed(
            sp.CompletedProcess([], 3, b"\xffout", b"\xfeerr"),
        )
        self.assertEqual(run.returncode, 3)
        self.assertEqual(run.stdout, "�out")
        self.assertEqual(run.stderr, "�err")


class TestHarnessSessionArgv(unittest.TestCase):
    """Exhaustive over the builder's whole 2x2 flag domain — the validation
    session and the real import differ ONLY by ``--pretend``, and both
    lanes' historical argv shapes are pinned byte-for-byte."""

    def test_every_flag_combination_produces_the_exact_argv(self) -> None:
        wrapper = "/nix/store/x/harness/run_beets_harness.sh"
        cases = [
            (
                "import",
                False, False,
                [wrapper, "--noincremental", "--search-id", "mb-1", "/a"],
            ),
            (
                "import preserving flat subtracks",
                False, True,
                [
                    wrapper, "--noincremental",
                    "--preserve-discogs-flat-subtracks",
                    "--search-id", "mb-1", "/a",
                ],
            ),
            (
                "validation",
                True, False,
                [
                    wrapper, "--pretend", "--noincremental",
                    "--search-id", "mb-1", "/a",
                ],
            ),
            (
                "validation preserving flat subtracks",
                True, True,
                [
                    wrapper, "--pretend", "--noincremental",
                    "--preserve-discogs-flat-subtracks",
                    "--search-id", "mb-1", "/a",
                ],
            ),
        ]
        for desc, pretend, preserve, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(
                    harness_session_argv(
                        wrapper,
                        mb_release_id="mb-1",
                        album_path="/a",
                        pretend=pretend,
                        preserve_discogs_flat_subtracks=preserve,
                    ),
                    expected,
                )


class TestSpawnHarnessSession(unittest.TestCase):
    def test_spawns_a_real_text_mode_child_with_the_beets_env(self) -> None:
        """No mocks: a real child proves the production spawner passes
        ``beets_subprocess_env()``'s environment (the Blueline Medic
        0-candidates incident class — a harness child resolving config
        from the wrong environment), pipes all three streams, and runs in
        text mode."""
        import sys

        with runtime_config():
            child_source = (
                "import os, sys\n"
                "sys.stdout.write(os.environ['BEETSDIR'] + '\\n')\n"
                "sys.stdout.write(sys.stdin.readline())\n"
            )
            proc = spawn_harness_session([sys.executable, "-c", child_source])
            stdin = proc.stdin
            stdout = proc.stdout
            assert stdin is not None
            assert stdout is not None
            stdin.write("echoed\n")
            stdin.flush()
            lines = list(stdout)
            returncode = proc.wait(timeout=30)

        self.assertEqual(lines, ["/var/lib/cratedigger/beets\n", "echoed\n"])
        self.assertEqual(returncode, 0)


if __name__ == "__main__":
    unittest.main()
