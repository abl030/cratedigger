"""Exact process and wire contracts for the import-one subprocess runner."""

from __future__ import annotations

import os
import subprocess as sp
import sys
import tempfile
import threading
import unittest
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from lib.dispatch.subprocess_runner import (
    build_import_one_command,
    run_import_one,
)
from lib.import_execution import CancellationToken

_BEETS_CONFIG_DIR = "/pinned/beets-config"
_BEETS_LIBRARY_DB = "/pinned/library.db"
_BEETS_LIBRARY_ROOT = "/pinned/music"


class _ImmediateTimer:
    """Deterministic deadline timer that records its daemon contract."""

    def __init__(self, interval: float, callback: Callable[[], None]) -> None:
        self.interval = interval
        self.callback = callback
        self.daemon: bool | None = None

    def start(self) -> None:
        self.callback()

    def cancel(self) -> None:
        pass

    def join(self) -> None:
        pass


@contextmanager
def _replace_timer(
    factory: Callable[[float, Callable[[], None]], _ImmediateTimer],
) -> Iterator[None]:
    """Install a typed timer fake and restore the process-global binding."""
    original = threading.Timer
    threading.__dict__["Timer"] = factory
    try:
        yield
    finally:
        threading.__dict__["Timer"] = original


class _FinishedGroup:
    """Typed process-group stand-in for deadline result contracts."""

    def __init__(self, termination_error: BaseException | None = None) -> None:
        self.termination_error = termination_error
        self.terminations = 0

    @property
    def pid(self) -> int:
        return 4321

    def terminate_and_wait(self, *, timeout: float = 5.0) -> int:
        del timeout
        self.terminations += 1
        if self.termination_error is not None:
            raise self.termination_error
        return -15

    def wait(
        self,
        token: CancellationToken,
        *,
        owner_session_probe: Callable[[], bool] | None = None,
        probe_interval: float = 1.0,
    ) -> int:
        del token, owner_session_probe, probe_interval
        return -15


class TestImportOneCommandWireContract(unittest.TestCase):
    def test_default_command_has_no_optional_mode_flags(self) -> None:
        command = build_import_one_command(
            path="/incoming/album",
            mb_release_id="release-1",
            beets_harness_path="/opt/harness/run-harness",
        )

        self.assertEqual(
            command,
            [
                sys.executable,
                "/opt/harness/import_one.py",
                "/incoming/album",
                "release-1",
            ],
        )

    def test_request_and_dry_run_are_exact_argv_fields(self) -> None:
        command = build_import_one_command(
            path="/incoming/album",
            mb_release_id="release-1",
            beets_harness_path="/opt/harness/run-harness",
            request_id=17,
            dry_run=True,
        )

        self.assertEqual(
            command,
            [
                sys.executable,
                "/opt/harness/import_one.py",
                "/incoming/album",
                "release-1",
                "--request-id",
                "17",
                "--dry-run",
            ],
        )


class TestImportOneSimpleProcessContract(unittest.TestCase):
    def test_defaults_and_process_options_reach_subprocess_run_exactly(self) -> None:
        calls: list[tuple[list[str], dict[str, object]]] = []

        def completed(command: list[str], **kwargs: object) -> sp.CompletedProcess[str]:
            check = kwargs.pop("check", False)
            if check:
                raise sp.CalledProcessError(7, command, "child-out", "child-err")
            calls.append((command, kwargs))
            return sp.CompletedProcess(command, 7, stdout="child-out", stderr="child-err")

        with patch("lib.dispatch.subprocess_runner.sp.run", side_effect=completed):
            result = run_import_one(
                path="/incoming/album",
                mb_release_id="release-1",
                beets_harness_path="/opt/harness/run-harness",
                beets_config_dir=_BEETS_CONFIG_DIR,
                beets_library_db_path=_BEETS_LIBRARY_DB,
                beets_library_root=_BEETS_LIBRARY_ROOT,
            )

        self.assertEqual(
            calls,
            [(
                [
                    sys.executable,
                    "/opt/harness/import_one.py",
                    "/incoming/album",
                    "release-1",
                    "--beets-library-db",
                    "/pinned/library.db",
                    "--beets-library-root",
                    "/pinned/music",
                    "--beets-config-dir",
                    "/pinned/beets-config",
                ],
                {
                    "capture_output": True,
                    "text": True,
                    "errors": "replace",
                    "timeout": 1800,
                    "env": {
                        **os.environ,
                        "BEETSDIR": "/pinned/beets-config",
                        "BEETS_DB": "/pinned/library.db",
                    },
                },
            )],
        )
        self.assertEqual(result.command, tuple(calls[0][0]))
        self.assertEqual(result.returncode, 7)
        self.assertEqual(result.stdout, "child-out")
        self.assertEqual(result.stderr, "child-err")
        self.assertIsNone(result.import_result)

    def test_explicit_request_and_dry_run_reach_the_result_command(self) -> None:
        def completed(command: list[str], **_kwargs: object) -> sp.CompletedProcess[str]:
            return sp.CompletedProcess(command, 0, stdout="", stderr="")

        with patch("lib.dispatch.subprocess_runner.sp.run", side_effect=completed):
            result = run_import_one(
                path="/incoming/album",
                mb_release_id="release-1",
                beets_harness_path="/opt/harness/run-harness",
                request_id=17,
                dry_run=True,
                beets_config_dir=_BEETS_CONFIG_DIR,
                beets_library_db_path=_BEETS_LIBRARY_DB,
                beets_library_root=_BEETS_LIBRARY_ROOT,
            )

        request_flag_index = result.command.index("--request-id")
        self.assertEqual(result.command[request_flag_index : request_flag_index + 2], (
            "--request-id",
            "17",
        ))
        self.assertIn("--dry-run", result.command)

    def test_none_outputs_are_normalized_to_empty_text(self) -> None:
        def completed(command: list[str], **_kwargs: object) -> sp.CompletedProcess[str]:
            return sp.CompletedProcess(command, 0, stdout=None, stderr=None)

        with patch("lib.dispatch.subprocess_runner.sp.run", side_effect=completed):
            result = run_import_one(
                path="/incoming/album",
                mb_release_id="release-1",
                beets_harness_path="/opt/harness/run-harness",
                beets_config_dir=_BEETS_CONFIG_DIR,
                beets_library_db_path=_BEETS_LIBRARY_DB,
                beets_library_root=_BEETS_LIBRARY_ROOT,
            )

        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_supervision_options_require_a_cancellation_token(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^spawn callbacks and owner-session probes require cancellation$",
        ):
            run_import_one(
                path="/incoming/album",
                mb_release_id="release-1",
                beets_harness_path="/opt/harness/run-harness",
                on_spawn=lambda _pid: None,
                beets_config_dir=_BEETS_CONFIG_DIR,
                beets_library_db_path=_BEETS_LIBRARY_DB,
                beets_library_root=_BEETS_LIBRARY_ROOT,
            )


class TestImportOneSupervisedProcessContract(unittest.TestCase):
    def test_real_child_receives_env_and_captures_complete_replacement_decoded_output(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            harness = root / "run-harness"
            script = root / "import_one.py"
            script.write_text(
                "import os\n"
                "os.write(1, b'env=' + os.environ['BEETSDIR'].encode() + b'\\n')\n"
                "os.write(1, b'prefix-\\xff-suffix\\n')\n"
                "os.write(2, b'error-\\xfe-end\\n')\n"
                "raise SystemExit(7)\n"
            )

            result = run_import_one(
                path="/incoming/album",
                mb_release_id="release-1",
                beets_harness_path=str(harness),
                timeout=10,
                cancellation_token=CancellationToken(),
                beets_config_dir=_BEETS_CONFIG_DIR,
                beets_library_db_path=_BEETS_LIBRARY_DB,
                beets_library_root=_BEETS_LIBRARY_ROOT,
            )

        self.assertEqual(result.returncode, 7)
        self.assertEqual(
            result.stdout,
            "env=/pinned/beets-config\nprefix-\ufffd-suffix\n",
        )
        self.assertEqual(result.stderr, "error-\ufffd-end\n")
        self.assertEqual(result.command[1], str(script))
        self.assertIsNone(result.import_result)

    def test_timeout_preserves_exact_command_timeout_and_daemon_timer(self) -> None:
        timers: list[_ImmediateTimer] = []
        group = _FinishedGroup()

        def timer_factory(interval: float, callback: Callable[[], None]) -> _ImmediateTimer:
            timer = _ImmediateTimer(interval, callback)
            timers.append(timer)
            return timer

        def group_factory(_process: sp.Popen[bytes]) -> _FinishedGroup:
            return group

        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen", return_value=object()),
            _replace_timer(timer_factory),
            self.assertRaises(sp.TimeoutExpired) as caught,
        ):
            run_import_one(
                path="/incoming/album",
                mb_release_id="release-1",
                beets_harness_path="/opt/harness/run-harness",
                timeout=23,
                cancellation_token=CancellationToken(),
                process_group_factory=group_factory,
                beets_config_dir=_BEETS_CONFIG_DIR,
                beets_library_db_path=_BEETS_LIBRARY_DB,
                beets_library_root=_BEETS_LIBRARY_ROOT,
            )

        expected_command = build_import_one_command(
            path="/incoming/album",
            mb_release_id="release-1",
            beets_harness_path="/opt/harness/run-harness",
            beets_config_dir=_BEETS_CONFIG_DIR,
            beets_library_db_path=_BEETS_LIBRARY_DB,
            beets_library_root=_BEETS_LIBRARY_ROOT,
        )
        self.assertEqual(caught.exception.cmd, expected_command)
        self.assertEqual(caught.exception.timeout, 23)
        self.assertEqual(group.terminations, 1)
        self.assertEqual(len(timers), 1)
        self.assertEqual(timers[0].interval, 23)
        self.assertIs(timers[0].daemon, True)

    def test_deadline_termination_failure_propagates_the_original_exception(self) -> None:
        termination_error = RuntimeError("termination failed")
        group = _FinishedGroup(termination_error)

        def timer_factory(interval: float, callback: Callable[[], None]) -> _ImmediateTimer:
            return _ImmediateTimer(interval, callback)

        def group_factory(_process: sp.Popen[bytes]) -> _FinishedGroup:
            return group

        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen", return_value=object()),
            _replace_timer(timer_factory),
            self.assertRaises(RuntimeError) as caught,
        ):
            run_import_one(
                path="/incoming/album",
                mb_release_id="release-1",
                beets_harness_path="/opt/harness/run-harness",
                timeout=23,
                cancellation_token=CancellationToken(),
                process_group_factory=group_factory,
                beets_config_dir=_BEETS_CONFIG_DIR,
                beets_library_db_path=_BEETS_LIBRARY_DB,
                beets_library_root=_BEETS_LIBRARY_ROOT,
            )

        self.assertIs(caught.exception, termination_error)
        self.assertEqual(group.terminations, 1)


if __name__ == "__main__":
    unittest.main()
