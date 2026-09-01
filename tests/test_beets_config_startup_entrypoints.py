"""Entrypoint-placement tests for startup-only Beets authority checks."""

from __future__ import annotations

import builtins
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cratedigger
from tests.beets_config_startup_support import (
    _isolated_installed_authority,
    _PostGateEffect,
    _record_admission_events,
    _restart_argv,
    _RestartCase,
    _snapshot_runtime_tree,
)
from tests.fakes.beets_contract import (
    BeetsContractWorld,
    assert_redacted_load_failure,
)
from web import server


class TestEntrypointStartupPlacement(unittest.TestCase):
    def setUp(self) -> None:
        from lib import config as runtime_config_module

        prior_runtime_config = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
        prior_beetsdir = os.environ.get("BEETSDIR")
        prior_admitted = runtime_config_module._admitted_runtime_config

        def restore_runtime_config() -> None:
            if prior_runtime_config is None:
                os.environ.pop("CRATEDIGGER_RUNTIME_CONFIG", None)
            else:
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = prior_runtime_config
            if prior_beetsdir is None:
                os.environ.pop("BEETSDIR", None)
            else:
                os.environ["BEETSDIR"] = prior_beetsdir
            runtime_config_module._admitted_runtime_config = prior_admitted

        self.addCleanup(restore_runtime_config)
        # No web globals to save: `web/server.py::main` builds one
        # `WebRuntime` and installs it only around its serve block, so a
        # rejected startup leaves no process state of that kind behind
        # (#1313).
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config_path = os.path.join(self.tmp.name, "runtime.ini")
        Path(self.config_path).write_text("[Pipeline DB]\nenabled = true\n")
        self.fixture = Path(self.tmp.name) / "fixture.bin"
        self.fixture.write_bytes(b"no application state")

    def test_main_invalid_utf8_precedes_umask_lock_and_runtime_effects(
        self,
    ) -> None:
        from lib import config as runtime_config_module

        invalid = Path(self.tmp.name) / "invalid-utf8.ini"
        invalid.write_bytes(b"[Beets]\nconfig_dir = \xff\n")
        runtime_root = Path(self.tmp.name)
        before_runtime = _snapshot_runtime_tree(runtime_root)
        lock_path = runtime_root / ".cratedigger.lock"
        prior_runtime = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
        prior_beetsdir = os.environ.get("BEETSDIR")
        prior_admitted = runtime_config_module._admitted_runtime_config
        real_open = builtins.open
        lock_opens: list[str] = []

        def observe_open(
            file: str | bytes | int | os.PathLike[str] | os.PathLike[bytes],
            mode: str = "r",
            *,
            encoding: str | None = None,
        ):
            opened_path = (
                file
                if isinstance(file, str)
                else str(file) if isinstance(file, os.PathLike) else ""
            )
            if opened_path == str(lock_path):
                lock_opens.append(opened_path)
            return real_open(file, mode, encoding=encoding)

        self.assertFalse(lock_path.exists())
        saved_umask = os.umask(0o022)
        try:
            with (
                patch.object(sys, "argv", [
                    "cratedigger.py",
                    "--config", str(invalid),
                    "--runtime-dir", self.tmp.name,
                ]),
                patch("builtins.open", side_effect=observe_open),
            ):
                self.assertEqual(cratedigger.main(), 1)
        finally:
            observed_umask = os.umask(saved_umask)

        self.assertEqual(observed_umask, 0o022)
        self.assertEqual(lock_opens, [])
        self.assertFalse(lock_path.exists())
        self.assertEqual(_snapshot_runtime_tree(runtime_root), before_runtime)
        self.assertEqual(os.environ.get("CRATEDIGGER_RUNTIME_CONFIG"), prior_runtime)
        self.assertEqual(os.environ.get("BEETSDIR"), prior_beetsdir)
        self.assertIs(runtime_config_module._admitted_runtime_config, prior_admitted)
        self.assertEqual(self.fixture.read_bytes(), b"no application state")

    def test_main_admission_precedes_umask_lock_and_database_effects(self) -> None:
        world = BeetsContractWorld(role="main")
        self.addCleanup(world.close)
        events: list[str] = []
        lock_path = world.runtime_dir / ".cratedigger.lock"
        real_open = builtins.open
        real_umask = os.umask

        def observe_open(
            file: str | bytes | int | os.PathLike[str] | os.PathLike[bytes],
            mode: str = "r",
            *,
            encoding: str | None = None,
        ):
            opened_path = (
                file
                if isinstance(file, str)
                else str(file) if isinstance(file, os.PathLike) else ""
            )
            if opened_path == str(lock_path):
                events.append("lock")
            return real_open(file, mode, encoding=encoding)

        def observe_umask(mask: int) -> int:
            events.append("umask")
            return real_umask(mask)

        def observe_database(*_args: object, **_kwargs: object) -> None:
            events.append("database")
            raise _PostGateEffect

        saved_umask = os.umask(0o022)
        try:
            with (
                _isolated_installed_authority(),
                patch.object(sys, "argv", _restart_argv(
                    _RestartCase("main", cratedigger.main),
                    world,
                )),
                _record_admission_events(
                    _RestartCase("main", cratedigger.main),
                    events,
                ),
                patch("builtins.open", side_effect=observe_open),
                patch("lib.permissions.os.umask", side_effect=observe_umask),
                patch(
                    "lib.migrator.psycopg2.connect",
                    side_effect=observe_database,
                ),
                self.assertRaises(_PostGateEffect),
            ):
                cratedigger.main()
        finally:
            real_umask(saved_umask)

        self.assertEqual(events, ["admitted", "umask", "lock", "database"])

    def test_web_hard_failure_precedes_production_listener_takeover(self) -> None:
        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(**{
            "import": {
                "autotag": True,
                "move": True,
                "write": False,
                "duplicate_keys": {
                    "album": ["mb_albumid", "discogs_albumid"],
                },
            },
        })
        world._seal("web")
        argv = [
            "server.py",
            "--config", str(world.runtime_config),
            "--runtime-dir", str(world.runtime_dir),
            "--canonical-origin", "https://music.example",
        ]
        environment = {
            "LISTEN_PID": str(os.getpid()),
            "LISTEN_FDS": "1",
        }

        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", argv),
            patch.dict(os.environ, environment, clear=False),
            patch.object(socket, "socket") as listener_takeover,
        ):
            self.assertEqual(server.main(), 1)

        listener_takeover.assert_not_called()

    def test_web_admission_precedes_beets_rebind_and_listener_takeover(self) -> None:
        from beets import config as active_beets_config

        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        events: list[str] = []
        case = _RestartCase("web", server.main)
        real_clear = active_beets_config.clear
        real_read = active_beets_config.read

        def observe_clear() -> None:
            events.append("beets-clear")
            real_clear()

        def observe_read(*, user: bool, defaults: bool) -> None:
            events.append("beets-read")
            real_read(user=user, defaults=defaults)

        def observe_listener(*, fileno: int) -> None:
            self.assertEqual(fileno, 3)
            events.append("listener")
            raise _PostGateEffect

        argv = [
            "server.py",
            "--config", str(world.runtime_config),
            "--runtime-dir", str(world.runtime_dir),
            "--canonical-origin", "https://music.example",
        ]
        environment = {
            "LISTEN_PID": str(os.getpid()),
            "LISTEN_FDS": "1",
        }
        try:
            with (
                _isolated_installed_authority(),
                patch.object(sys, "argv", argv),
                patch.dict(os.environ, environment, clear=False),
                _record_admission_events(case, events),
                patch.object(active_beets_config, "clear", side_effect=observe_clear),
                patch.object(active_beets_config, "read", side_effect=observe_read),
                patch.object(socket, "socket", side_effect=observe_listener),
                self.assertRaises(_PostGateEffect),
            ):
                server.main()
        finally:
            active_beets_config.clear()
            active_beets_config.read(user=True, defaults=True)

        self.assertEqual(
            events,
            ["admitted", "beets-clear", "beets-read", "listener"],
        )

    def test_real_script_boundaries_exit_nonzero_without_state_mutation(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        invalid = Path(self.tmp.name) / "subprocess-invalid.ini"
        token = "PLANTED_TOKEN_759_REAL_ENTRYPOINTS"
        invalid.write_text(
            f"[Beets\nuser_token = [{token}\n",
            encoding="utf-8",
        )
        runtime_dir = Path(self.tmp.name) / "subprocess-runtime"
        runtime_dir.mkdir()
        before = _snapshot_runtime_tree(Path(self.tmp.name))
        # Each entrypoint's own OWN documented rejection exit code — NOT
        # uniformly 1 (#1142 review N9): the retag-divergence census
        # oneshot returns its own EXIT_CONFIG_ABORT (2), distinct from
        # the other four real entrypoints' shared BeetsStartupError
        # exit-1 convention.
        commands = (
            (
                "main",
                [sys.executable, "cratedigger.py"],
                (),
                1,
            ),
            (
                "importer",
                [sys.executable, "scripts/importer.py"],
                ("--once",),
                1,
            ),
            (
                "preview",
                [sys.executable, "scripts/import_preview_worker.py"],
                ("--once",),
                1,
            ),
            (
                "web",
                [sys.executable, "web/server.py"],
                ("--canonical-origin", "https://music.example", "--dev-port", "0"),
                1,
            ),
            (
                "retag-census",
                [sys.executable, "scripts/run_retag_divergence_census.py"],
                (),
                2,
            ),
            (
                "library-completeness-census",
                [sys.executable, "scripts/run_library_completeness_census.py"],
                (),
                2,
            ),
        )

        for role, executable, extra, expected_exit_code in commands:
            with self.subTest(role=role):
                proc = subprocess.run(
                    [
                        *executable,
                        "--config", str(invalid),
                        "--runtime-dir", str(runtime_dir),
                        *extra,
                    ],
                    cwd=repo_root,
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(proc.returncode, expected_exit_code, proc.stderr)
                output = proc.stderr + proc.stdout
                self.assertIn("runtime_config_load_error", output)
                assert_redacted_load_failure(output, token)
                self.assertEqual(
                    _snapshot_runtime_tree(Path(self.tmp.name)),
                    before,
                )

    def test_web_rejects_retired_post_check_beets_overrides(self) -> None:
        for flag in ("--beets-db", "--beets-directory"):
            with self.subTest(flag=flag), patch.object(sys, "argv", [
                "server.py",
                "--config", self.config_path,
                "--runtime-dir", self.tmp.name,
                "--canonical-origin", "https://music.example",
                flag, "/post-check/override",
            ]), self.assertRaises(SystemExit) as raised:
                server.main()
            self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
