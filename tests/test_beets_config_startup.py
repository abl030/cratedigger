"""Startup-only enforcement of the external Beets authority (issue #759)."""

from __future__ import annotations

import builtins
import logging
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import cratedigger
from lib.beets_config_contract import BeetsRole
from lib.config import CratediggerConfig
from scripts import import_preview_worker, importer
from tests.test_beets_config_contract import (
    BeetsContractWorld,
    snapshot_contract_world,
)
from web import server


class _PostGateEffect(Exception):
    """Stop an entrypoint at its first admitted application effect."""


class _AdmissionEventHandler(logging.Handler):
    """Record only the real startup adapter's admitted-result log."""

    def __init__(self, role: BeetsRole, events: list[str]) -> None:
        super().__init__(level=logging.INFO)
        self._admission_prefix = "Beets configuration admitted for "
        self._expected_prefix = f"{self._admission_prefix}{role} "
        self._events = events

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message.startswith(self._admission_prefix):
            self._events.append(
                "admitted"
                if message.startswith(self._expected_prefix)
                else "wrong-role-admission"
            )


@contextmanager
def _isolated_installed_authority() -> Iterator[None]:
    """Restore process-global startup authority after a real admission."""
    from lib import config as runtime_config_module

    prior_runtime = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
    prior_beetsdir = os.environ.get("BEETSDIR")
    prior_admitted = runtime_config_module._admitted_runtime_config
    runtime_config_module._admitted_runtime_config = None
    try:
        yield
    finally:
        runtime_config_module._admitted_runtime_config = prior_admitted
        if prior_runtime is None:
            os.environ.pop("CRATEDIGGER_RUNTIME_CONFIG", None)
        else:
            os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = prior_runtime
        if prior_beetsdir is None:
            os.environ.pop("BEETSDIR", None)
        else:
            os.environ["BEETSDIR"] = prior_beetsdir


@dataclass(frozen=True)
class _RestartCase:
    role: BeetsRole
    entrypoint: Callable[[], int]


_RESTART_CASES: tuple[_RestartCase, ...] = (
    _RestartCase("main", cratedigger.main),
    _RestartCase("importer", importer.main),
    _RestartCase("preview", import_preview_worker.main),
    _RestartCase("web", server.main),
)


def _entrypoint_logger(case: _RestartCase) -> logging.Logger:
    if case.role == "main":
        return cratedigger.logger
    if case.role == "importer":
        return importer.logger
    if case.role == "preview":
        return import_preview_worker.logger
    return server.log


@contextmanager
def _record_admission_events(
    case: _RestartCase,
    events: list[str],
) -> Iterator[None]:
    logger = _entrypoint_logger(case)
    prior_level = logger.level
    handler = _AdmissionEventHandler(case.role, events)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prior_level)


def assert_one_admission_before_effect(events: tuple[str, ...]) -> None:
    """Require one admitted result/log before the first application effect."""
    admission_count = events.count("admitted")
    if admission_count != 1:
        raise AssertionError(
            "startup must emit exactly one admitted result/log before its "
            f"first application effect; observed {admission_count}: {events!r}"
        )
    if events != ("admitted", "effect"):
        raise AssertionError(
            "startup admission must precede exactly one first application "
            f"effect; observed {events!r}"
        )


def _snapshot_runtime_tree(
    root: Path,
) -> tuple[tuple[str, str, int, bytes | None], ...]:
    """Capture runtime names, contents, and modes around a rejected start."""
    snapshot: list[tuple[str, str, int, bytes | None]] = []
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        mode = path.lstat().st_mode & 0o7777
        if path.is_symlink():
            snapshot.append((relative, "symlink", mode, os.readlink(path).encode()))
        elif path.is_file():
            snapshot.append((relative, "file", mode, path.read_bytes()))
        elif path.is_dir():
            snapshot.append((relative, "dir", mode, None))
        else:
            snapshot.append((relative, "other", mode, None))
    return tuple(snapshot)


def _snapshot_web_process_state() -> tuple[object, ...]:
    """Capture Beets, web globals, and caches that admission may rebind."""
    from beets import config as active_beets_config

    from web import cache

    return (
        active_beets_config["library"].as_filename(),
        active_beets_config["directory"].as_filename(),
        active_beets_config["statefile"].as_filename(),
        server.beets_db_path,
        server.beets_library_root,
        server.canonical_origin,
        server.insecure_mode,
        server._db_dsn,
        server.mb_api.MB_API_BASE,
        server._discogs.DISCOGS_API_BASE,
        id(cache._redis),
        server._rendered_index_document.cache_info(),
    )


@contextmanager
def _patched_rejection_cache_constructor(
    case: _RestartCase,
) -> Iterator[MagicMock | None]:
    """Expose the web cache's third-party constructor without running it."""
    if case.role == "web":
        with patch("redis.Redis") as constructor:
            yield constructor
        return
    yield None


@contextmanager
def _patched_rejection_beets_mutators(
    case: _RestartCase,
) -> Iterator[tuple[MagicMock, MagicMock] | None]:
    """Observe that web never rebinds Beets before a rejected admission."""
    if case.role == "web":
        from beets import config as active_beets_config

        with (
            patch.object(active_beets_config, "clear") as clear,
            patch.object(active_beets_config, "read") as read,
        ):
            yield clear, read
        return
    yield None


@contextmanager
def _patched_restart_boundary(
    case: _RestartCase,
    *,
    side_effect: object | None = None,
) -> Iterator[MagicMock]:
    """Stop a real entrypoint only at its external database boundary."""
    if case.role == "main":
        with patch("lib.migrator.psycopg2.connect", side_effect=side_effect) as boundary:
            yield boundary
        return
    if case.role == "importer":
        with patch("scripts.importer.PipelineDB", side_effect=side_effect) as boundary:
            yield boundary
        return
    if case.role == "preview":
        with patch("scripts.import_preview_worker.PipelineDB", side_effect=side_effect) as boundary:
            yield boundary
        return
    with patch("web.server.PipelineDB", side_effect=side_effect) as boundary:
        yield boundary


def _restart_argv(
    case: _RestartCase,
    world: BeetsContractWorld,
    *,
    probe_pre_gate_effects: bool = False,
) -> list[str]:
    common = [
        "--config", str(world.runtime_config),
        "--runtime-dir", str(world.runtime_dir),
    ]
    if case.role == "main":
        return ["cratedigger.py", *common]
    if case.role == "importer":
        return ["importer.py", *common, "--once"]
    if case.role == "preview":
        return ["import_preview_worker.py", *common, "--once"]
    argv = [
        "server.py", *common,
        "--canonical-origin", "https://music.example",
        "--dev-port", "0",
    ]
    if probe_pre_gate_effects:
        argv.extend([
            "--insecure-mode",
            "--redis-host", "must-not-connect.invalid",
            "--mb-api", "https://must-not-bind.invalid/ws/2",
            "--discogs-api", "https://must-not-bind.invalid/discogs",
        ])
    return argv


def _exercise_real_rejection_and_restart(
    test: unittest.TestCase,
    case: _RestartCase,
) -> None:
    """Reject, repair, and restart one real top-level application."""
    from lib import config as runtime_config_module
    from web import cache

    world = BeetsContractWorld(role=case.role)
    prior_main_config = cratedigger.cfg
    saved_umask = os.umask(0o027) if case.role == "main" else None
    prior_web_globals = (
        (
            server.beets_db_path,
            server.beets_library_root,
            server.canonical_origin,
            server.insecure_mode,
            server._db_dsn,
            server.mb_api.MB_API_BASE,
            server._discogs.DISCOGS_API_BASE,
            cache._redis,
        )
        if case.role == "web"
        else None
    )
    try:
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
        world._seal(case.role)
        rejection_argv = _restart_argv(
            case,
            world,
            probe_pre_gate_effects=True,
        )
        before_rejection = snapshot_contract_world(world)
        before_runtime = _snapshot_runtime_tree(world.runtime_dir)
        lock_path = world.runtime_dir / ".cratedigger.lock"
        test.assertFalse(lock_path.exists())
        before_web = (
            _snapshot_web_process_state() if case.role == "web" else None
        )
        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", rejection_argv),
            _patched_restart_boundary(case) as effect,
            _patched_rejection_cache_constructor(case) as redis_constructor,
            _patched_rejection_beets_mutators(case) as beets_mutators,
        ):
            prior_runtime = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
            prior_beetsdir = os.environ.get("BEETSDIR")
            test.assertEqual(case.entrypoint(), 1)
            effect.assert_not_called()
            test.assertEqual(
                os.environ.get("CRATEDIGGER_RUNTIME_CONFIG"),
                prior_runtime,
            )
            test.assertEqual(os.environ.get("BEETSDIR"), prior_beetsdir)
            test.assertIsNone(runtime_config_module._admitted_runtime_config)
            if case.role == "main":
                test.assertIs(cratedigger.cfg, prior_main_config)
            if case.role == "web":
                if redis_constructor is None:
                    raise AssertionError("web cache constructor was not observed")
                if beets_mutators is None:
                    raise AssertionError("web Beets mutators were not observed")
                clear_beets, read_beets = beets_mutators
                clear_beets.assert_not_called()
                read_beets.assert_not_called()
                redis_constructor.assert_not_called()
                test.assertEqual(_snapshot_web_process_state(), before_web)
        test.assertEqual(snapshot_contract_world(world), before_rejection)
        test.assertEqual(_snapshot_runtime_tree(world.runtime_dir), before_runtime)
        test.assertFalse(lock_path.exists())
        if saved_umask is not None:
            observed_umask = os.umask(0o027)
            test.assertEqual(observed_umask, 0o027)

        world.unseal()
        world._write_main_config()
        world._seal(case.role)
        before_restart = snapshot_contract_world(world)
        events: list[str] = []

        def first_application_effect(
            *_args: object,
            **_kwargs: object,
        ) -> None:
            events.append("effect")
            raise _PostGateEffect

        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", _restart_argv(case, world)),
            _record_admission_events(case, events),
            _patched_restart_boundary(
                case,
                side_effect=first_application_effect,
            ) as effect,
            test.assertRaises(_PostGateEffect),
        ):
            case.entrypoint()
        effect.assert_called_once()
        assert_one_admission_before_effect(tuple(events))
        test.assertEqual(snapshot_contract_world(world), before_restart)
        test.assertFalse(lock_path.exists())
    finally:
        if saved_umask is not None:
            os.umask(saved_umask)
        cratedigger.cfg = prior_main_config
        if prior_web_globals is not None:
            from beets import config as active_beets_config

            from web import cache

            (
                server.beets_db_path,
                server.beets_library_root,
                server.canonical_origin,
                server.insecure_mode,
                server._db_dsn,
                server.mb_api.MB_API_BASE,
                server._discogs.DISCOGS_API_BASE,
                cache._redis,
            ) = prior_web_globals
            active_beets_config.clear()
            active_beets_config.read(user=True, defaults=True)
        world.close()


class TestBeetsStartupAdapter(unittest.TestCase):
    def test_warning_logs_and_returns_the_exact_strict_config(self) -> None:
        from lib.beets_startup import enforce_beets_startup

        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(
            musicbrainz={"host": "mirror.invalid", "https": True},
        )
        world._seal("web")
        logger = logging.getLogger("test.beets-startup-warning")
        with (
            _isolated_installed_authority(),
            self.assertLogs(logger, level="WARNING") as captured,
        ):
            admitted = enforce_beets_startup(
                role="web",
                config_path=str(world.runtime_config),
                runtime_dir=str(world.runtime_dir),
                logger=logger,
            )

        self.assertEqual(admitted.beets_config_dir, str(world.beets_dir))
        self.assertEqual(admitted.beets_library_db, str(world.library_db))
        self.assertEqual(admitted.beets_directory, str(world.library_root))
        self.assertEqual(admitted.beets_state_file, str(world.state_file))
        self.assertEqual(admitted.beets_python, str(Path(sys.executable).resolve()))
        self.assertEqual(
            admitted.beets_secret_include,
            str(world.secret_include),
        )
        self.assertIn("musicbrainz_endpoint_drift", captured.output[0])

    def test_hard_report_logs_bounded_reason_and_refuses_startup(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        world = BeetsContractWorld(role="main")
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
        world._seal("main")
        logger = logging.getLogger("test.beets-startup-hard")
        with (
            _isolated_installed_authority(),
            self.assertLogs(logger, level="ERROR") as captured,
            self.assertRaises(BeetsStartupError),
        ):
            enforce_beets_startup(
                role="main",
                config_path=str(world.runtime_config),
                runtime_dir=str(world.runtime_dir),
                logger=logger,
            )

        self.assertIn("import_write_disabled", captured.output[0])
        self.assertNotIn("secret", captured.output[0].lower())

    def test_native_parser_failure_remains_actionable(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        logger = logging.getLogger("test.beets-startup-native")
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "runtime.ini"
            config_path.write_text("[Beets\nbroken = [\n", encoding="utf-8")
            with (
                _isolated_installed_authority(),
                self.assertLogs(logger, level="ERROR") as captured,
                self.assertRaises(BeetsStartupError),
            ):
                enforce_beets_startup(
                    role="preview",
                    config_path=str(config_path),
                    runtime_dir=directory,
                    logger=logger,
                )

        self.assertIn("runtime.ini", captured.output[0])
        self.assertIn("no section headers", captured.output[0])
        self.assertIn("[Beets", captured.output[0])

    def test_loader_value_error_is_a_native_startup_failure(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        world = BeetsContractWorld(role="main")
        self.addCleanup(world.close)
        world.unseal()
        with world.runtime_config.open("a", encoding="utf-8") as handle:
            handle.write(
                "[Search Settings]\nnumber_of_albums_to_grab = many\n"
            )
        world._seal("main")
        logger = logging.getLogger("test.beets-startup-value-error")
        with (
            _isolated_installed_authority(),
            self.assertLogs(logger, level="ERROR") as captured,
            self.assertRaises(BeetsStartupError),
        ):
            enforce_beets_startup(
                role="main",
                config_path=str(world.runtime_config),
                runtime_dir=str(world.runtime_dir),
                logger=logger,
            )

        self.assertIn("invalid literal", captured.output[0])

    def test_malformed_authority_path_is_a_stable_startup_failure(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        world = BeetsContractWorld(role="main")
        self.addCleanup(world.close)
        world.unseal()
        world._write_runtime_config(
            config_dir="~cratedigger-no-such-user-759/config",
        )
        world._seal("main")
        logger = logging.getLogger("test.beets-startup-malformed-path")

        with (
            _isolated_installed_authority(),
            self.assertLogs(logger, level="ERROR") as captured,
            self.assertRaises(BeetsStartupError),
        ):
            enforce_beets_startup(
                role="main",
                config_path=str(world.runtime_config),
                runtime_dir=str(world.runtime_dir),
                logger=logger,
            )

        self.assertIn("Beets configuration check failed", captured.output[0])

    def test_install_caches_normalized_authority_without_downstream_reparse(
        self,
    ) -> None:
        from lib import config as runtime_config_module

        admitted = CratediggerConfig(
            beets_config_dir="/normalized/beets",
            beets_library_db="/normalized/library.db",
            beets_directory="/normalized/music",
            beets_state_file="/normalized/state.pickle",
            beets_python=sys.executable,
            beets_secret_include="/normalized/secret.yaml",
        )
        with tempfile.TemporaryDirectory() as directory:
            raw_path = f"{directory}/./runtime.ini"
            Path(raw_path).write_text("[Pipeline DB]\n", encoding="utf-8")
            with (
                _isolated_installed_authority(),
                patch(
                    "lib.config.CratediggerConfig.from_ini",
                    side_effect=AssertionError("raw config was reparsed"),
                ) as parser,
            ):
                runtime_config_module.install_admitted_runtime_config(
                    raw_path,
                    admitted,
                )
                self.assertEqual(
                    os.environ["CRATEDIGGER_RUNTIME_CONFIG"],
                    str(Path(raw_path).resolve()),
                )
                self.assertEqual(os.environ["BEETSDIR"], "/normalized/beets")
                self.assertIs(runtime_config_module.read_runtime_config(), admitted)
                self.assertIs(
                    runtime_config_module.read_runtime_config(raw_path),
                    admitted,
                )
                borrowed_db = MagicMock()
                borrowed_db.dsn = "postgresql://admitted"
                runtime_ctx = importer._build_runtime_context(
                    borrowed_db,
                    borrow_session=True,
                )
                self.assertIs(runtime_ctx.cfg, admitted)
                self.assertIs(
                    runtime_ctx.pipeline_db_source._get_db(),
                    borrowed_db,
                )
                runtime_ctx.pipeline_db_source.close()
                parser.assert_not_called()

    def test_startup_path_resolution_precedence_is_deployment_neutral(self) -> None:
        from lib.config import resolve_startup_config_paths

        with (
            patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": "/env/runtime.ini"},
                clear=False,
            ),
            patch("lib.config.os.getcwd", return_value="/working"),
        ):
            self.assertEqual(
                resolve_startup_config_paths(
                    config_path="/arg/runtime.ini",
                    config_dir="/arg/config-dir",
                    runtime_dir="/arg/state",
                ),
                ("/arg/runtime.ini", "/arg/state"),
            )
            self.assertEqual(
                resolve_startup_config_paths(
                    config_path=None,
                    config_dir="/arg/config-dir",
                    runtime_dir=None,
                ),
                ("/arg/config-dir/config.ini", "/working"),
            )
            self.assertEqual(
                resolve_startup_config_paths(
                    config_path=None,
                    config_dir=None,
                    runtime_dir=None,
                ),
                ("/env/runtime.ini", "/working"),
            )
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("lib.config.os.getcwd", return_value="/working"),
        ):
            self.assertEqual(
                resolve_startup_config_paths(
                    config_path=None,
                    config_dir=None,
                    runtime_dir=None,
                ),
                ("/working/config.ini", "/working"),
            )

    def test_real_invalid_contract_is_zero_effect_then_each_app_restarts(
        self,
    ) -> None:
        for case in _RESTART_CASES:
            with self.subTest(role=case.role):
                _exercise_real_rejection_and_restart(self, case)

    def test_direct_web_rebinds_eager_beets_config_to_normalized_authority(
        self,
    ) -> None:
        """A caller's BEETSDIR cannot leak into web distance behavior."""
        from beets import config as active_beets_config

        from lib import beets_distance
        from lib import config as runtime_config_module

        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(match={
            "preferred": {
                "media": ["Vinyl"],
                "countries": [],
                "original_year": False,
            },
        })
        noncanonical_config_dir = (
            f"{world.beets_dir}/../{world.beets_dir.name}"
        )
        noncanonical_library = (
            f"{world.library_db.parent}/./{world.library_db.name}"
        )
        noncanonical_root = (
            f"{world.library_root.parent}/./{world.library_root.name}"
        )
        world._write_runtime_config(
            config_dir=noncanonical_config_dir,
            library=noncanonical_library,
            directory=noncanonical_root,
        )
        world._seal("web")

        wrong_dir_tmp = tempfile.TemporaryDirectory()
        self.addCleanup(wrong_dir_tmp.cleanup)
        wrong_dir = Path(wrong_dir_tmp.name)
        (wrong_dir / "config.yaml").write_text(
            "library: /wrong/library.db\n"
            "directory: /wrong/music\n"
            "match:\n"
            "  preferred:\n"
            "    media: [Cassette]\n",
            encoding="utf-8",
        )
        prior_runtime = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
        prior_beetsdir = os.environ.get("BEETSDIR")
        prior_web_globals = (
            server.beets_db_path,
            server.beets_library_root,
            server.canonical_origin,
            server.insecure_mode,
            server._db_dsn,
            server.mb_api.MB_API_BASE,
            server._discogs.DISCOGS_API_BASE,
        )
        normalized_config_dir = str(world.beets_dir.resolve())
        normalized_library = str(world.library_db.resolve())
        normalized_root = str(world.library_root.resolve())
        normalized_state = str(world.state_file.resolve())

        def restore_environment() -> None:
            if prior_runtime is None:
                os.environ.pop("CRATEDIGGER_RUNTIME_CONFIG", None)
            else:
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = prior_runtime
            if prior_beetsdir is None:
                os.environ.pop("BEETSDIR", None)
            else:
                os.environ["BEETSDIR"] = prior_beetsdir
            active_beets_config.clear()
            active_beets_config.read(user=True, defaults=True)
            (
                server.beets_db_path,
                server.beets_library_root,
                server.canonical_origin,
                server.insecure_mode,
                server._db_dsn,
                server.mb_api.MB_API_BASE,
                server._discogs.DISCOGS_API_BASE,
            ) = prior_web_globals

        with _isolated_installed_authority():
            try:
                os.environ["BEETSDIR"] = str(wrong_dir)
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = "/caller/raw.ini"
                active_beets_config.clear()
                active_beets_config.read(user=True, defaults=True)
                self.assertEqual(
                    active_beets_config["library"].as_filename(),
                    "/wrong/library.db",
                )

                def observe_first_database(_dsn: str) -> None:
                    self.assertEqual(os.environ["BEETSDIR"], normalized_config_dir)
                    self.assertEqual(
                        active_beets_config["library"].as_filename(),
                        normalized_library,
                    )
                    self.assertEqual(
                        active_beets_config["directory"].as_filename(),
                        normalized_root,
                    )
                    self.assertEqual(
                        active_beets_config["statefile"].as_filename(),
                        normalized_state,
                    )
                    self.assertEqual(
                        active_beets_config["match"]["preferred"][
                            "media"
                        ].as_str_seq(),
                        ["Vinyl"],
                    )
                    distance_globals = beets_distance._beets_distance_fn.__globals__
                    self.assertIs(
                        distance_globals["config"],
                        active_beets_config,
                    )
                    admitted = runtime_config_module.read_runtime_config()
                    self.assertEqual(
                        admitted.beets_config_dir,
                        normalized_config_dir,
                    )
                    self.assertEqual(
                        admitted.beets_library_db,
                        normalized_library,
                    )
                    self.assertEqual(
                        admitted.beets_directory,
                        normalized_root,
                    )
                    raise _PostGateEffect

                with (
                    patch.object(sys, "argv", [
                        "server.py",
                        "--config", str(world.runtime_config),
                        "--runtime-dir", str(world.runtime_dir),
                        "--canonical-origin", "https://music.example",
                        "--dev-port", "0",
                    ]),
                    patch("web.server.PipelineDB", side_effect=observe_first_database),
                    self.assertRaises(_PostGateEffect),
                ):
                    server.main()
            finally:
                restore_environment()


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
        prior_web_globals = (
            server.canonical_origin,
            server.insecure_mode,
            server.beets_db_path,
            server.beets_library_root,
        )

        def restore_web_globals() -> None:
            (
                server.canonical_origin,
                server.insecure_mode,
                server.beets_db_path,
                server.beets_library_root,
            ) = prior_web_globals

        self.addCleanup(restore_web_globals)
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
        prior_main_config = cratedigger.cfg
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
        self.assertIs(cratedigger.cfg, prior_main_config)
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
        invalid.write_text("[Beets\nbroken = true\n", encoding="utf-8")
        runtime_dir = Path(self.tmp.name) / "subprocess-runtime"
        runtime_dir.mkdir()
        before = _snapshot_runtime_tree(Path(self.tmp.name))
        commands = (
            (
                "main",
                [sys.executable, "cratedigger.py"],
                (),
            ),
            (
                "importer",
                [sys.executable, "scripts/importer.py"],
                ("--once",),
            ),
            (
                "preview",
                [sys.executable, "scripts/import_preview_worker.py"],
                ("--once",),
            ),
            (
                "web",
                [sys.executable, "web/server.py"],
                ("--canonical-origin", "https://music.example", "--dev-port", "0"),
            ),
        )

        for role, executable, extra in commands:
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
                self.assertEqual(proc.returncode, 1, proc.stderr)
                self.assertIn("Beets configuration load failed", proc.stderr)
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
