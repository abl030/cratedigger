"""Startup-only enforcement of the external Beets authority (issue #759)."""

from __future__ import annotations

import logging
import os
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


def _restart_argv(case: _RestartCase, world: BeetsContractWorld) -> list[str]:
    common = [
        "--config", str(world.runtime_config),
        "--runtime-dir", str(world.runtime_dir),
    ]
    if case.role == "main":
        return ["cratedigger.py", *common, "--no-lock-file"]
    if case.role == "importer":
        return ["importer.py", *common, "--once"]
    if case.role == "preview":
        return ["import_preview_worker.py", *common, "--once"]
    return [
        "server.py", *common,
        "--canonical-origin", "https://music.example",
        "--dev-port", "0",
    ]


def _exercise_real_rejection_and_restart(
    test: unittest.TestCase,
    case: _RestartCase,
) -> None:
    """Reject, repair, and restart one real top-level application."""
    from lib import config as runtime_config_module

    world = BeetsContractWorld(role=case.role)
    prior_main_config = cratedigger.cfg
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
        argv = _restart_argv(case, world)
        before_rejection = snapshot_contract_world(world)
        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", argv),
            _patched_restart_boundary(case) as effect,
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
        test.assertEqual(snapshot_contract_world(world), before_rejection)

        world.unseal()
        world._write_main_config()
        world._seal(case.role)
        before_restart = snapshot_contract_world(world)
        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", argv),
            _patched_restart_boundary(
                case,
                side_effect=_PostGateEffect,
            ) as effect,
            test.assertRaises(_PostGateEffect),
        ):
            case.entrypoint()
        effect.assert_called_once()
        test.assertEqual(snapshot_contract_world(world), before_restart)
    finally:
        cratedigger.cfg = prior_main_config
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

        logger = logging.getLogger("test.beets-startup-value-error")
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "runtime.ini"
            config_path.write_text(
                "[Search Settings]\nnumber_of_albums_to_grab = many\n",
                encoding="utf-8",
            )
            with (
                _isolated_installed_authority(),
                self.assertLogs(logger, level="ERROR") as captured,
                self.assertRaises(BeetsStartupError),
            ):
                enforce_beets_startup(
                    role="main",
                    config_path=str(config_path),
                    runtime_dir=directory,
                    logger=logger,
                )

        self.assertIn("invalid literal", captured.output[0])

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

    def test_main_invalid_utf8_reaches_strict_guard_and_has_zero_effects(
        self,
    ) -> None:
        invalid = Path(self.tmp.name) / "invalid-utf8.ini"
        invalid.write_bytes(b"[Beets]\nconfig_dir = \xff\n")
        saved_umask = os.umask(0o022)
        try:
            with patch.object(sys, "argv", [
                "cratedigger.py",
                "--config", str(invalid),
                "--runtime-dir", self.tmp.name,
                "--no-lock-file",
            ]):
                self.assertEqual(cratedigger.main(), 1)
            observed_umask = os.umask(0o022)
            self.assertEqual(observed_umask, 0o022)
        finally:
            os.umask(saved_umask)

        self.assertEqual(self.fixture.read_bytes(), b"no application state")

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
