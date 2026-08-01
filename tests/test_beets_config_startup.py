"""Startup-only enforcement of the external Beets authority (issue #759)."""

from __future__ import annotations

import configparser
import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import cratedigger
from lib.beets_config_contract import (
    BeetsAuthority,
    BeetsConfigReport,
    BeetsPluginContract,
    ContractFinding,
)
from lib.config import CratediggerConfig
from scripts import import_preview_worker, importer
from tests.test_beets_config_contract import BeetsContractWorld
from web import server


class _PostGateEffect(Exception):
    """Stop an entrypoint at its first admitted application effect."""


def _authority() -> BeetsAuthority:
    return BeetsAuthority(
        config_dir="/immutable/beets",
        library="/library.db",
        directory="/music",
        state_file="/state/import-state.pickle",
        python=sys.executable,
        secret_include="/run/secrets/beets.yaml",
        beets_version="2.12.0",
        beets_package="/package/beets",
    )


def _report(
    *,
    hard: tuple[ContractFinding, ...] = (),
    warnings: tuple[ContractFinding, ...] = (),
) -> BeetsConfigReport:
    return BeetsConfigReport(
        ok=not hard,
        role="web",
        authority=_authority(),
        plugin_contract=BeetsPluginContract(
            musicbrainz=True,
            permissions=True,
            inline=True,
        ),
        hard_failures=hard,
        warnings=warnings,
        fingerprint="f" * 64,
    )


class TestBeetsStartupAdapter(unittest.TestCase):
    def test_warning_logs_and_returns_the_exact_strict_config(self) -> None:
        from lib.beets_startup import enforce_beets_startup

        cfg = CratediggerConfig()
        report = _report(
            warnings=(ContractFinding(
                code="musicbrainz_endpoint_drift",
                message="effective MusicBrainz endpoint differs",
            ),),
        )
        logger = logging.getLogger("test.beets-startup-warning")
        with (
            patch(
                "lib.beets_startup.read_runtime_config_strict",
                return_value=cfg,
            ) as strict_load,
            patch(
                "lib.beets_startup.check_beets_config",
                return_value=report,
            ) as check,
            self.assertLogs(logger, level="WARNING") as captured,
        ):
            admitted = enforce_beets_startup(
                role="web",
                config_path="/immutable/runtime.ini",
                runtime_dir="/mutable/state",
                logger=logger,
            )

        self.assertIsNot(admitted, cfg)
        self.assertEqual(admitted.beets_config_dir, "/immutable/beets")
        self.assertEqual(admitted.beets_library_db, "/library.db")
        self.assertEqual(admitted.beets_directory, "/music")
        self.assertEqual(admitted.beets_state_file, "/state/import-state.pickle")
        self.assertEqual(admitted.beets_python, sys.executable)
        self.assertEqual(
            admitted.beets_secret_include,
            "/run/secrets/beets.yaml",
        )
        strict_load.assert_called_once_with(
            "/immutable/runtime.ini", "/mutable/state",
        )
        check.assert_called_once_with(cfg, role="web")
        self.assertIn("musicbrainz_endpoint_drift", captured.output[0])

    def test_hard_report_logs_bounded_reason_and_refuses_startup(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        cfg = CratediggerConfig()
        report = _report(hard=(ContractFinding(
            code="duplicate_keys_unsafe",
            message="album duplicate keys are unsafe",
        ),))
        logger = logging.getLogger("test.beets-startup-hard")
        with (
            patch(
                "lib.beets_startup.read_runtime_config_strict",
                return_value=cfg,
            ),
            patch(
                "lib.beets_startup.check_beets_config",
                return_value=report,
            ),
            self.assertLogs(logger, level="ERROR") as captured,
            self.assertRaises(BeetsStartupError),
        ):
            enforce_beets_startup(
                role="main",
                config_path="/immutable/runtime.ini",
                runtime_dir="/mutable/state",
                logger=logger,
            )

        self.assertIn("duplicate_keys_unsafe", captured.output[0])
        self.assertNotIn("secret", captured.output[0].lower())

    def test_native_parser_failure_remains_actionable(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        native = configparser.ParsingError("/immutable/runtime.ini")
        native.append(7, "broken = [")
        logger = logging.getLogger("test.beets-startup-native")
        with (
            patch(
                "lib.beets_startup.read_runtime_config_strict",
                side_effect=native,
            ),
            patch("lib.beets_startup.check_beets_config") as check,
            self.assertLogs(logger, level="ERROR") as captured,
            self.assertRaises(BeetsStartupError),
        ):
            enforce_beets_startup(
                role="preview",
                config_path="/immutable/runtime.ini",
                runtime_dir="/mutable/state",
                logger=logger,
            )

        check.assert_not_called()
        self.assertIn("runtime.ini", captured.output[0])
        self.assertIn("broken = [", captured.output[0])

    def test_loader_value_error_is_a_native_startup_failure(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        logger = logging.getLogger("test.beets-startup-value-error")
        with (
            patch(
                "lib.beets_startup.read_runtime_config_strict",
                side_effect=ValueError("invalid integer value: many"),
            ),
            patch("lib.beets_startup.check_beets_config") as check,
            self.assertLogs(logger, level="ERROR") as captured,
            self.assertRaises(BeetsStartupError),
        ):
            enforce_beets_startup(
                role="main",
                config_path="/immutable/runtime.ini",
                runtime_dir="/mutable/state",
                logger=logger,
            )

        check.assert_not_called()
        self.assertIn("invalid integer value: many", captured.output[0])

    def test_checker_value_error_is_not_reclassified_as_a_load_failure(self) -> None:
        from lib.beets_startup import enforce_beets_startup

        cfg = CratediggerConfig()
        logger = logging.getLogger("test.beets-startup-checker-value-error")
        with (
            patch(
                "lib.beets_startup.read_runtime_config_strict",
                return_value=cfg,
            ),
            patch(
                "lib.beets_startup.check_beets_config",
                side_effect=ValueError("checker programming defect"),
            ),
            self.assertRaisesRegex(ValueError, "checker programming defect"),
        ):
            enforce_beets_startup(
                role="web",
                config_path="/immutable/runtime.ini",
                runtime_dir="/mutable/state",
                logger=logger,
            )

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
                patch.object(
                    runtime_config_module,
                    "_ADMITTED_RUNTIME_CONFIG",
                    None,
                ),
                patch.dict(os.environ, {}, clear=False),
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

    def test_real_hard_failure_is_zero_effect_and_corrected_restart_admits(
        self,
    ) -> None:
        """Drive the actual contract, importer entrypoint, and connected files."""
        from lib import config as runtime_config_module
        from scripts import importer as importer_module

        prior_admitted = runtime_config_module._ADMITTED_RUNTIME_CONFIG
        prior_beetsdir = os.environ.get("BEETSDIR")
        self.addCleanup(
            setattr,
            runtime_config_module,
            "_ADMITTED_RUNTIME_CONFIG",
            prior_admitted,
        )

        def restore_beetsdir() -> None:
            if prior_beetsdir is None:
                os.environ.pop("BEETSDIR", None)
            else:
                os.environ["BEETSDIR"] = prior_beetsdir

        self.addCleanup(restore_beetsdir)
        world = BeetsContractWorld(role="importer")
        self.addCleanup(world.close)
        world.library_db.write_bytes(b"beets-library-sentinel")
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
        world._seal("importer")

        connected = (
            world.runtime_config,
            world.main_config,
            world.secret_include,
            world.state_file,
            world.library_db,
        )
        before_failure = {path: path.read_bytes() for path in connected}
        prior_runtime = "/caller/original-runtime.ini"
        argv = [
            "importer.py",
            "--config", str(world.runtime_config),
            "--runtime-dir", str(world.runtime_dir),
            "--once",
        ]
        with (
            patch.object(sys, "argv", argv),
            patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": prior_runtime},
                clear=False,
            ),
            patch("scripts.importer.PipelineDB") as pipeline_db,
        ):
            self.assertEqual(importer_module.main(), 1)
            pipeline_db.assert_not_called()
            self.assertEqual(
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"],
                prior_runtime,
            )
        self.assertEqual(
            {path: path.read_bytes() for path in connected},
            before_failure,
        )

        world.unseal()
        world._write_main_config()
        world._seal("importer")
        stable_after_correction = {
            path: path.read_bytes()
            for path in (
                world.runtime_config,
                world.secret_include,
                world.state_file,
                world.library_db,
            )
        }
        with (
            patch.object(sys, "argv", argv),
            patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": prior_runtime},
                clear=False,
            ),
            patch(
                "scripts.importer.PipelineDB",
                side_effect=_PostGateEffect,
            ) as pipeline_db,
            self.assertRaises(_PostGateEffect),
        ):
            importer_module.main()
        pipeline_db.assert_called_once()
        self.assertEqual(
            {
                path: path.read_bytes()
                for path in stable_after_correction
            },
            stable_after_correction,
        )

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

        with patch.object(
            runtime_config_module,
            "_ADMITTED_RUNTIME_CONFIG",
            None,
        ):
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
                    patch(
                        "web.server.PipelineDB",
                        side_effect=observe_first_database,
                    ),
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
        prior_admitted = runtime_config_module._ADMITTED_RUNTIME_CONFIG

        def restore_runtime_config() -> None:
            if prior_runtime_config is None:
                os.environ.pop("CRATEDIGGER_RUNTIME_CONFIG", None)
            else:
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = prior_runtime_config
            if prior_beetsdir is None:
                os.environ.pop("BEETSDIR", None)
            else:
                os.environ["BEETSDIR"] = prior_beetsdir
            runtime_config_module._ADMITTED_RUNTIME_CONFIG = prior_admitted

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

    def _guard_failure(self) -> Exception:
        from lib.beets_startup import BeetsStartupError

        return BeetsStartupError("external Beets authority rejected")

    def test_main_hard_failure_precedes_umask_lock_and_runtime_context(self) -> None:
        lock_path = Path(self.tmp.name) / ".cratedigger.lock"
        with (
            patch.object(sys, "argv", [
                "cratedigger.py",
                "--config", self.config_path,
                "--runtime-dir", self.tmp.name,
            ]),
            patch(
                "cratedigger.enforce_beets_startup",
                side_effect=self._guard_failure(),
            ) as guard,
            patch("lib.permissions.reset_umask") as reset_umask,
        ):
            self.assertEqual(cratedigger.main(), 1)

        guard.assert_called_once()
        reset_umask.assert_not_called()
        self.assertFalse(lock_path.exists())
        self.assertEqual(self.fixture.read_bytes(), b"no application state")

    def test_importer_hard_failure_precedes_pipeline_db_and_recovery(self) -> None:
        with (
            patch.object(sys, "argv", [
                "importer.py",
                "--config", self.config_path,
                "--runtime-dir", self.tmp.name,
                "--dsn", "postgresql://unused",
                "--once",
            ]),
            patch(
                "scripts.importer.enforce_beets_startup",
                side_effect=self._guard_failure(),
            ) as guard,
            patch("scripts.importer.PipelineDB") as pipeline_db,
            patch("scripts.importer.recover_abandoned_running_jobs") as recovery,
        ):
            self.assertEqual(importer.main(), 1)

        guard.assert_called_once()
        pipeline_db.assert_not_called()
        recovery.assert_not_called()
        self.assertEqual(self.fixture.read_bytes(), b"no application state")

    def test_preview_hard_failure_precedes_pipeline_db_and_recovery(self) -> None:
        with (
            patch.object(sys, "argv", [
                "import_preview_worker.py",
                "--config", self.config_path,
                "--runtime-dir", self.tmp.name,
                "--dsn", "postgresql://unused",
                "--once",
            ]),
            patch(
                "scripts.import_preview_worker.enforce_beets_startup",
                side_effect=self._guard_failure(),
            ) as guard,
            patch("scripts.import_preview_worker.PipelineDB") as pipeline_db,
            patch(
                "scripts.import_preview_worker.recover_running_preview_jobs",
            ) as recovery,
        ):
            self.assertEqual(import_preview_worker.main(), 1)

        guard.assert_called_once()
        pipeline_db.assert_not_called()
        recovery.assert_not_called()
        self.assertEqual(self.fixture.read_bytes(), b"no application state")

    def test_web_hard_failure_precedes_auth_listener_cache_db_and_server(self) -> None:
        from beets import config as active_beets_config

        prior_origin = server.canonical_origin
        prior_insecure = server.insecure_mode
        with (
            patch.dict(os.environ, {
                "CRATEDIGGER_RUNTIME_CONFIG": "/caller/runtime.ini",
                "BEETSDIR": "/caller/beets",
            }),
            patch.object(sys, "argv", [
                "server.py",
                "--config", self.config_path,
                "--runtime-dir", self.tmp.name,
                "--canonical-origin", "https://music.example",
            ]),
            patch(
                "web.server.enforce_beets_startup",
                side_effect=self._guard_failure(),
            ) as guard,
            patch("web.server.configure_insecure_mode") as insecure,
            patch("web.server._take_systemd_unix_listener") as listener,
            patch("web.server.cache.invalidate_pattern") as invalidate,
            patch("web.server.PipelineDB") as pipeline_db,
            patch("web.server.ThreadingUnixHTTPServer") as http_server,
            patch.object(active_beets_config, "clear") as beets_clear,
            patch.object(active_beets_config, "read") as beets_read,
        ):
            self.assertEqual(server.main(), 1)
            self.assertEqual(
                os.environ["CRATEDIGGER_RUNTIME_CONFIG"],
                "/caller/runtime.ini",
            )
            self.assertEqual(os.environ["BEETSDIR"], "/caller/beets")

        guard.assert_called_once()
        insecure.assert_not_called()
        listener.assert_not_called()
        invalidate.assert_not_called()
        pipeline_db.assert_not_called()
        http_server.assert_not_called()
        beets_clear.assert_not_called()
        beets_read.assert_not_called()
        self.assertEqual(server.canonical_origin, prior_origin)
        self.assertEqual(server.insecure_mode, prior_insecure)
        self.assertEqual(self.fixture.read_bytes(), b"no application state")

    def test_each_entrypoint_checks_exactly_once_before_first_effect(self) -> None:
        cfg = CratediggerConfig(
            beets_library_db="/admitted/library.db",
            beets_directory="/admitted/music",
        )
        cases = (
            (
                "main",
                cratedigger.main,
                [
                    "cratedigger.py", "--config", self.config_path,
                    "--runtime-dir", self.tmp.name, "--no-lock-file",
                ],
                "cratedigger.enforce_beets_startup",
                "lib.permissions.reset_umask",
            ),
            (
                "importer",
                importer.main,
                [
                    "importer.py", "--config", self.config_path,
                    "--runtime-dir", self.tmp.name, "--once",
                ],
                "scripts.importer.enforce_beets_startup",
                "scripts.importer.PipelineDB",
            ),
            (
                "preview",
                import_preview_worker.main,
                [
                    "import_preview_worker.py", "--config", self.config_path,
                    "--runtime-dir", self.tmp.name, "--once",
                ],
                "scripts.import_preview_worker.enforce_beets_startup",
                "scripts.import_preview_worker.PipelineDB",
            ),
            (
                "web",
                server.main,
                [
                    "server.py", "--config", self.config_path,
                    "--runtime-dir", self.tmp.name,
                    "--canonical-origin", "https://music.example",
                ],
                "web.server.enforce_beets_startup",
                "web.server.configure_insecure_mode",
            ),
        )
        for role, entrypoint, argv, guard_path, effect_path in cases:
            with self.subTest(role=role):
                with (
                    patch.object(sys, "argv", argv),
                    patch(guard_path, return_value=cfg) as guard,
                    patch(effect_path, side_effect=_PostGateEffect),
                    self.assertRaises(_PostGateEffect),
                ):
                    entrypoint()
                guard.assert_called_once_with(
                    role=role,
                    config_path=self.config_path,
                    runtime_dir=self.tmp.name,
                    logger=ANY,
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
