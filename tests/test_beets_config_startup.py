"""Startup-only enforcement of the external Beets authority (issue #759)."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import importer
from tests.beets_config_startup_support import (
    _RESTART_CASES,
    _exercise_real_rejection_and_restart,
    _isolated_installed_authority,
    _PostGateEffect,
)
from tests.fakes import FakePipelineDB
from tests.fakes.beets_contract import (
    BASELINE_PLUGINS,
    BeetsContractWorld,
    assert_redacted_load_failure,
)
from web import server


class TestBeetsStartupAdapter(unittest.TestCase):
    def test_warning_logs_and_returns_the_exact_strict_config(self) -> None:
        from lib.beets_startup import enforce_beets_startup
        from lib.config import read_runtime_config
        from lib.util import beets_subprocess_env

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
            self.assertIs(read_runtime_config(), admitted)
            child_env = beets_subprocess_env()
            self.assertEqual(
                child_env["CRATEDIGGER_BEETS_PYTHON"],
                sys.executable,
            )

        self.assertEqual(admitted.beets_config_dir, str(world.beets_dir))
        self.assertEqual(admitted.beets_library_db, str(world.library_db))
        self.assertEqual(admitted.beets_directory, str(world.library_root))
        self.assertEqual(admitted.beets_state_file, str(world.state_file))
        self.assertEqual(admitted.beets_python, sys.executable)
        self.assertEqual(
            admitted.beets_secret_include,
            str(world.secret_include),
        )
        self.assertIn("musicbrainz_endpoint_drift", captured.output[0])

    def test_real_virtualenv_keeps_its_invocation_symlink_identity(self) -> None:
        with tempfile.TemporaryDirectory(dir="/dev/shm") as directory:
            venv_dir = Path(directory) / "venv"
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "venv",
                    "--system-site-packages",
                    str(venv_dir),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            invoked = venv_dir / "bin" / "python"
            proc = subprocess.run(
                [
                    str(invoked),
                    "-c",
                    (
                        "import os, sys; "
                        "print(sys.executable); "
                        "print(os.path.realpath(sys.executable))"
                    ),
                ],
                check=True,
                capture_output=True,
                text=True,
            )

        invocation, resolved = proc.stdout.splitlines()
        self.assertEqual(invocation, str(invoked))
        self.assertNotEqual(invocation, resolved)

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

    def test_missing_mbsync_plugin_refuses_startup(self) -> None:
        """Without mbsync a merge cannot be followed, so nothing may start."""
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        world = BeetsContractWorld(role="main")
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(plugins=[
            plugin for plugin in BASELINE_PLUGINS if plugin != "mbsync"
        ])
        world._seal("main")
        logger = logging.getLogger("test.beets-startup-mbsync")

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

        self.assertIn("mbsync_plugin_missing", captured.output[0])

    def test_runtime_parser_failure_logs_only_a_redacted_category(self) -> None:
        from lib.beets_startup import BeetsStartupError, enforce_beets_startup

        token = "PLANTED_RUNTIME_TOKEN_759"
        logger = logging.getLogger("test.beets-startup-native")
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "runtime.ini"
            config_path.write_text(
                f"[Beets\nuser_token = [{token}\n",
                encoding="utf-8",
            )
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

        self.assertEqual(
            captured.output,
            [
                (
                    "ERROR:test.beets-startup-native:"
                    "Beets configuration load failed [runtime_config_load_error]"
                )
            ],
        )
        assert_redacted_load_failure(captured.output[0], token)

    def test_loader_value_error_uses_the_redacted_startup_category(self) -> None:
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

        self.assertIn("runtime_config_load_error", captured.output[0])
        self.assertNotIn("invalid literal", captured.output[0])

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

        world = BeetsContractWorld()
        self.addCleanup(world.close)
        admitted = world.cfg()
        raw_path = f"{world.contract_dir}/./runtime.ini"
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
            self.assertEqual(os.environ["BEETSDIR"], str(world.beets_dir))
            self.assertIs(runtime_config_module.read_runtime_config(), admitted)
            self.assertIs(
                runtime_config_module.read_runtime_config(raw_path),
                admitted,
            )
            borrowed_db = FakePipelineDB(dsn="postgresql://admitted")
            runtime_ctx = importer._build_runtime_context(
                borrowed_db,  # pyright: ignore[reportArgumentType]
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
