"""Generated startup-boundary properties and known-bad self-tests."""

from __future__ import annotations

import logging
import sys
import unittest
from dataclasses import replace
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_config_contract import BeetsRole
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import CratediggerConfig, read_runtime_config
from tests.test_beets_config_contract import BeetsContractWorld
from tests.test_beets_config_startup import (
    _RESTART_CASES,
    _exercise_real_rejection_and_restart,
    _isolated_installed_authority,
    _RestartCase,
)
from web import server

ROLES: tuple[BeetsRole, ...] = ("main", "importer", "preview", "web")


class _FirstApplicationEffect(Exception):
    pass


def _entrypoint_argv(
    role: BeetsRole,
    config_path: str,
    runtime_dir: str,
) -> list[str]:
    common = ["--config", config_path, "--runtime-dir", runtime_dir]
    if role == "main":
        return ["cratedigger.py", *common, "--no-lock-file"]
    if role == "importer":
        return ["importer.py", *common, "--once"]
    if role == "preview":
        return ["import_preview_worker.py", *common, "--once"]
    return [
        "server.py",
        *common,
        "--canonical-origin", "https://music.example",
        "--dev-port", "0",
    ]


def self_raises(exception: type[BaseException]):
    """Return an assertion context without coupling helpers to a TestCase."""
    return unittest.TestCase().assertRaises(exception)


def assert_web_authority_preserved(events: tuple[str, ...]) -> None:
    if events != ("check", "effect"):
        raise AssertionError(
            "web startup must preserve the exact admitted authority before "
            f"its first database effect; observed {events!r}"
        )


def _run_real_web_post_check_override() -> tuple[str, ...]:
    """Plant authority drift after the real atomic guard but before DB use."""
    from beets import config as active_beets_config

    events: list[str] = []
    world = BeetsContractWorld(role="web")
    prior_web_globals = (
        server.beets_db_path,
        server.beets_library_root,
        server.canonical_origin,
        server.insecure_mode,
        server._db_dsn,
        server.mb_api.MB_API_BASE,
        server._discogs.DISCOGS_API_BASE,
    )

    def checked_then_overridden(
        *,
        role: BeetsRole,
        config_path: str,
        runtime_dir: str,
        logger: logging.Logger,
    ) -> CratediggerConfig:
        checked = enforce_beets_startup(
            role=role,
            config_path=config_path,
            runtime_dir=runtime_dir,
            logger=logger,
        )
        events.append("check")
        return replace(
            checked,
            beets_library_db=str(world.root / "post-check-override.db"),
        )

    def database_effect(_dsn: str) -> None:
        installed = read_runtime_config(str(world.runtime_config))
        if server.beets_db_path != installed.beets_library_db:
            events.append("authority_override")
        events.append("effect")
        raise _FirstApplicationEffect

    try:
        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", _entrypoint_argv(
                "web", str(world.runtime_config), str(world.runtime_dir)
            )),
            patch("web.server.enforce_beets_startup", side_effect=checked_then_overridden),
            patch.object(active_beets_config, "clear"),
            patch.object(active_beets_config, "read"),
            patch("web.server.PipelineDB", side_effect=database_effect),
            self_raises(_FirstApplicationEffect),
        ):
            server.main()
    finally:
        (
            server.beets_db_path,
            server.beets_library_root,
            server.canonical_origin,
            server.insecure_mode,
            server._db_dsn,
            server.mb_api.MB_API_BASE,
            server._discogs.DISCOGS_API_BASE,
        ) = prior_web_globals
        world.close()
    return tuple(events)


class TestGeneratedStartupBoundary(unittest.TestCase):
    @given(
        role=st.sampled_from(ROLES),
        outcome=st.sampled_from((
            "admitted",
            "warning",
            "hard",
            "load_error",
            "load_value_error",
        )),
    )
    def test_real_startup_adapter_enforces_every_generated_role_and_result(
        self,
        role: BeetsRole,
        outcome: str,
    ) -> None:
        world = BeetsContractWorld(role=role)
        self.addCleanup(world.close)
        if outcome != "admitted":
            world.unseal()
        if outcome == "warning":
            world._write_main_config(
                musicbrainz={"host": "mirror.invalid", "https": True},
            )
        elif outcome == "hard":
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
        elif outcome == "load_error":
            world.runtime_config.write_text(
                "[Beets\nbroken = [\n",
                encoding="utf-8",
            )
        elif outcome == "load_value_error":
            world.runtime_config.write_text(
                "[Search Settings]\nnumber_of_albums_to_grab = many\n",
                encoding="utf-8",
            )
        if outcome != "admitted":
            world._seal(role)

        startup_logger = logging.getLogger("test.generated-beets-startup")
        with (
            _isolated_installed_authority(),
            patch.object(startup_logger, "disabled", True),
        ):
            if outcome in ("hard", "load_error", "load_value_error"):
                with self.assertRaises(BeetsStartupError):
                    enforce_beets_startup(
                        role=role,
                        config_path=str(world.runtime_config),
                        runtime_dir=str(world.runtime_dir),
                        logger=startup_logger,
                    )
            else:
                admitted = enforce_beets_startup(
                    role=role,
                    config_path=str(world.runtime_config),
                    runtime_dir=str(world.runtime_dir),
                    logger=startup_logger,
                )
                self.assertEqual(
                    admitted.beets_config_dir,
                    str(world.beets_dir),
                )
                self.assertEqual(
                    admitted.beets_library_db,
                    str(world.library_db),
                )

    @given(case=st.sampled_from(_RESTART_CASES))
    def test_every_real_entrypoint_restarts_after_contract_correction(
        self,
        case: _RestartCase,
    ) -> None:
        _exercise_real_rejection_and_restart(self, case)

    def test_known_bad_post_check_override_mutant_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "exact admitted authority"):
            assert_web_authority_preserved(
                _run_real_web_post_check_override()
            )


if __name__ == "__main__":
    unittest.main()
