"""Generated startup-boundary properties and known-bad self-tests."""

from __future__ import annotations

import logging
import unittest
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_config_contract import BeetsRole
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from tests.test_beets_config_contract import BeetsContractWorld
from tests.test_beets_config_startup import (
    _RESTART_CASES,
    _exercise_real_rejection_and_restart,
    _isolated_installed_authority,
    _RestartCase,
    assert_one_admission_before_effect,
)

ROLES: tuple[BeetsRole, ...] = ("main", "importer", "preview", "web")


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

    def test_known_bad_omitted_guard_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "exactly one admitted"):
            assert_one_admission_before_effect(("effect",))

    def test_known_bad_late_guard_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "must precede"):
            assert_one_admission_before_effect(("effect", "admitted"))

    def test_known_bad_duplicate_guard_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "exactly one admitted"):
            assert_one_admission_before_effect((
                "admitted",
                "admitted",
                "effect",
            ))


if __name__ == "__main__":
    unittest.main()
