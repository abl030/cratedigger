"""The shared held-upgrade display projection (issue #1445 item 3)."""

from __future__ import annotations

import json
import unittest

import msgspec

from lib.held_upgrade_view import held_upgrade_view
from lib.quality.installed_copy_guard import compare_installed_copy
from tests.test_installed_copy_guard import fairways


def fairways_validation_result(*, scenario: str = "upgrade_held") -> str:
    return json.dumps({
        "scenario": scenario,
        "failed_path": "/albums/wrong_matches/The Fairways",
        "upgrade_hold": msgspec.to_builtins(
            compare_installed_copy(*fairways())),
    })


class TestHeldUpgradeView(unittest.TestCase):
    def test_fairways_rows_are_plain_minutes_and_seconds(self) -> None:
        view = held_upgrade_view(fairways_validation_result())
        assert view is not None
        self.assertTrue(view.sentence.startswith("Kept your current copy."))
        second = view.rows[1]
        self.assertEqual(
            (second.track, second.title, second.release, second.current,
             second.new, second.differs),
            ("2", "Secretive Girl", "2:07", "2:07", "3:26", True),
        )
        self.assertEqual(
            [row.track for row in view.rows if row.differs], ["2", "5"])

    def test_not_a_held_upgrade(self) -> None:
        self.assertIsNone(held_upgrade_view(
            fairways_validation_result(scenario="high_distance")))
        self.assertIsNone(held_upgrade_view(
            json.dumps({"scenario": "upgrade_held"})))
        self.assertIsNone(held_upgrade_view(None))
        self.assertIsNone(held_upgrade_view("not json"))

    def test_upgrade_kind_and_label(self) -> None:
        view = held_upgrade_view(fairways_validation_result())
        assert view is not None
        self.assertEqual((view.kind, view.label), ("upgrade", "Upgrade held"))


class TestFolderCollisionHeldView(unittest.TestCase):
    """Issue #1465: the second held kind, from the stored collision."""

    @staticmethod
    def _row(*, collision: bool = True) -> str:
        payload: dict[str, object] = {
            "scenario": "folder_exclusivity_guard_failed",
            "failed_path": "/albums/wrong_matches/Guard Artist - Foo",
        }
        if collision:
            payload["folder_collision"] = {
                "folder": "/Beets/Guard Artist/2004 - Foo_",
                "occupants": [{
                    "beets_album_id": 7, "albumartist": "Guard Artist",
                    "album": "Foo_",
                }],
                "unowned_files": ["/Beets/Guard Artist/2004 - Foo_/x.mp3"],
            }
        return json.dumps(payload)

    def test_collision_view_has_its_own_kind_label_and_no_table(self) -> None:
        view = held_upgrade_view(self._row())
        assert view is not None
        self.assertEqual(view.kind, "folder_collision")
        self.assertEqual(view.label, "Library folder occupied")
        self.assertEqual(view.rows, [])
        self.assertEqual(
            view.sentence,
            "Not imported: the library folder /Beets/Guard Artist/2004 - Foo_ "
            "already holds Guard Artist - Foo_ (beets id 7); 1 file no album "
            "owns. Nothing was replaced. Move or retag what is there, then "
            "retry the import.",
        )

    def test_collision_without_stored_evidence_is_not_held(self) -> None:
        self.assertIsNone(held_upgrade_view(self._row(collision=False)))

    def test_policy_constant_is_the_produced_scenario(self) -> None:
        from lib.wrong_match_policy import (
            FOLDER_COLLISION_HELD_SCENARIO,
            HELD_SCENARIOS,
            held_scenarios_sql_list,
        )

        self.assertEqual(
            FOLDER_COLLISION_HELD_SCENARIO, "folder_exclusivity_guard_failed")
        self.assertEqual(
            HELD_SCENARIOS,
            frozenset({"upgrade_held", "folder_exclusivity_guard_failed"}))
        self.assertEqual(
            held_scenarios_sql_list(),
            "'folder_exclusivity_guard_failed', 'upgrade_held'")


if __name__ == "__main__":
    unittest.main()
