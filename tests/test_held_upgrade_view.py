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


if __name__ == "__main__":
    unittest.main()
