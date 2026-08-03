"""The JS fixture is a checked serialization of production wire structs."""

from __future__ import annotations

import unittest
from pathlib import Path

from tests.fixtures.build_cd_rip_proof_fixture import fixture_text


class TestCdRipJsFixture(unittest.TestCase):
    def test_checked_fixture_matches_production_serialization(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "cd_rip_proof.json"
        self.assertEqual(fixture_path.read_text(), fixture_text())
