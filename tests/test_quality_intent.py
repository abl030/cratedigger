"""Tests for search_tiers, INTENT_NAMES, and quality override narrowing contracts."""

import unittest

from lib.quality import (
    QUALITY_UPGRADE_TIERS,
    QUALITY_FLAC_ONLY,
    INTENT_NAMES,
    search_tiers,
    narrow_override_on_downgrade,
    DownloadInfo,
)


class TestSearchTiers(unittest.TestCase):
    """search_tiers: CSV → (filetype list, catch_all)."""

    def test_none_returns_config_with_catch_all(self):
        tiers, catch_all = search_tiers(None, ["flac", "mp3"])
        self.assertEqual(tiers, ["flac", "mp3"])
        self.assertTrue(catch_all)

    def test_empty_string_returns_config_with_catch_all(self):
        tiers, catch_all = search_tiers("", ["flac", "mp3"])
        self.assertEqual(tiers, ["flac", "mp3"])
        self.assertTrue(catch_all)

    def test_flac_only(self):
        tiers, catch_all = search_tiers("flac", ["flac", "mp3"])
        self.assertEqual(tiers, ["flac"])
        self.assertFalse(catch_all)

    def test_upgrade_tiers(self):
        tiers, catch_all = search_tiers("flac,mp3 v0,mp3 320", [])
        self.assertEqual(tiers, ["flac", "mp3 v0", "mp3 320"])
        self.assertFalse(catch_all)

    def test_narrowed_csv(self):
        tiers, catch_all = search_tiers("flac,mp3 v0", ["flac", "mp3"])
        self.assertEqual(tiers, ["flac", "mp3 v0"])
        self.assertFalse(catch_all)

    def test_whitespace_in_csv(self):
        tiers, _ = search_tiers("flac, mp3 v0, mp3 320", [])
        self.assertEqual(tiers, ["flac", "mp3 v0", "mp3 320"])

    def test_config_order_preserved(self):
        tiers, _ = search_tiers(None, ["mp3", "flac"])
        self.assertEqual(tiers, ["mp3", "flac"])


class TestIntentNames(unittest.TestCase):
    """INTENT_NAMES maps friendly CLI/web names to DB values."""

    def test_best_effort_is_none(self):
        self.assertIsNone(INTENT_NAMES["best_effort"])

    def test_flac_only(self):
        self.assertEqual(INTENT_NAMES["flac_only"], QUALITY_FLAC_ONLY)

    def test_flac_alias(self):
        self.assertEqual(INTENT_NAMES["flac"], QUALITY_FLAC_ONLY)

    def test_upgrade(self):
        self.assertEqual(INTENT_NAMES["upgrade"], QUALITY_UPGRADE_TIERS)

    def test_all_values_are_string_or_none(self):
        for name, val in INTENT_NAMES.items():
            self.assertTrue(val is None or isinstance(val, str),
                            f"{name!r} has value {val!r}")


class TestNarrowSearchContract(unittest.TestCase):
    """Contract: narrowed override → search_tiers excludes removed tier."""

    def test_narrowed_320_excluded_from_search(self):
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        narrowed = narrow_override_on_downgrade("flac,mp3 v0,mp3 320", dl)
        self.assertEqual(narrowed, "flac,mp3 v0")

        tiers, catch_all = search_tiers(narrowed, ["flac", "mp3 v0", "mp3 320"])
        self.assertNotIn("mp3 320", tiers)
        self.assertEqual(tiers, ["flac", "mp3 v0"])
        self.assertFalse(catch_all)

    def test_full_override_includes_all_tiers(self):
        tiers, _ = search_tiers("flac,mp3 v0,mp3 320", [])
        self.assertEqual(tiers, ["flac", "mp3 v0", "mp3 320"])

    def test_quality_gate_accept_clears_override(self):
        """After quality gate accepts, override=None → search uses global config."""
        tiers, catch_all = search_tiers(None, ["flac", "mp3 v0", "mp3 320"])
        self.assertEqual(tiers, ["flac", "mp3 v0", "mp3 320"])
        self.assertTrue(catch_all)


if __name__ == "__main__":
    unittest.main()
