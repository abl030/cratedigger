"""Tests for search_tiers and quality override narrowing contracts."""

import unittest

from lib.quality import (
    QUALITY_UPGRADE_TIERS,
    QUALITY_FLAC_ONLY,
    search_tiers,
    should_clear_lossless_search_override,
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


class TestEffectiveSearchTiers(unittest.TestCase):
    """effective_search_tiers: merge target_format + search_filetype_override.

    Priority: search_filetype_override > target_format > config defaults.
    This function doesn't exist yet — tests document the desired behavior.
    """

    def _effective(self, search_override, target_format, config):
        from lib.quality import effective_search_tiers
        return effective_search_tiers(search_override, target_format, config)

    def test_search_override_wins_over_target_format(self):
        """System search override takes precedence over user target_format."""
        tiers, catch_all = self._effective(
            "flac,mp3 v0", "flac", ["flac", "mp3 v0", "mp3 320"])
        self.assertEqual(tiers, ["flac", "mp3 v0"])
        self.assertFalse(catch_all)

    def test_target_format_used_when_no_search_override(self):
        """With no search override, target_format drives search tiers."""
        tiers, catch_all = self._effective(
            None, "flac", ["flac", "mp3 v0", "mp3 320"])
        self.assertEqual(tiers, ["flac"])
        self.assertFalse(catch_all)

    def test_config_default_when_neither_set(self):
        """No overrides → fall back to global config with catch-all."""
        tiers, catch_all = self._effective(
            None, None, ["flac", "mp3 v0", "mp3 320"])
        self.assertEqual(tiers, ["flac", "mp3 v0", "mp3 320"])
        self.assertTrue(catch_all)

    def test_target_format_flac_no_catch_all(self):
        """target_format constrains search — no catch-all fallback."""
        _, catch_all = self._effective(None, "flac", ["flac", "mp3"])
        self.assertFalse(catch_all)

    def test_search_override_empty_falls_through_to_target(self):
        """Empty string search override → treat as None → use target_format."""
        tiers, catch_all = self._effective(
            "", "flac", ["flac", "mp3 v0", "mp3 320"])
        self.assertEqual(tiers, ["flac"])
        self.assertFalse(catch_all)


class TestClearLosslessOverride(unittest.TestCase):
    """Clearing default intent should only remove user-triggered lossless search."""

    def test_default_clears_previous_lossless_toggle_override(self):
        self.assertTrue(should_clear_lossless_search_override(
            new_target_format=None,
            old_target_format="lossless",
            search_filetype_override="lossless",
        ))

    def test_default_clears_legacy_flac_target_override(self):
        self.assertTrue(should_clear_lossless_search_override(
            new_target_format=None,
            old_target_format="flac",
            search_filetype_override="lossless",
        ))

    def test_default_preserves_upgrade_override(self):
        self.assertFalse(should_clear_lossless_search_override(
            new_target_format=None,
            old_target_format="lossless",
            search_filetype_override=QUALITY_UPGRADE_TIERS,
        ))

    def test_lossless_intent_never_clears_override(self):
        self.assertFalse(should_clear_lossless_search_override(
            new_target_format="lossless",
            old_target_format="lossless",
            search_filetype_override="lossless",
        ))


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


class TestResolveUserRequeueOverride(unittest.TestCase):
    """resolve_user_requeue_override: pick override for user-initiated requeue.

    The quality gate narrows search_filetype_override to a stricter value
    (e.g. 'lossless' for CBR 320 that needs verified lossless). User
    actions that re-queue for search (Upgrade button, status reset, ban
    source) must preserve that narrowing — otherwise the same tier the
    gate intentionally closed gets re-opened and the pipeline re-downloads
    the same quality, which gets rejected as a downgrade in a loop.

    Returns QUALITY_UPGRADE_TIERS only when no override is currently set.
    """

    def _resolve(self, existing):
        from lib.quality import resolve_user_requeue_override
        return resolve_user_requeue_override(existing)

    def test_none_falls_back_to_full_tiers(self):
        self.assertEqual(self._resolve(None), QUALITY_UPGRADE_TIERS)

    def test_empty_string_falls_back_to_full_tiers(self):
        """Empty string is treated like NULL — no prior narrowing to preserve."""
        self.assertEqual(self._resolve(""), QUALITY_UPGRADE_TIERS)

    def test_lossless_preserved(self):
        """Quality gate set 'lossless' after CBR 320 import → preserve."""
        self.assertEqual(self._resolve("lossless"), "lossless")

    def test_narrowed_preserved(self):
        """narrow_override_on_downgrade output survives user requeue clicks."""
        self.assertEqual(
            self._resolve("lossless,mp3 v0"), "lossless,mp3 v0")

    def test_full_tiers_preserved_even_when_matches_default(self):
        """If the existing value already equals the fallback, pass it through unchanged."""
        self.assertEqual(
            self._resolve(QUALITY_UPGRADE_TIERS), QUALITY_UPGRADE_TIERS)


if __name__ == "__main__":
    unittest.main()
