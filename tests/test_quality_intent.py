"""Tests for QualityIntent enum and associated pure functions."""

import unittest

from lib.quality import (
    QualityIntent,
    QUALITY_UPGRADE_TIERS,
    search_filetypes,
    intent_allows_catch_all,
    derive_intent,
    intent_to_quality_override,
    ResolvedIntent,
    resolve_search_intent,
)


class TestQualityIntentEnum(unittest.TestCase):
    """QualityIntent has exactly four values."""

    def test_enum_values(self):
        self.assertEqual(QualityIntent.best_effort.value, "best_effort")
        self.assertEqual(QualityIntent.flac_only.value, "flac_only")
        self.assertEqual(QualityIntent.flac_preferred.value, "flac_preferred")
        self.assertEqual(QualityIntent.upgrade.value, "upgrade")

    def test_enum_count(self):
        self.assertEqual(len(QualityIntent), 4)


class TestSearchFiletypes(unittest.TestCase):
    """search_filetypes maps intent to ordered filetype lists."""

    def test_best_effort_returns_config(self):
        result = search_filetypes(QualityIntent.best_effort, ["flac", "mp3"])
        self.assertEqual(result, ["flac", "mp3"])

    def test_best_effort_preserves_order(self):
        result = search_filetypes(QualityIntent.best_effort, ["mp3", "flac"])
        self.assertEqual(result, ["mp3", "flac"])

    def test_flac_only(self):
        result = search_filetypes(QualityIntent.flac_only, ["flac", "mp3"])
        self.assertEqual(result, ["flac"])

    def test_flac_only_ignores_config(self):
        result = search_filetypes(QualityIntent.flac_only, ["mp3"])
        self.assertEqual(result, ["flac"])

    def test_flac_preferred_flac_first_then_lossy(self):
        result = search_filetypes(QualityIntent.flac_preferred, ["flac", "mp3"])
        self.assertEqual(result, ["flac", "mp3 v0", "mp3 320"])

    def test_flac_preferred_ignores_config_beyond_flac(self):
        """flac_preferred always produces the same list regardless of config."""
        result = search_filetypes(QualityIntent.flac_preferred, ["mp3"])
        self.assertEqual(result, ["flac", "mp3 v0", "mp3 320"])

    def test_upgrade(self):
        expected = [ft.strip() for ft in QUALITY_UPGRADE_TIERS.split(",")]
        result = search_filetypes(QualityIntent.upgrade, ["flac", "mp3"])
        self.assertEqual(result, expected)

    def test_upgrade_matches_constant(self):
        result = search_filetypes(QualityIntent.upgrade, [])
        self.assertEqual(result, ["flac", "mp3 v0", "mp3 320"])


class TestIntentAllowsCatchAll(unittest.TestCase):
    """Only best_effort allows catch-all fallback."""

    def test_best_effort_allows(self):
        self.assertTrue(intent_allows_catch_all(QualityIntent.best_effort))

    def test_flac_only_disallows(self):
        self.assertFalse(intent_allows_catch_all(QualityIntent.flac_only))

    def test_flac_preferred_disallows(self):
        self.assertFalse(intent_allows_catch_all(QualityIntent.flac_preferred))

    def test_upgrade_disallows(self):
        self.assertFalse(intent_allows_catch_all(QualityIntent.upgrade))


class TestDeriveIntent(unittest.TestCase):
    """derive_intent maps existing quality_override DB values to intents."""

    def test_none_is_best_effort(self):
        self.assertEqual(derive_intent(None), QualityIntent.best_effort)

    def test_empty_string_is_best_effort(self):
        self.assertEqual(derive_intent(""), QualityIntent.best_effort)

    def test_flac_is_flac_only(self):
        self.assertEqual(derive_intent("flac"), QualityIntent.flac_only)

    def test_upgrade_tiers_is_upgrade(self):
        self.assertEqual(derive_intent(QUALITY_UPGRADE_TIERS), QualityIntent.upgrade)

    def test_explicit_upgrade_tiers_string(self):
        self.assertEqual(derive_intent("flac,mp3 v0,mp3 320"), QualityIntent.upgrade)

    def test_whitespace_variants(self):
        """Handles minor whitespace differences in CSV."""
        self.assertEqual(derive_intent("flac, mp3 v0, mp3 320"), QualityIntent.upgrade)

    def test_flac_preferred_string(self):
        """derive_intent recognizes 'flac_preferred' as a literal intent value."""
        self.assertEqual(derive_intent("flac_preferred"), QualityIntent.flac_preferred)

    def test_flac_only_string(self):
        """derive_intent recognizes 'flac_only' as a literal intent value."""
        self.assertEqual(derive_intent("flac_only"), QualityIntent.flac_only)

    def test_best_effort_string(self):
        """derive_intent recognizes 'best_effort' as a literal intent value."""
        self.assertEqual(derive_intent("best_effort"), QualityIntent.best_effort)

    def test_upgrade_string(self):
        """derive_intent recognizes 'upgrade' as a literal intent value."""
        self.assertEqual(derive_intent("upgrade"), QualityIntent.upgrade)

    def test_unknown_csv_falls_back_to_upgrade(self):
        """Unrecognized multi-value CSV treated as upgrade (custom override)."""
        self.assertEqual(derive_intent("flac,mp3 v2"), QualityIntent.upgrade)


class TestIntentToQualityOverride(unittest.TestCase):
    """intent_to_quality_override maps intents back to DB strings."""

    def test_best_effort_is_none(self):
        self.assertIsNone(intent_to_quality_override(QualityIntent.best_effort))

    def test_flac_only(self):
        self.assertEqual(intent_to_quality_override(QualityIntent.flac_only), "flac")

    def test_flac_preferred(self):
        self.assertEqual(
            intent_to_quality_override(QualityIntent.flac_preferred),
            "flac_preferred",
        )

    def test_upgrade(self):
        self.assertEqual(
            intent_to_quality_override(QualityIntent.upgrade),
            QUALITY_UPGRADE_TIERS,
        )


class TestRoundTrip(unittest.TestCase):
    """Existing DB values round-trip through derive -> intent_to_override."""

    def test_none_roundtrip(self):
        override = intent_to_quality_override(derive_intent(None))
        self.assertIsNone(override)

    def test_flac_roundtrip(self):
        override = intent_to_quality_override(derive_intent("flac"))
        self.assertEqual(override, "flac")

    def test_upgrade_tiers_roundtrip(self):
        override = intent_to_quality_override(derive_intent(QUALITY_UPGRADE_TIERS))
        self.assertEqual(override, QUALITY_UPGRADE_TIERS)

    def test_flac_preferred_roundtrip(self):
        """flac_preferred survives a DB write/read cycle."""
        override = intent_to_quality_override(derive_intent("flac_preferred"))
        intent = derive_intent(override)
        self.assertEqual(intent, QualityIntent.flac_preferred)


class TestResolveSearchIntent(unittest.TestCase):
    """resolve_search_intent combines derive + search + catch_all in one call."""

    def test_best_effort_returns_config_with_catch_all(self):
        r = resolve_search_intent(None, ["flac", "mp3"])
        self.assertEqual(r.intent, QualityIntent.best_effort)
        self.assertEqual(r.search_tiers, ["flac", "mp3"])
        self.assertTrue(r.catch_all)

    def test_best_effort_empty_string(self):
        r = resolve_search_intent("", ["flac", "mp3"])
        self.assertEqual(r.intent, QualityIntent.best_effort)
        self.assertTrue(r.catch_all)

    def test_flac_only(self):
        r = resolve_search_intent("flac", ["flac", "mp3"])
        self.assertEqual(r.intent, QualityIntent.flac_only)
        self.assertEqual(r.search_tiers, ["flac"])
        self.assertFalse(r.catch_all)

    def test_flac_only_literal_intent(self):
        r = resolve_search_intent("flac_only", ["mp3"])
        self.assertEqual(r.search_tiers, ["flac"])
        self.assertFalse(r.catch_all)

    def test_flac_preferred(self):
        r = resolve_search_intent("flac_preferred", ["mp3"])
        self.assertEqual(r.intent, QualityIntent.flac_preferred)
        self.assertEqual(r.search_tiers, ["flac", "mp3 v0", "mp3 320"])
        self.assertFalse(r.catch_all)

    def test_upgrade_full_csv(self):
        r = resolve_search_intent("flac,mp3 v0,mp3 320", [])
        self.assertEqual(r.intent, QualityIntent.upgrade)
        self.assertEqual(r.search_tiers, ["flac", "mp3 v0", "mp3 320"])
        self.assertFalse(r.catch_all)

    def test_upgrade_narrowed_csv(self):
        """Narrowed CSV is used literally — the core fix."""
        r = resolve_search_intent("flac,mp3 v0", ["flac", "mp3"])
        self.assertEqual(r.intent, QualityIntent.upgrade)
        self.assertEqual(r.search_tiers, ["flac", "mp3 v0"])
        self.assertFalse(r.catch_all)

    def test_upgrade_literal_intent_name(self):
        """Literal 'upgrade' (no comma) falls through to default tiers."""
        r = resolve_search_intent("upgrade", [])
        self.assertEqual(r.search_tiers, ["flac", "mp3 v0", "mp3 320"])

    def test_upgrade_none_is_best_effort(self):
        r = resolve_search_intent(None, ["flac"])
        self.assertEqual(r.intent, QualityIntent.best_effort)

    def test_catch_all_matches_intent(self):
        """catch_all is True only for best_effort."""
        cases = [
            (None, True),
            ("flac", False),
            ("flac_preferred", False),
            ("flac,mp3 v0,mp3 320", False),
        ]
        for override, expected in cases:
            r = resolve_search_intent(override, ["flac", "mp3"])
            self.assertEqual(r.catch_all, expected,
                             f"catch_all wrong for override={override!r}")

    def test_frozen(self):
        r = resolve_search_intent(None, ["flac"])
        with self.assertRaises(AttributeError):
            r.catch_all = False  # type: ignore[misc]


class TestNarrowOverrideSearchContract(unittest.TestCase):
    """Contract tests: verify overrides are honored across subsystem boundaries."""

    def test_narrowed_320_excluded_from_search(self):
        """After narrow_override_on_downgrade removes mp3 320, search excludes it."""
        from lib.quality import narrow_override_on_downgrade, DownloadInfo
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        narrowed = narrow_override_on_downgrade("flac,mp3 v0,mp3 320", dl)
        self.assertEqual(narrowed, "flac,mp3 v0")

        resolved = resolve_search_intent(narrowed, ["flac", "mp3 v0", "mp3 320"])
        self.assertNotIn("mp3 320", resolved.search_tiers)
        self.assertEqual(resolved.search_tiers, ["flac", "mp3 v0"])
        self.assertFalse(resolved.catch_all)

    def test_full_override_still_includes_all_tiers(self):
        resolved = resolve_search_intent("flac,mp3 v0,mp3 320", [])
        self.assertEqual(resolved.search_tiers, ["flac", "mp3 v0", "mp3 320"])

    def test_import_genuine_320_quality_gate_narrows_to_flac(self):
        """After importing genuine CBR 320, quality gate → requeue_flac → search excludes mp3 320."""
        from lib.quality import quality_gate_decision, AudioQualityMeasurement
        # CBR 320, not verified lossless → quality gate says requeue_flac
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=320, is_cbr=True, verified_lossless=False)
        decision = quality_gate_decision(measurement)
        self.assertEqual(decision, "requeue_flac")

        # requeue_flac writes override = "flac"
        flac_override = intent_to_quality_override(QualityIntent.flac_only)
        self.assertEqual(flac_override, "flac")

        # Next search must exclude mp3 320
        resolved = resolve_search_intent(flac_override, ["flac", "mp3 v0", "mp3 320"])
        self.assertNotIn("mp3 320", resolved.search_tiers)
        self.assertNotIn("mp3 v0", resolved.search_tiers)
        self.assertEqual(resolved.search_tiers, ["flac"])


if __name__ == "__main__":
    unittest.main()
