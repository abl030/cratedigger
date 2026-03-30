#!/usr/bin/env python3
"""Unit tests for web/classify.py — recents tab classification.

Tests every scenario the pipeline can produce, ensuring each gets
the correct badge, verdict, and summary line.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from web.classify import classify_log_entry, build_summary_line, quality_label


# ---------------------------------------------------------------------------
# Helper to build a minimal download_log item dict
# ---------------------------------------------------------------------------

def _item(**overrides) -> dict:
    """Build a download_log item with sensible defaults, overridden as needed."""
    base = {
        "id": 1,
        "request_id": 100,
        "outcome": "success",
        "beets_scenario": "strong_match",
        "beets_distance": 0.012,
        "beets_detail": None,
        "soulseek_username": "testuser",
        "was_converted": False,
        "original_filetype": None,
        "actual_filetype": "mp3",
        "actual_min_bitrate": 320,
        "slskd_filetype": "mp3",
        "slskd_bitrate": 320000,
        "spectral_grade": None,
        "spectral_bitrate": None,
        "existing_min_bitrate": None,
        "existing_spectral_bitrate": None,
        "prev_min_bitrate": None,
        "request_min_bitrate": 320,
        "quality_override": None,
        "request_status": "imported",
        "bitrate": 320000,
        "filetype": "mp3",
    }
    base.update(overrides)
    return base


# ============================================================================
# quality_label
# ============================================================================

class TestQualityLabel(unittest.TestCase):

    def test_flac(self):
        self.assertEqual(quality_label("flac", 0), "FLAC")

    def test_mp3_320(self):
        self.assertEqual(quality_label("mp3", 320), "MP3 320")

    def test_mp3_v0(self):
        self.assertEqual(quality_label("mp3", 243), "MP3 V0")

    def test_mp3_v2(self):
        self.assertEqual(quality_label("mp3", 192), "MP3 V2")

    def test_mp3_low(self):
        self.assertEqual(quality_label("mp3", 128), "MP3 128k")

    def test_no_format(self):
        self.assertEqual(quality_label("", 320), "?")

    def test_no_bitrate(self):
        self.assertEqual(quality_label("mp3", 0), "MP3")

    def test_high_v0_boundary(self):
        self.assertEqual(quality_label("mp3", 220), "MP3 V0")

    def test_just_below_v0(self):
        self.assertEqual(quality_label("mp3", 219), "MP3 V2")

    def test_alac(self):
        self.assertEqual(quality_label("alac", 0), "ALAC")


# ============================================================================
# classify_log_entry — badge classification
# ============================================================================

class TestClassifyBadge(unittest.TestCase):
    """Test that classify_log_entry returns the correct badge for each scenario."""

    def test_new_import(self):
        """First-time import, nothing on disk before."""
        item = _item(outcome="success", prev_min_bitrate=None)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Imported")
        self.assertEqual(result["badge_class"], "badge-new")

    def test_upgrade(self):
        """Successful import that upgraded existing quality."""
        item = _item(outcome="success", prev_min_bitrate=192,
                     request_min_bitrate=320)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Upgraded")
        self.assertEqual(result["badge_class"], "badge-upgraded")

    def test_rejected_quality_downgrade(self):
        """Rejected because new quality not better than existing."""
        item = _item(outcome="rejected", beets_scenario="quality_downgrade",
                     actual_min_bitrate=320, existing_min_bitrate=320)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Rejected")
        self.assertEqual(result["badge_class"], "badge-rejected")

    def test_rejected_spectral(self):
        """Rejected because spectral analysis said not better."""
        item = _item(outcome="rejected", beets_scenario="spectral_reject",
                     spectral_bitrate=160, existing_spectral_bitrate=192)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Rejected")
        self.assertEqual(result["badge_class"], "badge-rejected")

    def test_rejected_transcode_downgrade(self):
        """Rejected: transcode and not better than existing."""
        item = _item(outcome="rejected", beets_scenario="transcode_downgrade",
                     actual_min_bitrate=197, existing_min_bitrate=320)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Rejected")
        self.assertEqual(result["badge_class"], "badge-rejected")

    def test_rejected_high_distance(self):
        """Rejected: wrong MusicBrainz match."""
        item = _item(outcome="rejected", beets_scenario="high_distance",
                     beets_distance=0.45)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Rejected")
        self.assertEqual(result["badge_class"], "badge-rejected")

    def test_rejected_audio_corrupt(self):
        """Rejected: corrupt audio files."""
        item = _item(outcome="rejected", beets_scenario="audio_corrupt")
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Rejected")
        self.assertEqual(result["badge_class"], "badge-rejected")

    def test_rejected_no_candidates(self):
        """Rejected: no MusicBrainz candidates."""
        item = _item(outcome="rejected", beets_scenario="no_candidates")
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Rejected")
        self.assertEqual(result["badge_class"], "badge-rejected")

    def test_rejected_album_name_mismatch(self):
        """Rejected: album name didn't match."""
        item = _item(outcome="rejected", beets_scenario="album_name_mismatch")
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Rejected")
        self.assertEqual(result["badge_class"], "badge-rejected")

    def test_transcode_upgrade(self):
        """Imported transcode that was still an upgrade."""
        item = _item(outcome="success", beets_scenario="transcode_upgrade",
                     was_converted=True, actual_min_bitrate=240,
                     existing_min_bitrate=192)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Transcode")
        self.assertEqual(result["badge_class"], "badge-transcode")

    def test_transcode_first(self):
        """Imported transcode when nothing was on disk."""
        item = _item(outcome="success", beets_scenario="transcode_first",
                     was_converted=True, actual_min_bitrate=197)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Transcode")
        self.assertEqual(result["badge_class"], "badge-transcode")

    def test_force_import(self):
        """Force-imported after manual review."""
        item = _item(outcome="force_import")
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Force imported")
        self.assertEqual(result["badge_class"], "badge-force")

    def test_failed(self):
        """Import failed."""
        item = _item(outcome="failed", beets_scenario="exception")
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Failed")
        self.assertEqual(result["badge_class"], "badge-failed")

    def test_timeout(self):
        """Import timed out."""
        item = _item(outcome="timeout", beets_scenario="timeout")
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Failed")
        self.assertEqual(result["badge_class"], "badge-failed")

    def test_quality_override_upgrade(self):
        """quality_override set — replacing garbage CBR with genuine V0.
        Even though nominal bitrate went down, this is an upgrade."""
        item = _item(outcome="success", quality_override="flac",
                     prev_min_bitrate=320, request_min_bitrate=243)
        result = classify_log_entry(item)
        self.assertEqual(result["badge"], "Upgraded")
        self.assertEqual(result["badge_class"], "badge-upgraded")


# ============================================================================
# classify_log_entry — border colors
# ============================================================================

class TestClassifyBorderColor(unittest.TestCase):

    def test_success_green_border(self):
        item = _item(outcome="success", prev_min_bitrate=None)
        result = classify_log_entry(item)
        # Green-ish border for success
        self.assertIn(result["border_color"], ("#3a6", "#1a4a2a"))

    def test_rejected_red_border(self):
        item = _item(outcome="rejected", beets_scenario="quality_downgrade")
        result = classify_log_entry(item)
        self.assertEqual(result["border_color"], "#a33")

    def test_transcode_amber_border(self):
        item = _item(outcome="success", beets_scenario="transcode_upgrade",
                     was_converted=True, actual_min_bitrate=240,
                     existing_min_bitrate=192)
        result = classify_log_entry(item)
        self.assertEqual(result["border_color"], "#a93")

    def test_force_import_blue_border(self):
        item = _item(outcome="force_import")
        result = classify_log_entry(item)
        self.assertEqual(result["border_color"], "#46a")


# ============================================================================
# classify_log_entry — verdicts
# ============================================================================

class TestClassifyVerdict(unittest.TestCase):
    """Test that verdicts are human-readable and contain key information."""

    def test_quality_downgrade_verdict(self):
        """Verdict mentions both bitrates."""
        item = _item(outcome="rejected", beets_scenario="quality_downgrade",
                     actual_min_bitrate=320, existing_min_bitrate=320)
        result = classify_log_entry(item)
        self.assertIn("320", result["verdict"])
        self.assertIn("not", result["verdict"].lower())

    def test_spectral_reject_verdict(self):
        """Verdict mentions spectral bitrates."""
        item = _item(outcome="rejected", beets_scenario="spectral_reject",
                     spectral_bitrate=160, existing_spectral_bitrate=192)
        result = classify_log_entry(item)
        self.assertIn("160", result["verdict"])
        self.assertIn("192", result["verdict"])

    def test_transcode_downgrade_verdict(self):
        """Verdict mentions transcode and bitrates."""
        item = _item(outcome="rejected", beets_scenario="transcode_downgrade",
                     actual_min_bitrate=197, existing_min_bitrate=320)
        result = classify_log_entry(item)
        self.assertIn("197", result["verdict"])
        self.assertIn("transcode", result["verdict"].lower())

    def test_high_distance_verdict(self):
        """Verdict mentions wrong match and distance."""
        item = _item(outcome="rejected", beets_scenario="high_distance",
                     beets_distance=0.45)
        result = classify_log_entry(item)
        self.assertIn("wrong match", result["verdict"].lower())
        self.assertIn("0.45", result["verdict"])

    def test_audio_corrupt_verdict(self):
        item = _item(outcome="rejected", beets_scenario="audio_corrupt")
        result = classify_log_entry(item)
        self.assertIn("corrupt", result["verdict"].lower())

    def test_no_candidates_verdict(self):
        item = _item(outcome="rejected", beets_scenario="no_candidates")
        result = classify_log_entry(item)
        self.assertIn("no", result["verdict"].lower())
        self.assertIn("match", result["verdict"].lower())

    def test_album_name_mismatch_verdict(self):
        item = _item(outcome="rejected", beets_scenario="album_name_mismatch")
        result = classify_log_entry(item)
        self.assertIn("name mismatch", result["verdict"].lower())

    def test_transcode_upgrade_verdict(self):
        """Verdict mentions it was imported but searching for better."""
        item = _item(outcome="success", beets_scenario="transcode_upgrade",
                     was_converted=True, actual_min_bitrate=240,
                     existing_min_bitrate=192)
        result = classify_log_entry(item)
        self.assertIn("searching", result["verdict"].lower())

    def test_transcode_first_verdict(self):
        item = _item(outcome="success", beets_scenario="transcode_first",
                     was_converted=True, actual_min_bitrate=197)
        result = classify_log_entry(item)
        self.assertIn("searching", result["verdict"].lower())

    def test_new_import_verdict(self):
        """New import verdict is simple."""
        item = _item(outcome="success", prev_min_bitrate=None)
        result = classify_log_entry(item)
        # Should not mention "not" or "reject" or bitrate comparison
        v = result["verdict"].lower()
        self.assertNotIn("not", v)
        self.assertNotIn("reject", v)

    def test_upgrade_verdict(self):
        """Upgrade verdict mentions improvement."""
        item = _item(outcome="success", prev_min_bitrate=192,
                     request_min_bitrate=320, actual_min_bitrate=320)
        result = classify_log_entry(item)
        v = result["verdict"].lower()
        self.assertTrue("upgrade" in v or "improved" in v or
                        "192" in v or "320" in v)

    def test_verified_lossless_upgrade_verdict(self):
        """FLAC→V0 verified lossless upgrade mentions verified lossless."""
        item = _item(outcome="success", was_converted=True,
                     original_filetype="flac", actual_filetype="mp3",
                     actual_min_bitrate=243, prev_min_bitrate=192,
                     request_min_bitrate=243, spectral_grade="genuine")
        result = classify_log_entry(item)
        self.assertIn("verified lossless", result["verdict"].lower())

    def test_timeout_verdict(self):
        item = _item(outcome="timeout", beets_scenario="timeout")
        result = classify_log_entry(item)
        self.assertIn("timed out", result["verdict"].lower())

    def test_exception_verdict(self):
        item = _item(outcome="failed", beets_scenario="exception")
        result = classify_log_entry(item)
        self.assertIn("error", result["verdict"].lower())

    def test_force_import_verdict(self):
        item = _item(outcome="force_import")
        result = classify_log_entry(item)
        self.assertIn("force", result["verdict"].lower())


# ============================================================================
# build_summary_line
# ============================================================================

class TestBuildSummaryLine(unittest.TestCase):
    """Test that summary lines are concise and contain key info."""

    def test_new_import_summary(self):
        """Summary shows format and username."""
        item = _item(outcome="success", prev_min_bitrate=None,
                     actual_min_bitrate=320, soulseek_username="aguavivi23")
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        self.assertIn("320", summary)
        self.assertIn("aguavivi23", summary)

    def test_upgrade_summary(self):
        """Summary shows old → new quality."""
        item = _item(outcome="success", prev_min_bitrate=192,
                     request_min_bitrate=320, actual_min_bitrate=320,
                     soulseek_username="gooduser")
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        self.assertIn("gooduser", summary)

    def test_rejected_summary_includes_username(self):
        """Rejection summaries include the source username."""
        item = _item(outcome="rejected", beets_scenario="quality_downgrade",
                     actual_min_bitrate=320, existing_min_bitrate=320,
                     soulseek_username="baduser")
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        self.assertIn("baduser", summary)

    def test_flac_conversion_summary(self):
        """FLAC→V0 conversion mentioned in summary."""
        item = _item(outcome="success", prev_min_bitrate=None,
                     was_converted=True, original_filetype="flac",
                     actual_filetype="mp3", actual_min_bitrate=243,
                     soulseek_username="flacuser")
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        # Should mention FLAC or conversion
        self.assertTrue("flac" in summary.lower() or "converted" in summary.lower()
                        or "V0" in summary)

    def test_spectral_reject_summary(self):
        """Spectral rejection summary mentions spectral bitrates."""
        item = _item(outcome="rejected", beets_scenario="spectral_reject",
                     spectral_bitrate=160, existing_spectral_bitrate=192,
                     soulseek_username="fakeflac")
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        self.assertIn("fakeflac", summary)
        self.assertIn("160", summary)

    def test_summary_no_html(self):
        """Summary must not contain HTML tags."""
        item = _item(outcome="success", prev_min_bitrate=192,
                     request_min_bitrate=320)
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        self.assertNotIn("<", summary)
        self.assertNotIn(">", summary)

    def test_no_arrow_chains(self):
        """Summary must not contain the old arrow chain format."""
        item = _item(outcome="rejected", beets_scenario="quality_downgrade",
                     actual_min_bitrate=320, existing_min_bitrate=320)
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        self.assertNotIn("slskd:", summary)
        self.assertNotIn("actual:", summary)
        self.assertNotIn("→", summary)


# ============================================================================
# Edge cases
# ============================================================================

class TestClassifyEdgeCases(unittest.TestCase):

    def test_missing_scenario(self):
        """Entry with no beets_scenario still classifies."""
        item = _item(outcome="success", beets_scenario=None)
        result = classify_log_entry(item)
        self.assertIn("badge", result)
        self.assertIn("verdict", result)

    def test_missing_username(self):
        """Entry with no username still produces a summary."""
        item = _item(outcome="success", soulseek_username=None)
        classified = classify_log_entry(item)
        summary = build_summary_line(item, classified)
        self.assertIsInstance(summary, str)
        self.assertTrue(len(summary) > 0)

    def test_zero_bitrate(self):
        """Zero bitrate (garbage data) doesn't crash."""
        item = _item(outcome="success", actual_min_bitrate=0,
                     existing_min_bitrate=0)
        result = classify_log_entry(item)
        self.assertIn("badge", result)

    def test_none_bitrate(self):
        """None bitrate doesn't crash."""
        item = _item(outcome="success", actual_min_bitrate=None,
                     existing_min_bitrate=None)
        result = classify_log_entry(item)
        self.assertIn("badge", result)

    def test_unknown_outcome(self):
        """Unknown outcome gets a sensible default."""
        item = _item(outcome="something_new")
        result = classify_log_entry(item)
        self.assertIn("badge", result)

    def test_all_results_have_required_keys(self):
        """Every classify result has badge, badge_class, border_color, verdict."""
        scenarios = [
            _item(outcome="success"),
            _item(outcome="rejected", beets_scenario="high_distance"),
            _item(outcome="force_import"),
            _item(outcome="timeout"),
            _item(outcome="failed"),
        ]
        for item in scenarios:
            result = classify_log_entry(item)
            for key in ("badge", "badge_class", "border_color", "verdict"):
                self.assertIn(key, result, f"Missing '{key}' for outcome={item['outcome']}")


if __name__ == "__main__":
    unittest.main()
