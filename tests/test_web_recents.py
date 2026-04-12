#!/usr/bin/env python3
"""Unit tests for web/classify.py — recents tab classification.

Tests every scenario the pipeline can produce, ensuring each gets
the correct badge, verdict, and summary line.
"""

import os
import sys
import unittest
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from web.classify import classify_log_entry, quality_label, LogEntry, ClassifiedEntry


# ---------------------------------------------------------------------------
# Helper to build a minimal LogEntry with sensible defaults
# ---------------------------------------------------------------------------

_DEFAULTS = LogEntry(
    id=1,
    request_id=100,
    outcome="success",
    beets_scenario="strong_match",
    beets_distance=0.012,
    soulseek_username="testuser",
    was_converted=False,
    actual_filetype="mp3",
    actual_min_bitrate=320,
    slskd_filetype="mp3",
    spectral_grade=None,
    spectral_bitrate=None,
    existing_min_bitrate=None,
    existing_spectral_bitrate=None,
    request_min_bitrate=320,
    search_filetype_override=None,
    request_status="imported",
    bitrate=320000,
    filetype="mp3",
)


def _entry(**overrides: object) -> LogEntry:
    """Build a LogEntry with sensible defaults, overridden as needed."""
    return replace(_DEFAULTS, **overrides)  # type: ignore[arg-type]


# ============================================================================
# LogEntry
# ============================================================================

class TestLogEntry(unittest.TestCase):

    def test_from_row_basic(self):
        """Construct from a dict (simulating psycopg2 row)."""
        row = {
            "id": 42, "request_id": 100, "outcome": "success",
            "beets_scenario": "strong_match", "beets_distance": 0.012,
            "soulseek_username": "testuser", "album_title": "Test Album",
            "artist_name": "Test Artist",
        }
        entry = LogEntry.from_row(row)
        self.assertEqual(entry.id, 42)
        self.assertEqual(entry.outcome, "success")
        self.assertEqual(entry.album_title, "Test Album")

    def test_from_row_missing_fields(self):
        """Missing fields get defaults, not KeyError."""
        row = {"id": 1, "outcome": "rejected"}
        entry = LogEntry.from_row(row)
        self.assertEqual(entry.id, 1)
        self.assertIsNone(entry.soulseek_username)
        self.assertEqual(entry.was_converted, False)
        self.assertEqual(entry.album_title, "")

    def test_from_row_datetime_serialized(self):
        """Datetime objects get serialized to ISO strings."""
        from datetime import datetime, timezone
        row = {"id": 1, "created_at": datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)}
        entry = LogEntry.from_row(row)
        self.assertIsInstance(entry.created_at, str)
        assert entry.created_at is not None
        self.assertIn("2026", entry.created_at)

    def test_to_json_dict(self):
        """to_json_dict returns a plain dict suitable for JSON."""
        entry = _entry(album_title="Test", artist_name="Artist")
        d = entry.to_json_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["album_title"], "Test")
        self.assertEqual(d["outcome"], "success")

    def test_to_json_dict_no_datetime_objects(self):
        """to_json_dict should not contain datetime objects."""
        from datetime import datetime, timezone
        entry = _entry()
        entry.created_at = "2026-03-30T12:00:00+00:00"
        d = entry.to_json_dict()
        for v in d.values():
            self.assertNotIsInstance(v, datetime)


# ============================================================================
# ClassifiedEntry
# ============================================================================

class TestClassifiedEntry(unittest.TestCase):

    def test_has_required_fields(self):
        c = ClassifiedEntry(badge="Imported", badge_class="badge-new",
                            border_color="#1a4a2a", verdict="MP3 320",
                            summary="MP3 320 · testuser")
        self.assertEqual(c.badge, "Imported")
        self.assertEqual(c.summary, "MP3 320 · testuser")


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
        result = classify_log_entry(_entry(outcome="success"))
        self.assertEqual(result.badge, "Imported")
        self.assertEqual(result.badge_class, "badge-new")

    def test_upgrade(self):
        """Successful import that upgraded existing quality."""
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320))
        self.assertEqual(result.badge, "Upgraded")
        self.assertEqual(result.badge_class, "badge-upgraded")

    def test_rejected_quality_downgrade(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_spectral(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=160, existing_spectral_bitrate=192))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_transcode_downgrade(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="transcode_downgrade",
            actual_min_bitrate=197, existing_min_bitrate=320))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_high_distance(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="high_distance", beets_distance=0.45))
        self.assertEqual(result.badge, "Rejected")
        self.assertEqual(result.badge_class, "badge-rejected")

    def test_rejected_audio_corrupt(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="audio_corrupt"))
        self.assertEqual(result.badge, "Rejected")

    def test_rejected_no_candidates(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="no_candidates"))
        self.assertEqual(result.badge, "Rejected")

    def test_rejected_album_name_mismatch(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="album_name_mismatch"))
        self.assertEqual(result.badge, "Rejected")

    def test_transcode_upgrade(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_upgrade",
            was_converted=True, actual_min_bitrate=240, existing_min_bitrate=192))
        self.assertEqual(result.badge, "Transcode")
        self.assertEqual(result.badge_class, "badge-transcode")

    def test_transcode_first(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_first",
            was_converted=True, actual_min_bitrate=197))
        self.assertEqual(result.badge, "Transcode")
        self.assertEqual(result.badge_class, "badge-transcode")

    def test_force_import(self):
        result = classify_log_entry(_entry(outcome="force_import"))
        self.assertEqual(result.badge, "Force imported")
        self.assertEqual(result.badge_class, "badge-force")

    def test_failed(self):
        result = classify_log_entry(_entry(outcome="failed", beets_scenario="exception"))
        self.assertEqual(result.badge, "Failed")
        self.assertEqual(result.badge_class, "badge-failed")

    def test_timeout(self):
        result = classify_log_entry(_entry(outcome="timeout", beets_scenario="timeout"))
        self.assertEqual(result.badge, "Failed")
        self.assertEqual(result.badge_class, "badge-failed")

    def test_search_filetype_override_upgrade(self):
        """search_filetype_override set - replacing garbage CBR with genuine V0."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=320, actual_min_bitrate=243))
        self.assertEqual(result.badge, "Upgraded")
        self.assertEqual(result.badge_class, "badge-upgraded")


# ============================================================================
# classify_log_entry — border colors
# ============================================================================

class TestClassifyBorderColor(unittest.TestCase):

    def test_success_green_border(self):
        result = classify_log_entry(_entry(outcome="success"))
        self.assertIn(result.border_color, ("#3a6", "#1a4a2a"))

    def test_rejected_red_border(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade"))
        self.assertEqual(result.border_color, "#a33")

    def test_transcode_amber_border(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_upgrade",
            was_converted=True, actual_min_bitrate=240, existing_min_bitrate=192))
        self.assertEqual(result.border_color, "#a93")

    def test_force_import_blue_border(self):
        result = classify_log_entry(_entry(outcome="force_import"))
        self.assertEqual(result.border_color, "#46a")


# ============================================================================
# classify_log_entry — verdicts
# ============================================================================

class TestClassifyVerdict(unittest.TestCase):

    def test_quality_downgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320))
        self.assertIn("320", result.verdict)
        self.assertIn("not", result.verdict.lower())

    def test_spectral_reject_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=160, existing_spectral_bitrate=192))
        self.assertIn("160", result.verdict)
        self.assertIn("192", result.verdict)

    def test_spectral_reject_verdict_falls_back_to_min_bitrate(self):
        """When existing_spectral_bitrate is 0/None (genuine files have no cliff),
        the verdict should fall back to existing_min_bitrate."""
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=192, existing_spectral_bitrate=0,
            existing_min_bitrate=226))
        self.assertIn("192", result.verdict)
        self.assertIn("226", result.verdict)
        self.assertNotIn("unknown", result.verdict)

    def test_transcode_downgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="transcode_downgrade",
            actual_min_bitrate=197, existing_min_bitrate=320))
        self.assertIn("197", result.verdict)
        self.assertIn("transcode", result.verdict.lower())

    def test_high_distance_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="high_distance", beets_distance=0.45))
        self.assertIn("wrong match", result.verdict.lower())
        self.assertIn("0.45", result.verdict)

    def test_audio_corrupt_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="audio_corrupt"))
        self.assertIn("corrupt", result.verdict.lower())

    def test_no_candidates_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="no_candidates"))
        self.assertIn("no", result.verdict.lower())
        self.assertIn("match", result.verdict.lower())

    def test_album_name_mismatch_verdict(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="album_name_mismatch"))
        self.assertIn("name mismatch", result.verdict.lower())

    def test_transcode_upgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_upgrade",
            was_converted=True, actual_min_bitrate=240, existing_min_bitrate=192))
        self.assertIn("searching", result.verdict.lower())

    def test_transcode_first_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", beets_scenario="transcode_first",
            was_converted=True, actual_min_bitrate=197))
        self.assertIn("searching", result.verdict.lower())

    def test_new_import_verdict(self):
        result = classify_log_entry(_entry(outcome="success"))
        v = result.verdict.lower()
        self.assertNotIn("not", v)
        self.assertNotIn("reject", v)

    def test_upgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320))
        v = result.verdict.lower()
        self.assertTrue("upgrade" in v or "192" in v or "320" in v)

    def test_verified_lossless_upgrade_verdict(self):
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True, original_filetype="flac",
            actual_filetype="mp3", actual_min_bitrate=243,
            existing_min_bitrate=192, spectral_grade="genuine"))
        self.assertIn("verified lossless", result.verdict.lower())

    def test_timeout_verdict(self):
        result = classify_log_entry(_entry(outcome="timeout", beets_scenario="timeout"))
        self.assertIn("timed out", result.verdict.lower())

    def test_exception_verdict(self):
        result = classify_log_entry(_entry(outcome="failed", beets_scenario="exception"))
        self.assertIn("error", result.verdict.lower())

    def test_force_import_verdict(self):
        result = classify_log_entry(_entry(outcome="force_import"))
        self.assertIn("force", result.verdict.lower())


# ============================================================================
# classify_log_entry — summary (folded in from build_summary_line)
# ============================================================================

class TestClassifySummary(unittest.TestCase):
    """Test that ClassifiedEntry.summary is concise and contains key info."""

    def test_new_import_summary(self):
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=320,
            soulseek_username="aguavivi23"))
        self.assertIn("320", result.summary)
        self.assertIn("aguavivi23", result.summary)

    def test_upgrade_summary_includes_username(self):
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320,
            soulseek_username="gooduser"))
        self.assertIn("gooduser", result.summary)

    def test_rejected_summary_includes_username(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320,
            soulseek_username="baduser"))
        self.assertIn("baduser", result.summary)

    def test_flac_conversion_summary(self):
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True, original_filetype="flac",
            actual_filetype="mp3", actual_min_bitrate=243,
            soulseek_username="flacuser"))
        self.assertTrue("flac" in result.summary.lower()
                        or "converted" in result.summary.lower()
                        or "V0" in result.summary)

    def test_spectral_reject_summary(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="spectral_reject",
            spectral_bitrate=160, existing_spectral_bitrate=192,
            soulseek_username="fakeflac"))
        self.assertIn("fakeflac", result.summary)
        self.assertIn("160", result.summary)

    def test_summary_no_html(self):
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192, actual_min_bitrate=320))
        self.assertNotIn("<", result.summary)
        self.assertNotIn(">", result.summary)

    def test_no_arrow_chains(self):
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="quality_downgrade",
            actual_min_bitrate=320, existing_min_bitrate=320))
        self.assertNotIn("slskd:", result.summary)
        self.assertNotIn("actual:", result.summary)
        self.assertNotIn("\u2192", result.summary)

    def test_missing_username(self):
        result = classify_log_entry(_entry(
            outcome="success", soulseek_username=None))
        self.assertIsInstance(result.summary, str)
        self.assertTrue(len(result.summary) > 0)


# ============================================================================
# Edge cases
# ============================================================================

class TestClassifyEdgeCases(unittest.TestCase):

    def test_missing_scenario(self):
        result = classify_log_entry(_entry(outcome="success", beets_scenario=None))
        self.assertIsInstance(result.badge, str)
        self.assertIsInstance(result.verdict, str)

    def test_zero_bitrate(self):
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=0, existing_min_bitrate=0))
        self.assertIsInstance(result.badge, str)

    def test_none_bitrate(self):
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=None, existing_min_bitrate=None))
        self.assertIsInstance(result.badge, str)

    def test_unknown_outcome(self):
        result = classify_log_entry(_entry(outcome="something_new"))
        self.assertIsInstance(result.badge, str)

    def test_all_results_are_classified_entry(self):
        """Every result is a ClassifiedEntry with all fields."""
        entries = [
            _entry(outcome="success"),
            _entry(outcome="rejected", beets_scenario="high_distance"),
            _entry(outcome="force_import"),
            _entry(outcome="timeout"),
            _entry(outcome="failed"),
        ]
        for entry in entries:
            result = classify_log_entry(entry)
            self.assertIsInstance(result, ClassifiedEntry,
                                 f"Expected ClassifiedEntry for outcome={entry.outcome}")
            self.assertTrue(result.badge)
            self.assertTrue(result.verdict)
            self.assertTrue(result.summary)


# ============================================================================
# Exception verdicts with error_message
# ============================================================================

class TestExceptionVerdicts(unittest.TestCase):

    def test_exception_with_error_message(self):
        """Exception verdict should include the error_message when available."""
        result = classify_log_entry(_entry(
            outcome="failed", beets_scenario="exception",
            error_message="FileNotFoundError: /mnt/virtio/music/slskd/foo"))
        self.assertIn("FileNotFoundError", result.verdict)

    def test_exception_without_error_message(self):
        """Exception verdict without error_message should still work."""
        result = classify_log_entry(_entry(
            outcome="failed", beets_scenario="exception",
            error_message=None))
        self.assertIn("error", result.verdict.lower())

    def test_failed_falls_back_to_import_result_downgrade(self):
        """Manual-import failures with only import_result still get a verdict."""
        result = classify_log_entry(_entry(
            outcome="failed",
            beets_scenario=None,
            error_message=None,
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {"min_bitrate_kbps": 239},
                "existing_measurement": {"min_bitrate_kbps": 320},
            },
        ))
        self.assertIn("239", result.verdict)
        self.assertIn("320", result.verdict)

    def test_failed_falls_back_to_import_result_error(self):
        """ImportResult error text is surfaced when error_message is blank."""
        result = classify_log_entry(_entry(
            outcome="failed",
            beets_scenario=None,
            error_message=None,
            import_result={
                "version": 2,
                "exit_code": 2,
                "decision": "import_failed",
                "error": "Harness returned rc=2",
            },
        ))
        self.assertIn("Harness returned rc=2", result.verdict)

    def test_timeout_ignores_error_message(self):
        """Timeout verdict is fixed, doesn't use error_message."""
        result = classify_log_entry(_entry(
            outcome="timeout", beets_scenario="timeout",
            error_message="some error"))
        self.assertIn("timed out", result.verdict.lower())


# ============================================================================
# downloaded_label — server-computed download quality label
# ============================================================================

class TestDownloadedLabel(unittest.TestCase):

    def test_mp3_download(self):
        """MP3 320 download gets a label."""
        result = classify_log_entry(_entry(
            outcome="success", actual_filetype="mp3", actual_min_bitrate=320))
        self.assertTrue(hasattr(result, "downloaded_label"))
        self.assertIn("320", result.downloaded_label)

    def test_flac_converted_download(self):
        """FLAC converted to V0 shows conversion."""
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True,
            original_filetype="flac", actual_filetype="mp3",
            actual_min_bitrate=243, bitrate=243000))
        self.assertTrue(hasattr(result, "downloaded_label"))
        self.assertIn("FLAC", result.downloaded_label)
        self.assertIn("V0", result.downloaded_label)

    def test_opus_converted_download(self):
        """FLAC converted to Opus shows correct format, not MP3."""
        result = classify_log_entry(_entry(
            outcome="success", was_converted=True,
            original_filetype="flac", actual_filetype="opus",
            actual_min_bitrate=117, bitrate=117000))
        self.assertIn("FLAC", result.downloaded_label)
        self.assertIn("OPUS", result.downloaded_label)
        self.assertNotIn("MP3", result.downloaded_label)

    def test_no_filetype_download(self):
        """Missing filetype doesn't crash."""
        result = classify_log_entry(_entry(
            outcome="force_import", actual_filetype=None, filetype=None))
        self.assertTrue(hasattr(result, "downloaded_label"))

    def test_bitrate_fallback(self):
        """Falls back to bitrate (bps) when actual_min_bitrate is None."""
        result = classify_log_entry(_entry(
            outcome="success", actual_min_bitrate=None,
            bitrate=155000, actual_filetype="mp3"))
        self.assertTrue(hasattr(result, "downloaded_label"))
        self.assertIn("155", result.downloaded_label)


# ============================================================================
# search_filetype_override - should only trigger with existing files on disk
# ============================================================================

class TestSearchFiletypeOverride(unittest.TestCase):

    def test_search_filetype_override_without_existing_is_new_import(self):
        """search_filetype_override set but nothing on disk = new import, not upgrade."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=None, actual_min_bitrate=243))
        self.assertEqual(result.badge, "Imported")

    def test_search_filetype_override_with_existing_is_upgrade(self):
        """search_filetype_override set AND existing on disk = upgrade."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=320, actual_min_bitrate=243))
        self.assertEqual(result.badge, "Upgraded")

    def test_search_filetype_override_opus_shows_opus(self):
        """Opus upgrade should show OPUS in verdict, not MP3."""
        result = classify_log_entry(_entry(
            outcome="success", search_filetype_override="flac",
            existing_min_bitrate=320, actual_filetype="opus",
            was_converted=True, original_filetype="flac"))
        self.assertEqual(result.badge, "Upgraded")
        self.assertIn("OPUS", result.verdict)
        self.assertNotIn("MP3", result.verdict)

    def test_upgrade_opus_shows_opus_in_verdict(self):
        """Opus upgrade verdict should use actual filetype."""
        result = classify_log_entry(_entry(
            outcome="success", existing_min_bitrate=192,
            actual_filetype="opus", actual_min_bitrate=117,
            was_converted=True, original_filetype="flac"))
        self.assertEqual(result.badge, "Upgraded")
        self.assertIn("OPUS", result.verdict)


# ============================================================================
# Spectral fallback bug — verdict must use real bitrate, not spectral estimate
# ============================================================================

class TestVerdictSpectralFallback(unittest.TestCase):
    """Verdicts must show real file bitrate, not the spectral cliff estimate.

    When actual_min_bitrate is NULL (rejected downloads that were never
    imported), the or-chain in _rejection_verdict falls through to
    spectral_bitrate — a cliff estimate that answers "what was the original
    source quality?" not "what bitrate are these files?".

    These tests reproduce exact live scenarios where the UI showed misleading
    numbers (e.g. "96kbps is not better than existing 128kbps" when both
    downloads were actually 128kbps min).
    """

    def test_quality_downgrade_uses_real_bitrate_not_spectral(self):
        """The Ataris / Welcome the Night bug: actual_min_bitrate is NULL,
        spectral_bitrate is 96, but the real download is 128kbps min.
        The import_result JSONB has the correct new_measurement."""
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            bitrate=128000,
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {
                    "min_bitrate_kbps": 128,
                    "avg_bitrate_kbps": 187,
                    "median_bitrate_kbps": 192,
                    "spectral_bitrate_kbps": 96,
                    "format": "MP3",
                    "is_cbr": False,
                    "verified_lossless": False,
                },
                "existing_measurement": {
                    "min_bitrate_kbps": 128,
                    "avg_bitrate_kbps": 187,
                    "median_bitrate_kbps": 192,
                    "spectral_bitrate_kbps": 96,
                    "format": "MP3",
                    "is_cbr": False,
                    "verified_lossless": False,
                },
            },
        ))
        # Verdict must say 128, not 96
        self.assertIn("128", result.verdict)
        self.assertNotIn("96", result.verdict)

    def test_quality_downgrade_without_import_result_uses_container_bitrate(self):
        """When there's no import_result at all, fall back to bitrate field
        (container bitrate in bps), not spectral_bitrate."""
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            bitrate=128000,
            import_result=None,
        ))
        self.assertIn("128", result.verdict)
        self.assertNotIn("96", result.verdict)

    def test_transcode_downgrade_uses_real_bitrate_not_spectral(self):
        """Same spectral fallback bug in transcode_downgrade scenario."""
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="transcode_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=240,
            existing_spectral_bitrate=None,
            bitrate=192000,
            import_result={
                "version": 2,
                "exit_code": 6,
                "decision": "transcode_downgrade",
                "new_measurement": {"min_bitrate_kbps": 192},
                "existing_measurement": {"min_bitrate_kbps": 240},
            },
        ))
        self.assertIn("192", result.verdict)
        self.assertNotIn("96", result.verdict)

    def test_transcode_classify_uses_real_bitrate_not_spectral(self):
        """_classify_transcode has the same or-chain bug for success transcodes."""
        result = classify_log_entry(_entry(
            outcome="success",
            beets_scenario="transcode_upgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=192,
            bitrate=210000,
            was_converted=True,
        ))
        # Should show 210 (bitrate // 1000), not 96 (spectral)
        self.assertIn("210", result.verdict)
        self.assertNotIn("96", result.verdict)

    def test_summary_also_uses_real_bitrate(self):
        """The summary line (collapsed card) inherits from the verdict.
        If the verdict is wrong, the summary is wrong too."""
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="quality_downgrade",
            actual_min_bitrate=None,
            spectral_bitrate=96,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            bitrate=128000,
            soulseek_username="nexus15",
            import_result={
                "version": 2,
                "exit_code": 5,
                "decision": "downgrade",
                "new_measurement": {"min_bitrate_kbps": 128},
                "existing_measurement": {"min_bitrate_kbps": 128},
            },
        ))
        self.assertIn("128", result.summary)
        self.assertNotIn("96", result.summary)
        self.assertIn("nexus15", result.summary)


if __name__ == "__main__":
    unittest.main()
