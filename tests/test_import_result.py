#!/usr/bin/env python3
"""Tests for ImportResult dataclass, JSON serialization, and stdout parsing.

RED/GREEN TDD — these tests define the contract before implementation.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import (
    ImportResult, ConversionInfo, QualityInfo, SpectralInfo, PostflightInfo,
    parse_import_result, IMPORT_RESULT_SENTINEL,
)


class TestImportResultConstruction(unittest.TestCase):
    """Test dataclass construction and field defaults."""

    def test_default_construction(self):
        r = ImportResult()
        self.assertEqual(r.version, 1)
        self.assertEqual(r.exit_code, 0)
        self.assertIsNone(r.decision)
        self.assertFalse(r.already_in_beets)
        self.assertIsNone(r.error)
        self.assertIsInstance(r.conversion, ConversionInfo)
        self.assertIsInstance(r.quality, QualityInfo)
        self.assertIsInstance(r.spectral, SpectralInfo)
        self.assertIsInstance(r.postflight, PostflightInfo)

    def test_conversion_defaults(self):
        c = ConversionInfo()
        self.assertEqual(c.converted, 0)
        self.assertEqual(c.failed, 0)
        self.assertFalse(c.was_converted)
        self.assertIsNone(c.original_filetype)
        self.assertIsNone(c.target_filetype)

    def test_quality_defaults(self):
        q = QualityInfo()
        self.assertIsNone(q.new_min_bitrate)
        self.assertIsNone(q.prev_min_bitrate)
        self.assertFalse(q.is_transcode)
        self.assertFalse(q.will_be_verified_lossless)

    def test_spectral_defaults(self):
        s = SpectralInfo()
        self.assertIsNone(s.grade)
        self.assertIsNone(s.bitrate)
        self.assertIsNone(s.cliff_freq_hz)
        self.assertIsNone(s.existing_grade)
        self.assertIsNone(s.existing_bitrate)

    def test_postflight_defaults(self):
        p = PostflightInfo()
        self.assertIsNone(p.beets_id)
        self.assertIsNone(p.track_count)
        self.assertIsNone(p.imported_path)

    def test_full_construction(self):
        r = ImportResult(
            exit_code=0,
            decision="import",
            already_in_beets=True,
            conversion=ConversionInfo(
                converted=10, failed=0, was_converted=True,
                original_filetype="flac", target_filetype="mp3"),
            quality=QualityInfo(
                new_min_bitrate=245, prev_min_bitrate=192,
                is_transcode=False, will_be_verified_lossless=True),
            spectral=SpectralInfo(
                grade="genuine", bitrate=None, cliff_freq_hz=None,
                existing_grade="suspect", existing_bitrate=128),
            postflight=PostflightInfo(
                beets_id=1234, track_count=12,
                imported_path="/mnt/virtio/Music/Beets/Artist/Album"),
        )
        self.assertEqual(r.decision, "import")
        self.assertEqual(r.conversion.converted, 10)
        self.assertTrue(r.quality.will_be_verified_lossless)
        self.assertEqual(r.spectral.existing_bitrate, 128)
        self.assertEqual(r.postflight.track_count, 12)


class TestImportResultSerialization(unittest.TestCase):
    """Test JSON round-trip serialization."""

    def test_round_trip_default(self):
        r = ImportResult()
        j = r.to_json()
        r2 = ImportResult.from_json(j)
        self.assertEqual(r, r2)

    def test_round_trip_full(self):
        r = ImportResult(
            exit_code=6,
            decision="transcode_upgrade",
            conversion=ConversionInfo(converted=8, failed=0, was_converted=True,
                                      original_filetype="flac", target_filetype="mp3"),
            quality=QualityInfo(new_min_bitrate=180, prev_min_bitrate=128,
                                is_transcode=True, will_be_verified_lossless=False),
            spectral=SpectralInfo(grade="suspect", bitrate=128, cliff_freq_hz=16500,
                                  existing_grade="suspect", existing_bitrate=96),
            postflight=PostflightInfo(beets_id=42, track_count=8,
                                      imported_path="/Beets/Artist/Album"),
        )
        j = r.to_json()
        r2 = ImportResult.from_json(j)
        self.assertEqual(r, r2)

    def test_to_json_is_valid_json(self):
        r = ImportResult(decision="import", exit_code=0)
        parsed = json.loads(r.to_json())
        self.assertEqual(parsed["decision"], "import")
        self.assertEqual(parsed["version"], 1)

    def test_from_dict_missing_optional_sections(self):
        """from_dict should handle missing sub-dicts gracefully."""
        d = {"version": 1, "exit_code": 0, "decision": "import"}
        r = ImportResult.from_dict(d)
        self.assertEqual(r.decision, "import")
        self.assertEqual(r.conversion.converted, 0)
        self.assertIsNone(r.spectral.grade)

    def test_from_dict_with_extra_fields_in_sub(self):
        """Unknown fields in sub-dicts should raise (strict typing)."""
        d = {
            "version": 1, "exit_code": 0, "decision": "import",
            "conversion": {"converted": 5, "failed": 0, "was_converted": True,
                           "original_filetype": "flac", "target_filetype": "mp3",
                           "bogus_field": 999},
        }
        with self.assertRaises(TypeError):
            ImportResult.from_dict(d)


class TestSentinelLine(unittest.TestCase):
    """Test sentinel line formatting."""

    def test_sentinel_prefix(self):
        r = ImportResult(decision="import")
        line = r.to_sentinel_line()
        self.assertTrue(line.startswith(IMPORT_RESULT_SENTINEL))

    def test_sentinel_parseable(self):
        r = ImportResult(decision="downgrade", exit_code=5)
        line = r.to_sentinel_line()
        json_part = line[len(IMPORT_RESULT_SENTINEL):]
        parsed = json.loads(json_part)
        self.assertEqual(parsed["decision"], "downgrade")
        self.assertEqual(parsed["exit_code"], 5)


class TestParseImportResult(unittest.TestCase):
    """Test parsing ImportResult from mixed stdout."""

    def test_parse_from_clean_stdout(self):
        r = ImportResult(decision="import", exit_code=0)
        stdout = r.to_sentinel_line() + "\n"
        parsed = parse_import_result(stdout)
        assert parsed is not None
        self.assertEqual(parsed.decision, "import")

    def test_parse_from_mixed_stdout(self):
        """JSON on last line, human text before it."""
        r = ImportResult(decision="transcode_upgrade", exit_code=6,
                         quality=QualityInfo(new_min_bitrate=180))
        stdout = (
            "[CONVERT] /tmp/album\n"
            "  Converted 10, failed 0\n"
            "  min_bitrate=180\n"
            "[IMPORT] /tmp/album → beets\n"
            "[OK] Transcode imported\n"
            + r.to_sentinel_line() + "\n"
        )
        parsed = parse_import_result(stdout)
        assert parsed is not None
        self.assertEqual(parsed.decision, "transcode_upgrade")
        self.assertEqual(parsed.quality.new_min_bitrate, 180)

    def test_parse_no_sentinel(self):
        """Old import_one.py or crash — no JSON emitted."""
        stdout = "[OK] Import complete\n"
        parsed = parse_import_result(stdout)
        self.assertIsNone(parsed)

    def test_parse_empty_stdout(self):
        parsed = parse_import_result("")
        self.assertIsNone(parsed)

    def test_parse_malformed_json(self):
        stdout = IMPORT_RESULT_SENTINEL + "{broken json\n"
        parsed = parse_import_result(stdout)
        self.assertIsNone(parsed)

    def test_parse_sentinel_not_last_line(self):
        """Sentinel in the middle — still found (reverse scan)."""
        r = ImportResult(decision="import")
        stdout = (
            "some output\n"
            + r.to_sentinel_line() + "\n"
            + "trailing beets log line\n"
        )
        # The trailing line is NOT a sentinel, so reverse scan skips it
        # and finds the sentinel on the second-to-last line
        parsed = parse_import_result(stdout)
        assert parsed is not None
        self.assertEqual(parsed.decision, "import")


class TestImportResultScenarios(unittest.TestCase):
    """Test that ImportResult correctly represents each pipeline scenario."""

    def test_successful_flac_import(self):
        """Gold standard: genuine FLAC → V0, imported."""
        r = ImportResult(
            exit_code=0,
            decision="import",
            conversion=ConversionInfo(
                converted=12, failed=0, was_converted=True,
                original_filetype="flac", target_filetype="mp3"),
            quality=QualityInfo(
                new_min_bitrate=245, prev_min_bitrate=None,
                is_transcode=False, will_be_verified_lossless=True),
            spectral=SpectralInfo(grade="genuine"),
            postflight=PostflightInfo(
                beets_id=100, track_count=12,
                imported_path="/Beets/Artist/Album"),
        )
        self.assertEqual(r.exit_code, 0)
        self.assertTrue(r.conversion.was_converted)
        self.assertTrue(r.quality.will_be_verified_lossless)
        self.assertFalse(r.quality.is_transcode)
        self.assertIsNone(r.error)

    def test_downgrade_prevented(self):
        """New files worse than existing — exit 5."""
        r = ImportResult(
            exit_code=5,
            decision="downgrade",
            quality=QualityInfo(
                new_min_bitrate=192, prev_min_bitrate=320),
        )
        self.assertEqual(r.exit_code, 5)
        self.assertEqual(r.decision, "downgrade")
        self.assertIsNone(r.postflight.beets_id)  # not imported

    def test_transcode_upgrade(self):
        """Fake FLAC detected but better than existing — exit 6, imported."""
        r = ImportResult(
            exit_code=6,
            decision="transcode_upgrade",
            conversion=ConversionInfo(
                converted=10, failed=0, was_converted=True,
                original_filetype="flac", target_filetype="mp3"),
            quality=QualityInfo(
                new_min_bitrate=180, prev_min_bitrate=128,
                is_transcode=True, will_be_verified_lossless=False),
            spectral=SpectralInfo(grade="suspect", bitrate=128, cliff_freq_hz=16500),
            postflight=PostflightInfo(beets_id=42, track_count=10,
                                      imported_path="/Beets/Artist/Album"),
        )
        self.assertEqual(r.exit_code, 6)
        self.assertTrue(r.quality.is_transcode)
        self.assertIsNotNone(r.postflight.beets_id)  # was imported

    def test_transcode_downgrade(self):
        """Fake FLAC and not better — exit 6, not imported."""
        r = ImportResult(
            exit_code=6,
            decision="transcode_downgrade",
            quality=QualityInfo(
                new_min_bitrate=128, prev_min_bitrate=180,
                is_transcode=True),
        )
        self.assertEqual(r.exit_code, 6)
        self.assertEqual(r.decision, "transcode_downgrade")
        self.assertIsNone(r.postflight.beets_id)

    def test_path_missing(self):
        r = ImportResult(exit_code=3, decision="path_missing",
                         error="Path not found: /tmp/gone")
        self.assertEqual(r.exit_code, 3)
        self.assertIsNotNone(r.error)

    def test_preflight_existing(self):
        """Already in beets, no new files to compare."""
        r = ImportResult(
            exit_code=0,
            decision="preflight_existing",
            already_in_beets=True,
            postflight=PostflightInfo(
                beets_id=99, track_count=12,
                imported_path="/Beets/Artist/Album"),
        )
        self.assertTrue(r.already_in_beets)
        self.assertEqual(r.decision, "preflight_existing")

    def test_conversion_failed(self):
        r = ImportResult(
            exit_code=1,
            decision="conversion_failed",
            conversion=ConversionInfo(converted=3, failed=2),
            error="2 FLAC files failed to convert",
        )
        self.assertEqual(r.exit_code, 1)
        self.assertEqual(r.conversion.failed, 2)

    def test_import_failed(self):
        r = ImportResult(
            exit_code=2,
            decision="import_failed",
            error="Harness timeout after 300s",
        )
        self.assertEqual(r.exit_code, 2)

    def test_mbid_missing(self):
        r = ImportResult(
            exit_code=4,
            decision="mbid_missing",
            error="MBID abc-123 not in 5 candidates",
        )
        self.assertEqual(r.exit_code, 4)


class TestImportResultToDlInfo(unittest.TestCase):
    """Test that ImportResult fields map correctly to download_info dict.

    This validates the contract between ImportResult and soularr.py's
    dl_info population logic.
    """

    def _result_to_dl_info(self, r):
        """Simulate what soularr.py will do with an ImportResult."""
        dl = {}
        if r.conversion.was_converted:
            dl["was_converted"] = True
            dl["original_filetype"] = r.conversion.original_filetype
            dl["filetype"] = r.conversion.target_filetype
            dl["is_vbr"] = True
            dl["slskd_filetype"] = r.conversion.original_filetype
            dl["actual_filetype"] = r.conversion.target_filetype
        if r.quality.new_min_bitrate is not None:
            dl["bitrate"] = r.quality.new_min_bitrate * 1000
        dl["spectral_grade"] = r.spectral.grade
        dl["spectral_bitrate"] = r.spectral.bitrate
        dl["existing_spectral_bitrate"] = r.spectral.existing_bitrate
        dl["import_result"] = r.to_json()
        return dl

    def test_flac_conversion_dl_info(self):
        r = ImportResult(
            decision="import",
            conversion=ConversionInfo(
                converted=10, was_converted=True,
                original_filetype="flac", target_filetype="mp3"),
            quality=QualityInfo(new_min_bitrate=245),
            spectral=SpectralInfo(grade="genuine"),
        )
        dl = self._result_to_dl_info(r)
        self.assertTrue(dl["was_converted"])
        self.assertEqual(dl["original_filetype"], "flac")
        self.assertEqual(dl["filetype"], "mp3")
        self.assertTrue(dl["is_vbr"])
        self.assertEqual(dl["slskd_filetype"], "flac")
        self.assertEqual(dl["actual_filetype"], "mp3")
        self.assertEqual(dl["bitrate"], 245000)
        self.assertEqual(dl["spectral_grade"], "genuine")
        # Full JSON stored
        self.assertIn("import_result", dl)
        stored = json.loads(dl["import_result"])
        self.assertEqual(stored["decision"], "import")

    def test_mp3_no_conversion_dl_info(self):
        r = ImportResult(
            decision="import",
            conversion=ConversionInfo(),  # no conversion
            quality=QualityInfo(new_min_bitrate=320),
            spectral=SpectralInfo(grade="genuine"),
        )
        dl = self._result_to_dl_info(r)
        self.assertNotIn("was_converted", dl)
        self.assertEqual(dl["bitrate"], 320000)

    def test_spectral_data_preserved(self):
        r = ImportResult(
            decision="import",
            spectral=SpectralInfo(
                grade="suspect", bitrate=128, cliff_freq_hz=16500,
                existing_grade="genuine", existing_bitrate=256),
        )
        dl = self._result_to_dl_info(r)
        self.assertEqual(dl["spectral_grade"], "suspect")
        self.assertEqual(dl["spectral_bitrate"], 128)
        self.assertEqual(dl["existing_spectral_bitrate"], 256)


if __name__ == "__main__":
    unittest.main()
