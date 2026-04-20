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
    ImportResult, ConversionInfo, SpectralDetail, PostflightInfo,
    AudioQualityMeasurement,
    DownloadInfo, SpectralMeasurement,
    parse_import_result, IMPORT_RESULT_SENTINEL,
)


class TestImportResultConstruction(unittest.TestCase):
    """Test dataclass construction and field defaults."""

    def test_default_construction(self):
        r = ImportResult()
        self.assertEqual(r.version, 2)
        self.assertEqual(r.exit_code, 0)
        self.assertIsNone(r.decision)
        self.assertFalse(r.already_in_beets)
        self.assertIsNone(r.error)
        self.assertIsNone(r.new_measurement)
        self.assertIsNone(r.existing_measurement)
        self.assertIsInstance(r.conversion, ConversionInfo)
        self.assertIsInstance(r.spectral, SpectralDetail)
        self.assertIsInstance(r.postflight, PostflightInfo)

    def test_opus_fields_default_none(self):
        r = ImportResult()
        self.assertIsNone(r.v0_verification_bitrate)
        self.assertIsNone(r.final_format)

    def test_conversion_defaults(self):
        c = ConversionInfo()
        self.assertEqual(c.converted, 0)
        self.assertEqual(c.failed, 0)
        self.assertFalse(c.was_converted)
        self.assertIsNone(c.original_filetype)
        self.assertIsNone(c.target_filetype)
        self.assertIsNone(c.post_conversion_min_bitrate)
        self.assertFalse(c.is_transcode)
        self.assertIsNone(c.final_format)

    def test_spectral_detail_defaults(self):
        s = SpectralDetail()
        self.assertIsNone(s.cliff_freq_hz)
        self.assertEqual(s.suspect_pct, 0.0)
        self.assertEqual(s.per_track, [])
        self.assertEqual(s.existing_suspect_pct, 0.0)

    def test_measurement_defaults(self):
        m = AudioQualityMeasurement()
        self.assertIsNone(m.min_bitrate_kbps)
        self.assertIsNone(m.avg_bitrate_kbps)
        self.assertIsNone(m.median_bitrate_kbps)
        self.assertFalse(m.is_cbr)
        self.assertIsNone(m.spectral_grade)
        self.assertIsNone(m.spectral_bitrate_kbps)
        self.assertFalse(m.verified_lossless)
        self.assertIsNone(m.was_converted_from)

    def test_postflight_defaults(self):
        p = PostflightInfo()
        self.assertIsNone(p.beets_id)
        self.assertIsNone(p.track_count)
        self.assertIsNone(p.imported_path)
        self.assertFalse(p.disambiguated)
        self.assertIsNone(p.disambiguation_failure)

    def test_postflight_disambiguated_roundtrip(self):
        """disambiguated field survives JSON round-trip."""
        r = ImportResult(
            postflight=PostflightInfo(
                beets_id=42, track_count=11,
                imported_path="/Beets/Artist/Album [CAD 3X03]",
                disambiguated=True))
        j = r.to_json()
        r2 = ImportResult.from_json(j)
        self.assertTrue(r2.postflight.disambiguated)
        self.assertIsNone(r2.postflight.disambiguation_failure)
        self.assertEqual(r2.postflight.imported_path, "/Beets/Artist/Album [CAD 3X03]")

    def test_postflight_disambiguation_failure_roundtrip(self):
        """disambiguation_failure survives JSON round-trip (issue #127).

        When the post-import ``beet move`` fails (timeout, missing
        binary, non-zero rc), the album is still imported but the path
        wasn't fixed. The typed failure record lives on PostflightInfo
        so the audit trail in download_log.import_result preserves
        both the coarse reason tag and the human-readable detail.
        """
        from lib.quality import DisambiguationFailure

        r = ImportResult(
            postflight=PostflightInfo(
                beets_id=42, track_count=11,
                imported_path="/Beets/Artist/Album",
                disambiguated=False,
                disambiguation_failure=DisambiguationFailure(
                    reason="timeout", detail="timeout after 120s")))
        j = r.to_json()
        r2 = ImportResult.from_json(j)
        self.assertFalse(r2.postflight.disambiguated)
        assert r2.postflight.disambiguation_failure is not None
        self.assertEqual(
            r2.postflight.disambiguation_failure.reason, "timeout")
        self.assertEqual(
            r2.postflight.disambiguation_failure.detail,
            "timeout after 120s")

    def test_postflight_legacy_v2_row_without_failure_field(self):
        """Old v2 download_log rows serialized BEFORE issue #127 lack
        the disambiguation_failure key. Deserialization must default
        it to None — never raise."""
        # Hand-rolled dict mimicking a pre-#127 v2 row: postflight has
        # disambiguated but no disambiguation_failure key at all.
        d = {
            "version": 2,
            "exit_code": 0,
            "decision": "import",
            "postflight": {
                "beets_id": 42,
                "track_count": 11,
                "imported_path": "/Beets/Artist/Album",
                "disambiguated": True,
                # NO disambiguation_failure / disambiguation_error key
            },
        }
        r = ImportResult.from_dict(d)
        self.assertTrue(r.postflight.disambiguated)
        self.assertIsNone(r.postflight.disambiguation_failure)

    def test_moved_siblings_roundtrip(self):
        """Issue #132 P2 / issue #133: ``PostflightInfo.moved_siblings``
        survives ImportResult JSON round-trip. Harness emits the list
        for kept-duplicate imports so ``dispatch_import_core`` can
        propagate each sibling's new on-disk path to the pipeline DB.
        """
        from lib.quality import MovedSibling, PostflightInfo
        r = ImportResult(
            exit_code=0, decision="import",
            postflight=PostflightInfo(
                beets_id=42, track_count=11,
                imported_path="/Beets/Artist/Album [2007]",
                disambiguated=True,
                moved_siblings=[
                    MovedSibling(
                        album_id=10314,
                        new_path="/Beets/Artist/Album [2006]",
                        mb_albumid="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                        discogs_albumid=""),
                    MovedSibling(
                        album_id=10315,
                        new_path="/Beets/Artist/Album [2008]",
                        mb_albumid="",
                        discogs_albumid="12856590"),
                ]))
        r2 = ImportResult.from_json(r.to_json())
        self.assertEqual(len(r2.postflight.moved_siblings), 2)
        self.assertEqual(r2.postflight.moved_siblings[0].album_id, 10314)
        self.assertEqual(
            r2.postflight.moved_siblings[0].mb_albumid,
            "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb")
        self.assertEqual(
            r2.postflight.moved_siblings[0].new_path,
            "/Beets/Artist/Album [2006]")
        self.assertEqual(r2.postflight.moved_siblings[1].album_id, 10315)
        self.assertEqual(
            r2.postflight.moved_siblings[1].discogs_albumid, "12856590")

    def test_postflight_legacy_row_without_moved_siblings_field(self):
        """Old download_log rows predating issue #133 have no
        ``moved_siblings`` key. Deserialization must default to an
        empty list, not raise."""
        d = {
            "version": 2,
            "exit_code": 0,
            "decision": "import",
            "postflight": {
                "beets_id": 42,
                "track_count": 11,
                "imported_path": "/Beets/Artist/Album",
                "disambiguated": True,
                # NO moved_siblings key.
            },
        }
        r = ImportResult.from_dict(d)
        self.assertEqual(r.postflight.moved_siblings, [])

    def test_postflight_moved_siblings_malformed_value_falls_back_to_empty(self):
        """Wire-boundary robustness: if ``moved_siblings`` arrives as
        a non-list (corrupt legacy JSONB row, future bug), the loader
        silently drops it and defaults to ``[]``. We prefer "silent
        empty" over "crash whole row" — the row's other fields are
        still useful for the web UI, and moved_siblings being wrong
        is already a data-quality issue somewhere upstream."""
        for bad in ("not-a-list", 42, {}, None):
            with self.subTest(bad_value=bad):
                d = {
                    "version": 2,
                    "exit_code": 0,
                    "decision": "import",
                    "postflight": {
                        "beets_id": 42,
                        "imported_path": "/Beets/Artist/Album",
                        "moved_siblings": bad,
                    },
                }
                r = ImportResult.from_dict(d)
                self.assertEqual(r.postflight.moved_siblings, [])

    def test_postflight_moved_siblings_wrong_element_type_raises(self):
        """Issue #99 wire-boundary contract: if a harness change ever
        emits a ``MovedSibling`` with a wrong-typed field (e.g.
        ``album_id`` as string, which beets COULD theoretically
        return as a string in some future patch), the loader must
        raise ``msgspec.ValidationError`` at the boundary rather than
        silently propagating bad data downstream.

        Guards against the PR #98 bug pattern where a dataclass
        declared ``album_id: str`` but the wire carried ``int`` and
        every decode silently succeeded with a type mismatch.
        """
        import msgspec
        d = {
            "version": 2,
            "exit_code": 0,
            "decision": "import",
            "postflight": {
                "beets_id": 42,
                "imported_path": "/Beets/Artist/Album",
                # album_id is declared ``int`` on MovedSibling —
                # feeding a string must raise.
                "moved_siblings": [
                    {"album_id": "10314", "new_path": "/p"},
                ],
            },
        }
        with self.assertRaises(msgspec.ValidationError):
            ImportResult.from_dict(d)

    def test_postflight_pre_133_failure_row_without_selector_field(self):
        """Issue #133 back-compat: old download_log rows serialized
        AFTER #127 but BEFORE #133 have ``disambiguation_failure``
        with only ``{reason, detail}`` — no ``selector``. The unified
        ``BeetsOpFailure`` (was ``DisambiguationFailure``) added
        ``selector: str = ""`` so these rows still deserialize.

        Without the default, ``DisambiguationFailure(**{"reason":"timeout",
        "detail":"x"})`` would raise ``TypeError`` and every
        ``/api/pipeline/force-import`` call that parses old import_result
        JSONB would 500. This test nails the contract so a future
        refactor can't remove the default silently.
        """
        d = {
            "version": 2,
            "exit_code": 0,
            "decision": "import",
            "postflight": {
                "beets_id": 42,
                "track_count": 11,
                "imported_path": "/Beets/Artist/Album",
                "disambiguated": False,
                "disambiguation_failure": {
                    "reason": "timeout",
                    "detail": "timeout after 120s",
                    # NO selector key — predates issue #133.
                },
            },
        }
        r = ImportResult.from_dict(d)
        assert r.postflight.disambiguation_failure is not None
        self.assertEqual(r.postflight.disambiguation_failure.reason, "timeout")
        self.assertEqual(
            r.postflight.disambiguation_failure.detail, "timeout after 120s")
        # Missing-in-JSON → default empty string on the dataclass.
        self.assertEqual(r.postflight.disambiguation_failure.selector, "")

    def test_full_construction(self):
        r = ImportResult(
            exit_code=0,
            decision="import",
            already_in_beets=True,
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245, spectral_grade="genuine",
                verified_lossless=True, was_converted_from="flac"),
            existing_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=192, spectral_grade="suspect",
                spectral_bitrate_kbps=128),
            conversion=ConversionInfo(
                converted=10, failed=0, was_converted=True,
                original_filetype="flac", target_filetype="mp3"),
            postflight=PostflightInfo(
                beets_id=1234, track_count=12,
                imported_path="/mnt/virtio/Music/Beets/Artist/Album"),
        )
        self.assertEqual(r.decision, "import")
        self.assertEqual(r.conversion.converted, 10)
        assert r.new_measurement is not None
        self.assertTrue(r.new_measurement.verified_lossless)
        assert r.existing_measurement is not None
        self.assertEqual(r.existing_measurement.spectral_bitrate_kbps, 128)
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
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=180, spectral_grade="suspect",
                spectral_bitrate_kbps=128),
            existing_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128, spectral_grade="suspect",
                spectral_bitrate_kbps=96),
            conversion=ConversionInfo(converted=8, failed=0, was_converted=True,
                                      original_filetype="flac", target_filetype="mp3",
                                      is_transcode=True),
            spectral=SpectralDetail(cliff_freq_hz=16500),
            postflight=PostflightInfo(beets_id=42, track_count=8,
                                      imported_path="/Beets/Artist/Album"),
        )
        j = r.to_json()
        r2 = ImportResult.from_json(j)
        self.assertEqual(r, r2)

    def test_round_trip_opus_fields(self):
        """Opus audit fields survive JSON round-trip."""
        r = ImportResult(
            decision="import",
            v0_verification_bitrate=247,
            final_format="opus 128",
            conversion=ConversionInfo(
                converted=10, was_converted=True,
                original_filetype="flac", target_filetype="mp3",
                final_format="opus 128"),
        )
        j = r.to_json()
        r2 = ImportResult.from_json(j)
        self.assertEqual(r2.v0_verification_bitrate, 247)
        self.assertEqual(r2.final_format, "opus 128")
        self.assertEqual(r2.conversion.final_format, "opus 128")

    def test_from_dict_without_opus_fields_defaults_none(self):
        """Old JSONB rows without opus fields should parse with None defaults."""
        d = {"version": 2, "exit_code": 0, "decision": "import",
             "conversion": {"converted": 5, "failed": 0, "was_converted": True,
                            "original_filetype": "flac", "target_filetype": "mp3",
                            "post_conversion_min_bitrate": 247,
                            "is_transcode": False}}
        r = ImportResult.from_dict(d)
        self.assertIsNone(r.v0_verification_bitrate)
        self.assertIsNone(r.final_format)
        self.assertIsNone(r.conversion.final_format)

    def test_to_json_is_valid_json(self):
        r = ImportResult(decision="import", exit_code=0)
        parsed = json.loads(r.to_json())
        self.assertEqual(parsed["decision"], "import")
        self.assertEqual(parsed["version"], 2)

    def test_from_dict_missing_optional_sections(self):
        """from_dict should handle missing sub-dicts gracefully."""
        d = {"version": 2, "exit_code": 0, "decision": "import"}
        r = ImportResult.from_dict(d)
        self.assertEqual(r.decision, "import")
        self.assertEqual(r.conversion.converted, 0)
        self.assertIsNone(r.new_measurement)

    def test_from_dict_with_extra_fields_in_sub(self):
        """Unknown fields in sub-dicts should raise (strict typing)."""
        d = {
            "version": 2, "exit_code": 0, "decision": "import",
            "conversion": {"converted": 5, "failed": 0, "was_converted": True,
                           "original_filetype": "flac", "target_filetype": "mp3",
                           "post_conversion_min_bitrate": None,
                           "is_transcode": False,
                           "bogus_field": 999},
        }
        with self.assertRaises(TypeError):
            ImportResult.from_dict(d)

    def test_v1_migration(self):
        """Old format (with quality/spectral sub-objects) migrates to v2."""
        v1_dict = {
            "version": 1,
            "exit_code": 0,
            "decision": "import",
            "quality": {
                "new_min_bitrate": 245,
                "prev_min_bitrate": 192,
                "post_conversion_min_bitrate": 240,
                "is_transcode": False,
                "will_be_verified_lossless": True,
            },
            "spectral": {
                "grade": "genuine",
                "bitrate": None,
                "cliff_freq_hz": None,
                "suspect_pct": 0.1,
                "per_track": [{"grade": "genuine", "hf_deficit_db": 25.0}],
                "existing_grade": "suspect",
                "existing_bitrate": 128,
                "existing_suspect_pct": 0.8,
            },
            "conversion": {
                "converted": 10,
                "failed": 0,
                "was_converted": True,
                "original_filetype": "flac",
                "target_filetype": "mp3",
            },
            "postflight": {
                "beets_id": 100,
                "track_count": 10,
                "imported_path": "/Beets/Artist/Album",
                "bad_extensions": [],
                "disambiguated": False,
            },
        }
        r = ImportResult.from_dict(v1_dict)
        self.assertEqual(r.version, 2)
        assert r.new_measurement is not None
        self.assertEqual(r.new_measurement.min_bitrate_kbps, 245)
        self.assertEqual(r.new_measurement.spectral_grade, "genuine")
        self.assertTrue(r.new_measurement.verified_lossless)
        self.assertEqual(r.new_measurement.was_converted_from, "flac")
        assert r.existing_measurement is not None
        self.assertEqual(r.existing_measurement.min_bitrate_kbps, 192)
        self.assertEqual(r.existing_measurement.spectral_grade, "suspect")
        self.assertEqual(r.existing_measurement.spectral_bitrate_kbps, 128)
        # Process data migrated to ConversionInfo
        self.assertEqual(r.conversion.post_conversion_min_bitrate, 240)
        self.assertFalse(r.conversion.is_transcode)
        # SpectralDetail has per-track only
        self.assertEqual(r.spectral.suspect_pct, 0.1)
        self.assertEqual(len(r.spectral.per_track), 1)
        self.assertEqual(r.spectral.existing_suspect_pct, 0.8)

    def test_v1_migration_no_existing(self):
        """Old format with no prev_min_bitrate → existing_measurement is None."""
        v1_dict = {
            "version": 1,
            "decision": "import",
            "quality": {"new_min_bitrate": 245},
            "spectral": {"grade": "genuine"},
            "conversion": {},
        }
        r = ImportResult.from_dict(v1_dict)
        assert r.new_measurement is not None
        self.assertEqual(r.new_measurement.min_bitrate_kbps, 245)
        self.assertIsNone(r.existing_measurement)


class TestImportResultProductionFixtures(unittest.TestCase):
    """Pin tests: verbatim production ``download_log.import_result`` JSONB rows
    must round-trip via ``from_dict`` → ``to_json`` → ``from_dict`` and yield
    an equal ``ImportResult`` each time. These fixtures were captured from
    the live doc2 database while scoping issue #141 (wire-boundary encoder
    unification) — they lock down the shapes the refactor MUST preserve.
    """

    def test_production_v2_current_shape_roundtrip(self):
        """Current (2026-04) production v2 row from a successful import with
        moved_siblings=[] and disambiguation_failure=null. Captured verbatim.
        """
        prod_row = {
            "error": None,
            "version": 2,
            "decision": "import",
            "spectral": {
                "per_track": [
                    {"grade": "marginal", "cliff_freq_hz": None,
                     "hf_deficit_db": 57.9, "cliff_detected": False,
                     "estimated_bitrate_kbps": None},
                    {"grade": "suspect", "cliff_freq_hz": None,
                     "hf_deficit_db": 61.6, "cliff_detected": False,
                     "estimated_bitrate_kbps": None},
                ],
                "suspect_pct": 80.0,
                "cliff_freq_hz": None,
                "existing_suspect_pct": 100.0,
            },
            "beets_log": [],
            "exit_code": 0,
            "conversion": {
                "failed": 0, "converted": 0, "final_format": None,
                "is_transcode": False, "was_converted": False,
                "target_filetype": None, "original_filetype": None,
                "post_conversion_min_bitrate": None,
            },
            "postflight": {
                "beets_id": 10323,
                "track_count": 5,
                "disambiguated": False,
                "imported_path": "/mnt/virtio/Music/Beets/Shearwater/2005 - Thieves",
                "bad_extensions": [],
                "disambiguation_failure": None,
            },
            "final_format": None,
            "new_measurement": {
                "format": "MP3", "is_cbr": False,
                "spectral_grade": "likely_transcode",
                "avg_bitrate_kbps": 246, "min_bitrate_kbps": 233,
                "verified_lossless": False, "was_converted_from": None,
                "median_bitrate_kbps": 248,
                "spectral_bitrate_kbps": None,
            },
            "already_in_beets": True,
            "existing_measurement": {
                "format": "MP3", "is_cbr": False,
                "spectral_grade": "likely_transcode",
                "avg_bitrate_kbps": 160, "min_bitrate_kbps": 160,
                "verified_lossless": False, "was_converted_from": None,
                "median_bitrate_kbps": 160,
                "spectral_bitrate_kbps": 160,
            },
            "v0_verification_bitrate": None,
        }
        r = ImportResult.from_dict(prod_row)
        self.assertEqual(r.decision, "import")
        self.assertTrue(r.already_in_beets)
        self.assertEqual(r.postflight.beets_id, 10323)
        assert r.new_measurement is not None
        self.assertEqual(r.new_measurement.avg_bitrate_kbps, 246)
        self.assertEqual(r.new_measurement.median_bitrate_kbps, 248)
        self.assertEqual(r.new_measurement.format, "MP3")
        assert r.existing_measurement is not None
        self.assertEqual(r.existing_measurement.spectral_bitrate_kbps, 160)
        self.assertEqual(len(r.spectral.per_track), 2)
        self.assertEqual(r.spectral.suspect_pct, 80.0)
        # Round-trip via to_json preserves everything.
        r2 = ImportResult.from_json(r.to_json())
        self.assertEqual(r, r2)

    def test_production_v2_with_moved_siblings_roundtrip(self):
        """v2 row with populated moved_siblings. Same row schema, but the
        sibling list carries typed ``MovedSibling`` records — the refactor
        must preserve both the Python type (strict decode at the wire
        boundary) AND the on-disk JSON shape."""
        prod_row = {
            "error": None, "version": 2, "decision": "import",
            "spectral": {"per_track": [], "suspect_pct": 0.0,
                         "cliff_freq_hz": None, "existing_suspect_pct": 0.0},
            "beets_log": [], "exit_code": 0,
            "conversion": {
                "failed": 0, "converted": 10, "final_format": "mp3 v0",
                "is_transcode": False, "was_converted": True,
                "target_filetype": "mp3", "original_filetype": "flac",
                "post_conversion_min_bitrate": 245,
            },
            "postflight": {
                "beets_id": 10325,
                "track_count": 9,
                "disambiguated": True,
                "imported_path": "/Beets/Artist/Album [2007]",
                "bad_extensions": [],
                "moved_siblings": [
                    {"album_id": 10314,
                     "new_path": "/Beets/Artist/Album [2006]",
                     "mb_albumid": "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                     "discogs_albumid": ""},
                ],
                "disambiguation_failure": None,
            },
            "final_format": "mp3 v0",
            "new_measurement": {
                "format": "mp3 v0", "is_cbr": False,
                "spectral_grade": "genuine", "avg_bitrate_kbps": 248,
                "min_bitrate_kbps": 245, "verified_lossless": True,
                "was_converted_from": "flac", "median_bitrate_kbps": 246,
                "spectral_bitrate_kbps": None,
            },
            "already_in_beets": False,
            "existing_measurement": None,
            "v0_verification_bitrate": 245,
        }
        r = ImportResult.from_dict(prod_row)
        self.assertEqual(len(r.postflight.moved_siblings), 1)
        sib = r.postflight.moved_siblings[0]
        self.assertEqual(sib.album_id, 10314)
        self.assertEqual(sib.mb_albumid,
                         "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb")
        assert r.new_measurement is not None
        self.assertTrue(r.new_measurement.verified_lossless)
        r2 = ImportResult.from_json(r.to_json())
        self.assertEqual(r, r2)

    def test_production_v1_row_roundtrip(self):
        """Verbatim v1 row captured from prod (226 such rows live in
        download_log on doc2 as of 2026-04-20). The v1→v2 migration must
        keep working; the Python object is a normalised v2 ``ImportResult``.
        """
        v1_row = {
            "error": None,
            "quality": {
                "is_transcode": False,
                "new_min_bitrate": 320,
                "prev_min_bitrate": 128,
                "will_be_verified_lossless": False,
                "post_conversion_min_bitrate": None,
            },
            "version": 1,
            "decision": "import",
            "spectral": {
                "grade": None, "bitrate": None, "per_track": [],
                "suspect_pct": 0.0, "cliff_freq_hz": None,
                "existing_grade": None, "existing_bitrate": None,
                "existing_suspect_pct": 0.0,
            },
            "beets_log": [
                "INFO: pylast: album.getTopTags",
                "INFO: httpx: HTTP Request: POST https://ws.audioscrobbler.com/2.0/",
            ],
            "exit_code": 0,
            "conversion": {
                "failed": 0, "converted": 0, "was_converted": False,
                "target_filetype": None, "original_filetype": None,
            },
            "postflight": {
                "beets_id": 9026, "track_count": 13, "disambiguated": False,
                "imported_path": "/mnt/virtio/Music/Beets/Ye/2026 - BULLY",
                "bad_extensions": [],
            },
            "already_in_beets": True,
        }
        r = ImportResult.from_dict(v1_row)
        # Migrated to v2 in memory.
        self.assertEqual(r.version, 2)
        self.assertEqual(r.decision, "import")
        assert r.new_measurement is not None
        self.assertEqual(r.new_measurement.min_bitrate_kbps, 320)
        assert r.existing_measurement is not None
        self.assertEqual(r.existing_measurement.min_bitrate_kbps, 128)
        self.assertEqual(r.postflight.beets_id, 9026)
        self.assertEqual(r.postflight.track_count, 13)
        # And v1 migrates cleanly to v2 JSON, which round-trips as v2.
        r2 = ImportResult.from_json(r.to_json())
        self.assertEqual(r, r2)

    def test_production_v2_pre_133_disambiguation_failure_roundtrip(self):
        """Pre-#133 production shape: disambiguation_failure is populated
        with ``{reason, detail}`` only — no ``selector`` key (the field
        was added in PR #131). ``BeetsOpFailure.selector`` must default
        to ``""`` so force-import / web UI reads don't 500 on old rows.
        """
        prod_row = {
            "version": 2,
            "exit_code": 0,
            "decision": "import",
            "postflight": {
                "beets_id": 42,
                "track_count": 11,
                "imported_path": "/Beets/Artist/Album",
                "disambiguated": False,
                "bad_extensions": [],
                "disambiguation_failure": {
                    "reason": "nonzero_rc",
                    "detail": "rc=1: ModuleNotFoundError: No module named 'msgspec'",
                    # NO selector field — this is the pre-#133 shape.
                },
            },
        }
        r = ImportResult.from_dict(prod_row)
        df = r.postflight.disambiguation_failure
        assert df is not None
        self.assertEqual(df.reason, "nonzero_rc")
        self.assertIn("ModuleNotFoundError", df.detail)
        self.assertEqual(df.selector, "")
        # Round-trip via to_json → from_json — new JSON carries the
        # default-materialised selector="" but the semantic object is equal.
        r2 = ImportResult.from_json(r.to_json())
        assert r2.postflight.disambiguation_failure is not None
        self.assertEqual(r2.postflight.disambiguation_failure.reason,
                         "nonzero_rc")
        self.assertEqual(r2.postflight.disambiguation_failure.selector, "")


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
        r = ImportResult(
            decision="transcode_upgrade", exit_code=6,
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=180))
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
        assert parsed.new_measurement is not None
        self.assertEqual(parsed.new_measurement.min_bitrate_kbps, 180)

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
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245, spectral_grade="genuine",
                verified_lossless=True, was_converted_from="flac"),
            conversion=ConversionInfo(
                converted=12, failed=0, was_converted=True,
                original_filetype="flac", target_filetype="mp3"),
            postflight=PostflightInfo(
                beets_id=100, track_count=12,
                imported_path="/Beets/Artist/Album"),
        )
        self.assertEqual(r.exit_code, 0)
        self.assertTrue(r.conversion.was_converted)
        assert r.new_measurement is not None
        self.assertTrue(r.new_measurement.verified_lossless)
        self.assertFalse(r.conversion.is_transcode)
        self.assertIsNone(r.error)

    def test_downgrade_prevented(self):
        """New files worse than existing — exit 5."""
        r = ImportResult(
            exit_code=5,
            decision="downgrade",
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=192),
            existing_measurement=AudioQualityMeasurement(min_bitrate_kbps=320),
        )
        self.assertEqual(r.exit_code, 5)
        self.assertEqual(r.decision, "downgrade")
        self.assertIsNone(r.postflight.beets_id)  # not imported

    def test_transcode_upgrade(self):
        """Fake FLAC detected but better than existing — exit 6, imported."""
        r = ImportResult(
            exit_code=6,
            decision="transcode_upgrade",
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=180, spectral_grade="suspect",
                spectral_bitrate_kbps=128),
            existing_measurement=AudioQualityMeasurement(min_bitrate_kbps=128),
            conversion=ConversionInfo(
                converted=10, failed=0, was_converted=True,
                original_filetype="flac", target_filetype="mp3",
                is_transcode=True),
            spectral=SpectralDetail(cliff_freq_hz=16500),
            postflight=PostflightInfo(beets_id=42, track_count=10,
                                      imported_path="/Beets/Artist/Album"),
        )
        self.assertEqual(r.exit_code, 6)
        self.assertTrue(r.conversion.is_transcode)
        self.assertIsNotNone(r.postflight.beets_id)  # was imported

    def test_transcode_downgrade(self):
        """Fake FLAC and not better — exit 6, not imported."""
        r = ImportResult(
            exit_code=6,
            decision="transcode_downgrade",
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=128),
            existing_measurement=AudioQualityMeasurement(min_bitrate_kbps=180),
            conversion=ConversionInfo(is_transcode=True),
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


class TestDownloadInfo(unittest.TestCase):
    """Test DownloadInfo dataclass."""

    def test_defaults(self) -> None:
        dl = DownloadInfo()
        self.assertIsNone(dl.username)
        self.assertIsNone(dl.filetype)
        self.assertFalse(dl.was_converted)
        self.assertIsNone(dl.download_spectral)
        self.assertIsNone(dl.import_result)

    def test_flac_conversion(self) -> None:
        dl = DownloadInfo(
            username="testuser",
            filetype="mp3",
            bitrate=245000,
            is_vbr=True,
            was_converted=True,
            original_filetype="flac",
            slskd_filetype="flac",
            actual_filetype="mp3",
            download_spectral=SpectralMeasurement(grade="genuine"),
        )
        self.assertTrue(dl.was_converted)
        self.assertEqual(dl.original_filetype, "flac")
        self.assertEqual(dl.actual_filetype, "mp3")
        assert dl.download_spectral is not None
        self.assertEqual(dl.download_spectral.grade, "genuine")

    def test_attribute_error_on_typo(self) -> None:
        """Key advantage over dict: typos are caught at attribute access."""
        dl = DownloadInfo()
        with self.assertRaises(AttributeError):
            _ = dl.spectral_grad  # type: ignore[attr-defined]

    def test_populate_from_import_result(self) -> None:
        """Verify the contract: ImportResult fields map to DownloadInfo fields."""
        ir = ImportResult(
            decision="import",
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245, spectral_grade="genuine",
                verified_lossless=True, was_converted_from="flac"),
            existing_measurement=AudioQualityMeasurement(
                spectral_bitrate_kbps=128),
            conversion=ConversionInfo(
                converted=10, was_converted=True,
                original_filetype="flac", target_filetype="mp3"),
        )
        new_m = ir.new_measurement
        assert new_m is not None
        existing_m = ir.existing_measurement
        assert existing_m is not None
        dl = DownloadInfo(
            was_converted=ir.conversion.was_converted,
            original_filetype=ir.conversion.original_filetype,
            filetype=ir.conversion.target_filetype,
            is_vbr=True,
            slskd_filetype=ir.conversion.original_filetype,
            actual_filetype=ir.conversion.target_filetype,
            bitrate=(new_m.min_bitrate_kbps * 1000
                     if new_m.min_bitrate_kbps else None),
            download_spectral=SpectralMeasurement.from_parts(
                new_m.spectral_grade, new_m.spectral_bitrate_kbps),
            current_spectral=SpectralMeasurement.from_parts(
                existing_m.spectral_grade, existing_m.spectral_bitrate_kbps),
            import_result=ir.to_json(),
        )
        self.assertTrue(dl.was_converted)
        self.assertEqual(dl.bitrate, 245000)
        assert dl.download_spectral is not None
        self.assertEqual(dl.download_spectral.grade, "genuine")
        assert dl.current_spectral is not None
        self.assertEqual(dl.current_spectral.bitrate_kbps, 128)
        stored = json.loads(dl.import_result)  # type: ignore[arg-type]
        self.assertEqual(stored["decision"], "import")


class TestPopulateDlInfoFromImportResult(unittest.TestCase):
    """Test the _populate_dl_info_from_import_result helper."""

    def setUp(self) -> None:
        from lib.import_dispatch import _populate_dl_info_from_import_result
        self.populate = _populate_dl_info_from_import_result

    def test_flac_conversion(self) -> None:
        dl = DownloadInfo(filetype="flac", bitrate=0)
        ir = ImportResult(
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245, spectral_grade="genuine",
                verified_lossless=True, was_converted_from="flac"),
            existing_measurement=AudioQualityMeasurement(
                spectral_bitrate_kbps=128),
            conversion=ConversionInfo(converted=10, was_converted=True,
                                      original_filetype="flac", target_filetype="mp3"),
        )
        self.populate(dl, ir)
        self.assertTrue(dl.was_converted)
        self.assertEqual(dl.filetype, "mp3")
        self.assertEqual(dl.slskd_filetype, "flac")
        self.assertEqual(dl.actual_filetype, "mp3")
        self.assertEqual(dl.bitrate, 245000)
        assert dl.download_spectral is not None
        self.assertEqual(dl.download_spectral.grade, "genuine")
        assert dl.current_spectral is not None
        self.assertEqual(dl.current_spectral.bitrate_kbps, 128)
        self.assertTrue(dl.verified_lossless_override)
        self.assertIsNotNone(dl.import_result)

    def test_no_conversion(self) -> None:
        dl = DownloadInfo(filetype="mp3", bitrate=320000)
        ir = ImportResult(
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, spectral_grade="genuine"),
            conversion=ConversionInfo(),
        )
        self.populate(dl, ir)
        self.assertFalse(dl.was_converted)
        self.assertEqual(dl.slskd_filetype, "mp3")
        self.assertEqual(dl.actual_filetype, "mp3")
        self.assertEqual(dl.bitrate, 320000)


class TestActiveDownloadState(unittest.TestCase):
    """Test ActiveDownloadState and ActiveDownloadFileState dataclasses."""

    def test_active_download_state_to_json(self):
        """Serialize, verify JSON structure."""
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            last_progress_at="2026-04-03T12:03:00+00:00",
            processing_started_at="2026-04-03T12:05:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1",
                    filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music",
                    size=30000000,
                    retry_count=2,
                    bytes_transferred=1024,
                    last_state="InProgress",
                ),
            ],
        )
        j = json.loads(state.to_json())
        self.assertEqual(j["filetype"], "flac")
        self.assertEqual(j["enqueued_at"], "2026-04-03T12:00:00+00:00")
        self.assertEqual(j["last_progress_at"], "2026-04-03T12:03:00+00:00")
        self.assertEqual(j["processing_started_at"], "2026-04-03T12:05:00+00:00")
        self.assertEqual(len(j["files"]), 1)
        self.assertEqual(j["files"][0]["username"], "user1")
        self.assertEqual(j["files"][0]["size"], 30000000)
        self.assertEqual(j["files"][0]["retry_count"], 2)
        self.assertEqual(j["files"][0]["bytes_transferred"], 1024)
        self.assertEqual(j["files"][0]["last_state"], "InProgress")

    def test_active_download_state_from_json(self):
        """Deserialize, verify all fields."""
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        raw = json.dumps({
            "filetype": "mp3 v0",
            "enqueued_at": "2026-04-03T14:30:00+00:00",
            "last_progress_at": "2026-04-03T14:31:00+00:00",
            "processing_started_at": "2026-04-03T14:35:00+00:00",
            "files": [
                {"username": "bob", "filename": "bob\\Tunes\\01.mp3",
                 "file_dir": "bob\\Tunes", "size": 5000000, "retry_count": 1,
                 "bytes_transferred": 2048, "last_state": "InProgress"},
                {"username": "bob", "filename": "bob\\Tunes\\02.mp3",
                 "file_dir": "bob\\Tunes", "size": 6000000,
                 "disk_no": 1, "disk_count": 2, "retry_count": 4,
                 "bytes_transferred": 4096, "last_state": "Completed, Succeeded"},
            ],
        })
        state = ActiveDownloadState.from_json(raw)
        self.assertEqual(state.filetype, "mp3 v0")
        self.assertEqual(state.enqueued_at, "2026-04-03T14:30:00+00:00")
        self.assertEqual(state.last_progress_at, "2026-04-03T14:31:00+00:00")
        self.assertEqual(state.processing_started_at, "2026-04-03T14:35:00+00:00")
        self.assertEqual(len(state.files), 2)
        self.assertEqual(state.files[0].username, "bob")
        self.assertEqual(state.files[0].retry_count, 1)
        self.assertEqual(state.files[0].bytes_transferred, 2048)
        self.assertEqual(state.files[0].last_state, "InProgress")
        self.assertEqual(state.files[1].disk_no, 1)
        self.assertEqual(state.files[1].disk_count, 2)
        self.assertEqual(state.files[1].retry_count, 4)
        self.assertEqual(state.files[1].bytes_transferred, 4096)
        self.assertEqual(state.files[1].last_state, "Completed, Succeeded")
        self.assertIsNone(state.files[0].disk_no)

    def test_active_download_state_roundtrip(self):
        """to_json → from_json identity."""
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        original = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            last_progress_at="2026-04-03T12:01:00+00:00",
            processing_started_at="2026-04-03T12:02:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=30000000,
                    disk_no=2, disk_count=3, retry_count=5,
                    bytes_transferred=8192, last_state="InProgress",
                ),
            ],
        )
        restored = ActiveDownloadState.from_json(original.to_json())
        self.assertEqual(restored.filetype, original.filetype)
        self.assertEqual(restored.enqueued_at, original.enqueued_at)
        self.assertEqual(restored.last_progress_at, original.last_progress_at)
        self.assertEqual(restored.processing_started_at, original.processing_started_at)
        self.assertEqual(len(restored.files), 1)
        self.assertEqual(restored.files[0].username, "user1")
        self.assertEqual(restored.files[0].disk_no, 2)
        self.assertEqual(restored.files[0].disk_count, 3)
        self.assertEqual(restored.files[0].retry_count, 5)
        self.assertEqual(restored.files[0].bytes_transferred, 8192)
        self.assertEqual(restored.files[0].last_state, "InProgress")

    def test_active_download_file_state_fields(self):
        """Verify per-file fields present."""
        from lib.quality import ActiveDownloadFileState
        f = ActiveDownloadFileState(
            username="alice", filename="alice\\Music\\track.flac",
            file_dir="alice\\Music", size=25000000,
        )
        self.assertEqual(f.username, "alice")
        self.assertEqual(f.filename, "alice\\Music\\track.flac")
        self.assertEqual(f.file_dir, "alice\\Music")
        self.assertEqual(f.size, 25000000)
        self.assertIsNone(f.disk_no)
        self.assertIsNone(f.disk_count)
        self.assertEqual(f.retry_count, 0)
        self.assertEqual(f.bytes_transferred, 0)
        self.assertIsNone(f.last_state)

    def test_active_download_state_enqueued_at_iso(self):
        """Verify ISO8601 datetime format."""
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[],
        )
        j = json.loads(state.to_json())
        # Should be valid ISO8601 — parse it
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(j["enqueued_at"])
        self.assertEqual(dt.tzinfo, timezone.utc)


if __name__ == "__main__":
    unittest.main()
