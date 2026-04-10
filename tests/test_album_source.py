"""Tests for album_source.py — AlbumRecord and DatabaseSource."""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Bootstrap ephemeral PostgreSQL if available
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tagging-workspace", "scripts"))

from album_source import AlbumRecord, DatabaseSource
from lib.grab_list import GrabListEntry, DownloadFile
from lib.quality import ValidationResult

TEST_DSN = os.environ.get("TEST_DB_DSN")


def _make_record(**overrides):
    """Build a minimal GrabListEntry for album_source method tests."""
    defaults = dict(
        album_id=0, files=[], filetype="mp3", title="T", artist="A",
        year="2024", mb_release_id="",
    )
    defaults.update(overrides)
    return GrabListEntry(**defaults)  # type: ignore[arg-type]


SAMPLE_DB_ROW = {
    "id": 42,
    "mb_release_id": "44438bf9-26d9-4460-9b4f-1a1b015e37a1",
    "mb_release_group_id": "rg-uuid",
    "mb_artist_id": "artist-uuid",
    "discogs_release_id": None,
    "artist_name": "Buke and Gase",
    "album_title": "Riposte",
    "year": 2014,
    "country": "US",
    "format": "CD",
    "source": "request",
    "status": "wanted",
}

SAMPLE_TRACKS = [
    {"disc_number": 1, "track_number": 1, "title": "Houdini Crush", "length_seconds": 200},
    {"disc_number": 1, "track_number": 2, "title": "Hiccup", "length_seconds": 180},
    {"disc_number": 1, "track_number": 3, "title": "Metazoa", "length_seconds": 220},
]


class TestAlbumRecordFromDbRow(unittest.TestCase):
    def test_basic_shape(self):
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, SAMPLE_TRACKS)
        self.assertEqual(record.title, "Riposte")
        self.assertEqual(record.artist_name, "Buke and Gase")
        self.assertIsNotNone(record.release_date)
        self.assertEqual(len(record.releases), 1)

    def test_release_has_correct_fields(self):
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, SAMPLE_TRACKS)
        release = record.releases[0]
        self.assertEqual(release.foreign_release_id, "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        self.assertEqual(release.track_count, 3)
        self.assertTrue(release.monitored)
        self.assertEqual(len(release.media), 1)  # single disc

    def test_multi_disc(self):
        tracks = [
            {"disc_number": 1, "track_number": 1, "title": "D1T1", "length_seconds": 200},
            {"disc_number": 1, "track_number": 2, "title": "D1T2", "length_seconds": 200},
            {"disc_number": 2, "track_number": 1, "title": "D2T1", "length_seconds": 200},
        ]
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, tracks)
        release = record.releases[0]
        self.assertEqual(release.track_count, 3)
        self.assertEqual(len(release.media), 2)  # two discs

    def test_db_metadata_preserved(self):
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, SAMPLE_TRACKS)
        self.assertEqual(record.db_request_id, 42)
        self.assertEqual(record.db_source, "request")
        self.assertEqual(record.db_mb_release_id, "44438bf9-26d9-4460-9b4f-1a1b015e37a1")

    def test_negative_id_space(self):
        """DB records use negative IDs."""
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, SAMPLE_TRACKS)
        self.assertLess(record.id, 0)


@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set — skipping PostgreSQL tests")
class TestDatabaseSource(unittest.TestCase):
    def _make_source(self):
        """Create a DatabaseSource with test PostgreSQL DB."""
        from pipeline_db import PipelineDB
        db = PipelineDB(TEST_DSN)
        for table in ["source_denylist", "download_log", "album_tracks", "album_requests"]:
            db._execute(f"TRUNCATE {table} CASCADE")
        db.conn.commit()
        source = DatabaseSource(TEST_DSN)
        source._db = db
        return source, db

    def test_get_wanted_returns_lidarr_shaped_records(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="test-uuid",
            artist_name="Test",
            album_title="Album",
            source="request",
        )
        db.set_tracks(req_id, SAMPLE_TRACKS)

        records = source.get_wanted()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].title, "Album")
        self.assertEqual(records[0].artist_name, "Test")

    def test_get_tracks_lidarr_format(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="track-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        db.set_tracks(req_id, SAMPLE_TRACKS)

        record = _make_record(db_request_id=req_id)
        tracks = source.get_tracks(record)
        self.assertEqual(len(tracks), 3)
        self.assertEqual(tracks[0]["title"], "Houdini Crush")
        self.assertIn("trackNumber", tracks[0])
        self.assertIn("mediumNumber", tracks[0])
        self.assertIn("duration", tracks[0])

    def test_mark_done_redownload_stages(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="rd-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        record = _make_record(db_request_id=req_id, db_source="redownload")
        bv_result = ValidationResult(valid=True, distance=0.08, scenario="strong_match")

        source.mark_done(record, bv_result, dest_path="/Incoming/A/B")

        req = db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "imported")
        self.assertAlmostEqual(req["beets_distance"], 0.08)

    def test_mark_done_request_stages(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="req-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=True, distance=0.05, scenario="strong_match")

        source.mark_done(record, bv_result, dest_path="/Incoming/A/B")

        req = db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "imported")

    def test_reject_and_requeue_updates_status_and_denylists(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="fail-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=False, distance=0.35, scenario="high_distance")

        source.reject_and_requeue(record, bv_result, usernames={"bad_user1", "bad_user2"})

        req = db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["validation_attempts"], 1)

        denied = db.get_denylisted_users(req_id)
        usernames = {d["username"] for d in denied}
        self.assertEqual(usernames, {"bad_user1", "bad_user2"})

    def test_reject_and_requeue_preserves_existing_search_filetype_override(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="fail-preserve-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        db.update_request_fields(req_id, search_filetype_override="flac,mp3 v0,mp3 320")
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=False, distance=0.35, scenario="high_distance")

        source.reject_and_requeue(record, bv_result)

        req = db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0,mp3 320")

    def test_reject_and_requeue_uses_explicit_narrowed_override(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="fail-narrow-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        db.update_request_fields(req_id, search_filetype_override="flac,mp3 v0,mp3 320")
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=False, distance=0.35, scenario="quality_downgrade")

        source.reject_and_requeue(record, bv_result,
                                  search_filetype_override="flac,mp3 v0")

        req = db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_mark_done_sets_on_disk_spectral(self):
        """Successful import updates current_spectral_grade/bitrate."""
        from lib.quality import DownloadInfo, SpectralMeasurement
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="spectral-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=True, distance=0.05, scenario="strong_match")
        dl = DownloadInfo()
        dl.download_spectral = SpectralMeasurement(grade="suspect", bitrate_kbps=160)

        source.mark_done(record, bv_result, dest_path="/Incoming/A/B",
                         download_info=dl)

        req = db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["current_spectral_grade"], "suspect")
        self.assertEqual(req["current_spectral_bitrate"], 160)

    def test_mark_done_override_false_prevents_verified_lossless(self):
        """import_one says will_be_verified_lossless=False — mark_done must not
        set verified_lossless=True even if is_verified_lossless() would."""
        from lib.quality import DownloadInfo, SpectralMeasurement
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="vl-override-uuid", artist_name="The National",
            album_title="The Virginia EP", source="request")
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=True, distance=0.025, scenario="strong_match")
        dl = DownloadInfo()
        dl.was_converted = True
        dl.original_filetype = "flac"
        dl.download_spectral = SpectralMeasurement(grade="genuine")
        dl.verified_lossless_override = False  # but import_one says no
        source.mark_done(record, bv_result, dest_path="/Incoming/A/B",
                         download_info=dl)
        req = db.get_request(req_id)
        assert req is not None
        self.assertFalse(req.get("verified_lossless", False))

    def test_mark_done_override_true_sets_verified_lossless(self):
        """import_one says will_be_verified_lossless=True — sets it."""
        from lib.quality import DownloadInfo, SpectralMeasurement
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="vl-true-uuid", artist_name="A",
            album_title="B", source="request")
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=True, distance=0.05, scenario="strong_match")
        dl = DownloadInfo()
        dl.was_converted = True
        dl.original_filetype = "flac"
        dl.download_spectral = SpectralMeasurement(grade="genuine")
        dl.verified_lossless_override = True
        source.mark_done(record, bv_result, dest_path="/Incoming/A/B",
                         download_info=dl)
        req = db.get_request(req_id)
        assert req is not None
        self.assertTrue(req.get("verified_lossless", False))

    def test_mark_done_verified_lossless_uses_bitrate_for_spectral(self):
        """When verified_lossless, current_spectral_bitrate should be the
        actual V0 min bitrate, not the spectral cliff estimate."""
        from lib.quality import DownloadInfo, SpectralMeasurement
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="vl-bitrate-uuid", artist_name="The National",
            album_title="Alligator", source="request")
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=True, distance=0.05, scenario="strong_match")
        dl = DownloadInfo()
        dl.was_converted = True
        dl.original_filetype = "flac"
        dl.download_spectral = SpectralMeasurement(grade="genuine", bitrate_kbps=128)
        dl.bitrate = 245000  # actual V0 min bitrate (bps)
        dl.verified_lossless_override = True
        source.mark_done(record, bv_result, dest_path="/Incoming/A/B",
                         download_info=dl)
        req = db.get_request(req_id)
        assert req is not None
        # Should use V0 bitrate (245), not spectral estimate (128)
        self.assertEqual(req["current_spectral_bitrate"], 245)
        # last_download_spectral_bitrate (the download's raw spectral) stays as-is
        self.assertEqual(req["last_download_spectral_bitrate"], 128)

    def test_mark_done_no_override_falls_back(self):
        """No override (legacy path) — derives from is_verified_lossless()."""
        from lib.quality import DownloadInfo, SpectralMeasurement
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="vl-fallback-uuid", artist_name="A",
            album_title="B", source="request")
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=True, distance=0.05, scenario="strong_match")
        dl = DownloadInfo()
        dl.was_converted = True
        dl.original_filetype = "flac"
        dl.download_spectral = SpectralMeasurement(grade="genuine")
        # No verified_lossless_override set
        source.mark_done(record, bv_result, dest_path="/Incoming/A/B",
                         download_info=dl)
        req = db.get_request(req_id)
        assert req is not None
        self.assertTrue(req.get("verified_lossless", False))

    def test_mark_done_genuine_clears_stale_spectral_bitrate(self):
        """When genuine files (no spectral cliff) replace a transcode,
        stale current_spectral_bitrate and last_download_spectral_bitrate
        must be cleared.
        Regression test for issue #18."""
        from lib.quality import DownloadInfo, SpectralMeasurement
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="stale-spectral-uuid", artist_name="Brand New",
            album_title="Daisy", source="request")
        # Simulate prior transcode import leaving stale spectral data
        db._execute(
            "UPDATE album_requests SET last_download_spectral_bitrate = %s, "
            "current_spectral_bitrate = %s, current_spectral_grade = %s, "
            "last_download_spectral_grade = %s WHERE id = %s",
            (192, 192, "likely_transcode", "likely_transcode", req_id))

        # New import: genuine MP3 V0 (no cliff → bitrate_kbps is None)
        record = _make_record(db_request_id=req_id, db_source="request")
        bv_result = ValidationResult(valid=True, distance=0.0, scenario="strong_match")
        dl = DownloadInfo()
        dl.download_spectral = SpectralMeasurement(grade="genuine", bitrate_kbps=None)
        source.mark_done(record, bv_result, dest_path="/Incoming/Brand New/Daisy",
                         download_info=dl)

        req = db.get_request(req_id)
        assert req is not None
        # Stale values must be cleared, not left at 192
        self.assertIsNone(req["current_spectral_bitrate"],
                          "current_spectral_bitrate should be cleared for genuine import")
        self.assertIsNone(req["last_download_spectral_bitrate"],
                          "last_download_spectral_bitrate should be cleared for genuine import")
        self.assertEqual(req["current_spectral_grade"], "genuine")
        self.assertEqual(req["last_download_spectral_grade"], "genuine")

    def test_get_denylisted_users(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="deny-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        db.add_denylist(req_id, "user1", "bad quality")
        record = _make_record(db_request_id=req_id)

        denied = source.get_denylisted_users(record)
        self.assertEqual(denied, {"user1"})



if __name__ == "__main__":
    unittest.main()
