"""Tests for album_source.py — AlbumSource abstraction for Soularr."""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Add both soularr root and tagging-workspace scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tagging-workspace", "scripts"))

from album_source import AlbumRecord, DatabaseSource, LidarrSource


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
        self.assertEqual(record["title"], "Riposte")
        self.assertEqual(record["artist"]["artistName"], "Buke and Gase")
        self.assertIn("releaseDate", record)
        self.assertEqual(len(record["releases"]), 1)

    def test_release_has_correct_fields(self):
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, SAMPLE_TRACKS)
        release = record["releases"][0]
        self.assertEqual(release["foreignReleaseId"], "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        self.assertEqual(release["trackCount"], 3)
        self.assertTrue(release["monitored"])
        self.assertEqual(len(release["media"]), 1)  # single disc

    def test_multi_disc(self):
        tracks = [
            {"disc_number": 1, "track_number": 1, "title": "D1T1", "length_seconds": 200},
            {"disc_number": 1, "track_number": 2, "title": "D1T2", "length_seconds": 200},
            {"disc_number": 2, "track_number": 1, "title": "D2T1", "length_seconds": 200},
        ]
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, tracks)
        release = record["releases"][0]
        self.assertEqual(release["trackCount"], 3)
        self.assertEqual(len(release["media"]), 2)  # two discs

    def test_db_metadata_preserved(self):
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, SAMPLE_TRACKS)
        self.assertEqual(record["_db_request_id"], 42)
        self.assertEqual(record["_db_source"], "request")
        self.assertEqual(record["_db_mb_release_id"], "44438bf9-26d9-4460-9b4f-1a1b015e37a1")

    def test_negative_id_space(self):
        """DB records use negative IDs to avoid collision with Lidarr IDs."""
        record = AlbumRecord.from_db_row(SAMPLE_DB_ROW, SAMPLE_TRACKS)
        self.assertLess(record["id"], 0)


class TestDatabaseSource(unittest.TestCase):
    def _make_source(self):
        """Create a DatabaseSource with in-memory DB."""
        # We need to set up the in-memory DB ourselves since DatabaseSource
        # normally connects to a file path
        from pipeline_db import PipelineDB
        db = PipelineDB(":memory:")
        source = DatabaseSource(":memory:")
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
        self.assertEqual(records[0]["title"], "Album")
        self.assertEqual(records[0]["artist"]["artistName"], "Test")

    def test_get_tracks_lidarr_format(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="track-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        db.set_tracks(req_id, SAMPLE_TRACKS)

        record = {"_db_request_id": req_id}
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
        record = {"_db_request_id": req_id, "_db_source": "redownload"}
        bv_result = {"valid": True, "distance": 0.08, "scenario": "strong_match"}

        source.mark_done(record, bv_result, dest_path="/Incoming/A/B")

        req = db.get_request(req_id)
        self.assertEqual(req["status"], "staged")  # NOT imported
        self.assertAlmostEqual(req["beets_distance"], 0.08)

    def test_mark_done_request_stages(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="req-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        record = {"_db_request_id": req_id, "_db_source": "request"}
        bv_result = {"valid": True, "distance": 0.05, "scenario": "strong_match"}

        source.mark_done(record, bv_result, dest_path="/Incoming/A/B")

        req = db.get_request(req_id)
        self.assertEqual(req["status"], "staged")

    def test_mark_failed_updates_status_and_denylists(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="fail-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        record = {"_db_request_id": req_id, "_db_source": "request"}
        bv_result = {"valid": False, "distance": 0.35, "scenario": "high_distance"}

        source.mark_failed(record, bv_result, usernames={"bad_user1", "bad_user2"})

        req = db.get_request(req_id)
        self.assertEqual(req["status"], "rejected")
        self.assertEqual(req["validation_attempts"], 1)

        denied = db.get_denylisted_users(req_id)
        usernames = {d["username"] for d in denied}
        self.assertEqual(usernames, {"bad_user1", "bad_user2"})

    def test_get_denylisted_users(self):
        source, db = self._make_source()
        req_id = db.add_request(
            mb_release_id="deny-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        db.add_denylist(req_id, "user1", "bad quality")
        record = {"_db_request_id": req_id}

        denied = source.get_denylisted_users(record)
        self.assertEqual(denied, {"user1"})


class TestLidarrSource(unittest.TestCase):
    def test_get_wanted_raises(self):
        """LidarrSource.get_wanted() should not be called directly."""
        source = LidarrSource(MagicMock(), MagicMock())
        with self.assertRaises(NotImplementedError):
            source.get_wanted()

    def test_mark_done_is_noop(self):
        """mark_done doesn't crash for Lidarr source."""
        source = LidarrSource(MagicMock(), MagicMock())
        source.mark_done({}, {"valid": True})  # should not raise

    def test_mark_failed_is_noop(self):
        source = LidarrSource(MagicMock(), MagicMock())
        source.mark_failed({}, {"valid": False})  # should not raise


if __name__ == "__main__":
    unittest.main()
