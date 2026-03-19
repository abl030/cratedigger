"""Tests for scripts/pipeline_db.py — Pipeline DB module (in-memory SQLite).

Red/green TDD: these tests are written first, then pipeline_db.py is implemented.
"""

import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


class TestSchemaCreation(unittest.TestCase):
    def test_tables_exist(self):
        import pipeline_db
        db = pipeline_db.PipelineDB(":memory:")
        tables = db._execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = {t[0] for t in tables}
        self.assertIn("album_requests", table_names)
        self.assertIn("album_tracks", table_names)
        self.assertIn("download_log", table_names)
        self.assertIn("source_denylist", table_names)

    def test_indexes_exist(self):
        import pipeline_db
        db = pipeline_db.PipelineDB(":memory:")
        indexes = db._execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        index_names = {i[0] for i in indexes}
        self.assertIn("idx_requests_status", index_names)
        self.assertIn("idx_requests_mb_release", index_names)

    def test_idempotent_init(self):
        """Calling init_schema twice doesn't raise."""
        import pipeline_db
        db = pipeline_db.PipelineDB(":memory:")
        db.init_schema()  # second call should be safe


class TestAddAndGetRequest(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")

    def test_add_get_roundtrip(self):
        req_id = self.db.add_request(
            mb_release_id="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
            artist_name="Buke and Gase",
            album_title="Riposte",
            source="redownload",
            year=2014,
            country="US",
        )
        self.assertIsInstance(req_id, int)

        req = self.db.get_request(req_id)
        self.assertEqual(req["mb_release_id"], "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        self.assertEqual(req["artist_name"], "Buke and Gase")
        self.assertEqual(req["album_title"], "Riposte")
        self.assertEqual(req["source"], "redownload")
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["year"], 2014)
        self.assertEqual(req["country"], "US")

    def test_add_minimal_fields(self):
        """Only mb_release_id, artist_name, album_title, source are required."""
        req_id = self.db.add_request(
            mb_release_id="test-uuid",
            artist_name="Test",
            album_title="Test Album",
            source="request",
        )
        req = self.db.get_request(req_id)
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["year"])

    def test_duplicate_mb_release_id_raises(self):
        self.db.add_request(
            mb_release_id="dup-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        with self.assertRaises(Exception):  # IntegrityError
            self.db.add_request(
                mb_release_id="dup-uuid",
                artist_name="C",
                album_title="D",
                source="request",
            )

    def test_get_nonexistent_returns_none(self):
        self.assertIsNone(self.db.get_request(9999))

    def test_get_by_mb_release_id(self):
        self.db.add_request(
            mb_release_id="find-me-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        req = self.db.get_request_by_mb_release_id("find-me-uuid")
        self.assertIsNotNone(req)
        self.assertEqual(req["artist_name"], "A")

    def test_get_by_mb_release_id_not_found(self):
        self.assertIsNone(self.db.get_request_by_mb_release_id("nope"))

    def test_add_with_discogs_id(self):
        req_id = self.db.add_request(
            artist_name="Test",
            album_title="Test Album",
            source="request",
            discogs_release_id="12345",
        )
        req = self.db.get_request(req_id)
        self.assertEqual(req["discogs_release_id"], "12345")
        self.assertIsNone(req["mb_release_id"])

    def test_add_with_all_optional_fields(self):
        req_id = self.db.add_request(
            mb_release_id="full-uuid",
            mb_release_group_id="rg-uuid",
            mb_artist_id="artist-uuid",
            discogs_release_id="99999",
            artist_name="Full Artist",
            album_title="Full Album",
            year=2020,
            country="GB",
            format="CD",
            source="manual",
            source_path="/some/path",
            reasoning="Because reasons",
            lidarr_album_id=100,
            lidarr_artist_id=200,
        )
        req = self.db.get_request(req_id)
        self.assertEqual(req["mb_release_group_id"], "rg-uuid")
        self.assertEqual(req["mb_artist_id"], "artist-uuid")
        self.assertEqual(req["format"], "CD")
        self.assertEqual(req["source_path"], "/some/path")
        self.assertEqual(req["reasoning"], "Because reasons")
        self.assertEqual(req["lidarr_album_id"], 100)
        self.assertEqual(req["lidarr_artist_id"], 200)

    def test_delete_request(self):
        req_id = self.db.add_request(
            mb_release_id="del-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.delete_request(req_id)
        self.assertIsNone(self.db.get_request(req_id))


class TestUpdateStatus(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")
        self.req_id = self.db.add_request(
            mb_release_id="status-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )

    def test_status_transitions(self):
        statuses = [
            "searching", "downloading", "downloaded",
            "validating", "staged", "converting", "importing", "imported",
        ]
        for s in statuses:
            self.db.update_status(self.req_id, s)
            req = self.db.get_request(self.req_id)
            self.assertEqual(req["status"], s)

    def test_update_status_sets_updated_at(self):
        before = datetime.now(timezone.utc).isoformat()
        self.db.update_status(self.req_id, "searching")
        req = self.db.get_request(self.req_id)
        self.assertGreaterEqual(req["updated_at"], before[:19])

    def test_update_status_with_extra_fields(self):
        self.db.update_status(self.req_id, "imported",
                              beets_distance=0.05,
                              imported_path="/Beets/A/2020 - B")
        req = self.db.get_request(self.req_id)
        self.assertEqual(req["status"], "imported")
        self.assertAlmostEqual(req["beets_distance"], 0.05)
        self.assertEqual(req["imported_path"], "/Beets/A/2020 - B")


class TestGetWanted(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")

    def test_get_wanted_returns_only_wanted(self):
        id1 = self.db.add_request(mb_release_id="w1", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="w2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="w3", artist_name="E", album_title="F", source="request")
        self.db.update_status(id2, "importing")

        wanted = self.db.get_wanted()
        wanted_ids = [w["id"] for w in wanted]
        self.assertIn(id1, wanted_ids)
        self.assertNotIn(id2, wanted_ids)
        self.assertIn(id3, wanted_ids)

    def test_get_wanted_respects_retry_backoff(self):
        id1 = self.db.add_request(mb_release_id="r1", artist_name="A", album_title="B", source="request")
        # Set next_retry_after to the future
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = ? WHERE id = ?",
            (future, id1),
        )
        self.db.conn.commit()

        wanted = self.db.get_wanted()
        self.assertEqual(len(wanted), 0)

    def test_get_wanted_ordered_by_created_at(self):
        id1 = self.db.add_request(mb_release_id="o1", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="o2", artist_name="C", album_title="D", source="request")
        wanted = self.db.get_wanted()
        self.assertEqual(wanted[0]["id"], id1)
        self.assertEqual(wanted[1]["id"], id2)

    def test_get_wanted_with_limit(self):
        for i in range(5):
            self.db.add_request(mb_release_id=f"lim-{i}", artist_name="A", album_title=f"B{i}", source="request")
        wanted = self.db.get_wanted(limit=3)
        self.assertEqual(len(wanted), 3)


class TestGetByStatus(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")

    def test_get_by_status(self):
        id1 = self.db.add_request(mb_release_id="s1", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="s2", artist_name="C", album_title="D", source="request")
        self.db.update_status(id1, "staged")

        staged = self.db.get_by_status("staged")
        self.assertEqual(len(staged), 1)
        self.assertEqual(staged[0]["id"], id1)

    def test_count_by_status(self):
        self.db.add_request(mb_release_id="c1", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="c2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="c3", artist_name="E", album_title="F", source="redownload")
        self.db.update_status(id3, "staged")

        counts = self.db.count_by_status()
        self.assertEqual(counts["wanted"], 2)
        self.assertEqual(counts["staged"], 1)


class TestTrackManagement(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")
        self.req_id = self.db.add_request(
            mb_release_id="track-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def test_set_get_tracks_roundtrip(self):
        tracks = [
            {"disc_number": 1, "track_number": 1, "title": "Intro", "length_seconds": 120},
            {"disc_number": 1, "track_number": 2, "title": "Song", "length_seconds": 240},
            {"disc_number": 1, "track_number": 3, "title": "Outro", "length_seconds": 180},
        ]
        self.db.set_tracks(self.req_id, tracks)

        result = self.db.get_tracks(self.req_id)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["title"], "Intro")
        self.assertEqual(result[1]["disc_number"], 1)
        self.assertEqual(result[2]["length_seconds"], 180)

    def test_multi_disc_tracks(self):
        tracks = [
            {"disc_number": 1, "track_number": 1, "title": "D1T1", "length_seconds": 200},
            {"disc_number": 1, "track_number": 2, "title": "D1T2", "length_seconds": 200},
            {"disc_number": 2, "track_number": 1, "title": "D2T1", "length_seconds": 200},
            {"disc_number": 2, "track_number": 2, "title": "D2T2", "length_seconds": 200},
        ]
        self.db.set_tracks(self.req_id, tracks)
        result = self.db.get_tracks(self.req_id)
        self.assertEqual(len(result), 4)
        # Should be ordered by disc, then track
        self.assertEqual(result[2]["title"], "D2T1")

    def test_set_tracks_replaces_existing(self):
        self.db.set_tracks(self.req_id, [
            {"disc_number": 1, "track_number": 1, "title": "Old", "length_seconds": 100},
        ])
        self.db.set_tracks(self.req_id, [
            {"disc_number": 1, "track_number": 1, "title": "New", "length_seconds": 200},
        ])
        result = self.db.get_tracks(self.req_id)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "New")


class TestDownloadLog(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")
        self.req_id = self.db.add_request(
            mb_release_id="dl-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def test_log_and_get_download(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user123",
            filetype="flac",
            download_path="/tmp/dl/files",
            beets_distance=0.08,
            beets_scenario="single-disc",
            outcome="staged",
            staged_path="/Incoming/A/B",
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["soulseek_username"], "user123")
        self.assertAlmostEqual(history[0]["beets_distance"], 0.08)
        self.assertEqual(history[0]["outcome"], "staged")
        self.assertEqual(history[0]["staged_path"], "/Incoming/A/B")

    def test_multiple_downloads(self):
        self.db.log_download(self.req_id, "user1", "flac", "/tmp/1", outcome="rejected")
        self.db.log_download(self.req_id, "user2", "flac", "/tmp/2", outcome="staged",
                             beets_distance=0.05, staged_path="/Incoming/A/B")
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 2)
        # Most recent first
        self.assertEqual(history[0]["soulseek_username"], "user2")


class TestDenylist(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")
        self.req_id = self.db.add_request(
            mb_release_id="deny-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def test_add_and_get_denylist(self):
        self.db.add_denylist(self.req_id, "bad_user", "low bitrate")
        denied = self.db.get_denylisted_users(self.req_id)
        self.assertEqual(len(denied), 1)
        self.assertEqual(denied[0]["username"], "bad_user")
        self.assertEqual(denied[0]["reason"], "low bitrate")

    def test_multiple_denied_users(self):
        self.db.add_denylist(self.req_id, "user1", "bad quality")
        self.db.add_denylist(self.req_id, "user2", "incomplete")
        denied = self.db.get_denylisted_users(self.req_id)
        usernames = {d["username"] for d in denied}
        self.assertEqual(usernames, {"user1", "user2"})

    def test_duplicate_denylist_ignored(self):
        self.db.add_denylist(self.req_id, "user1", "reason1")
        self.db.add_denylist(self.req_id, "user1", "reason2")  # should not raise
        denied = self.db.get_denylisted_users(self.req_id)
        # First insert wins (OR IGNORE)
        self.assertEqual(len(denied), 1)


class TestRetryLogic(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")
        self.req_id = self.db.add_request(
            mb_release_id="retry-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def test_record_attempt_increments_counters(self):
        self.db.record_attempt(self.req_id, "search")
        req = self.db.get_request(self.req_id)
        self.assertEqual(req["search_attempts"], 1)

        self.db.record_attempt(self.req_id, "search")
        req = self.db.get_request(self.req_id)
        self.assertEqual(req["search_attempts"], 2)

    def test_record_attempt_sets_backoff(self):
        self.db.record_attempt(self.req_id, "download")
        req = self.db.get_request(self.req_id)
        self.assertEqual(req["download_attempts"], 1)
        self.assertIsNotNone(req["last_attempt_at"])
        self.assertIsNotNone(req["next_retry_after"])
        # Backoff should be in the future
        next_retry = datetime.fromisoformat(req["next_retry_after"])
        now = datetime.now(timezone.utc)
        self.assertGreater(next_retry, now)

    def test_exponential_backoff(self):
        """Each attempt increases the backoff exponentially."""
        self.db.record_attempt(self.req_id, "search")
        req1 = self.db.get_request(self.req_id)
        retry1 = datetime.fromisoformat(req1["next_retry_after"])

        self.db.record_attempt(self.req_id, "search")
        req2 = self.db.get_request(self.req_id)
        retry2 = datetime.fromisoformat(req2["next_retry_after"])

        # Second backoff should be further out than first
        now = datetime.now(timezone.utc)
        delta1 = (retry1 - now).total_seconds()
        delta2 = (retry2 - now).total_seconds()
        self.assertGreater(delta2, delta1)

    def test_record_validation_attempt(self):
        self.db.record_attempt(self.req_id, "validation")
        req = self.db.get_request(self.req_id)
        self.assertEqual(req["validation_attempts"], 1)


class TestImportFromJsonl(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")

    def test_import_pending(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            json.dump({
                "artist": "Test Artist",
                "album": "Test Album",
                "mb_release_id": "import-uuid-1",
                "source_path": "/AI/Test/Album",
                "reasoning": "Bad rip",
                "timestamp": "2026-03-16T08:50:12.848632",
            }, f)
            f.write("\n")
            f.flush()
            self.addCleanup(os.unlink, f.name)

            count = self.db.import_from_jsonl(f.name, source="redownload", status="wanted")

        self.assertEqual(count, 1)
        req = self.db.get_request_by_mb_release_id("import-uuid-1")
        self.assertIsNotNone(req)
        self.assertEqual(req["artist_name"], "Test Artist")
        self.assertEqual(req["album_title"], "Test Album")
        self.assertEqual(req["source"], "redownload")
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["reasoning"], "Bad rip")
        self.assertEqual(req["source_path"], "/AI/Test/Album")

    def test_import_processed(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            json.dump({
                "status": "processed",
                "artist": "Processed Artist",
                "album": "Processed Album",
                "mb_release_id": "proc-uuid",
                "release_group_id": "rg-uuid",
                "artist_mb_id": "artist-uuid",
                "lidarr_artist_id": 100,
                "lidarr_album_id": 200,
                "timestamp": "2026-03-09T12:35:12",
            }, f)
            f.write("\n")
            f.flush()
            self.addCleanup(os.unlink, f.name)

            count = self.db.import_from_jsonl(f.name, source="redownload", status="searching")

        self.assertEqual(count, 1)
        req = self.db.get_request_by_mb_release_id("proc-uuid")
        self.assertEqual(req["mb_release_group_id"], "rg-uuid")
        self.assertEqual(req["mb_artist_id"], "artist-uuid")
        self.assertEqual(req["lidarr_album_id"], 200)
        self.assertEqual(req["lidarr_artist_id"], 100)

    def test_import_validated(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            json.dump({
                "timestamp": "2026-03-09T10:55:36",
                "artist": "Beirut",
                "album": "Gulag Orkestar",
                "mb_release_id": "val-uuid",
                "lidarr_album_id": 108842,
                "status": "rejected",
                "distance": 0.17,
                "dest_path": None,
                "error": None,
            }, f)
            f.write("\n")
            f.flush()
            self.addCleanup(os.unlink, f.name)

            count = self.db.import_from_jsonl(f.name, source="redownload", status="wanted")

        self.assertEqual(count, 1)

    def test_import_skips_duplicates(self):
        """Importing same mb_release_id twice skips the duplicate."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for i in range(2):
                json.dump({
                    "artist": "A",
                    "album": "B",
                    "mb_release_id": "dup-import-uuid",
                    "source_path": "/path",
                    "reasoning": "reason",
                    "timestamp": "2026-01-01T00:00:00",
                }, f)
                f.write("\n")
            f.flush()
            self.addCleanup(os.unlink, f.name)

            count = self.db.import_from_jsonl(f.name, source="redownload", status="wanted")

        self.assertEqual(count, 1)  # second line skipped


class TestTwoTrackSourcePreservation(unittest.TestCase):
    """Verify that source type (request vs redownload) is preserved through lifecycle."""

    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")

    def test_request_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="req-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "staged")
        req = self.db.get_request(req_id)
        self.assertEqual(req["source"], "request")

    def test_redownload_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="rd-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        self.db.update_status(req_id, "imported")
        req = self.db.get_request(req_id)
        self.assertEqual(req["source"], "redownload")


class TestResetToWanted(unittest.TestCase):
    def setUp(self):
        import pipeline_db
        self.db = pipeline_db.PipelineDB(":memory:")

    def test_reset_failed_to_wanted(self):
        req_id = self.db.add_request(
            mb_release_id="reset-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "rejected")
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["next_retry_after"])
        self.assertEqual(req["search_attempts"], 0)
        self.assertEqual(req["download_attempts"], 0)
        self.assertEqual(req["validation_attempts"], 0)


class TestFileBasedDB(unittest.TestCase):
    """Test with a real file-based SQLite DB (WAL mode)."""

    def test_wal_mode(self):
        import pipeline_db
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        self.addCleanup(os.unlink, db_path)

        db = pipeline_db.PipelineDB(db_path)
        mode = db._execute("PRAGMA journal_mode").fetchone()[0]
        self.assertEqual(mode, "wal")

    def test_persistence(self):
        import pipeline_db
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        self.addCleanup(os.unlink, db_path)

        db1 = pipeline_db.PipelineDB(db_path)
        db1.add_request(mb_release_id="persist-uuid", artist_name="A", album_title="B", source="request")
        db1.close()

        db2 = pipeline_db.PipelineDB(db_path)
        req = db2.get_request_by_mb_release_id("persist-uuid")
        self.assertIsNotNone(req)
        db2.close()


if __name__ == "__main__":
    unittest.main()
