"""Tests for lib/pipeline_db.py — Pipeline DB module (PostgreSQL).

Requires a PostgreSQL server. Set TEST_DB_DSN env var to run, e.g.:
    TEST_DB_DSN=postgresql://soularr@localhost/soularr_test python3 -m unittest tests.test_pipeline_db -v

Tests create/drop tables in the target database — use a dedicated test DB.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

# Bootstrap ephemeral PostgreSQL if available
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

TEST_DSN = os.environ.get("TEST_DB_DSN")

def requires_postgres(cls):
    """Skip test class if TEST_DB_DSN is not set."""
    if not TEST_DSN:
        return unittest.skip("TEST_DB_DSN not set — skipping PostgreSQL tests")(cls)
    return cls


def make_db():
    """Create a PipelineDB connected to the test database, with clean tables."""
    import pipeline_db
    db = pipeline_db.PipelineDB(TEST_DSN, run_migrations=True)
    # Truncate all tables for a clean slate
    for table in ["source_denylist", "download_log", "album_tracks", "album_requests"]:
        db._execute(f"TRUNCATE {table} CASCADE")
    db.conn.commit()
    return db


@requires_postgres
class TestSchemaCreation(unittest.TestCase):
    def test_tables_exist(self):
        db = make_db()
        cur = db._execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        table_names = {r["table_name"] for r in cur.fetchall()}
        self.assertIn("album_requests", table_names)
        self.assertIn("album_tracks", table_names)
        self.assertIn("download_log", table_names)
        self.assertIn("source_denylist", table_names)
        db.close()

    def test_idempotent_init(self):
        db = make_db()
        db.init_schema()  # second call should be safe
        db.close()


@requires_postgres
class TestAddAndGetRequest(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

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
        assert req is not None
        self.assertEqual(req.mb_release_id, "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        self.assertEqual(req.artist_name, "Buke and Gase")
        self.assertEqual(req.album_title, "Riposte")
        self.assertEqual(req.source, "redownload")
        self.assertEqual(req.status, "wanted")
        self.assertEqual(req.year, 2014)
        self.assertEqual(req.country, "US")

    def test_add_minimal_fields(self):
        req_id = self.db.add_request(
            mb_release_id="test-uuid",
            artist_name="Test",
            album_title="Test Album",
            source="request",
        )
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req.status, "wanted")
        self.assertIsNone(req.year)

    def test_duplicate_mb_release_id_raises(self):
        self.db.add_request(
            mb_release_id="dup-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        with self.assertRaises(Exception):
            self.db.add_request(
                mb_release_id="dup-uuid",
                artist_name="C",
                album_title="D",
                source="request",
            )
        self.db.conn.rollback()

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
        assert req is not None
        self.assertEqual(req.artist_name, "A")

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
        assert req is not None
        self.assertEqual(req.discogs_release_id, "12345")
        self.assertIsNone(req.mb_release_id)

    def test_delete_request(self):
        req_id = self.db.add_request(
            mb_release_id="del-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.delete_request(req_id)
        self.assertIsNone(self.db.get_request(req_id))


@requires_postgres
class TestUpdateStatus(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="status-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )

    def tearDown(self):
        self.db.close()

    def test_status_transitions(self):
        for s in ["wanted", "imported", "manual"]:
            self.db.update_status(self.req_id, s)
            req = self.db.get_request(self.req_id)
            assert req is not None
            self.assertEqual(req.status, s)

    def test_update_status_with_extra_fields(self):
        self.db.update_status(self.req_id, "imported",
                              beets_distance=0.05,
                              imported_path="/Beets/A/2020 - B")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req.status, "imported")
        assert req.beets_distance is not None
        self.assertAlmostEqual(req.beets_distance, 0.05)
        self.assertEqual(req.imported_path, "/Beets/A/2020 - B")


@requires_postgres
class TestGetWanted(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_wanted_returns_only_wanted(self):
        id1 = self.db.add_request(mb_release_id="w1", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="w2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="w3", artist_name="E", album_title="F", source="request")
        self.db.update_status(id2, "imported")

        wanted = self.db.get_wanted()
        wanted_ids = [w["id"] for w in wanted]
        self.assertIn(id1, wanted_ids)
        self.assertNotIn(id2, wanted_ids)
        self.assertIn(id3, wanted_ids)

    def test_get_wanted_respects_retry_backoff(self):
        id1 = self.db.add_request(mb_release_id="r1", artist_name="A", album_title="B", source="request")
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (future, id1),
        )
        self.db.conn.commit()

        wanted = self.db.get_wanted()
        self.assertEqual(len(wanted), 0)

    def test_get_wanted_with_limit(self):
        for i in range(5):
            self.db.add_request(mb_release_id=f"lim-{i}", artist_name="A", album_title=f"B{i}", source="request")
        wanted = self.db.get_wanted(limit=3)
        self.assertEqual(len(wanted), 3)


@requires_postgres
class TestGetByStatus(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_by_status(self):
        id1 = self.db.add_request(mb_release_id="s1", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="s2", artist_name="C", album_title="D", source="request")
        self.db.update_status(id1, "imported")

        imported = self.db.get_by_status("imported")
        self.assertEqual(len(imported), 1)
        self.assertEqual(imported[0]["id"], id1)

    def test_count_by_status(self):
        self.db.add_request(mb_release_id="c1", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="c2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="c3", artist_name="E", album_title="F", source="redownload")
        self.db.update_status(id3, "imported")

        counts = self.db.count_by_status()
        self.assertEqual(counts["wanted"], 2)
        self.assertEqual(counts["imported"], 1)


@requires_postgres
class TestTrackManagement(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="track-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

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


@requires_postgres
class TestDownloadLog(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="dl-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_and_get_download(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user123",
            filetype="flac",
            download_path="/tmp/dl/files",
            beets_distance=0.08,
            beets_scenario="single-disc",
            outcome="success",
            staged_path="/Incoming/A/B",
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["soulseek_username"], "user123")
        self.assertAlmostEqual(history[0]["beets_distance"], 0.08)
        self.assertEqual(history[0]["outcome"], "success")

    def test_multiple_downloads(self):
        self.db.log_download(self.req_id, "user1", "flac", "/tmp/1", outcome="rejected")
        self.db.log_download(self.req_id, "user2", "flac", "/tmp/2", outcome="success",
                             beets_distance=0.05, staged_path="/Incoming/A/B")
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["soulseek_username"], "user2")


@requires_postgres
class TestDenylist(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="deny-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

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
        self.db.add_denylist(self.req_id, "user1", "reason2")
        denied = self.db.get_denylisted_users(self.req_id)
        self.assertEqual(len(denied), 1)


@requires_postgres
class TestRetryLogic(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="retry-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_record_attempt_increments_counters(self):
        self.db.record_attempt(self.req_id, "search")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req.search_attempts, 1)

        self.db.record_attempt(self.req_id, "search")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req.search_attempts, 2)

    def test_record_attempt_sets_backoff(self):
        self.db.record_attempt(self.req_id, "download")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req.download_attempts, 1)
        self.assertIsNotNone(req.last_attempt_at)
        assert req.next_retry_after is not None
        self.assertGreater(req.next_retry_after, datetime.now(timezone.utc))

    def test_exponential_backoff(self):
        self.db.record_attempt(self.req_id, "search")
        req1 = self.db.get_request(self.req_id)
        assert req1 is not None
        assert req1.next_retry_after is not None
        retry1 = req1.next_retry_after

        self.db.record_attempt(self.req_id, "search")
        req2 = self.db.get_request(self.req_id)
        assert req2 is not None
        assert req2.next_retry_after is not None
        retry2 = req2.next_retry_after

        now = datetime.now(timezone.utc)
        delta1 = (retry1 - now).total_seconds()
        delta2 = (retry2 - now).total_seconds()
        self.assertGreater(delta2, delta1)


@requires_postgres
class TestSourcePreservation(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_request_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="req-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req.source, "request")

    def test_redownload_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="rd-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        self.db.update_status(req_id, "imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req.source, "redownload")


@requires_postgres
class TestResetToWanted(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_reset_to_wanted(self):
        req_id = self.db.add_request(
            mb_release_id="reset-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req.status, "wanted")
        self.assertIsNone(req.next_retry_after)
        self.assertEqual(req.search_attempts, 0)
        self.assertEqual(req.download_attempts, 0)
        self.assertEqual(req.validation_attempts, 0)


@requires_postgres
class TestSpectralColumns(unittest.TestCase):
    """Test spectral quality columns on download_log and album_requests."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="spectral-uuid",
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_download_with_spectral_fields(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            filetype="mp3",
            outcome="success",
            spectral_grade="suspect",
            spectral_bitrate=128,
            slskd_filetype="mp3",
            slskd_bitrate=320000,
            actual_filetype="mp3",
            actual_min_bitrate=320000,
            existing_min_bitrate=92,
            existing_spectral_bitrate=64,
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        h = history[0]
        self.assertEqual(h["spectral_grade"], "suspect")
        self.assertEqual(h["spectral_bitrate"], 128)
        self.assertEqual(h["slskd_filetype"], "mp3")
        self.assertEqual(h["slskd_bitrate"], 320000)
        self.assertEqual(h["actual_filetype"], "mp3")
        self.assertEqual(h["actual_min_bitrate"], 320000)
        self.assertEqual(h["existing_min_bitrate"], 92)
        self.assertEqual(h["existing_spectral_bitrate"], 64)

    def test_spectral_fields_null_by_default(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            outcome="success",
        )
        history = self.db.get_download_history(self.req_id)
        h = history[0]
        self.assertIsNone(h.get("spectral_grade"))
        self.assertIsNone(h.get("spectral_bitrate"))
        self.assertIsNone(h.get("slskd_filetype"))

    def test_album_request_spectral_columns(self):
        self.db.update_status(self.req_id, "imported",
                              spectral_bitrate=128,
                              spectral_grade="suspect")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req.spectral_bitrate, 128)
        self.assertEqual(req.spectral_grade, "suspect")

    def test_on_disk_spectral_columns(self):
        """on_disk_spectral_grade/bitrate describe files currently in beets."""
        self.db.update_status(self.req_id, "imported",
                              on_disk_spectral_grade="suspect",
                              on_disk_spectral_bitrate=160)
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req.on_disk_spectral_grade, "suspect")
        self.assertEqual(req.on_disk_spectral_bitrate, 160)

    def test_on_disk_spectral_null_by_default(self):
        """on_disk_spectral columns are NULL for pre-existing albums."""
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req.on_disk_spectral_grade)
        self.assertIsNone(req.on_disk_spectral_bitrate)


@requires_postgres
class TestBatchHistory(unittest.TestCase):
    """Test get_download_history_batch — batch download history lookup."""

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="batch-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="batch-2", artist_name="C", album_title="D", source="request")
        self.req3 = self.db.add_request(
            mb_release_id="batch-3", artist_name="E", album_title="F", source="request")
        # Add history for req1 and req2, but not req3
        self.db.log_download(self.req1, soulseek_username="user1", outcome="success")
        self.db.log_download(self.req1, soulseek_username="user2", outcome="rejected")
        self.db.log_download(self.req2, soulseek_username="user3", outcome="success")

    def tearDown(self):
        self.db.close()

    def test_returns_grouped_by_request_id(self):
        result = self.db.get_download_history_batch([self.req1, self.req2, self.req3])
        self.assertIn(self.req1, result)
        self.assertIn(self.req2, result)
        self.assertNotIn(self.req3, result)  # no history
        self.assertEqual(len(result[self.req1]), 2)
        self.assertEqual(len(result[self.req2]), 1)

    def test_empty_list(self):
        result = self.db.get_download_history_batch([])
        self.assertEqual(result, {})

    def test_order_is_desc_by_id(self):
        result = self.db.get_download_history_batch([self.req1])
        history = result[self.req1]
        # Most recent first (rejected was logged after success)
        self.assertEqual(history[0]["outcome"], "rejected")
        self.assertEqual(history[1]["outcome"], "success")


@requires_postgres
class TestTrackCounts(unittest.TestCase):
    """Test get_track_counts — batch track count lookup."""

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="tc-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="tc-2", artist_name="C", album_title="D", source="request")
        self.req3 = self.db.add_request(
            mb_release_id="tc-3", artist_name="E", album_title="F", source="request")
        self.db.set_tracks(self.req1, [
            {"disc_number": 1, "track_number": 1, "title": "T1", "length_seconds": 100},
            {"disc_number": 1, "track_number": 2, "title": "T2", "length_seconds": 200},
        ])
        self.db.set_tracks(self.req2, [
            {"disc_number": 1, "track_number": 1, "title": "T1", "length_seconds": 100},
        ])
        # req3 has no tracks

    def tearDown(self):
        self.db.close()

    def test_returns_counts(self):
        result = self.db.get_track_counts([self.req1, self.req2, self.req3])
        self.assertEqual(result[self.req1], 2)
        self.assertEqual(result[self.req2], 1)
        self.assertNotIn(self.req3, result)  # no tracks

    def test_empty_list(self):
        result = self.db.get_track_counts([])
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
