"""Tests for scripts/pipeline_cli.py — Pipeline CLI commands."""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import pipeline_db
import pipeline_cli


SAMPLE_MB_RELEASE = {
    "id": "44438bf9-26d9-4460-9b4f-1a1b015e37a1",
    "title": "Riposte",
    "date": "2014-05-06",
    "country": "US",
    "release-group": {"id": "rg-uuid"},
    "artist-credit": [{
        "name": "Buke and Gase",
        "artist": {"id": "artist-uuid", "name": "Buke and Gase"},
    }],
    "media": [{
        "position": 1,
        "tracks": [
            {"position": 1, "title": "Houdini Crush", "length": 200000},
            {"position": 2, "title": "Hiccup", "length": 180000},
            {"position": 3, "title": "Metazoa", "length": 220000},
        ],
    }],
}


class CLITestBase(unittest.TestCase):
    def setUp(self):
        self.db = pipeline_db.PipelineDB(":memory:")


class TestCmdAdd(CLITestBase):
    @patch("pipeline_cli.fetch_mb_release")
    def test_add_with_mbid(self, mock_fetch):
        mock_fetch.return_value = SAMPLE_MB_RELEASE
        args = MagicMock(mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request")
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id("44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        self.assertIsNotNone(req)
        self.assertEqual(req["artist_name"], "Buke and Gase")
        self.assertEqual(req["album_title"], "Riposte")
        self.assertEqual(req["year"], 2014)
        self.assertEqual(req["source"], "request")

        tracks = self.db.get_tracks(req["id"])
        self.assertEqual(len(tracks), 3)

    @patch("pipeline_cli.fetch_mb_release")
    def test_add_duplicate_skipped(self, mock_fetch):
        self.db.add_request(
            mb_release_id="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
            artist_name="A", album_title="B", source="request",
        )
        args = MagicMock(mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request")
        pipeline_cli.cmd_add(self.db, args)
        mock_fetch.assert_not_called()


class TestCmdList(CLITestBase):
    def test_list_by_status(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="b", artist_name="C", album_title="D", source="request")
        self.db.update_status(id2, "staged")

        args = MagicMock(filter_status="wanted")
        pipeline_cli.cmd_list(self.db, args)  # should not raise

    def test_list_all(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        args = MagicMock(filter_status=None)
        pipeline_cli.cmd_list(self.db, args)  # should not raise


class TestCmdStatus(CLITestBase):
    def test_status_counts(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="b", artist_name="C", album_title="D", source="request")
        args = MagicMock()
        pipeline_cli.cmd_status(self.db, args)  # should not raise


class TestCmdRetry(CLITestBase):
    def test_retry_resets_to_wanted(self):
        req_id = self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        self.db.update_status(req_id, "rejected")
        args = MagicMock(id=req_id)
        pipeline_cli.cmd_retry(self.db, args)
        req = self.db.get_request(req_id)
        self.assertEqual(req["status"], "wanted")


class TestCmdCancel(CLITestBase):
    def test_cancel_sets_skipped(self):
        req_id = self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        args = MagicMock(id=req_id)
        pipeline_cli.cmd_cancel(self.db, args)
        req = self.db.get_request(req_id)
        self.assertEqual(req["status"], "skipped")


class TestCmdMigrate(CLITestBase):
    def test_migrate_pending_jsonl(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            json.dump({
                "artist": "Test Artist",
                "album": "Test Album",
                "mb_release_id": "mig-uuid",
                "source_path": "/AI/path",
                "reasoning": "Bad rip",
                "timestamp": "2026-01-01T00:00:00",
            }, f)
            f.write("\n")
            f.flush()
            self.addCleanup(os.unlink, f.name)

            args = MagicMock(dry_run=False)
            with patch.object(pipeline_cli, "REDOWNLOAD_DIR", os.path.dirname(f.name)):
                # Monkey-patch the paths
                orig_fn = self.db.import_from_jsonl
                count = self.db.import_from_jsonl(f.name, source="redownload", status="wanted")

        self.assertEqual(count, 1)
        req = self.db.get_request_by_mb_release_id("mig-uuid")
        self.assertIsNotNone(req)


class TestTracksFromMbRelease(unittest.TestCase):
    def test_extract_tracks(self):
        tracks = pipeline_cli.tracks_from_mb_release(SAMPLE_MB_RELEASE)
        self.assertEqual(len(tracks), 3)
        self.assertEqual(tracks[0]["title"], "Houdini Crush")
        self.assertEqual(tracks[0]["disc_number"], 1)
        self.assertAlmostEqual(tracks[0]["length_seconds"], 200.0)


if __name__ == "__main__":
    unittest.main()
