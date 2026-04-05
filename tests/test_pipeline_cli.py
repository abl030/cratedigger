"""Tests for scripts/pipeline_cli.py — Pipeline CLI commands."""

import io
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch, MagicMock

# Bootstrap ephemeral PostgreSQL if available
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
_scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
sys.path.insert(0, os.path.abspath(_scripts_dir))
import pipeline_cli
from tests.helpers import make_request_row

TEST_DSN = os.environ.get("TEST_DB_DSN")

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


def make_db():
    from pipeline_db import PipelineDB
    db = PipelineDB(TEST_DSN, run_migrations=True)
    for table in ["source_denylist", "download_log", "album_tracks", "album_requests"]:
        db._execute(f"TRUNCATE {table} CASCADE")
    db.conn.commit()
    return db


@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestCmdAdd(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    @patch("pipeline_cli.fetch_mb_release")
    def test_add_with_mbid(self, mock_fetch):
        mock_fetch.return_value = SAMPLE_MB_RELEASE
        args = MagicMock(mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request")
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id("44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        assert req is not None
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


@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestCmdList(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_list_by_status(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="b", artist_name="C", album_title="D", source="request")
        self.db.update_status(id2, "imported")

        args = MagicMock(filter_status="wanted")
        pipeline_cli.cmd_list(self.db, args)

    def test_list_all(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        args = MagicMock(filter_status=None)
        pipeline_cli.cmd_list(self.db, args)


@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestCmdRetry(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_retry_resets_to_wanted(self):
        req_id = self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        self.db.update_status(req_id, "imported")
        args = MagicMock(id=req_id)
        pipeline_cli.cmd_retry(self.db, args)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")


@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestCmdCancel(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_cancel_sets_manual(self):
        req_id = self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        args = MagicMock(id=req_id)
        pipeline_cli.cmd_cancel(self.db, args)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "manual")


class TestTracksFromMbRelease(unittest.TestCase):
    def test_extract_tracks(self):
        tracks = pipeline_cli.tracks_from_mb_release(SAMPLE_MB_RELEASE)
        self.assertEqual(len(tracks), 3)
        self.assertEqual(tracks[0]["title"], "Houdini Crush")
        self.assertEqual(tracks[0]["disc_number"], 1)
        self.assertAlmostEqual(tracks[0]["length_seconds"], 200.0)


class TestCmdManualImport(unittest.TestCase):
    @patch("builtins.print")
    def test_failed_manual_import_logs_error_message(self, _mock_print):
        from lib.import_service import ImportOutcome
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        )

        mock_outcome = ImportOutcome(
            success=False, exit_code=5,
            message="import_one.py exited with code 5",
            import_result_json='{"decision":"downgrade","exit_code":5}',
        )
        with patch("lib.import_service.run_import", return_value=mock_outcome):
            args = MagicMock(id=123, path="/tmp/Album")
            pipeline_cli.cmd_manual_import(db, args)

        db.log_download.assert_called_once()
        kwargs = db.log_download.call_args.kwargs
        self.assertEqual(kwargs["outcome"], "failed")
        self.assertEqual(kwargs["staged_path"], "/tmp/Album")


class TestCmdQuery(unittest.TestCase):
    def test_query_renders_table_output_in_read_only_mode(self):
        db = MagicMock()
        query_cur = MagicMock()
        query_cur.description = [("id",), ("artist_name",), ("details",)]
        query_cur.fetchall.return_value = [
            {"id": 7, "artist_name": "Buke and Gase", "details": {"tracks": 3}},
        ]
        db._execute.side_effect = [MagicMock(), query_cur, MagicMock()]

        args = MagicMock(sql="SELECT id, artist_name, details FROM album_requests", json=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(db, args)

        self.assertIsNone(rc)
        self.assertEqual(
            db._execute.call_args_list[0][0][0],
            "SET SESSION default_transaction_read_only = on",
        )
        self.assertEqual(
            db._execute.call_args_list[1][0][0],
            "SELECT id, artist_name, details FROM album_requests",
        )
        self.assertEqual(
            db._execute.call_args_list[2][0][0],
            "SET SESSION default_transaction_read_only = off",
        )
        output = stdout.getvalue()
        self.assertIn("id | artist_name", output)
        self.assertIn('{"tracks": 3}', output)
        self.assertIn("(1 row)", output)

    def test_query_reads_sql_from_stdin_when_dash_is_passed(self):
        db = MagicMock()
        query_cur = MagicMock()
        query_cur.description = [("value",)]
        query_cur.fetchall.return_value = [{"value": 1}]
        db._execute.side_effect = [MagicMock(), query_cur, MagicMock()]

        args = MagicMock(sql="-", json=False)
        stdout = io.StringIO()
        with patch("sys.stdin", io.StringIO("SELECT 1 AS value")), redirect_stdout(stdout):
            pipeline_cli.cmd_query(db, args)

        self.assertEqual(
            db._execute.call_args_list[1][0][0],
            "SELECT 1 AS value",
        )
        self.assertIn("value", stdout.getvalue())

    def test_query_can_emit_json(self):
        db = MagicMock()
        query_cur = MagicMock()
        query_cur.description = [("id",), ("status",)]
        query_cur.fetchall.return_value = [{"id": 3, "status": "wanted"}]
        db._execute.side_effect = [MagicMock(), query_cur, MagicMock()]

        args = MagicMock(sql="SELECT id, status FROM album_requests", json=True)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            pipeline_cli.cmd_query(db, args)

        self.assertEqual(
            stdout.getvalue().strip(),
            '[\n  {\n    "id": 3,\n    "status": "wanted"\n  }\n]',
        )

    def test_query_reports_sql_errors_and_resets_read_only(self):
        import psycopg2

        db = MagicMock()
        db._execute.side_effect = [
            MagicMock(),
            psycopg2.ProgrammingError('syntax error at or near "BOOM"'),
            MagicMock(),
        ]

        args = MagicMock(sql="BOOM", json=False)
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(db, args)

        self.assertEqual(rc, 1)
        self.assertIn("syntax error", stderr.getvalue())
        self.assertEqual(
            db._execute.call_args_list[2][0][0],
            "SET SESSION default_transaction_read_only = off",
        )


@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestCmdQueryIntegration(unittest.TestCase):
    """Integration test: read-only session rejects writes against real DB."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_query_rejects_writes(self):
        args = MagicMock(sql="DELETE FROM album_requests", json=False)
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertEqual(rc, 1)
        self.assertIn("read-only", stderr.getvalue().lower())

    def test_query_allows_reads(self):
        args = MagicMock(sql="SELECT count(*) AS n FROM album_requests", json=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertIsNone(rc)
        self.assertIn("n", stdout.getvalue())


@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestCmdStatusShowsDownloading(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_status_shows_downloading_count(self):
        """pipeline-cli status includes downloading in the count display."""
        import json
        id1 = self.db.add_request(mb_release_id="dl-1", artist_name="A",
                                  album_title="B", source="request")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "now", "files": []})
        self.db.set_downloading(id1, state_json)

        counts = self.db.count_by_status()
        self.assertIn("downloading", counts)
        self.assertEqual(counts["downloading"], 1)

    def test_show_displays_active_download_state(self):
        """pipeline-cli show renders active_download_state for downloading albums."""
        import json
        id1 = self.db.add_request(mb_release_id="show-dl", artist_name="A",
                                  album_title="B", source="request")
        state = {"filetype": "flac", "enqueued_at": "2026-04-03T12:00:00+00:00",
                 "files": [{"username": "user1", "filename": "f.flac",
                            "file_dir": "d", "size": 1000}]}
        self.db.set_downloading(id1, json.dumps(state))

        req = self.db.get_request(id1)
        assert req is not None
        ads = req.get("active_download_state")
        assert ads is not None
        self.assertEqual(ads["filetype"], "flac")
        self.assertEqual(len(ads["files"]), 1)


class TestCmdSetIntent(unittest.TestCase):
    """Tests for cmd_set_intent — quality intent CLI command."""

    @patch("builtins.print")
    def test_set_intent_on_wanted(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        )
        args = MagicMock(id=1, intent="flac_only")
        pipeline_cli.cmd_set_intent(db, args)
        db._execute.assert_called_once()
        call_args = db._execute.call_args[0]
        self.assertIn("quality_override", call_args[0])
        self.assertEqual(call_args[1][0], "flac")

    @patch("builtins.print")
    def test_set_intent_on_imported_requeues(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=2, status="imported", artist_name="A", album_title="B",
            min_bitrate=245,
        )
        args = MagicMock(id=2, intent="upgrade")
        pipeline_cli.cmd_set_intent(db, args)
        db.reset_to_wanted.assert_called_once()
        call_kwargs = db.reset_to_wanted.call_args.kwargs if db.reset_to_wanted.call_args.kwargs else db.reset_to_wanted.call_args[1]
        self.assertEqual(call_kwargs.get("quality_override"), "flac,mp3 v0,mp3 320")

    @patch("builtins.print")
    def test_set_intent_refuses_downloading(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=3, status="downloading", artist_name="A", album_title="B",
        )
        args = MagicMock(id=3, intent="flac_only")
        pipeline_cli.cmd_set_intent(db, args)
        db.reset_to_wanted.assert_not_called()
        db._execute.assert_not_called()

    @patch("builtins.print")
    def test_set_intent_not_found(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = None
        args = MagicMock(id=99, intent="upgrade")
        pipeline_cli.cmd_set_intent(db, args)
        db.reset_to_wanted.assert_not_called()
        db._execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
