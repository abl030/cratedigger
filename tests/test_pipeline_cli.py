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


class TestCmdForceImport(unittest.TestCase):
    @patch("builtins.print")
    @patch("pipeline_cli._resolve_failed_path", return_value="/tmp/Test Album")
    def test_force_import_passes_source_username_to_dispatch(self, _mock_resolve, _mock_print):
        from lib.import_dispatch import DispatchOutcome

        db = MagicMock()
        db.get_download_log_entry.return_value = {
            "request_id": 123,
            "soulseek_username": "baduser",
            "validation_result": {"failed_path": "/tmp/Test Album"},
        }
        db.get_request.return_value = make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        )

        mock_outcome = DispatchOutcome(success=True, message="ok")
        with patch("lib.import_dispatch.dispatch_import_from_db",
                    return_value=mock_outcome) as mock_dispatch:
            args = MagicMock(download_log_id=42)
            pipeline_cli.cmd_force_import(db, args)

        mock_dispatch.assert_called_once_with(
            db, request_id=123, failed_path="/tmp/Test Album",
            force=True, outcome_label="force_import",
            source_username="baduser",
        )


class TestCmdManualImport(unittest.TestCase):
    @patch("builtins.print")
    def test_failed_manual_import_prints_error(self, _mock_print):
        from lib.import_dispatch import DispatchOutcome
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        )

        mock_outcome = DispatchOutcome(
            success=False,
            message="Rejected: quality_downgrade — new 192kbps <= existing 320kbps",
        )
        with patch("lib.import_dispatch.dispatch_import_from_db",
                    return_value=mock_outcome):
            args = MagicMock(id=123, path="/tmp/Album")
            pipeline_cli.cmd_manual_import(db, args)

        # Should print failure message
        _mock_print.assert_any_call("  [FAIL] Rejected: quality_downgrade — new 192kbps <= existing 320kbps")

    @patch("builtins.print")
    def test_manual_import_calls_dispatch_from_db(self, _mock_print):
        from lib.import_dispatch import DispatchOutcome
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        )

        mock_outcome = DispatchOutcome(success=True, message="ok")
        with patch("lib.import_dispatch.dispatch_import_from_db",
                    return_value=mock_outcome) as mock_dispatch:
            args = MagicMock(id=123, path="/tmp/Album")
            pipeline_cli.cmd_manual_import(db, args)

        mock_dispatch.assert_called_once_with(
            db, request_id=123, failed_path="/tmp/Album",
            force=False, outcome_label="manual_import",
        )


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
    """Tests for cmd_set_intent — lossless-on-disk toggle."""

    @patch("builtins.print")
    def test_set_lossless_on_wanted(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        )
        args = MagicMock(id=1, intent="lossless")
        pipeline_cli.cmd_set_intent(db, args)
        db.update_request_fields.assert_called_once_with(1, target_format="lossless")

    @patch("builtins.print")
    def test_set_default_clears_target(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        )
        args = MagicMock(id=1, intent="default")
        pipeline_cli.cmd_set_intent(db, args)
        db.update_request_fields.assert_called_once_with(1, target_format=None)

    @patch("builtins.print")
    @patch("lib.transitions.apply_transition")
    def test_set_lossless_on_imported_requeues(self, mock_transition, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=2, status="imported", artist_name="A", album_title="B",
            min_bitrate=245,
        )
        args = MagicMock(id=2, intent="lossless")
        pipeline_cli.cmd_set_intent(db, args)
        mock_transition.assert_called_once()
        call_kwargs = mock_transition.call_args.kwargs or mock_transition.call_args[1]
        self.assertEqual(call_kwargs.get("search_filetype_override"), "lossless")
        db.update_request_fields.assert_called_once_with(2, target_format="lossless")

    @patch("builtins.print")
    def test_set_default_clears_stale_lossless_override(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=4, status="wanted", artist_name="A", album_title="B",
            target_format="lossless", search_filetype_override="lossless",
        )
        args = MagicMock(id=4, intent="default")
        pipeline_cli.cmd_set_intent(db, args)
        db.update_request_fields.assert_called_once_with(
            4, target_format=None, search_filetype_override=None)

    @patch("builtins.print")
    def test_set_intent_refuses_downloading(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=3, status="downloading", artist_name="A", album_title="B",
        )
        args = MagicMock(id=3, intent="lossless")
        pipeline_cli.cmd_set_intent(db, args)
        db.update_request_fields.assert_not_called()

    @patch("builtins.print")
    def test_set_intent_not_found(self, _mock_print):
        db = MagicMock()
        db.get_request.return_value = None
        args = MagicMock(id=99, intent="lossless")
        pipeline_cli.cmd_set_intent(db, args)
        db.update_request_fields.assert_not_called()


if __name__ == "__main__":
    unittest.main()
