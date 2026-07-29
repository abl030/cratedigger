"""Tests for scripts/cleanup_ghost_imported.py."""

import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.beets_db import BeetsDB
from scripts import cleanup_ghost_imported
from scripts.cleanup_ghost_imported import classify_imported_rows


def _make_beets_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE albums (
            id INTEGER PRIMARY KEY,
            mb_albumid TEXT,
            discogs_albumid INTEGER
        );
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            path BLOB,
            title TEXT,
            track INTEGER,
            disc INTEGER,
            length REAL,
            format TEXT,
            bitrate INTEGER,
            samplerate INTEGER,
            bitdepth INTEGER
        );
    """)
    conn.commit()
    conn.close()


class TestCleanupGhostImported(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "beets.db")
        _make_beets_db(self.db_path)

    def tearDown(self) -> None:
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass
        os.rmdir(self.tmpdir)

    def test_classify_imported_rows_detects_missing_mb_and_discogs_releases(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO albums (id, mb_albumid) VALUES (1, ?)",
            ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",),
        )
        conn.execute(
            "INSERT INTO albums (id, discogs_albumid) VALUES (2, ?)",
            (12856590,),
        )
        conn.execute(
            "INSERT INTO items (id, album_id, path) VALUES (11, 1, ?)",
            (os.path.join(self.tmpdir, "mb", "01.flac"),),
        )
        conn.execute(
            "INSERT INTO items (id, album_id, path) VALUES (21, 2, ?)",
            (os.path.join(self.tmpdir, "discogs", "01.flac"),),
        )
        conn.commit()
        conn.close()

        rows = [
            {
                "id": 1,
                "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_release_id": None,
                "artist_name": "Present MB",
                "album_title": "Keep",
            },
            {
                "id": 2,
                "mb_release_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "discogs_release_id": None,
                "artist_name": "Missing MB",
                "album_title": "Ghost",
            },
            {
                "id": 3,
                "mb_release_id": None,
                "discogs_release_id": "12856590",
                "artist_name": "Present Discogs",
                "album_title": "Keep Too",
            },
            {
                "id": 4,
                "mb_release_id": None,
                "discogs_release_id": "5555555",
                "artist_name": "Missing Discogs",
                "album_title": "Ghost Too",
            },
        ]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual([row["id"] for row in ghosts], [2, 4])
        self.assertEqual(manual_review, [])

    def test_classify_imported_rows_flags_missing_release_ids_for_manual_review(self):
        rows = [
            {
                "id": 7,
                "mb_release_id": None,
                "discogs_release_id": None,
                "artist_name": "Unknown",
                "album_title": "Needs Review",
            }
        ]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual(ghosts, [])
        self.assertEqual([row["id"] for row in manual_review], [7])

    def test_conflicting_release_fields_require_manual_review(self):
        rows = [{
            "id": 9,
            "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "discogs_release_id": "12856590",
            "artist_name": "Conflicting",
            "album_title": "Never Auto Delete",
        }]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual(ghosts, [])
        self.assertEqual([row["id"] for row in manual_review], [9])

    def test_classify_imported_rows_fails_closed_on_ambiguous_exact_identity(self):
        release_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        conn = sqlite3.connect(self.db_path)
        for album_id in (1, 2):
            conn.execute(
                "INSERT INTO albums (id, mb_albumid) VALUES (?, ?)",
                (album_id, release_id),
            )
            conn.execute(
                "INSERT INTO items (id, album_id, path) VALUES (?, ?, ?)",
                (
                    album_id * 10,
                    album_id,
                    os.path.join(self.tmpdir, str(album_id), "01.flac"),
                ),
            )
        conn.commit()
        conn.close()
        rows = [{
            "id": 8,
            "mb_release_id": release_id,
            "discogs_release_id": None,
            "artist_name": "Ambiguous",
            "album_title": "Manual Review",
        }]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual(ghosts, [])
        self.assertEqual([row["id"] for row in manual_review], [8])

    def test_apply_reports_conditional_delete_rejection(self) -> None:
        row = {
            "id": 12,
            "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "discogs_release_id": None,
            "artist_name": "Owned",
            "album_title": "Preserved",
        }
        db = MagicMock()
        db.get_by_status.return_value = [row]
        db.delete_request.return_value = False
        stdout = io.StringIO()

        with BeetsDB(self.db_path) as beets, redirect_stdout(stdout):
            result = cleanup_ghost_imported.cmd_apply(db, beets)

        self.assertEqual(result, 4)
        db.delete_request.assert_called_once_with(12)
        self.assertIn("preserved 12", stdout.getvalue())
        self.assertIn("deleted 0 ghost imported rows", stdout.getvalue())

    def test_apply_reports_exact_processing_owner_rejection(self) -> None:
        row = {
            "id": 13,
            "mb_release_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "discogs_release_id": None,
            "artist_name": "Owned",
            "album_title": "Processing",
        }
        db = MagicMock()
        db.get_by_status.return_value = [row]
        db.delete_request.return_value = False
        db.get_request.return_value = {
            **row,
            "status": "processing",
            "processing_owner": {
                "job_id": 91,
                "status": "running",
                "preview_status": "evidence_ready",
            },
        }
        stdout = io.StringIO()

        with BeetsDB(self.db_path) as beets, redirect_stdout(stdout):
            result = cleanup_ghost_imported.cmd_apply(db, beets)

        self.assertEqual(result, 4)
        payload = json.loads(stdout.getvalue().splitlines()[0])
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["processing_owner"], {
            "job_id": 91,
            "status": "running",
            "preview_status": "evidence_ready",
        })
        self.assertIn("deleted 0 ghost imported rows", stdout.getvalue())


class TestDefaultDsnFailsLoud(unittest.TestCase):
    """#479 item 2: no hardcoded fallback — fail loud instead."""

    @patch.object(cleanup_ghost_imported, "DEFAULT_DSN", None)
    def test_main_fails_loud_when_dsn_is_not_configured(self) -> None:
        with patch.object(sys, "argv", ["cleanup_ghost_imported.py"]):
            stderr = io.StringIO()
            with redirect_stderr(stderr), self.assertRaises(SystemExit) as cm:
                cleanup_ghost_imported.main()

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("PIPELINE_DB_DSN", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
