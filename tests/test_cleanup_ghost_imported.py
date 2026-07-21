#!/usr/bin/env python3
"""Tests for scripts/cleanup_ghost_imported.py."""

import io
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

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
            "imported_path": "/poisoned/request/cache",
        }]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual(ghosts, [])
        self.assertEqual([row["id"] for row in manual_review], [8])


class TestDefaultDsnFailsLoud(unittest.TestCase):
    """#479 item 2: no hardcoded fallback — fail loud instead."""

    @patch.object(cleanup_ghost_imported, "DEFAULT_DSN", None)
    def test_main_fails_loud_when_dsn_is_not_configured(self) -> None:
        with patch.object(sys, "argv", ["cleanup_ghost_imported.py"]):
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as cm:
                    cleanup_ghost_imported.main()

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("PIPELINE_DB_DSN", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
