#!/usr/bin/env python3
"""Unit tests for lib/beets_db.py — beets library database queries.

Uses a temporary SQLite database to test queries without needing the real
beets library. The schema matches what beets creates.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.beets_db import BeetsDB, AlbumInfo


def _create_test_db(path: str) -> None:
    """Create a minimal beets-like SQLite DB for testing."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE albums (
            id INTEGER PRIMARY KEY,
            mb_albumid TEXT,
            album TEXT,
            albumartist TEXT
        );
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            bitrate INTEGER,
            path BLOB
        );
    """)
    conn.close()


def _insert_album(path: str, album_id: int, mbid: str,
                   tracks: list[tuple[int, str]]) -> None:
    """Insert an album with tracks. tracks = [(bitrate_bps, path_str), ...]"""
    conn = sqlite3.connect(path)
    conn.execute("INSERT INTO albums (id, mb_albumid) VALUES (?, ?)",
                 (album_id, mbid))
    for i, (bitrate, track_path) in enumerate(tracks):
        conn.execute(
            "INSERT INTO items (album_id, bitrate, path) VALUES (?, ?, ?)",
            (album_id, bitrate, track_path.encode()))
    conn.commit()
    conn.close()


class TestBeetsDBConnection(unittest.TestCase):
    """Test connection and basic operations."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)

    def test_connect_readonly(self) -> None:
        db = BeetsDB(self.db_path)
        self.assertIsNotNone(db)
        db.close()

    def test_missing_db_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            BeetsDB("/nonexistent/path.db")

    def test_context_manager(self) -> None:
        with BeetsDB(self.db_path) as db:
            self.assertIsNotNone(db)


class TestAlbumExists(unittest.TestCase):
    """Test album_exists (preflight check)."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "abc-123",
                       [(320000, "/music/Artist/Album/01.mp3")])

    def test_exists(self) -> None:
        with BeetsDB(self.db_path) as db:
            self.assertTrue(db.album_exists("abc-123"))

    def test_not_exists(self) -> None:
        with BeetsDB(self.db_path) as db:
            self.assertFalse(db.album_exists("xyz-999"))


class TestGetAlbumInfo(unittest.TestCase):
    """Test get_album_info (postflight verify + quality gate data)."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)

    def test_single_album(self) -> None:
        _insert_album(self.db_path, 1, "abc-123", [
            (320000, "/music/Artist/Album/01.mp3"),
            (320000, "/music/Artist/Album/02.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("abc-123")
        assert info is not None
        self.assertEqual(info.album_id, 1)
        self.assertEqual(info.track_count, 2)
        self.assertEqual(info.min_bitrate_kbps, 320)
        self.assertTrue(info.is_cbr)
        self.assertEqual(info.album_path, "/music/Artist/Album")

    def test_vbr_album(self) -> None:
        _insert_album(self.db_path, 2, "def-456", [
            (245000, "/music/A/B/01.mp3"),
            (238000, "/music/A/B/02.mp3"),
            (251000, "/music/A/B/03.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("def-456")
        assert info is not None
        self.assertEqual(info.min_bitrate_kbps, 238)
        self.assertFalse(info.is_cbr)
        self.assertEqual(info.track_count, 3)

    def test_not_found(self) -> None:
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("nonexistent")
        self.assertIsNone(info)

    def test_album_no_tracks(self) -> None:
        """Album exists but no items — should return None."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO albums (id, mb_albumid) VALUES (5, 'empty-1')")
        conn.commit()
        conn.close()
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("empty-1")
        self.assertIsNone(info)

    def test_zero_bitrate_ignored(self) -> None:
        """Tracks with 0 bitrate should be treated as no data."""
        _insert_album(self.db_path, 3, "ghi-789", [
            (0, "/music/A/B/01.mp3"),
            (256000, "/music/A/B/02.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("ghi-789")
        assert info is not None
        self.assertEqual(info.min_bitrate_kbps, 256)

    def test_path_as_bytes(self) -> None:
        """Beets stores paths as bytes — should decode correctly."""
        _insert_album(self.db_path, 4, "jkl-012", [
            (320000, "/music/Ärtiöst/Albüm/01.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("jkl-012")
        assert info is not None
        self.assertIn("Albüm", info.album_path)


class TestGetMinBitrate(unittest.TestCase):
    """Test get_min_bitrate (standalone bitrate query)."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)

    def test_returns_kbps(self) -> None:
        _insert_album(self.db_path, 1, "abc", [
            (320000, "/m/a/01.mp3"),
            (256000, "/m/a/02.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            self.assertEqual(db.get_min_bitrate("abc"), 256)

    def test_not_found(self) -> None:
        with BeetsDB(self.db_path) as db:
            self.assertIsNone(db.get_min_bitrate("nonexistent"))

    def test_zero_bitrate(self) -> None:
        _insert_album(self.db_path, 1, "abc", [
            (0, "/m/a/01.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            self.assertIsNone(db.get_min_bitrate("abc"))


if __name__ == "__main__":
    unittest.main()
