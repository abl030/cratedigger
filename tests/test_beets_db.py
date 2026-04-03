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
            albumartist TEXT,
            year INTEGER,
            albumtype TEXT,
            label TEXT,
            country TEXT,
            added REAL,
            mb_releasegroupid TEXT,
            release_group_title TEXT,
            format TEXT,
            artpath BLOB,
            discogs_albumid TEXT,
            mb_albumartistid TEXT,
            mb_albumartistids TEXT
        );
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            bitrate INTEGER,
            path BLOB,
            title TEXT,
            artist TEXT,
            track INTEGER,
            disc INTEGER,
            length REAL,
            format TEXT,
            samplerate INTEGER,
            bitdepth INTEGER
        );
    """)
    conn.close()


def _insert_album(path: str, album_id: int, mbid: str,
                   tracks: list[tuple[int, str]], **kwargs: object) -> None:
    """Insert an album with tracks. tracks = [(bitrate_bps, path_str), ...]
    Extra kwargs are set as album columns (e.g. album='Foo', albumartist='Bar').
    """
    conn = sqlite3.connect(path)
    cols = "id, mb_albumid"
    vals: list[object] = [album_id, mbid]
    for k, v in kwargs.items():
        cols += f", {k}"
        vals.append(v)
    placeholders = ", ".join(["?"] * len(vals))
    conn.execute(f"INSERT INTO albums ({cols}) VALUES ({placeholders})", vals)
    for i, (bitrate, track_path) in enumerate(tracks):
        conn.execute(
            "INSERT INTO items (album_id, bitrate, path) VALUES (?, ?, ?)",
            (album_id, bitrate, track_path.encode()))
    conn.commit()
    conn.close()


def _insert_album_full(path: str, album_id: int, mbid: str,
                       tracks: list[dict[str, object]],
                       **kwargs: object) -> None:
    """Insert an album with full track details.
    tracks = [{'bitrate': 320000, 'path': '/a/b.mp3', 'title': 'Song', ...}, ...]
    Extra kwargs are set as album columns.
    """
    conn = sqlite3.connect(path)
    cols = "id, mb_albumid"
    vals: list[object] = [album_id, mbid]
    for k, v in kwargs.items():
        cols += f", {k}"
        vals.append(v)
    placeholders = ", ".join(["?"] * len(vals))
    conn.execute(f"INSERT INTO albums ({cols}) VALUES ({placeholders})", vals)
    for t in tracks:
        t_cols = ["album_id"]
        t_vals: list[object] = [album_id]
        for k, v in t.items():
            if k == "path":
                t_cols.append(k)
                t_vals.append(str(v).encode())
            else:
                t_cols.append(k)
                t_vals.append(v)
        t_placeholders = ", ".join(["?"] * len(t_vals))
        conn.execute(
            f"INSERT INTO items ({', '.join(t_cols)}) VALUES ({t_placeholders})",
            t_vals)
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


from lib.quality import AUDIO_EXTENSIONS_DOTTED as AUDIO_EXTENSIONS


class TestGetItemPaths(unittest.TestCase):
    """Test get_item_paths for post-import extension checking."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)

    def test_returns_paths(self) -> None:
        _insert_album(self.db_path, 1, "abc", [
            (320000, "/m/a/01.mp3"),
            (320000, "/m/a/02.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            paths = db.get_item_paths("abc")
        self.assertEqual(len(paths), 2)
        self.assertEqual(paths[0][1], "/m/a/01.mp3")

    def test_not_found(self) -> None:
        with BeetsDB(self.db_path) as db:
            paths = db.get_item_paths("nonexistent")
        self.assertEqual(paths, [])

    def test_detects_bak_extension(self) -> None:
        """The .bak bug: track 01 gets renamed to .bak after import."""
        _insert_album(self.db_path, 1, "abc", [
            (320000, "/m/a/01 Track.bak"),
            (320000, "/m/a/02 Track.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            paths = db.get_item_paths("abc")
        bad = [(item_id, p) for item_id, p in paths
               if os.path.splitext(p)[1].lower() not in AUDIO_EXTENSIONS]
        self.assertEqual(len(bad), 1)
        self.assertIn(".bak", bad[0][1])


class TestCheckMbids(unittest.TestCase):
    """Test check_mbids — batch MBID existence check."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111",
                       [(320000, "/m/a/01.mp3")])
        _insert_album(self.db_path, 2, "bbb-222",
                       [(256000, "/m/b/01.mp3")])

    def test_mix_existing_and_missing(self) -> None:
        with BeetsDB(self.db_path) as db:
            found = db.check_mbids(["aaa-111", "bbb-222", "zzz-999"])
        self.assertEqual(found, {"aaa-111", "bbb-222"})

    def test_empty_list(self) -> None:
        with BeetsDB(self.db_path) as db:
            found = db.check_mbids([])
        self.assertEqual(found, set())

    def test_all_found(self) -> None:
        with BeetsDB(self.db_path) as db:
            found = db.check_mbids(["aaa-111", "bbb-222"])
        self.assertEqual(found, {"aaa-111", "bbb-222"})

    def test_none_found(self) -> None:
        with BeetsDB(self.db_path) as db:
            found = db.check_mbids(["xxx-000", "yyy-000"])
        self.assertEqual(found, set())


class TestCheckMbidsDetail(unittest.TestCase):
    """Test check_mbids_detail — batch MBID detail lookup."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album_full(self.db_path, 1, "aaa-111", [
            {"bitrate": 320000, "path": "/m/a/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
            {"bitrate": 320000, "path": "/m/a/02.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ])
        _insert_album_full(self.db_path, 2, "bbb-222", [
            {"bitrate": 1411000, "path": "/m/b/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ])

    def test_returns_correct_detail(self) -> None:
        with BeetsDB(self.db_path) as db:
            detail = db.check_mbids_detail(["aaa-111", "bbb-222"])
        self.assertIn("aaa-111", detail)
        self.assertEqual(detail["aaa-111"]["beets_tracks"], 2)
        self.assertEqual(detail["aaa-111"]["beets_format"], "MP3")
        self.assertEqual(detail["aaa-111"]["beets_samplerate"], 44100)
        self.assertIn("bbb-222", detail)
        self.assertEqual(detail["bbb-222"]["beets_tracks"], 1)
        self.assertEqual(detail["bbb-222"]["beets_format"], "FLAC")
        self.assertEqual(detail["bbb-222"]["beets_bitdepth"], 16)

    def test_missing_mbid_not_in_result(self) -> None:
        with BeetsDB(self.db_path) as db:
            detail = db.check_mbids_detail(["zzz-999"])
        self.assertEqual(detail, {})


class TestSearchAlbums(unittest.TestCase):
    """Test search_albums — LIKE search on artist or album."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111",
                       [(320000, "/m/a/01.mp3")],
                       album="OK Computer", albumartist="Radiohead")
        _insert_album(self.db_path, 2, "bbb-222",
                       [(256000, "/m/b/01.mp3")],
                       album="Kid A", albumartist="Radiohead")
        _insert_album(self.db_path, 3, "ccc-333",
                       [(256000, "/m/c/01.mp3")],
                       album="Blue Lines", albumartist="Massive Attack")

    def test_match_by_artist(self) -> None:
        with BeetsDB(self.db_path) as db:
            results = db.search_albums("Radiohead")
        self.assertEqual(len(results), 2)

    def test_match_by_album(self) -> None:
        with BeetsDB(self.db_path) as db:
            results = db.search_albums("Blue Lines")
        self.assertEqual(len(results), 1)

    def test_no_results(self) -> None:
        with BeetsDB(self.db_path) as db:
            results = db.search_albums("Nonexistent Band")
        self.assertEqual(len(results), 0)


class TestGetRecent(unittest.TestCase):
    """Test get_recent — most recently added albums."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111",
                       [(320000, "/m/a/01.mp3")],
                       album="Old Album", albumartist="Artist A", added=1000.0)
        _insert_album(self.db_path, 2, "bbb-222",
                       [(256000, "/m/b/01.mp3")],
                       album="New Album", albumartist="Artist B", added=2000.0)
        _insert_album(self.db_path, 3, "ccc-333",
                       [(256000, "/m/c/01.mp3")],
                       album="Newest Album", albumartist="Artist C", added=3000.0)

    def test_returns_most_recent_first(self) -> None:
        with BeetsDB(self.db_path) as db:
            results = db.get_recent(limit=3)
        self.assertEqual(len(results), 3)
        # Most recent first
        self.assertEqual(results[0]["album"], "Newest Album")
        self.assertEqual(results[1]["album"], "New Album")
        self.assertEqual(results[2]["album"], "Old Album")

    def test_limit_parameter(self) -> None:
        with BeetsDB(self.db_path) as db:
            results = db.get_recent(limit=2)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["album"], "Newest Album")


class TestGetAlbumDetail(unittest.TestCase):
    """Test get_album_detail — full album with tracks."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album_full(self.db_path, 1, "aaa-111", [
            {"bitrate": 320000, "path": "/m/a/01.mp3", "title": "Track 1",
             "artist": "Artist A", "track": 1, "disc": 1, "length": 240.5,
             "format": "MP3", "samplerate": 44100, "bitdepth": 0},
            {"bitrate": 320000, "path": "/m/a/02.mp3", "title": "Track 2",
             "artist": "Artist A", "track": 2, "disc": 1, "length": 180.0,
             "format": "MP3", "samplerate": 44100, "bitdepth": 0},
        ], album="Test Album", albumartist="Artist A", year=2020, label="Test Label")

    def test_returns_album_with_tracks(self) -> None:
        with BeetsDB(self.db_path) as db:
            detail = db.get_album_detail(1)
        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(detail["album"], "Test Album")
        self.assertEqual(detail["artist"], "Artist A")
        self.assertIn("tracks", detail)
        self.assertEqual(len(detail["tracks"]), 2)
        self.assertEqual(detail["tracks"][0]["title"], "Track 1")

    def test_nonexistent_returns_none(self) -> None:
        with BeetsDB(self.db_path) as db:
            detail = db.get_album_detail(999)
        self.assertIsNone(detail)


class TestGetAlbumsByArtist(unittest.TestCase):
    """Test get_albums_by_artist — albums by artist name."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111",
                       [(320000, "/m/a/01.mp3")],
                       album="Album One", albumartist="Radiohead")
        _insert_album(self.db_path, 2, "bbb-222",
                       [(256000, "/m/b/01.mp3")],
                       album="Album Two", albumartist="Radiohead")
        _insert_album(self.db_path, 3, "ccc-333",
                       [(256000, "/m/c/01.mp3")],
                       album="Other Album", albumartist="Other Artist")

    def test_returns_all_albums_for_artist(self) -> None:
        with BeetsDB(self.db_path) as db:
            results = db.get_albums_by_artist("Radiohead")
        self.assertEqual(len(results), 2)

    def test_empty_result(self) -> None:
        with BeetsDB(self.db_path) as db:
            results = db.get_albums_by_artist("Nonexistent")
        self.assertEqual(len(results), 0)


class TestFindByArtistAlbum(unittest.TestCase):
    """Test find_by_artist_album — track count for artist+album match."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111", [
            (320000, "/m/a/01.mp3"),
            (320000, "/m/a/02.mp3"),
            (320000, "/m/a/03.mp3"),
        ], album="OK Computer", albumartist="Radiohead")

    def test_returns_track_count(self) -> None:
        with BeetsDB(self.db_path) as db:
            count = db.find_by_artist_album("Radiohead", "OK Computer")
        self.assertEqual(count, 3)

    def test_returns_none_for_no_match(self) -> None:
        with BeetsDB(self.db_path) as db:
            count = db.find_by_artist_album("Nonexistent", "Nonexistent")
        self.assertIsNone(count)


class TestGetAvgBitrateKbps(unittest.TestCase):
    """Test get_avg_bitrate_kbps — average bitrate in kbps."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111", [
            (320000, "/m/a/01.mp3"),
            (256000, "/m/a/02.mp3"),
        ])

    def test_correct_average(self) -> None:
        with BeetsDB(self.db_path) as db:
            avg = db.get_avg_bitrate_kbps("aaa-111")
        self.assertEqual(avg, 288)  # (320000 + 256000) / 2 / 1000 = 288

    def test_returns_none_for_missing(self) -> None:
        with BeetsDB(self.db_path) as db:
            avg = db.get_avg_bitrate_kbps("zzz-999")
        self.assertIsNone(avg)


class TestGetTracksByMbReleaseId(unittest.TestCase):
    """Test get_tracks_by_mb_release_id — track list for an MBID."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album_full(self.db_path, 1, "aaa-111", [
            {"bitrate": 320000, "path": "/m/a/01.mp3", "title": "Track 1",
             "artist": "Artist A", "track": 1, "disc": 1, "length": 200.0,
             "format": "MP3", "samplerate": 44100, "bitdepth": 0},
            {"bitrate": 320000, "path": "/m/a/02.mp3", "title": "Track 2",
             "artist": "Artist A", "track": 2, "disc": 1, "length": 180.0,
             "format": "MP3", "samplerate": 44100, "bitdepth": 0},
        ], album="Test Album", albumartist="Artist A")

    def test_returns_tracks(self) -> None:
        with BeetsDB(self.db_path) as db:
            tracks = db.get_tracks_by_mb_release_id("aaa-111")
        self.assertIsNotNone(tracks)
        assert tracks is not None
        self.assertEqual(len(tracks), 2)
        self.assertEqual(tracks[0]["title"], "Track 1")
        self.assertEqual(tracks[0]["bitrate"], 320000)

    def test_returns_none_for_missing(self) -> None:
        with BeetsDB(self.db_path) as db:
            tracks = db.get_tracks_by_mb_release_id("zzz-999")
        self.assertIsNone(tracks)


class TestGetAlbumIdsByMbids(unittest.TestCase):
    """Test get_album_ids_by_mbids — batch MBID to album ID lookup."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111", [(320000, "/a.mp3")])
        _insert_album(self.db_path, 2, "bbb-222", [(320000, "/b.mp3")])

    def test_returns_mapping(self) -> None:
        with BeetsDB(self.db_path) as db:
            result = db.get_album_ids_by_mbids(["aaa-111", "bbb-222"])
        self.assertEqual(result, {"aaa-111": 1, "bbb-222": 2})

    def test_partial_match(self) -> None:
        with BeetsDB(self.db_path) as db:
            result = db.get_album_ids_by_mbids(["aaa-111", "zzz-999"])
        self.assertEqual(result, {"aaa-111": 1})

    def test_empty_input(self) -> None:
        with BeetsDB(self.db_path) as db:
            result = db.get_album_ids_by_mbids([])
        self.assertEqual(result, {})


class TestDeleteAlbum(unittest.TestCase):
    """Test delete_album — static method for writable deletion."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111", [
            (320000, "/m/a/01.mp3"), (320000, "/m/a/02.mp3"),
        ], album="Test Album", albumartist="Test Artist")

    def test_deletes_and_returns_metadata(self) -> None:
        album, artist, paths = BeetsDB.delete_album(self.db_path, 1)
        self.assertEqual(album, "Test Album")
        self.assertEqual(artist, "Test Artist")
        self.assertEqual(len(paths), 2)
        # Verify rows are gone
        with BeetsDB(self.db_path) as db:
            self.assertFalse(db.album_exists("aaa-111"))

    def test_not_found_raises(self) -> None:
        with self.assertRaises(ValueError):
            BeetsDB.delete_album(self.db_path, 999)


class TestAlbumRowSource(unittest.TestCase):
    """Test that _album_row_to_dict computes source correctly."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        # MB album (UUID with hyphens)
        _insert_album(self.db_path, 1, "aaa-bbb-ccc", [(320000, "/a.mp3")],
                       album="MB Album", albumartist="Artist")
        # Discogs album (numeric ID, no hyphens)
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO albums (id, mb_albumid, album, albumartist, discogs_albumid) "
            "VALUES (2, '12345', 'Discogs Album', 'Artist', '67890')")
        conn.execute("INSERT INTO items (album_id, bitrate, path) VALUES (2, 320000, X'2F622E6D7033')")
        conn.commit()
        conn.close()

    def test_mb_source(self) -> None:
        with BeetsDB(self.db_path) as db:
            albums = db.get_albums_by_artist("Artist")
        mb = [a for a in albums if a["album"] == "MB Album"]
        self.assertEqual(len(mb), 1)
        self.assertEqual(mb[0]["source"], "musicbrainz")

    def test_discogs_source(self) -> None:
        with BeetsDB(self.db_path) as db:
            albums = db.get_albums_by_artist("Artist")
        discogs = [a for a in albums if a["album"] == "Discogs Album"]
        self.assertEqual(len(discogs), 1)
        self.assertEqual(discogs[0]["source"], "discogs")


if __name__ == "__main__":
    unittest.main()
