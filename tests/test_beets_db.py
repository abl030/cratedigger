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
            discogs_albumid INTEGER,
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
                   tracks: list[tuple[int, str]],
                   track_format: str = "MP3",
                   **kwargs: object) -> None:
    """Insert an album with tracks. tracks = [(bitrate_bps, path_str), ...]
    Extra kwargs are set as album columns (e.g. album='Foo', albumartist='Bar').
    ``track_format`` is written to every item's format column — defaults to
    "MP3" for historical tests.
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
            "INSERT INTO items (album_id, bitrate, path, format) "
            "VALUES (?, ?, ?, ?)",
            (album_id, bitrate, track_path.encode(), track_format))
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


class TestGetReleaseIdsByAlbumId(unittest.TestCase):
    """Codex round-1 P1 on PR #136: beets' ``albums.discogs_albumid``
    is ``INTEGER`` in SQLite, so SQLite returns a Python ``int`` for
    that column — not a ``str``. ``MovedSibling.discogs_albumid`` is
    typed ``str`` and ``ImportResult.from_dict``'s
    ``msgspec.convert`` decoder validates types strictly.

    Without coercion, every Discogs-sourced kept-duplicate import
    raised ``msgspec.ValidationError`` at the wire boundary AFTER
    beets had already moved the sibling files — the dispatcher then
    recorded the whole import as an exception. This is the same
    "subprocess side effect done, sentinel emission broken" hazard
    PR #131 documented for earlier missing-sentinel cases.

    ``get_release_ids_by_album_id`` now coerces both columns to ``str``
    at the emit site. These tests pin the contract.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_discogs_albumid_coerced_from_int_to_str(self) -> None:
        """``discogs_albumid INTEGER`` in SQLite → Python ``int``.
        The getter must coerce to ``str`` so the wire contract holds."""
        _insert_album_full(self.db_path, 42, "", [
            {"bitrate": 320000, "path": "/m/x.mp3", "format": "MP3"},
        ], discogs_albumid=12856590)
        with BeetsDB(self.db_path) as db:
            mb, discogs = db.get_release_ids_by_album_id(42)
        self.assertEqual(mb, "")
        self.assertEqual(discogs, "12856590")
        self.assertIsInstance(discogs, str,
                              "discogs_albumid must be str at the wire "
                              "boundary — msgspec.convert will reject int.")

    def test_mb_albumid_returned_as_str(self) -> None:
        """``mb_albumid`` is ``TEXT`` so already arrives as ``str`` —
        the getter still string-wraps defensively."""
        _insert_album(self.db_path, 43, "abc-uuid-1234",
                      [(320000, "/m/y.mp3")])
        with BeetsDB(self.db_path) as db:
            mb, discogs = db.get_release_ids_by_album_id(43)
        self.assertEqual(mb, "abc-uuid-1234")
        self.assertIsInstance(mb, str)
        self.assertEqual(discogs, "")

    def test_both_empty_when_album_not_found(self) -> None:
        """Missing album → ``("", "")``, never raises."""
        with BeetsDB(self.db_path) as db:
            mb, discogs = db.get_release_ids_by_album_id(99999)
        self.assertEqual((mb, discogs), ("", ""))

    def test_null_columns_map_to_empty_strings(self) -> None:
        """A row where both columns are NULL (unlikely but possible
        for partially-tagged imports) must still return ``("", "")``
        without raising."""
        _insert_album_full(self.db_path, 44, "", [
            {"bitrate": 320000, "path": "/m/z.mp3", "format": "MP3"},
        ])
        with BeetsDB(self.db_path) as db:
            mb, discogs = db.get_release_ids_by_album_id(44)
        self.assertEqual((mb, discogs), ("", ""))


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

    def test_discogs_id_matches_discogs_albumid(self) -> None:
        """Discogs-backed requests pack a numeric ID into ``mb_release_id``
        that beets actually stores in ``albums.discogs_albumid``. The
        existence check must dispatch on ID shape, otherwise a
        Discogs-imported album reads as 'not in beets' and ban-source
        skips the ``beet remove -d`` altogether.
        """
        _insert_album_full(self.db_path, 99, "", [
            {"bitrate": 1411000, "path": "/m/disc/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ], discogs_albumid=12856590)

        with BeetsDB(self.db_path) as db:
            self.assertTrue(db.album_exists("12856590"),
                            "Discogs numeric ID must resolve via discogs_albumid.")
            self.assertFalse(db.album_exists("999"),
                             "Unmatched numeric ID must return False.")

    def test_discogs_id_matches_legacy_mb_albumid(self) -> None:
        """Legacy beets libraries (imported before the Discogs plugin started
        populating ``discogs_albumid``) stored Discogs numeric IDs as
        TEXT in ``mb_albumid``. ``lib/artist_compare.py`` and the
        webui-primer explicitly document this duality, so the existence
        check must fall back to ``mb_albumid`` for numeric IDs too —
        otherwise ban-source / status-reset skip ``beet remove`` for
        those albums and the Discogs copy lingers forever.
        """
        _insert_album_full(self.db_path, 88, "5555555", [
            {"bitrate": 1411000, "path": "/m/legacy/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ])

        with BeetsDB(self.db_path) as db:
            self.assertTrue(db.album_exists("5555555"),
                            "Legacy numeric mb_albumid must still resolve.")


class TestLocate(unittest.TestCase):
    """Single seam: ``BeetsDB.locate`` answers 'is this release on disk?'.

    Every existing ``album_exists`` / ``get_album_info`` / ``get_min_bitrate``
    / ``get_item_paths`` / ``get_album_path`` / ``get_tracks_by_mb_release_id``
    / ``get_avg_bitrate_kbps`` / ``check_mbids`` caller must route through
    this — see issue #121. Four outcomes:

    - UUID in ``albums.mb_albumid`` → ``kind="exact"``,
      ``selectors=("mb_albumid:<uuid>",)``.
    - Discogs numeric in ``albums.discogs_albumid`` → ``kind="exact"``,
      ``selectors`` iterates BOTH the new-layout and the legacy
      ``mb_albumid`` selector so ``beet remove -d`` can't silently skip
      the column that actually holds the album.
    - Discogs numeric in legacy ``albums.mb_albumid`` (pre-plugin-patch
      libraries) → ``kind="exact"`` with the same selector pair.
    - Nothing matches → ``kind="absent"`` with ``selectors=()`` and
      ``album_id=None``.

    Issue #123 sharpened the seam: the fuzzy ``kind="fuzzy"`` fallback was
    deleted because it silently attributed quality to sibling pressings
    (the PR #119 marathon). After this change, 'is this release on disk?'
    is answered solely by exact-ID match and ``locate()`` takes only a
    ``release_id`` argument — no artist/album fallback.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        # UUID-indexed MusicBrainz album
        _insert_album_full(self.db_path, 1, "aaa0bbb0-cccc-dddd-eeee-ffffffffffff", [
            {"bitrate": 320000, "path": "/m/a/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ], album="OK Computer", albumartist="Radiohead")
        # New-layout Discogs numeric (in discogs_albumid)
        _insert_album_full(self.db_path, 2, "", [
            {"bitrate": 1411000, "path": "/m/disc/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ], discogs_albumid=12856590, album="New Ritual", albumartist="DICE")
        # Legacy-layout Discogs (numeric in mb_albumid, no discogs_albumid)
        _insert_album_full(self.db_path, 3, "5555555", [
            {"bitrate": 320000, "path": "/m/legacy/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ], album="Legacy Press", albumartist="Old Band")

    def test_locate_uuid_exact(self) -> None:
        """UUID → exact hit, selector is ``mb_albumid:<uuid>`` only."""
        with BeetsDB(self.db_path) as db:
            loc = db.locate("aaa0bbb0-cccc-dddd-eeee-ffffffffffff")
        self.assertEqual(loc.kind, "exact")
        self.assertEqual(loc.album_id, 1)
        self.assertEqual(
            loc.selectors,
            ("mb_albumid:aaa0bbb0-cccc-dddd-eeee-ffffffffffff",))

    def test_locate_discogs_numeric_new_layout(self) -> None:
        """New-layout Discogs → exact hit, selectors cover both columns.

        Even though the album lives in ``discogs_albumid`` on this
        install, a sibling install might hold the same ID in
        ``mb_albumid`` (legacy). ``beet remove -d`` must hit both.
        """
        with BeetsDB(self.db_path) as db:
            loc = db.locate("12856590")
        self.assertEqual(loc.kind, "exact")
        self.assertEqual(loc.album_id, 2)
        self.assertEqual(
            set(loc.selectors),
            {"discogs_albumid:12856590", "mb_albumid:12856590"})

    def test_locate_discogs_legacy_mb_albumid(self) -> None:
        """Numeric ID lives in ``mb_albumid`` (legacy) — still exact.

        The only path that ever exposed this kind of album to the
        pipeline before issue #121 was the fuzzy fallback; now the
        locate seam resolves it by ID.
        """
        with BeetsDB(self.db_path) as db:
            loc = db.locate("5555555")
        self.assertEqual(loc.kind, "exact")
        self.assertEqual(loc.album_id, 3)
        self.assertEqual(
            set(loc.selectors),
            {"discogs_albumid:5555555", "mb_albumid:5555555"})

    def test_locate_normalizes_uuid_and_discogs_inputs(self) -> None:
        with BeetsDB(self.db_path) as db:
            uuid_loc = db.locate(" AAA0BBB0-CCCC-DDDD-EEEE-FFFFFFFFFFFF ")
            discogs_loc = db.locate(" 0012856590 ")
        self.assertEqual(uuid_loc.kind, "exact")
        self.assertEqual(uuid_loc.album_id, 1)
        self.assertEqual(
            uuid_loc.selectors,
            ("mb_albumid:aaa0bbb0-cccc-dddd-eeee-ffffffffffff",))
        self.assertEqual(discogs_loc.kind, "exact")
        self.assertEqual(discogs_loc.album_id, 2)
        self.assertEqual(
            set(discogs_loc.selectors),
            {"discogs_albumid:12856590", "mb_albumid:12856590"})

    def test_locate_absent(self) -> None:
        """No ID hit + no artist/album → absent with empty selectors."""
        with BeetsDB(self.db_path) as db:
            loc = db.locate("zzz-999-not-present")
        self.assertEqual(loc.kind, "absent")
        self.assertIsNone(loc.album_id)
        self.assertEqual(loc.selectors, ())

    def test_locate_untagged_album_is_absent(self) -> None:
        """Legacy untagged albums resolve to absent, not a fuzzy ghost hit.

        Issue #123: the old fuzzy fallback claimed the album was 'in
        library' when artist+album matched an untagged row. That leaked
        a sibling pressing's quality into the UI (see the PR #119
        marathon). After the refactor, the honest answer is ``absent``
        — the user can re-tag their library or add the release to the
        pipeline.
        """
        _insert_album_full(self.db_path, 4, "", [
            {"bitrate": 320000, "path": "/m/u/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ], album="Untagged", albumartist="Some Artist")
        with BeetsDB(self.db_path) as db:
            loc = db.locate("no-id-at-all")
        self.assertEqual(loc.kind, "absent")
        self.assertIsNone(loc.album_id)
        self.assertEqual(loc.selectors, ())

    def test_enumerate_all_same_mbid_single_row(self) -> None:
        """One album → single-element list. The common case.

        Pins the contract used by ``import_one.main`` pre-import: if
        the release is present exactly once, ``stale_ids = [id]`` and
        post-import cleanup removes that id by PK.
        """
        with BeetsDB(self.db_path) as db:
            ids = db.get_all_album_ids_for_release(
                "aaa0bbb0-cccc-dddd-eeee-ffffffffffff")
        self.assertEqual(ids, [1])

    def test_enumerate_all_same_mbid_multi_row_split_brain(self) -> None:
        """Two rows with same MBID → both ids returned.

        Regression guard for Codex PR #131 round 3 P2: the earlier
        ``locate()``-based capture picked up just one row via LIMIT 1,
        so cleanup deleted the first but left the second behind.
        ``main`` now enumerates and fails fast if len > 1 — operator
        must reduce to one row before re-running.
        """
        # Insert a second row with the same MBID as album 1.
        _insert_album_full(self.db_path, 99,
                           "aaa0bbb0-cccc-dddd-eeee-ffffffffffff", [
                               {"bitrate": 192000, "path": "/m/dup/01.mp3",
                                "format": "MP3", "samplerate": 44100,
                                "bitdepth": 0},
                           ], album="OK Computer",
                           albumartist="Radiohead")
        with BeetsDB(self.db_path) as db:
            ids = db.get_all_album_ids_for_release(
                "aaa0bbb0-cccc-dddd-eeee-ffffffffffff")
        self.assertEqual(sorted(ids), [1, 99])

    def test_enumerate_all_same_mbid_absent(self) -> None:
        """No match → empty list, not None."""
        with BeetsDB(self.db_path) as db:
            ids = db.get_all_album_ids_for_release("zzz-not-present")
        self.assertEqual(ids, [])

    def test_enumerate_all_same_mbid_discogs_dual_layout(self) -> None:
        """Discogs numeric → both new-layout and legacy rows returned.

        The enumeration must cover both columns for the same reason
        ``locate()``'s selector tuple does: a library that has some
        rows in ``discogs_albumid`` and some in ``mb_albumid`` (mid-
        migration) is a valid state, and cleanup needs to see every
        row or silently leaves one behind.
        """
        # Insert another Discogs row under the legacy mb_albumid column
        # with the same numeric id as album 2 (which is under
        # discogs_albumid).
        _insert_album_full(self.db_path, 98, "12856590", [
            {"bitrate": 320000, "path": "/m/disc_legacy/01.mp3",
             "format": "MP3", "samplerate": 44100, "bitdepth": 0},
        ], album="New Ritual (legacy press)", albumartist="DICE")
        with BeetsDB(self.db_path) as db:
            ids = db.get_all_album_ids_for_release("12856590")
        self.assertEqual(sorted(ids), [2, 98])

    def test_enumerate_all_same_mbid_empty_release_id(self) -> None:
        """Empty release_id short-circuits to ``[]`` (caller safety)."""
        with BeetsDB(self.db_path) as db:
            ids = db.get_all_album_ids_for_release("")
        self.assertEqual(ids, [])

    def test_locate_rejects_artist_album_kwargs(self) -> None:
        """``locate`` takes only a release_id — no fuzzy escape hatch.

        Issue #123: the old signature accepted optional artist/album
        kwargs to drive the fuzzy fallback. Removing the fallback means
        the kwargs are dead weight that would invite future callers to
        re-introduce the bug. Passing them now is a TypeError.
        """
        # Cast to Any so the test's runtime TypeError assertion is the
        # guard — without the cast, pyright statically rejects the call
        # (which is also desired, just not what this runtime test is
        # proving).
        from typing import Any
        with BeetsDB(self.db_path) as db:
            locate_any: Any = db.locate
            with self.assertRaises(TypeError):
                locate_any("no-id-at-all",
                           artist="Some Artist",
                           album="Untagged")

    def test_locate_numeric_but_not_in_either_column(self) -> None:
        """Numeric ID with no exact hit → absent."""
        with BeetsDB(self.db_path) as db:
            loc = db.locate("99999999")
        self.assertEqual(loc.kind, "absent")
        self.assertEqual(loc.selectors, ())

    def test_release_location_kind_literal_is_exact_or_absent(self) -> None:
        """``ReleaseLocation.kind`` is narrowed to 2 states (issue #123).

        Pyright enforces the Literal at static time; this runtime guard
        asserts the ``__args__`` of the annotation so a future
        well-meaning contributor can't re-add ``"fuzzy"`` as a valid
        value without tripping the test suite.
        """
        from typing import get_args, get_type_hints
        from lib.beets_db import ReleaseLocation
        hints = get_type_hints(ReleaseLocation)
        kind_args = get_args(hints["kind"])
        self.assertEqual(set(kind_args), {"exact", "absent"})


class TestCheckMbidsDiscogsAware(unittest.TestCase):
    """Batch MBID existence check must handle Discogs IDs too.

    ``check_mbids`` was a latent bug: it only queried ``mb_albumid``,
    so Discogs releases with a numeric ID in ``discogs_albumid``
    disappeared from every browse route that marks "already in library"
    (release-group, master, artist discography). Downstream symptom:
    Discogs releases showed an "Add" button even when the exact
    pressing was already on disk. Routes through the ``locate`` seam
    after issue #121.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        # Normal UUID
        _insert_album_full(self.db_path, 1, "aaa-111", [
            {"bitrate": 320000, "path": "/m/a/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ])
        # Discogs numeric in discogs_albumid (new layout)
        _insert_album_full(self.db_path, 2, "", [
            {"bitrate": 1411000, "path": "/m/d/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ], discogs_albumid=12856590)
        # Discogs numeric in mb_albumid (legacy layout)
        _insert_album_full(self.db_path, 3, "5555555", [
            {"bitrate": 320000, "path": "/m/l/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ])

    def test_check_mbids_detects_new_layout_discogs(self) -> None:
        with BeetsDB(self.db_path) as db:
            found = db.check_mbids(["aaa-111", "12856590", "zzz-999"])
        self.assertEqual(found, {"aaa-111", "12856590"})

    def test_check_mbids_detects_legacy_discogs(self) -> None:
        with BeetsDB(self.db_path) as db:
            found = db.check_mbids(["5555555"])
        self.assertEqual(found, {"5555555"})

    def test_check_mbids_mixed_batch(self) -> None:
        """Single batch mixing UUID, new-layout numeric, legacy numeric."""
        with BeetsDB(self.db_path) as db:
            found = db.check_mbids(["aaa-111", "12856590", "5555555", "99"])
        self.assertEqual(found, {"aaa-111", "12856590", "5555555"})


class TestBatchLookupAlbumIds(unittest.TestCase):
    """``_batch_lookup_album_ids`` is the shared batched seam for
    ``check_mbids`` + ``get_album_ids_by_mbids`` (issue #121 / Codex
    round 2). Two invariants: same ID shapes as ``locate``, and
    strictly ≤2 SQL queries regardless of input size — no N+1.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        _insert_album(self.db_path, 1, "aaa-111", [(320000, "/a.mp3")])
        _insert_album(self.db_path, 2, "bbb-222", [(320000, "/b.mp3")])
        _insert_album_full(self.db_path, 3, "", [
            {"bitrate": 1411000, "path": "/m/d/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ], discogs_albumid=12856590)
        _insert_album_full(self.db_path, 4, "5555555", [
            {"bitrate": 320000, "path": "/m/l/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ])
        _insert_album_full(self.db_path, 5, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", [
            {"bitrate": 320000, "path": "/m/u/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ])

    def test_resolves_mixed_batch(self) -> None:
        with BeetsDB(self.db_path) as db:
            result = db._batch_lookup_album_ids(
                ["aaa-111", "bbb-222", "12856590", "5555555", "absent-id"])
        self.assertEqual(result, {
            "aaa-111": 1, "bbb-222": 2,
            "12856590": 3, "5555555": 4,
        })

    def test_resolves_normalized_batch_inputs(self) -> None:
        with BeetsDB(self.db_path) as db:
            result = db._batch_lookup_album_ids(
                [" AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA ", "0012856590"])
        self.assertEqual(result, {
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa": 5,
            "12856590": 3,
        })

    def test_uses_at_most_two_queries(self) -> None:
        """No N+1: regardless of batch size, at most 2 SQL round-trips
        (one ``mb_albumid IN (...)``, one ``discogs_albumid IN (...)``).

        This is the Codex round 2 latency guard — the browse overlays
        call ``check_beets_library`` on whole release-group result sets,
        so a per-ID loop would add hundreds of round-trips on large
        artist pages.
        """
        calls: list[str] = []

        class _TrackingConn:
            def __init__(self, real: sqlite3.Connection) -> None:
                self._real = real

            def execute(self, sql: str, *args: object, **kwargs: object):
                calls.append(sql)
                return self._real.execute(sql, *args, **kwargs)  # type: ignore[arg-type]

            def close(self) -> None:
                self._real.close()

        with BeetsDB(self.db_path) as db:
            db._conn = _TrackingConn(db._conn)  # type: ignore[assignment]
            db._batch_lookup_album_ids(
                ["aaa-111", "bbb-222", "12856590", "5555555",
                 "missing-1", "missing-2", "missing-3"])

        self.assertLessEqual(
            len(calls), 2,
            f"_batch_lookup_album_ids must issue at most 2 queries, "
            f"got {len(calls)}: {calls}")

    def test_empty_input(self) -> None:
        with BeetsDB(self.db_path) as db:
            self.assertEqual(db._batch_lookup_album_ids([]), {})


class TestPostflightLookupsSupportDiscogs(unittest.TestCase):
    """Regression guard: ``album_exists`` understands Discogs IDs, so the
    postflight lookups called during import (``get_album_info``,
    ``get_min_bitrate``, ``get_album_path``, ``get_item_paths``) must
    agree — otherwise ``import_dispatch`` sees a preflight hit with an
    empty postflight, marks the import successful against vanished
    metadata, and persists a stale ``imported_path``.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        # New-layout Discogs entry (numeric in discogs_albumid).
        _insert_album_full(self.db_path, 1, "", [
            {"bitrate": 320000, "path": "/m/disc/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
            {"bitrate": 256000, "path": "/m/disc/02.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ], discogs_albumid=12856590)
        # Legacy-layout Discogs entry (numeric in mb_albumid).
        _insert_album_full(self.db_path, 2, "5555555", [
            {"bitrate": 1411000, "path": "/m/legacy/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ])

    def test_get_min_bitrate_resolves_discogs(self) -> None:
        with BeetsDB(self.db_path) as db:
            self.assertEqual(db.get_min_bitrate("12856590"), 256)
            self.assertEqual(db.get_min_bitrate("5555555"), 1411)

    def test_get_album_path_resolves_discogs(self) -> None:
        with BeetsDB(self.db_path) as db:
            self.assertEqual(db.get_album_path("12856590"), "/m/disc")
            self.assertEqual(db.get_album_path("5555555"), "/m/legacy")

    def test_get_item_paths_resolves_discogs(self) -> None:
        with BeetsDB(self.db_path) as db:
            paths = db.get_item_paths("12856590")
        self.assertEqual(len(paths), 2)
        self.assertTrue(all(p.startswith("/m/disc/") for _, p in paths))

    def test_get_album_info_resolves_discogs(self) -> None:
        from lib.quality import QualityRankConfig
        cfg = QualityRankConfig.defaults()
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("12856590", cfg)
        self.assertIsNotNone(info)
        assert info is not None
        self.assertEqual(info.track_count, 2)
        self.assertEqual(info.min_bitrate_kbps, 256)


class TestGetAlbumInfo(unittest.TestCase):
    """Test get_album_info (postflight verify + quality gate data)."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        _create_test_db(self.db_path)
        from lib.quality import QualityRankConfig
        self.cfg = QualityRankConfig.defaults()

    def test_single_album(self) -> None:
        _insert_album(self.db_path, 1, "abc-123", [
            (320000, "/music/Artist/Album/01.mp3"),
            (320000, "/music/Artist/Album/02.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("abc-123", self.cfg)
        assert info is not None
        self.assertEqual(info.album_id, 1)
        self.assertEqual(info.track_count, 2)
        self.assertEqual(info.min_bitrate_kbps, 320)
        self.assertEqual(info.avg_bitrate_kbps, 320)
        self.assertEqual(info.median_bitrate_kbps, 320)
        self.assertTrue(info.is_cbr)
        self.assertEqual(info.album_path, "/music/Artist/Album")
        self.assertEqual(info.format, "MP3")

    def test_vbr_album(self) -> None:
        _insert_album(self.db_path, 2, "def-456", [
            (245000, "/music/A/B/01.mp3"),
            (238000, "/music/A/B/02.mp3"),
            (251000, "/music/A/B/03.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("def-456", self.cfg)
        assert info is not None
        self.assertEqual(info.min_bitrate_kbps, 238)
        self.assertEqual(info.avg_bitrate_kbps, 244)  # (245+238+251)/3 = 244.66 → 244
        # Median of {238, 245, 251} = 245
        self.assertEqual(info.median_bitrate_kbps, 245)
        self.assertFalse(info.is_cbr)
        self.assertEqual(info.track_count, 3)
        self.assertEqual(info.format, "MP3")

    def test_median_resists_outliers(self) -> None:
        """Median ignores a single very-low track that would tank min/avg.

        Issue #64: a V0 album with one quiet 60kbps interlude should still
        classify as TRANSPARENT under the MEDIAN rank metric. The pure
        rank classification is unit-tested in test_quality_decisions; here
        we just verify BeetsDB computes the median field correctly.
        """
        _insert_album(self.db_path, 8, "median-1", [
            ( 60000, "/m/M/00.mp3"),  # silent intro
            (245000, "/m/M/01.mp3"),
            (250000, "/m/M/02.mp3"),
            (255000, "/m/M/03.mp3"),
            (260000, "/m/M/04.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("median-1", self.cfg)
        assert info is not None
        self.assertEqual(info.min_bitrate_kbps, 60)
        # avg = (60+245+250+255+260)/5 = 214 → int(214) = 214
        self.assertEqual(info.avg_bitrate_kbps, 214)
        # median of 5 sorted values {60, 245, 250, 255, 260} = 250
        self.assertEqual(info.median_bitrate_kbps, 250)
        self.assertFalse(info.is_cbr)

    def test_median_even_track_count(self) -> None:
        """statistics.median() averages the two middle values for even counts."""
        _insert_album(self.db_path, 9, "median-2", [
            (200000, "/m/E/01.mp3"),
            (220000, "/m/E/02.mp3"),
            (240000, "/m/E/03.mp3"),
            (260000, "/m/E/04.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("median-2", self.cfg)
        assert info is not None
        # median of {200, 220, 240, 260} = (220+240)/2 = 230
        self.assertEqual(info.median_bitrate_kbps, 230)

    def test_not_found(self) -> None:
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("nonexistent", self.cfg)
        self.assertIsNone(info)

    def test_album_no_tracks(self) -> None:
        """Album exists but no items — should return None."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO albums (id, mb_albumid) VALUES (5, 'empty-1')")
        conn.commit()
        conn.close()
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("empty-1", self.cfg)
        self.assertIsNone(info)

    def test_zero_bitrate_ignored(self) -> None:
        """Tracks with 0 bitrate should be treated as no data."""
        _insert_album(self.db_path, 3, "ghi-789", [
            (0, "/music/A/B/01.mp3"),
            (256000, "/music/A/B/02.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("ghi-789", self.cfg)
        assert info is not None
        self.assertEqual(info.min_bitrate_kbps, 256)

    def test_path_as_bytes(self) -> None:
        """Beets stores paths as bytes — should decode correctly."""
        _insert_album(self.db_path, 4, "jkl-012", [
            (320000, "/music/Ärtiöst/Albüm/01.mp3"),
        ])
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("jkl-012", self.cfg)
        assert info is not None
        self.assertIn("Albüm", info.album_path)

    def test_opus_album_format(self) -> None:
        """Opus tracks report format='Opus'."""
        _insert_album(self.db_path, 5, "opus-1", [
            (128000, "/m/O/01.opus"),
            (120000, "/m/O/02.opus"),
            (135000, "/m/O/03.opus"),
        ], track_format="Opus")
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("opus-1", self.cfg)
        assert info is not None
        self.assertEqual(info.format, "Opus")
        self.assertEqual(info.min_bitrate_kbps, 120)
        self.assertEqual(info.avg_bitrate_kbps, 127)  # (128+120+135)/3 = 127.66 → 127
        self.assertFalse(info.is_cbr)

    def test_flac_album_format(self) -> None:
        """FLAC tracks report format='FLAC'."""
        _insert_album(self.db_path, 6, "flac-1", [
            (900000, "/m/F/01.flac"),
        ], track_format="FLAC")
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("flac-1", self.cfg)
        assert info is not None
        self.assertEqual(info.format, "FLAC")

    def test_mixed_format_album_reduces_via_precedence(self) -> None:
        """Mixed-format album picks worst codec per cfg.mixed_format_precedence."""
        # Insert manually because _insert_album uses a single format per album
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO albums (id, mb_albumid) VALUES (7, 'mixed-1')")
        conn.execute(
            "INSERT INTO items (album_id, bitrate, path, format) "
            "VALUES (?, ?, ?, ?)",
            (7, 1000000, b"/m/mix/01.flac", "FLAC"))
        conn.execute(
            "INSERT INTO items (album_id, bitrate, path, format) "
            "VALUES (?, ?, ?, ?)",
            (7, 245000, b"/m/mix/02.mp3", "MP3"))
        conn.commit()
        conn.close()
        with BeetsDB(self.db_path) as db:
            info = db.get_album_info("mixed-1", self.cfg)
        assert info is not None
        # Default precedence is ("mp3", "aac", "opus", "flac") — MP3 wins.
        self.assertEqual(info.format, "MP3")


class TestReduceAlbumFormat(unittest.TestCase):
    """Direct unit tests for _reduce_album_format — pure function, no DB."""

    def setUp(self) -> None:
        from lib.quality import QualityRankConfig
        self.cfg = QualityRankConfig.defaults()

    def test_single_format_passes_through(self) -> None:
        from lib.beets_db import _reduce_album_format
        self.assertEqual(_reduce_album_format({"MP3"}, self.cfg), "MP3")

    def test_empty_set_returns_empty_string(self) -> None:
        from lib.beets_db import _reduce_album_format
        self.assertEqual(_reduce_album_format(set(), self.cfg), "")

    def test_alphabetical_fallback_when_no_precedence_match(self) -> None:
        """Unknown codecs fall back to sorted()[0]."""
        from lib.beets_db import _reduce_album_format
        # Default precedence: ("mp3", "aac", "opus", "flac") — neither matches
        self.assertEqual(
            _reduce_album_format({"Vorbis", "WAV"}, self.cfg), "Vorbis")

    def test_precedence_beats_alphabetical(self) -> None:
        """A precedence-match wins over an alphabetically earlier unknown codec."""
        from lib.beets_db import _reduce_album_format
        # "AAC" is earlier alphabetically than "Vorbis" AND is in precedence
        self.assertEqual(
            _reduce_album_format({"Vorbis", "AAC"}, self.cfg), "AAC")

    def test_case_insensitive_precedence_match(self) -> None:
        """Lowercase beets format ("flac") still matches precedence."""
        from lib.beets_db import _reduce_album_format
        self.assertEqual(
            _reduce_album_format({"flac"}, self.cfg), "flac")
        # Mixed case
        self.assertEqual(
            _reduce_album_format({"flac", "mp3"}, self.cfg), "mp3")

    def test_three_way_mix(self) -> None:
        """{FLAC, Opus, AAC} → AAC (first precedence match)."""
        from lib.beets_db import _reduce_album_format
        self.assertEqual(
            _reduce_album_format({"FLAC", "Opus", "AAC"}, self.cfg), "AAC")


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

    def test_discogs_numeric_id_matches_discogs_albumid(self) -> None:
        """Discogs-sourced releases are stored in beets under
        ``albums.discogs_albumid`` (INTEGER), not ``mb_albumid``. The
        pipeline DB packs both kinds of identifier into ``mb_release_id``
        — a UUID for MusicBrainz, a numeric string for Discogs — so the
        detail lookup must round-trip the numeric string back to the
        right beets column. Without this, Discogs wrong-matches lose
        their quality summary and regress to "nothing on disk".
        """
        _insert_album_full(self.db_path, 10, "", [
            {"bitrate": 1411000, "path": "/m/disc/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ], discogs_albumid=12856590)

        with BeetsDB(self.db_path) as db:
            detail = db.check_mbids_detail(["12856590"])

        self.assertIn("12856590", detail)
        self.assertEqual(detail["12856590"]["beets_tracks"], 1)
        self.assertEqual(detail["12856590"]["beets_format"], "FLAC")

    def test_mixed_mbid_and_discogs_ids(self) -> None:
        """A single batch can contain both UUID and numeric IDs — e.g.
        the web UI renders a grid that contains both sources at once.
        """
        _insert_album_full(self.db_path, 11, "", [
            {"bitrate": 320000, "path": "/m/disc/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ], discogs_albumid=99)

        with BeetsDB(self.db_path) as db:
            detail = db.check_mbids_detail(["aaa-111", "99"])

        self.assertIn("aaa-111", detail)
        self.assertIn("99", detail)
        self.assertEqual(detail["99"]["beets_format"], "MP3")

    def test_legacy_discogs_numeric_in_mb_albumid(self) -> None:
        """Legacy Discogs imports stored the numeric ID as TEXT in
        ``mb_albumid``; ``check_mbids_detail`` must still return those
        rows so the web UI doesn't blank real quality data (and then
        render "different edition on disk" for a release that's
        actually the exact pressing on disk).
        """
        _insert_album_full(self.db_path, 12, "5555555", [
            {"bitrate": 1411000, "path": "/m/legacy/01.flac", "format": "FLAC",
             "samplerate": 44100, "bitdepth": 16},
        ])

        with BeetsDB(self.db_path) as db:
            detail = db.check_mbids_detail(["5555555"])

        self.assertIn("5555555", detail)
        self.assertEqual(detail["5555555"]["beets_format"], "FLAC")
        self.assertEqual(detail["5555555"]["beets_bitdepth"], 16)


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
        tracks = detail["tracks"]
        assert isinstance(tracks, list)
        self.assertEqual(len(tracks), 2)
        self.assertEqual(tracks[0]["title"], "Track 1")

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


class TestFuzzyMethodsRemoved(unittest.TestCase):
    """Issue #123: ``find_by_artist_album`` / ``_fuzzy_album_id`` deleted.

    The fuzzy presence path conflated identity (which pressing?) with
    presence (is anything by this artist here?) and silently attributed
    quality to sibling pressings. These methods were the last entry
    points into that path. Guard against accidental reintroduction.
    """

    def test_find_by_artist_album_no_longer_exists(self) -> None:
        from lib.beets_db import BeetsDB
        self.assertFalse(
            hasattr(BeetsDB, "find_by_artist_album"),
            "find_by_artist_album was deleted in issue #123 "
            "— fuzzy presence checks must not return.",
        )

    def test_fuzzy_album_id_no_longer_exists(self) -> None:
        from lib.beets_db import BeetsDB
        self.assertFalse(
            hasattr(BeetsDB, "_fuzzy_album_id"),
            "_fuzzy_album_id was deleted in issue #123 "
            "— fuzzy LIKE query must not return.",
        )


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

    def test_resolves_discogs_new_layout(self) -> None:
        """Codex round 1: ``get_album_ids_by_mbids`` MUST stay in sync
        with ``check_mbids`` now that both route through ``locate``.

        Before the fix, ``check_mbids`` reported Discogs releases as
        present (correct), but ``get_album_ids_by_mbids`` silently
        returned an empty mapping for them — so the browse routes
        would emit ``in_library=true`` with ``beets_album_id=null``
        and the frontend's 'Remove from beets' button would disable
        for the very rows the presence check just surfaced.
        """
        _insert_album_full(self.db_path, 99, "", [
            {"bitrate": 320000, "path": "/m/d/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ], discogs_albumid=12856590)

        with BeetsDB(self.db_path) as db:
            result = db.get_album_ids_by_mbids(
                ["aaa-111", "12856590", "zzz-999"])
        self.assertEqual(result, {"aaa-111": 1, "12856590": 99})

    def test_resolves_discogs_legacy_mb_albumid(self) -> None:
        """Legacy Discogs imports (numeric in ``mb_albumid``) must also
        resolve so the mapping stays consistent with ``check_mbids``.
        """
        _insert_album_full(self.db_path, 88, "5555555", [
            {"bitrate": 320000, "path": "/m/l/01.mp3", "format": "MP3",
             "samplerate": 44100, "bitdepth": 0},
        ])

        with BeetsDB(self.db_path) as db:
            result = db.get_album_ids_by_mbids(["5555555"])
        self.assertEqual(result, {"5555555": 88})


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
        _insert_album(self.db_path, 1, "aaa0bbb0-cccc-dddd-eeee-ffffffffffff", [(320000, "/a.mp3")],
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
        self.assertEqual(discogs[0]["discogs_albumid"], "67890")

    def test_discogs_zero_sentinel_normalized_away(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO albums (id, mb_albumid, album, albumartist, discogs_albumid) "
            "VALUES (3, '', 'Unknown Album', 'Artist', 0)"
        )
        conn.execute(
            "INSERT INTO items (album_id, bitrate, path) VALUES (3, 192000, X'2F632E6D7033')"
        )
        conn.commit()
        conn.close()

        with BeetsDB(self.db_path) as db:
            albums = db.get_albums_by_artist("Artist")
        unknown = [a for a in albums if a["album"] == "Unknown Album"]
        self.assertEqual(len(unknown), 1)
        self.assertIsNone(unknown[0]["mb_albumid"])
        self.assertIsNone(unknown[0]["discogs_albumid"])
        self.assertEqual(unknown[0]["source"], "unknown")


if __name__ == "__main__":
    unittest.main()
