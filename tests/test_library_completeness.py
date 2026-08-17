"""Pins for the independent source/catalog/files completeness classifier."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest

from mediafile.exceptions import UnreadableFileError

from lib.beets_db import BeetsDB
from lib.library_completeness import (
    AudioTagReadError,
    CatalogItem,
    LibraryAlbum,
    SourceManifest,
    SourceManifestError,
    classify_album,
    discogs_manifest,
    enumerate_audio_files,
    musicbrainz_manifest,
    read_audio_tag_identities,
    scan_library_completeness,
)
from lib.mb_canonical import (
    CanonicalReleaseAnswer,
    CanonicalReleaseCurrent,
    CanonicalReleaseRedirected,
    CanonicalReleaseUnavailable,
)
from lib.release_identity import ReleaseIdentity


def _discogs_raw(
    positions: list[str], release_id: str | int = "1",
) -> dict[str, object]:
    return {
        "id": release_id,
        "tracks": [{"position": position, "title": position} for position in positions],
    }


def _mb_raw(
    rows: list[tuple[str, str, str, bool]], release_id: str = "release",
) -> dict[str, object]:
    return {"id": release_id, "media": [{"tracks": [
        {"id": release_track, "title": title,
         "recording": {"id": recording, "video": video}}
        for release_track, recording, title, video in rows
    ]}]}


class TestLiveIncidentPins(unittest.TestCase):
    def _classify(self, album: LibraryAlbum, raw: dict[str, object], *, tags: dict[str, tuple[str, str]]) -> set[str]:
        assert album.identity is not None
        raw = {**raw, "id": album.identity.release_id}
        manifest = (discogs_manifest(album.identity.release_id, raw)
                    if album.identity.source == "discogs"
                    else musicbrainz_manifest(album.identity.release_id, raw))
        result = classify_album(
            album, manifest,
            enumerate_files=lambda _directory: tuple(sorted(tags)),
            tag_reader=lambda path: tags[path],
        )
        return {finding.kind for finding in result.findings}

    def test_bowie_discogs_subtrack_is_missing_and_space_oddity_is_drift(self) -> None:
        release = "2823685"
        paths = {f"/album/{position}.opus": (f"{release}-{position}", "")
                 for position in ("A2.1", "A3", "A4", "B1", "B2", "B3", "B4", "B5")}
        paths["/album/Space Oddity.opus"] = (f"{release}-A1", "")
        album = LibraryAlbum(11782, "David Bowie", "David Bowie", ReleaseIdentity("discogs", release), "/album",
                             tuple(CatalogItem(path, tag[0], "") for path, tag in paths.items() if "Space" not in path))
        kinds = self._classify(album, _discogs_raw(["A1", "A2.1", "A2.2", "A3", "A4", "B1", "B2", "B3", "B4", "B5"]), tags=paths)
        self.assertEqual(kinds, {"missing_source_audio", "catalog_drift"})

    def test_dirt_discogs_1b_is_missing_without_drift(self) -> None:
        release = "4738671"
        paths = {f"/album/{position}.opus": (f"{release}-{position}", "")
                 for position in ("1A", "2A", "3A", "4A")}
        album = LibraryAlbum(1255, "Dirt Dress", "Theme Songs", ReleaseIdentity("discogs", release), "/album",
                             tuple(CatalogItem(path, tag[0], "") for path, tag in paths.items()))
        kinds = self._classify(album, _discogs_raw(["1A", "2A", "3A", "4A", "1B"]), tags=paths)
        self.assertEqual(kinds, {"missing_source_audio"})

    def test_uncatalogued_discogs_file_uses_mb_trackid_when_release_track_tag_blank(self) -> None:
        release = "4738671"
        paths = {"/album/01.opus": ("", f"{release}-1A")}
        album = LibraryAlbum(1255, "Dirt Dress", "Theme Songs", ReleaseIdentity("discogs", release), "/album", ())
        self.assertEqual(self._classify(album, _discogs_raw(["1A"]), tags=paths), {"catalog_drift"})

    def test_uncatalogued_discogs_mb_trackid_wins_over_stale_release_track_tag(self) -> None:
        release = "4738671"
        paths = {"/album/01.opus": ("stale-release-track", f"{release}-1A")}
        album = LibraryAlbum(1255, "Dirt Dress", "Theme Songs", ReleaseIdentity("discogs", release), "/album", ())
        self.assertEqual(self._classify(album, _discogs_raw(["1A"]), tags=paths), {"catalog_drift"})

    def test_uncatalogued_discogs_conflicting_valid_tag_keys_are_unknown(self) -> None:
        release = "4738671"
        paths = {"/album/01.opus": (f"{release}-1A", f"{release}-2A")}
        album = LibraryAlbum(1255, "Dirt Dress", "Theme Songs", ReleaseIdentity("discogs", release), "/album", ())
        self.assertEqual(
            self._classify(album, _discogs_raw(["1A", "2A"]), tags=paths),
            {"catalog_drift", "unknown"},
        )

    def test_adventure_untracked_mb_track_satisfies_source_but_is_drift(self) -> None:
        release = "ece1f7a2-7f59-4eaa-b109-23743ba633bb"
        rows = [(f"rt-{n}", f"rec-{n}", f"Track {n}", False) for n in range(1, 12)]
        paths = {f"/album/{n:02}.mp3": (f"rt-{n}", f"rec-{n}")
                 for n in (*range(1, 4), *range(5, 12))}
        paths["/album/04 In Between.mp3"] = ("rt-4", "rec-4")
        catalog = tuple(CatalogItem(path, tag[0], tag[1]) for path, tag in paths.items() if "Between" not in path)
        album = LibraryAlbum(1433, "...soihadto...", "Adventure Stories", ReleaseIdentity("musicbrainz", release), "/album", catalog)
        kinds = self._classify(album, _mb_raw(rows), tags=paths)
        self.assertEqual(kinds, {"catalog_drift"})

    def test_omitted_mb_video_is_ignored_not_missing_audio(self) -> None:
        release = "video-release"
        paths = {"/album/audio.flac": ("audio-rt", "audio-rec")}
        album = LibraryAlbum(7, "Artist", "Video album", ReleaseIdentity("musicbrainz", release), "/album",
                             (CatalogItem("/album/audio.flac", "audio-rt", "audio-rec"),))
        kinds = self._classify(album, _mb_raw([
            ("audio-rt", "audio-rec", "Audio", False),
            ("video-rt", "video-rec", "Video", True),
        ]), tags=paths)
        self.assertEqual(kinds, set())

    def test_whole_program_mb_identity_churn_uses_safe_global_coordinate_control(self) -> None:
        release = "d87dbe79-82f7-4055-b9d4-379cef3f9bdd"
        rows = [(f"new-{n}", f"new-rec-{n}", title, False) for n, title in enumerate(("We Know the Way", "How Far I'll Go"), 1)]
        paths = {"/album/01.flac": ("old-1", "old-rec-1"), "/album/02.flac": ("old-2", "old-rec-2")}
        album = LibraryAlbum(11541, "Moana", "Moana Deluxe", ReleaseIdentity("musicbrainz", release), "/album", (
            CatalogItem("/album/01.flac", "old-1", "old-rec-1", "We Know The Way - From Vaiana", 1),
            CatalogItem("/album/02.flac", "old-2", "old-rec-2", "How Far I'll Go", 2),
        ))
        self.assertEqual(self._classify(album, _mb_raw(rows), tags=paths), set())

    def _mb_churn_world(self, track_count: int, mismatches: set[int]) -> set[str]:
        release = "long-churn-release"
        source_titles = ["Shiny", *(f"Track {index}" for index in range(2, track_count + 1))]
        rows = [(f"new-{index}", f"new-rec-{index}", title, False)
                for index, title in enumerate(source_titles, 1)]
        paths = {f"/album/{index:02}.flac": (f"old-{index}", f"old-rec-{index}")
                 for index in range(1, track_count + 1)}
        catalog = tuple(CatalogItem(
            path, tags[0], tags[1],
            f"Different {index}" if index in mismatches else source_titles[index - 1],
            index,
        ) for index, (path, tags) in enumerate(paths.items(), 1))
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", release), "/album", catalog)
        return self._classify(album, _mb_raw(rows), tags=paths)

    def test_long_mb_churn_tolerates_one_title_variant(self) -> None:
        # 59 source coordinates mirror the live Moana shape. "Shiny" also
        # pins that an exact one-word title is valid corroboration.
        self.assertEqual(self._mb_churn_world(59, {13}), set())

    def test_long_mb_churn_rejects_two_title_variants(self) -> None:
        self.assertEqual(self._mb_churn_world(59, {13, 42}), {"unknown"})

    def test_short_mb_churn_rejects_one_title_variant(self) -> None:
        self.assertEqual(self._mb_churn_world(2, {2}), {"unknown"})

    def test_discogs_never_uses_musicbrainz_program_fallback(self) -> None:
        release = "4738671"
        album = LibraryAlbum(1255, "Dirt Dress", "Theme Songs", ReleaseIdentity("discogs", release), "/album", (
            CatalogItem("/album/01.opus", "historical-1", "", "1A", 1),
            CatalogItem("/album/02.opus", "historical-2", "", "2A", 2),
        ))
        manifest = discogs_manifest(release, _discogs_raw(["1A", "2A"], release))
        result = classify_album(
            album, manifest,
            enumerate_files=lambda _directory: ("/album/01.opus", "/album/02.opus"),
        )
        self.assertEqual({finding.kind for finding in result.findings}, {"missing_source_audio"})

    def test_unproven_whole_program_mb_churn_is_unknown_not_missing(self) -> None:
        release = "churn-release"
        rows = [("new-1", "new-rec-1", "One", False), ("new-2", "new-rec-2", "Two", False)]
        paths = {"/album/01.flac": ("old-1", "old-rec-1"), "/album/02.flac": ("old-2", "old-rec-2")}
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", release), "/album", (
            CatalogItem("/album/01.flac", "old-1", "old-rec-1", "Not One", 1),
            CatalogItem("/album/02.flac", "old-2", "old-rec-2", "Not Two", 2),
        ))
        kinds = self._classify(album, _mb_raw(rows), tags=paths)
        self.assertEqual(kinds, {"unknown"})

    def test_mb_missing_is_definite_when_no_stale_identity_witness_exists(self) -> None:
        release = "missing-release"
        rows = [("rt-1", "rec-1", "One", False), ("rt-2", "rec-2", "Two", False)]
        paths = {"/album/01.flac": ("rt-1", "rec-1")}
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", release), "/album", (
            CatalogItem("/album/01.flac", "rt-1", "rec-1", "One", 1),
        ))
        self.assertEqual(self._classify(album, _mb_raw(rows), tags=paths), {"missing_source_audio"})

    def test_stale_mb_identity_on_uncatalogued_extra_is_unknown_not_missing(self) -> None:
        release = "stale-extra-release"
        rows = [("rt-1", "rec-1", "One", False), ("rt-2", "rec-2", "Two", False)]
        paths = {"/album/01.flac": ("rt-1", "rec-1"), "/album/02.flac": ("old-2", "old-rec-2")}
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", release), "/album", (
            CatalogItem("/album/01.flac", "rt-1", "rec-1", "One", 1),
        ))
        kinds = self._classify(album, _mb_raw(rows), tags=paths)
        self.assertEqual(kinds, {"catalog_drift", "unknown"})

    def test_blank_tag_mb_extra_is_unknown_not_missing(self) -> None:
        release = "blank-extra-release"
        rows = [("rt-1", "rec-1", "One", False), ("rt-2", "rec-2", "Two", False)]
        paths = {"/album/01.flac": ("rt-1", "rec-1"), "/album/02.flac": ("", "")}
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", release), "/album", (
            CatalogItem("/album/01.flac", "rt-1", "rec-1", "One", 1),
        ))
        kinds = self._classify(album, _mb_raw(rows), tags=paths)
        self.assertEqual(kinds, {"catalog_drift", "unknown"})

    def test_unknown_extra_cannot_make_missing_definite(self) -> None:
        release = "1"
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("discogs", release), "/album", ())
        manifest = discogs_manifest(release, _discogs_raw(["A1"]))
        result = classify_album(album, manifest, enumerate_files=lambda _path: ("/album/x.flac",),
                                tag_reader=lambda _path: (_ for _ in ()).throw(AudioTagReadError("bad tag")))
        self.assertIn("unknown", {finding.kind for finding in result.findings})
        self.assertNotIn("missing_source_audio", {finding.kind for finding in result.findings})

    def test_missing_directory_is_unknown_not_empty(self) -> None:
        manifest = discogs_manifest("1", _discogs_raw(["A1"]))
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("discogs", "1"), "/does/not/exist", ())
        result = classify_album(album, manifest)
        self.assertEqual(result.findings[0].kind, "unknown")


class TestSourceRawContracts(unittest.TestCase):
    def test_direct_empty_manifest_is_unknown_not_complete(self) -> None:
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("discogs", "1"), "/album", ())
        result = classify_album(
            album, SourceManifest("discogs", "1", ()),
            enumerate_files=lambda _path: (),
        )
        self.assertEqual({finding.kind for finding in result.findings}, {"unknown"})

    def test_mb_pregap_is_a_source_component(self) -> None:
        raw = {"id": "release", "media": [{"pregap": {"id": "pg", "title": "Pregap", "recording": {"id": "rpg", "video": False}}, "tracks": []}]}
        self.assertEqual(musicbrainz_manifest("release", raw).components[0].key, "pg")

    def test_musicbrainz_top_level_release_identity_must_match(self) -> None:
        for raw_id in (None, 1, "other-release"):
            with self.subTest(raw_id=raw_id):
                raw = _mb_raw([("rt", "rec", "Track", False)])
                raw["id"] = raw_id
                with self.assertRaisesRegex(SourceManifestError, "release identity"):
                    musicbrainz_manifest("release", raw)

    def test_discogs_top_level_release_identity_accepts_exact_number_or_string(self) -> None:
        for raw_id in (4738671, "4738671"):
            with self.subTest(raw_id=raw_id):
                manifest = discogs_manifest(
                    "4738671", _discogs_raw(["A1"], raw_id),
                )
                self.assertEqual(manifest.release_id, "4738671")

    def test_discogs_top_level_release_identity_must_match(self) -> None:
        for raw_id in (None, True, 4738672, "4738672", "not-an-id"):
            with self.subTest(raw_id=raw_id):
                raw = _discogs_raw(["A1"])
                raw["id"] = raw_id
                with self.assertRaisesRegex(SourceManifestError, "release identity"):
                    discogs_manifest("4738671", raw)

    def test_discogs_literal_subtrack_position_is_preserved(self) -> None:
        self.assertEqual(discogs_manifest("2823685", _discogs_raw(["A2.1", "A2.2"], "2823685")).components[1].key, "2823685-A2.2")

    def test_empty_source_program_is_rejected_not_complete(self) -> None:
        with self.assertRaisesRegex(SourceManifestError, "no playable components"):
            discogs_manifest("1", {"id": "1", "tracks": []})

    def test_discogs_index_parent_is_not_double_counted_with_subtracks(self) -> None:
        manifest = discogs_manifest("1", {"id": "1", "tracks": [{
            "position": "A2", "title": "Suite", "sub_tracks": [
                {"position": "A2.1", "title": "Part one"},
                {"position": "A2.2", "title": "Part two"},
            ],
        }]})
        self.assertEqual([component.key for component in manifest.components], ["1-A2.1", "1-A2.2"])

    def test_discogs_flattened_empty_position_empty_duration_header_is_skipped(self) -> None:
        manifest = discogs_manifest("1", {"id": "1", "tracks": [
            {"position": "", "duration": "", "title": "Side One"},
            {"position": "A1", "duration": "3:00", "title": "Song"},
        ]})
        self.assertEqual([component.key for component in manifest.components], ["1-A1"])

    def test_discogs_empty_position_with_nonempty_or_unknown_duration_is_rejected(self) -> None:
        for header in (
            {"position": "", "duration": "1:00", "title": "ambiguous"},
            {"position": "", "title": "unknown"},
        ):
            with self.subTest(header=header), self.assertRaisesRegex(
                SourceManifestError, "literal position",
            ):
                discogs_manifest("1", {"id": "1", "tracks": [header]})


class TestPhysicalInventoryContracts(unittest.TestCase):
    def test_audio_symlink_is_not_an_inventory_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.flac")
            link = os.path.join(tmpdir, "link.flac")
            open(target, "wb").close()
            os.symlink(target, link)
            with self.assertRaisesRegex(OSError, "regular file"):
                enumerate_audio_files(tmpdir)

    def test_fifo_with_audio_extension_is_not_an_inventory_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fifo = os.path.join(tmpdir, "stream.flac")
            os.mkfifo(fifo)
            with self.assertRaisesRegex(OSError, "regular file"):
                enumerate_audio_files(tmpdir)

    def test_nested_walk_error_propagates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            blocked = os.path.join(tmpdir, "blocked")
            os.mkdir(blocked)
            os.chmod(blocked, 0)
            try:
                with self.assertRaises(PermissionError):
                    enumerate_audio_files(tmpdir)
            finally:
                os.chmod(blocked, 0o700)

    def test_bad_audio_inventory_entry_becomes_classifier_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.flac")
            link = os.path.join(tmpdir, "link.flac")
            open(target, "wb").close()
            os.symlink(target, link)
            album = LibraryAlbum(
                1, "Artist", "Album", ReleaseIdentity("discogs", "1"), tmpdir, (),
            )
            result = classify_album(
                album, discogs_manifest("1", _discogs_raw(["A1"])),
                tag_reader=lambda _path: ("1-A1", ""),
            )
            self.assertEqual({finding.kind for finding in result.findings}, {"unknown"})


class TestAudioTagBoundary(unittest.TestCase):
    def test_mediafile_unreadable_error_becomes_named_tag_read_error(self) -> None:
        def unreadable(_path: str) -> object:
            raise UnreadableFileError("broken.flac", "broken")
        with self.assertRaisesRegex(AudioTagReadError, "broken"):
            read_audio_tag_identities(
                "broken.flac", media_file_factory=unreadable,
                mediafile_error_type=UnreadableFileError,
            )


class TestBeetsCompletenessProjection(unittest.TestCase):
    def test_real_sqlite_projection_preserves_discogs_synthetic_identity_and_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = os.path.join(tmpdir, "library")
            os.mkdir(root)
            album_dir = os.path.join(root, "Album")
            os.mkdir(album_dir)
            path = os.path.join(album_dir, "01.flac")
            open(path, "wb").close()
            db_path = os.path.join(tmpdir, "library.db")
            conn = sqlite3.connect(db_path)
            conn.executescript("""
              CREATE TABLE albums (id INTEGER PRIMARY KEY, albumartist TEXT, album TEXT,
                mb_albumid TEXT, discogs_albumid INTEGER);
              CREATE TABLE items (id INTEGER PRIMARY KEY, album_id INTEGER, path BLOB,
                mb_releasetrackid TEXT, mb_trackid TEXT, title TEXT, track INTEGER);
            """)
            conn.execute("INSERT INTO albums VALUES (1, 'Dirt Dress', 'Theme Songs', NULL, 4738671)")
            conn.execute("INSERT INTO items VALUES (1, 1, ?, '', '4738671-1A', '1A', 1)", (path.encode(),))
            conn.commit()
            conn.close()
            with BeetsDB(db_path, library_root=root) as beets:
                rows = beets.list_library_completeness_albums()
            self.assertEqual(rows[0].identity, ReleaseIdentity("discogs", "4738671"))
            self.assertEqual(rows[0].catalog_items[0].source_key, "4738671-1A")
            self.assertEqual(rows[0].catalog_items[0].path, path)

    def test_symlink_escape_is_refused_without_rewriting_catalog_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = os.path.join(tmpdir, "library")
            album_dir = os.path.join(root, "Album")
            os.makedirs(album_dir)
            outside = os.path.join(tmpdir, "outside.flac")
            escaped_path = os.path.join(album_dir, "escape.flac")
            open(outside, "wb").close()
            os.symlink(outside, escaped_path)
            db_path = os.path.join(tmpdir, "library.db")
            conn = sqlite3.connect(db_path)
            conn.executescript("""
              CREATE TABLE albums (id INTEGER PRIMARY KEY, albumartist TEXT, album TEXT,
                mb_albumid TEXT, discogs_albumid INTEGER);
              CREATE TABLE items (id INTEGER PRIMARY KEY, album_id INTEGER, path BLOB,
                mb_releasetrackid TEXT, mb_trackid TEXT, title TEXT, track INTEGER);
            """)
            conn.execute("INSERT INTO albums VALUES (1, 'Artist', 'Album', NULL, 1)")
            conn.execute("INSERT INTO items VALUES (1, 1, ?, '', '1-A1', 'A1', 1)", (escaped_path.encode(),))
            conn.commit()
            conn.close()
            with BeetsDB(db_path, library_root=root) as beets:
                row = beets.list_library_completeness_albums()[0]
            self.assertEqual(row.catalog_items, ())
            self.assertEqual(row.refused_paths, (escaped_path,))

    def test_dual_mb_and_discogs_identity_is_deliberately_unclassifiable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = os.path.join(tmpdir, "library")
            os.mkdir(root)
            db_path = os.path.join(tmpdir, "library.db")
            conn = sqlite3.connect(db_path)
            conn.executescript("""
              CREATE TABLE albums (id INTEGER PRIMARY KEY, albumartist TEXT, album TEXT,
                mb_albumid TEXT, discogs_albumid INTEGER);
              CREATE TABLE items (id INTEGER PRIMARY KEY, album_id INTEGER, path BLOB,
                mb_releasetrackid TEXT, mb_trackid TEXT, title TEXT, track INTEGER);
            """)
            conn.execute("INSERT INTO albums VALUES (1, 'a', 'b', '00000000-0000-4000-8000-000000000001', 4738671)")
            conn.commit()
            conn.close()
            with BeetsDB(db_path, library_root=root) as beets:
                row = beets.list_library_completeness_albums()[0]
            self.assertIsNone(row.identity)


class TestSourceWireFailures(unittest.TestCase):
    def _album(self) -> LibraryAlbum:
        return LibraryAlbum(
            1, "Artist", "Album", ReleaseIdentity("discogs", "1"), "/album", (),
        )

    def test_raw_wire_decode_errors_publish_per_album_unknown(self) -> None:
        class OneAlbumBeets:
            def list_library_completeness_albums(self) -> list[LibraryAlbum]:
                return [self_album]

        self_album = self._album()
        for error in (
            json.JSONDecodeError("bad JSON", "not json", 0),
            UnicodeDecodeError("utf-8", b"\\xff", 0, 1, "invalid"),
        ):
            with self.subTest(error=type(error).__name__):
                def fetch(_release_id: str, exc: Exception = error) -> dict[str, object]:
                    raise exc

                report = scan_library_completeness(
                    OneAlbumBeets(), fetch_musicbrainz_raw=lambda _: {},
                    fetch_discogs_raw=fetch,
                )
                self.assertEqual(report.counts.unknown, 1)
                self.assertEqual(report.albums[0].findings[0].kind, "unknown")

    def test_unexpected_source_value_error_propagates(self) -> None:
        class OneAlbumBeets:
            def list_library_completeness_albums(self) -> list[LibraryAlbum]:
                return [self_album]

        self_album = self._album()
        with self.assertRaisesRegex(ValueError, "programmer defect"):
            scan_library_completeness(
                OneAlbumBeets(), fetch_musicbrainz_raw=lambda _: {},
                fetch_discogs_raw=lambda _: (_ for _ in ()).throw(ValueError("programmer defect")),
            )


class TestMusicBrainzRedirectProof(unittest.TestCase):
    """A body mismatch is usable only after the merge resolver proves it."""

    requested = "00000000-0000-4000-8000-000000000001"
    survivor = "00000000-0000-4000-8000-000000000002"

    def _scan(self, redirect: CanonicalReleaseAnswer):
        album = LibraryAlbum(
            1, "Artist", "Album", ReleaseIdentity("musicbrainz", self.requested),
            "/album", (CatalogItem("/album/01.flac", "track", "recording"),),
        )
        raw = _mb_raw([("track", "recording", "Track", False)], self.survivor)

        class OneAlbumBeets:
            def list_library_completeness_albums(self) -> list[LibraryAlbum]:
                return [album]

        return scan_library_completeness(
            OneAlbumBeets(), fetch_musicbrainz_raw=lambda _release_id: raw,
            fetch_discogs_raw=lambda _release_id: {},
            enumerate_files=lambda _directory: ("/album/01.flac",),
            resolve_musicbrainz_redirect=lambda _release_id: redirect,
        )

    def test_mismatched_sibling_without_redirect_proof_is_unknown(self) -> None:
        report = self._scan(CanonicalReleaseCurrent())
        self.assertEqual(report.counts.unknown, 1)
        self.assertEqual(report.albums[0].findings[0].kind, "unknown")

    def test_proven_redirect_to_raw_survivor_is_admitted(self) -> None:
        report = self._scan(CanonicalReleaseRedirected(self.survivor))
        self.assertEqual(report.counts.audio_complete, 1)
        self.assertEqual(report.albums, ())

    def test_wrong_or_unavailable_redirect_proof_is_unknown(self) -> None:
        for proof in (
            CanonicalReleaseUnavailable(),
            CanonicalReleaseRedirected("00000000-0000-4000-8000-000000000003"),
        ):
            with self.subTest(proof=proof):
                report = self._scan(proof)
                self.assertEqual(report.counts.unknown, 1)
                self.assertEqual(report.albums[0].findings[0].kind, "unknown")


if __name__ == "__main__":
    unittest.main()
