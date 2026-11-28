"""Measured track lengths in the library completeness census (issue #1472)."""
from __future__ import annotations

import os
import tempfile
import unittest
from collections.abc import Sequence

from lib.library_completeness import (
    CatalogItem,
    CompletenessFinding,
    LibraryAlbum,
    SourceManifest,
    TrackLengthResult,
    discogs_manifest,
    musicbrainz_manifest,
    scan_library_completeness,
)
from lib.library_track_lengths import (
    CachedAlbumLengths,
    TrackLengthCache,
    TrackLengthCheck,
    TrackLengthMeasurer,
    TrackLengthReadError,
    album_fingerprint,
    length_pairs,
    read_track_length_cache,
    read_track_length_seconds,
    track_length_result,
    write_track_length_cache,
)
from lib.release_identity import ReleaseIdentity

MB_RELEASE = "00000000-0000-4000-8000-0000000000aa"


def _mb_raw(lengths_ms: Sequence[int | None], *, media: int = 1) -> dict[str, object]:
    """``media`` discs, each listing every length in ``lengths_ms``."""
    return {"id": MB_RELEASE, "media": [
        {"position": disc, "tracks": [
            {"id": f"rt-{disc}-{index}", "title": f"Song {disc}.{index}",
             "position": index, "length": length,
             "recording": {"id": f"rec-{disc}-{index}", "video": False}}
            for index, length in enumerate(lengths_ms, 1)
        ]}
        for disc in range(1, media + 1)
    ]}


def _mb_album(keys: Sequence[str], *, album_id: int = 1) -> LibraryAlbum:
    # Items deliberately arrive in reverse order: pairing is by identity.
    return LibraryAlbum(
        album_id, "Artist", "Album", ReleaseIdentity("musicbrainz", MB_RELEASE), "/lib/a",
        tuple(CatalogItem(path=f"/lib/a/{key}.opus", source_key=key, recording_id="")
              for key in reversed(keys)),
    )


def _measured(album: LibraryAlbum, seconds: dict[str, float]) -> dict[str, float | None]:
    return {item.path: seconds.get(item.source_key) for item in album.catalog_items}


class TestManifestCarriesListedLengths(unittest.TestCase):
    def test_musicbrainz_lengths_and_placement(self) -> None:
        raw = _mb_raw([200_000, None], media=2)
        manifest = musicbrainz_manifest(MB_RELEASE, raw)
        first, second, disc_two_first, _ = manifest.components
        self.assertEqual(first.declared_seconds, 200.0)
        self.assertIsNone(second.declared_seconds)
        self.assertEqual((first.medium, first.medium_index, first.medium_track_count), (1, 1, 2))
        self.assertEqual((disc_two_first.medium, disc_two_first.medium_index), (2, 1))

    def test_musicbrainz_pregap_is_unplaced(self) -> None:
        raw = _mb_raw([200_000])
        media = raw["media"]
        assert isinstance(media, list)
        medium = media[0]
        assert isinstance(medium, dict)
        medium["pregap"] = {"id": "pregap", "title": "Hidden", "position": 0, "length": 30_000,
                            "recording": {"id": "rec-pregap", "video": False}}
        manifest = musicbrainz_manifest(MB_RELEASE, raw)
        pregap = manifest.components[0]
        self.assertEqual((pregap.medium_index, pregap.declared_seconds), (None, 30.0))
        self.assertEqual(pregap.medium_track_count, 1)

    def test_trailing_video_does_not_move_the_last_track(self) -> None:
        raw = _mb_raw([200_000, 400_000, 100_000])
        media = raw["media"]
        assert isinstance(media, list) and isinstance(media[0], dict)
        tracks = media[0]["tracks"]
        assert isinstance(tracks, list) and isinstance(tracks[2], dict)
        tracks[2]["recording"] = {"id": "rec-video", "video": True}
        manifest = musicbrainz_manifest(MB_RELEASE, raw)
        self.assertEqual({c.medium_track_count for c in manifest.components}, {2})
        album = _mb_album(["rt-1-1", "rt-1-2"])
        result = track_length_result(album, manifest, _measured(
            album, {"rt-1-1": 200.0, "rt-1-2": 285.0}))
        assert result.finding is not None
        self.assertEqual(result.finding.detail, "ending missing? (track 2)")

    def test_discogs_durations_and_composites(self) -> None:
        raw: dict[str, object] = {"id": "7", "tracks": [
            {"position": "A1", "title": "One", "duration": "3:20"},
            {"position": "A2", "title": "Two", "duration": ""},
            {"position": "3.1", "title": "Part 1", "duration": "1:00"},
            {"position": "3.2", "title": "Part 2", "duration": "2:00"},
        ]}
        components = discogs_manifest("7", raw).components
        self.assertEqual([c.declared_seconds for c in components], [200.0, None, None])
        self.assertIsNone(components[0].medium)


class TestPairingAndFinding(unittest.TestCase):
    def _manifest(self, lengths_ms: Sequence[int | None]) -> SourceManifest:
        return musicbrainz_manifest(MB_RELEASE, _mb_raw(lengths_ms))

    def test_pairs_by_release_track_identity_not_item_order(self) -> None:
        manifest = self._manifest([100_000, 200_000, 300_000])
        album = _mb_album(["rt-1-1", "rt-1-2", "rt-1-3"])
        pairs = length_pairs(album, manifest, _measured(
            album, {"rt-1-1": 100.0, "rt-1-2": 200.0, "rt-1-3": 300.0}))
        self.assertEqual([(p.medium_index, p.file_seconds, p.listed_seconds) for p in pairs],
                         [(1, 100.0, 100.0), (2, 200.0, 200.0), (3, 300.0, 300.0)])

    def test_duplicate_identity_is_not_paired(self) -> None:
        manifest = self._manifest([100_000, 200_000])
        album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", MB_RELEASE), "/d", (
            CatalogItem("/d/1.opus", "rt-1-1", ""), CatalogItem("/d/1b.opus", "rt-1-1", ""),
            CatalogItem("/d/2.opus", "rt-1-2", ""),
        ))
        pairs = length_pairs(album, manifest, {"/d/1.opus": 10.0, "/d/1b.opus": 10.0, "/d/2.opus": 200.0})
        self.assertEqual([p.medium_index for p in pairs], [2])

    def test_ending_missing_is_a_finding_that_needs_review(self) -> None:
        manifest = self._manifest([200_000, 200_000, 400_000])
        album = _mb_album(["rt-1-1", "rt-1-2", "rt-1-3"])
        result = track_length_result(album, manifest, _measured(
            album, {"rt-1-1": 200.0, "rt-1-2": 200.0, "rt-1-3": 285.0}))
        assert result.finding is not None
        self.assertEqual(result.finding.kind, "track_length")
        self.assertEqual(result.finding.detail, "ending missing? (track 3)")
        self.assertIn("4:45", result.finding.sentence)
        self.assertFalse(result.finding.probably_harmless)

    def test_hidden_track_is_probably_harmless(self) -> None:
        manifest = self._manifest([200_000, 200_000])
        album = _mb_album(["rt-1-1", "rt-1-2"])
        result = track_length_result(album, manifest, _measured(
            album, {"rt-1-1": 200.0, "rt-1-2": 600.0}))
        assert result.finding is not None
        self.assertTrue(result.finding.probably_harmless)
        self.assertEqual(result.finding.detail, "hidden track? (track 2)")

    def test_detail_is_stable_when_only_seconds_change(self) -> None:
        album = _mb_album(["rt-1-1", "rt-1-2"])
        measured = _measured(album, {"rt-1-1": 200.0, "rt-1-2": 100.0})
        before = track_length_result(album, self._manifest([200_000, 200_000]), measured)
        after = track_length_result(album, self._manifest([200_000, 201_000]), measured)
        assert before.finding is not None and after.finding is not None
        self.assertEqual(before.finding.detail, after.finding.detail)
        self.assertNotEqual(before.finding.sentence, after.finding.sentence)

    def test_within_bound_is_measured_without_finding(self) -> None:
        manifest = self._manifest([200_000])
        album = _mb_album(["rt-1-1"])
        result = track_length_result(album, manifest, _measured(album, {"rt-1-1": 250.0}))
        self.assertEqual(result, TrackLengthResult(status="measured"))

    def test_release_without_listed_lengths_is_said_not_flagged(self) -> None:
        manifest = self._manifest([None, None])
        album = _mb_album(["rt-1-1", "rt-1-2"])
        result = track_length_result(album, manifest, _measured(
            album, {"rt-1-1": 10.0, "rt-1-2": 900.0}))
        self.assertEqual(result, TrackLengthResult(status="measured", listed=False))

    def test_unreadable_file_is_not_compared(self) -> None:
        manifest = self._manifest([200_000, 200_000])
        album = _mb_album(["rt-1-1", "rt-1-2"])
        result = track_length_result(album, manifest, {
            "/lib/a/rt-1-1.opus": 200.0, "/lib/a/rt-1-2.opus": None})
        self.assertIsNone(result.finding)

    def test_multi_disc_detail_names_the_disc(self) -> None:
        manifest = musicbrainz_manifest(MB_RELEASE, _mb_raw([200_000, 200_000], media=2))
        keys = ["rt-1-1", "rt-1-2", "rt-2-1", "rt-2-2"]
        album = _mb_album(keys)
        result = track_length_result(album, manifest, _measured(
            album, {"rt-1-1": 200.0, "rt-1-2": 200.0, "rt-2-1": 200.0, "rt-2-2": 100.0}))
        assert result.finding is not None
        self.assertEqual(result.finding.detail, "ending missing? (disc 2 track 2)")


DISCOGS_VINYL: dict[str, object] = {"id": "9", "tracks": [
    {"position": "A1", "title": "One", "duration": "3:20"},
    {"position": "A2", "title": "Two", "duration": "3:20"},
    {"position": "B1", "title": "Three", "duration": "6:40"},
]}


class TestDiscogsPlacement(unittest.TestCase):
    def _album(self, positions: Sequence[str]) -> LibraryAlbum:
        return LibraryAlbum(1, "a", "b", ReleaseIdentity("discogs", "9"), "/d", tuple(
            CatalogItem(f"/d/{p}.opus", f"9-{p}", f"9-{p}", disc=1) for p in positions))

    def test_disc_from_beets_and_order_within_disc(self) -> None:
        album = self._album(["A1", "A2", "B1"])
        manifest = discogs_manifest("9", DISCOGS_VINYL)
        result = track_length_result(album, manifest, {
            "/d/A1.opus": 200.0, "/d/A2.opus": 200.0, "/d/B1.opus": 300.0})
        assert result.finding is not None
        # Side B is not a second disc: B1 is the last track of disc 1.
        self.assertEqual(result.finding.detail, "ending missing? (track 3)")

    def test_tracks_are_unplaced_when_one_is_not_installed(self) -> None:
        album = self._album(["A1", "B1"])
        manifest = discogs_manifest("9", DISCOGS_VINYL)
        pairs = length_pairs(album, manifest, {"/d/A1.opus": 200.0, "/d/B1.opus": 300.0})
        self.assertEqual([p.medium_index for p in pairs], [None, None])
        result = track_length_result(album, manifest, {"/d/A1.opus": 200.0, "/d/B1.opus": 300.0})
        assert result.finding is not None
        self.assertEqual(result.finding.detail, 'one track shorter ("Three")')


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class _Stat:
    def __init__(self, sizes: dict[str, int]) -> None:
        self.sizes = sizes

    def __call__(self, path: str) -> os.stat_result:
        if path not in self.sizes:
            raise FileNotFoundError(path)
        return os.stat_result((0, 0, 0, 0, 0, 0, self.sizes[path], 0, 0, 0))


class TestMeasurer(unittest.TestCase):
    def _measurer(
        self, cache: TrackLengthCache, *, budget: float = 100.0,
        lengths: dict[str, float] | None = None, sizes: dict[str, int] | None = None,
        clock: _Clock | None = None, read_cost: float = 0.0,
    ) -> tuple[TrackLengthMeasurer, list[str], _Clock]:
        reads: list[str] = []
        clock = clock or _Clock()
        table = lengths or {}

        def read(path: str) -> float:
            reads.append(path)
            clock.now += read_cost
            if path not in table:
                raise TrackLengthReadError("unreadable")
            return table[path]

        measurer = TrackLengthMeasurer(
            cache, budget_seconds=budget, reads_per_second=10.0, read_length=read,
            stat=_Stat(sizes if sizes is not None else dict.fromkeys(table, 1)),
            clock=clock, sleep=clock.sleep,
        )
        return measurer, reads, clock

    def test_reads_then_serves_from_cache(self) -> None:
        lengths = {"/a/1": 10.0, "/a/2": 20.0}
        first, reads, _ = self._measurer(TrackLengthCache(), lengths=lengths)
        self.assertEqual(first.lengths_for(1, ["/a/2", "/a/1"]), lengths)
        self.assertEqual(reads, ["/a/1", "/a/2"])
        second, reads_again, _ = self._measurer(first.updated_cache(), lengths=lengths)
        self.assertEqual(second.lengths_for(1, ["/a/1", "/a/2"]), lengths)
        self.assertEqual(reads_again, [])

    def test_changed_file_is_read_again(self) -> None:
        lengths = {"/a/1": 10.0}
        first, _, _ = self._measurer(TrackLengthCache(), lengths=lengths)
        first.lengths_for(1, ["/a/1"])
        second, reads, _ = self._measurer(first.updated_cache(), lengths=lengths, sizes={"/a/1": 2})
        second.lengths_for(1, ["/a/1"])
        self.assertEqual(reads, ["/a/1"])

    def test_reads_are_spaced_by_the_rate_limit(self) -> None:
        measurer, _, clock = self._measurer(
            TrackLengthCache(), lengths={"/a/1": 1.0, "/a/2": 1.0, "/a/3": 1.0})
        measurer.lengths_for(1, ["/a/1", "/a/2", "/a/3"])
        self.assertEqual(clock.slept, [0.1, 0.1])

    def test_budget_leaves_later_albums_pending(self) -> None:
        lengths = {"/a/1": 1.0, "/b/1": 1.0}
        measurer, reads, _ = self._measurer(
            TrackLengthCache(), budget=0.5, lengths=lengths, read_cost=1.0)
        self.assertIsNotNone(measurer.lengths_for(1, ["/a/1"]))
        self.assertIsNone(measurer.lengths_for(2, ["/b/1"]))
        self.assertEqual(reads, ["/a/1"])

    def test_zero_budget_uses_only_the_cache(self) -> None:
        sizes = {"/a/1": 1}
        cache = TrackLengthCache(albums={"1": CachedAlbumLengths(
            album_fingerprint(["/a/1"], stat=_Stat(sizes)), (12.0,))})
        measurer, reads, _ = self._measurer(cache, budget=0.0, sizes=sizes)
        self.assertEqual(measurer.lengths_for(1, ["/a/1"]), {"/a/1": 12.0})
        self.assertIsNone(measurer.lengths_for(2, ["/b/1"]))
        self.assertEqual(reads, [])

    def test_unreadable_file_is_cached_and_not_reread(self) -> None:
        sizes = {"/a/1": 1, "/a/2": 1}
        first, _, _ = self._measurer(TrackLengthCache(), lengths={"/a/1": 5.0}, sizes=sizes)
        self.assertEqual(first.lengths_for(1, ["/a/1", "/a/2"]), {"/a/1": 5.0, "/a/2": None})
        second, reads, _ = self._measurer(first.updated_cache(), lengths={"/a/1": 5.0}, sizes=sizes)
        self.assertEqual(second.lengths_for(1, ["/a/1", "/a/2"]), {"/a/1": 5.0, "/a/2": None})
        self.assertEqual(reads, [])

    def test_lengths_are_rounded_when_read(self) -> None:
        measurer, _, _ = self._measurer(TrackLengthCache(), lengths={"/a/1": 5.123456})
        self.assertEqual(measurer.lengths_for(1, ["/a/1"]), {"/a/1": 5.12})

    def test_unseen_albums_drop_and_kept_albums_carry_forward(self) -> None:
        cache = TrackLengthCache(albums={
            "1": CachedAlbumLengths("x", (1.0,)), "2": CachedAlbumLengths("y", (2.0,))})
        measurer, _, _ = self._measurer(cache)
        measurer.keep(1)
        self.assertEqual(set(measurer.updated_cache().albums), {"1"})

    def test_cache_round_trips_and_bad_cache_starts_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")
            self.assertEqual(read_track_length_cache(path), TrackLengthCache())
            cache = TrackLengthCache(albums={"3": CachedAlbumLengths("f", (1.5, None))})
            write_track_length_cache(path, cache)
            self.assertEqual(read_track_length_cache(path), cache)
            with open(path, "w") as fh:
                fh.write("{not json")
            with self.assertLogs("lib.library_track_lengths", "ERROR"):
                self.assertEqual(read_track_length_cache(path), TrackLengthCache())

    def test_real_reader_maps_unreadable_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "broken.flac")
            with open(path, "wb") as fh:
                fh.write(b"not audio")
            with self.assertRaises(TrackLengthReadError):
                read_track_length_seconds(path)


class TestScanIntegration(unittest.TestCase):
    def _scan(self, check: TrackLengthCheck, albums: list[LibraryAlbum], raw: dict[str, object]):
        class Beets:
            def list_library_completeness_albums(self) -> list[LibraryAlbum]:
                return albums

        def enumerate_files(directory: str) -> tuple[str, ...]:
            return tuple(sorted(item.path for album in albums for item in album.catalog_items
                                if album.directory == directory))

        return scan_library_completeness(
            Beets(), fetch_musicbrainz_raw=lambda _: raw, fetch_discogs_raw=lambda _: {},
            enumerate_files=enumerate_files, track_lengths=check,
        )

    def test_length_finding_lists_album_without_changing_completeness(self) -> None:
        raw = _mb_raw([200_000, 400_000])
        album = _mb_album(["rt-1-1", "rt-1-2"])
        lengths = {"/lib/a/rt-1-1.opus": 200.0, "/lib/a/rt-1-2.opus": 280.0}
        measurer = TrackLengthMeasurer(
            TrackLengthCache(), read_length=lengths.__getitem__,
            stat=_Stat(dict.fromkeys(lengths, 1)), sleep=lambda _: None)
        report = self._scan(TrackLengthCheck(measurer), [album], raw)
        self.assertEqual(report.status, "complete")
        self.assertEqual(report.counts.audio_complete, 1)
        self.assertEqual((report.counts.track_length, report.counts.track_length_harmless), (1, 0))
        self.assertEqual(report.counts.track_length_measured, 1)
        self.assertEqual([f.kind for f in report.albums[0].findings], ["track_length"])

    def test_pending_and_unlisted_are_counted(self) -> None:
        unlisted = _mb_album(["rt-1-1"], album_id=1)
        waiting = LibraryAlbum(2, "a", "b", ReleaseIdentity("musicbrainz", MB_RELEASE), "/lib/b",
                               (CatalogItem("/lib/b/x.opus", "rt-1-1", ""),))
        fingerprint = album_fingerprint(["/lib/a/rt-1-1.opus"],
                                        stat=_Stat({"/lib/a/rt-1-1.opus": 1}))
        measurer = TrackLengthMeasurer(
            TrackLengthCache(albums={"1": CachedAlbumLengths(fingerprint, (100.0,))}),
            budget_seconds=0.0, stat=_Stat({"/lib/a/rt-1-1.opus": 1, "/lib/b/x.opus": 1}))
        report = self._scan(TrackLengthCheck(measurer), [unlisted, waiting], _mb_raw([None]))
        counts = report.counts
        self.assertEqual((counts.track_length_measured, counts.track_length_pending,
                          counts.track_length_unlisted), (1, 1, 1))
        self.assertEqual(report.albums, ())

    def test_pending_album_keeps_its_previous_finding(self) -> None:
        album = _mb_album(["rt-1-1"])
        previous = CompletenessFinding(
            "track_length", "ending missing? (track 1)", sentence="old")
        measurer = TrackLengthMeasurer(
            TrackLengthCache(), budget_seconds=0.0, stat=_Stat({"/lib/a/rt-1-1.opus": 1}))
        report = self._scan(TrackLengthCheck(measurer, previous={1: previous}),
                            [album], _mb_raw([200_000]))
        self.assertEqual(report.counts.track_length_pending, 1)
        self.assertEqual(report.counts.track_length, 1)
        self.assertEqual(report.albums[0].findings, (previous,))

    def test_unreachable_source_keeps_cached_measurements(self) -> None:
        album = _mb_album(["rt-1-1"])
        cache = TrackLengthCache(albums={"1": CachedAlbumLengths("f", (1.0,))})
        measurer = TrackLengthMeasurer(cache, budget_seconds=0.0)

        class Beets:
            def list_library_completeness_albums(self) -> list[LibraryAlbum]:
                return [album]

        def unreachable(_: str) -> dict[str, object]:
            raise OSError("mirror down")

        report = scan_library_completeness(
            Beets(), fetch_musicbrainz_raw=unreachable, fetch_discogs_raw=unreachable,
            track_lengths=TrackLengthCheck(measurer))
        self.assertEqual(report.counts.unknown, 1)
        self.assertEqual(measurer.updated_cache(), cache)


if __name__ == "__main__":
    unittest.main()
