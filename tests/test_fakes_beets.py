"""Self-tests for ``tests/fakes/beets.py``'s FakeBeetsDB.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest

from lib.beets_db import AlbumInfo
from tests.fakes import (
    FakeBeetsDB,
)


class TestFakeBeetsDB(unittest.TestCase):
    """Self-tests for FakeBeetsDB — the minimal in-memory BeetsDB stand-in."""

    def test_album_mb_identities_round_trip(self) -> None:
        """#1093 item 1 — the retag divergence audit's read seam."""
        from lib.beets_db import BeetsAlbumIdentityRow

        beets = FakeBeetsDB()
        self.assertEqual(beets.list_album_mb_identities(), [])

        rows = [
            BeetsAlbumIdentityRow(
                album_id=7,
                mb_albumid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                item_paths=("/library/Artist/Album/01.flac",),
            ),
            BeetsAlbumIdentityRow(album_id=8, mb_albumid="", item_paths=()),
        ]
        beets.set_album_mb_identities(rows)

        self.assertEqual(beets.list_album_mb_identities(), rows)
        # Returns a fresh list — callers mutating the result never poison
        # the fake's seeded state.
        beets.list_album_mb_identities().append(rows[0])
        self.assertEqual(beets.list_album_mb_identities(), rows)

    def test_get_album_mb_identity_looks_up_by_id(self) -> None:
        """#1142 — the per-album retag recheck's narrow read seam."""
        from lib.beets_db import BeetsAlbumIdentityRow

        beets = FakeBeetsDB()
        rows = [
            BeetsAlbumIdentityRow(
                album_id=7,
                mb_albumid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                item_paths=("/library/Artist/Album/01.flac",),
            ),
            BeetsAlbumIdentityRow(album_id=8, mb_albumid="", item_paths=()),
        ]
        beets.set_album_mb_identities(rows)

        self.assertEqual(beets.get_album_mb_identity(7), rows[0])
        self.assertEqual(beets.get_album_mb_identity(8), rows[1])
        self.assertIsNone(beets.get_album_mb_identity(999))

    def test_current_resolver_preserves_cardinality_and_topology(self) -> None:
        from lib.beets_db import CurrentBeetsAmbiguous, CurrentBeetsUnique
        from lib.release_identity import ReleaseIdentity

        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id="11111111-1111-1111-1111-111111111111",
        )
        beets = FakeBeetsDB(library_root="/library")
        beets.set_album_ids_for_release(identity.release_id, [7])
        beets.set_item_paths(identity.release_id, [(70, "Artist/Album/01.flac")])
        unique = beets.resolve_current_release(identity)
        self.assertIsInstance(unique, CurrentBeetsUnique)
        assert isinstance(unique, CurrentBeetsUnique)
        self.assertEqual(unique.album_path, "/library/Artist/Album")

        beets.set_album_ids_for_release(identity.release_id, [7, 8])
        ambiguous = beets.resolve_current_release(identity)
        self.assertIsInstance(ambiguous, CurrentBeetsAmbiguous)
        assert isinstance(ambiguous, CurrentBeetsAmbiguous)
        self.assertEqual(ambiguous.reason, "multiple_matches")

        beets.set_album_ids_for_release(identity.release_id, [7])
        for invalid in ("", None, "bad\x00path.flac", "../outside.flac"):
            with self.subTest(invalid=invalid):
                beets.set_item_paths(identity.release_id, [(70, invalid)])
                poisoned = beets.resolve_current_release(identity)
                self.assertIsInstance(poisoned, CurrentBeetsAmbiguous)
                assert isinstance(poisoned, CurrentBeetsAmbiguous)
                self.assertEqual(poisoned.reason, "invalid_path")

    def test_resolve_current_release_error_mirrors_beets_authority_failure(
        self,
    ) -> None:
        """#1089 MINOR-5 (test-fidelity Rule B): the fake must be able to
        RAISE, not just return a shape, so ``MergeRekeyService``'s Beets-
        authority classify-or-reraise boundary is exercised with a real
        exception instance rather than a synthetic stand-in."""
        import sqlite3

        from lib.release_identity import ReleaseIdentity

        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id="11111111-1111-1111-1111-111111111111",
        )
        beets = FakeBeetsDB()
        locked = sqlite3.OperationalError("database is locked")
        locked.sqlite_errorcode = sqlite3.SQLITE_LOCKED
        beets.set_resolve_current_release_error(identity.release_id, locked)

        with self.assertRaises(sqlite3.OperationalError):
            beets.resolve_current_release(identity)
        self.assertEqual(beets.resolve_current_release_calls, [identity])

        # A different release id is unaffected — the error is keyed, not
        # global.
        other = ReleaseIdentity(
            source="musicbrainz",
            release_id="22222222-2222-2222-2222-222222222222",
        )
        beets.set_album_ids_for_release(other.release_id, [])
        beets.resolve_current_release(other)  # does not raise

    def test_discogs_alias_reseed_replaces_the_canonical_current_snapshot(
        self,
    ) -> None:
        from lib.beets_db import AlbumInfo, CurrentBeetsUnique
        from lib.release_identity import ReleaseIdentity

        beets = FakeBeetsDB(library_root="/library")
        for release_id, album_id, path in (
            ("0012856590", 7, "/library/stale"),
            ("12856590", 8, "/library/current"),
        ):
            beets.set_album_info(release_id, AlbumInfo(
                album_id=album_id,
                track_count=1,
                min_bitrate_kbps=245,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                is_cbr=True,
                album_path=path,
                format="MP3",
            ))

        identity = ReleaseIdentity.from_id("0012856590")
        assert identity is not None
        current = beets.resolve_current_release(identity)
        self.assertIsInstance(current, CurrentBeetsUnique)
        assert isinstance(current, CurrentBeetsUnique)
        self.assertEqual(current.album_id, 8)
        self.assertEqual(current.album_path, "/library/current")
        self.assertEqual(
            beets.get_album_info("0012856590", None).album_id,
            8,
        )

    def test_check_mbids_detail_returns_seeded_rows_only(self) -> None:
        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            "mbid-1",
            {
                "beets_tracks": 11,
                "beets_bitrate": 194,
                "beets_avg_bitrate": 288,
            },
        )
        out = beets.check_mbids_detail(["mbid-1", "mbid-2"])
        self.assertEqual(out, {"mbid-1": {
            "beets_tracks": 11,
            "beets_format": None,
            "beets_bitrate": 194,
            "beets_avg_bitrate": 288,
            "beets_samplerate": None,
            "beets_bitdepth": None,
        }})
        self.assertEqual(beets.check_mbids_detail_calls,
                         [["mbid-1", "mbid-2"]])

    def test_get_albums_by_artist_returns_seeded_rows(self) -> None:
        beets = FakeBeetsDB()
        beets.set_albums_by_artist("X", [{"album": "A"}])
        self.assertEqual(beets.get_albums_by_artist("X", "mb-1"),
                         [{"album": "A"}])
        self.assertEqual(beets.get_albums_by_artist("Y"), [])
        self.assertEqual(beets.get_albums_by_artist_calls,
                         [("X", "mb-1"), ("Y", "")])

    def test_exact_album_projection_matches_each_cross_source_identity(self) -> None:
        beets = FakeBeetsDB()
        album = {
            "id": 7,
            "album": "Dual-tagged pressing",
            "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "discogs_albumid": "12856590",
        }
        beets.set_albums_by_artist("X", [album])

        rows = beets.get_albums_by_release_ids([
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "12856590",
        ])

        self.assertEqual(rows, [album])
        self.assertEqual(beets.get_albums_by_release_ids_calls, [[
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "12856590",
        ]])

    def test_exact_album_projection_rejects_conflicting_numeric_identity(self) -> None:
        beets = FakeBeetsDB()
        beets.set_albums_by_artist("X", [{
            "id": 7,
            "album": "Conflicting pressing",
            "mb_albumid": "12856590",
            "discogs_albumid": "12856591",
        }])

        with self.assertRaisesRegex(
            ValueError,
            "conflicting numeric Discogs release identities",
        ):
            beets.get_albums_by_release_ids(["12856590"])

        self.assertEqual(
            beets.get_albums_by_release_ids(["99999999"]),
            [],
            "an unrelated conflicting row is outside the requested snapshot",
        )

    def test_get_tracks_by_mb_release_id_returns_seeded_or_none(self) -> None:
        # Real method returns None when locate finds no exact hit —
        # NOT an empty list (the browse route branches on that).
        beets = FakeBeetsDB()
        tracks = [{"title": "T1", "track": 1, "disc": 1, "length": 180,
                   "format": "MP3", "bitrate": 320000,
                   "samplerate": 44100, "bitdepth": 16}]
        beets.set_tracks_for_release("mbid-1", tracks)
        self.assertEqual(beets.get_tracks_by_mb_release_id("mbid-1"), tracks)
        self.assertIsNone(beets.get_tracks_by_mb_release_id("mbid-2"))
        self.assertEqual(beets.get_tracks_by_mb_release_id_calls,
                         ["mbid-1", "mbid-2"])

    def test_get_tracks_empty_list_when_album_present_without_seeds(self) -> None:
        # Production: an exact album hit always yields a list (its
        # items), never None. 'Album present but tracks None' is not a
        # reachable state, so the fake must not express it either.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [7])
        self.assertEqual(beets.get_tracks_by_mb_release_id("mbid-1"), [{
            "title": None, "track": None, "disc": None, "length": None,
            "format": None, "bitrate": None, "samplerate": None,
            "bitdepth": None,
        }])

    def test_album_id_seeds_imply_presence(self) -> None:
        # Production derives presence and album-id mapping from one
        # seam (issue #121) — seeded ids mean the release IS in
        # library. An explicit set_album_exists seed still wins.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [7])
        self.assertTrue(beets.album_exists("mbid-1"))
        self.assertEqual(beets.check_mbids(["mbid-1", "mbid-2"]), {"mbid-1"})
        beets.set_album_exists("mbid-1", False)
        self.assertFalse(beets.album_exists("mbid-1"))

    def test_get_album_ids_by_mbids_normalizes_like_production(self) -> None:
        # _batch_lookup_album_ids normalizes every input and keys the
        # result by the canonical form — '0012856590' hits the row
        # stored '12856590'.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("12856590", [8])
        out = beets.get_album_ids_by_mbids(["0012856590"])
        self.assertEqual(out, {"12856590": 8})

    def test_get_album_ids_by_mbids_honors_album_ids_default(self) -> None:
        # The shared store's _default affordance applies to both
        # readers — get_all_album_ids_for_release and this map.
        beets = FakeBeetsDB()
        beets._album_ids_default = [5]
        self.assertEqual(beets.get_album_ids_by_mbids(["mbid-x"]),
                         {"mbid-x": 5})

    def test_locate_state_derived_from_album_id_seeds(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(
            "11111111-1111-1111-1111-111111111111", [4])
        beets.set_album_ids_for_release("12856590", [9])
        loc = beets.locate("11111111-1111-1111-1111-111111111111")
        self.assertEqual(loc, ReleaseLocation(
            kind="exact", album_id=4,
            selectors=("mb_albumid:11111111-1111-1111-1111-111111111111",)))
        # Discogs numeric shape → both selector columns, normalized id.
        loc = beets.locate("0012856590")
        self.assertEqual(loc, ReleaseLocation(
            kind="exact", album_id=9,
            selectors=("discogs_albumid:12856590",
                       "mb_albumid:12856590")))
        self.assertEqual(
            beets.locate("unseeded-mbid"),
            ReleaseLocation(kind="absent", album_id=None, selectors=()))
        self.assertEqual(
            beets.locate_calls,
            ["11111111-1111-1111-1111-111111111111", "0012856590",
             "unseeded-mbid"])

    def test_locate_queue_consumes_in_order_and_repeats_last(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        beets.queue_locate_results([
            ReleaseLocation(kind="exact", album_id=1, selectors=()),
            ReleaseLocation(kind="absent", album_id=None, selectors=()),
        ])
        first = beets.locate("mbid-x")
        # Empty selectors on an exact entry auto-fill from the queried
        # id's shape at call time.
        self.assertEqual(first.kind, "exact")
        self.assertEqual(first.selectors, ("mb_albumid:mbid-x",))
        self.assertEqual(beets.locate("mbid-x").kind, "absent")
        self.assertEqual(beets.locate("mbid-x").kind, "absent")

    def test_get_min_bitrate_seeded_and_default(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [1])
        beets.set_min_bitrate("mbid-1", 245)
        self.assertEqual(beets.get_min_bitrate("mbid-1"), 245)
        beets.set_album_ids_for_release("mbid-2", [2])
        beets.set_min_bitrate("mbid-2", 320)
        self.assertEqual(beets.get_min_bitrate("mbid-2"), 320)
        self.assertEqual(beets.get_min_bitrate_calls,
                         ["mbid-1", "mbid-2"])

    def test_get_min_bitrate_gates_on_presence_like_production(self) -> None:
        # Production resolves presence via locate first — an absent
        # release returns None no matter what; bitrate keys normalize.
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        self.assertIsNone(beets.get_min_bitrate("mbid-absent"))
        beets.set_album_ids_for_release("12856590", [7])
        beets.set_min_bitrate("12856590", 245)
        self.assertEqual(beets.get_min_bitrate("0012856590"), 245)
        # Queued locate head models "current" state — after a queued
        # removal lands at absent, min_bitrate goes None with it.
        beets.queue_locate_results([
            ReleaseLocation(kind="absent", album_id=None, selectors=())])
        self.assertIsNone(beets.get_min_bitrate("0012856590"))
        self.assertFalse(beets.album_exists("0012856590"))

    def test_locate_queue_rejects_impossible_locations(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        with self.assertRaises(AssertionError):
            beets.queue_locate_results([ReleaseLocation(
                kind="exact", album_id=None, selectors=())])
        with self.assertRaises(AssertionError):
            beets.queue_locate_results([ReleaseLocation(
                kind="absent", album_id=None,
                selectors=("mb_albumid:x",))])

    def test_locate_queue_passes_explicit_selectors_verbatim(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        entry = ReleaseLocation(
            kind="exact", album_id=3,
            selectors=("discogs_albumid:9", "mb_albumid:9"))
        beets.queue_locate_results([entry])
        self.assertEqual(beets.locate("9"), entry)

    def test_get_album_detail_keyed_by_album_id(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_detail(7, {"id": 7, "album": "A", "tracks": []})
        detail = beets.get_album_detail(7)
        assert detail is not None
        self.assertEqual(detail["album"], "A")
        detail["album"] = "mutated"
        got = beets.get_album_detail(7)
        assert got is not None
        self.assertEqual(got["album"], "A")
        self.assertIsNone(beets.get_album_detail(8))
        self.assertEqual(beets.get_album_detail_calls, [7, 7, 8])

    def test_album_and_items_absent_requires_both_absence_facts(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_detail(7, {"id": 7, "album": "A", "tracks": []})
        self.assertFalse(beets.album_and_items_absent(7))
        beets._album_detail.pop(7)
        beets.set_orphan_items_present(7)
        self.assertFalse(beets.album_and_items_absent(7))
        beets.set_orphan_items_present(7, False)
        self.assertTrue(beets.album_and_items_absent(7))

    def test_get_album_ids_by_mbids_derives_from_release_id_seeds(self) -> None:
        # Shares the set_album_ids_for_release seed store so presence
        # and album-id mapping can't disagree (the paired-consistency
        # concern from issue #121 the real _batch_lookup_album_ids
        # exists to solve). Multiple exact rows are ambiguous, never first-wins.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [17, 18])
        beets.set_album_ids_for_release("mbid-empty", [])
        out = beets.get_album_ids_by_mbids(["mbid-1", "mbid-empty", "mbid-2"])
        self.assertEqual(out, {})
        self.assertEqual(beets.get_album_ids_by_mbids_calls,
                         [["mbid-1", "mbid-empty", "mbid-2"]])

    def test_album_exists_returns_seeded_value(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_exists("mbid-1", True)
        beets.set_album_exists("mbid-2", False)
        self.assertTrue(beets.album_exists("mbid-1"))
        self.assertFalse(beets.album_exists("mbid-2"))
        # Unseeded keys default to False (matches "no row" semantics).
        self.assertFalse(beets.album_exists("mbid-unknown"))
        self.assertEqual(
            beets.album_exists_calls,
            ["mbid-1", "mbid-2", "mbid-unknown"],
        )

    def test_get_album_info_keyed_by_release_id(self) -> None:
        from lib.beets_db import AlbumInfo
        beets = FakeBeetsDB()
        info = AlbumInfo(
            album_id=7,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Artist/Album",
        )
        beets.set_album_info("mbid-1", info)
        beets.set_item_paths("mbid-1", [
            (700 + index, f"/Beets/Moved/{index + 1:02d}.flac")
            for index in range(info.track_count)
        ])
        # Two-arg form (matches real signature: mb_release_id + cfg).
        current = beets.get_album_info("mbid-1", None)
        self.assertIsNot(current, info)
        self.assertEqual(current.album_path, "/Beets/Moved")

        beets.set_item_paths("mbid-1", [(700, "/Beets/Moved/01.flac")])
        narrowed = beets.get_album_info("mbid-1", None)
        self.assertIsNotNone(narrowed)
        assert narrowed is not None
        self.assertEqual(narrowed.track_count, 1)
        # Unseeded returns None.
        self.assertIsNone(beets.get_album_info("mbid-unknown"))
        self.assertEqual(
            beets.get_album_info_calls,
            ["mbid-1", "mbid-1", "mbid-unknown"],
        )

    def _rounding_boundary_album(self) -> AlbumInfo:
        """What the shrunk lineage world really reduces to.

        Hypothesis shrank to three tracks at 32 / 57 / 57 kbps. Their mean
        is 48666 bps, which production's ``kbps_from_bps`` rounds to 49
        while a floored copy reads 48 — the one-kbps gap that made a seeded
        AlbumInfo and its rebuilt twin disagree. (Seeding this AlbumInfo
        synthesizes 32 / 57 / 58, the whole-kilobit world with the same
        aggregates.)
        """
        return AlbumInfo(
            album_id=1,
            track_count=3,
            min_bitrate_kbps=32,
            avg_bitrate_kbps=49,
            median_bitrate_kbps=57,
            is_cbr=False,
            album_path="/Beets/Artist/Rounding",
            format="AAC",
        )

    def _sub_kilobit_tracks(self) -> list[dict[str, object]]:
        """Two items whose rate is not a whole number of kilobits.

        Real Beets rows carry raw bits per second, which almost never land
        on a kilobit boundary. 255600 is where flooring and rounding
        disagree — 255 against 256 — so a floored copy of either projection
        shows here and nowhere in the whole-kbps worlds ``set_album_info``
        can express.
        """
        return [
            {"bitrate": 255_600, "format": "MP3"},
            {"bitrate": 255_600, "format": "MP3"},
        ]

    def test_get_album_info_is_the_production_projection(self) -> None:
        """The fake must not re-derive what album_info_from_current derives.

        Its hand-copied projection floored bps->kbps after production moved
        to rounding, so the fake answered one kbps low for identical seeded
        items. Both sides of every assertion were the copy, so nothing could
        see it.
        """
        from lib.beets_db import CurrentBeetsUnique, album_info_from_current
        from lib.quality import QualityRankConfig
        from tests.fakes.beets import _lookup_identity

        beets = FakeBeetsDB()
        beets.set_tracks_for_release("mbid-sub", self._sub_kilobit_tracks())

        identity = _lookup_identity("mbid-sub")
        assert identity is not None
        resolution = beets.resolve_current_release(identity)
        assert isinstance(resolution, CurrentBeetsUnique)
        expected = album_info_from_current(
            resolution, QualityRankConfig.defaults(),
        )
        assert expected is not None
        actual = beets.get_album_info("mbid-sub")
        assert actual is not None

        self.assertEqual(actual.min_bitrate_kbps, expected.min_bitrate_kbps)
        self.assertEqual(actual.avg_bitrate_kbps, expected.avg_bitrate_kbps)
        self.assertEqual(
            actual.median_bitrate_kbps, expected.median_bitrate_kbps,
        )
        # 255 is what the floored copy reported.
        self.assertEqual(actual.min_bitrate_kbps, 256)
        self.assertEqual(actual.avg_bitrate_kbps, 256)
        self.assertEqual(actual.median_bitrate_kbps, 256)
        # Production also publishes the per-item codec set; the copy never
        # did, so a mixed-codec fake album silently looked single-codec.
        self.assertEqual(actual.formats_on_disk, expected.formats_on_disk)
        self.assertEqual(actual.formats_on_disk, frozenset({"mp3"}))

    def test_check_mbids_detail_shares_the_projection_reduction(self) -> None:
        """The two projections of one album must not disagree in the fake."""
        beets = FakeBeetsDB()
        beets.set_tracks_for_release("mbid-sub", self._sub_kilobit_tracks())

        detail = beets.check_mbids_detail(["mbid-sub"])["mbid-sub"]
        projected = beets.get_album_info("mbid-sub")
        assert projected is not None

        self.assertEqual(detail["beets_bitrate"], projected.min_bitrate_kbps)
        self.assertEqual(
            detail["beets_avg_bitrate"], projected.avg_bitrate_kbps,
        )
        self.assertEqual(detail["beets_bitrate"], 256)
        self.assertEqual(detail["beets_avg_bitrate"], 256)

    def test_seeding_refuses_a_world_production_cannot_reduce_to(self) -> None:
        """min/avg/median the synthesizer can no longer build.

        32 / 48 / 57 is what the floored helper derived from tracks at
        32 / 57 / 57, and it is not reachable from whole-kilobit tracks:
        the median pins two of them at 57000 bps, so the smallest mean the
        synthesizer can reach already rounds to 49. (Sub-kilobit tracks —
        31500 / 56500 / 56500 — do reduce to it, which is why the refusal
        is a limit of this constructor, not a claim about production.)
        """
        beets = FakeBeetsDB()
        with self.assertRaisesRegex(
            AssertionError, "not jointly expressible",
        ):
            beets.set_album_info("mbid-floored", AlbumInfo(
                album_id=1,
                track_count=3,
                min_bitrate_kbps=32,
                avg_bitrate_kbps=48,
                median_bitrate_kbps=57,
                is_cbr=False,
                album_path="/Beets/Artist/Floored",
                format="AAC",
            ))

    def test_seed_projection_checker_rejects_a_mismatched_album(self) -> None:
        """Known-bad self-test for the seed round-trip's mismatch clause.

        Driven through the real ``set_album_info``, so it also proves the
        check is wired into seeding rather than merely callable.
        """
        class _WrongSynthesis(FakeBeetsDB):
            @staticmethod
            def _synthesize_bitrates(info: AlbumInfo) -> list[int]:
                # Right min and median, wrong top track: production
                # averages this world to 53, not the 49 asked for. (Nudging
                # the top track by one kbps is NOT a mutant — 58000 still
                # reduces to 49, which is the whole point of this check.)
                return [32_000, 57_000, 70_000]

        beets = _WrongSynthesis()
        with self.assertRaisesRegex(
            AssertionError, "do not reduce to the requested AlbumInfo",
        ):
            beets.set_album_info("mbid-round", self._rounding_boundary_album())

    def test_seed_projection_checker_rejects_an_unresolvable_release(
        self,
    ) -> None:
        """Known-bad self-test for the seed round-trip's resolution clause.

        An album path that escapes the library root resolves ambiguous, so
        there is no projection to compare against — also through the real
        seeding path.
        """
        import dataclasses

        beets = FakeBeetsDB()
        with self.assertRaisesRegex(
            AssertionError, "does not resolve to a unique current release",
        ):
            beets.set_album_info("mbid-escape", dataclasses.replace(
                self._rounding_boundary_album(), album_path="../escape",
            ))

    def test_seeding_does_not_record_its_own_verification(self) -> None:
        """A seed is not an observation the test asked for."""
        beets = FakeBeetsDB()
        beets.set_album_info("mbid-round", self._rounding_boundary_album())

        self.assertEqual(beets.resolve_current_release_calls, [])
        self.assertEqual(beets.get_album_info_calls, [])

    def test_check_mbids_uses_seeded_album_exists_state(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_exists("mbid-1", True)
        beets.set_album_exists("missing", False)

        self.assertEqual(beets.check_mbids(["mbid-1", "missing"]), {"mbid-1"})
        self.assertEqual(beets.check_mbids_calls, [["mbid-1", "missing"]])

    def test_list_release_identities_returns_seeded_rows(self) -> None:
        beets = FakeBeetsDB()
        beets.set_release_identities([
            {
                "id": 7,
                "album": "Album",
                "albumartist": "Artist",
                "mb_albumid": "mbid-1",
                "discogs_albumid": None,
            },
        ])

        rows = beets.list_release_identities()

        self.assertEqual(rows[0]["mb_albumid"], "mbid-1")
        self.assertEqual(beets.list_release_identities_calls, 1)

    def test_get_all_album_ids_for_release_returns_list(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [77, 88])
        self.assertEqual(beets.get_all_album_ids_for_release("mbid-1"), [77, 88])
        # Unseeded returns empty list (matches "no row" semantics).
        self.assertEqual(beets.get_all_album_ids_for_release("mbid-other"), [])

    def test_get_item_paths_returns_list_of_pairs(self) -> None:
        beets = FakeBeetsDB()
        paths = [(11, "/Beets/01.flac"), (12, "/Beets/02.flac")]
        beets.set_item_paths("mbid-1", paths)
        self.assertEqual(beets.get_item_paths("mbid-1"), paths)
        self.assertEqual(beets.get_item_paths("mbid-other"), [])

    def test_close_is_context_manager(self) -> None:
        beets = FakeBeetsDB()
        with beets as ctx:
            self.assertIs(ctx, beets)
            self.assertEqual(beets.close_calls, 0)
        self.assertEqual(beets.close_calls, 1)


if __name__ == "__main__":
    unittest.main()
