"""Tests for lib.artist_releases — release group coverage analysis."""

import unittest
from lib.artist_releases import (
    TrackInfo,
    ReleaseGroupInfo,
    ArtistDisambiguation,
    filter_non_live,
    analyse_artist_releases,
)


def _release(
    release_id: str,
    title: str,
    tracks: list[dict],
    *,
    primary_type: str = "Album",
    secondary_types: list[str] | None = None,
    date: str = "2020",
    rg_id: str = "rg-1",
    rg_title: str = "RG Title",
    status: str = "Official",
) -> dict:
    """Build a fake MB release dict matching the shape from the API."""
    return {
        "id": release_id,
        "title": title,
        "date": date,
        "status": status,
        "release-group": {
            "id": rg_id,
            "title": rg_title,
            "primary-type": primary_type,
            "secondary-types": secondary_types or [],
        },
        "media": [
            {
                "position": 1,
                "format": "CD",
                "track-count": len(tracks),
                "tracks": [
                    {
                        "position": i + 1,
                        "number": str(i + 1),
                        "title": t["title"],
                        "length": t.get("length"),
                        "recording": {"id": t["rec_id"], "title": t["title"]},
                    }
                    for i, t in enumerate(tracks)
                ],
            }
        ],
    }


class TestFilterNonLive(unittest.TestCase):
    def test_removes_live_albums(self) -> None:
        releases = [
            _release("r1", "Studio Album", [], primary_type="Album"),
            _release("r2", "Live Album", [], primary_type="Album", secondary_types=["Live"]),
        ]
        result = filter_non_live(releases)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "r1")

    def test_removes_live_broadcasts(self) -> None:
        releases = [
            _release("r1", "EP", [], primary_type="EP"),
            _release("r2", "Broadcast", [], primary_type="Broadcast", secondary_types=["Live"]),
        ]
        result = filter_non_live(releases)
        self.assertEqual(len(result), 1)

    def test_keeps_studio_single_ep_compilation(self) -> None:
        releases = [
            _release("r1", "Album", [], primary_type="Album"),
            _release("r2", "Single", [], primary_type="Single"),
            _release("r3", "EP", [], primary_type="EP"),
            _release("r4", "Compilation", [], primary_type="Album", secondary_types=["Compilation"]),
        ]
        result = filter_non_live(releases)
        self.assertEqual(len(result), 4)

    def test_removes_live_ep(self) -> None:
        releases = [
            _release("r1", "Live EP", [], primary_type="EP", secondary_types=["Live"]),
        ]
        result = filter_non_live(releases)
        self.assertEqual(len(result), 0)

    def test_empty_input(self) -> None:
        self.assertEqual(filter_non_live([]), [])


class TestAnalyseArtistReleases(unittest.TestCase):
    """Tests for the tier-based release group coverage algorithm."""

    def test_single_covered_by_album(self) -> None:
        """A single whose track also appears on an album is fully covered."""
        releases = [
            _release("r1", "Album", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "Track B", "rec_id": "rec-2"},
            ], rg_id="rg-album", rg_title="The Album", primary_type="Album"),
            _release("r2", "Track A", [
                {"title": "Track A", "rec_id": "rec-1"},
            ], rg_id="rg-single", rg_title="Track A", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        album_rg = [rg for rg in result if rg.release_group_id == "rg-album"][0]
        single_rg = [rg for rg in result if rg.release_group_id == "rg-single"][0]

        self.assertEqual(album_rg.unique_track_count, 2)
        self.assertIsNone(album_rg.covered_by)
        self.assertEqual(single_rg.unique_track_count, 0)
        self.assertEqual(single_rg.covered_by, "The Album")

    def test_single_with_bside_has_unique(self) -> None:
        """A single with a B-side not on any album has unique tracks."""
        releases = [
            _release("r1", "Album", [
                {"title": "Track A", "rec_id": "rec-1"},
            ], rg_id="rg-album", rg_title="The Album", primary_type="Album"),
            _release("r2", "Single", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "B-side", "rec_id": "rec-99"},
            ], rg_id="rg-single", rg_title="The Single", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        single_rg = [rg for rg in result if rg.release_group_id == "rg-single"][0]

        self.assertEqual(single_rg.unique_track_count, 1)
        self.assertIsNone(single_rg.covered_by)
        bside = [t for t in single_rg.tracks if t.title == "B-side"][0]
        self.assertTrue(bside.unique)

    def test_ep_covers_singles(self) -> None:
        """An EP that is a superset of singles covers them."""
        releases = [
            _release("r1", "EP", [
                {"title": "Song A", "rec_id": "rec-1"},
                {"title": "Song B", "rec_id": "rec-2"},
                {"title": "Song C", "rec_id": "rec-3"},
            ], rg_id="rg-ep", rg_title="The EP", primary_type="EP"),
            _release("r2", "Song A", [
                {"title": "Song A", "rec_id": "rec-1"},
            ], rg_id="rg-s1", rg_title="Song A", primary_type="Single"),
            _release("r3", "Song B", [
                {"title": "Song B", "rec_id": "rec-2"},
            ], rg_id="rg-s2", rg_title="Song B", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        ep = [rg for rg in result if rg.release_group_id == "rg-ep"][0]
        s1 = [rg for rg in result if rg.release_group_id == "rg-s1"][0]
        s2 = [rg for rg in result if rg.release_group_id == "rg-s2"][0]

        self.assertEqual(ep.unique_track_count, 3)
        self.assertIsNone(ep.covered_by)
        self.assertEqual(s1.covered_by, "The EP")
        self.assertEqual(s2.covered_by, "The EP")

    def test_pressings_union_recordings(self) -> None:
        """Multiple pressings of the same RG union their recordings."""
        releases = [
            _release("r1", "EP (US)", [
                {"title": "Track A", "rec_id": "rec-1"},
            ], rg_id="rg-ep", rg_title="EP", primary_type="EP"),
            _release("r2", "EP (JP)", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "Bonus", "rec_id": "rec-2"},
            ], rg_id="rg-ep", rg_title="EP", primary_type="EP"),
            _release("r3", "Bonus Single", [
                {"title": "Bonus", "rec_id": "rec-2"},
            ], rg_id="rg-single", rg_title="Bonus Single", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        # The EP's union includes rec-1 and rec-2, covering the single
        single = [rg for rg in result if rg.release_group_id == "rg-single"][0]
        self.assertEqual(single.covered_by, "EP")

    def test_same_tier_larger_covers_smaller(self) -> None:
        """At same tier, larger RG covers smaller if it's a superset."""
        releases = [
            _release("r1", "Big EP", [
                {"title": "A", "rec_id": "rec-1"},
                {"title": "B", "rec_id": "rec-2"},
                {"title": "C", "rec_id": "rec-3"},
            ], rg_id="rg-big", rg_title="Big EP", primary_type="EP", date="2020"),
            _release("r2", "Small EP", [
                {"title": "A", "rec_id": "rec-1"},
                {"title": "B", "rec_id": "rec-2"},
            ], rg_id="rg-small", rg_title="Small EP", primary_type="EP", date="2020"),
        ]
        result = analyse_artist_releases(releases)
        small = [rg for rg in result if rg.release_group_id == "rg-small"][0]
        self.assertEqual(small.covered_by, "Big EP")

    def test_partial_overlap_both_have_unique(self) -> None:
        """Two EPs with partial overlap — neither covers the other."""
        releases = [
            _release("r1", "EP One", [
                {"title": "A", "rec_id": "rec-1"},
                {"title": "B", "rec_id": "rec-2"},
            ], rg_id="rg-1", rg_title="EP One", primary_type="EP"),
            _release("r2", "EP Two", [
                {"title": "B", "rec_id": "rec-2"},
                {"title": "C", "rec_id": "rec-3"},
            ], rg_id="rg-2", rg_title="EP Two", primary_type="EP"),
        ]
        result = analyse_artist_releases(releases)
        ep1 = [rg for rg in result if rg.release_group_id == "rg-1"][0]
        ep2 = [rg for rg in result if rg.release_group_id == "rg-2"][0]

        self.assertIsNone(ep1.covered_by)
        self.assertIsNone(ep2.covered_by)
        # Neither covers the other, so shared rec-2 is unique to both
        self.assertEqual(ep1.unique_track_count, 2)
        self.assertEqual(ep2.unique_track_count, 2)

    def test_track_also_on_shows_covering_rg(self) -> None:
        """Non-unique tracks show which RGs also contain them."""
        releases = [
            _release("r1", "Album", [
                {"title": "Hit", "rec_id": "rec-1"},
                {"title": "Deep Cut", "rec_id": "rec-2"},
            ], rg_id="rg-album", rg_title="The Album", primary_type="Album"),
            _release("r2", "Hit", [
                {"title": "Hit", "rec_id": "rec-1"},
            ], rg_id="rg-single", rg_title="Hit", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        single = [rg for rg in result if rg.release_group_id == "rg-single"][0]
        hit = single.tracks[0]
        self.assertFalse(hit.unique)
        self.assertIn("The Album", hit.also_on)

    def test_album_tracks_not_marked_also_on_single(self) -> None:
        """Album tracks shouldn't list lower-tier duplicates in also_on."""
        releases = [
            _release("r1", "Album", [
                {"title": "Hit", "rec_id": "rec-1"},
            ], rg_id="rg-album", rg_title="The Album", primary_type="Album"),
            _release("r2", "Hit Single", [
                {"title": "Hit", "rec_id": "rec-1"},
            ], rg_id="rg-single", rg_title="Hit Single", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        album = [rg for rg in result if rg.release_group_id == "rg-album"][0]
        # Album track is unique (it's the highest tier) — not "also on" the single
        self.assertTrue(album.tracks[0].unique)
        self.assertEqual(album.tracks[0].also_on, [])

    def test_rg_info_fields(self) -> None:
        releases = [
            _release("r1", "My Album", [
                {"title": "Song", "rec_id": "rec-1", "length": 180000},
            ], date="2020-06-15", rg_id="rg-1", rg_title="My Album", primary_type="Album"),
        ]
        result = analyse_artist_releases(releases)
        rg = result[0]
        self.assertEqual(rg.release_group_id, "rg-1")
        self.assertEqual(rg.title, "My Album")
        self.assertEqual(rg.primary_type, "Album")
        self.assertEqual(rg.track_count, 1)
        self.assertIsNone(rg.covered_by)

    def test_track_info_fields(self) -> None:
        releases = [
            _release("r1", "Album", [
                {"title": "Song", "rec_id": "rec-1", "length": 240000},
            ]),
        ]
        result = analyse_artist_releases(releases)
        track = result[0].tracks[0]
        self.assertEqual(track.recording_id, "rec-1")
        self.assertEqual(track.title, "Song")
        self.assertTrue(track.unique)
        self.assertEqual(track.also_on, [])

    def test_empty_input(self) -> None:
        self.assertEqual(analyse_artist_releases([]), [])

    def test_release_ids_and_formats(self) -> None:
        """ReleaseGroupInfo should list all release IDs and formats."""
        releases = [
            _release("r1", "EP (CD)", [
                {"title": "A", "rec_id": "rec-1"},
            ], rg_id="rg-1", rg_title="EP", primary_type="EP"),
        ]
        result = analyse_artist_releases(releases)
        rg = result[0]
        self.assertIn("r1", rg.release_ids)

    def test_christmas_ep_covers_singles(self) -> None:
        """Real-world: EP bundles tracks from multiple singles — singles are covered."""
        releases = [
            _release("r1", "So Much Wine", [
                {"title": "So Much Wine", "rec_id": "rec-1"},
                {"title": "Day After Tomorrow", "rec_id": "rec-2"},
                {"title": "If We Make It", "rec_id": "rec-3"},
                {"title": "Christmas Song", "rec_id": "rec-4"},
            ], rg_id="rg-ep", rg_title="So Much Wine", primary_type="EP"),
            _release("r2", "So Much Wine", [
                {"title": "So Much Wine", "rec_id": "rec-1"},
            ], rg_id="rg-s1", rg_title="So Much Wine", primary_type="Single"),
            _release("r3", "Day After Tomorrow", [
                {"title": "Day After Tomorrow", "rec_id": "rec-2"},
            ], rg_id="rg-s2", rg_title="Day After Tomorrow", primary_type="Single"),
            _release("r4", "If We Make It", [
                {"title": "If We Make It", "rec_id": "rec-3"},
                {"title": "Day After Tomorrow", "rec_id": "rec-2"},
            ], rg_id="rg-s3", rg_title="If We Make It", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        ep = [rg for rg in result if rg.release_group_id == "rg-ep"][0]
        s1 = [rg for rg in result if rg.release_group_id == "rg-s1"][0]
        s2 = [rg for rg in result if rg.release_group_id == "rg-s2"][0]
        s3 = [rg for rg in result if rg.release_group_id == "rg-s3"][0]

        self.assertIsNone(ep.covered_by)
        self.assertEqual(ep.unique_track_count, 4)
        self.assertEqual(s1.covered_by, "So Much Wine")
        self.assertEqual(s2.covered_by, "So Much Wine")
        self.assertEqual(s3.covered_by, "So Much Wine")


    def test_within_rg_dedup_by_title(self) -> None:
        """Same song on different pressings within a RG should show once in track list."""
        releases = [
            _release("r1", "Album (CD)", [
                {"title": "ICU", "rec_id": "rec-1"},
                {"title": "Kyoto", "rec_id": "rec-2"},
            ], rg_id="rg-1", rg_title="Punisher", primary_type="Album"),
            _release("r2", "Album (Vinyl)", [
                {"title": "ICU", "rec_id": "rec-10"},  # different recording ID!
                {"title": "Kyoto", "rec_id": "rec-11"},
            ], rg_id="rg-1", rg_title="Punisher", primary_type="Album"),
        ]
        result = analyse_artist_releases(releases)
        rg = result[0]
        titles = [t.title for t in rg.tracks]
        self.assertEqual(len(titles), 2)  # ICU and Kyoto, not 4
        self.assertIn("ICU", titles)
        self.assertIn("Kyoto", titles)

    def test_within_rg_dedup_remaster_suffix(self) -> None:
        """'Song (Remaster)' and 'Song' within same RG should deduplicate."""
        releases = [
            _release("r1", "Album", [
                {"title": "Song", "rec_id": "rec-1"},
            ], rg_id="rg-1", rg_title="Album", primary_type="Album"),
            _release("r2", "Album (Remaster)", [
                {"title": "Song (2024 Remaster)", "rec_id": "rec-2"},
            ], rg_id="rg-1", rg_title="Album", primary_type="Album"),
        ]
        result = analyse_artist_releases(releases)
        rg = result[0]
        self.assertEqual(len(rg.tracks), 1)

    def test_within_rg_dedup_keeps_all_recording_ids(self) -> None:
        """Even with title dedup, all recording IDs are in the coverage set."""
        releases = [
            _release("r1", "Album", [
                {"title": "ICU", "rec_id": "rec-1"},
            ], rg_id="rg-1", rg_title="Album", primary_type="Album"),
            _release("r2", "Album (Vinyl)", [
                {"title": "ICU", "rec_id": "rec-10"},
            ], rg_id="rg-1", rg_title="Album", primary_type="Album"),
            # Single with rec-10 should still be covered
            _release("r3", "ICU Single", [
                {"title": "ICU", "rec_id": "rec-10"},
            ], rg_id="rg-single", rg_title="ICU", primary_type="Single"),
        ]
        result = analyse_artist_releases(releases)
        single = [rg for rg in result if rg.release_group_id == "rg-single"][0]
        self.assertEqual(single.covered_by, "Album")

    def test_bonus_tracks_still_shown(self) -> None:
        """Bonus tracks on deluxe pressings still appear in track list."""
        releases = [
            _release("r1", "Album", [
                {"title": "Song A", "rec_id": "rec-1"},
            ], rg_id="rg-1", rg_title="Album", primary_type="Album"),
            _release("r2", "Album (Deluxe)", [
                {"title": "Song A", "rec_id": "rec-1"},
                {"title": "Bonus Track", "rec_id": "rec-99"},
            ], rg_id="rg-1", rg_title="Album", primary_type="Album"),
        ]
        result = analyse_artist_releases(releases)
        rg = result[0]
        titles = [t.title for t in rg.tracks]
        self.assertEqual(len(titles), 2)
        self.assertIn("Song A", titles)
        self.assertIn("Bonus Track", titles)


class TestArtistDisambiguation(unittest.TestCase):
    def test_dataclass_fields(self) -> None:
        d = ArtistDisambiguation(
            artist_id="a1",
            artist_name="The National",
            release_groups=[],
        )
        self.assertEqual(d.artist_id, "a1")
        self.assertEqual(d.artist_name, "The National")
        self.assertEqual(d.release_groups, [])


if __name__ == "__main__":
    unittest.main()
