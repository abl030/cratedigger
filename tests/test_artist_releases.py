"""Tests for lib.artist_releases — pure recording uniqueness logic."""

import unittest
from lib.artist_releases import (
    TrackInfo,
    ReleaseInfo,
    ArtistDisambiguation,
    filter_non_live,
    build_recording_map,
    mark_unique_tracks,
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


class TestBuildRecordingMap(unittest.TestCase):
    def test_single_release(self) -> None:
        releases = [
            _release("r1", "Album", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "Track B", "rec_id": "rec-2"},
            ]),
        ]
        rec_map = build_recording_map(releases)
        self.assertEqual(rec_map["rec-1"], {"r1"})
        self.assertEqual(rec_map["rec-2"], {"r1"})

    def test_shared_recording_across_releases(self) -> None:
        releases = [
            _release("r1", "Album", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "Track B", "rec_id": "rec-2"},
            ], rg_id="rg-1"),
            _release("r2", "Single", [
                {"title": "Track A", "rec_id": "rec-1"},  # same recording
                {"title": "B-side", "rec_id": "rec-3"},
            ], rg_id="rg-2"),
        ]
        rec_map = build_recording_map(releases)
        self.assertEqual(rec_map["rec-1"], {"r1", "r2"})  # shared
        self.assertEqual(rec_map["rec-2"], {"r1"})  # unique to r1
        self.assertEqual(rec_map["rec-3"], {"r2"})  # unique to r2

    def test_same_recording_different_pressings(self) -> None:
        """Two pressings of same album share recording IDs."""
        releases = [
            _release("r1", "Album (US)", [
                {"title": "Track A", "rec_id": "rec-1"},
            ], rg_id="rg-1"),
            _release("r2", "Album (UK)", [
                {"title": "Track A", "rec_id": "rec-1"},
            ], rg_id="rg-1"),
        ]
        rec_map = build_recording_map(releases)
        self.assertEqual(rec_map["rec-1"], {"r1", "r2"})

    def test_empty_releases(self) -> None:
        self.assertEqual(build_recording_map([]), {})


class TestMarkUniqueTracks(unittest.TestCase):
    def test_bonus_track_is_unique(self) -> None:
        releases = [
            _release("r1", "Album", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "Track B", "rec_id": "rec-2"},
            ], rg_id="rg-1", rg_title="Album"),
            _release("r2", "Single", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "B-side", "rec_id": "rec-3"},
            ], rg_id="rg-2", rg_title="Single"),
        ]
        rec_map = build_recording_map(releases)
        result = mark_unique_tracks(releases, rec_map)

        # Find the single
        single = [r for r in result if r.release_id == "r2"][0]
        self.assertEqual(single.unique_track_count, 1)
        bside = [t for t in single.tracks if t.title == "B-side"][0]
        self.assertTrue(bside.unique)
        self.assertEqual(bside.also_on, [])
        track_a = [t for t in single.tracks if t.title == "Track A"][0]
        self.assertFalse(track_a.unique)
        self.assertEqual(track_a.also_on, ["Album"])

    def test_album_with_no_unique_tracks(self) -> None:
        """Album whose every track also appears on a compilation."""
        releases = [
            _release("r1", "Album", [
                {"title": "Track A", "rec_id": "rec-1"},
            ], rg_id="rg-1", rg_title="Album"),
            _release("r2", "Compilation", [
                {"title": "Track A", "rec_id": "rec-1"},
            ], rg_id="rg-2", rg_title="Compilation"),
        ]
        rec_map = build_recording_map(releases)
        result = mark_unique_tracks(releases, rec_map)

        album = [r for r in result if r.release_id == "r1"][0]
        self.assertEqual(album.unique_track_count, 0)
        self.assertFalse(album.tracks[0].unique)

    def test_all_unique_on_single_release(self) -> None:
        """Solo release — all tracks are unique by definition."""
        releases = [
            _release("r1", "EP", [
                {"title": "Track A", "rec_id": "rec-1"},
                {"title": "Track B", "rec_id": "rec-2"},
            ], rg_id="rg-1", rg_title="EP"),
        ]
        rec_map = build_recording_map(releases)
        result = mark_unique_tracks(releases, rec_map)
        self.assertEqual(result[0].unique_track_count, 2)
        self.assertTrue(all(t.unique for t in result[0].tracks))

    def test_release_info_fields(self) -> None:
        releases = [
            _release(
                "r1", "My Album",
                [{"title": "Song", "rec_id": "rec-1", "length": 180000}],
                date="2020-06-15",
                rg_id="rg-1",
                rg_title="My Album RG",
                primary_type="Album",
            ),
        ]
        rec_map = build_recording_map(releases)
        result = mark_unique_tracks(releases, rec_map)

        ri = result[0]
        self.assertEqual(ri.release_id, "r1")
        self.assertEqual(ri.title, "My Album")
        self.assertEqual(ri.date, "2020-06-15")
        self.assertEqual(ri.release_group_id, "rg-1")
        self.assertEqual(ri.release_group_title, "My Album RG")
        self.assertEqual(ri.release_group_type, "Album")
        self.assertEqual(ri.track_count, 1)
        self.assertIsNone(ri.library_status)
        self.assertIsNone(ri.pipeline_status)

    def test_track_info_fields(self) -> None:
        releases = [
            _release("r1", "Album", [
                {"title": "Song", "rec_id": "rec-1", "length": 240000},
            ]),
        ]
        rec_map = build_recording_map(releases)
        result = mark_unique_tracks(releases, rec_map)
        track = result[0].tracks[0]
        self.assertEqual(track.recording_id, "rec-1")
        self.assertEqual(track.title, "Song")
        self.assertEqual(track.position, 1)
        self.assertEqual(track.disc, 1)
        self.assertAlmostEqual(track.length_seconds, 240.0)  # type: ignore[arg-type]
        self.assertTrue(track.unique)

    def test_multi_disc_release(self) -> None:
        """Multi-disc release preserves disc numbers."""
        release = {
            "id": "r1",
            "title": "Double Album",
            "date": "2020",
            "status": "Official",
            "release-group": {
                "id": "rg-1",
                "title": "Double Album",
                "primary-type": "Album",
                "secondary-types": [],
            },
            "media": [
                {
                    "position": 1,
                    "format": "CD",
                    "track-count": 1,
                    "tracks": [
                        {
                            "position": 1,
                            "number": "1",
                            "title": "Disc 1 Track",
                            "recording": {"id": "rec-1", "title": "Disc 1 Track"},
                        }
                    ],
                },
                {
                    "position": 2,
                    "format": "CD",
                    "track-count": 1,
                    "tracks": [
                        {
                            "position": 1,
                            "number": "1",
                            "title": "Disc 2 Track",
                            "recording": {"id": "rec-2", "title": "Disc 2 Track"},
                        }
                    ],
                },
            ],
        }
        rec_map = build_recording_map([release])
        result = mark_unique_tracks([release], rec_map)
        self.assertEqual(len(result[0].tracks), 2)
        self.assertEqual(result[0].tracks[0].disc, 1)
        self.assertEqual(result[0].tracks[1].disc, 2)

    def test_empty_input(self) -> None:
        self.assertEqual(mark_unique_tracks([], {}), [])

    def test_format_from_media(self) -> None:
        releases = [
            _release("r1", "Album", [
                {"title": "Song", "rec_id": "rec-1"},
            ]),
        ]
        rec_map = build_recording_map(releases)
        result = mark_unique_tracks(releases, rec_map)
        self.assertEqual(result[0].format, "CD")

    def test_null_length_handled(self) -> None:
        releases = [
            _release("r1", "Album", [
                {"title": "Song", "rec_id": "rec-1"},  # no "length" key
            ]),
        ]
        rec_map = build_recording_map(releases)
        result = mark_unique_tracks(releases, rec_map)
        self.assertIsNone(result[0].tracks[0].length_seconds)


class TestArtistDisambiguation(unittest.TestCase):
    def test_dataclass_fields(self) -> None:
        d = ArtistDisambiguation(
            artist_id="a1",
            artist_name="The National",
            releases=[],
        )
        self.assertEqual(d.artist_id, "a1")
        self.assertEqual(d.artist_name, "The National")
        self.assertEqual(d.releases, [])


if __name__ == "__main__":
    unittest.main()
