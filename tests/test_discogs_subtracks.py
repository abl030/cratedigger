"""Real-Beets contracts for Discogs indexed-subtrack representation."""

from __future__ import annotations

import unittest

from beetsplug.discogs import DiscogsPlugin
from beetsplug.discogs.types import AudioTrack, IndexTrack, Track

from harness.beets_compat import configure_discogs_subtracks


def _audio(position: str, title: str, duration: str) -> AudioTrack:
    return {
        "type_": "track",
        "position": position,
        "title": title,
        "duration": duration,
    }


_BOWIE_TRACKLIST: list[Track] = [
    _audio("A1", "Space Oddity", "5:13"),
    _audio("A2.1", "Unwashed And Somewhat Slightly Dazed", "6:09"),
    _audio("A2.2", "Don't Sit Down", "0:39"),
    _audio("A3", "Letter To Hermione", "2:30"),
    _audio("A4", "Cygnet Committee", "9:30"),
    _audio("B1", "Janine", "3:21"),
    _audio("B2", "An Occasional Dream", "3:00"),
    _audio("B3", "Wild Eyed Boy From Freecloud", "4:52"),
    _audio("B4", "God Knows I'm Good", "3:16"),
    _audio("B5", "Memory Of A Free Festival", "7:07"),
]


class TestDiscogsSubtrackCompatibility(unittest.TestCase):
    def tearDown(self) -> None:
        configure_discogs_subtracks(preserve_flat=False)

    def test_default_composite_sums_every_component_duration(self) -> None:
        configure_discogs_subtracks(preserve_flat=False)
        plugin = object.__new__(DiscogsPlugin)

        tracks = plugin._coalesce_tracks(_BOWIE_TRACKLIST)

        self.assertEqual(len(tracks), 9)
        self.assertEqual(
            tracks[1]["title"],
            "Unwashed And Somewhat Slightly Dazed / Don't Sit Down",
        )
        self.assertEqual(tracks[1]["duration"], "6:48")
        self.assertEqual(plugin.get_track_length(tracks[1]["duration"]), 408)

    def test_flat_preservation_keeps_bowie_a2_components_separate(self) -> None:
        configure_discogs_subtracks(preserve_flat=True)
        plugin = object.__new__(DiscogsPlugin)

        tracks = plugin._coalesce_tracks(_BOWIE_TRACKLIST)

        self.assertEqual(len(tracks), 10)
        self.assertEqual(
            [(tracks[1]["position"], tracks[1]["title"]),
             (tracks[2]["position"], tracks[2]["title"])],
            [
                ("A2.1", "Unwashed And Somewhat Slightly Dazed"),
                ("A2.2", "Don't Sit Down"),
            ],
        )

    def test_flat_preservation_does_not_split_nested_index_container(self) -> None:
        configure_discogs_subtracks(preserve_flat=True)
        plugin = object.__new__(DiscogsPlugin)
        nested: IndexTrack = {
            "type_": "index",
            "position": "",
            "title": "A Physical Suite",
            "duration": "8:00",
            "sub_tracks": [
                _audio("A2.1", "Part One", "4:00"),
                _audio("A2.2", "Part Two", "4:00"),
            ],
        }

        tracks = plugin._coalesce_tracks([nested])

        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0]["title"], "A Physical Suite")
        self.assertEqual(tracks[0]["position"], "A2")


if __name__ == "__main__":
    unittest.main()
