"""Real-Beets contracts for Discogs indexed-subtrack representation."""

from __future__ import annotations

import unittest

from beets import config
from beetsplug.discogs import ArtistState, DiscogsPlugin
from beetsplug.discogs.types import Artist, AudioTrack, IndexTrack, Track

from harness.beets_compat import (
    BeetsCapabilityError,
    _discogs_subtrack_methods,
    configure_discogs_subtracks,
    discogs_indexed_component_count,
    discogs_indexed_duration_complete,
)


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
        configured_plugin = object.__new__(DiscogsPlugin)
        configured_plugin.config = config["discogs"]
        configured_plugin.config.add({
            "index_tracks": False,
            "strip_disambiguation": True,
            "featured_string": "Feat.",
            "anv": {
                "artist_credit": True,
                "artist": False,
                "album_artist": False,
            },
        })
        artist: Artist = {
            "id": "1",
            "name": "David Bowie",
            "anv": "",
            "join": "",
            "role": "",
            "tracks": "",
            "resource_url": "",
        }
        artist_info = ArtistState.from_config(
            configured_plugin.config,
            [artist],
        )
        track_infos = configured_plugin.get_tracks(
            _BOWIE_TRACKLIST,
            artist_info,
        )
        self.assertEqual(
            discogs_indexed_component_count(track_infos[1]),
            2,
        )
        self.assertTrue(discogs_indexed_duration_complete(track_infos[1]))

    def test_missing_or_zero_component_duration_is_incomplete_evidence(
        self,
    ) -> None:
        configure_discogs_subtracks(preserve_flat=False)
        plugin = object.__new__(DiscogsPlugin)

        for unproved_duration in ("", "0:00"):
            with self.subTest(duration=unproved_duration):
                merged = plugin._merge_subtracks([
                    _audio("A2.1", "Part One", "4:00"),
                    _audio("A2.2", "Part Two", unproved_duration),
                ])

                merged_fields: dict[str, object] = dict(merged)
                self.assertEqual(
                    merged_fields.get(
                        "_cratedigger_discogs_indexed_component_count"
                    ),
                    2,
                )
                self.assertFalse(
                    merged_fields.get(
                        "_cratedigger_discogs_indexed_duration_complete"
                    )
                )

    def test_older_discogs_plugin_without_coalescing_seam_is_noop(self) -> None:
        class LegacyDiscogsPlugin:
            def get_tracks(self) -> list[object]:
                return []

        self.assertIsNone(_discogs_subtrack_methods(LegacyDiscogsPlugin))

    def test_partial_discogs_coalescing_seam_fails_closed(self) -> None:
        class PartialDiscogsPlugin:
            def _subtrack_position(self) -> None:
                return None

        with self.assertRaisesRegex(
            BeetsCapabilityError,
            "lacks callable _merge_subtracks",
        ):
            _discogs_subtrack_methods(PartialDiscogsPlugin)

    def test_partial_legacy_discogs_coalescing_seam_fails_closed(self) -> None:
        class PartialLegacyDiscogsPlugin:
            def _coalesce_tracks(self) -> list[object]:
                return []

        with self.assertRaisesRegex(
            BeetsCapabilityError,
            "lacks callable _add_merged_subtracks",
        ):
            _discogs_subtrack_methods(PartialLegacyDiscogsPlugin)

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


class TestDiscogsHeadingRowExclusion(unittest.TestCase):
    """Heading rows never become candidate tracks (the Tiny Dots shape).

    Live evidence (request 6937 / Discogs 8439330, download_log 40576):
    two section-label rows survived candidate construction as zero-length
    tracks, inflating the candidate to 14 tracks for a 12-file rip —
    `extra_tracks` rejection at distance 0.5669 for a copy whose true
    mapping is 12↔12. The deployment's beets reads the DISCOGS MIRROR
    (nix/beets.nix repoints discogs_client), which serves headings
    RETYPED type_=='track' with empty position/duration — so upstream
    Beets' own heading handling never fires. The pipeline's manifests
    drop headings (`lib/discogs_positions.py`, same measured shape
    rule); the validation-side candidate must agree.
    """

    def tearDown(self) -> None:
        configure_discogs_subtracks(preserve_flat=False)

    def _configured_plugin(self) -> DiscogsPlugin:
        plugin = object.__new__(DiscogsPlugin)
        plugin.config = config["discogs"]
        plugin.config.add({
            "index_tracks": False,
            "strip_disambiguation": True,
            "featured_string": "Feat.",
            "anv": {
                "artist_credit": True,
                "artist": False,
                "album_artist": False,
            },
        })
        return plugin

    def _artist_state(self, plugin: DiscogsPlugin) -> ArtistState:
        artist: Artist = {
            "id": "1",
            "name": "La Dispute",
            "anv": "",
            "join": "",
            "role": "",
            "tracks": "",
            "resource_url": "",
        }
        return ArtistState.from_config(plugin.config, [artist])

    def _heading(self, title: str) -> Track:
        return {
            "type_": "heading",
            "position": "",
            "title": title,
            "duration": "",
        }

    def test_typed_heading_rows_are_excluded_from_candidates(self) -> None:
        configure_discogs_subtracks(preserve_flat=False)
        plugin = self._configured_plugin()
        tracklist: list[Track] = [
            self._heading('Selections From "Tiny Dots" Original Score'),
            _audio("A1", "A", "2:00"),
            _audio("A2", "B", "2:00"),
            self._heading("Selections From Live Seated Performance"),
            _audio("B1", "A Departure (live)", "4:00"),
            _audio("B2", "For Mayor in Splitsville (live)", "4:00"),
        ]

        track_infos = plugin.get_tracks(tracklist, self._artist_state(plugin))

        self.assertEqual(len(track_infos), 4)
        titles = [t.title for t in track_infos]
        self.assertNotIn(
            'Selections From "Tiny Dots" Original Score', titles)
        # A/B are two sides of ONE vinyl medium (sides_per_medium=2).
        # NOTE: typed headings are already excluded by the pinned Beets'
        # own TracklistState.build, so this pin alone cannot kill a
        # filter-removal mutant — the mirror-retyped pin below is the
        # decisive one. This branch is fail-closed legislation for a
        # typed heading carrying a duration (see the direct filter pin).
        self.assertEqual({t.medium for t in track_infos}, {1})

    @staticmethod
    def _mirror_heading(title: str) -> Track:
        return {
            "type_": "track",
            "position": "",
            "title": title,
            "duration": "",
        }

    def test_mirror_retyped_heading_rows_are_excluded(self) -> None:
        # The DECISIVE live shape: the mirror serves section labels as
        # type_=='track' with empty position and duration (probed on
        # /releases/8439330). Upstream Beets keeps these as zero-length
        # tracks that each start a phantom medium — only the compat
        # filter's shape rule removes them. Both subtrack modes: the
        # preserve_flat=True observational rerun is a live path (it is
        # triggered by exactly the unmapped-audio situation headings
        # cause).
        for preserve_flat in (False, True):
            with self.subTest(preserve_flat=preserve_flat):
                configure_discogs_subtracks(preserve_flat=preserve_flat)
                plugin = self._configured_plugin()
                tracklist: list[Track] = [
                    self._mirror_heading(
                        'Selections From "Tiny Dots" Original Score'),
                    _audio("A1", "A", "6:44"),
                    _audio("A2", "B", "2:32"),
                    self._mirror_heading(
                        "Selections From Live Seated Performance"),
                    _audio("B1", "A Departure", "3:33"),
                    _audio("B2", "For Mayor In Splitsville", "3:29"),
                ]

                track_infos = plugin.get_tracks(
                    tracklist, self._artist_state(plugin))

                self.assertEqual(len(track_infos), 4)
                self.assertEqual({t.medium for t in track_infos}, {1})

    def test_positionless_release_keeps_every_row(self) -> None:
        # R1: a release that positions NOTHING has no heading signal —
        # the acquisition manifest (lib/discogs_positions.py) keeps all
        # rows, so the candidate must count the same tracks.
        configure_discogs_subtracks(preserve_flat=False)
        plugin = self._configured_plugin()
        tracklist: list[Track] = [
            {"type_": "track", "position": "", "title": "Untitled One",
             "duration": "3:00"},
            {"type_": "track", "position": "", "title": "Untitled Two",
             "duration": ""},
            {"type_": "track", "position": "", "title": "Untitled Three",
             "duration": "2:30"},
        ]

        track_infos = plugin.get_tracks(tracklist, self._artist_state(plugin))

        self.assertEqual(len(track_infos), 3)

    def test_shape_rule_is_a_conjunction(self) -> None:
        # R3: each conjunct alone is catastrophic — position-only eats
        # the ambiguous positionless-but-timed row; duration-only eats
        # every untimed REAL track (routine on Discogs). Both survive.
        configure_discogs_subtracks(preserve_flat=False)
        plugin = self._configured_plugin()
        tracklist: list[Track] = [
            self._mirror_heading("Heading"),
            _audio("A1", "Untimed Real Track", ""),
            {"type_": "track", "position": "", "title": "Timed Ambiguous",
             "duration": "2:00"},
            _audio("A2", "Ordinary", "3:00"),
        ]

        track_infos = plugin.get_tracks(tracklist, self._artist_state(plugin))

        titles = [t.title for t in track_infos]
        self.assertEqual(len(track_infos), 3)
        self.assertIn("Untimed Real Track", titles)
        self.assertIn("Timed Ambiguous", titles)
        self.assertNotIn("Heading", titles)

    def test_typed_heading_with_duration_is_still_dropped(self) -> None:
        # R5: the typed branch's own observable pin — beets' data model
        # allows a heading to carry a duration, and the type_ marker
        # outranks the shape. Only the direct filter call can see this
        # (upstream drops typed headings through get_tracks regardless).
        from harness.beets_compat import filter_discogs_heading_rows

        rows: list[dict[str, object]] = [
            {"type_": "heading", "position": "", "title": "Timed Heading",
             "duration": "3:00"},
            {"type_": "track", "position": "A1", "title": "Real",
             "duration": "3:00"},
        ]
        kept = filter_discogs_heading_rows(rows)
        self.assertEqual([r["title"] for r in kept], ["Real"])

    def test_heading_inside_flat_subtrack_run_keeps_program_marking(
        self,
    ) -> None:
        # R6: the pre-pass coalesce dry-run and the real get_tracks call
        # must see the SAME filtered list — a mirror heading interrupting
        # a flat A2.1/A2.2 run would otherwise split the groupby and lose
        # the indexed-program marker on the merged composite.
        configure_discogs_subtracks(preserve_flat=False)
        plugin = self._configured_plugin()
        tracklist: list[Track] = [
            _audio("A1", "Opener", "3:00"),
            _audio("A2.1", "Part One", "4:00"),
            self._mirror_heading("Interrupting Heading"),
            _audio("A2.2", "Part Two", "4:00"),
        ]

        track_infos = plugin.get_tracks(tracklist, self._artist_state(plugin))

        self.assertEqual(len(track_infos), 2)
        composite = track_infos[1]
        self.assertEqual(discogs_indexed_component_count(composite), 2)
        self.assertTrue(discogs_indexed_duration_complete(composite))

    def test_all_heading_tracklist_is_left_untouched(self) -> None:
        # Pathological upstream data: every row a heading. The filter
        # must never manufacture an empty candidate from non-empty
        # input — the unfiltered list passes through to Beets.
        from harness.beets_compat import filter_discogs_heading_rows

        tracklist: list[dict[str, object]] = [
            dict(self._heading("Section One")),
            dict(self._heading("Section Two")),
        ]
        self.assertEqual(
            filter_discogs_heading_rows(list(tracklist)), tracklist)

    def test_nested_index_parent_is_never_dropped(self) -> None:
        # An index parent carries real audio in sub_tracks — kept and
        # coalesced by Beets' own path, in both subtrack modes.
        for preserve_flat in (False, True):
            with self.subTest(preserve_flat=preserve_flat):
                configure_discogs_subtracks(preserve_flat=preserve_flat)
                plugin = self._configured_plugin()
                nested: IndexTrack = {
                    "type_": "index",
                    "position": "",
                    "title": "A Physical Suite",
                    "duration": "",
                    "sub_tracks": [
                        _audio("A2.1", "Part One", "4:00"),
                        _audio("A2.2", "Part Two", "4:00"),
                    ],
                }
                tracklist: list[Track] = [
                    _audio("A1", "Opener", "3:00"),
                    nested,
                ]

                track_infos = plugin.get_tracks(
                    tracklist, self._artist_state(plugin))

                self.assertEqual(len(track_infos), 2)


if __name__ == "__main__":
    unittest.main()
