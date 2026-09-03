"""Unit tests for web/discogs.py — Discogs mirror API wrapper."""
import json
import os
import sys
import unittest
import urllib.parse
from typing import ClassVar, TypeGuard
from unittest.mock import MagicMock, patch

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import web.discogs


def _is_dict(value: object) -> TypeGuard[dict[str, object]]:
    """Narrow one of ``get_release()``/``get_master_releases()``'s
    ``dict[str, object]`` nested values for a test assertion."""
    return isinstance(value, dict)


def _is_list(value: object) -> TypeGuard[list[object]]:
    """Narrow one of ``get_release()``/``get_master_releases()``'s
    ``dict[str, object]`` nested list values for a test assertion."""
    return isinstance(value, list)


def setUpModule() -> None:
    # These tests exercise the REAL web/discogs.py with urlopen patched.
    # Since tier-2 U6 the module ships with NO default base (Discogs is
    # mirror-required, R13) — give the suite a synthetic mirror origin so
    # URL construction proceeds; assertions check paths, not the origin.
    web.discogs.DISCOGS_API_BASE = "https://discogs-mirror.test"


def tearDownModule() -> None:
    web.discogs.DISCOGS_API_BASE = None


from lib.discogs_positions import (
    parse_duration as _parse_duration,
)
from lib.discogs_positions import (
    parse_position as _parse_position,
)
from web.discogs import (
    LabelEntity,
    _DiscogsArtistRef,
    _parse_year,
    _primary_artist_name,
    get_artist_name,
    get_artist_releases,
    get_label,
    get_label_releases,
    get_master_releases,
    get_release,
    get_release_raw,
    search_artists,
    search_labels,
    search_releases,
)


class TestParseDuration(unittest.TestCase):
    CASES: ClassVar = [
        ("normal", "4:44", 284.0),
        ("short", "0:30", 30.0),
        ("long", "1:02:15", 3735.0),
        ("empty", "", None),
        ("none", None, None),
        ("invalid", "abc", None),
    ]

    def test_parse_duration(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(_parse_duration(input_val), expected)


class TestParsePosition(unittest.TestCase):
    CASES: ClassVar = [
        ("simple number", "3", (1, 3)),
        ("cd disc-track", "2-5", (2, 5)),
        ("vinyl side", "A1", (1, 1)),
        ("vinyl side B", "B3", (2, 3)),
        ("empty", "", (1, 0)),
        ("cd sub-position", "10.1", (1, 10)),
        ("vinyl sub-position", "A2.2", (1, 2)),
        ("disc-track sub-position", "2-5.3", (2, 5)),
        ("bare vinyl side A", "A", (1, 1)),
        ("bare vinyl side B", "B", (2, 1)),
    ]

    def test_parse_position(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(_parse_position(input_val), expected)


class TestParseYear(unittest.TestCase):
    CASES: ClassVar = [
        ("full date", "1997-06-16", 1997),
        ("year only", "2020", 2020),
        ("empty", "", None),
        ("none", None, None),
    ]

    def test_parse_year(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(_parse_year(input_val), expected)


class TestPrimaryArtistName(unittest.TestCase):
    def test_with_artists(self):
        self.assertEqual(
            _primary_artist_name([_DiscogsArtistRef(id=1, name="Radiohead")]),
            "Radiohead",
        )

    def test_empty(self):
        self.assertEqual(_primary_artist_name([]), "Unknown")


def _mock_urlopen(response_data):
    """Create a mock for urllib.request.urlopen that returns JSON data."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(response_data).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return patch("web.discogs.urllib.request.urlopen", return_value=mock_resp)


class TestGetRelease(unittest.TestCase):
    RELEASE_DATA: ClassVar = {
        "id": 83182,
        "title": "OK Computer",
        "country": "Europe",
        "released": "1997-06-16",
        "master_id": 21491,
        "artists": [{"id": 3840, "name": "Radiohead", "role": "", "anv": ""}],
        "labels": [{"id": 2294, "name": "Parlophone", "catno": "NODATA 02"}],
        "formats": [{"name": "CD", "qty": 1, "descriptions": "Album"}],
        "tracks": [
            {"position": "1", "title": "Airbag", "duration": "4:44", "artists": []},
            {"position": "2", "title": "Paranoid Android", "duration": "6:23", "artists": []},
        ],
    }

    def test_normalizes_release(self):
        with _mock_urlopen(self.RELEASE_DATA):
            result = get_release(83182)

        self.assertEqual(result["id"], "83182")
        self.assertEqual(result["title"], "OK Computer")
        self.assertEqual(result["artist_name"], "Radiohead")
        self.assertEqual(result["artist_id"], "3840")
        self.assertEqual(result["release_group_id"], "21491")
        self.assertEqual(result["year"], 1997)
        self.assertEqual(result["country"], "Europe")
        tracks = result["tracks"]
        assert _is_list(tracks)
        self.assertEqual(len(tracks), 2)
        track0 = tracks[0]
        assert _is_dict(track0)
        self.assertEqual(track0["title"], "Airbag")
        self.assertEqual(track0["disc_number"], 1)
        self.assertEqual(track0["track_number"], 1)
        self.assertEqual(track0["length_seconds"], 284.0)

    def test_raw_release_preserves_literal_positions_and_subtracks(self):
        raw_data = {
            **self.RELEASE_DATA,
            "tracks": [{"position": "A2", "title": "Index", "sub_tracks": [
                {"position": "A2.1", "title": "Part One", "duration": "1:00"},
                {"position": "A2.2", "title": "Part Two", "duration": "2:00"},
            ]}],
        }
        with _mock_urlopen(raw_data):
            raw = get_release_raw(83182, fresh=True)
        tracks = raw["tracks"]
        assert _is_list(tracks)
        row = tracks[0]
        assert _is_dict(row)
        self.assertEqual(row["position"], "A2")
        children = row["sub_tracks"]
        assert _is_list(children)
        child = children[1]
        assert _is_dict(child)
        self.assertEqual(child["position"], "A2.2")
        with _mock_urlopen(raw_data):
            slim = get_release(83182, fresh=True)
        slim_tracks = slim["tracks"]
        assert _is_list(slim_tracks)
        self.assertEqual(slim_tracks[0], {
            "disc_number": 1, "track_number": 2, "title": "Index",
            "length_seconds": None,
        })


class TestGetReleaseSubPositionTracks(unittest.TestCase):
    """Issue #1261 — flat ``N.M`` sub-positions collapse to rip-shaped rows.

    Discogs encodes hidden-track runs as sub-positions of one physical
    track; a rip of the disc has ONE file for that position. The slim
    ``get_release()`` manifest must count tracks the way a rip does, or
    the matcher count gate rejects every real copy forever.
    """

    @staticmethod
    def _release(release_id: int, tracks: list[dict[str, object]]) -> dict[str, object]:
        return {
            "id": release_id,
            "title": "Harmonies For The Haunted",
            "country": "US",
            "released": "2005-09-13",
            "master_id": 158925,
            "artists": [{"id": 272594, "name": "Stellastarr*", "role": "", "anv": ""}],
            "labels": [],
            "formats": [{"name": "CD", "qty": 1, "descriptions": "Album"}],
            "tracks": tracks,
        }

    def _tracks(self, release_id: int, tracks: list[dict[str, object]]) -> list[object]:
        with _mock_urlopen(self._release(release_id, tracks)):
            result = get_release(release_id, fresh=True)
        out = result["tracks"]
        assert _is_list(out)
        return out

    def test_hidden_track_subgroup_collapses_to_single_track(self):
        # The live defect shape: Discogs release 521474 (request 6188).
        tracks = self._tracks(521474, [
            {"position": "1", "title": "Lost In Time", "duration": "4:31"},
            {"position": "2", "title": "Damn This Foolish Heart", "duration": "3:30"},
            {"position": "3", "title": "The Diver", "duration": "4:32"},
            {"position": "4", "title": "Sweet Troubled Soul", "duration": "4:07"},
            {"position": "5", "title": "Born In A Fleamarket", "duration": "2:34"},
            {"position": "6", "title": "On My Own", "duration": "4:52"},
            {"position": "7", "title": "When I Disappeaar", "duration": "3:48"},
            {"position": "8", "title": "Love And Longing", "duration": "4:17"},
            {"position": "9", "title": "Stay Entertained", "duration": "3:46"},
            {"position": "10.1", "title": "Island Lost At Sea", "duration": "3:46"},
            {"position": "10.2", "title": "(silence)", "duration": "0:20"},
            {"position": "10.3", "title": "Untitled", "duration": "3:49"},
        ])
        self.assertEqual(len(tracks), 10)
        numbers = [t["track_number"] for t in tracks if _is_dict(t)]
        self.assertEqual(numbers, list(range(1, 11)))
        last = tracks[9]
        assert _is_dict(last)
        self.assertEqual(last["title"], "Island Lost At Sea")
        self.assertEqual(last["length_seconds"], 226.0 + 20.0 + 229.0)
        # Source data passes through verbatim otherwise — the Discogs
        # typo is not ours to fix.
        seventh = tracks[6]
        assert _is_dict(seventh)
        self.assertEqual(seventh["title"], "When I Disappeaar")

    def test_leading_silence_sub_entry_yields_first_real_title(self):
        tracks = self._tracks(910001, [
            {"position": "1", "title": "Opener", "duration": "3:00"},
            {"position": "2.1", "title": "(silence)", "duration": "0:10"},
            {"position": "2.2", "title": "Hidden Song", "duration": "3:00"},
        ])
        self.assertEqual(len(tracks), 2)
        hidden = tracks[1]
        assert _is_dict(hidden)
        self.assertEqual(hidden["track_number"], 2)
        self.assertEqual(hidden["title"], "Hidden Song")
        self.assertEqual(hidden["length_seconds"], 190.0)

    def test_title_containing_silence_is_not_a_placeholder(self):
        # Only exact "(silence)"-style placeholders are skipped — a real
        # song title that merely contains the word keeps its slot.
        tracks = self._tracks(910006, [
            {"position": "1.1", "title": "Silence Kit", "duration": "5:00"},
            {"position": "1.2", "title": "Untitled", "duration": "1:00"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["title"], "Silence Kit")

    def test_all_silence_subgroup_keeps_first_title(self):
        tracks = self._tracks(910002, [
            {"position": "1.1", "title": "(silence)", "duration": "0:10"},
            {"position": "1.2", "title": "[silence]", "duration": "0:20"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["title"], "(silence)")
        self.assertEqual(only["length_seconds"], 30.0)

    def test_sub_entry_durations_sum_known_or_stay_none(self):
        tracks = self._tracks(910003, [
            {"position": "1.1", "title": "Song", "duration": ""},
            {"position": "1.2", "title": "Coda", "duration": "1:00"},
            {"position": "2.1", "title": "Unknown A", "duration": ""},
            {"position": "2.2", "title": "Unknown B", "duration": ""},
        ])
        self.assertEqual(len(tracks), 2)
        first, second = tracks[0], tracks[1]
        assert _is_dict(first) and _is_dict(second)
        self.assertEqual(first["length_seconds"], 60.0)
        self.assertIsNone(second["length_seconds"])

    def test_bare_letter_sides_number_as_track_one(self):
        # 7" singles list positions as bare 'A'/'B'; both used to land at
        # track 0, scrambling manifest order (issue #1261 second flavor).
        tracks = self._tracks(910004, [
            {"position": "A", "title": "Side A Song", "duration": "3:10"},
            {"position": "B", "title": "Side B Song", "duration": "2:50"},
        ])
        self.assertEqual(len(tracks), 2)
        side_a, side_b = tracks[0], tracks[1]
        assert _is_dict(side_a) and _is_dict(side_b)
        self.assertEqual(
            (side_a["disc_number"], side_a["track_number"]), (1, 1))
        self.assertEqual(
            (side_b["disc_number"], side_b["track_number"]), (2, 1))

    def test_unparseable_sub_bases_group_separately(self):
        # Grouping keys on the literal base string: 'CD1.x' and 'CD2.x'
        # both parse to the (1, 0) sentinel but are distinct physical
        # tracks and must stay distinct rows.
        tracks = self._tracks(910007, [
            {"position": "CD1.1", "title": "One A", "duration": "1:00"},
            {"position": "CD1.2", "title": "One B", "duration": "1:00"},
            {"position": "CD2.1", "title": "Two A", "duration": "1:00"},
        ])
        self.assertEqual(len(tracks), 2)
        first, second = tracks[0], tracks[1]
        assert _is_dict(first) and _is_dict(second)
        self.assertEqual(first["title"], "One A")
        self.assertEqual(first["length_seconds"], 120.0)
        self.assertEqual(second["title"], "Two A")

    def test_flat_parent_row_joins_its_sub_group(self):
        # An index-style parent row sharing a sub run's base merges into
        # that run instead of duplicating its (disc, track).
        tracks = self._tracks(910008, [
            {"position": "10", "title": "Medley", "duration": ""},
            {"position": "10.1", "title": "Part One", "duration": "1:00"},
            {"position": "10.2", "title": "Part Two", "duration": "2:00"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["track_number"], 10)
        self.assertEqual(only["title"], "Medley")
        self.assertEqual(only["length_seconds"], 180.0)

    def test_parent_duration_is_authoritative_total(self):
        # An index parent's duration is the physical track's total (the
        # shape Beets' own nested-index fixtures document) — it replaces
        # the children's sum, never adds to it.
        tracks = self._tracks(910015, [
            {"position": "10", "title": "Suite", "duration": "8:00"},
            {"position": "10.1", "title": "Part One", "duration": "4:00"},
            {"position": "10.2", "title": "Part Two", "duration": "4:00"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["length_seconds"], 480.0)

    def test_zero_duration_parent_does_not_zero_the_group(self):
        # A 0:00 parent is upstream nonsense, not an authoritative
        # total — the children's sum stands.
        tracks = self._tracks(910018, [
            {"position": "10", "title": "Medley", "duration": "0:00"},
            {"position": "10.1", "title": "Part One", "duration": "3:00"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["length_seconds"], 180.0)

    def test_absent_duration_key_heading_shape_is_kept(self):
        # library_completeness's exact rule: only a literal empty-string
        # duration marks a heading; an ABSENT key is ambiguous.
        tracks = self._tracks(910019, [
            {"position": "1", "title": "One", "duration": "3:00"},
            {"position": "", "title": "Mystery"},
        ])
        self.assertEqual(len(tracks), 2)
        kept = tracks[1]
        assert _is_dict(kept)
        self.assertEqual(kept["title"], "Mystery")

    def test_parent_after_subs_still_titles_the_group(self):
        # The parent's title is authoritative wherever the row sits.
        tracks = self._tracks(910016, [
            {"position": "10.1", "title": "Part One", "duration": "1:00"},
            {"position": "10.2", "title": "Part Two", "duration": "1:00"},
            {"position": "10", "title": "Medley", "duration": ""},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["title"], "Medley")
        self.assertEqual(only["length_seconds"], 120.0)

    def test_empty_position_row_with_duration_is_kept(self):
        # Only empty-position AND empty-duration rows are headings (the
        # measured mirror rule in lib/library_completeness.py); an
        # empty position with a duration is ambiguous and survives.
        tracks = self._tracks(910017, [
            {"position": "1", "title": "Opener", "duration": "3:00"},
            {"position": "", "title": "Ambiguous", "duration": "2:00"},
        ])
        self.assertEqual(len(tracks), 2)
        kept = tracks[1]
        assert _is_dict(kept)
        self.assertEqual(kept["title"], "Ambiguous")

    def test_blank_sub_title_is_placeholder(self):
        tracks = self._tracks(910009, [
            {"position": "1.1", "title": "", "duration": "0:10"},
            {"position": "1.2", "title": "Real Song", "duration": "3:00"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["title"], "Real Song")
        self.assertEqual(only["length_seconds"], 190.0)

    def test_duplicate_flat_positions_stay_separate(self):
        # Flat rows never merge with each other, even on identical
        # positions (upstream data errors, heading rows).
        tracks = self._tracks(910010, [
            {"position": "1", "title": "Take One", "duration": "1:00"},
            {"position": "1", "title": "Take Two", "duration": "2:00"},
        ])
        self.assertEqual(len(tracks), 2)
        first, second = tracks[0], tracks[1]
        assert _is_dict(first) and _is_dict(second)
        self.assertEqual(first["title"], "Take One")
        self.assertEqual(second["title"], "Take Two")

    def test_heading_rows_are_dropped_when_release_positions_its_tracks(self):
        # The live Kid A vinyl shape (Discogs 1450555): side-name heading
        # rows carry an empty position and no duration; a rip has no file
        # for them. 14 raw entries, 10 real tracks.
        tracks = self._tracks(910011, [
            {"position": "", "title": "Alpha", "duration": ""},
            {"position": "A1", "title": "Everything in Its Right Place", "duration": "4:11"},
            {"position": "A2", "title": "Kid A", "duration": "4:44"},
            {"position": "", "title": "Beta", "duration": ""},
            {"position": "B1", "title": "The National Anthem", "duration": "5:50"},
        ])
        self.assertEqual(len(tracks), 3)
        numbers = [
            (t["disc_number"], t["track_number"]) for t in tracks if _is_dict(t)
        ]
        self.assertEqual(numbers, [(1, 1), (1, 2), (2, 1)])

    def test_all_empty_positions_are_preserved(self):
        # A release that positions NOTHING has no heading signal — every
        # row is a real track and the count must survive. Durations are
        # empty ON PURPOSE: rows must reach the any_positioned clause
        # (a duration short-circuits the heading predicate first and
        # turns this pin into a bystander for its named clause).
        tracks = self._tracks(910012, [
            {"position": "", "title": "First", "duration": ""},
            {"position": "", "title": "Second", "duration": ""},
        ])
        self.assertEqual(len(tracks), 2)

    def test_empty_position_index_parent_with_subtracks_is_kept(self):
        # A nested index parent (mirror keeps children under sub_tracks)
        # is a real physical track, not a heading, even at position "".
        tracks = self._tracks(910013, [
            {"position": "1", "title": "Opener", "duration": "3:00"},
            {"position": "", "title": "Medley", "duration": "", "sub_tracks": [
                {"position": "2.1", "title": "Part One", "duration": "1:00"},
            ]},
        ])
        self.assertEqual(len(tracks), 2)
        medley = tracks[1]
        assert _is_dict(medley)
        self.assertEqual(medley["title"], "Medley")

    def test_number_letter_and_trailing_dot_positions_parse(self):
        # Live cohort grammars: '1A/2A/1B' (Dirt Dress 4738671) and
        # '1./2.' (Deloris 3938744) used to land at track 0.
        tracks = self._tracks(910014, [
            {"position": "1A", "title": "Side A One", "duration": "2:00"},
            {"position": "2A", "title": "Side A Two", "duration": "2:00"},
            {"position": "1B", "title": "Side B One", "duration": "2:00"},
            {"position": "1.", "title": "Dotted One", "duration": "2:00"},
        ])
        numbers = [
            (t["disc_number"], t["track_number"]) for t in tracks if _is_dict(t)
        ]
        self.assertEqual(numbers, [(1, 1), (1, 2), (2, 1), (1, 1)])

    def test_video_position_row_is_dropped_from_mixed_release(self):
        # The live 5936 shape (Discogs 4345679): an enhanced-CD bonus
        # video carries a literal "Video" position; no audio rip has a
        # file for it, so it must not count against the audio gate.
        tracks = self._tracks(910020, [
            {"position": "1", "title": "Opener", "duration": "3:00"},
            {"position": "2", "title": "Closer", "duration": "3:00"},
            {"position": "Video", "title": "Bonus Clip", "duration": "4:00"},
        ])
        self.assertEqual(len(tracks), 2)
        titles = [t["title"] for t in tracks if _is_dict(t)]
        self.assertEqual(titles, ["Opener", "Closer"])

    def test_numbered_video_positions_drop_too(self):
        tracks = self._tracks(910021, [
            {"position": "1", "title": "Song", "duration": "3:00"},
            {"position": "Video 1", "title": "Clip One", "duration": "1:00"},
            {"position": "Video2", "title": "Clip Two", "duration": "1:00"},
        ])
        self.assertEqual(len(tracks), 1)

    def test_whole_release_video_positions_are_preserved(self):
        # The Placebo precedent (docs/plans/2026-05-12-001): a release
        # whose content IS video is rip-real — its files exist in rips,
        # so an all-video tracklist keeps every row.
        tracks = self._tracks(910022, [
            {"position": "Video 1", "title": "Part One", "duration": "20:00"},
            {"position": "Video 2", "title": "Part Two", "duration": "20:00"},
        ])
        self.assertEqual(len(tracks), 2)

    def test_index_parent_vote_keeps_video_rows_droppable(self):
        # A nested sub_tracks index parent survives the heading rule as
        # real audio, so it must VOTE in the all-video decision: this
        # release has audio, and the phantom video row drops.
        tracks = self._tracks(910024, [
            {"position": "", "title": "Medley", "duration": "", "sub_tracks": [
                {"position": "2.1", "title": "Part One", "duration": "1:00"},
            ]},
            {"position": "Video", "title": "Bonus Clip", "duration": "4:00"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["title"], "Medley")

    def test_heading_plus_all_video_release_keeps_video_rows(self):
        # A dropped section heading casts no vote: the remaining rows
        # are all video, so the whole-release guard preserves them
        # (never an empty manifest).
        tracks = self._tracks(910025, [
            {"position": "", "title": "Bonus Section", "duration": ""},
            {"position": "Video", "title": "Clip", "duration": "4:00"},
        ])
        self.assertEqual(len(tracks), 1)
        only = tracks[0]
        assert _is_dict(only)
        self.assertEqual(only["title"], "Clip")

    def test_video_like_positions_are_not_markers(self):
        # The marker grammar is anchored: positions merely CONTAINING
        # the word survive (they fall to the (1, 0) sentinel but are
        # never dropped).
        tracks = self._tracks(910026, [
            {"position": "1", "title": "Song", "duration": "3:00"},
            {"position": "Videos", "title": "Kept One", "duration": "2:00"},
            {"position": "DVD Video", "title": "Kept Two", "duration": "2:00"},
            {"position": "Video 1-2", "title": "Kept Three", "duration": "2:00"},
        ])
        self.assertEqual(len(tracks), 4)

    def test_video_sub_positions_drop_before_grouping(self):
        # 'Video.1'/'Video.2' bases match the marker too; on a mixed
        # release they drop before the grouping pass, leaving no phantom
        # group behind.
        tracks = self._tracks(910027, [
            {"position": "1", "title": "Song", "duration": "3:00"},
            {"position": "Video.1", "title": "Clip A", "duration": "1:00"},
            {"position": "Video.2", "title": "Clip B", "duration": "1:00"},
        ])
        self.assertEqual(len(tracks), 1)

    def test_video_in_title_never_drops_a_row(self):
        # Only the POSITION grammar decides; titles are never consulted.
        tracks = self._tracks(910023, [
            {"position": "1", "title": "Video Killed the Radio Star", "duration": "3:00"},
            {"position": "2", "title": "Closer", "duration": "3:00"},
        ])
        self.assertEqual(len(tracks), 2)

    def test_flat_tracklists_pass_through_unchanged(self):
        tracks = self._tracks(910005, [
            {"position": "1", "title": "Airbag", "duration": "4:44"},
            {"position": "2", "title": "Paranoid Android", "duration": "6:23"},
        ])
        self.assertEqual(tracks, [
            {"disc_number": 1, "track_number": 1, "title": "Airbag",
             "length_seconds": 284.0},
            {"disc_number": 1, "track_number": 2, "title": "Paranoid Android",
             "length_seconds": 383.0},
        ])


class TestGetMasterReleases(unittest.TestCase):
    MASTER_DATA: ClassVar = {
        "id": 21491,
        "title": "OK Computer",
        "year": 1997,
        "main_release_id": 4950798,
        "primary_type": "Album",
        "first_release_date": "1997",
        "artist_credit": "Radiohead",
        "primary_artist_id": 3840,
        "artists": [{"id": 3840, "name": "Radiohead"}],
        "releases": [
            {
                "id": 83182,
                "title": "OK Computer",
                "country": "Europe",
                "released": "1997-06-16",
                "track_count": 12,
                "formats": [{"name": "CD", "qty": 1}],
                "labels": [{"id": 2294, "name": "Parlophone", "catno": "X"}],
            },
            {
                "id": 105704,
                "title": "OK Computer",
                "country": "US",
                "released": "1997-07-01",
                "track_count": 12,
                "formats": [{"name": "CD", "qty": 1, "descriptions": "Album, Promo"}],
                "labels": [],
            },
        ],
    }

    def test_normalizes_master(self):
        with _mock_urlopen(self.MASTER_DATA):
            result = get_master_releases(21491)

        self.assertEqual(result["title"], "OK Computer")
        self.assertEqual(result["type"], "Album")
        self.assertEqual(result["first_release_date"], "1997")
        self.assertEqual(result["artist_credit"], "Radiohead")
        self.assertEqual(result["primary_artist_id"], "3840")
        releases = result["releases"]
        assert _is_list(releases)
        self.assertEqual(len(releases), 2)
        release0 = releases[0]
        assert _is_dict(release0)
        self.assertEqual(release0["id"], "83182")
        self.assertEqual(release0["country"], "Europe")
        self.assertEqual(release0["format"], "CD")
        self.assertEqual(release0["date"], "1997-06-16")
        self.assertEqual(release0["track_count"], 12)
        self.assertEqual(release0["status"], "Official")
        release1 = releases[1]
        assert _is_dict(release1)
        self.assertEqual(release1["status"], "Promotion")

    def test_master_children_derive_unofficial_and_mixed_status(self):
        master = {
            "id": 1,
            "title": "Evidence",
            "releases": [
                {
                    "id": 1, "title": "Unofficial", "formats": [{
                        "name": "CD", "qty": 1,
                        "descriptions": "Album, Unofficial Release",
                    }],
                },
                {
                    "id": 2, "title": "Mixed", "formats": [{
                        "name": "CD", "qty": 1,
                        "descriptions": "Album, Promo, Unofficial Release",
                    }],
                },
            ],
        }
        with _mock_urlopen(master):
            result = get_master_releases(1)
        releases = result["releases"]
        assert _is_list(releases)
        statuses: list[object] = []
        for row in releases:
            assert _is_dict(row)
            statuses.append(row["status"])
        self.assertEqual(
            statuses,
            ["Bootleg", "Bootleg / Promo"],
        )

    def test_track_count_defaults_to_zero_when_missing(self):
        """Discogs CC0 dump occasionally lacks tracklists; fall back to 0
        rather than the old format-quantity fudge that displayed '1t'."""
        master = {
            "id": 1,
            "title": "Sparse",
            "releases": [
                {"id": 99, "title": "Sparse", "country": "AU",
                 "formats": [{"name": "CD", "qty": 1}], "labels": []},
            ],
        }
        with _mock_urlopen(master):
            result = get_master_releases(1)
        releases = result["releases"]
        assert _is_list(releases)
        release0 = releases[0]
        assert _is_dict(release0)
        self.assertEqual(release0["track_count"], 0)
        self.assertEqual(release0["date"], "")


class TestSearchReleases(unittest.TestCase):
    SEARCH_DATA: ClassVar = {
        "results": [
            {
                "id": 83182,
                "title": "OK Computer",
                "master_id": 21491,
                "master_title": "OK Computer",
                "master_first_released": "1997",
                "primary_type": "Album",
                "score": 0.099,
                "released": "1997-06-16",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
            {
                "id": 105704,
                "title": "OK Computer (US)",
                "master_id": 21491,
                "master_title": "OK Computer",
                "master_first_released": "1997",
                "primary_type": "Album",
                "score": 0.05,
                "released": "1997-07-01",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
            {
                "id": 999,
                "title": "OK Computer Demos",
                "master_id": None,
                "primary_type": "Other",
                "score": 0.02,
                "released": "1996",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
        ],
    }

    def test_deduplicates_by_master_with_master_metadata(self):
        with _mock_urlopen(self.SEARCH_DATA):
            results = search_releases("OK Computer")

        # 1 master (deduped) + 1 masterless = 2 entries
        self.assertEqual(len(results), 2)
        first = results[0]
        self.assertEqual(first["id"], "21491")
        self.assertEqual(first["title"], "OK Computer")  # master_title, not per-release title
        self.assertEqual(first["primary_type"], "Album")
        self.assertEqual(first["first_release_date"], "1997")  # master_first_released
        self.assertEqual(first["artist_name"], "Radiohead")
        self.assertTrue(first["is_master"])
        self.assertEqual(first["score"], 9)  # int(0.099 * 100)
        self.assertEqual(first["discogs_release_id"], "83182")

        masterless = results[1]
        self.assertEqual(masterless["id"], "999")
        self.assertEqual(masterless["title"], "OK Computer Demos")
        self.assertFalse(masterless["is_master"])
        self.assertEqual(masterless["first_release_date"], "1996")  # falls back to released

    def test_long_query_uses_bounded_cache_key(self):
        long_query = "r" * 250
        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_releases(long_query)

        cache_key = memo.call_args[0][0]
        self.assertTrue(cache_key.startswith("discogs:search:releases:"))
        self.assertIn(f":#{len(long_query)}:", cache_key)
        self.assertLess(len(cache_key), len(f"discogs:search:releases:{long_query}"))


class TestSearchReleasesVaRewrite(unittest.TestCase):
    """VA-token handling in the Discogs title search (#199).

    The dump's VA artist (id 194) has no name row, so "Various Artists"
    tokens can never match — pre-fix they ANDed into the title match and
    returned zero results. The fix strips the tokens from the title and
    pins the mirror's ``artist_id=194`` exact filter so the mirror itself
    returns only VA-credited releases.
    """

    SEARCH_DATA: ClassVar = {
        "results": [
            {
                "id": 32457180,
                "title": "Rock Christmas (The Very Best Of)",
                "master_id": 3673686,
                "master_title": "Rock Christmas (The Very Best Of)",
                "master_first_released": "1992",
                "primary_type": "Album",
                "score": 0.10,
                "released": "2024",
                "artists": [{"id": 194, "name": "Various"}],
            },
        ],
    }

    def _requested_qs(self, mock_urlopen) -> dict:
        url = mock_urlopen.call_args[0][0].full_url
        return urllib.parse.parse_qs(urllib.parse.urlparse(url).query)

    def test_va_query_strips_tokens_and_pins_artist_id(self):
        with _mock_urlopen(self.SEARCH_DATA) as m:
            search_releases("Rock Christmas Various Artists")
        qs = self._requested_qs(m)
        self.assertEqual(qs["title"][0], "Rock Christmas")
        self.assertEqual(qs["artist_id"][0], "194")

    def test_plain_query_sends_no_artist_id(self):
        with _mock_urlopen(self.SEARCH_DATA) as m:
            search_releases("Rock Christmas")
        qs = self._requested_qs(m)
        self.assertEqual(qs["title"][0], "Rock Christmas")
        self.assertNotIn("artist_id", qs)

    def test_va_only_query_keeps_raw_title_and_no_pin(self):
        # No title remainder after the strip — keep the raw passthrough
        # rather than pinning artist_id with an empty title (which would
        # make the mirror scan every one of artist 194's releases).
        with _mock_urlopen(self.SEARCH_DATA) as m:
            search_releases("Various Artists")
        qs = self._requested_qs(m)
        self.assertEqual(qs["title"][0], "Various Artists")
        self.assertNotIn("artist_id", qs)

    def _cache_key_for(self, query: str) -> str:
        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_releases(query)
        return memo.call_args[0][0]

    def test_va_query_uses_distinct_cache_key(self):
        # The artist_id-pinned fetch is a different upstream query than
        # the bare-title fetch, so it must NOT collide with the plain
        # "Rock Christmas" cache entry.
        va_key = self._cache_key_for("Rock Christmas Various Artists")
        plain_key = self._cache_key_for("Rock Christmas")
        self.assertNotEqual(va_key, plain_key)
        self.assertTrue(va_key.startswith("discogs:search:releases:"))

    def test_va_flag_cannot_be_forged_by_user_text(self):
        # The va discriminator sits before the user query text, so a plain
        # query crafted to look like the VA key's tail must not collide.
        va_key = self._cache_key_for("Rock Christmas Various Artists")
        for adversarial in ("Rock Christmas:va", "va=1:Rock Christmas",
                            "Rock Christmas va=1"):
            self.assertNotEqual(self._cache_key_for(adversarial), va_key)


class TestSearchArtists(unittest.TestCase):
    """search_artists() now hits /api/artists?name= (real artist-name index)."""

    ARTIST_SEARCH_DATA: ClassVar = {
        "results": [
            {
                "id": 3840,
                "name": "Radiohead",
                "profile": "British alternative rock band...",
                "score": 0.06079271,
            },
            {
                "id": 104129,
                "name": "Radioheads",
                "profile": "",
                "score": 0.06079271,
            },
        ],
        "total": 6,
        "page": 1,
        "per_page": 25,
    }

    def test_returns_name_matched_artists(self):
        with _mock_urlopen(self.ARTIST_SEARCH_DATA) as mock:
            results = search_artists("Radiohead")

        # Verify the new endpoint was hit (not the old release-search hack)
        called_url = mock.call_args_list[0][0][0].full_url
        self.assertIn("/api/artists?name=", called_url)
        self.assertNotIn("/api/search?artist=", called_url)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], "3840")
        self.assertEqual(results[0]["name"], "Radiohead")
        self.assertEqual(results[0]["disambiguation"], "")  # left empty intentionally
        self.assertEqual(results[0]["score"], 6)  # int(0.06 * 100)
        self.assertEqual(results[1]["name"], "Radioheads")

    def test_long_query_uses_bounded_cache_key(self):
        long_query = "a" * 250
        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_artists(long_query)

        cache_key = memo.call_args[0][0]
        self.assertTrue(cache_key.startswith("discogs:search:artists:"))
        self.assertIn(f":#{len(long_query)}:", cache_key)
        self.assertLess(len(cache_key), len(f"discogs:search:artists:{long_query}"))

    def test_exact_four_tet_search_surfaces_symbol_alias(self):
        symbol_name = "⣎⡇ꉺლ༽இ•̛)ྀ◞ ༎ຶ ༽ৣৢ؞ৢ؞ؖ ꉺლ"
        search_data = {
            "results": [
                {"id": 3543, "name": "Four Tet", "score": 1.0},
                {"id": 2039081, "name": "The Urge Four Tet", "score": 0.09},
            ],
        }
        detail = {
            "id": 3543,
            "name": "Four Tet",
            "aliases": [
                {"id": 60342, "name": "Kieran Hebden"},
                {"id": 6400214, "name": symbol_name},
            ],
        }
        with _mock_urlopen_by_url({
            "/api/artists?name=Four%20Tet": search_data,
            "/api/artists/3543": detail,
        }):
            results = search_artists("Four Tet")

        self.assertEqual(
            [row["id"] for row in results[:4]],
            ["3543", "60342", "6400214", "2039081"],
        )
        self.assertEqual(results[2]["name"], symbol_name)


def _mock_urlopen_by_url(responses: dict):
    """Mock urllib.request.urlopen with per-URL-substring responses.

    ``responses`` is a dict mapping a substring of the URL to the JSON payload
    that should come back. Each match is independent — callers can mock
    /masters and /appearances with different bodies in the same context.
    """

    def _side_effect(req, *args, **kwargs):
        url = req.full_url
        for needle, payload in responses.items():
            if needle in url:
                mock_resp = MagicMock()
                mock_resp.read.return_value = json.dumps(payload).encode()
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)
                return mock_resp
        raise AssertionError(f"no mock response configured for URL: {url}")

    return patch("web.discogs.urllib.request.urlopen", side_effect=_side_effect)


class TestGetArtistReleases(unittest.TestCase):
    """get_artist_releases() merges /masters + /appearances from the mirror."""

    MASTERS_DATA: ClassVar = {
        "results": [
            {
                "id": 21481,
                "title": "Creep",
                "type": "EP",
                "primary_types": ["EP", "Single"],
                "format_qualifiers": ["12\"", "EP"],
                "provenance": ["ordinary", "promo"],
                "first_release_date": "1992",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": False,
            },
            {
                "id": 13344,
                "title": "Pablo Honey",
                "type": "Album",
                "primary_types": ["Album"],
                "format_qualifiers": ["Album", "LP"],
                "provenance": ["ordinary"],
                "first_release_date": "1993",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": False,
            },
            {
                "id": "release-83182",
                "title": "Stupid Car (demo)",
                "type": "Other",
                "primary_types": [],
                "format_qualifiers": ["Unofficial Release"],
                "provenance": ["unofficial"],
                "first_release_date": "1993",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": True,
            },
        ],
        "total": 3,
        "page": 1,
        "per_page": 100,
    }

    EMPTY_APPEARANCES: ClassVar = {"results": [], "total": 0, "page": 1, "per_page": 1}

    def _assert_incomplete_envelope_rejected(
        self, *, endpoint: str, payload: dict,
    ) -> None:
        responses = {
            "/masters": self.MASTERS_DATA,
            "/appearances": self.EMPTY_APPEARANCES,
        }
        responses[endpoint] = payload
        with _mock_urlopen_by_url(responses), self.assertRaises(
            web.discogs.DiscogsArtistCatalogueIncomplete,
        ):
            get_artist_releases(3840)

    def test_rejects_truncated_masters_envelope(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/masters",
            payload={**self.MASTERS_DATA, "total": 4},
        )

    def test_rejects_nonfirst_masters_page(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/masters",
            payload={**self.MASTERS_DATA, "page": 2},
        )

    def test_rejects_truncated_appearances_envelope(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/appearances",
            payload={**self.EMPTY_APPEARANCES, "total": 1},
        )

    def test_rejects_nonfirst_appearances_page(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/appearances",
            payload={**self.EMPTY_APPEARANCES, "page": 2},
        )

    def test_normalizes_master_discography(self):
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": self.EMPTY_APPEARANCES,
        }) as mock, patch(
            "web.discogs._cache.memoize_meta",
            side_effect=lambda _key, fetch: fetch(),
        ) as memo:
            results = msgspec.to_builtins(get_artist_releases(3840))

        called_urls = [c.args[0].full_url for c in mock.call_args_list]
        self.assertEqual(
            called_urls,
            [
                "https://discogs-mirror.test/api/artists/3840/masters/all",
                "https://discogs-mirror.test/api/artists/3840/appearances",
            ],
            "cold artist metadata must use one explicit fail-loud bulk request",
        )
        self.assertEqual(
            memo.call_args.args[0], "discogs:artist:3840:releases:v7",
        )

        self.assertEqual(len(results), 3)

        album = next(r for r in results if r["title"] == "Pablo Honey")
        self.assertEqual(album["id"], "13344")
        self.assertEqual(album["type"], "Album")
        self.assertEqual(album["source"], "discogs")
        self.assertEqual(album["identity_kind"], "work")
        self.assertEqual(album["primary_types"], ["Album"])
        self.assertEqual(album["first_release_date"], "1993")
        self.assertEqual(album["artist_credit"], "Radiohead")
        self.assertEqual(album["primary_artist_id"], "3840")
        self.assertEqual(album["secondary_types"], [])
        self.assertIs(album["is_appearance"], False)
        self.assertEqual(album["provenance"], ["ordinary"])

        masterless = next(r for r in results if r["title"] == "Stupid Car (demo)")
        self.assertEqual(masterless["id"], "83182")  # "release-" prefix stripped
        self.assertEqual(masterless.get("discogs_release_id"), "83182")
        self.assertEqual(masterless["primary_types"], [])
        self.assertEqual(masterless["identity_kind"], "release")
        self.assertEqual(masterless["provenance"], ["unofficial"])

    def test_appearances_merged_and_classified_as_non_primary(self):
        appearances = {
            "results": [
                {
                    "id": 555,
                    "title": "Indie 1996",
                    "type": "Album",
                    "primary_types": ["Album"],
                    "format_qualifiers": ["Album"],
                    "provenance": ["ordinary"],
                    "first_release_date": "1996",
                    "artist_credit": "Various",
                    "primary_artist_id": 194,
                    "is_masterless": False,
                },
            ],
            "total": 1,
            "page": 1,
            "per_page": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": appearances,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))
        comp = next(r for r in results if r["title"] == "Indie 1996")
        self.assertEqual(comp["primary_artist_id"], "194")
        self.assertEqual(comp["artist_credit"], "Various")
        self.assertIs(comp["is_appearance"], True)
        # The JS classifier reads primary_artist_id !== artist_id to route into
        # the Appearances section — so it must NOT equal the queried artist id.
        self.assertNotEqual(comp["primary_artist_id"], "3840")
        self.assertEqual(len(results), 4)

    def test_dedup_masters_wins_over_appearances(self):
        """When a master shows up in BOTH endpoints (split release where the
        artist is a primary credit on one release and a track-only credit on
        a sibling release in the same master), the /masters classification
        wins — we don't downgrade an own-work master to an appearance."""
        appearance_dup = {
            "results": [
                {
                    "id": 13344,  # same master id as Pablo Honey in /masters
                    "title": "Pablo Honey (Various comp version)",
                    "type": "Album",
                    "primary_types": ["Album"],
                    "format_qualifiers": ["Album"],
                    "provenance": ["ordinary"],
                    "first_release_date": "1993",
                    "artist_credit": "Various",
                    "primary_artist_id": 194,
                    "is_masterless": False,
                },
            ],
            "total": 1,
            "page": 1,
            "per_page": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": appearance_dup,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))
        self.assertEqual(len(results), 3)
        pablo = next(r for r in results if r["id"] == "13344")
        self.assertEqual(pablo["artist_credit"], "Radiohead")
        self.assertEqual(pablo["primary_artist_id"], "3840")
        self.assertIs(pablo["is_appearance"], False)

    def test_duplicate_primary_credit_rows_keep_first_projection(self):
        """Duplicate release_artist credits are one catalogue identity."""
        duplicate = {
            **self.MASTERS_DATA,
            "results": [
                self.MASTERS_DATA["results"][0],
                {
                    **self.MASTERS_DATA["results"][0],
                    "title": "duplicate credit must not replace the first",
                },
            ],
            "total": 2,
        }
        with _mock_urlopen_by_url({
            "/masters": duplicate,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "Creep")

    def test_master_and_same_numeric_masterless_release_both_survive(self):
        """Master and release ids occupy separate Discogs namespaces."""
        masters = {
            **self.MASTERS_DATA,
            "results": [
                self.MASTERS_DATA["results"][0],
                {
                    **self.MASTERS_DATA["results"][2],
                    "id": "release-21481",
                },
            ],
            "total": 2,
        }
        with _mock_urlopen_by_url({
            "/masters": masters,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))

        collisions = [row for row in results if row["id"] == "21481"]
        self.assertEqual(len(collisions), 2)
        self.assertEqual(
            [row["identity_kind"] for row in collisions],
            ["work", "release"],
        )

    def test_rejects_malformed_or_nonpositive_artist_identity_ids(self):
        for raw_id, is_masterless in (
            ("foo", True),
            ("release-", True),
            ("release-abc", True),
            ("release-0", True),
            ("release--1", True),
            (0, False),
            (-1, False),
            ("122", False),
        ):
            with self.subTest(raw_id=raw_id, is_masterless=is_masterless):
                invalid_row = {
                    **self.MASTERS_DATA["results"][0],
                    "id": raw_id,
                    "is_masterless": is_masterless,
                }
                payload = {
                    **self.MASTERS_DATA,
                    "results": [invalid_row],
                    "total": 1,
                }
                with _mock_urlopen_by_url({
                    "/masters": payload,
                    "/appearances": self.EMPTY_APPEARANCES,
                }), self.assertRaises(ValueError):
                    get_artist_releases(3840)

    def test_missing_primary_types_is_rejected_at_boundary(self):
        invalid = {
            **self.MASTERS_DATA,
            "results": [
                {
                    key: value
                    for key, value in self.MASTERS_DATA["results"][0].items()
                    if key != "primary_types"
                },
            ],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases(3840)

    def test_wrong_primary_types_element_is_rejected_at_boundary(self):
        invalid = {
            **self.MASTERS_DATA,
            "results": [
                {
                    **self.MASTERS_DATA["results"][0],
                    "primary_types": [7],
                    "format_qualifiers": ["Album"],
                    "provenance": ["ordinary"],
                },
            ],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases(3840)

    def test_missing_provenance_is_rejected_at_boundary(self):
        invalid_row = {
            key: value
            for key, value in self.MASTERS_DATA["results"][0].items()
            if key != "provenance"
        }
        invalid = {
            **self.MASTERS_DATA,
            "results": [invalid_row],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases(3840)

    def test_wrong_provenance_element_is_rejected_at_boundary(self):
        invalid = {
            **self.MASTERS_DATA,
            "results": [{
                **self.MASTERS_DATA["results"][0],
                "provenance": [7],
            }],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases(3840)

    def test_invalid_appearance_row_is_rejected_at_boundary(self):
        invalid_appearances = {
            "results": [{
                "id": 555,
                "title": "Sampler",
                "type": "Album",
                "primary_types": ["Compilation"],
                "format_qualifiers": ["Compilation"],
                "provenance": ["ordinary"],
                "first_release_date": "2001",
                "artist_credit": "Various",
                "primary_artist_id": 194,
                "is_masterless": False,
            }],
            "total": 1,
            "page": 1,
            "per_page": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": invalid_appearances,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases(3840)

    def test_null_primary_artist_id_normalizes_to_empty_string(self):
        null_artist = {
            "results": [{
                "id": 60,
                "title": "Mixed appearance master",
                "type": "EP",
                "primary_types": ["EP", "Single"],
                "format_qualifiers": ["EP"],
                "provenance": ["ordinary"],
                "first_release_date": "2005",
                "artist_credit": "",
                "primary_artist_id": None,
                "is_masterless": False,
            }],
            "total": 1,
            "page": 1,
            "per_page": 100,
        }
        with _mock_urlopen_by_url({
            "/masters": null_artist,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))
        self.assertEqual(results[0]["primary_artist_id"], "")


class TestGetArtistName(unittest.TestCase):
    def test_returns_name(self):
        with _mock_urlopen({"id": 3840, "name": "Radiohead"}):
            self.assertEqual(get_artist_name(3840), "Radiohead")


# ── Label adapter tests (U3) ────────────────────────────────────────────


class TestSearchLabels(unittest.TestCase):
    """search_labels() hits /api/labels?name= and returns LabelEntity list."""

    LABEL_SEARCH_DATA: ClassVar = {
        "results": [
            {
                "id": 2294,
                "name": "Parlophone",
                "profile": "British record label founded in 1896.",
                "parent_label_id": None,
                "parent_label_name": None,
                "release_count": 18452,
                "score": 0.087,
            },
            {
                "id": 25693,
                "name": "Parlophone Records Ltd.",
                "profile": "Subsidiary trading name.",
                "parent_label_id": 2294,
                "parent_label_name": "Parlophone",
                "release_count": 412,
                "score": 0.072,
            },
        ],
        "total": 2,
        "page": 1,
        "per_page": 25,
    }

    def test_returns_label_entities(self):
        with _mock_urlopen(self.LABEL_SEARCH_DATA) as mock:
            results = search_labels("Parlophone")

        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/labels?name=", called_url)

        self.assertEqual(len(results), 2)
        first = results[0]
        self.assertIsInstance(first, LabelEntity)
        self.assertEqual(first.source, "discogs")
        self.assertEqual(first.id, "2294")  # int → str coercion
        self.assertEqual(first.name, "Parlophone")
        self.assertIsNone(first.country)  # discogs has no country column
        self.assertEqual(first.profile, "British record label founded in 1896.")
        self.assertIsNone(first.parent_label_id)
        self.assertIsNone(first.parent_label_name)
        self.assertEqual(first.release_count, 18452)

        sub = results[1]
        self.assertEqual(sub.id, "25693")
        self.assertEqual(sub.parent_label_id, "2294")  # int → str coercion
        self.assertEqual(sub.parent_label_name, "Parlophone")

    def test_empty_results_returns_empty_list(self):
        with _mock_urlopen({"results": [], "total": 0, "page": 1, "per_page": 25}):
            results = search_labels("zzzzznosuchlabel")
        self.assertEqual(results, [])

    def test_wire_boundary_validates_release_count_int(self):
        """RED-first regression guard: a release_count arriving as a STRING
        instead of int must raise msgspec.ValidationError at the boundary.
        Per .claude/rules/code-quality.md, every wire-boundary type owes
        at least one test that proves it actually catches drift."""
        bad = {
            "results": [
                {
                    "id": 2294,
                    "name": "Parlophone",
                    "profile": "x",
                    "parent_label_id": None,
                    "parent_label_name": None,
                    "release_count": "18452",  # WRONG: string, not int
                    "score": 0.087,
                },
            ],
            "total": 1,
            "page": 1,
            "per_page": 25,
        }
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            search_labels("Parlophone")

    def test_long_query_uses_bounded_distinct_cache_key(self):
        q1 = "x" * 250
        q2 = ("x" * 200) + ("y" * 50)

        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_labels(q1)
            search_labels(q2)

        key1 = memo.call_args_list[0].args[0]
        key2 = memo.call_args_list[1].args[0]
        self.assertNotEqual(key1, key2)
        self.assertIn(f":#{len(q1)}:", key1)
        self.assertIn(f":#{len(q2)}:", key2)
        self.assertLess(len(key1), len(f"discogs:search:labels:{q1}:p=1:pp=25"))


class TestGetLabel(unittest.TestCase):
    """get_label() hits /api/labels/{id} and returns a LabelEntity."""

    TOP_LEVEL_DATA: ClassVar = {
        "id": 2294,
        "name": "Parlophone",
        "profile": "British record label.",
        "contactinfo": "",
        "data_quality": "Correct",
        "parent_label_id": None,
        "parent_label_name": None,
        "total_releases": 18452,
        "sub_labels": [
            {"id": 25693, "name": "Parlophone Records Ltd.", "release_count": 412},
        ],
    }

    SUB_LABEL_DATA: ClassVar = {
        "id": 25693,
        "name": "Parlophone Records Ltd.",
        "profile": "",
        "contactinfo": "",
        "data_quality": "Needs Vote",
        "parent_label_id": 2294,
        "parent_label_name": "Parlophone",
        "total_releases": 412,
        "sub_labels": [],
    }

    def test_top_level_label(self):
        with _mock_urlopen(self.TOP_LEVEL_DATA) as mock:
            entity = get_label(2294)

        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/labels/2294", called_url)

        self.assertIsInstance(entity, LabelEntity)
        self.assertEqual(entity.source, "discogs")
        self.assertEqual(entity.id, "2294")
        self.assertEqual(entity.name, "Parlophone")
        self.assertIsNone(entity.country)
        self.assertEqual(entity.profile, "British record label.")
        self.assertIsNone(entity.parent_label_id)
        self.assertIsNone(entity.parent_label_name)
        self.assertEqual(entity.release_count, 18452)  # comes from total_releases
        self.assertEqual(entity.sub_labels, [
            {"id": 25693, "name": "Parlophone Records Ltd.", "release_count": 412},
        ])

    def test_sub_label_has_parent(self):
        with _mock_urlopen(self.SUB_LABEL_DATA):
            entity = get_label(25693)

        self.assertEqual(entity.parent_label_id, "2294")
        self.assertEqual(entity.parent_label_name, "Parlophone")
        self.assertEqual(entity.release_count, 412)
        self.assertEqual(entity.sub_labels, [])

    def test_rejects_non_numeric_label_id(self):
        with self.assertRaises(AssertionError):
            get_label("../etc/passwd")

        with self.assertRaises(AssertionError):
            get_label("123 OR 1=1")


class TestGetLabelReleases(unittest.TestCase):
    """get_label_releases() hits /api/labels/{id}/releases."""

    RELEASES_DATA: ClassVar = {
        "results": [
            {
                "id": 83182,
                "title": "OK Computer",
                "country": "Europe",
                "released": "1997-06-16",
                "master_id": 21491,
                "master_title": "OK Computer",
                "master_first_released": "1997",
                "primary_type": "Album",
                "label_id": 2294,
                "sub_label_name": None,
                "artists": [{"id": 3840, "name": "Radiohead", "role": "", "anv": ""}],
                "labels": [{"id": 2294, "name": "Parlophone", "catno": "NODATA 02"}],
                "formats": [
                    {"name": "CD", "qty": 1, "descriptions": "Album", "free_text": ""}
                ],
            },
            {
                "id": 999111,
                "title": "Some Sub-label Release",
                "country": "UK",
                "released": "2001",
                "master_id": None,
                "primary_type": "Single",
                "label_id": 25693,
                "sub_label_name": "Parlophone Records Ltd.",
                "artists": [{"id": 1, "name": "Various", "role": "", "anv": ""}],
                "labels": [
                    {"id": 25693, "name": "Parlophone Records Ltd.", "catno": "PRL 1"}
                ],
                "formats": [
                    {"name": "Vinyl", "qty": 1, "descriptions": "7\"", "free_text": ""}
                ],
            },
        ],
        "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 2},
        "include_sublabels": True,
    }

    def test_returns_release_rows(self):
        with _mock_urlopen(self.RELEASES_DATA) as mock:
            payload = get_label_releases(2294, include_sublabels=True, page=1, per_page=100)

        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/labels/2294/releases", called_url)
        self.assertIn("include_sublabels=true", called_url)
        self.assertIn("page=1", called_url)
        self.assertIn("per_page=100", called_url)

        self.assertIn("results", payload)
        self.assertIn("pagination", payload)
        self.assertIn("include_sublabels", payload)
        self.assertTrue(payload["include_sublabels"])
        pagination = payload["pagination"]
        assert _is_dict(pagination)
        self.assertEqual(pagination["items"], 2)

        rows = payload["results"]
        assert _is_list(rows)
        self.assertEqual(len(rows), 2)

        direct = rows[0]
        assert _is_dict(direct)
        # Match shape used by web/discogs.py::get_master_releases / get_release
        # so the U4 route layer can overlay library/pipeline state without
        # renaming fields. ID is stringified, year derived from `released`,
        # primary_artist_id surfaces for cross-source overlay.
        self.assertEqual(direct["id"], "83182")
        self.assertEqual(direct["title"], "OK Computer")
        self.assertEqual(direct["primary_type"], "Album")
        self.assertEqual(direct["country"], "Europe")
        self.assertEqual(direct["date"], "1997-06-16")
        self.assertEqual(direct["year"], 1997)
        self.assertEqual(direct["release_group_id"], "21491")
        self.assertEqual(direct["master_title"], "OK Computer")
        self.assertEqual(direct["master_first_released"], "1997")
        self.assertEqual(direct["artist_name"], "Radiohead")
        self.assertEqual(direct["artist_id"], "3840")
        self.assertEqual(direct["label_id"], "2294")
        self.assertEqual(direct["via_label_id"], "2294")
        self.assertIsNone(direct["sub_label_name"])  # direct-parent release
        self.assertEqual(direct["format"], "CD")

        sub = rows[1]
        assert _is_dict(sub)
        self.assertEqual(sub["id"], "999111")
        self.assertEqual(sub["sub_label_name"], "Parlophone Records Ltd.")
        self.assertEqual(sub["label_id"], "25693")
        self.assertEqual(sub["via_label_id"], "25693")
        self.assertIsNone(sub["release_group_id"])  # masterless
        self.assertEqual(sub["primary_type"], "Single")
        self.assertEqual(sub["format"], "Vinyl")

    def test_accepts_legacy_via_label_id_payload(self):
        legacy = json.loads(json.dumps(self.RELEASES_DATA))
        for row in legacy["results"]:
            row["via_label_id"] = row.pop("label_id")

        with _mock_urlopen(legacy):
            payload = get_label_releases(112294, include_sublabels=True)

        results = payload["results"]
        assert _is_list(results)
        result0 = results[0]
        assert _is_dict(result0)
        self.assertEqual(result0["label_id"], "2294")
        self.assertEqual(result0["via_label_id"], "2294")

    def test_default_pagination_kwargs(self):
        with _mock_urlopen(self.RELEASES_DATA) as mock:
            get_label_releases(2294)

        called_url = mock.call_args[0][0].full_url
        # Defaults per signature: include_sublabels=True, page=1, per_page=100
        self.assertIn("include_sublabels=true", called_url)
        self.assertIn("page=1", called_url)
        self.assertIn("per_page=100", called_url)

    def test_rejects_non_numeric_label_id(self):
        with self.assertRaises(AssertionError):
            get_label_releases("../etc/passwd")

        with self.assertRaises(AssertionError):
            get_label_releases("123 OR 1=1")

    def test_include_sublabels_false_passes_through(self):
        with _mock_urlopen({**self.RELEASES_DATA, "include_sublabels": False}) as mock:
            payload = get_label_releases(2294, include_sublabels=False)
        called_url = mock.call_args[0][0].full_url
        self.assertIn("include_sublabels=false", called_url)
        self.assertFalse(payload["include_sublabels"])

    def test_sub_labels_dropped_default_false(self):
        """Plan 002 U3: every successful response carries
        `sub_labels_dropped` so the contract is stable. Default False."""
        with _mock_urlopen(self.RELEASES_DATA):
            payload = get_label_releases(2294, include_sublabels=True)
        self.assertIn("sub_labels_dropped", payload)
        self.assertFalse(payload["sub_labels_dropped"])

    def test_503_falls_back_to_no_sublabels(self):
        """Plan 002 U3: when the upstream returns 503 (timeout) and the
        caller asked for sub-labels, the adapter retries once with
        include_sublabels=False and flags the response."""
        from io import BytesIO
        from urllib.error import HTTPError

        # First call (sub=true) raises 503; second call (sub=false) succeeds.
        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps(
            {**self.RELEASES_DATA, "include_sublabels": False}).encode()
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        seen_urls = []

        def _urlopen(req, *_args, **_kwargs):
            seen_urls.append(req.full_url)
            if "include_sublabels=true" in req.full_url:
                raise HTTPError(
                    req.full_url, 503, "Service Unavailable",
                    hdrs=None,  # type: ignore[arg-type]
                    fp=BytesIO(b'{"error":"timeout"}'))
            return success_resp

        with patch("web.discogs.urllib.request.urlopen", side_effect=_urlopen):
            payload = get_label_releases(
                99887766, include_sublabels=True, page=3, per_page=50)

        self.assertTrue(payload["sub_labels_dropped"])
        # Fallback fetch ran and surfaced its successful payload
        self.assertFalse(payload["include_sublabels"])
        fallback_results = payload["results"]
        assert _is_list(fallback_results)
        self.assertEqual(len(fallback_results), 2)
        self.assertIn("include_sublabels=true", seen_urls[0])
        self.assertIn("page=3", seen_urls[0])
        self.assertIn("per_page=50", seen_urls[0])
        self.assertIn("include_sublabels=false", seen_urls[1])
        self.assertIn("page=3", seen_urls[1])
        self.assertIn("per_page=50", seen_urls[1])

    def test_timeout_falls_back_to_no_sublabels(self):
        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps(
            {**self.RELEASES_DATA, "include_sublabels": False}).encode()
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        def _urlopen(req, *_args, **_kwargs):
            if "include_sublabels=true" in req.full_url:
                raise TimeoutError("timed out")
            return success_resp

        with patch("web.discogs.urllib.request.urlopen", side_effect=_urlopen):
            payload = get_label_releases(99887762, include_sublabels=True)

        self.assertTrue(payload["sub_labels_dropped"])
        self.assertFalse(payload["include_sublabels"])

    def test_include_sublabels_uses_bounded_timeout(self):
        seen_timeouts = []

        def _urlopen(req, *_args, **kwargs):
            seen_timeouts.append(kwargs.get("timeout"))
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(self.RELEASES_DATA).encode()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        with patch("web.discogs._cache.memoize_meta",
                   side_effect=lambda _key, fn: fn()), \
                patch("web.discogs.urllib.request.urlopen", side_effect=_urlopen):
            get_label_releases(99887761, include_sublabels=True)
            get_label_releases(99887760, include_sublabels=False)

        self.assertEqual(seen_timeouts, [20, 60])

    def test_503_then_503_reraises(self):
        """Plan 002 U3: if the fallback also 503s, the original HTTPError
        re-raises. No infinite retry."""
        from io import BytesIO
        from urllib.error import HTTPError

        def _always_503(req, *_args, **_kwargs):
            raise HTTPError(
                req.full_url, 503, "Service Unavailable",
                hdrs=None,  # type: ignore[arg-type]
                fp=BytesIO(b'{"error":"timeout"}'))

        with patch("web.discogs.urllib.request.urlopen", side_effect=_always_503), self.assertRaises(HTTPError):
            get_label_releases(99887765, include_sublabels=True)

    def test_503_when_sub_labels_already_false_reraises(self):
        """Plan 002 U3: 503 with include_sublabels=False has nothing to fall
        back to — re-raise."""
        from io import BytesIO
        from urllib.error import HTTPError

        def _503(req, *_args, **_kwargs):
            raise HTTPError(
                req.full_url, 503, "Service Unavailable",
                hdrs=None,  # type: ignore[arg-type]
                fp=BytesIO(b'{"error":"timeout"}'))

        with patch("web.discogs.urllib.request.urlopen", side_effect=_503), self.assertRaises(HTTPError):
            get_label_releases(99887764, include_sublabels=False)

    def test_404_propagates_unchanged(self):
        """Plan 002 U3: 404 surfaces as 404 (existing route maps it). The
        503 retry must not swallow other HTTP errors."""
        from io import BytesIO
        from urllib.error import HTTPError

        def _404(req, *_args, **_kwargs):
            raise HTTPError(
                req.full_url, 404, "Not Found",
                hdrs=None,  # type: ignore[arg-type]
                fp=BytesIO(b'{"error":"not found"}'))

        with patch("web.discogs.urllib.request.urlopen", side_effect=_404), self.assertRaises(HTTPError):
            get_label_releases(99887763, include_sublabels=True)


class TestWireBoundaryValidation(unittest.TestCase):
    """Issue #1355 item 5 — every general-purpose Discogs endpoint that was
    previously untyped now decodes through a strict ``msgspec.Struct``.
    One RED test per newly-decoded endpoint family: feed a real field the
    wrong wire type and assert ``msgspec.ValidationError`` fires at the
    boundary rather than a ``.get()`` silently tolerating it."""

    def test_search_releases_rejects_non_float_score(self) -> None:
        bad = {"results": [{"id": 1, "title": "Bad Score", "score": "not-a-float"}]}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            search_releases("bad score query")

    def test_search_artists_rejects_non_int_id(self) -> None:
        bad = {"results": [{"id": "not-an-int", "name": "Bad Id"}]}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            search_artists("bad artist id query")

    def test_get_master_releases_rejects_non_int_track_count(self) -> None:
        bad = {
            "title": "Bad Master",
            "primary_type": "Album",
            "releases": [{
                "id": 1, "title": "Bad Release", "released": "2024",
                "country": "US", "track_count": "twelve", "formats": [],
            }],
        }
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_master_releases(9999901)

    def test_get_release_rejects_non_str_title(self) -> None:
        bad = {"id": 9999902, "title": 12345, "artists": [], "tracks": []}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_release(9999902, fresh=True)

    def test_get_artist_name_rejects_non_str_name(self) -> None:
        bad = {"id": 9999903, "name": 12345}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_artist_name(9999903)


if __name__ == "__main__":
    unittest.main()
