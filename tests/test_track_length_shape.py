"""Track-length disagreement shapes (issue #1445 item 1).

Pure unit tests for ``lib.track_length_shape``. The named worlds are real
download_log rows pinned in ``tests/fixtures/track_length_shapes.json``;
the boundary worlds are hand-built.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from lib.track_length_shape import (
    TRACK_LENGTH_WARNING_BOUND_SECONDS,
    LengthPair,
    TrackLengthShape,
    TrackLengthShapeKind,
    classify_track_length_shape,
    format_duration,
)

_FIXTURE = Path(__file__).parent / "fixtures" / "track_length_shapes.json"


def _real_rows() -> dict[int, list[LengthPair]]:
    """download_log id -> its target-candidate pairs.

    The export carries no ``medium_total``; the medium's track count is
    the highest mapped position on that medium, which is what it is for
    every pinned row (none has an unmapped trailing track).
    """
    raw = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    rows: dict[int, list[LengthPair]] = {}
    for row in raw["rows"]:
        last_on_medium: dict[int, int] = {}
        for medium, index, *_rest in row["pairs"]:
            last_on_medium[medium] = max(last_on_medium.get(medium, 0), index)
        rows[row["download_log_id"]] = [
            LengthPair(
                medium=medium, medium_index=index,
                medium_track_count=last_on_medium[medium],
                file_seconds=file_seconds, listed_seconds=listed_seconds,
                component_count=component_count, title=title,
                file_name=f"{index:02d} {title}.flac",
            )
            for medium, index, file_seconds, listed_seconds,
            component_count, title in row["pairs"]
        ]
    return rows


REAL = _real_rows()


def _album(
    lengths: list[tuple[float, float]], *, medium: int = 1,
    track_count: int | None = None,
) -> list[LengthPair]:
    """One medium of (file, listed) pairs, positions 1..n."""
    count = len(lengths) if track_count is None else track_count
    return [
        LengthPair(
            medium=medium, medium_index=index, medium_track_count=count,
            file_seconds=file_seconds, listed_seconds=listed_seconds,
            title=f"Song {index}", file_name=f"{index:02d}.flac",
        )
        for index, (file_seconds, listed_seconds) in enumerate(lengths, 1)
    ]


def _shape(pairs: list[LengthPair]) -> TrackLengthShape:
    shape = classify_track_length_shape(pairs)
    assert shape is not None, "expected a shape, got None"
    return shape


class TestRealRows(unittest.TestCase):
    def test_kurt_vile_rotation_is_wrong_slots(self) -> None:
        shape = _shape(REAL[24179])
        self.assertEqual(shape.kind, TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS)
        self.assertEqual(shape.label, "wrong order?")
        self.assertEqual(
            shape.sentence,
            "Tracks 2, 3 and 5 have each other's lengths. "
            "The titles are probably on the wrong songs.",
        )
        self.assertFalse(shape.probably_harmless)
        self.assertEqual([slot.medium_index for slot in shape.slots], [2, 3, 5])

    def test_genesis_split_is_split_differently(self) -> None:
        shape = _shape(REAL[15039])
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY)
        self.assertEqual(shape.label, "split differently")
        self.assertEqual(
            shape.sentence,
            "Tracks 2 and 3 on disc 1 are cut at a different point but add "
            "up to about the same total.",
        )
        self.assertTrue(shape.probably_harmless)
        self.assertEqual([slot.medium_index for slot in shape.slots], [2, 3])

    def test_chumbawamba_neighbour_absorbs_the_difference(self) -> None:
        shape = _shape(REAL[35012])
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY)
        self.assertEqual([slot.medium_index for slot in shape.slots], [6, 7])
        self.assertIn("Tracks 6 and 7 are cut", shape.sentence)

    def test_bob_interview_disc_shorter_is_single_track_disc_and_warns(
        self,
    ) -> None:
        shape = _shape(REAL[22324])
        self.assertEqual(shape.kind, TrackLengthShapeKind.SINGLE_TRACK_DISC)
        self.assertEqual(
            shape.sentence,
            "Disc 2 is a single track that runs 5:19 but the release lists "
            "30:15. Some of the audio listed for that disc may be absent.",
        )
        self.assertFalse(shape.probably_harmless)

    def test_slipknot_dvd_disc_is_single_track_disc_and_formats_hours(
        self,
    ) -> None:
        shape = _shape(REAL[14702])
        self.assertEqual(shape.kind, TrackLengthShapeKind.SINGLE_TRACK_DISC)
        self.assertEqual(
            shape.sentence,
            "Disc 2 is a single track that runs 1:12:19 but the release "
            "lists 34:13. Usually a bonus or video disc in a different cut.",
        )
        self.assertTrue(shape.probably_harmless)

    def test_phoebe_bridgers_last_track_longer_is_hidden_track(self) -> None:
        shape = _shape(REAL[40061])
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.HIDDEN_TRACK_IN_FILE)
        self.assertEqual(shape.label, "hidden track?")
        self.assertEqual(
            shape.sentence,
            "Last track runs 3:58 but the release lists 0:15. Usually a "
            "hidden track kept in the same file. Probably fine.",
        )
        self.assertTrue(shape.probably_harmless)

    def test_eels_last_track_shorter_is_ending_missing(self) -> None:
        shape = _shape(REAL[12290])
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.END_OF_LAST_TRACK_MISSING)
        self.assertEqual(shape.label, "ending missing?")
        self.assertEqual(
            shape.sentence,
            "Last track runs 3:13 but the release lists 15:05. Either the "
            "release counts a hidden track here that this copy does not "
            "have, or the file stops early.",
        )
        self.assertFalse(shape.probably_harmless)

    def test_ye_mid_album_longer_is_one_track_longer(self) -> None:
        shape = _shape(REAL[13331])
        self.assertEqual(shape.kind, TrackLengthShapeKind.ONE_TRACK_LONGER)
        self.assertTrue(shape.sentence.startswith(
            "Track 13 runs 9:48 but the release lists 4:47."))

    def test_okkervil_river_shorter_with_unlisted_neighbours(self) -> None:
        """Four of five slots carry no listed length — they are unusable,
        so they can neither count nor absorb anything."""
        shape = _shape(REAL[10043])
        self.assertEqual(shape.kind, TrackLengthShapeKind.ONE_TRACK_SHORTER)
        self.assertTrue(shape.sentence.startswith(
            "Track 2 runs 3:53 but the release lists 6:50."))

    def test_fairways_two_unrelated_tracks_is_several_differ(self) -> None:
        shape = _shape(REAL[42692])
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER)
        self.assertEqual(shape.label, "several differ")
        self.assertEqual(
            shape.sentence,
            "2 of 10 tracks are a different length from the release "
            "(track 2 runs 3:26, listed 2:07). Could be a different "
            "edition or the wrong audio.",
        )
        self.assertEqual([slot.medium_index for slot in shape.slots], [2, 5])

    def test_razorlight_partial_rotation_is_not_wrong_slots(self) -> None:
        """Slot 13's file has slot 12's listed length, but slot 9 (off 16s)
        and slot 12 have no partner: not a clean rotation."""
        shape = _shape(REAL[28366])
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER)


class TestBoundaries(unittest.TestCase):
    def test_no_pairs_no_shape(self) -> None:
        self.assertIsNone(classify_track_length_shape([]))

    def test_exactly_the_bound_is_not_a_disagreement(self) -> None:
        pairs = _album([
            (100.0, 100.0),
            (100.0 + TRACK_LENGTH_WARNING_BOUND_SECONDS, 100.0),
            (100.0, 100.0),
        ])
        self.assertIsNone(classify_track_length_shape(pairs))

    def test_just_over_the_bound_is(self) -> None:
        pairs = _album([
            (100.0, 100.0), (160.1, 100.0), (100.0, 100.0),
        ])
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.ONE_TRACK_LONGER)

    def test_missing_lengths_are_skipped(self) -> None:
        pairs = _album([(0.0, 500.0), (500.0, 0.0), (100.0, 100.0)])
        self.assertIsNone(classify_track_length_shape(pairs))

    def test_composite_pair_is_skipped(self) -> None:
        pairs = [
            LengthPair(
                medium=1, medium_index=1, medium_track_count=1,
                file_seconds=900.0, listed_seconds=100.0, component_count=3,
            ),
        ]
        self.assertIsNone(classify_track_length_shape(pairs))

    def test_composite_cannot_absorb_a_neighbours_difference(self) -> None:
        pairs = _album([(100.0, 100.0), (100.0, 200.0), (300.0, 200.0),
                        (100.0, 100.0)])
        pairs[2] = LengthPair(
            medium=1, medium_index=3, medium_track_count=4,
            file_seconds=300.0, listed_seconds=200.0, component_count=2,
        )
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.ONE_TRACK_SHORTER)

    def test_rotation_within_exactly_two_seconds_matches(self) -> None:
        pairs = _album([(302.0, 200.0), (200.0, 300.0), (100.0, 100.0)])
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS)

    def test_rotation_just_over_two_seconds_does_not(self) -> None:
        pairs = _album([(302.1, 200.0), (200.0, 300.0), (100.0, 100.0),
                        (100.0, 100.0)])
        self.assertNotEqual(
            _shape(pairs).kind, TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS)

    def test_rotation_ignores_slots_off_by_ten_seconds_or_less(self) -> None:
        # Slot 3 is off by exactly 10s: it is not in the set that has to
        # rotate, so the swap of 1 and 2 still reads as wrong slots.
        pairs = _album([(300.0, 200.0), (200.0, 300.0), (110.0, 100.0)])
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS)

    def test_split_within_two_seconds_per_track(self) -> None:
        # Two-track run: allowance is 4s total.
        pairs = _album([(100.0, 100.0), (130.0, 200.0), (274.0, 200.0),
                        (100.0, 100.0)])
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY)

    def test_split_just_outside_allowance(self) -> None:
        pairs = _album([(100.0, 100.0), (130.0, 200.0), (274.1, 200.0),
                        (100.0, 100.0)])
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER)

    def test_split_runs_never_cross_a_disc(self) -> None:
        pairs = [
            *_album([(100.0, 100.0), (130.0, 200.0)], medium=1),
            *_album([(270.0, 200.0), (100.0, 100.0)], medium=2),
        ]
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER)

    def test_last_track_of_first_disc_on_a_multi_disc_release(self) -> None:
        pairs = [
            *_album([(100.0, 100.0), (400.0, 200.0)], medium=1),
            *_album([(100.0, 100.0), (100.0, 100.0)], medium=2),
        ]
        shape = _shape(pairs)
        self.assertEqual(shape.kind, TrackLengthShapeKind.HIDDEN_TRACK_IN_FILE)
        self.assertTrue(shape.sentence.startswith(
            "Last track on disc 1 runs 6:40 but the release lists 3:20."))

    def test_last_mapped_is_not_last_when_the_medium_has_more_tracks(
        self,
    ) -> None:
        pairs = _album([(100.0, 100.0), (400.0, 200.0)], track_count=3)
        self.assertEqual(
            _shape(pairs).kind, TrackLengthShapeKind.ONE_TRACK_LONGER)

    def test_unknown_positions_fall_back_to_the_title(self) -> None:
        pairs = [
            LengthPair(
                medium=None, medium_index=None, medium_track_count=None,
                file_seconds=237.6, listed_seconds=15.0,
                title="Lost Weekend", file_name="00 - Hidden Track.flac",
            ),
        ]
        shape = _shape(pairs)
        self.assertEqual(shape.kind, TrackLengthShapeKind.ONE_TRACK_LONGER)
        self.assertTrue(shape.sentence.startswith(
            '"Lost Weekend" runs 3:58 but the release lists 0:15.'))

    def test_slot_summary_names_title_and_both_lengths(self) -> None:
        shape = _shape(REAL[42692])
        self.assertEqual(
            shape.slots[0].summary,
            'Track 2 "Secretive Girl" runs 3:26, release lists 2:07',
        )


class TestReviewFindings(unittest.TestCase):
    def test_a_neighbour_is_borrowed_at_most_once(self) -> None:
        """Tracks 2 and 4 each balance against track 3, but track 3 can
        only absorb one of them: tracks 2-4 run 62s over the listing, so
        this is not a clean split."""
        pairs = _album([(200.0, 200.0), (261.0, 200.0), (140.0, 200.0),
                        (261.0, 200.0), (200.0, 200.0)])
        shape = _shape(pairs)
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER)
        self.assertEqual([slot.medium_index for slot in shape.slots], [2, 4])

    def test_non_finite_lengths_are_unusable(self) -> None:
        for bad in (float("inf"), float("-inf"), float("nan")):
            with self.subTest(bad=bad):
                self.assertIsNone(classify_track_length_shape(
                    _album([(100.0, 100.0), (bad, 100.0)])))
                self.assertIsNone(classify_track_length_shape(
                    _album([(100.0, 100.0), (100.0, bad)])))
                shape = _shape(_album([
                    (bad, 100.0), (100.0, 100.0), (300.0, 100.0),
                    (100.0, 100.0)]))
                self.assertEqual(
                    shape.kind, TrackLengthShapeKind.ONE_TRACK_LONGER)
                self.assertTrue(shape.sentence.startswith(
                    "Track 3 runs 5:00 but the release lists 1:40."))

    def test_one_track_release_is_not_a_single_track_disc(self) -> None:
        longer = _shape(_album([(400.0, 200.0)]))
        self.assertEqual(longer.kind, TrackLengthShapeKind.ONE_TRACK_LONGER)
        self.assertTrue(longer.sentence.startswith(
            "Track 1 runs 6:40 but the release lists 3:20."))
        shorter = _shape(_album([(100.0, 200.0)]))
        self.assertEqual(shorter.kind, TrackLengthShapeKind.ONE_TRACK_SHORTER)

    def test_single_track_disc_longer_is_calm_shorter_warns(self) -> None:
        first_disc = _album([(100.0, 100.0), (100.0, 100.0)], medium=1)
        longer = _shape([*first_disc, *_album([(400.0, 200.0)], medium=2)])
        self.assertEqual(longer.kind, TrackLengthShapeKind.SINGLE_TRACK_DISC)
        self.assertTrue(longer.probably_harmless)
        shorter = _shape([*first_disc, *_album([(100.0, 200.0)], medium=2)])
        self.assertEqual(shorter.kind, TrackLengthShapeKind.SINGLE_TRACK_DISC)
        self.assertFalse(shorter.probably_harmless)
        self.assertEqual(
            shorter.sentence,
            "Disc 2 is a single track that runs 1:40 but the release lists "
            "3:20. Some of the audio listed for that disc may be absent.",
        )

    def test_several_counts_only_compared_tracks(self) -> None:
        pairs = _album([(300.0, 100.0), (100.0, 100.0), (50.0, 0.0),
                        (100.0, 200.0), (100.0, 100.0), (0.0, 80.0)])
        shape = _shape(pairs)
        self.assertEqual(
            shape.kind, TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER)
        self.assertTrue(shape.sentence.startswith(
            "2 of 4 tracks are a different length from the release"))

    def test_split_sentence_says_about_the_same_total(self) -> None:
        shape = _shape(REAL[35012])
        self.assertEqual(
            shape.sentence,
            "Tracks 6 and 7 are cut at a different point but add up to "
            "about the same total.",
        )


class TestCopy(unittest.TestCase):
    def test_every_real_sentence_avoids_internal_words(self) -> None:
        for log_id, pairs in REAL.items():
            shape = _shape(pairs)
            text = " ".join(
                [shape.sentence, shape.label,
                 *(slot.summary for slot in shape.slots)]).lower()
            for word in ("slot", "deviat", "medium", "candidate", "mapping"):
                self.assertNotIn(word, text, f"{log_id}: {text}")

    def test_format_duration(self) -> None:
        self.assertEqual(format_duration(0.4), "0:00")
        self.assertEqual(format_duration(59.5), "1:00")
        self.assertEqual(format_duration(905.4), "15:05")
        self.assertEqual(format_duration(4338.9), "1:12:19")


if __name__ == "__main__":
    unittest.main()
