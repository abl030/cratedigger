"""Candidate audio-coverage invariants for the Beets import boundary.

Issue #1183: Beets mapped nine of ten admitted Bowie files and exposed the
tenth as ``extra_items``. Applying that candidate discarded real audio. The
mapping is safe iff every admitted item appears exactly once and Beets reports
neither an extra local item nor an unmatched release track.
"""

from __future__ import annotations

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_candidate_coverage import candidate_audio_coverage
from lib.quality import (
    CandidateSummary,
    HarnessItem,
    HarnessTrackInfo,
    TrackMapping,
)


def _item(path: str) -> HarnessItem:
    return HarnessItem(path=path)


def _track(title: str) -> HarnessTrackInfo:
    return HarnessTrackInfo(title=title)


def _candidate(
    *,
    mapped_paths: list[str],
    extra_items: list[str] | None = None,
    extra_tracks: list[str] | None = None,
) -> CandidateSummary:
    return CandidateSummary(
        mbid="2823685",
        data_source="Discogs",
        mapping=[
            TrackMapping(item=_item(path), track=_track(f"track-{idx}"))
            for idx, path in enumerate(mapped_paths, start=1)
        ],
        extra_items=[_item(path) for path in (extra_items or [])],
        extra_tracks=[_track(title) for title in (extra_tracks or [])],
    )


class TestCandidateAudioCoverage(unittest.TestCase):
    def test_exact_one_to_one_mapping_is_complete(self) -> None:
        admitted = [_item("A1.flac"), _item("A2.1.flac"), _item("A2.2.flac")]
        coverage = candidate_audio_coverage(
            admitted,
            _candidate(mapped_paths=[item.path for item in admitted]),
        )

        self.assertTrue(coverage.complete)
        self.assertEqual(coverage.admitted_count, 3)
        self.assertEqual(coverage.mapped_count, 3)
        self.assertEqual(coverage.detail(), "")

    def test_bowie_indexed_subtrack_extra_item_is_incomplete(self) -> None:
        """Pinned production shape: A2.2 must never be discarded again."""
        admitted = [
            _item("01 Space Oddity.flac"),
            _item("02 Unwashed And Somewhat Slightly Dazed.flac"),
            _item("03 Don't Sit Down.flac"),
        ]
        coverage = candidate_audio_coverage(
            admitted,
            _candidate(
                mapped_paths=[
                    "01 Space Oddity.flac",
                    "02 Unwashed And Somewhat Slightly Dazed.flac",
                ],
                extra_items=["03 Don't Sit Down.flac"],
            ),
        )

        self.assertFalse(coverage.complete)
        self.assertEqual(coverage.unmapped_paths, ("03 Don't Sit Down.flac",))
        self.assertIn("unmapped admitted audio", coverage.detail())

    def test_duplicate_mapping_does_not_mask_missing_audio_by_count(self) -> None:
        admitted = [_item("A1.flac"), _item("A2.flac")]
        coverage = candidate_audio_coverage(
            admitted,
            _candidate(mapped_paths=["A1.flac", "A1.flac"]),
        )

        self.assertFalse(coverage.complete)
        self.assertEqual(coverage.duplicate_mapped_paths, ("A1.flac",))
        self.assertEqual(coverage.unmapped_paths, ("A2.flac",))

    def test_unmatched_release_track_is_incomplete(self) -> None:
        coverage = candidate_audio_coverage(
            [_item("A1.flac")],
            _candidate(mapped_paths=["A1.flac"], extra_tracks=["A2"]),
        )

        self.assertFalse(coverage.complete)
        self.assertEqual(coverage.unmatched_track_count, 1)


_PATHS = tuple(f"track-{idx}.flac" for idx in range(6))


@given(
    admitted=st.sets(st.sampled_from(_PATHS), min_size=1),
    mapped=st.lists(st.sampled_from(_PATHS), max_size=8),
    reported_extra=st.sets(st.sampled_from(_PATHS)),
    extra_track_count=st.integers(min_value=0, max_value=3),
)
@example(
    admitted={"track-0.flac", "track-1.flac"},
    mapped=["track-0.flac", "track-0.flac"],
    reported_extra=set(),
    extra_track_count=0,
)
def test_generated_candidate_coverage_oracle(
    admitted: set[str],
    mapped: list[str],
    reported_extra: set[str],
    extra_track_count: int,
) -> None:
    candidate = _candidate(
        mapped_paths=mapped,
        extra_items=sorted(reported_extra),
        extra_tracks=[f"extra-{idx}" for idx in range(extra_track_count)],
    )
    coverage = candidate_audio_coverage(
        [_item(path) for path in sorted(admitted)],
        candidate,
    )
    expected = (
        len(mapped) == len(set(mapped))
        and set(mapped) == admitted
        and not reported_extra
        and extra_track_count == 0
    )
    assert coverage.complete is expected


def test_generated_oracle_kills_count_only_mutant() -> None:
    """Known-bad self-test: equal counts are not set coverage."""
    admitted = {"track-0.flac", "track-1.flac"}
    mapped = ["track-0.flac", "track-0.flac"]
    count_only_mutant = len(mapped) == len(admitted)
    assert count_only_mutant
    coverage = candidate_audio_coverage(
        [_item(path) for path in sorted(admitted)],
        _candidate(mapped_paths=mapped),
    )
    assert not coverage.complete


if __name__ == "__main__":
    unittest.main()
