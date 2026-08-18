"""Per-track length gate at the candidate→scenario boundary (issue #1178).

download_log 40061 imported request 8954 (MB release
``ff3d5ba9-c7ab-41a5-86bd-9ceb7e1f9bd1``) at beets distance 0.1441 —
``strong_match`` — while its selected mapping paired local file
``00 - Hidden Track.flac`` (237.6s) with MB track 17 "Lost Weekend" (declared
15.0s). Beets clamps track-length error at ``track_length_max`` (30s) when
computing distance, so a 222.6s mismatch cost the same as a 41s one and never
moved the distance past 0.15. ``apply_candidate_scenario`` (``lib/beets.py``)
is the single place a candidate becomes a scenario — both ``beets_validate``
and the merge-redirect seam in ``lib/download_validation.py`` call it — so a
gate added there is inherited by both.
"""

import unittest

from lib.beets import TRACK_LENGTH_MISMATCH_BOUND_SECONDS, apply_candidate_scenario
from lib.quality import (
    CandidateSummary,
    HarnessItem,
    HarnessTrackInfo,
    TrackMapping,
    ValidationResult,
)

#: The exact release named in the #1178 report.
_LOST_WEEKEND_MBID = "ff3d5ba9-c7ab-41a5-86bd-9ceb7e1f9bd1"


def _mapping(
    path: str,
    *,
    item_length: float,
    track_length: float,
    item_title: str = "",
    track_title: str = "",
) -> TrackMapping:
    return TrackMapping(
        item=HarnessItem(path=path, title=item_title or path, length=item_length),
        track=HarnessTrackInfo(
            title=track_title or item_title or path, length=track_length,
        ),
    )


def _candidate(
    mappings: list[TrackMapping],
    *,
    distance: float,
    mbid: str = _LOST_WEEKEND_MBID,
    extra_track_titles: list[str] | None = None,
) -> CandidateSummary:
    """One beets candidate as the harness would serialize it.

    ``extra_track_titles`` models a coverage-incomplete or extra-tracks
    world for the ordering pins below; empty by default.
    """
    return CandidateSummary(
        mbid=mbid,
        artist="Phoebe Bridgers",
        album="Lost Weekend",
        distance=distance,
        mapping=mappings,
        extra_tracks=[
            HarnessTrackInfo(title=title) for title in (extra_track_titles or [])
        ],
    )


def _admitted_items(candidate: CandidateSummary) -> list[HarnessItem]:
    """Every admitted item maps exactly once — coverage is complete by
    construction, so these pins isolate the track-length gate alone."""
    return [mapping.item for mapping in candidate.mapping]


def _apply(
    candidate: CandidateSummary,
    *,
    distance_threshold: float = 0.15,
    track_length_bound: float | None = TRACK_LENGTH_MISMATCH_BOUND_SECONDS,
) -> ValidationResult:
    result = ValidationResult()
    apply_candidate_scenario(
        result,
        candidate,
        distance_threshold,
        admitted_items=_admitted_items(candidate),
        track_length_bound=track_length_bound,
    )
    return result


class TestTrackLengthMismatchGate(unittest.TestCase):
    def test_download_log_40061_hidden_track_pairing_is_rejected(self) -> None:
        """The exact #1178 world: a 237.6s file paired with a declared
        15.0s track, at a distance (0.1441) that clears the 0.15 gate."""
        candidate = _candidate(
            distance=0.1441,
            mappings=[
                _mapping(
                    "02 The Outside.flac",
                    item_length=237.7, track_length=237.0,
                    item_title="The Outside", track_title="The Outside",
                ),
                _mapping(
                    "00 - Hidden Track.flac",
                    item_length=237.633167, track_length=15.0,
                    item_title="Hidden Track", track_title="Lost Weekend",
                ),
            ],
        )

        result = _apply(candidate)

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "track_length_mismatch")
        assert result.detail is not None
        self.assertIn("00 - Hidden Track.flac", result.detail)
        self.assertIn("237.6", result.detail)
        self.assertIn("15.0", result.detail)

    def test_compliant_world_under_bound_stays_strong_match(self) -> None:
        """Must-still-work (a): every deviation is well under the bound."""
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping("01.flac", item_length=100.0, track_length=100.4),
                _mapping("02.flac", item_length=140.0, track_length=141.0),
            ],
        )

        result = _apply(candidate)

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")

    def test_pregap_world_with_missing_declared_length_is_skipped(self) -> None:
        """Must-still-work (b): a CD pregap hidden track carries NO declared
        MB length (0.0) — the gate must skip it, never flag it."""
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping(
                    "00 - Hidden Track.flac",
                    item_length=237.6, track_length=0.0,
                    item_title="Hidden Track",
                ),
                _mapping("02.flac", item_length=100.0, track_length=100.0),
            ],
        )

        result = _apply(candidate)

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")

    def test_force_lane_disables_the_gate(self) -> None:
        """Must-still-work (c): ``track_length_bound=None`` is force
        import's exact override — the same treatment as ``distance_threshold
        =FORCE_IMPORT_DISTANCE_THRESHOLD`` (#1080)."""
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping(
                    "00 - Hidden Track.flac",
                    item_length=237.6, track_length=15.0,
                    item_title="Hidden Track", track_title="Lost Weekend",
                ),
            ],
        )

        result = _apply(candidate, track_length_bound=None)

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")

    def test_a_file_much_shorter_than_its_declared_track_is_rejected(self) -> None:
        """The deviation is unsigned: a file SHORTER than its declared
        length by more than the bound is rejected exactly like one that is
        longer. Kills a mutant that compares ``item.length - track.length``
        (signed) instead of the absolute deviation."""
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping(
                    "01.flac", item_length=15.0, track_length=237.6,
                ),
            ],
        )

        result = _apply(candidate)

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "track_length_mismatch")

    def test_exact_bound_deviation_is_not_a_mismatch(self) -> None:
        """The comparison is strictly-greater-than: a deviation exactly AT
        the bound still clears the gate."""
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping(
                    "01.flac",
                    item_length=100.0 + TRACK_LENGTH_MISMATCH_BOUND_SECONDS,
                    track_length=100.0,
                ),
            ],
        )

        result = _apply(candidate)

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")

    def test_deviation_just_over_the_bound_is_rejected(self) -> None:
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping(
                    "01.flac",
                    item_length=100.0 + TRACK_LENGTH_MISMATCH_BOUND_SECONDS + 0.1,
                    track_length=100.0,
                ),
            ],
        )

        result = _apply(candidate)

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "track_length_mismatch")

    def test_extra_tracks_takes_precedence_over_track_length_mismatch(self) -> None:
        """Ordering pin: ``extra_tracks`` is checked before the length gate,
        so a candidate hitting both is named by the earlier branch."""
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping(
                    "00 - Hidden Track.flac",
                    item_length=237.6, track_length=15.0,
                ),
            ],
            extra_track_titles=["Bonus Track"],
        )

        result = _apply(candidate)

        self.assertEqual(result.scenario, "extra_tracks")

    def test_unmapped_audio_takes_precedence_over_track_length_mismatch(self) -> None:
        """Ordering pin: an incomplete mapping is named ``unmapped_audio``
        even when a mapped pair also carries a length mismatch — admitted
        items are a superset of what the candidate mapped."""
        candidate = _candidate(
            distance=0.05,
            mappings=[
                _mapping(
                    "00 - Hidden Track.flac",
                    item_length=237.6, track_length=15.0,
                ),
            ],
        )
        result = ValidationResult()
        admitted = [
            *_admitted_items(candidate),
            HarnessItem(path="99 - Unmapped Bonus.flac", length=42.0),
        ]

        apply_candidate_scenario(
            result, candidate, 0.15, admitted_items=admitted,
        )

        self.assertEqual(result.scenario, "unmapped_audio")


if __name__ == "__main__":
    unittest.main()
