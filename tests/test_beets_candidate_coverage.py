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
from tests.helpers import make_candidate_summary


def _item(path: str, *, length: float = 0.0) -> HarnessItem:
    return HarnessItem(path=path, length=length)


def _track(title: str) -> HarnessTrackInfo:
    return HarnessTrackInfo(title=title)


def _candidate(
    *,
    mapped_paths: list[str],
    extra_items: list[str] | None = None,
    extra_tracks: list[str] | None = None,
    composite_path: str | None = None,
    composite_length: float = 0.0,
    component_count: int = 1,
    duration_complete: bool = True,
    indexed_program_length: float = 408.0,
) -> CandidateSummary:
    return make_candidate_summary(
        mbid="2823685",
        data_source="Discogs",
        mapping=[
            TrackMapping(
                item=_item(
                    path,
                    length=(
                        composite_length if path == composite_path else 0.0
                    ),
                ),
                track=HarnessTrackInfo(
                    title=f"track-{idx}",
                    length=(
                        indexed_program_length if path == composite_path else 0.0
                    ),
                    discogs_indexed_component_count=(
                        component_count if path == composite_path else 1
                    ),
                    discogs_indexed_duration_complete=(
                        duration_complete if path == composite_path else True
                    ),
                ),
            )
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

    def test_short_local_composite_duration_disagreement_no_longer_fails_coverage(self) -> None:
        """Issue #1237: a composite duration disagreement is EVIDENCE, never
        a coverage failure -- it cannot distinguish a genuinely short
        composite from Discogs' overlapping-duration convention (the file's
        FIRST sub-position duration already covering the whole physical
        track; see ``test_overlapping_duration_convention_never_fails_
        coverage`` for that exact live shape). Renamed from
        ``test_force_cannot_treat_first_component_as_complete_composite``,
        which pinned the pre-fix reject-on-duration-alone bug this issue
        replaces. The observation itself is preserved (still populates
        ``incomplete_composite_paths`` and ``detail()``) -- only the
        ``complete`` gate moved.
        """
        path = "02 Unwashed And Somewhat Slightly Dazed.flac"
        coverage = candidate_audio_coverage(
            [_item(path, length=369.0)],
            _candidate(
                mapped_paths=[path],
                composite_path=path,
                composite_length=369.0,
                component_count=2,
            ),
        )

        self.assertTrue(coverage.complete)
        self.assertEqual(
            coverage.incomplete_composite_paths,
            (f"{path} (local=369.0s, indexed_program=408.0s)",),
        )
        self.assertIn("incomplete indexed composite audio", coverage.detail())

    def test_overlapping_duration_convention_never_fails_coverage(self) -> None:
        """Issue #1237's founding live shape: Bouncing Souls -- Anchors
        Aweigh (Discogs 461206). Declared sub-durations 9:37 + 3:24 = 781s
        SUM over the installed 579.7s file because Discogs' overlapping
        convention makes the FIRST sub-position's own declared duration
        already cover the whole physical track. The mapping itself drops
        nothing -- must not be rejected for the duration disagreement.
        """
        path = "16 Untitled.flac"
        coverage = candidate_audio_coverage(
            [_item(path, length=579.7)],
            _candidate(
                mapped_paths=[path],
                composite_path=path,
                composite_length=579.7,
                component_count=2,
                indexed_program_length=781.0,
            ),
        )

        self.assertTrue(coverage.complete)
        self.assertEqual(
            coverage.incomplete_composite_paths,
            (f"{path} (local=579.7s, indexed_program=781.0s)",),
        )

    def test_complete_composite_program_is_covered(self) -> None:
        path = "02 Unwashed And Somewhat Slightly Dazed + Don't Sit Down.flac"
        coverage = candidate_audio_coverage(
            [_item(path, length=408.0)],
            _candidate(
                mapped_paths=[path],
                composite_path=path,
                composite_length=408.0,
                component_count=2,
            ),
        )

        self.assertTrue(coverage.complete)

    def test_short_indexed_component_no_longer_fails_but_remains_evidenced(self) -> None:
        """Renamed from ``test_short_indexed_component_cannot_hide_inside_
        tolerance`` -- issue #1237 removes the duration gate; the
        observation is unchanged.
        """
        path = "A2 incomplete composite.flac"
        coverage = candidate_audio_coverage(
            [_item(path, length=399.0)],
            _candidate(
                mapped_paths=[path],
                composite_path=path,
                composite_length=399.0,
                component_count=2,
            ),
        )

        self.assertTrue(coverage.complete)
        self.assertEqual(
            coverage.incomplete_composite_paths,
            (f"{path} (local=399.0s, indexed_program=408.0s)",),
        )

    def test_unknown_component_duration_no_longer_fails_but_remains_evidenced(self) -> None:
        """Renamed from ``test_unknown_component_duration_fails_closed`` --
        issue #1237 removes the duration gate (including the unprovable-
        duration case); the observation is unchanged.
        """
        path = "A2 unprovable composite.flac"
        coverage = candidate_audio_coverage(
            [_item(path, length=500.0)],
            _candidate(
                mapped_paths=[path],
                composite_path=path,
                composite_length=500.0,
                component_count=2,
                duration_complete=False,
            ),
        )

        self.assertTrue(coverage.complete)
        self.assertEqual(
            coverage.incomplete_composite_paths,
            (f"{path} (indexed component duration evidence incomplete)",),
        )


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
def _property_candidate_coverage_oracle(
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


@given(
    local_length=st.floats(min_value=0.0, max_value=2000.0, allow_nan=False, allow_infinity=False),
    indexed_length=st.floats(min_value=0.0, max_value=2000.0, allow_nan=False, allow_infinity=False),
    component_count=st.integers(min_value=1, max_value=5),
    duration_complete=st.booleans(),
)
def _property_complete_mapping_never_rejected_for_duration_disagreement(
    local_length: float,
    indexed_length: float,
    component_count: int,
    duration_complete: bool,
) -> None:
    """Issue #1237: a candidate whose mapping drops no admitted file is
    never rejected for a composite duration disagreement -- regardless of
    how short, long, or unprovable the declared indexed program looks.
    """
    path = "composite.flac"
    coverage = candidate_audio_coverage(
        [_item(path, length=local_length)],
        _candidate(
            mapped_paths=[path],
            composite_path=path,
            composite_length=local_length,
            component_count=component_count,
            duration_complete=duration_complete,
            indexed_program_length=indexed_length,
        ),
    )
    assert coverage.complete


@given(
    include_extra_file=st.booleans(),
    local_length=st.floats(min_value=0.0, max_value=2000.0, allow_nan=False, allow_infinity=False),
    indexed_length=st.floats(min_value=0.0, max_value=2000.0, allow_nan=False, allow_infinity=False),
    component_count=st.integers(min_value=1, max_value=5),
)
@example(include_extra_file=True, local_length=369.0, indexed_length=408.0, component_count=1)
def _property_provable_audio_loss_needs_a_lost_file_and_composite_evidence(
    include_extra_file: bool,
    local_length: float,
    indexed_length: float,
    component_count: int,
) -> None:
    """Issue #1237: two NECESSARY-CONDITION invariants for the shared
    retry trigger, driven through the REAL ``candidate_audio_coverage()``
    pipeline (not a hand-built ``CandidateAudioCoverage``, and NOT a
    mirror of ``provable_audio_loss``'s own ``(unmapped or extra) and
    incomplete_composite_paths`` expression -- issue #1237 review C2: a
    property that restates the implementation verbatim cannot catch a
    defect IN that expression):

    1. If Beets' own mapping drops no admitted file, the retry is never
       worth attempting -- regardless of any composite evidence.
    2. If there is no composite evidence a coalesced track could explain
       the loss, the retry is never worth attempting -- regardless of any
       unmapped/extra file.

    The pinned ``@example`` (component_count=1, so no composite evidence
    is even possible) is the exact world that kills the mutant dropping
    the ``and incomplete_composite_paths`` clause: with it removed,
    ``provable_audio_loss`` would wrongly become ``True`` from the extra
    file alone, violating invariant 2.
    """
    composite_path = "composite.flac"
    mapped_paths = [composite_path]
    extra_items: list[str] = []
    admitted_paths = [composite_path]
    if include_extra_file:
        extra_items = ["extra.flac"]
        admitted_paths = [composite_path, "extra.flac"]
    coverage = candidate_audio_coverage(
        [_item(path) for path in admitted_paths],
        _candidate(
            mapped_paths=mapped_paths,
            extra_items=extra_items,
            composite_path=composite_path,
            composite_length=local_length,
            component_count=component_count,
            indexed_program_length=indexed_length,
        ),
    )
    if not (coverage.unmapped_paths or coverage.reported_extra_paths):
        assert not coverage.provable_audio_loss
    if not coverage.incomplete_composite_paths:
        assert not coverage.provable_audio_loss


class TestBeetsCandidateCoverageGenerated(unittest.TestCase):
    """Wraps the module-level Hypothesis properties above as real
    ``unittest`` test methods (issue #1237 review C2): every automated
    runner in this repo (targeted selection, the full suite, the fuzz
    burst) discovers tests via ``unittest.defaultTestLoader``, which never
    finds a bare ``def test_*`` function outside a ``TestCase`` -- proven
    by replanting the ``provable_audio_loss`` mutant (dropping the
    composite-evidence clause) and confirming it now dies through THIS
    class, where it previously survived the whole module.
    """

    def test_candidate_coverage_oracle(self) -> None:
        _property_candidate_coverage_oracle()

    def test_complete_mapping_never_rejected_for_duration_disagreement(self) -> None:
        _property_complete_mapping_never_rejected_for_duration_disagreement()

    def test_provable_audio_loss_needs_a_lost_file_and_composite_evidence(self) -> None:
        _property_provable_audio_loss_needs_a_lost_file_and_composite_evidence()

    def test_oracle_kills_count_only_mutant(self) -> None:
        """Known-bad self-test: equal counts are not set coverage."""
        admitted = {"track-0.flac", "track-1.flac"}
        mapped = ["track-0.flac", "track-0.flac"]
        count_only_mutant = len(mapped) == len(admitted)
        self.assertTrue(count_only_mutant)
        coverage = candidate_audio_coverage(
            [_item(path) for path in sorted(admitted)],
            _candidate(mapped_paths=mapped),
        )
        self.assertFalse(coverage.complete)


if __name__ == "__main__":
    unittest.main()
