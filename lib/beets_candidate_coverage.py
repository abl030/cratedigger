"""One-to-one audio coverage at the Beets candidate boundary.

Beets' distance answers whether metadata resembles a release. It does not
authorize dropping local files: ``AlbumMatch.extra_items`` is precisely the
set Beets would leave behind when applying a candidate. This module keeps the
manifest-integrity decision separate from distance so automatic and force
imports share the same fail-closed rule.
"""

from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass

from lib.quality import CandidateSummary, HarnessItem, TrackMapping


def _normalized_path(path: str) -> str:
    return os.path.normpath(path).replace("\\", os.sep)


@dataclass(frozen=True)
class CandidateAudioCoverage:
    admitted_count: int
    mapped_count: int
    unmapped_paths: tuple[str, ...]
    unexpected_mapped_paths: tuple[str, ...]
    duplicate_admitted_paths: tuple[str, ...]
    duplicate_mapped_paths: tuple[str, ...]
    reported_extra_paths: tuple[str, ...]
    unmatched_track_count: int
    incomplete_composite_paths: tuple[str, ...]

    @property
    def complete(self) -> bool:
        # ``incomplete_composite_paths`` deliberately does NOT gate
        # completeness (issue #1237): a summed declared sub-track duration
        # exceeding the local file's length cannot distinguish a genuinely
        # short composite from Discogs' overlapping-duration convention,
        # where the FIRST sub-position's own declared duration already
        # covers the whole physical track. It remains computed as evidence
        # (see ``detail()`` and ``ValidationResult.incomplete_composite_paths``)
        # but never fails an otherwise-complete mapping.
        return not any((
            self.unmapped_paths,
            self.unexpected_mapped_paths,
            self.duplicate_admitted_paths,
            self.duplicate_mapped_paths,
            self.reported_extra_paths,
            self.unmatched_track_count,
        ))

    @property
    def provable_audio_loss(self) -> bool:
        """Whether the flat-subtrack retry is worth attempting (issue #1237).

        Unmapped admitted audio and/or Beets-reported extra items are the
        provable facts Beets' own mapping asserts -- a composite duration
        disagreement alone is NOT provable loss (see :attr:`complete`) and
        must never trigger the retry by itself, since it cannot tell a
        short composite from Discogs' overlapping-duration convention.

        The retry is still gated on ALSO having :attr:`incomplete_composite_
        paths` -- i.e. Beets already coalesced SOME mapped composite whose
        local duration disagrees with its declared program. Confirmed
        empirically (``tests/test_discogs_subtracks_e2e.py::
        test_complete_composite_plus_extra_cannot_be_flattened_under_
        force``): an unmapped/extra file coexisting with an EXACT-duration
        composite match is a genuine duplicate, and retrying there does not
        merely waste a harness pass -- Beets' real flat-subtrack matcher can
        pair the duplicate against the freed-up second sub-position by
        title/length alone, turning a correct reject into a FALSE
        ``strong_match``. Requiring composite evidence closes THAT specific
        hole (an exact-duration match, so no composite ambiguity at all)
        while still covering the Bowie (extra item) and UNKLE (unmapped
        item) shapes, both of which coalesce a genuinely split composite.

        Stated plainly, this clause does NOT close every such hole. For
        the overlapping-duration convention this issue is centrally about
        (a Discogs release whose first sub-position's OWN declared
        duration already covers the whole physical track), the naive
        declared-duration SUM this module compares against always
        overshoots the true single-file length -- so
        ``incomplete_composite_paths`` is non-empty BY CONSTRUCTION for
        that entire album family, regardless of whether anything is
        actually wrong. An overlapping-duration album that ALSO happens to
        carry some unrelated genuine unmapped/extra file therefore still
        retries and can still hit the same title/length duplicate-pairing
        risk described above. This is a PRE-EXISTING characteristic, not
        one this fix introduces: the pre-#1237 trigger
        (``bool(incomplete_composite_paths)`` alone, with no unmapped/extra
        check at all) retried unconditionally whenever composite evidence
        existed, so an overlapping-duration album plus an unrelated extra
        file behaved identically before this change. Closing it is out of
        scope for issue #1237 (review C8) and remains an open residual.
        """
        return bool(
            (self.unmapped_paths or self.reported_extra_paths)
            and self.incomplete_composite_paths
        )

    def detail(self) -> str:
        parts: list[str] = []
        if self.unmapped_paths:
            parts.append(
                "unmapped admitted audio: " + ", ".join(self.unmapped_paths)
            )
        if self.unexpected_mapped_paths:
            parts.append(
                "mapping references unadmitted audio: "
                + ", ".join(self.unexpected_mapped_paths)
            )
        if self.duplicate_admitted_paths:
            parts.append(
                "duplicate admitted audio paths: "
                + ", ".join(self.duplicate_admitted_paths)
            )
        if self.duplicate_mapped_paths:
            parts.append(
                "audio mapped more than once: "
                + ", ".join(self.duplicate_mapped_paths)
            )
        if self.reported_extra_paths:
            parts.append(
                "beets extra_items: " + ", ".join(self.reported_extra_paths)
            )
        if self.unmatched_track_count:
            parts.append(
                f"beets extra_tracks: {self.unmatched_track_count}"
            )
        if self.incomplete_composite_paths:
            parts.append(
                "incomplete indexed composite audio: "
                + ", ".join(self.incomplete_composite_paths)
            )
        return "; ".join(parts)


def _duplicates(paths: list[str]) -> tuple[str, ...]:
    counts = Counter(paths)
    return tuple(sorted(path for path, count in counts.items() if count > 1))


def _incomplete_composite_detail(mapping: TrackMapping) -> str | None:
    track = mapping.track
    if track.discogs_indexed_component_count <= 1:
        return None
    path = _normalized_path(mapping.item.path)
    if (
        not track.discogs_indexed_duration_complete
        or track.length <= 0
    ):
        return f"{path} (indexed component duration evidence incomplete)"
    if mapping.item.length <= 0:
        return f"{path} (local audio duration unavailable)"
    if mapping.item.length < track.length:
        return (
            f"{path} (local={mapping.item.length:.1f}s, "
            f"indexed_program={track.length:.1f}s)"
        )
    return None


def candidate_audio_coverage(
    admitted_items: list[HarnessItem],
    candidate: CandidateSummary,
) -> CandidateAudioCoverage:
    """Compare the selected mapping with every item Beets admitted.

    Set equality alone is insufficient: a duplicated mapping can otherwise
    hide one missing path behind an equal count. ``extra_items`` and
    ``extra_tracks`` remain explicit independent failures even when a future
    Beets version emits an internally surprising mapping.
    """

    admitted = [_normalized_path(item.path) for item in admitted_items]
    mapped = [
        _normalized_path(mapping.item.path) for mapping in candidate.mapping
    ]
    admitted_set = set(admitted)
    mapped_set = set(mapped)
    reported_extra = tuple(sorted({
        _normalized_path(item.path) for item in candidate.extra_items
    }))
    incomplete_composites = tuple(sorted(
        detail
        for mapping in candidate.mapping
        if (detail := _incomplete_composite_detail(mapping)) is not None
    ))
    return CandidateAudioCoverage(
        admitted_count=len(admitted),
        mapped_count=len(mapped),
        unmapped_paths=tuple(sorted(admitted_set - mapped_set)),
        unexpected_mapped_paths=tuple(sorted(mapped_set - admitted_set)),
        duplicate_admitted_paths=_duplicates(admitted),
        duplicate_mapped_paths=_duplicates(mapped),
        reported_extra_paths=reported_extra,
        unmatched_track_count=len(candidate.extra_tracks),
        incomplete_composite_paths=incomplete_composites,
    )
