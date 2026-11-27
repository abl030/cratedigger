"""Compare a candidate copy with the installed copy it would replace.

Issue #1445 item 3. An automatic upgrade is the one automatic action that
destroys something: Beets removes the installed copy while it imports the
candidate. Request 1649 (The Fairways) lost a correct MP3 that way to a
FLAC whose track 2 ran 3:26 where the old copy and the disc both say 2:07
(a different song). Beets clamps track-length error at 30s, so the match
distance never noticed.

This module is pure: typed tracks in, a typed comparison out, no I/O. It
does not decide anything on its own. The dispatch core turns
:attr:`InstalledCopyComparison.holds` into the
``AlbumQualityEvidenceDecisionFacts.installed_track_lengths_disagree`` fact,
and ``lib.quality.pipeline.full_pipeline_decision_from_evidence`` (the one
import decision function) turns that fact into a hold. The comparison is
persisted on the audit row (``ValidationResult.upgrade_hold``) so every
surface renders from stored evidence, never a fresh measurement.

Tracks are keyed by release-track identity, never by position: Beets'
``item.track``/``item.disc`` depend on ``per_disc_numbering``, which the
deployment contract does not pin. The key spelling per source mirrors
``lib.beets_db.BeetsDB.list_library_completeness_albums``.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Sequence
from typing import Final, Literal

import msgspec

from lib.track_length_shape import SAME_AUDIO_TOLERANCE_SECONDS, format_duration

#: A track whose two copies differ by more than this many seconds trips the
#: guard (unless the release's own listing explains the new copy better).
#: Beets clamps its own track-length distance at 30s, which is exactly why
#: a wrong song of a similar-enough length was invisible to the match.
UPGRADE_GUARD_TRACK_LENGTH_SECONDS: Final[float] = 30.0

#: How many tripped tracks the plain-English sentence names before it
#: switches to a count.
_NAMED_TRACK_LIMIT: Final[int] = 3


class CandidateTrackLength(msgspec.Struct, frozen=True, kw_only=True):
    """One release track as the candidate's selected beets mapping placed it."""

    key: str
    medium: int | None
    medium_index: int | None
    title: str
    declared_seconds: float
    candidate_seconds: float
    component_count: int = 1


class InstalledTrackFile(msgspec.Struct, frozen=True, kw_only=True):
    """One installed Beets item: its release-track key and its file path."""

    key: str
    path: str


class InstalledTrackLength(msgspec.Struct, frozen=True, kw_only=True):
    """One installed item's length MEASURED from its audio file.

    Never Beets' ``items.length``: that column holds the release's declared
    length, not the file's (see ``BeetsDB.installed_track_files``).
    """

    key: str
    installed_seconds: float


class TrackLengthComparisonRow(msgspec.Struct, frozen=True, kw_only=True):
    """One release track as both copies have it. ``None`` = not known."""

    medium: int | None
    medium_index: int | None
    title: str
    declared_seconds: float | None
    installed_seconds: float | None
    candidate_seconds: float | None
    compared: bool
    tripped: bool


#: How the installed side's lengths were obtained. Always measured from the
#: installed audio files at decision time, never read from Beets' database
#: (whose ``items.length`` is the release's declared length).
INSTALLED_LENGTHS_MEASURED_FROM_FILES: Final = "measured_from_files"

#: What a hold compared the candidate against. ``installed_copy`` is the
#: issue #1445 guard (the new copy disagrees with the copy it would replace).
#: ``declared_program`` is issue #1471 gap 2: the operator marked the
#: installed copy incomplete, so the release's own listing is the yardstick
#: and the new copy is held when it is measurably short of it.
HOLD_BASIS_INSTALLED_COPY: Final = "installed_copy"
HOLD_BASIS_DECLARED_PROGRAM: Final = "declared_program"


class InstalledCopyComparison(msgspec.Struct, frozen=True, kw_only=True):
    """The stored per-track evidence behind an upgrade hold."""

    threshold_seconds: float
    rows: tuple[TrackLengthComparisonRow, ...]
    compared_count: int
    holds: bool
    installed_lengths_source: str = INSTALLED_LENGTHS_MEASURED_FROM_FILES
    # Defaulted so every hold stored before #1471 decodes as the guard it was.
    basis: str = HOLD_BASIS_INSTALLED_COPY


def _usable(seconds: float | None) -> float | None:
    if seconds is None or not math.isfinite(seconds) or seconds <= 0:
        return None
    return seconds


def _unique_keys(keys: Sequence[str]) -> frozenset[str]:
    counts = Counter(key for key in keys if key)
    return frozenset(key for key, count in counts.items() if count == 1)


def _trips(
    *, declared: float | None, installed: float, candidate: float,
) -> bool:
    if abs(candidate - installed) <= UPGRADE_GUARD_TRACK_LENGTH_SECONDS:
        return False
    # When the release itself explains the new copy strictly better than
    # the old one, the installed copy is the odd one out and replacing it
    # is exactly what an upgrade should do.
    return not (
        declared is not None
        and abs(candidate - declared) < abs(installed - declared)
    )


def compare_installed_copy(
    candidate_tracks: Sequence[CandidateTrackLength],
    installed_tracks: Sequence[InstalledTrackLength],
) -> InstalledCopyComparison:
    """Compare the candidate with the installed copy, track by track.

    A track is compared only when its key is non-blank and unique on BOTH
    sides, it is not a coalesced Discogs composite (whose file legitimately
    overshoots its listed program), and both copies have a positive, finite
    length. Every other track is shown but never trips the guard, so a world
    nothing can be keyed in fails open (``compared_count == 0``).
    """
    candidate_keys = _unique_keys([track.key for track in candidate_tracks])
    installed_keys = _unique_keys([track.key for track in installed_tracks])
    installed_by_key = {
        track.key: _usable(track.installed_seconds)
        for track in installed_tracks
        if track.key in installed_keys
    }
    rows: list[TrackLengthComparisonRow] = []
    for track in candidate_tracks:
        declared = _usable(track.declared_seconds)
        candidate = _usable(track.candidate_seconds)
        keyed = (
            track.key in candidate_keys and track.key in installed_by_key
        )
        installed = installed_by_key.get(track.key) if keyed else None
        compared = (
            keyed
            and track.component_count <= 1
            and installed is not None
            and candidate is not None
        )
        tripped = (
            compared
            and installed is not None
            and candidate is not None
            and _trips(
                declared=declared, installed=installed, candidate=candidate,
            )
        )
        rows.append(TrackLengthComparisonRow(
            medium=track.medium,
            medium_index=track.medium_index,
            title=track.title,
            declared_seconds=declared,
            installed_seconds=installed,
            candidate_seconds=candidate,
            compared=compared,
            tripped=tripped,
        ))
    rows.sort(key=lambda row: (row.medium or 0, row.medium_index or 0))
    return InstalledCopyComparison(
        threshold_seconds=UPGRADE_GUARD_TRACK_LENGTH_SECONDS,
        rows=tuple(rows),
        compared_count=sum(1 for row in rows if row.compared),
        holds=any(row.tripped for row in rows),
    )


def compare_with_declared_program(
    candidate_tracks: Sequence[CandidateTrackLength],
    installed_tracks: Sequence[InstalledTrackLength] | None,
) -> InstalledCopyComparison:
    """Is the candidate measurably short of the release's declared program?

    Issue #1471 gap 2. Used only when the operator has marked the installed
    copy incomplete: beets' ``strong_match`` proves every release track has
    a file, never that a file is as long as the release lists, so a second
    rip missing the same hidden track would otherwise replace the marked
    copy and clear the mark. A track trips when the candidate runs more than
    :data:`UPGRADE_GUARD_TRACK_LENGTH_SECONDS` SHORT of its declared length;
    a longer file (an unlisted hidden track kept in it) never trips.

    A track is compared when its declared and candidate lengths are both
    positive and finite and it is not a coalesced Discogs composite. The
    installed copy's measured lengths are shown when keyed, for the
    operator, and never decide anything.
    """
    installed = tuple(installed_tracks or ())
    installed_keys = _unique_keys([track.key for track in installed])
    candidate_keys = _unique_keys([track.key for track in candidate_tracks])
    installed_by_key = {
        track.key: _usable(track.installed_seconds)
        for track in installed
        if track.key in installed_keys
    }
    rows: list[TrackLengthComparisonRow] = []
    for track in candidate_tracks:
        declared = _usable(track.declared_seconds)
        candidate = _usable(track.candidate_seconds)
        installed_seconds = (
            installed_by_key.get(track.key)
            if track.key in candidate_keys else None
        )
        compared = (
            track.component_count <= 1
            and declared is not None
            and candidate is not None
        )
        tripped = (
            compared
            and declared is not None
            and candidate is not None
            and declared - candidate > UPGRADE_GUARD_TRACK_LENGTH_SECONDS
        )
        rows.append(TrackLengthComparisonRow(
            medium=track.medium,
            medium_index=track.medium_index,
            title=track.title,
            declared_seconds=declared,
            installed_seconds=installed_seconds,
            candidate_seconds=candidate,
            compared=compared,
            tripped=tripped,
        ))
    rows.sort(key=lambda row: (row.medium or 0, row.medium_index or 0))
    return InstalledCopyComparison(
        threshold_seconds=UPGRADE_GUARD_TRACK_LENGTH_SECONDS,
        rows=tuple(rows),
        compared_count=sum(1 for row in rows if row.compared),
        holds=any(row.tripped for row in rows),
        basis=HOLD_BASIS_DECLARED_PROGRAM,
    )


# --- the operator's sentence ------------------------------------------------

_Variant = Literal["odd_one_out", "no_listing", "neither"]


def _variant(row: TrackLengthComparisonRow) -> _Variant:
    declared = row.declared_seconds
    installed = row.installed_seconds
    if declared is None:
        return "no_listing"
    if (
        installed is not None
        and abs(declared - installed) <= SAME_AUDIO_TOLERANCE_SECONDS
    ):
        return "odd_one_out"
    return "neither"


def _join(parts: Sequence[str]) -> str:
    if len(parts) == 1:
        return parts[0]
    return f"{', '.join(parts[:-1])} and {parts[-1]}"


def _ref(row: TrackLengthComparisonRow, *, multi_disc: bool) -> str:
    if row.medium_index is None:
        return f'"{row.title}"' if row.title else "one track"
    disc = (
        f" on disc {row.medium}"
        if multi_disc and row.medium is not None else ""
    )
    return f"track {row.medium_index}{disc}"


def _refs(
    rows: Sequence[TrackLengthComparisonRow], *, multi_disc: bool,
) -> str:
    """'track 2', 'tracks 2 and 5', or the per-row refs when unplaced."""
    if len(rows) > 1 and all(row.medium_index is not None for row in rows):
        media = {row.medium for row in rows}
        if len(media) == 1 or not multi_disc:
            medium = next(iter(media))
            disc = (
                f" on disc {medium}"
                if multi_disc and medium is not None else ""
            )
            indices = [str(row.medium_index) for row in rows]
            return f"tracks {_join(indices)}{disc}"
    return _join([_ref(row, multi_disc=multi_disc) for row in rows])


def _seconds(values: Sequence[float | None]) -> str:
    return _join([format_duration(value or 0.0) for value in values])


def _capitalize(text: str) -> str:
    return text[:1].upper() + text[1:]


def _odd_one_out(
    rows: Sequence[TrackLengthComparisonRow], *, multi_disc: bool,
) -> str:
    runs = _join([
        f"{_ref(row, multi_disc=multi_disc)} runs "
        f"{format_duration(row.candidate_seconds or 0.0)}"
        for row in rows
    ])
    agreed = _seconds([row.declared_seconds for row in rows])
    return (
        f"On the new copy {runs}. Your current copy and the release both "
        f"say {agreed}, so the new copy is the odd one out."
    )


def _no_listing(
    rows: Sequence[TrackLengthComparisonRow], *, multi_disc: bool,
) -> str:
    plural = len(rows) > 1
    subject = _capitalize(_refs(rows, multi_disc=multi_disc))
    pronoun = "them" if plural else "it"
    return (
        f"{subject} {'run' if plural else 'runs'} "
        f"{_seconds([row.candidate_seconds for row in rows])} on the new "
        f"copy but {_seconds([row.installed_seconds for row in rows])} on "
        "your current copy. The release lists no length for "
        f"{pronoun}, so it does not say which is right."
    )


def _neither(
    rows: Sequence[TrackLengthComparisonRow], *, multi_disc: bool,
) -> str:
    plural = len(rows) > 1
    subject = _capitalize(_refs(rows, multi_disc=multi_disc))
    return (
        f"{subject} {'run' if plural else 'runs'} "
        f"{_seconds([row.candidate_seconds for row in rows])} on the new "
        f"copy and {_seconds([row.installed_seconds for row in rows])} on "
        "your current copy, but the release says "
        f"{_seconds([row.declared_seconds for row in rows])}. Neither copy "
        "matches the release there."
    )


def _short_of_program(
    rows: Sequence[TrackLengthComparisonRow],
    *,
    remaining: int,
    multi_disc: bool,
) -> str:
    runs = _join([
        f"{_ref(row, multi_disc=multi_disc)} runs "
        f"{format_duration(row.candidate_seconds or 0.0)} where the release "
        f"says {format_duration(row.declared_seconds or 0.0)}"
        for row in rows
    ])
    sentences = [
        "Kept your current copy, which you marked incomplete.",
        f"The new copy is short too: {runs}.",
    ]
    if remaining:
        sentences.append(
            f"{remaining} more track{'s' if remaining > 1 else ''} "
            f"{'are' if remaining > 1 else 'is'} short too."
        )
    return " ".join(sentences)


def held_upgrade_sentence(comparison: InstalledCopyComparison) -> str:
    """Plain-English reason an upgrade was held, from the stored evidence.

    Never claims more than the lengths prove: when the release agrees with
    the current copy the new copy is called the odd one out; when the
    release lists no length the two copies simply disagree; when the
    release matches neither copy it says so. At most three tracks are
    named; the rest are counted.
    """
    tripped = [row for row in comparison.rows if row.tripped]
    if not tripped:
        return "Kept your current copy."
    multi_disc = len({
        row.medium for row in comparison.rows if row.medium is not None
    }) > 1
    named = tripped[:_NAMED_TRACK_LIMIT]
    if comparison.basis == HOLD_BASIS_DECLARED_PROGRAM:
        return _short_of_program(
            named, remaining=len(tripped) - len(named), multi_disc=multi_disc,
        )
    sentences = ["Kept your current copy."]
    for variant, render in (
        ("odd_one_out", _odd_one_out),
        ("neither", _neither),
        ("no_listing", _no_listing),
    ):
        group = [row for row in named if _variant(row) == variant]
        if group:
            sentences.append(render(group, multi_disc=multi_disc))
    remaining = len(tripped) - len(named)
    if remaining:
        sentences.append(
            f"{remaining} more track{'s' if remaining > 1 else ''} "
            f"{'differ' if remaining > 1 else 'differs'} too."
        )
    return " ".join(sentences)
