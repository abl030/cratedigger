"""Name the shape of a track-length disagreement (issue #1445 item 1).

Pure: typed pairs in, a typed shape out, no I/O. An imported album whose
file lengths contradict its matched release used to get one generic
warning naming the worst pair. This module says what KIND of
disagreement it is (a hidden track kept in the last file, tracks cut at a
different point, titles on the wrong songs, ...) in plain words the
operator can read at a glance.

Render-time labelling only. Nothing here gates, rejects, routes or writes
anything; see the comment on :data:`TRACK_LENGTH_WARNING_BOUND_SECONDS`
for why a reject at this bound was tried and reverted.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum
from typing import Final

import msgspec

#: Render-time warning threshold — NOT a pipeline gate. Issue #1178's first
#: attempt used this exact magnitude as a validation-time REJECT, and
#: independent review found the design could not separate the defect from
#: the legitimate hidden-track/appended-medley population: of the historical
#: cohort whose worst per-track deviation exceeds 60s, 85% is a single
#: offending pair (usually the last track) on an otherwise-correct rip —
#: Damien Rice "Eskimo" deviates 653s, well ABOVE #1178's own 222.6s case,
#: and is a correct rip. Every reject also permanently denylisted
#: contributing peers, so a reject gate at this bound would have serially
#: banned peers offering CORRECT rips of exactly the long-tail records this
#: system exists to rescue.
#:
#: This constant instead gates a render-time WARNING on an ALREADY-IMPORTED
#: card: nothing is denylisted, no scenario is renamed, no folder moves —
#: the disagreement is labelled for the operator to judge (Replace / Bad
#: Rip / leave as-is), never decided by the pipeline. Because the
#: derivation runs at render time over already-persisted evidence, it is
#: retroactive: the full historical cohort becomes visible, not just future
#: imports.
TRACK_LENGTH_WARNING_BOUND_SECONDS: Final[float] = 60.0

#: Two lengths within this many seconds are treated as the same audio.
SAME_AUDIO_TOLERANCE_SECONDS: Final[float] = 2.0

#: A position joins the "might be in the wrong place" set above this.
#: Deliberately far below the warning bound: in a real rotation (Kurt Vile,
#: download_log 24179) one member is only 46s off its own listing.
WRONG_SLOT_MIN_OFFSET_SECONDS: Final[float] = 10.0


class TrackLengthShapeKind(StrEnum):
    TRACKS_IN_WRONG_SLOTS = "tracks_in_wrong_slots"
    TRACKS_SPLIT_DIFFERENTLY = "tracks_split_differently"
    SINGLE_TRACK_DISC = "single_track_disc"
    HIDDEN_TRACK_IN_FILE = "hidden_track_in_file"
    END_OF_LAST_TRACK_MISSING = "end_of_last_track_missing"
    ONE_TRACK_LONGER = "one_track_longer"
    ONE_TRACK_SHORTER = "one_track_shorter"
    SEVERAL_TRACKS_DIFFER = "several_tracks_differ"


#: Shapes whose lengths say the audio is all there, just arranged or
#: counted differently from the release.
_PROBABLY_HARMLESS: Final[frozenset[TrackLengthShapeKind]] = frozenset({
    TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY,
    TrackLengthShapeKind.SINGLE_TRACK_DISC,
    TrackLengthShapeKind.HIDDEN_TRACK_IN_FILE,
})

_LABELS: Final[dict[TrackLengthShapeKind, str]] = {
    TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS: "wrong order?",
    TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY: "split differently",
    TrackLengthShapeKind.SINGLE_TRACK_DISC: "one-track disc",
    TrackLengthShapeKind.HIDDEN_TRACK_IN_FILE: "hidden track?",
    TrackLengthShapeKind.END_OF_LAST_TRACK_MISSING: "ending missing?",
    TrackLengthShapeKind.ONE_TRACK_LONGER: "one track longer",
    TrackLengthShapeKind.ONE_TRACK_SHORTER: "one track shorter",
    TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER: "several differ",
}


class LengthPair(msgspec.Struct, frozen=True, kw_only=True):
    """One mapped (local file, release track) pair.

    ``medium``/``medium_index`` place the release track; either may be
    unknown. ``medium_track_count`` is how many tracks the release lists on
    that medium (not how many were mapped). A non-positive length means
    "not known" and makes the pair unusable, as does a coalesced Discogs
    composite (``component_count > 1``, #1183), whose file legitimately
    overshoots its listed program by any margin.
    """

    medium: int | None
    medium_index: int | None
    medium_track_count: int | None
    file_seconds: float
    listed_seconds: float
    title: str = ""
    file_name: str = ""
    component_count: int = 1


class TrackLengthSlot(msgspec.Struct, frozen=True, kw_only=True):
    """One release track the shape is about, with a ready-made line."""

    medium: int | None
    medium_index: int | None
    title: str
    file_name: str
    file_seconds: float
    listed_seconds: float
    summary: str


class TrackLengthShape(msgspec.Struct, frozen=True, kw_only=True):
    kind: TrackLengthShapeKind
    #: Short chip text.
    label: str
    #: The full plain-English explanation.
    sentence: str
    probably_harmless: bool
    slots: tuple[TrackLengthSlot, ...]


def format_duration(seconds: float) -> str:
    """``m:ss``, or ``h:mm:ss`` from an hour up, rounded to the second."""
    total = max(0, round(seconds))
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def _usable(pair: LengthPair) -> bool:
    return (
        pair.component_count <= 1
        and pair.file_seconds > 0
        and pair.listed_seconds > 0
    )


def _offset(pair: LengthPair) -> float:
    return abs(pair.file_seconds - pair.listed_seconds)


def _position_key(pair: LengthPair) -> tuple[int, int]:
    return (pair.medium or 0, pair.medium_index or 0)


def _is_last_on_multi_track_medium(pair: LengthPair) -> bool:
    count = pair.medium_track_count
    return (
        count is not None and count > 1 and pair.medium_index == count
    )


# --- wrong slots ------------------------------------------------------------

def _is_rotation(moved: Sequence[LengthPair]) -> bool:
    """Every file in ``moved`` matches a DIFFERENT member's listed length
    within tolerance, one-to-one (bipartite perfect matching)."""
    if len(moved) < 2:
        return False
    fits = [
        [
            target for target, other in enumerate(moved)
            if target != source
            and abs(pair.file_seconds - other.listed_seconds)
            <= SAME_AUDIO_TOLERANCE_SECONDS
        ]
        for source, pair in enumerate(moved)
    ]
    owner: dict[int, int] = {}

    def assign(source: int, seen: set[int]) -> bool:
        for target in fits[source]:
            if target in seen:
                continue
            seen.add(target)
            if target not in owner or assign(owner[target], seen):
                owner[target] = source
                return True
        return False

    return all(assign(source, set()) for source in range(len(moved)))


# --- split differently ------------------------------------------------------

def _sums_match(group: Sequence[LengthPair]) -> bool:
    file_total = sum(pair.file_seconds for pair in group)
    listed_total = sum(pair.listed_seconds for pair in group)
    return (
        abs(file_total - listed_total)
        <= SAME_AUDIO_TOLERANCE_SECONDS * len(group)
    )


def _split_groups(
    usable: Sequence[LengthPair], deviating: Sequence[LengthPair],
) -> list[list[LengthPair]] | None:
    """Groups of adjacent tracks that explain every deviating track by a
    moved cut point, or ``None`` when some deviating track is unexplained.

    A group is a maximal run of adjacent deviating tracks on one medium
    whose totals agree. A lone deviating track may instead borrow ONE
    adjacent in-bound neighbour that absorbs the difference (Chumbawamba,
    download_log 35012).
    """
    if any(pair.medium_index is None for pair in deviating):
        return None
    by_position = {
        _position_key(pair): pair
        for pair in usable if pair.medium_index is not None
    }
    runs: list[list[LengthPair]] = []
    for pair in sorted(deviating, key=_position_key):
        medium, index = _position_key(pair)
        if runs and _position_key(runs[-1][-1]) == (medium, index - 1):
            runs[-1].append(pair)
        else:
            runs.append([pair])
    deviating_ids = {id(pair) for pair in deviating}
    groups: list[list[LengthPair]] = []
    for run in runs:
        if len(run) >= 2:
            if not _sums_match(run):
                return None
            groups.append(run)
            continue
        lone = run[0]
        medium, index = _position_key(lone)
        explained: list[LengthPair] | None = None
        for neighbour_index in (index - 1, index + 1):
            neighbour = by_position.get((medium, neighbour_index))
            if neighbour is None or id(neighbour) in deviating_ids:
                continue
            group = sorted([lone, neighbour], key=_position_key)
            if _sums_match(group):
                explained = group
                break
        if explained is None:
            return None
        groups.append(explained)
    return groups


# --- copy -------------------------------------------------------------------

def _join(parts: Sequence[str]) -> str:
    if len(parts) == 1:
        return parts[0]
    return f"{', '.join(parts[:-1])} and {parts[-1]}"


def _capitalize(text: str) -> str:
    return text[:1].upper() + text[1:]


def _ref(pair: LengthPair, *, multi_disc: bool) -> str:
    """'track 5', 'track 5 on disc 2', or the quoted title."""
    if pair.medium_index is None:
        return f'"{pair.title}"' if pair.title else "one track"
    disc = (
        f" on disc {pair.medium}"
        if multi_disc and pair.medium is not None else ""
    )
    return f"track {pair.medium_index}{disc}"


def _refs(pairs: Sequence[LengthPair], *, multi_disc: bool) -> str:
    """'tracks 2, 3 and 5' (grouped per disc), else individual refs."""
    placed = [pair for pair in pairs if pair.medium_index is not None]
    unplaced = [pair for pair in pairs if pair.medium_index is None]
    parts: list[str] = []
    media: list[int | None] = []
    for pair in placed:
        if pair.medium not in media:
            media.append(pair.medium)
    for medium in media:
        indices = [
            str(pair.medium_index) for pair in placed if pair.medium == medium
        ]
        noun = "tracks" if len(indices) > 1 else "track"
        disc = (
            f" on disc {medium}" if multi_disc and medium is not None else ""
        )
        parts.append(f"{noun} {_join(indices)}{disc}")
    parts.extend(_ref(pair, multi_disc=multi_disc) for pair in unplaced)
    return _join(parts)


def _slot(pair: LengthPair, *, multi_disc: bool) -> TrackLengthSlot:
    title = f' "{pair.title}"' if pair.title and pair.medium_index is not None else ""
    return TrackLengthSlot(
        medium=pair.medium,
        medium_index=pair.medium_index,
        title=pair.title,
        file_name=pair.file_name,
        file_seconds=pair.file_seconds,
        listed_seconds=pair.listed_seconds,
        summary=(
            f"{_capitalize(_ref(pair, multi_disc=multi_disc))}{title} runs "
            f"{format_duration(pair.file_seconds)}, release lists "
            f"{format_duration(pair.listed_seconds)}"
        ),
    )


def _runs_against(pair: LengthPair, *, subject: str) -> str:
    return (
        f"{subject} runs {format_duration(pair.file_seconds)} but the "
        f"release lists {format_duration(pair.listed_seconds)}."
    )


def _shape(
    kind: TrackLengthShapeKind,
    sentence: str,
    slots: Sequence[LengthPair],
    *,
    multi_disc: bool,
) -> TrackLengthShape:
    return TrackLengthShape(
        kind=kind,
        label=_LABELS[kind],
        sentence=sentence,
        probably_harmless=kind in _PROBABLY_HARMLESS,
        slots=tuple(
            _slot(pair, multi_disc=multi_disc)
            for pair in sorted(slots, key=_position_key)
        ),
    )


def _single_deviation(
    pair: LengthPair, *, multi_disc: bool,
) -> TrackLengthShape:
    longer = pair.file_seconds > pair.listed_seconds
    if pair.medium_track_count == 1:
        disc = f"Disc {pair.medium}" if pair.medium is not None else "This disc"
        return _shape(
            TrackLengthShapeKind.SINGLE_TRACK_DISC,
            _runs_against(pair, subject=f"{disc} is a single track that")
            + " Usually a bonus or video disc in a different cut.",
            [pair], multi_disc=multi_disc,
        )
    if _is_last_on_multi_track_medium(pair):
        disc = (
            f" on disc {pair.medium}"
            if multi_disc and pair.medium is not None else ""
        )
        subject = f"Last track{disc}"
        if longer:
            return _shape(
                TrackLengthShapeKind.HIDDEN_TRACK_IN_FILE,
                _runs_against(pair, subject=subject)
                + " Usually a hidden track kept in the same file."
                " Probably fine.",
                [pair], multi_disc=multi_disc,
            )
        return _shape(
            TrackLengthShapeKind.END_OF_LAST_TRACK_MISSING,
            _runs_against(pair, subject=subject)
            + " The release usually counts a hidden track here that this"
            " copy does not have.",
            [pair], multi_disc=multi_disc,
        )
    subject = _capitalize(_ref(pair, multi_disc=multi_disc))
    if longer:
        return _shape(
            TrackLengthShapeKind.ONE_TRACK_LONGER,
            _runs_against(pair, subject=subject)
            + " Could be a different version, or extra audio in the same"
            " file.",
            [pair], multi_disc=multi_disc,
        )
    return _shape(
        TrackLengthShapeKind.ONE_TRACK_SHORTER,
        _runs_against(pair, subject=subject)
        + " Could be an edit, a different version, or a file that stops"
        " early.",
        [pair], multi_disc=multi_disc,
    )


def classify_track_length_shape(
    pairs: Sequence[LengthPair],
) -> TrackLengthShape | None:
    """The named shape of this mapping's length disagreement, or ``None``
    when no usable pair is off by more than
    :data:`TRACK_LENGTH_WARNING_BOUND_SECONDS`.

    Rules, first match wins: tracks in wrong slots (the off tracks' files
    are a one-to-one reshuffle of their listed lengths), tracks split
    differently (adjacent tracks add up to the listed total), one
    deviating track (by where it sits and which way it is off), else
    several tracks differ.
    """
    usable = [pair for pair in pairs if _usable(pair)]
    deviating = [
        pair for pair in usable
        if _offset(pair) > TRACK_LENGTH_WARNING_BOUND_SECONDS
    ]
    if not deviating:
        return None
    multi_disc = len({pair.medium for pair in pairs if pair.medium is not None}) > 1

    moved = [
        pair for pair in usable
        if _offset(pair) > WRONG_SLOT_MIN_OFFSET_SECONDS
    ]
    if _is_rotation(moved):
        refs = _refs(sorted(moved, key=_position_key), multi_disc=multi_disc)
        return _shape(
            TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS,
            f"{_capitalize(refs)} have each other's lengths. The titles are"
            " probably on the wrong songs.",
            moved, multi_disc=multi_disc,
        )

    groups = _split_groups(usable, deviating)
    if groups is not None:
        phrases = [_refs(group, multi_disc=multi_disc) for group in groups]
        if len(groups) == 1:
            sentence = (
                f"{_capitalize(phrases[0])} are cut at a different point but"
                " add up to the same total. Nothing is missing."
            )
        else:
            sentence = (
                f"{_capitalize(', and '.join(phrases))}, are cut at"
                " different points, but each group adds up to the same"
                " total. Nothing is missing."
            )
        return _shape(
            TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY, sentence,
            [pair for group in groups for pair in group],
            multi_disc=multi_disc,
        )

    if len(deviating) == 1:
        return _single_deviation(deviating[0], multi_disc=multi_disc)

    worst = max(deviating, key=_offset)
    return _shape(
        TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER,
        f"{len(deviating)} of {len(pairs)} tracks are a different length"
        f" from the release ({_ref(worst, multi_disc=multi_disc)} runs"
        f" {format_duration(worst.file_seconds)}, listed"
        f" {format_duration(worst.listed_seconds)}). Could be a different"
        " edition or the wrong audio.",
        deviating, multi_disc=multi_disc,
    )
