"""Generated properties for Discogs tracklist normalization (issue #1261).

Deterministic pins live in ``tests/test_discogs_api.py``
(``TestGetReleaseSubPositionTracks``, which drives the real
``get_release()`` adapter) and ``tests/test_album_source.py`` (the search
worker's fallback writer); these properties patrol the world space
around them through ``lib.discogs_positions.normalize_release_tracks`` —
the one canonical normalizer both production writers run before a
manifest is persisted into ``album_tracks``.

The oracle is STRUCTURAL: every world is drawn as a list of declared
groups (flat row, duplicated flat row, sub-position run, sub run with a
flat parent row), and the expected normalized rows — including
disc/track numbers and summed durations — are recorded AT DRAW TIME from
the form definitions, never by re-running production parsing. Production
must recover the declared structure from the position strings alone.

**P1 — the normalizer emits exactly the spec rows.** One row per
physical position in first-appearance order: flat rows pass through
verbatim (titles, numbers, durations — even placeholder titles), never
merging with each other (a duplicated flat position stays two rows —
with one declared carve-out: when the duplicated position is also a sub
run's base, both flat rows join that group, the first as its parent and
the second as an ordinary member); a
sub-position run merges to one row keyed by its literal base (so
unparseable bases like ``CD1``/``CD2`` stay distinct), titled by its
first non-placeholder sub-entry (blank counts as placeholder), durations
summed losslessly; a flat parent row sharing a sub run's base joins that
run instead of duplicating its position, with its title authoritative
and its duration, when present, replacing the children's sum;
empty-position-AND-empty-duration heading rows are dropped exactly when
some other row is positioned (an all-unpositioned tracklist keeps every
row, and an empty position with a duration survives); video-marker
positions drop as non-audio unless every non-heading row is
video-marked (a whole-release video pressing keeps all rows).

**P2 — flat tracklists are untouched.** The no-sub-position subset of
P1, kept as an explicitly named law: normalization must never reshape an
ordinary release.

Known limits (deliberate): the placeholder-title alphabet here is the
exact marker list, so these worlds cannot distinguish production's
anchored ``SILENCE_TITLE_RE`` from a substring check — the deterministic
pin ``test_title_containing_silence_is_not_a_placeholder`` owns that
axis; sub-parent rows are always emitted immediately before their
run, so parent-position independence is owned by the deterministic pin
``test_parent_after_subs_still_titles_the_group``; and video positions
are drawn from the marker literals only — regex anchoring
(``test_video_like_positions_are_not_markers``), video sub-positions
(``test_video_sub_positions_drop_before_grouping``), and the nested
``sub_tracks`` index-parent vote
(``test_index_parent_vote_keeps_video_rows_droppable``) are owned by
deterministic pins.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.discogs_positions import normalize_release_tracks


@dataclass(frozen=True)
class Entry:
    title: str
    duration_text: str
    duration_seconds: float | None


@dataclass(frozen=True)
class World:
    wire: tuple[tuple[str, Entry], ...]
    expected: tuple[tuple[int, int, str, float | None], ...]


@dataclass(frozen=True)
class _FlatSlot:
    position: str
    disc: int
    track: int
    entry: Entry
    is_video: bool = False


@dataclass(frozen=True)
class _MergedSlot:
    row: tuple[int, int, str, float | None]


_SILENCE_TITLES = ("(silence)", "[silence]", "silence", "(Silence)", " (silence) ")


def _is_placeholder(title: str) -> bool:
    return not title.strip() or "silence" in title.lower()


_real_titles = st.text(min_size=1, max_size=24).filter(
    lambda t: not _is_placeholder(t)
)

_titles = st.one_of(
    _real_titles,
    st.sampled_from(_SILENCE_TITLES),
    st.just(""),
    st.just("   "),
)


@st.composite
def _entries(draw: st.DrawFn) -> Entry:
    title = draw(_titles)
    if draw(st.booleans()):
        minutes = draw(st.integers(min_value=0, max_value=59))
        seconds = draw(st.integers(min_value=0, max_value=59))
        return Entry(title, f"{minutes}:{seconds:02d}", minutes * 60 + seconds)
    return Entry(title, "", None)


def _merged_expectation(
    disc: int, track: int, parent: Entry | None, members: list[Entry],
) -> tuple[int, int, str, float | None]:
    titles = ([parent.title] if parent is not None else []) + [
        entry.title for entry in members
    ]
    real = [t for t in titles if not _is_placeholder(t)]
    title = real[0] if real else titles[0]
    if parent is not None and parent.duration_seconds:
        return (disc, track, title, parent.duration_seconds)
    known = [
        entry.duration_seconds
        for entry in members
        if entry.duration_seconds is not None
    ]
    return (disc, track, title, sum(known) if known else None)


@st.composite
def worlds(draw: st.DrawFn) -> World:
    """Structured worlds with draw-time expectations.

    Flat position forms carry their expected ``(disc, track)`` as
    literals from ``parse_position``'s documented grammar; sub bases are
    unique per group by construction (index-derived), covering both
    parseable numeric bases and unparseable ``CD<n>`` bases that all
    parse to the ``(1, 0)`` sentinel. Empty-position flat rows with no
    duration are HEADINGS when any other row in the world is positioned
    — expected to be dropped — and real tracks when they carry a
    duration or the whole world is unpositioned.
    """
    count = draw(st.integers(min_value=0, max_value=6))
    slots: list[_FlatSlot | _MergedSlot] = []
    wire: list[tuple[str, Entry]] = []
    for index in range(count):
        number = index + 1
        kind = draw(st.sampled_from(
            ("flat", "flat_dup", "sub", "sub_parent", "video"),
        ))
        if kind == "video":
            position = draw(st.sampled_from(
                ("Video", f"Video {number}", f"Video{number}"),
            ))
            entry = draw(_entries())
            wire.append((position, entry))
            slots.append(_FlatSlot(position, 1, 0, entry, is_video=True))
            continue
        if kind in ("flat", "flat_dup"):
            position, disc, track = draw(st.sampled_from((
                (str(number), 1, number),
                (f"1-{number}", 1, number),
                (f"A{number}", 1, number),
                (f"{number}A", 1, number),
                (f"{number}B", 2, number),
                (f"{number}.", 1, number),
                (chr(ord("A") + index), index + 1, 1),
                ("", 1, 0),
            )))
            copies = 2 if kind == "flat_dup" else 1
            for _ in range(copies):
                entry = draw(_entries())
                wire.append((position, entry))
                slots.append(_FlatSlot(position, disc, track, entry))
        else:
            base, disc, track = draw(st.sampled_from((
                (str(number), 1, number),
                (f"CD{number}", 1, 0),
            )))
            members: list[Entry] = []
            parent: Entry | None = None
            if kind == "sub_parent":
                parent = draw(_entries())
                wire.append((base, parent))
            sub_count = draw(st.integers(min_value=1, max_value=4))
            for sub_index in range(1, sub_count + 1):
                entry = draw(_entries())
                wire.append((f"{base}.{sub_index}", entry))
                members.append(entry)
            slots.append(
                _MergedSlot(_merged_expectation(disc, track, parent, members)),
            )

    any_positioned = any(position for position, _ in wire)

    def _is_heading(slot: _FlatSlot) -> bool:
        return not slot.position and not slot.entry.duration_text

    non_heading_flats = [
        s for s in slots if isinstance(s, _FlatSlot) and not _is_heading(s)
    ]
    all_video = (
        bool(non_heading_flats)
        and not any(isinstance(s, _MergedSlot) for s in slots)
        and all(s.is_video for s in non_heading_flats)
    )

    expected: list[tuple[int, int, str, float | None]] = []
    for slot in slots:
        if isinstance(slot, _MergedSlot):
            expected.append(slot.row)
            continue
        if _is_heading(slot) and any_positioned:
            continue  # heading row — dropped
        if slot.is_video and not all_video:
            continue  # enhanced-CD bonus video row — dropped
        expected.append(
            (slot.disc, slot.track, slot.entry.title, slot.entry.duration_seconds),
        )
    return World(wire=tuple(wire), expected=tuple(expected))


def _to_rows(world: World) -> list[dict[str, object]]:
    return normalize_release_tracks([
        {"position": position, "title": entry.title, "duration": entry.duration_text}
        for position, entry in world.wire
    ])


def _spec_rows(world: World) -> list[dict[str, object]]:
    return [
        {
            "disc_number": disc,
            "track_number": track,
            "title": title,
            "length_seconds": length,
        }
        for disc, track, title, length in world.expected
    ]


_HIDDEN_TRACK_PIN = World(
    wire=(
        ("9", Entry("Stay Entertained", "3:46", 226)),
        ("10.1", Entry("Island Lost At Sea", "3:46", 226)),
        ("10.2", Entry("(silence)", "0:20", 20)),
        ("10.3", Entry("Untitled", "3:49", 229)),
    ),
    expected=(
        (1, 9, "Stay Entertained", 226),
        (1, 10, "Island Lost At Sea", 475),
    ),
)

_UNPARSEABLE_BASES_PIN = World(
    wire=(
        ("CD1.1", Entry("One A", "1:00", 60)),
        ("CD1.2", Entry("One B", "1:00", 60)),
        ("CD2.1", Entry("Two A", "1:00", 60)),
    ),
    expected=(
        (1, 0, "One A", 120),
        (1, 0, "Two A", 60),
    ),
)

_FLAT_PARENT_PIN = World(
    wire=(
        ("10", Entry("Medley", "", None)),
        ("10.1", Entry("Part One", "1:00", 60)),
        ("10.2", Entry("Part Two", "2:00", 120)),
    ),
    expected=((1, 10, "Medley", 180),),
)

_FLAT_DUP_PIN = World(
    wire=(
        ("1", Entry("Take One", "1:00", 60)),
        ("1", Entry("Take Two", "2:00", 120)),
    ),
    expected=(
        (1, 1, "Take One", 60),
        (1, 1, "Take Two", 120),
    ),
)

_PARENT_TOTAL_PIN = World(
    wire=(
        ("10", Entry("Suite", "8:00", 480)),
        ("10.1", Entry("Part One", "4:00", 240)),
        ("10.2", Entry("Part Two", "4:00", 240)),
    ),
    expected=((1, 10, "Suite", 480),),
)

_VIDEO_MIXED_PIN = World(
    wire=(
        ("1", Entry("Opener", "3:00", 180)),
        ("Video", Entry("Bonus Clip", "4:00", 240)),
    ),
    expected=((1, 1, "Opener", 180),),
)

_ALL_VIDEO_PIN = World(
    wire=(
        ("Video 1", Entry("Part One", "20:00", 1200)),
        ("Video 2", Entry("Part Two", "20:00", 1200)),
    ),
    expected=(
        (1, 0, "Part One", 1200),
        (1, 0, "Part Two", 1200),
    ),
)

_HEADING_PLUS_ALL_VIDEO_PIN = World(
    wire=(
        ("", Entry("Bonus Section", "", None)),
        ("Video", Entry("Clip", "4:00", 240)),
    ),
    expected=((1, 0, "Clip", 240),),
)

_HEADING_PIN = World(
    wire=(
        ("", Entry("Alpha", "", None)),
        ("A1", Entry("Everything in Its Right Place", "4:11", 251)),
        ("", Entry("Beta", "", None)),
        ("B1", Entry("The National Anthem", "5:50", 350)),
    ),
    expected=(
        (1, 1, "Everything in Its Right Place", 251),
        (2, 1, "The National Anthem", 350),
    ),
)

# Empty durations ON PURPOSE: these rows must reach the any_positioned
# clause of the heading predicate (a duration short-circuits it first).
_ALL_UNPOSITIONED_PIN = World(
    wire=(
        ("", Entry("First", "", None)),
        ("", Entry("Second", "", None)),
    ),
    expected=(
        (1, 0, "First", None),
        (1, 0, "Second", None),
    ),
)


class TestNormalizeReleaseTracksProperties(unittest.TestCase):
    @given(worlds())
    @example(_HIDDEN_TRACK_PIN)
    @example(_UNPARSEABLE_BASES_PIN)
    @example(_FLAT_PARENT_PIN)
    @example(_FLAT_DUP_PIN)
    @example(_HEADING_PIN)
    @example(_ALL_UNPOSITIONED_PIN)
    @example(_PARENT_TOTAL_PIN)
    @example(_VIDEO_MIXED_PIN)
    @example(_ALL_VIDEO_PIN)
    @example(_HEADING_PLUS_ALL_VIDEO_PIN)
    def test_normalizer_emits_exactly_the_spec_rows(self, world: World):
        self.assertEqual(
            _to_rows(world), _spec_rows(world),
            "P1: normalized rows must equal the draw-time spec rows",
        )

    @given(worlds().filter(
        lambda w: all("." not in position for position, _ in w.wire)
    ))
    @example(_FLAT_DUP_PIN)
    @example(_ALL_UNPOSITIONED_PIN)
    def test_flat_tracklists_are_untouched(self, world: World):
        rows = _to_rows(world)
        # Full equality against the spec is the merge guard too: the spec
        # holds one row per surviving flat entry by construction (only
        # heading rows are absent), so any flat-row merge changes both
        # the length and the content of `rows`.
        self.assertEqual(
            rows, _spec_rows(world),
            "P2: a tracklist with no sub-positions passes through verbatim",
        )


if __name__ == "__main__":
    unittest.main()
