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
merging with each other (a duplicated flat position stays two rows); a
sub-position run merges to one row keyed by its literal base (so
unparseable bases like ``CD1``/``CD2`` stay distinct), titled by its
first non-placeholder sub-entry (blank counts as placeholder), durations
summed losslessly; a flat parent row sharing a sub run's base joins that
run instead of duplicating its position.

**P2 — flat tracklists are untouched.** The no-sub-position subset of
P1, kept as an explicitly named law: normalization must never reshape an
ordinary release.

Known limit (deliberate): the placeholder-title alphabet here is the
exact marker list, so these worlds cannot distinguish production's
anchored ``SILENCE_TITLE_RE`` from a substring check — the deterministic
pin ``test_title_containing_silence_is_not_a_placeholder`` owns that
axis.
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
    disc: int, track: int, members: list[Entry],
) -> tuple[int, int, str, float | None]:
    titles = [entry.title for entry in members]
    real = [t for t in titles if not _is_placeholder(t)]
    title = real[0] if real else titles[0]
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
    parse to the ``(1, 0)`` sentinel.
    """
    count = draw(st.integers(min_value=0, max_value=6))
    wire: list[tuple[str, Entry]] = []
    expected: list[tuple[int, int, str, float | None]] = []
    for index in range(count):
        number = index + 1
        kind = draw(st.sampled_from(("flat", "flat_dup", "sub", "sub_parent")))
        if kind in ("flat", "flat_dup"):
            position, disc, track = draw(st.sampled_from((
                (str(number), 1, number),
                (f"1-{number}", 1, number),
                (f"A{number}", 1, number),
                (chr(ord("A") + index), index + 1, 1),
                ("", 1, 0),
            )))
            entry = draw(_entries())
            wire.append((position, entry))
            expected.append((disc, track, entry.title, entry.duration_seconds))
            if kind == "flat_dup":
                dup = draw(_entries())
                wire.append((position, dup))
                expected.append((disc, track, dup.title, dup.duration_seconds))
        else:
            base, disc, track = draw(st.sampled_from((
                (str(number), 1, number),
                (f"CD{number}", 1, 0),
            )))
            members: list[Entry] = []
            if kind == "sub_parent":
                parent = draw(_entries())
                wire.append((base, parent))
                members.append(parent)
            sub_count = draw(st.integers(min_value=1, max_value=4))
            for sub_index in range(1, sub_count + 1):
                entry = draw(_entries())
                wire.append((f"{base}.{sub_index}", entry))
                members.append(entry)
            expected.append(_merged_expectation(disc, track, members))
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


class TestNormalizeReleaseTracksProperties(unittest.TestCase):
    @given(worlds())
    @example(_HIDDEN_TRACK_PIN)
    @example(_UNPARSEABLE_BASES_PIN)
    @example(_FLAT_PARENT_PIN)
    @example(_FLAT_DUP_PIN)
    def test_normalizer_emits_exactly_the_spec_rows(self, world: World):
        self.assertEqual(
            _to_rows(world), _spec_rows(world),
            "P1: normalized rows must equal the draw-time spec rows",
        )

    @given(worlds().filter(
        lambda w: all("." not in position for position, _ in w.wire)
    ))
    @example(_FLAT_DUP_PIN)
    def test_flat_tracklists_are_untouched(self, world: World):
        rows = _to_rows(world)
        self.assertEqual(
            rows, _spec_rows(world),
            "P2: a tracklist with no sub-positions passes through verbatim",
        )
        self.assertEqual(
            len(rows), len(world.wire),
            "P2: flat rows are never merged with each other",
        )


if __name__ == "__main__":
    unittest.main()
