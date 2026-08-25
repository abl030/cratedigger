"""Generated properties for Discogs tracklist normalization (issue #1261).

Deterministic pins live in ``tests/test_discogs_api.py``
(``TestGetReleaseSubPositionTracks``); these properties patrol the world
space around them through the REAL ``_normalize_release_tracks`` — the
function ``get_release()`` runs on every mirror payload before the
manifest is persisted into ``album_tracks``.

Invariants, each paired with its pin per ``.claude/rules/code-quality.md``:

**P1 — one manifest row per physical position.** However Discogs splits a
position into ``N.M`` sub-entries, the normalized manifest has exactly one
row per top-level position group, in first-appearance order. This is the
matcher count gate's precondition: a rip has one file per position.

**P2 — flat tracklists are untouched.** A tracklist with no sub-position
notation maps entry-for-entry to the legacy shape (same titles, numbers,
durations). Normalization must never reshape ordinary releases.

**P3 — a merged row's title is the first non-placeholder sub-title.**
``(silence)``-style placeholders never become the searched/matched track
title while a real title exists in the group; an all-placeholder group
falls back to its first title verbatim.

**P4 — durations sum lossy-safely.** A merged row's length is the sum of
the group's known durations, or None when none are known — never a
partial silent drop of a known duration.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from web.discogs import (
    _DiscogsTrackJSON,
    _normalize_release_tracks,
    _parse_duration,
)


@dataclass(frozen=True)
class SubEntry:
    title: str
    duration: str


@dataclass(frozen=True)
class TrackGroup:
    """One physical position: flat (single entry) or a sub-position run."""

    base: str
    entries: tuple[SubEntry, ...]
    is_sub: bool


_SILENCE_TITLES = ("(silence)", "[silence]", "silence", "(Silence)", " (silence) ")


def _is_placeholder(title: str) -> bool:
    return "silence" in title.lower()


_real_titles = st.text(min_size=1, max_size=24).filter(
    lambda t: not _is_placeholder(t)
)

_durations = st.one_of(
    st.just(""),
    st.builds(
        lambda m, s: f"{m}:{s:02d}",
        st.integers(min_value=0, max_value=59),
        st.integers(min_value=0, max_value=59),
    ),
)

_titles = st.one_of(_real_titles, st.sampled_from(_SILENCE_TITLES))


@st.composite
def track_groups(draw: st.DrawFn) -> list[TrackGroup]:
    """Worlds of flat tracks and sub-position runs with unique bases.

    Bases are distinct numeric positions by construction so each group's
    expected output row is unambiguous; flat groups additionally sample
    the vinyl (``A1``), disc-track (``1-3``), and bare-letter (``A``)
    grammars, which never merge.
    """
    count = draw(st.integers(min_value=0, max_value=8))
    groups: list[TrackGroup] = []
    for index in range(count):
        number = index + 1
        is_sub = draw(st.booleans())
        if is_sub:
            entries = tuple(
                SubEntry(title=draw(_titles), duration=draw(_durations))
                for _ in range(draw(st.integers(min_value=1, max_value=4)))
            )
            groups.append(
                TrackGroup(base=str(number), entries=entries, is_sub=True))
        else:
            base = draw(st.sampled_from((
                str(number),
                f"1-{number}",
                f"A{number}",
                chr(ord("A") + index),
            )))
            entries = (SubEntry(title=draw(_titles), duration=draw(_durations)),)
            groups.append(TrackGroup(base=base, entries=entries, is_sub=False))
    return groups


def _to_wire(groups: list[TrackGroup]) -> list[_DiscogsTrackJSON]:
    tracks: list[_DiscogsTrackJSON] = []
    for group in groups:
        for sub_index, entry in enumerate(group.entries, start=1):
            position = (
                f"{group.base}.{sub_index}" if group.is_sub else group.base
            )
            tracks.append(_DiscogsTrackJSON(
                position=position,
                title=entry.title,
                duration=entry.duration,
            ))
    return tracks


_HIDDEN_TRACK_PIN = [
    TrackGroup("9", (SubEntry("Stay Entertained", "3:46"),), False),
    TrackGroup("10", (
        SubEntry("Island Lost At Sea", "3:46"),
        SubEntry("(silence)", "0:20"),
        SubEntry("Untitled", "3:49"),
    ), True),
]

_LEADING_SILENCE_PIN = [
    TrackGroup("1", (
        SubEntry("(silence)", "0:10"),
        SubEntry("Hidden Song", "3:00"),
    ), True),
]


class TestNormalizeReleaseTracksProperties(unittest.TestCase):
    @given(track_groups())
    @example(_HIDDEN_TRACK_PIN)
    @example(_LEADING_SILENCE_PIN)
    def test_one_row_per_position_group(self, groups: list[TrackGroup]):
        rows = _normalize_release_tracks(_to_wire(groups))
        self.assertEqual(
            len(rows), len(groups),
            "P1: normalized manifest must have one row per position group",
        )

    @given(track_groups().filter(lambda gs: not any(g.is_sub for g in gs)))
    def test_flat_tracklists_are_untouched(self, groups: list[TrackGroup]):
        rows = _normalize_release_tracks(_to_wire(groups))
        self.assertEqual(len(rows), len(groups), "P2: flat count preserved")
        for group, row in zip(groups, rows, strict=True):
            self.assertEqual(
                row["title"], group.entries[0].title,
                "P2: flat titles pass through verbatim",
            )
            self.assertEqual(
                row["length_seconds"],
                _parse_duration(group.entries[0].duration),
                "P2: flat durations pass through verbatim",
            )

    @given(track_groups())
    @example(_HIDDEN_TRACK_PIN)
    @example(_LEADING_SILENCE_PIN)
    def test_merged_title_is_first_non_placeholder(
        self, groups: list[TrackGroup],
    ):
        rows = _normalize_release_tracks(_to_wire(groups))
        for group, row in zip(groups, rows, strict=True):
            titles = [entry.title for entry in group.entries]
            real = [t for t in titles if not _is_placeholder(t)]
            expected = real[0] if real else titles[0]
            self.assertEqual(
                row["title"], expected,
                "P3: merged title must be the first non-placeholder",
            )

    @given(track_groups())
    @example(_HIDDEN_TRACK_PIN)
    def test_merged_duration_sums_known(self, groups: list[TrackGroup]):
        rows = _normalize_release_tracks(_to_wire(groups))
        for group, row in zip(groups, rows, strict=True):
            known = [
                seconds
                for entry in group.entries
                if (seconds := _parse_duration(entry.duration)) is not None
            ]
            expected = sum(known) if known else None
            self.assertEqual(
                row["length_seconds"], expected,
                "P4: merged duration is the sum of known durations",
            )


if __name__ == "__main__":
    unittest.main()
