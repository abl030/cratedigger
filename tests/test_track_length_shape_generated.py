"""Generated property for the track-length shape classifier (issue #1445).

Drives the real ``lib.track_length_shape.classify_track_length_shape``.

Worlds are PLANTED, never recomputed as an oracle (the F9 lesson recorded
in ``tests/test_track_length_warning_generated.py``): a clean album is
built whose files match their listings within 0.9s, then exactly one
disturbance is planted and the world remembers its role. The checker reads
the expected shape from that role, never from re-running the rules on the
floats.

Every listed length on the main disc is a distinct multiple of 75s above
100s, so a planted shift can only collide with another track's listing by
coincidence, and each such coincidence is discarded with ``assume``.
Unusable decoys (no listed length, no measured length, or a Discogs
composite) sit on a separate disc with huge raw offsets: they must never
change the shape.

Invariant: the named shape equals the planted role's shape, no shape is
named on a clean album, and the shape's tracks include every planted one.
"""

from __future__ import annotations

import unittest
from typing import NamedTuple

from hypothesis import assume, example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.track_length_shape import (
    LengthPair,
    TrackLengthShape,
    TrackLengthShapeKind,
    classify_track_length_shape,
)

ROLE_NONE = "none"
ROLE_ROTATE = "rotate"
ROLE_SPLIT = "split"
ROLE_ONLY_TRACK_ON_DISC = "only_track_on_disc"
ROLE_LAST_LONGER = "last_longer"
ROLE_LAST_SHORTER = "last_shorter"
ROLE_MIDDLE_LONGER = "middle_longer"
ROLE_MIDDLE_SHORTER = "middle_shorter"
ROLE_SEVERAL = "several"

EXPECTED_KIND: dict[str, TrackLengthShapeKind] = {
    ROLE_ROTATE: TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS,
    ROLE_SPLIT: TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY,
    ROLE_ONLY_TRACK_ON_DISC: TrackLengthShapeKind.SINGLE_TRACK_DISC,
    ROLE_LAST_LONGER: TrackLengthShapeKind.HIDDEN_TRACK_IN_FILE,
    ROLE_LAST_SHORTER: TrackLengthShapeKind.END_OF_LAST_TRACK_MISSING,
    ROLE_MIDDLE_LONGER: TrackLengthShapeKind.ONE_TRACK_LONGER,
    ROLE_MIDDLE_SHORTER: TrackLengthShapeKind.ONE_TRACK_SHORTER,
    ROLE_SEVERAL: TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER,
}

_MAIN_DISC = 1
_SINGLE_DISC = 2
_DECOY_DISC = 9


class PlantedWorld(NamedTuple):
    role: str
    pairs: list[LengthPair]
    #: (medium, medium_index) of every planted disturbed track.
    planted: list[tuple[int, int]]


def _pair(
    medium: int, index: int, count: int, file_seconds: float,
    listed_seconds: float, *, component_count: int = 1,
) -> LengthPair:
    return LengthPair(
        medium=medium, medium_index=index, medium_track_count=count,
        file_seconds=file_seconds, listed_seconds=listed_seconds,
        component_count=component_count, title=f"Song {medium}-{index}",
        file_name=f"{medium}-{index:02d}.flac",
    )


_offset = st.floats(
    min_value=62.0, max_value=300.0, allow_nan=False, allow_infinity=False)
_noise = st.floats(
    min_value=-0.9, max_value=0.9, allow_nan=False, allow_infinity=False)


@st.composite
def _decoys(draw: st.DrawFn) -> list[LengthPair]:
    """Unusable pairs with huge raw offsets on their own disc."""
    count = draw(st.integers(min_value=0, max_value=3))
    decoys: list[LengthPair] = []
    for index in range(1, count + 1):
        kind = draw(st.sampled_from(["no_listed", "no_file", "composite"]))
        huge = draw(st.floats(min_value=500.0, max_value=3000.0))
        if kind == "no_listed":
            decoys.append(_pair(_DECOY_DISC, index, count, huge, 0.0))
        elif kind == "no_file":
            decoys.append(_pair(_DECOY_DISC, index, count, 0.0, huge))
        else:
            decoys.append(_pair(
                _DECOY_DISC, index, count, huge, 100.0,
                component_count=draw(st.integers(min_value=2, max_value=4)),
            ))
    return decoys


@st.composite
def planted_worlds(draw: st.DrawFn) -> PlantedWorld:
    n = draw(st.integers(min_value=4, max_value=10))
    steps = draw(st.lists(
        st.integers(min_value=0, max_value=30),
        min_size=n, max_size=n, unique=True,
    ))
    listed = [100.0 + 75.0 * step for step in steps]
    files = [length + draw(_noise) for length in listed]
    role = draw(st.sampled_from([ROLE_NONE, *EXPECTED_KIND]))
    planted: list[int] = []
    extra: list[LengthPair] = []

    if role == ROLE_ROTATE:
        k = draw(st.integers(min_value=2, max_value=min(n, 4)))
        chosen = draw(st.permutations(range(n)))[:k]
        shift = draw(st.integers(min_value=1, max_value=k - 1))
        for position, source in zip(
            chosen, chosen[shift:] + chosen[:shift], strict=True,
        ):
            files[position] = listed[source] + draw(_noise)
        planted = list(chosen)
    elif role == ROLE_SPLIT:
        first = draw(st.integers(min_value=0, max_value=n - 2))
        shift = draw(_offset) * draw(st.sampled_from([-1.0, 1.0]))
        files[first] -= shift
        files[first + 1] += shift
        assume(min(files[first], files[first + 1]) > 5.0)
        # A shift that lands each file on the other's listing is a swap.
        assume(abs(files[first] - listed[first + 1]) > 2.0)
        planted = [first, first + 1]
    elif role == ROLE_ONLY_TRACK_ON_DISC:
        length = draw(st.floats(min_value=100.0, max_value=3000.0))
        offset = draw(_offset) * draw(st.sampled_from([-1.0, 1.0]))
        assume(length + offset > 5.0)
        extra.append(_pair(_SINGLE_DISC, 1, 1, length + offset, length))
    elif role in (ROLE_LAST_LONGER, ROLE_LAST_SHORTER,
                  ROLE_MIDDLE_LONGER, ROLE_MIDDLE_SHORTER):
        position = (
            n - 1 if role in (ROLE_LAST_LONGER, ROLE_LAST_SHORTER)
            else draw(st.integers(min_value=0, max_value=n - 2))
        )
        offset = draw(_offset)
        if role in (ROLE_LAST_SHORTER, ROLE_MIDDLE_SHORTER):
            offset = -offset
        files[position] += offset
        assume(files[position] > 5.0)
        planted = [position]
    elif role == ROLE_SEVERAL:
        first = draw(st.integers(min_value=0, max_value=n - 3))
        second = draw(st.integers(min_value=first + 2, max_value=n - 1))
        for position in (first, second):
            files[position] += (
                draw(_offset) * draw(st.sampled_from([-1.0, 1.0])))
            assume(files[position] > 5.0)
        # Each file landing on the other's listing would be a swap.
        assume(
            abs(files[first] - listed[second]) > 2.0
            or abs(files[second] - listed[first]) > 2.0
        )
        planted = [first, second]

    pairs = [
        _pair(_MAIN_DISC, position + 1, n, files[position], listed[position])
        for position in range(n)
    ]
    pairs.extend(extra)
    pairs.extend(draw(_decoys()))
    pairs = draw(st.permutations(pairs))
    planted_keys = [(_MAIN_DISC, position + 1) for position in planted]
    if role == ROLE_ONLY_TRACK_ON_DISC:
        planted_keys = [(_SINGLE_DISC, 1)]
    return PlantedWorld(role, list(pairs), planted_keys)


def assert_shape_matches_planted(
    world: PlantedWorld, shape: TrackLengthShape | None,
) -> None:
    if world.role == ROLE_NONE:
        if shape is not None:
            raise AssertionError(
                f"no disagreement was planted but {shape.kind} was named")
        return
    if shape is None:
        raise AssertionError(
            f"planted {world.role} but no shape was named")
    if shape.kind != EXPECTED_KIND[world.role]:
        raise AssertionError(
            f"planted {world.role} but {shape.kind} was named")
    named = {(slot.medium, slot.medium_index) for slot in shape.slots}
    missing = [key for key in world.planted if key not in named]
    if missing:
        raise AssertionError(
            f"planted {world.role} but the shape omits tracks {missing}")


_KURT_VILE = PlantedWorld(
    ROLE_ROTATE,
    [
        _pair(1, index, 6, file_seconds, listed_seconds)
        for index, (file_seconds, listed_seconds) in enumerate([
            (338.1, 338.1), (336.3, 289.9), (228.6, 336.3),
            (393.1, 393.1), (289.9, 228.6), (273.1, 273.1),
        ], 1)
    ],
    [(1, 2), (1, 3), (1, 5)],
)


class TestTrackLengthShapeGenerated(unittest.TestCase):
    @given(world=planted_worlds())
    @example(world=_KURT_VILE)
    @example(world=PlantedWorld(ROLE_NONE, [], []))
    def test_shape_matches_planted_role(self, world: PlantedWorld) -> None:
        assert_shape_matches_planted(
            world, classify_track_length_shape(world.pairs))


class TestCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause."""

    def test_clause_shape_on_clean_album_trips(self) -> None:
        shape = classify_track_length_shape(_KURT_VILE.pairs)
        with self.assertRaisesRegex(
            AssertionError, "no disagreement was planted but",
        ):
            assert_shape_matches_planted(
                PlantedWorld(ROLE_NONE, [], []), shape)

    def test_clause_missing_shape_trips(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, "planted rotate but no shape was named",
        ):
            assert_shape_matches_planted(_KURT_VILE, None)

    def test_clause_wrong_kind_trips(self) -> None:
        shape = classify_track_length_shape(_KURT_VILE.pairs)
        with self.assertRaisesRegex(
            AssertionError, "planted split but tracks_in_wrong_slots",
        ):
            assert_shape_matches_planted(
                PlantedWorld(ROLE_SPLIT, _KURT_VILE.pairs, []), shape)

    def test_clause_omitted_track_trips(self) -> None:
        shape = classify_track_length_shape(_KURT_VILE.pairs)
        with self.assertRaisesRegex(
            AssertionError, r"omits tracks \[\(1, 6\)\]",
        ):
            assert_shape_matches_planted(
                PlantedWorld(
                    ROLE_ROTATE, _KURT_VILE.pairs, [(1, 2), (1, 6)]),
                shape,
            )

    def test_the_real_rotation_passes(self) -> None:
        assert_shape_matches_planted(
            _KURT_VILE, classify_track_length_shape(_KURT_VILE.pairs))


if __name__ == "__main__":
    unittest.main()
