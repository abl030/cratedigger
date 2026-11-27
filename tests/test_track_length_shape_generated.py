"""Generated property for the track-length shape classifier (issue #1445).

Drives the real ``lib.track_length_shape.classify_track_length_shape``.

Worlds are PLANTED, never recomputed as an oracle (the F9 lesson recorded
in ``tests/test_track_length_warning_generated.py``): a clean release of
one or two discs is built whose files match their listings within 0.9s,
then exactly one disturbance is planted and the world remembers its role.
The checker reads the expected shape from that role, never from re-running
the rules on the floats.

World dimensions:

- one or two discs, plus tracks with no known position;
- listed lengths either spread (distinct multiples of 75s, needed for a
  reshuffle to be recognisable) or near-equal (200-204s);
- unusable decoys (no listed length, no measured length, a non-finite
  length, or a Discogs composite) with huge raw offsets, either without a
  position or on their own disc;
- disturbances: a reshuffle, a moved cut point, two off tracks that both
  balance against ONE shared in-bound neighbour (it can absorb only one of
  them), a one-track disc, a one-track release, one off track at the end of
  a disc (optionally beside a compensating first track of the NEXT disc,
  which must not count as a neighbour), in the middle, or without a
  position, and two unrelated off tracks.

Planted tracks are exact (no noise) where the role depends on totals
landing within tolerance. A coincidental collision with another listing
is discarded with ``assume``.

Each role is its own property (see ``TestTrackLengthShapeGenerated``).

Invariant: the named shape equals the planted role's shape, no shape is
named on a clean release, the shape lists every planted track, and it
never names a decoy.
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
ROLE_SHARED_NEIGHBOUR = "shared_neighbour"
ROLE_ONLY_TRACK_ON_DISC_LONGER = "only_track_on_disc_longer"
ROLE_ONLY_TRACK_ON_DISC_SHORTER = "only_track_on_disc_shorter"
ROLE_ONLY_TRACK_OF_RELEASE_LONGER = "only_track_of_release_longer"
ROLE_ONLY_TRACK_OF_RELEASE_SHORTER = "only_track_of_release_shorter"
ROLE_LAST_LONGER = "last_longer"
ROLE_LAST_SHORTER = "last_shorter"
ROLE_MIDDLE_LONGER = "middle_longer"
ROLE_MIDDLE_SHORTER = "middle_shorter"
ROLE_UNPLACED_LONGER = "unplaced_longer"
ROLE_UNPLACED_SHORTER = "unplaced_shorter"
ROLE_SEVERAL = "several"

EXPECTED_KIND: dict[str, TrackLengthShapeKind] = {
    ROLE_ROTATE: TrackLengthShapeKind.TRACKS_IN_WRONG_SLOTS,
    ROLE_SPLIT: TrackLengthShapeKind.TRACKS_SPLIT_DIFFERENTLY,
    ROLE_SHARED_NEIGHBOUR: TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER,
    ROLE_ONLY_TRACK_ON_DISC_LONGER: TrackLengthShapeKind.SINGLE_TRACK_DISC,
    ROLE_ONLY_TRACK_ON_DISC_SHORTER: TrackLengthShapeKind.SINGLE_TRACK_DISC,
    ROLE_ONLY_TRACK_OF_RELEASE_LONGER: TrackLengthShapeKind.ONE_TRACK_LONGER,
    ROLE_ONLY_TRACK_OF_RELEASE_SHORTER:
        TrackLengthShapeKind.ONE_TRACK_SHORTER,
    ROLE_LAST_LONGER: TrackLengthShapeKind.HIDDEN_TRACK_IN_FILE,
    ROLE_LAST_SHORTER: TrackLengthShapeKind.END_OF_LAST_TRACK_MISSING,
    ROLE_MIDDLE_LONGER: TrackLengthShapeKind.ONE_TRACK_LONGER,
    ROLE_MIDDLE_SHORTER: TrackLengthShapeKind.ONE_TRACK_SHORTER,
    ROLE_UNPLACED_LONGER: TrackLengthShapeKind.ONE_TRACK_LONGER,
    ROLE_UNPLACED_SHORTER: TrackLengthShapeKind.ONE_TRACK_SHORTER,
    ROLE_SEVERAL: TrackLengthShapeKind.SEVERAL_TRACKS_DIFFER,
}

_LONGER_ROLES = frozenset({
    ROLE_ONLY_TRACK_ON_DISC_LONGER, ROLE_ONLY_TRACK_OF_RELEASE_LONGER,
    ROLE_LAST_LONGER, ROLE_MIDDLE_LONGER, ROLE_UNPLACED_LONGER,
})
_SINGLE_OFF_ROLES = frozenset({
    ROLE_ONLY_TRACK_ON_DISC_LONGER, ROLE_ONLY_TRACK_ON_DISC_SHORTER,
    ROLE_ONLY_TRACK_OF_RELEASE_LONGER, ROLE_ONLY_TRACK_OF_RELEASE_SHORTER,
    ROLE_LAST_LONGER, ROLE_LAST_SHORTER, ROLE_MIDDLE_LONGER,
    ROLE_MIDDLE_SHORTER, ROLE_UNPLACED_LONGER, ROLE_UNPLACED_SHORTER,
})

_DECOY_DISC = 9


class PlantedWorld(NamedTuple):
    role: str
    pairs: list[LengthPair]
    #: File names of every planted disturbed track.
    planted: list[str]
    #: File names of every unusable decoy.
    decoys: list[str]


def _pair(
    name: str, medium: int | None, index: int | None, count: int | None,
    file_seconds: float, listed_seconds: float, *, component_count: int = 1,
) -> LengthPair:
    return LengthPair(
        medium=medium, medium_index=index, medium_track_count=count,
        file_seconds=file_seconds, listed_seconds=listed_seconds,
        component_count=component_count, title=name, file_name=name,
    )


_offset = st.floats(
    min_value=62.0, max_value=150.0, allow_nan=False, allow_infinity=False)
_noise = st.floats(
    min_value=-0.9, max_value=0.9, allow_nan=False, allow_infinity=False)
#: Off-track magnitude and neighbour compensation chosen so the pair sums
#: land within 2s per track exactly (no noise on these tracks).
_tight_off = st.floats(
    min_value=61.5, max_value=62.0, allow_nan=False, allow_infinity=False)
_tight_comp = st.floats(
    min_value=58.5, max_value=59.0, allow_nan=False, allow_infinity=False)


@st.composite
def _decoys(
    draw: st.DrawFn, *, allow_own_disc: bool,
) -> list[LengthPair]:
    count = draw(st.integers(min_value=0, max_value=3))
    on_own_disc = allow_own_disc and draw(st.booleans())
    decoys: list[LengthPair] = []
    for index in range(1, count + 1):
        name = f"decoy-{index}"
        medium, position, total = (
            (_DECOY_DISC, index, count) if on_own_disc else (None, None, None))
        kind = draw(st.sampled_from(
            ["no_listed", "no_file", "non_finite", "composite"]))
        huge = draw(st.floats(min_value=500.0, max_value=3000.0))
        if kind == "no_listed":
            file_seconds, listed_seconds, components = huge, 0.0, 1
        elif kind == "no_file":
            file_seconds, listed_seconds, components = 0.0, huge, 1
        elif kind == "non_finite":
            bad = draw(st.sampled_from(
                [float("inf"), float("-inf"), float("nan")]))
            file_seconds, listed_seconds = (
                (bad, huge) if draw(st.booleans()) else (huge, bad))
            components = 1
        else:
            file_seconds, listed_seconds = huge, 100.0
            components = draw(st.integers(min_value=2, max_value=4))
        decoys.append(_pair(
            name, medium, position, total, file_seconds, listed_seconds,
            component_count=components,
        ))
    return decoys


@st.composite
def planted_worlds(draw: st.DrawFn, role: str) -> PlantedWorld:
    near_equal = role != ROLE_ROTATE and draw(st.booleans())

    if role in (ROLE_ONLY_TRACK_OF_RELEASE_LONGER,
                ROLE_ONLY_TRACK_OF_RELEASE_SHORTER):
        sizes = [1]
    elif role in (ROLE_ONLY_TRACK_ON_DISC_LONGER,
                  ROLE_ONLY_TRACK_ON_DISC_SHORTER):
        sizes = [draw(st.integers(min_value=2, max_value=6)), 1]
    else:
        sizes = [draw(st.integers(min_value=3, max_value=7))]
        if draw(st.booleans()):
            sizes.append(draw(st.integers(min_value=1, max_value=5)))
    unplaced_count = draw(st.integers(min_value=0, max_value=2))
    if role in (ROLE_UNPLACED_LONGER, ROLE_UNPLACED_SHORTER):
        unplaced_count += 1

    # keys: (disc, index) for placed tracks, (None, n) for unplaced ones.
    keys: list[tuple[int | None, int]] = []
    for disc, size in enumerate(sizes, 1):
        keys.extend((disc, index) for index in range(1, size + 1))
    keys.extend((None, n) for n in range(unplaced_count))
    if near_equal:
        listed = {
            key: 200.0 + draw(st.floats(min_value=0.0, max_value=4.0))
            for key in keys
        }
    else:
        steps = draw(st.lists(
            st.integers(min_value=0, max_value=40),
            min_size=len(keys), max_size=len(keys), unique=True,
        ))
        listed = {key: 200.0 + 75.0 * step for key, step in zip(keys, steps, strict=True)}
    files = {key: listed[key] + draw(_noise) for key in keys}
    placed = [key for key in keys if key[0] is not None]
    planted: list[tuple[int | None, int]] = []
    sign = 1.0 if role in _LONGER_ROLES else -1.0

    if role == ROLE_ROTATE:
        k = draw(st.integers(min_value=2, max_value=min(len(placed), 4)))
        chosen = list(draw(st.permutations(placed)))[:k]
        shift = draw(st.integers(min_value=1, max_value=k - 1))
        for target, source in zip(
            chosen, chosen[shift:] + chosen[:shift], strict=True,
        ):
            files[target] = listed[source] + draw(_noise)
        planted = chosen
    elif role == ROLE_SPLIT:
        disc = draw(st.sampled_from(
            [d for d, size in enumerate(sizes, 1) if size >= 2]))
        first = draw(st.integers(min_value=1, max_value=sizes[disc - 1] - 1))
        a, b = (disc, first), (disc, first + 1)
        shift = draw(_offset) * draw(st.sampled_from([-1.0, 1.0]))
        files[a] -= shift
        files[b] += shift
        # Each file landing on the other's listing would be a swap.
        assume(abs(files[a] - listed[b]) > 2.0)
        planted = [a, b]
    elif role == ROLE_SHARED_NEIGHBOUR:
        disc = 1
        centre = draw(st.integers(min_value=2, max_value=sizes[0] - 1))
        flip = draw(st.sampled_from([-1.0, 1.0]))
        left, mid, right = (disc, centre - 1), (disc, centre), (disc, centre + 1)
        files[left] = listed[left] + flip * draw(_tight_off)
        files[mid] = listed[mid] - flip * draw(_tight_comp)
        files[right] = listed[right] + flip * draw(_tight_off)
        planted = [left, right]
    elif role in (ROLE_ONLY_TRACK_ON_DISC_LONGER,
                  ROLE_ONLY_TRACK_ON_DISC_SHORTER):
        files[(2, 1)] = listed[(2, 1)] + sign * draw(_offset)
        planted = [(2, 1)]
    elif role in (ROLE_ONLY_TRACK_OF_RELEASE_LONGER,
                  ROLE_ONLY_TRACK_OF_RELEASE_SHORTER):
        files[(1, 1)] = listed[(1, 1)] + sign * draw(_offset)
        planted = [(1, 1)]
    elif role in (ROLE_LAST_LONGER, ROLE_LAST_SHORTER):
        disc = draw(st.sampled_from(
            [d for d, size in enumerate(sizes, 1) if size >= 2]))
        last = (disc, sizes[disc - 1])
        following = (disc + 1, 1)
        if following in files and draw(st.booleans()):
            # The next disc's first track compensates within the bound: it
            # is NOT a neighbour, so this must stay a last-track shape.
            files[last] = listed[last] + sign * draw(_tight_off)
            files[following] = listed[following] - sign * draw(_tight_comp)
        else:
            files[last] = listed[last] + sign * draw(_offset)
        planted = [last]
    elif role in (ROLE_MIDDLE_LONGER, ROLE_MIDDLE_SHORTER):
        disc = draw(st.sampled_from(
            [d for d, size in enumerate(sizes, 1) if size >= 2]))
        key = (disc, draw(st.integers(
            min_value=1, max_value=sizes[disc - 1] - 1)))
        files[key] = listed[key] + sign * draw(_offset)
        planted = [key]
    elif role in (ROLE_UNPLACED_LONGER, ROLE_UNPLACED_SHORTER):
        key = (None, unplaced_count - 1)
        files[key] = listed[key] + sign * draw(_offset)
        planted = [key]
    elif role == ROLE_SEVERAL:
        a, b = draw(st.lists(
            st.sampled_from(keys), min_size=2, max_size=2, unique=True))
        # Two placed tracks side by side would be a run, not unrelated.
        assume(not (
            a[0] is not None and a[0] == b[0] and abs(a[1] - b[1]) == 1))
        for key in (a, b):
            files[key] = listed[key] + draw(_offset) * draw(
                st.sampled_from([-1.0, 1.0]))
        # Each file landing on the other's listing would be a swap.
        assume(
            abs(files[a] - listed[b]) > 2.0
            or abs(files[b] - listed[a]) > 2.0
        )
        planted = [a, b]

    def name(key: tuple[int | None, int]) -> str:
        disc, index = key
        return f"unplaced-{index}" if disc is None else f"{disc}-{index:02d}"

    pairs = [
        _pair(
            name(key), key[0], key[1] if key[0] is not None else None,
            sizes[key[0] - 1] if key[0] is not None else None,
            files[key], listed[key],
        )
        for key in keys
    ]
    one_disc_release = len(sizes) == 1 and sizes[0] == 1
    decoys = draw(_decoys(allow_own_disc=not one_disc_release))
    pairs.extend(decoys)
    pairs = list(draw(st.permutations(pairs)))
    return PlantedWorld(
        role, pairs, [name(key) for key in planted],
        [decoy.file_name for decoy in decoys],
    )


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
    named = {slot.file_name for slot in shape.slots}
    missing = [file_name for file_name in world.planted if file_name not in named]
    if missing:
        raise AssertionError(
            f"planted {world.role} but the shape omits tracks {missing}")
    decoys = sorted(named & set(world.decoys))
    if decoys:
        raise AssertionError(
            f"planted {world.role} but the shape names decoys {decoys}")


def _single_disc_world(
    role: str, lengths: list[tuple[float, float]], planted: list[int],
) -> PlantedWorld:
    pairs = [
        _pair(f"1-{index:02d}", 1, index, len(lengths), file_seconds,
              listed_seconds)
        for index, (file_seconds, listed_seconds) in enumerate(lengths, 1)
    ]
    return PlantedWorld(
        role, pairs, [f"1-{index:02d}" for index in planted], [])


_KURT_VILE = _single_disc_world(
    ROLE_ROTATE,
    [(338.1, 338.1), (336.3, 289.9), (228.6, 336.3),
     (393.1, 393.1), (289.9, 228.6), (273.1, 273.1)],
    [2, 3, 5],
)
#: Review finding: track 3 balances tracks 2 and 4 but can absorb only one.
_SHARED_NEIGHBOUR = _single_disc_world(
    ROLE_SHARED_NEIGHBOUR,
    [(200.0, 200.0), (261.0, 200.0), (140.0, 200.0),
     (261.0, 200.0), (200.0, 200.0)],
    [2, 4],
)


def _check(world: PlantedWorld) -> None:
    assert_shape_matches_planted(
        world, classify_track_length_shape(world.pairs))


class TestTrackLengthShapeGenerated(unittest.TestCase):
    """One property per planted role, so every role gets the whole
    example budget: under the derandomized suite profile a single property
    drawing the role from all fifteen never reached some of them (measured:
    no shared-neighbour world in 150 examples)."""

    @given(world=planted_worlds(ROLE_NONE))
    @example(world=PlantedWorld(ROLE_NONE, [], [], []))
    def test_none(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_ROTATE))
    @example(world=_KURT_VILE)
    def test_rotate(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_SPLIT))
    def test_split(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_SHARED_NEIGHBOUR))
    @example(world=_SHARED_NEIGHBOUR)
    def test_shared_neighbour(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_ONLY_TRACK_ON_DISC_LONGER))
    def test_only_track_on_disc_longer(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_ONLY_TRACK_ON_DISC_SHORTER))
    def test_only_track_on_disc_shorter(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_ONLY_TRACK_OF_RELEASE_LONGER))
    def test_only_track_of_release_longer(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_ONLY_TRACK_OF_RELEASE_SHORTER))
    def test_only_track_of_release_shorter(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_LAST_LONGER))
    def test_last_longer(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_LAST_SHORTER))
    def test_last_shorter(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_MIDDLE_LONGER))
    def test_middle_longer(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_MIDDLE_SHORTER))
    def test_middle_shorter(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_UNPLACED_LONGER))
    def test_unplaced_longer(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_UNPLACED_SHORTER))
    def test_unplaced_shorter(self, world: PlantedWorld) -> None:
        _check(world)

    @given(world=planted_worlds(ROLE_SEVERAL))
    def test_several(self, world: PlantedWorld) -> None:
        _check(world)


class TestCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause."""

    def test_clause_shape_on_clean_album_trips(self) -> None:
        shape = classify_track_length_shape(_KURT_VILE.pairs)
        with self.assertRaisesRegex(
            AssertionError, "no disagreement was planted but",
        ):
            assert_shape_matches_planted(
                PlantedWorld(ROLE_NONE, [], [], []), shape)

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
                _KURT_VILE._replace(role=ROLE_SPLIT), shape)

    def test_clause_omitted_track_trips(self) -> None:
        shape = classify_track_length_shape(_KURT_VILE.pairs)
        with self.assertRaisesRegex(
            AssertionError, r"omits tracks \['1-06'\]",
        ):
            assert_shape_matches_planted(
                _KURT_VILE._replace(planted=["1-02", "1-06"]), shape)

    def test_clause_named_decoy_trips(self) -> None:
        """Every other clause passes: right kind, every planted track
        named; but one named track is declared a decoy."""
        shape = classify_track_length_shape(_KURT_VILE.pairs)
        with self.assertRaisesRegex(
            AssertionError, r"names decoys \['1-03'\]",
        ):
            assert_shape_matches_planted(
                _KURT_VILE._replace(decoys=["1-03"]), shape)

    def test_the_real_worlds_pass(self) -> None:
        for world in (_KURT_VILE, _SHARED_NEIGHBOUR):
            with self.subTest(role=world.role):
                assert_shape_matches_planted(
                    world, classify_track_length_shape(world.pairs))


if __name__ == "__main__":
    unittest.main()
