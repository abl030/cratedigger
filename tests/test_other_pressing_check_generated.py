"""Generated property for the other-pressing check (issue #1445).

Drives the real ``lib.other_pressing_check.check_other_pressings`` over a
fake download_log row and a fake mirror that serves PLANTED worlds.

Worlds are planted, never recomputed as an oracle: each sibling release is
built to play one role and remembers it, each renamed title is planted at a
known position, and each disc track is planted to agree or disagree with
its file. The checker reads expectations from those plantings, never from
re-running the 2s comparison on the floats.

- target: one or two discs; every file within 0.9s of its listing;
  optionally a disc ID on each disc whose table of contents (whole
  sectors, 75 per second) is planted per track to agree with the file
  within 1s or to miss it by 3 to 200s;
- siblings, each with a role: ``match`` (every listing within 1s of its
  file, a planted subset of titles renamed), ``off`` (one listing 2.5 to
  200s from its file), ``missing`` (one listing absent), ``count`` (one
  track more or fewer).

Invariant: the matching pressings are exactly the planted ``match``
siblings, each with exactly its planted renamed positions; every disc
track's agreement is the planted one; and the first sentence says
"no other pressing" exactly when nothing matches.
"""

from __future__ import annotations

import unittest
from typing import NamedTuple

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.mb_api import MBDiscRelease, MBDiscReleasePage
from lib.other_pressing_check import PressingCheck, check_other_pressings

ROLE_MATCH = "match"
ROLE_OFF = "off"
ROLE_MISSING = "missing"
ROLE_COUNT = "count"

_TARGET_ID = "00000000-0000-0000-0000-00000000aaaa"
_GROUP_ID = "00000000-0000-0000-0000-00000000bbbb"
_NO_MATCH_SENTENCE = "No other pressing in this release group matches."


class PlantedWorld(NamedTuple):
    log_row: dict[str, object]
    target: dict[str, object]
    siblings: list[dict[str, object]]
    #: sibling id -> renamed (medium, position) pairs, for planted matches.
    expected_matches: dict[str, list[tuple[int, int]]]
    #: (medium, position) -> planted agreement, for tracks with a disc.
    expected_disc: dict[tuple[int, int], bool]


def _sibling_id(n: int) -> str:
    return f"00000000-0000-0000-0000-{n:012d}"


@st.composite
def planted_worlds(draw: st.DrawFn) -> PlantedWorld:
    sizes = draw(st.lists(
        st.integers(min_value=1, max_value=6), min_size=1, max_size=2))
    positions = [
        (medium, index)
        for medium, size in enumerate(sizes, 1)
        for index in range(1, size + 1)
    ]
    listed_ms = {
        key: draw(st.integers(min_value=30_000, max_value=600_000))
        for key in positions
    }
    files = {
        key: listed_ms[key] / 1000 + draw(st.floats(
            min_value=-0.9, max_value=0.9, allow_nan=False))
        for key in positions
    }
    titles = {key: f"Song {key[0]}-{key[1]}" for key in positions}

    # Target media, each optionally with a planted disc table of contents.
    expected_disc: dict[tuple[int, int], bool] = {}
    media: list[dict[str, object]] = []
    for medium, size in enumerate(sizes, 1):
        discs: list[dict[str, object]] = []
        if draw(st.booleans()):
            offsets: list[int] = []
            cursor = 150
            for index in range(1, size + 1):
                key = (medium, index)
                agrees = draw(st.booleans())
                if agrees:
                    delta = draw(st.floats(min_value=-1.0, max_value=1.0))
                else:
                    delta = draw(st.floats(min_value=3.0, max_value=200.0)) \
                        * draw(st.sampled_from([-1.0, 1.0]))
                disc_seconds = max(1.0, files[key] + delta)
                if not agrees and abs(disc_seconds - files[key]) <= 2.5:
                    disc_seconds = files[key] + 3.0
                offsets.append(cursor)
                cursor += round(disc_seconds * 75)
                expected_disc[key] = agrees
            discs.append({
                "id": f"disc{medium}", "offset-count": size,
                "sectors": cursor, "offsets": offsets,
            })
        media.append({
            "position": medium, "format": "CD", "discs": discs,
            "tracks": [
                {"position": index, "title": titles[(medium, index)],
                 "length": listed_ms[(medium, index)]}
                for index in range(1, size + 1)
            ],
        })
    target = {
        "id": _TARGET_ID, "title": "Album", "country": "GB",
        "date": "2004", "media": media,
        "release-group": {"id": _GROUP_ID, "title": "Album"},
    }

    roles = draw(st.lists(
        st.sampled_from([ROLE_MATCH, ROLE_OFF, ROLE_MISSING, ROLE_COUNT]),
        max_size=5))
    siblings: list[dict[str, object]] = []
    expected: dict[str, list[tuple[int, int]]] = {}
    for n, role in enumerate(roles, 1):
        sibling_id = _sibling_id(n)
        lengths: list[int | None] = [
            round((files[key] + draw(st.floats(min_value=-1.0, max_value=1.0)))
                  * 1000)
            for key in positions
        ]
        names = [titles[key] for key in positions]
        renamed: list[tuple[int, int]] = []
        if role == ROLE_MATCH:
            for at, key in enumerate(positions):
                if draw(st.booleans()):
                    names[at] = f"Other title {at}"
                    renamed.append(key)
            expected[sibling_id] = renamed
        elif role == ROLE_OFF:
            at = draw(st.integers(min_value=0, max_value=len(positions) - 1))
            gap = draw(st.floats(min_value=2.5, max_value=200.0))
            sign = draw(st.sampled_from([-1.0, 1.0]))
            if sign < 0 and files[positions[at]] - gap < 1.0:
                sign = 1.0
            lengths[at] = round((files[positions[at]] + sign * gap) * 1000)
        elif role == ROLE_MISSING:
            at = draw(st.integers(min_value=0, max_value=len(positions) - 1))
            lengths[at] = None
        else:
            if draw(st.booleans()) and len(lengths) > 1:
                lengths = lengths[:-1]
                names = names[:-1]
            else:
                lengths = [*lengths, 200_000]
                names = [*names, "Bonus"]
        siblings.append({
            "id": sibling_id, "title": "Album", "country": "US",
            "date": "2005", "media": [{
                "position": 1, "format": "CD", "discs": [],
                "tracks": [
                    {"position": i, "title": name, "length": length}
                    for i, (name, length) in enumerate(
                        zip(names, lengths, strict=True), 1)
                ],
            }],
        })

    log_row: dict[str, object] = {
        "id": 1, "outcome": "success",
        "validation_result": {"candidates": [{
            "album_id": _TARGET_ID, "is_target": True,
            "extra_items": [], "extra_tracks": [],
            "mapping": [
                {"item": {"length": files[key]},
                 "track": {"medium": key[0], "medium_index": key[1]}}
                for key in positions
            ],
        }]},
    }
    return PlantedWorld(log_row, target, siblings, expected, expected_disc)


class _Log:
    def __init__(self, row: dict[str, object]) -> None:
        self.row = row

    def get_download_log_entry(self, log_id: int) -> dict[str, object] | None:
        return self.row if log_id == 1 else None


class _Mirror:
    def __init__(self, world: PlantedWorld) -> None:
        self.world = world

    def release(self, release_id: str, *, timeout: float) -> MBDiscRelease:
        return msgspec.convert(self.world.target, type=MBDiscRelease)

    def release_group_page(
        self, release_group_id: str, *, offset: int, limit: int,
        timeout: float,
    ) -> MBDiscReleasePage:
        releases = [self.world.target, *self.world.siblings][offset:offset + limit]
        return msgspec.convert(
            {"release-count": 1 + len(self.world.siblings),
             "releases": releases},
            type=MBDiscReleasePage)


def run(world: PlantedWorld) -> PressingCheck:
    return check_other_pressings(
        1, pdb=_Log(world.log_row), mirror=_Mirror(world), clock=lambda: 0.0)


def assert_check_matches_planted(
    world: PlantedWorld, result: PressingCheck,
) -> None:
    found = {
        match.release_id: [
            (d.medium, d.position) for d in match.title_differences]
        for match in result.matching_pressings
    }
    if set(found) != set(world.expected_matches):
        raise AssertionError(
            f"matched pressings {sorted(found)} but planted "
            f"{sorted(world.expected_matches)}")
    for release_id, renamed in world.expected_matches.items():
        if found[release_id] != renamed:
            raise AssertionError(
                f"title differences for {release_id} were {found[release_id]} "
                f"but planted {renamed}")
    lengths = result.target_lengths
    disc = {} if lengths is None else {
        (row.medium, row.position): row.agrees for row in lengths.tracks}
    if disc != world.expected_disc:
        raise AssertionError(
            f"disc agreement {disc} but planted {world.expected_disc}")
    says_none = bool(result.sentences) and result.sentences[0] == _NO_MATCH_SENTENCE
    if says_none != (not world.expected_matches):
        raise AssertionError(
            f"lead sentence {result.sentences[:1]} does not fit "
            f"{len(world.expected_matches)} planted matches")


class TestOtherPressingCheckGenerated(unittest.TestCase):
    @given(world=planted_worlds())
    def test_check_matches_planted_world(self, world: PlantedWorld) -> None:
        assert_check_matches_planted(world, run(world))


def _small_world() -> PlantedWorld:
    """One disc of two tracks, a disc ID agreeing on both, one planted
    matching sibling with track 2 renamed."""
    files = {(1, 1): 100.0, (1, 2): 200.0}
    target = {
        "id": _TARGET_ID, "title": "A", "country": "GB", "date": "2004",
        "release-group": {"id": _GROUP_ID, "title": "A"},
        "media": [{
            "position": 1, "format": "CD",
            "discs": [{"id": "d", "offset-count": 2, "sectors": 150 + 22500,
                       "offsets": [150, 150 + 7500]}],
            "tracks": [
                {"position": 1, "title": "One", "length": 100_000},
                {"position": 2, "title": "Two", "length": 200_000},
            ],
        }],
    }
    sibling = {
        "id": _sibling_id(1), "title": "A", "country": "US", "date": "2005",
        "media": [{"position": 1, "format": "CD", "discs": [], "tracks": [
            {"position": 1, "title": "One", "length": 100_400},
            {"position": 2, "title": "Deux", "length": 199_700},
        ]}],
    }
    row: dict[str, object] = {"id": 1, "validation_result": {"candidates": [{
        "album_id": _TARGET_ID, "is_target": True,
        "mapping": [
            {"item": {"length": seconds},
             "track": {"medium": key[0], "medium_index": key[1]}}
            for key, seconds in files.items()
        ],
    }]}}
    return PlantedWorld(
        row, target, [sibling], {_sibling_id(1): [(1, 2)]},
        {(1, 1): True, (1, 2): True})


class TestCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause. Each feeds the real result of
    the small world against a planting altered in one respect only."""

    def setUp(self) -> None:
        self.world = _small_world()
        self.result = run(self.world)

    def test_the_small_world_passes(self) -> None:
        assert_check_matches_planted(self.world, self.result)

    def test_clause_matched_set_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "matched pressings"):
            assert_check_matches_planted(
                self.world._replace(expected_matches={}), self.result)

    def test_clause_title_differences_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "title differences"):
            assert_check_matches_planted(
                self.world._replace(
                    expected_matches={_sibling_id(1): [(1, 1)]}),
                self.result)

    def test_clause_disc_agreement_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "disc agreement"):
            assert_check_matches_planted(
                self.world._replace(
                    expected_disc={(1, 1): True, (1, 2): False}),
                self.result)

    def test_clause_lead_sentence_trips(self) -> None:
        wrong = msgspec.structs.replace(
            self.result, sentences=[_NO_MATCH_SENTENCE])
        with self.assertRaisesRegex(AssertionError, "lead sentence"):
            assert_check_matches_planted(self.world, wrong)


if __name__ == "__main__":
    unittest.main()
