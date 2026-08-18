"""Generated property for the render-time track-length warning (issue
#1178, post-correction "surface-not-reject").

Deterministic adapter-level pins (the exact #1178 world, the pregap skip,
the under-bound world, the non-success outcome, the non-target-sibling
world, and the no-candidates world) live in
``tests/web/test_routes_pipeline.py``, driven through the FULL outermost
adapter (the real ``/api/pipeline/log`` route). This module patrols the
derivation's world space by calling ``classify_log_entry`` directly — the
real production function, never a reimplemented copy of it.

**F9 (test-fidelity review lesson): worlds are PLANTED, never recomputed
as an oracle.** Each generated pair is tagged with the role it was built
to play — ``"under"`` (deviation deliberately drawn below the bound),
``"over"`` (deliberately above it), or ``"unknown"`` (one side
deliberately non-positive, the other deliberately huge, so a broken skip
is loudly visible). The checker's expectation is derived from which roles
were planted, never from re-computing ``abs(item - track) > bound`` on the
raw floats — that recomputation is exactly the shared-misreading trap a
transcribed oracle cannot catch (a prior review, F9).

**Second review round (F1): the world model now has a CANDIDATE-LIST
dimension, not just a pair-list dimension.** Every earlier world built
exactly one candidate, always ``is_target=True``, so the ``is_target``
filter in ``_track_length_warning`` was never exercised by ANY generated
world — deleting the filter passed every test that existed at the time,
and only live data (146 rows flagged vs the real 124, independently
re-verified against the live corpus) proved the filter load-bearing: a
sibling pressing's own mismatch must never leak into the rendered warning
for a DIFFERENT selected release. ``_planted_candidate_world`` now
generates a list of candidates with exactly one ``is_target=True`` at a
randomized position; the checker's expectation is read from that target
candidate's own planted pairs ONLY, never from any sibling's.

Invariant: given ``outcome == "success"``, the rendered warning is present
iff at least one planted ``"over"`` pair exists among the SELECTED
(``is_target=True``) candidate's own mapping — a non-target sibling
candidate's pairs, however extreme, never count.
"""

from __future__ import annotations

import unittest

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.quality import (
    CandidateSummary,
    HarnessItem,
    HarnessTrackInfo,
    TrackMapping,
    ValidationResult,
)
from web.classify import (
    TRACK_LENGTH_WARNING_BOUND_SECONDS,
    LogEntry,
    classify_log_entry,
)

ROLE_UNDER = "under"
ROLE_OVER = "over"
ROLE_UNKNOWN = "unknown"

_BOUND = TRACK_LENGTH_WARNING_BOUND_SECONDS

#: One planted pair: (role, track_length, item_length).
_PlantedPair = tuple[str, float, float]
#: One planted candidate: (is_target, its own planted pairs).
_PlantedCandidate = tuple[bool, list[_PlantedPair]]


@st.composite
def _planted_pair(draw: st.DrawFn) -> _PlantedPair:
    """One ``(role, track_length, item_length)`` triple.

    ``"unknown"`` plants one side at 0 (no declared/measured length) and
    the OTHER side at a deliberately huge value — a broken missing-length
    skip would otherwise see a massive deviation and warn.
    """
    role = draw(st.sampled_from([ROLE_UNDER, ROLE_OVER, ROLE_UNKNOWN]))
    if role == ROLE_UNKNOWN:
        huge = draw(st.floats(
            min_value=100.0, max_value=1000.0,
            allow_nan=False, allow_infinity=False,
        ))
        track_first = draw(st.booleans())
        track_length = 0.0 if track_first else huge
        item_length = huge if track_first else 0.0
        return role, track_length, item_length

    base = draw(st.floats(
        min_value=5.0, max_value=300.0,
        allow_nan=False, allow_infinity=False,
    ))
    if role == ROLE_UNDER:
        deviation = draw(st.floats(
            min_value=0.0, max_value=_BOUND - 0.01,
            allow_nan=False, allow_infinity=False,
        ))
    else:
        deviation = draw(st.floats(
            min_value=_BOUND + 0.01, max_value=400.0,
            allow_nan=False, allow_infinity=False,
        ))
    # Both directions: the file measuring longer than declared, and the
    # file measuring shorter than declared (kills an unsigned-vs-signed
    # deviation mutant).
    item_longer = draw(st.booleans())
    if item_longer:
        return role, base, base + deviation
    return role, base + deviation, base


@st.composite
def _planted_candidate_world(
    draw: st.DrawFn,
) -> tuple[list[_PlantedCandidate], list[_PlantedPair]]:
    """A list of candidates with EXACTLY ONE ``is_target=True``, at a
    randomized position among 0-2 non-target siblings. Returns
    ``(candidates, target_pairs)`` — the checker's expectation is read
    from ``target_pairs`` alone, never from a sibling's.
    """
    target_pairs = draw(st.lists(_planted_pair(), min_size=0, max_size=4))
    num_siblings = draw(st.integers(min_value=0, max_value=2))
    sibling_pair_lists = draw(st.lists(
        st.lists(_planted_pair(), min_size=0, max_size=4),
        min_size=num_siblings, max_size=num_siblings,
    ))
    target_index = draw(st.integers(min_value=0, max_value=num_siblings))
    candidates: list[_PlantedCandidate] = []
    sibling_iter = iter(sibling_pair_lists)
    for position in range(num_siblings + 1):
        if position == target_index:
            candidates.append((True, target_pairs))
        else:
            candidates.append((False, next(sibling_iter)))
    return candidates, target_pairs


def _candidate_summary_from_pairs(
    pairs: list[_PlantedPair], *, mbid: str, is_target: bool,
) -> CandidateSummary:
    mapping = [
        TrackMapping(
            item=HarnessItem(path=f"{mbid}-track-{index}.flac", length=item_length),
            track=HarnessTrackInfo(
                title=f"{mbid} Track {index}", length=track_length),
        )
        for index, (_role, track_length, item_length) in enumerate(pairs)
    ]
    return CandidateSummary(
        mbid=mbid, distance=0.05, is_target=is_target, mapping=mapping,
    )


def _entry_for_candidate_world(candidates: list[_PlantedCandidate]) -> LogEntry:
    """A ``LogEntry`` whose ``validation_result`` carries every planted
    candidate, in the exact wire shape ``apply_candidate_scenario``
    persists (built from the real Structs, never hand-typed keys)."""
    summaries = [
        _candidate_summary_from_pairs(
            pairs, mbid=f"rel-{index}", is_target=is_target,
        )
        for index, (is_target, pairs) in enumerate(candidates)
    ]
    result = ValidationResult(
        valid=True, scenario="strong_match", distance=0.05,
        mbid_found=True,
        target_mbid=next(
            summary.mbid for summary in summaries if summary.is_target
        ),
        candidates=summaries,
    )
    return LogEntry(
        id=1, request_id=1, outcome="success",
        validation_result=msgspec.to_builtins(result),
    )


def _entry_for_planted_pairs(planted: list[_PlantedPair]) -> LogEntry:
    """Single-candidate convenience wrapper — the historical world shape,
    still a valid (0-sibling) instance of the candidate-list world."""
    return _entry_for_candidate_world([(True, planted)])


def assert_warning_matches_planted_roles(
    planted: list[_PlantedPair],
    warning: str | None,
) -> None:
    """The checker: two clauses, one per direction of the biconditional.

    ``planted`` is always the TARGET candidate's own pairs — callers never
    pass a sibling's roles in here, so this function has no notion of
    ``is_target`` itself; the candidate-list world's job is choosing which
    pairs count as ``planted`` before calling this.
    """
    expected = any(role == ROLE_OVER for role, _, _ in planted)
    if expected and warning is None:
        raise AssertionError(
            "a planted 'over' pair exists but no warning was rendered"
        )
    if not expected and warning is not None:
        raise AssertionError(
            "no planted 'over' pair exists but a warning was rendered: "
            f"{warning!r}"
        )


class TestTrackLengthWarningGenerated(unittest.TestCase):
    @given(world=_planted_candidate_world())
    @example(world=([(True, [])], []))
    @example(
        world=(
            [(True, [(ROLE_OVER, 15.0, 237.633167)])],
            [(ROLE_OVER, 15.0, 237.633167)],
        ),
    )  # #1178 shape, single candidate
    @example(
        world=(
            [(True, [(ROLE_UNKNOWN, 0.0, 500.0)])],
            [(ROLE_UNKNOWN, 0.0, 500.0)],
        ),
    )
    @example(
        world=(
            [(True, [(ROLE_UNKNOWN, 500.0, 0.0)])],
            [(ROLE_UNKNOWN, 500.0, 0.0)],
        ),
    )
    @example(
        world=(
            [(True, [
                (ROLE_UNDER, 100.0, 105.0), (ROLE_OVER, 100.0, 200.0),
            ])],
            [(ROLE_UNDER, 100.0, 105.0), (ROLE_OVER, 100.0, 200.0)],
        ),
    )
    # F9: a deviation of EXACTLY the bound (60.0) is not a mismatch — the
    # comparison is strictly-greater-than. Tagged ROLE_UNDER because that
    # is the real decided outcome (no warning); the strategy itself never
    # generates the exact boundary (it deliberately avoids it, see
    # ``_planted_pair``), so only a pinned example reaches this world.
    @example(
        world=(
            [(True, [(ROLE_UNDER, 100.0, 100.0 + TRACK_LENGTH_WARNING_BOUND_SECONDS)])],
            [(ROLE_UNDER, 100.0, 100.0 + TRACK_LENGTH_WARNING_BOUND_SECONDS)],
        ),
    )
    # F1: the target candidate is CLEAN (no over pair); a non-target
    # sibling BEFORE it carries an over-bound pair. Must NOT warn.
    @example(
        world=(
            [
                (False, [(ROLE_OVER, 15.0, 237.633167)]),
                (True, [(ROLE_UNDER, 100.0, 105.0)]),
            ],
            [(ROLE_UNDER, 100.0, 105.0)],
        ),
    )
    # F1: same, sibling AFTER the target — position must not matter.
    @example(
        world=(
            [
                (True, []),
                (False, [(ROLE_OVER, 500.0, 15.0)]),
            ],
            [],
        ),
    )
    def test_warning_matches_target_candidates_own_planted_roles(
        self, world: tuple[list[_PlantedCandidate], list[_PlantedPair]],
    ) -> None:
        candidates, target_pairs = world
        entry = _entry_for_candidate_world(candidates)
        classified = classify_log_entry(entry)
        assert_warning_matches_planted_roles(
            target_pairs, classified.track_length_warning,
        )


class TestCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause (per-clause proof)."""

    def test_clause_1_false_negative_trips(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "a planted 'over' pair exists but no warning was rendered",
        ):
            assert_warning_matches_planted_roles(
                [(ROLE_OVER, 15.0, 237.6)], None,
            )

    def test_clause_2_false_positive_trips(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "no planted 'over' pair exists but a warning was rendered",
        ):
            assert_warning_matches_planted_roles(
                [(ROLE_UNDER, 100.0, 105.0)], "fabricated warning",
            )

    def test_a_classified_world_passes(self) -> None:
        planted = [(ROLE_OVER, 15.0, 237.633167)]
        entry = _entry_for_planted_pairs(planted)
        classified = classify_log_entry(entry)
        assert_warning_matches_planted_roles(
            planted, classified.track_length_warning,
        )

    def test_a_non_target_sibling_over_pair_does_not_trip_the_checker(
        self,
    ) -> None:
        """Sanity companion to the F1 examples above: driving the real
        adapter over a world where ONLY a sibling is over-bound must
        classify clean, and the checker (fed the target's true empty
        pairs) must not raise."""
        candidates: list[_PlantedCandidate] = [
            (False, [(ROLE_OVER, 15.0, 237.633167)]),
            (True, []),
        ]
        entry = _entry_for_candidate_world(candidates)
        classified = classify_log_entry(entry)
        self.assertIsNone(classified.track_length_warning)
        assert_warning_matches_planted_roles(
            [], classified.track_length_warning,
        )


if __name__ == "__main__":
    unittest.main()
