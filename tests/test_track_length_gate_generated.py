"""Generated property for the per-track length gate (issue #1178).

``tests/test_track_length_gate.py`` pins the exact #1178 world plus the
force-lane and pregap must-still-work cases; this module patrols the space
around them.

V1. **Given no ``extra_tracks``, complete admitted-audio coverage, and a
    distance within threshold, ``apply_candidate_scenario`` names
    ``track_length_mismatch`` iff some mapped pair with BOTH a positive
    declared MB length and a positive measured file length deviates by
    more than the bound — otherwise ``strong_match``.** The strategy draws
    raw ``(track_length, item_length)`` pairs spanning zero (missing/
    unmeasured), values well under the bound, and values straddling it in
    both directions (file longer than declared AND file shorter than
    declared), so the property cannot be satisfied by a one-directional
    comparison.
"""

from __future__ import annotations

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.beets import TRACK_LENGTH_MISMATCH_BOUND_SECONDS, apply_candidate_scenario
from lib.quality import CandidateSummary, ValidationResult
from tests.test_track_length_gate import _admitted_items, _apply, _candidate, _mapping

#: Raw ``(track_length, item_length)`` pairs. ``0.0`` on either side models
#: a missing declared length (a CD pregap hidden track) or an unmeasured
#: file — the gate must skip such a pair regardless of the other side.
_LENGTH = st.floats(
    min_value=0.0, max_value=400.0, allow_nan=False, allow_infinity=False,
)
_PAIRS = st.lists(st.tuples(_LENGTH, _LENGTH), min_size=1, max_size=6)


def _oracle_worst_known_deviation(
    pairs: list[tuple[float, float]],
) -> float | None:
    """Independent restatement of the invariant over raw pairs — never the
    production helper, so the property cannot agree with a broken
    implementation by construction."""
    known = [
        abs(item_length - track_length)
        for track_length, item_length in pairs
        if track_length > 0 and item_length > 0
    ]
    return max(known) if known else None


def assert_track_length_gate_matches_worst_deviation(
    pairs: list[tuple[float, float]],
    result: ValidationResult,
    *,
    bound: float,
) -> None:
    """The checker: two clauses, one per direction of the biconditional."""
    worst = _oracle_worst_known_deviation(pairs)
    expected_mismatch = worst is not None and worst > bound
    if expected_mismatch and result.scenario != "track_length_mismatch":
        raise AssertionError(
            f"worst known deviation {worst} exceeds bound {bound} but "
            f"result named {result.scenario!r}, not track_length_mismatch"
        )
    if not expected_mismatch and result.scenario == "track_length_mismatch":
        raise AssertionError(
            f"no known pair deviates beyond bound {bound} (worst={worst}) "
            "but result named track_length_mismatch"
        )


def _candidate_from_pairs(
    pairs: list[tuple[float, float]], *, distance: float,
) -> CandidateSummary:
    mappings = [
        _mapping(
            f"track-{index}.flac",
            item_length=item_length,
            track_length=track_length,
        )
        for index, (track_length, item_length) in enumerate(pairs)
    ]
    return _candidate(mappings, distance=distance)


class TestTrackLengthGateGenerated(unittest.TestCase):
    @given(pairs=_PAIRS)
    @example(pairs=[(15.0, 237.633167)])  # #1178 shape
    @example(pairs=[(100.0, 100.0 + TRACK_LENGTH_MISMATCH_BOUND_SECONDS)])
    @example(pairs=[(100.0, 100.0 + TRACK_LENGTH_MISMATCH_BOUND_SECONDS + 0.1)])
    @example(pairs=[(237.6, 15.0)])  # file shorter than declared, mismatched
    @example(pairs=[(0.0, 500.0)])  # pregap: no declared length
    @example(pairs=[(0.0, 0.0)])  # neither side usable
    def test_worst_known_deviation_decides_the_scenario(
        self, pairs: list[tuple[float, float]],
    ) -> None:
        candidate = _candidate_from_pairs(pairs, distance=0.05)
        result = _apply(candidate)

        assert_track_length_gate_matches_worst_deviation(
            pairs, result, bound=TRACK_LENGTH_MISMATCH_BOUND_SECONDS,
        )


class TestCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause (per-clause proof)."""

    def test_clause_1_false_negative_trips(self) -> None:
        """A world where the oracle says track_length_mismatch is owed,
        fed a result that (falsely) claims strong_match."""
        broken_result = ValidationResult(valid=True, scenario="strong_match")
        with self.assertRaisesRegex(
            AssertionError, "exceeds bound .* but result named 'strong_match'",
        ):
            assert_track_length_gate_matches_worst_deviation(
                [(15.0, 237.6)], broken_result, bound=60.0,
            )

    def test_clause_2_false_positive_trips(self) -> None:
        """A world with no known-pair deviation beyond the bound (in fact
        no known pair at all), fed a result that (falsely) claims a
        mismatch — the pregap-shaped false-positive world."""
        broken_result = ValidationResult(
            valid=False, scenario="track_length_mismatch",
        )
        with self.assertRaisesRegex(
            AssertionError,
            "no known pair deviates beyond bound .* but result named "
            "track_length_mismatch",
        ):
            assert_track_length_gate_matches_worst_deviation(
                [(0.0, 500.0)], broken_result, bound=60.0,
            )

    def test_a_classified_world_passes(self) -> None:
        candidate = _candidate_from_pairs([(15.0, 237.6)], distance=0.05)
        result = ValidationResult()
        apply_candidate_scenario(
            result, candidate, 0.15,
            admitted_items=_admitted_items(candidate),
        )
        assert_track_length_gate_matches_worst_deviation(
            [(15.0, 237.6)], result, bound=TRACK_LENGTH_MISMATCH_BOUND_SECONDS,
        )


if __name__ == "__main__":
    unittest.main()
