"""Generated property for the render-time track-length warning (issue
#1178, post-correction "surface-not-reject").

Deterministic adapter-level pins (the exact #1178 world, the pregap skip,
the under-bound world, the non-success outcome, and the no-candidates
world) live in ``tests/web/test_routes_pipeline.py``, driven through the
FULL outermost adapter (the real ``/api/pipeline/log`` route). This module
patrols the derivation's world space by calling ``classify_log_entry``
directly — the real production function, never a reimplemented copy of it.

**F9 (test-fidelity review lesson): worlds are PLANTED, never recomputed
as an oracle.** Each generated pair is tagged with the role it was built
to play — ``"under"`` (deviation deliberately drawn below the bound),
``"over"`` (deliberately above it), or ``"unknown"`` (one side
deliberately non-positive, the other deliberately huge, so a broken skip
is loudly visible). The checker's expectation is derived from which roles
were planted, never from re-computing ``abs(item - track) > bound`` on the
raw floats — that recomputation is exactly the shared-misreading trap a
transcribed oracle cannot catch (a prior review, F9).

Invariant: given ``outcome == "success"``, the rendered warning is present
iff at least one planted ``"over"`` pair exists among the target
candidate's mapping.
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


@st.composite
def _planted_pair(draw: st.DrawFn) -> tuple[str, float, float]:
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


def _entry_for_planted_pairs(planted: list[tuple[str, float, float]]) -> LogEntry:
    """A ``LogEntry`` whose selected candidate carries exactly these
    planted pairs, in the exact wire shape ``apply_candidate_scenario``
    persists (built from the real Structs, never hand-typed keys)."""
    mapping = [
        TrackMapping(
            item=HarnessItem(path=f"track-{index}.flac", length=item_length),
            track=HarnessTrackInfo(
                title=f"Track {index}", length=track_length),
        )
        for index, (_role, track_length, item_length) in enumerate(planted)
    ]
    candidate = CandidateSummary(
        mbid="rel-1", distance=0.05, is_target=True, mapping=mapping,
    )
    result = ValidationResult(
        valid=True, scenario="strong_match", distance=0.05,
        mbid_found=True, target_mbid="rel-1", candidates=[candidate],
    )
    return LogEntry(
        id=1, request_id=1, outcome="success",
        validation_result=msgspec.to_builtins(result),
    )


def assert_warning_matches_planted_roles(
    planted: list[tuple[str, float, float]],
    warning: str | None,
) -> None:
    """The checker: two clauses, one per direction of the biconditional."""
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
    @given(planted=st.lists(_planted_pair(), min_size=0, max_size=6))
    @example(planted=[])
    @example(planted=[(ROLE_OVER, 15.0, 237.633167)])  # #1178 shape
    @example(planted=[(ROLE_UNKNOWN, 0.0, 500.0)])
    @example(planted=[(ROLE_UNKNOWN, 500.0, 0.0)])
    @example(planted=[
        (ROLE_UNDER, 100.0, 105.0),
        (ROLE_OVER, 100.0, 200.0),
    ])
    def test_warning_matches_planted_roles(
        self, planted: list[tuple[str, float, float]],
    ) -> None:
        entry = _entry_for_planted_pairs(planted)
        classified = classify_log_entry(entry)
        assert_warning_matches_planted_roles(
            planted, classified.track_length_warning,
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


if __name__ == "__main__":
    unittest.main()
