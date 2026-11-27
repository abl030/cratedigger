"""Generated properties for the upgrade guard (issue #1445 item 3).

Production boundary: ``lib.quality.installed_copy_guard.compare_installed_copy``.

  U1  ``holds`` is exactly the independently restated rule: some track whose
      key is unique and non-blank on both sides, that is not a Discogs
      composite, and whose two copy lengths are usable, differs by more than
      the threshold without the release explaining the new copy strictly
      better.
  U2  Adding tracks the guard cannot compare (unkeyed, composite, missing a
      length, absent from the installed copy) never changes the verdict.
  U3  Two copies that agree never hold, whatever the release says.

The checker restates the rule from the issue rather than asking the module,
and the known-bad self-test feeds it the planted mutants that matter: a
``>=`` threshold, a dropped exemption, and positional keying.
"""

from __future__ import annotations

import math
import unittest

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.quality.installed_copy_guard import (
    UPGRADE_GUARD_TRACK_LENGTH_SECONDS,
    CandidateTrackLength,
    InstalledCopyComparison,
    InstalledTrackLength,
    compare_installed_copy,
)

_LENGTHS = st.one_of(
    st.floats(min_value=1.0, max_value=900.0, allow_nan=False),
    st.sampled_from([0.0, -1.0, math.nan, math.inf]),
    # Around the threshold, where an off-by-one lives.
    st.sampled_from([100.0, 130.0, 130.05, 129.95, 70.0]),
)
_KEYS = st.sampled_from(["a", "b", "c", "d", ""])


@st.composite
def worlds(draw) -> tuple[list[CandidateTrackLength], list[InstalledTrackLength]]:
    candidate = [
        CandidateTrackLength(
            key=draw(_KEYS),
            medium=1,
            medium_index=index,
            title="",
            declared_seconds=draw(_LENGTHS),
            candidate_seconds=draw(_LENGTHS),
            component_count=draw(st.sampled_from([1, 1, 1, 2])),
        )
        for index in range(1, draw(st.integers(0, 5)) + 1)
    ]
    installed = [
        InstalledTrackLength(key=draw(_KEYS), installed_seconds=draw(_LENGTHS))
        for _ in range(draw(st.integers(0, 5)))
    ]
    return candidate, installed


def _ok(value: float) -> bool:
    return math.isfinite(value) and value > 0


def oracle_holds(
    candidate: list[CandidateTrackLength],
    installed: list[InstalledTrackLength],
) -> bool:
    """The rule as the issue states it, written independently."""
    for track in candidate:
        if not track.key or track.component_count > 1:
            continue
        if sum(1 for other in candidate if other.key == track.key) != 1:
            continue
        matches = [item for item in installed if item.key == track.key]
        if len(matches) != 1:
            continue
        old = matches[0].installed_seconds
        new = track.candidate_seconds
        if not (_ok(old) and _ok(new)):
            continue
        if not abs(new - old) > UPGRADE_GUARD_TRACK_LENGTH_SECONDS:
            continue
        declared = track.declared_seconds
        if _ok(declared) and abs(new - declared) < abs(old - declared):
            continue
        return True
    return False


def guard_violation(
    candidate: list[CandidateTrackLength],
    installed: list[InstalledTrackLength],
    result: InstalledCopyComparison,
) -> str | None:
    """Checker U1. Returns a message on violation, else None."""
    if result.holds != oracle_holds(candidate, installed):
        return f"holds={result.holds} disagrees with the stated rule"
    if result.holds != any(row.tripped for row in result.rows):
        return "holds disagrees with the per-track rows"
    if result.compared_count != sum(1 for row in result.rows if row.compared):
        return "compared_count disagrees with the rows"
    return None


class TestInstalledCopyGuardGenerated(unittest.TestCase):
    @given(world=worlds())
    def test_u1_verdict_is_the_stated_rule(self, world):
        candidate, installed = world
        violation = guard_violation(
            candidate, installed, compare_installed_copy(candidate, installed))
        self.assertIsNone(violation, violation)

    @given(world=worlds(), extra=st.integers(0, 3))
    def test_u2_uncomparable_tracks_never_change_the_verdict(
        self, world, extra,
    ):
        candidate, installed = world
        before = compare_installed_copy(candidate, installed).holds
        noise = [
            track
            for index in range(extra)
            for track in (
                CandidateTrackLength(
                    key="", medium=2, medium_index=index, title="",
                    declared_seconds=100.0, candidate_seconds=999.0),
                CandidateTrackLength(
                    key=f"only-new-{index}", medium=2, medium_index=index,
                    title="", declared_seconds=100.0,
                    candidate_seconds=999.0),
                CandidateTrackLength(
                    key=f"composite-{index}", medium=2, medium_index=index,
                    title="", declared_seconds=100.0,
                    candidate_seconds=999.0, component_count=3),
            )
        ]
        installed_noise = [
            InstalledTrackLength(
                key=f"composite-{index}", installed_seconds=100.0)
            for index in range(extra)
        ]
        after = compare_installed_copy(
            [*candidate, *noise], [*installed, *installed_noise]).holds
        self.assertEqual(before, after)

    @given(world=worlds())
    def test_u3_agreeing_copies_never_hold(self, world):
        candidate, _ = world
        agreeing = [
            InstalledTrackLength(
                key=track.key, installed_seconds=track.candidate_seconds)
            for track in candidate
        ]
        self.assertFalse(compare_installed_copy(candidate, agreeing).holds)

    def test_known_bad_inputs_qualify_the_checker(self):
        at_threshold = (
            [CandidateTrackLength(
                key="k", medium=1, medium_index=1, title="",
                declared_seconds=100.0,
                candidate_seconds=100.0 + UPGRADE_GUARD_TRACK_LENGTH_SECONDS)],
            [InstalledTrackLength(key="k", installed_seconds=100.0)],
        )
        closer = (
            [CandidateTrackLength(
                key="k", medium=1, medium_index=1, title="",
                declared_seconds=127.0, candidate_seconds=127.0)],
            [InstalledTrackLength(key="k", installed_seconds=206.0)],
        )
        renumbered = (
            [CandidateTrackLength(
                key="k", medium=1, medium_index=1, title="",
                declared_seconds=127.0, candidate_seconds=206.0)],
            [InstalledTrackLength(key="other", installed_seconds=127.0)],
        )

        def planted_hold(holds: bool, compared: bool = True) -> InstalledCopyComparison:
            real = compare_installed_copy(*at_threshold)
            row = real.rows[0]
            return InstalledCopyComparison(
                threshold_seconds=real.threshold_seconds,
                rows=(msgspec.structs.replace(
                    row, tripped=holds, compared=compared),),
                compared_count=1 if compared else 0,
                holds=holds,
            )

        # ">=" threshold mutant: holds at exactly 30s.
        self.assertIsNotNone(guard_violation(*at_threshold, planted_hold(True)))
        # Dropped exemption: holds although the new copy matches the release.
        self.assertIsNotNone(guard_violation(*closer, planted_hold(True)))
        # Positional keying: compares tracks whose keys differ.
        self.assertIsNotNone(guard_violation(*renumbered, planted_hold(True)))
        # The real module passes all three.
        for world in (at_threshold, closer, renumbered):
            self.assertIsNone(
                guard_violation(*world, compare_installed_copy(*world)))


if __name__ == "__main__":
    unittest.main()
