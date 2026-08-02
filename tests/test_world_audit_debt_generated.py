"""Generated laws for monotone live-world audit debt."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.world_audit_debt import assess_world_audit_debt
from lib.world_audit_service import (
    WorldAuditCounts,
    WorldAuditReport,
    build_world_audit_report,
)
from lib.world_invariants import WorldViolation


@dataclass(frozen=True)
class _DebtWorld:
    baseline: tuple[WorldViolation, ...]
    current: tuple[WorldViolation, ...]
    expected_new: int
    expected_changed: int
    expected_pass: bool


def _violation(request_id: int, *, changed: bool = False) -> WorldViolation:
    code = (
        "current_evidence_missing"
        if request_id % 2
        else "evidence_fingerprint_mismatch"
    )
    suffix = "changed" if changed else "original"
    return WorldViolation(
        code=code,
        detail=f"request {request_id} cause {suffix}",
        request_id=request_id,
        release_id=f"release-{request_id}",
    )


def _report(violations: tuple[WorldViolation, ...]) -> WorldAuditReport:
    return build_world_audit_report(
        counts=WorldAuditCounts(
            active_requests=50,
            beets_albums=45,
            linked_evidence=40,
            denylist_rows=5,
        ),
        violations=violations,
    )


@st.composite
def _debt_worlds(draw: st.DrawFn) -> _DebtWorld:
    baseline_ids = draw(st.lists(
        st.integers(min_value=1, max_value=10_000),
        min_size=1,
        max_size=12,
        unique=True,
    ))
    actions = draw(st.lists(
        st.sampled_from(("keep", "resolve", "change")),
        min_size=len(baseline_ids),
        max_size=len(baseline_ids),
    ))
    new_ids = draw(st.lists(
        st.integers(min_value=10_001, max_value=20_000),
        max_size=5,
        unique=True,
    ))

    baseline = tuple(_violation(request_id) for request_id in baseline_ids)
    current: list[WorldViolation] = []
    changed = 0
    for request_id, action in zip(baseline_ids, actions, strict=True):
        if action == "keep":
            current.append(_violation(request_id))
        elif action == "change":
            current.append(_violation(request_id, changed=True))
            changed += 1
    current.extend(_violation(request_id) for request_id in new_ids)
    return _DebtWorld(
        baseline=baseline,
        current=tuple(reversed(current)),
        expected_new=len(new_ids),
        expected_changed=changed,
        expected_pass=not new_ids and changed == 0,
    )


class TestGeneratedWorldAuditDebt(unittest.TestCase):
    @given(_debt_worlds())
    def test_only_an_exact_subset_can_advance_the_known_cohort(
        self,
        world: _DebtWorld,
    ) -> None:
        from lib.world_audit_debt import initialize_world_audit_debt_state

        state = initialize_world_audit_debt_state(_report(world.baseline))
        evaluation = assess_world_audit_debt(state, _report(world.current))

        self.assertEqual(evaluation.passed, world.expected_pass)
        self.assertEqual(evaluation.report.new_members, world.expected_new)
        self.assertEqual(
            evaluation.report.changed_members,
            world.expected_changed,
        )
        if world.expected_pass:
            self.assertIsNotNone(evaluation.next_state)
            assert evaluation.next_state is not None
            self.assertEqual(
                len(evaluation.next_state.remaining),
                len(world.current),
            )
            self.assertEqual(
                evaluation.report.newly_converged,
                len(world.baseline) - len(world.current),
            )
        else:
            self.assertIsNone(evaluation.next_state)

    def test_count_only_mutant_accepts_a_member_replacement(self) -> None:
        baseline = (_violation(1), _violation(2))
        current = (_violation(1), _violation(10_001))

        count_only_mutant_passes = len(current) <= len(baseline)

        self.assertTrue(count_only_mutant_passes)
        from lib.world_audit_debt import initialize_world_audit_debt_state

        evaluation = assess_world_audit_debt(
            initialize_world_audit_debt_state(_report(baseline)),
            _report(current),
        )
        self.assertFalse(evaluation.passed)
        self.assertEqual(evaluation.report.new_members, 1)


if __name__ == "__main__":
    unittest.main()
