"""Generated laws for monotone live-world audit debt."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.world_audit_debt import (
    NON_GATING_VIOLATION_CODES,
    assess_world_audit_debt,
    initialize_world_audit_debt_state,
)
from lib.world_audit_service import (
    WorldAuditCounts,
    WorldAuditReport,
    build_world_audit_report,
)
from lib.world_invariants import WorldViolation

#: Codes that must still fail the gate. Deliberately spans buckets A and B so
#: the exemption cannot widen into "bucket B is advisory" without a RED here.
GATING_CODES = ("current_evidence_missing", "current_beets_missing",
                "proof_lock_broken")
NON_GATING_CODES = tuple(sorted(NON_GATING_VIOLATION_CODES))


@dataclass(frozen=True)
class _DebtWorld:
    baseline: tuple[WorldViolation, ...]
    current: tuple[WorldViolation, ...]
    expected_new: int
    expected_changed: int
    expected_pass: bool


def _violation(
    request_id: int,
    code: str,
    *,
    changed: bool = False,
) -> WorldViolation:
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


def _gated(violations: tuple[WorldViolation, ...]) -> tuple[WorldViolation, ...]:
    return tuple(
        item for item in violations
        if item.code not in NON_GATING_VIOLATION_CODES
    )


@st.composite
def _debt_worlds(draw: st.DrawFn) -> _DebtWorld:
    baseline_ids = draw(st.lists(
        st.integers(min_value=1, max_value=10_000),
        min_size=1,
        max_size=12,
        unique=True,
    ))
    codes = draw(st.lists(
        st.sampled_from(GATING_CODES + NON_GATING_CODES),
        min_size=len(baseline_ids),
        max_size=len(baseline_ids),
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
    new_codes = draw(st.lists(
        st.sampled_from(GATING_CODES + NON_GATING_CODES),
        min_size=len(new_ids),
        max_size=len(new_ids),
    ))

    by_id = dict(zip(baseline_ids, codes, strict=True))
    baseline = tuple(
        _violation(request_id, by_id[request_id])
        for request_id in baseline_ids
    )
    current: list[WorldViolation] = []
    changed_gating = 0
    for request_id, action in zip(baseline_ids, actions, strict=True):
        code = by_id[request_id]
        if action == "keep":
            current.append(_violation(request_id, code))
        elif action == "change":
            current.append(_violation(request_id, code, changed=True))
            if code not in NON_GATING_VIOLATION_CODES:
                changed_gating += 1
    current.extend(
        _violation(request_id, code)
        for request_id, code in zip(new_ids, new_codes, strict=True)
    )
    new_gating = sum(
        1 for code in new_codes if code not in NON_GATING_VIOLATION_CODES
    )
    return _DebtWorld(
        baseline=baseline,
        current=tuple(reversed(current)),
        expected_new=new_gating,
        expected_changed=changed_gating,
        expected_pass=not new_gating and changed_gating == 0,
    )


class TestGeneratedWorldAuditDebt(unittest.TestCase):
    @given(_debt_worlds())
    def test_only_an_exact_subset_can_advance_the_known_cohort(
        self,
        world: _DebtWorld,
    ) -> None:
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
                len(_gated(world.current)),
            )
            self.assertEqual(
                evaluation.report.newly_converged,
                len(_gated(world.baseline)) - len(_gated(world.current)),
            )
        else:
            self.assertIsNone(evaluation.next_state)

    @given(_debt_worlds(), st.lists(
        st.integers(min_value=20_001, max_value=30_000),
        max_size=6,
        unique=True,
    ))
    def test_non_gating_violations_never_change_the_verdict(
        self,
        world: _DebtWorld,
        extra_ids: list[int],
    ) -> None:
        """Adding reported-only violations cannot flip pass/fail (#1233).

        This is the load-bearing half of the exemption. A production mutant
        that forgets to filter — or that filters the wrong way and lets a
        non-gating member suppress a gating one — dies here, because the two
        verdicts are compared against each other rather than against a
        constant.
        """
        state = initialize_world_audit_debt_state(_report(world.baseline))
        without = assess_world_audit_debt(state, _report(world.current))

        noisy = world.current + tuple(
            _violation(request_id, NON_GATING_CODES[0])
            for request_id in extra_ids
        )
        with_noise = assess_world_audit_debt(state, _report(noisy))

        self.assertEqual(with_noise.passed, without.passed)
        self.assertEqual(
            with_noise.report.new_members,
            without.report.new_members,
        )
        self.assertEqual(
            with_noise.report.changed_members,
            without.report.changed_members,
        )
        # ...and the noise is still visible rather than silently discarded.
        self.assertEqual(
            with_noise.report.non_gating_violations,
            without.report.non_gating_violations + len(extra_ids),
        )
        self.assertEqual(
            with_noise.report.strict_violations,
            without.report.strict_violations + len(extra_ids),
        )

    def test_count_only_mutant_accepts_a_member_replacement(self) -> None:
        baseline = (
            _violation(1, "current_evidence_missing"),
            _violation(2, "current_evidence_missing"),
        )
        current = (
            _violation(1, "current_evidence_missing"),
            _violation(10_001, "current_evidence_missing"),
        )

        count_only_mutant_passes = len(current) <= len(baseline)

        self.assertTrue(count_only_mutant_passes)
        evaluation = assess_world_audit_debt(
            initialize_world_audit_debt_state(_report(baseline)),
            _report(current),
        )
        self.assertFalse(evaluation.passed)
        self.assertEqual(evaluation.report.new_members, 1)


if __name__ == "__main__":
    unittest.main()
