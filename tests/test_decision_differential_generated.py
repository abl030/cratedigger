"""Generated coverage for native decision-differential current pairing.

The corpus is a graph, not an ordered pairing stream: every candidate's
``current_evidence_id`` must resolve to the evidence row with that exact ID,
regardless of which export batch emitted either row.  The deterministic pins
cover malformed, duplicate, and dangling references; this property patrols
the valid graph's order independence through the production resolver.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from scripts.decision_differential import (
    DecisionCorpusEvidence,
    resolve_native_current_pairs,
)


@dataclass(frozen=True)
class NativePairingWorld:
    entries: tuple[DecisionCorpusEvidence, ...]
    permuted_entries: tuple[DecisionCorpusEvidence, ...]


@st.composite
def native_pairing_worlds(draw) -> NativePairingWorld:
    evidence_ids = draw(st.lists(
        st.integers(min_value=1, max_value=100),
        min_size=1,
        max_size=8,
        unique=True,
    ))
    candidate_ids = set(draw(st.lists(
        st.sampled_from(evidence_ids),
        min_size=1,
        max_size=len(evidence_ids),
        unique=True,
    )))
    entries = tuple(
        DecisionCorpusEvidence(
            evidence_id=evidence_id,
            is_candidate=evidence_id in candidate_ids,
            current_evidence_id=(
                draw(st.sampled_from([None, *evidence_ids]))
                if evidence_id in candidate_ids else None
            ),
            row={"id": evidence_id},
        )
        for evidence_id in evidence_ids
    )
    return NativePairingWorld(
        entries=entries,
        permuted_entries=tuple(draw(st.permutations(entries))),
    )


def _resolved_ids(
    entries: tuple[DecisionCorpusEvidence, ...],
) -> dict[int, int | None]:
    return {
        candidate.evidence_id: (
            current.evidence_id if current is not None else None
        )
        for candidate, current in resolve_native_current_pairs(entries)
    }


class TestNativeCurrentPairingGenerated(unittest.TestCase):
    @given(world=native_pairing_worlds())
    def test_current_references_resolve_by_exact_id_not_export_order(
        self,
        world: NativePairingWorld,
    ) -> None:
        expected = {
            entry.evidence_id: entry.current_evidence_id
            for entry in world.entries
            if entry.is_candidate
        }
        self.assertEqual(_resolved_ids(world.entries), expected)
        self.assertEqual(_resolved_ids(world.permuted_entries), expected)


if __name__ == "__main__":
    unittest.main()
