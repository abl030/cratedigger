"""Generated contract for the Wrong Matches rejection taxonomy."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.wrong_match_policy import (
    WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
    rejection_scenario_is_wrong_match_candidate,
)
from tests.fakes import FakePipelineDB


EXPECTED_NON_MATCH_SCENARIOS = frozenset({
    "audio_corrupt",
    "bad_audio_hash",
    "nested_layout",
    "empty_fileset",
    "mixed_source",
    "spectral_reject",
})


def assert_wrong_match_visibility(
    scenario: str | None,
    *,
    visible: bool,
) -> None:
    """Assert a projected worklist result agrees with the shared taxonomy."""
    expected = scenario not in EXPECTED_NON_MATCH_SCENARIOS
    assert visible is expected
    assert rejection_scenario_is_wrong_match_candidate(scenario) is expected


def _fake_visibility(scenario: str | None) -> bool:
    db = FakePipelineDB()
    db.seed_request({
        "id": 1,
        "artist_name": "Policy",
        "album_title": "Candidate",
        "status": "wanted",
    })
    db.log_download(
        1,
        outcome="rejected",
        validation_result={
            "failed_path": "/failed/policy-candidate",
            "scenario": scenario,
        },
    )
    return bool(db.get_wrong_matches())


class TestWrongMatchPolicyChecker(unittest.TestCase):
    def test_operator_taxonomy_is_pinned_independently(self) -> None:
        self.assertEqual(
            WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
            EXPECTED_NON_MATCH_SCENARIOS,
        )

    def test_checker_rejects_a_fact_rejection_surfacing(self) -> None:
        with self.assertRaises(AssertionError):
            assert_wrong_match_visibility("nested_layout", visible=True)


class TestGeneratedWrongMatchPolicy(unittest.TestCase):
    @example(scenario="audio_corrupt")
    @example(scenario="bad_audio_hash")
    @example(scenario="nested_layout")
    @example(scenario="empty_fileset")
    @example(scenario="mixed_source")
    @example(scenario="spectral_reject")
    @example(scenario="high_distance")
    @example(scenario=None)
    @given(scenario=st.one_of(st.none(), st.text(max_size=40)))
    def test_fake_worklist_obeys_shared_scenario_policy(
        self,
        scenario: str | None,
    ) -> None:
        assert_wrong_match_visibility(
            scenario,
            visible=_fake_visibility(scenario),
        )


if __name__ == "__main__":
    unittest.main()
