"""Generated contract for the Wrong Matches rejection taxonomy."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.wrong_match_policy import (
    WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
    rejection_scenario_is_wrong_match_candidate,
)
from lib.wrong_matches import wrong_match_row_is_visible
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


def assert_wrong_match_row_visibility(
    scenario: str | None,
    request_status: str | None,
    include_replaced: bool,
    *,
    visible: bool,
) -> None:
    """Independent oracle for the full row-level worklist predicate."""
    expected = (
        scenario not in EXPECTED_NON_MATCH_SCENARIOS
        and (include_replaced or request_status != "replaced")
    )
    assert visible is expected


class TestWrongMatchPolicyChecker(unittest.TestCase):
    def test_operator_taxonomy_is_pinned_independently(self) -> None:
        self.assertEqual(
            WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
            EXPECTED_NON_MATCH_SCENARIOS,
        )

    def test_checker_rejects_a_fact_rejection_surfacing(self) -> None:
        with self.assertRaises(AssertionError):
            assert_wrong_match_visibility("nested_layout", visible=True)

    def test_row_checker_rejects_a_missing_scenario_guard(self) -> None:
        with self.assertRaises(AssertionError):
            assert_wrong_match_row_visibility(
                "mixed_source",
                "wanted",
                False,
                visible=True,
            )


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

    @example(
        scenario="audio_corrupt",
        request_status="wanted",
        include_replaced=False,
    )
    @example(
        scenario="bad_audio_hash",
        request_status="manual",
        include_replaced=True,
    )
    @example(
        scenario="nested_layout",
        request_status="replaced",
        include_replaced=True,
    )
    @example(
        scenario="empty_fileset",
        request_status=None,
        include_replaced=False,
    )
    @example(
        scenario="mixed_source",
        request_status="wanted",
        include_replaced=False,
    )
    @example(
        scenario="spectral_reject",
        request_status="wanted",
        include_replaced=True,
    )
    @example(
        scenario="high_distance",
        request_status="replaced",
        include_replaced=False,
    )
    @example(
        scenario=None,
        request_status="replaced",
        include_replaced=True,
    )
    @given(
        scenario=st.one_of(st.none(), st.text(max_size=40)),
        request_status=st.one_of(
            st.none(),
            st.sampled_from(("wanted", "downloading", "manual", "imported", "replaced")),
        ),
        include_replaced=st.booleans(),
    )
    def test_row_visibility_obeys_scenario_and_status_policy(
        self,
        scenario: str | None,
        request_status: str | None,
        include_replaced: bool,
    ) -> None:
        row: dict[str, object] = {
            "request_status": request_status,
            "validation_result": {
                "failed_path": "/failed/generated",
                "scenario": scenario,
            },
        }
        assert_wrong_match_row_visibility(
            scenario,
            request_status,
            include_replaced,
            visible=wrong_match_row_is_visible(
                row,
                include_replaced=include_replaced,
            ),
        )


if __name__ == "__main__":
    unittest.main()
