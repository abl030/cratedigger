"""Generated copy-policy checks for Recents rejection verdicts."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.test_web_recents import _entry
from web.classify import classify_log_entry


REJECT_SCENARIOS = (
    "quality_downgrade",
    "transcode_downgrade",
    "spectral_reject",
    "lossless_source_locked",
    "suspect_lossless_downgrade",
)


def assert_short_searching_verdict(verdict: str) -> None:
    if "searching continues" not in verdict.lower():
        raise AssertionError("perpetual-search rejection lost its searching marker")
    if any(char.isdigit() for char in verdict):
        raise AssertionError("measurement leaked into short verdict grammar")


def assert_triage_summary_uses_persisted_reject(
    summary: str,
    reason: str,
) -> None:
    expected = reason.replace("_", " ")
    if expected not in summary:
        raise AssertionError("triage summary lost persisted reject reason")
    if "spectral reject" in summary:
        raise AssertionError("non-reject spectral stage became a spectral reject")


class TestGeneratedRejectVerdictGrammar(unittest.TestCase):
    @given(
        scenario=st.sampled_from(REJECT_SCENARIOS),
        incoming=st.integers(min_value=1, max_value=2_000),
        existing=st.integers(min_value=1, max_value=2_000),
    )
    @example(
        scenario="lossless_source_locked",
        incoming=176,
        existing=240,
    )
    def test_measurements_never_change_the_short_decision_class_copy(
        self,
        scenario: str,
        incoming: int,
        existing: int,
    ) -> None:
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario=scenario,
            actual_min_bitrate=incoming,
            existing_min_bitrate=existing,
            spectral_bitrate=incoming,
            existing_spectral_bitrate=existing,
            spectral_grade="suspect",
            v0_probe_avg_bitrate=incoming,
            existing_v0_probe_avg_bitrate=existing,
        ))
        assert_short_searching_verdict(result.verdict)

    def test_checker_rejects_the_old_measurement_heavy_grammar(self) -> None:
        with self.assertRaisesRegex(AssertionError, "measurement leaked"):
            assert_short_searching_verdict(
                "176kbps is not better than existing 240kbps; searching continues",
            )

    @given(
        reason=st.sampled_from((
            "downgrade",
            "suspect_lossless_downgrade",
            "lossless_source_locked",
        )),
        stage=st.sampled_from((
            "stage0_spectral_gate:import",
            "stage1_spectral:skipped_vbr_high_avg",
            "stage1_spectral:import",
        )),
    )
    @example(
        reason="suspect_lossless_downgrade",
        stage="stage1_spectral:skipped_vbr_high_avg",
    )
    def test_triage_copy_uses_persisted_reason_not_stage_name(
        self,
        reason: str,
        stage: str,
    ) -> None:
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result={
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "outcome": "deleted",
                    "reason": reason,
                    "preview_verdict": "confident_reject",
                    "preview_decision": reason,
                    "stage_chain": [stage, f"stage2_import:{reason}"],
                },
            },
        ))
        self.assertEqual(result.badge, "Triaged · deleted")
        assert_triage_summary_uses_persisted_reject(
            result.wrong_match_triage_summary or "", reason)

    def test_triage_checker_rejects_stage_inferred_copy(self) -> None:
        with self.assertRaisesRegex(AssertionError, "persisted reject reason"):
            assert_triage_summary_uses_persisted_reject(
                "deleted: spectral reject", "suspect_lossless_downgrade")


if __name__ == "__main__":
    unittest.main()
