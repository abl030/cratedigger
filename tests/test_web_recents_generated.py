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


if __name__ == "__main__":
    unittest.main()
