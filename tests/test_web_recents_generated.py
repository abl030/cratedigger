"""Generated copy-policy checks for Recents rejection verdicts."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.test_web_recents import _entry
from web.classify import classify_log_entry
from web.routes.pipeline import (
    _project_current_library_have,
    _project_linked_import_evidence,
)


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


def assert_triaged_rejection_style(
    action: str,
    badge: str,
    badge_class: str,
    border_color: str,
) -> None:
    if border_color != "#a33":
        raise AssertionError("triaged rejection lost its rejected row border")
    if action.startswith("deleted_"):
        if badge != "Triaged · deleted" or badge_class != "badge-rejected":
            raise AssertionError("triaged deletion was styled as a successful library outcome")
    elif action.startswith("kept_"):
        if badge != "Triaged · kept" or badge_class != "badge-warn":
            raise AssertionError("kept triage lost its primary amber badge")
    elif badge_class != "badge-rejected":
        raise AssertionError("triaged rejection lost its rejected badge")


def assert_current_library_have_is_projected(
    item: dict[str, object],
    *,
    expected_format: str | None,
    expected_min: int | None,
    expected_avg: int | None,
    expected_median: int | None,
) -> None:
    if item.get("existing_format") != expected_format:
        raise AssertionError("current library format did not populate compact HAVE")
    if item.get("existing_min_bitrate") != expected_min:
        raise AssertionError("current library minimum did not populate compact HAVE")
    if item.get("existing_avg_bitrate") != expected_avg:
        raise AssertionError("current library average did not populate compact HAVE")
    if item.get("existing_median_bitrate") != expected_median:
        raise AssertionError("current library median did not populate compact HAVE")


def assert_mutating_attempt_has_no_projected_have(item: dict[str, object]) -> None:
    if any(item.get(field) is not None for field in (
        "existing_format",
        "existing_min_bitrate",
        "existing_avg_bitrate",
        "existing_median_bitrate",
    )):
        raise AssertionError("post-import current state leaked into attempt HAVE")


def assert_only_explicit_source_receives_materialized_output(
    items: list[dict[str, object]],
    *,
    source_id: int,
    unrelated_id: int,
    expected_format: str,
) -> None:
    by_id = {item["id"]: item for item in items}
    if by_id[source_id].get("materialized_format") != expected_format:
        raise AssertionError("explicit source row missed its linked output")
    if by_id[unrelated_id].get("materialized_format") is not None:
        raise AssertionError("unrelated same-release row received inferred output")


def assert_verified_lossless_upgrade_copy_is_concise(verdict: str) -> None:
    if "Equivalent:" in verdict or "both transparent" in verdict:
        raise AssertionError("internal comparison trace leaked into upgrade copy")
    if not verdict.startswith("Upgrade: "):
        raise AssertionError("verified-lossless import lost upgrade grammar")
    if "verified lossless" not in verdict:
        raise AssertionError("verified-lossless reason disappeared")


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
        assert_triaged_rejection_style(
            "deleted_reject",
            result.badge,
            result.badge_class,
            result.border_color,
        )
        assert_triage_summary_uses_persisted_reject(
            result.wrong_match_triage_summary or "", reason)

    def test_triage_checker_rejects_stage_inferred_copy(self) -> None:
        with self.assertRaisesRegex(AssertionError, "persisted reject reason"):
            assert_triage_summary_uses_persisted_reject(
                "deleted: spectral reject", "suspect_lossless_downgrade")

    def test_triage_style_checker_rejects_the_old_success_style(self) -> None:
        with self.assertRaisesRegex(AssertionError, "rejected row border"):
            assert_triaged_rejection_style(
                "deleted_reject",
                "Triaged · deleted",
                "badge-library",
                "#6a5",
            )

    def test_triage_style_checker_rejects_the_old_secondary_kept_label(self) -> None:
        with self.assertRaisesRegex(AssertionError, "primary amber badge"):
            assert_triaged_rejection_style(
                "kept_would_import",
                "Rejected",
                "badge-rejected",
                "#a33",
            )

    @given(action=st.sampled_from((
        "deleted_reject",
        "deleted_verified_lossless_parent",
        "kept_would_import",
        "kept_uncertain",
        "skipped_current_evidence_missing",
    )))
    @example(action="kept_would_import")
    def test_every_triaged_rejection_stays_red(self, action: str) -> None:
        result = classify_log_entry(_entry(
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result={
                "wrong_match_triage": {
                    "action": action,
                    "outcome": action,
                    "reason": "import",
                    "preview_verdict": "would_import",
                    "preview_decision": "import",
                    "stage_chain": ["stage2_import:import"],
                },
            },
        ))
        assert_triaged_rejection_style(
            action,
            result.badge,
            result.badge_class,
            result.border_color,
        )

    @given(
        existing_format=st.one_of(st.none(), st.sampled_from(("MP3", "Opus"))),
        existing_min=st.one_of(st.none(), st.integers(min_value=1, max_value=2_000)),
        has_attempt_spectral=st.booleans(),
        current_format=st.sampled_from(("MP3", "Opus", "FLAC")),
        current_min=st.integers(min_value=1, max_value=2_000),
    )
    @example(
        existing_format=None,
        existing_min=None,
        has_attempt_spectral=False,
        current_format="Opus",
        current_min=93,
    )
    @example(
        existing_format=None,
        existing_min=None,
        has_attempt_spectral=True,
        current_format="Opus",
        current_min=99,
    )
    def test_unproven_current_library_never_backfills_attempt_have(
        self,
        existing_format: str | None,
        existing_min: int | None,
        has_attempt_spectral: bool,
        current_format: str,
        current_min: int,
    ) -> None:
        item: dict[str, object] = {
            "existing_format": existing_format,
            "existing_min_bitrate": existing_min,
            "existing_spectral_grade": (
                "likely_transcode" if has_attempt_spectral else None
            ),
            "existing_spectral_bitrate": 160 if has_attempt_spectral else None,
        }
        _project_current_library_have(item, {}, {
            "beets_format": current_format,
            "beets_bitrate": current_min,
            "beets_avg_bitrate": current_min + 20,
        })

        assert_current_library_have_is_projected(
            item,
            expected_format=existing_format,
            expected_min=existing_min,
            expected_avg=None,
            expected_median=None,
        )

    @given(
        current_format=st.sampled_from(("MP3", "Opus", "FLAC")),
        current_min=st.integers(min_value=1, max_value=2_000),
        current_avg=st.integers(min_value=1, max_value=2_000),
        current_median=st.integers(min_value=1, max_value=2_000),
        current_spectral=st.one_of(
            st.none(), st.sampled_from(("genuine", "likely_transcode", "suspect"))
        ),
        current_v0=st.one_of(
            st.none(), st.integers(min_value=1, max_value=2_000)
        ),
    )
    @example(
        current_format="Opus",
        current_min=93,
        current_avg=129,
        current_median=128,
        current_spectral="suspect",
        current_v0=256,
    )
    def test_canonical_current_evidence_is_one_complete_have_snapshot(
        self,
        current_format: str,
        current_min: int,
        current_avg: int,
        current_median: int,
        current_spectral: str | None,
        current_v0: int | None,
    ) -> None:
        item: dict[str, object] = {
            "existing_format": None,
            "existing_min_bitrate": None,
            "existing_spectral_grade": None,
            "existing_spectral_bitrate": None,
            "existing_v0_probe_kind": None,
            "existing_v0_probe_min_bitrate": None,
            "existing_v0_probe_avg_bitrate": None,
            "existing_v0_probe_median_bitrate": None,
        }
        row: dict[str, object] = {
            "_current_evidence_id": 42,
            "_current_evidence_is_pre_attempt": True,
            "_current_evidence_format": current_format,
            "_current_evidence_min_bitrate": current_min,
            "_current_evidence_avg_bitrate": current_avg,
            "_current_evidence_median_bitrate": current_median,
            "_current_evidence_spectral_grade": current_spectral,
            "_current_evidence_spectral_bitrate": (
                96 if current_spectral is not None else None
            ),
            "_current_evidence_v0_probe_kind": (
                "lossless_source" if current_v0 is not None else None
            ),
            "_current_evidence_v0_probe_min_bitrate": (
                current_v0 - 1 if current_v0 is not None else None
            ),
            "_current_evidence_v0_probe_avg_bitrate": current_v0,
            "_current_evidence_v0_probe_median_bitrate": current_v0,
        }
        _project_current_library_have(item, row, {
            "beets_format": "conflicting-beets-format",
            "beets_bitrate": current_min + 100,
        })

        assert_current_library_have_is_projected(
            item,
            expected_format=current_format,
            expected_min=current_min,
            expected_avg=current_avg,
            expected_median=current_median,
        )
        self.assertEqual(item["existing_spectral_grade"], current_spectral)
        self.assertEqual(item["existing_v0_probe_avg_bitrate"], current_v0)

    @given(
        is_pre_attempt=st.booleans(),
        current_format=st.sampled_from(("MP3", "Opus", "FLAC")),
        current_min=st.integers(min_value=1, max_value=2_000),
    )
    @example(
        is_pre_attempt=False,
        current_format="Opus",
        current_min=117,
    )
    def test_current_overlay_requires_pre_attempt_timestamp(
        self,
        is_pre_attempt: bool,
        current_format: str,
        current_min: int,
    ) -> None:
        item: dict[str, object] = {
            "outcome": "rejected",
            "existing_format": None,
            "existing_min_bitrate": None,
            "existing_avg_bitrate": None,
            "existing_median_bitrate": None,
        }
        _project_current_library_have(item, {
            "_current_evidence_id": 42,
            "_current_evidence_is_pre_attempt": is_pre_attempt,
            "_current_evidence_format": current_format,
            "_current_evidence_min_bitrate": current_min,
            "_current_evidence_avg_bitrate": current_min + 10,
            "_current_evidence_median_bitrate": current_min + 5,
        }, {})
        if is_pre_attempt:
            self.assertEqual(item["existing_format"], current_format)
            self.assertEqual(item["existing_min_bitrate"], current_min)
        else:
            assert_mutating_attempt_has_no_projected_have(item)

    @given(
        outcome=st.sampled_from(("success", "force_import", "manual_import")),
        current_format=st.sampled_from(("MP3", "Opus", "FLAC")),
        current_min=st.integers(min_value=1, max_value=2_000),
        current_avg=st.integers(min_value=1, max_value=2_000),
    )
    @example(
        outcome="success",
        current_format="Opus",
        current_min=117,
        current_avg=131,
    )
    def test_mutating_attempt_never_projects_current_state_into_have(
        self,
        outcome: str,
        current_format: str,
        current_min: int,
        current_avg: int,
    ) -> None:
        item: dict[str, object] = {
            "outcome": outcome,
            "existing_format": None,
            "existing_min_bitrate": None,
            "existing_avg_bitrate": None,
            "existing_median_bitrate": None,
        }
        _project_current_library_have(item, {
            "_current_evidence_id": 42,
            "_current_evidence_is_pre_attempt": True,
            "_current_evidence_format": current_format,
            "_current_evidence_min_bitrate": current_min,
            "_current_evidence_avg_bitrate": current_avg,
            "_current_evidence_median_bitrate": current_avg,
        }, {
            "beets_format": current_format,
            "beets_bitrate": current_min,
            "beets_avg_bitrate": current_avg,
        })
        assert_mutating_attempt_has_no_projected_have(item)

    def test_mutating_have_checker_rejects_post_import_projection(self) -> None:
        with self.assertRaisesRegex(AssertionError, "post-import current state"):
            assert_mutating_attempt_has_no_projected_have({
                "existing_format": "Opus",
                "existing_min_bitrate": 117,
                "existing_avg_bitrate": 131,
            })

    def test_have_projection_checker_rejects_the_old_route_shape(self) -> None:
        with self.assertRaisesRegex(AssertionError, "current library format"):
            assert_current_library_have_is_projected(
                {
                    "existing_format": None,
                    "existing_min_bitrate": None,
                    "beets_format": "Opus",
                    "beets_bitrate": 93,
                },
                expected_format="Opus",
                expected_min=93,
                expected_avg=129,
                expected_median=128,
            )

    def test_linked_output_checker_rejects_album_inference(self) -> None:
        with self.assertRaisesRegex(AssertionError, "unrelated same-release"):
            assert_only_explicit_source_receives_materialized_output(
                [
                    {"id": 1, "materialized_format": "Opus"},
                    {"id": 2, "materialized_format": "Opus"},
                ],
                source_id=1,
                unrelated_id=2,
                expected_format="Opus",
            )

    def test_linked_output_checker_rejects_filter_dependent_projection(
        self,
    ) -> None:
        with self.assertRaisesRegex(AssertionError, "explicit source row missed"):
            assert_only_explicit_source_receives_materialized_output(
                [
                    {"id": 1, "materialized_format": None},
                    {"id": 2, "materialized_format": None},
                ],
                source_id=1,
                unrelated_id=2,
                expected_format="Opus",
            )

    def test_verified_lossless_copy_checker_rejects_internal_trace(self) -> None:
        with self.assertRaisesRegex(AssertionError, "internal comparison trace"):
            assert_verified_lossless_upgrade_copy_is_concise(
                "Equivalent: OPUS 128 vs MP3 — both transparent — "
                "imported: verified lossless",
            )

    @given(
        existing_min=st.integers(min_value=1, max_value=2_000),
        output_min=st.integers(min_value=1, max_value=2_000),
        existing_avg=st.integers(min_value=1, max_value=2_000),
        target=st.sampled_from(("opus 128", "mp3 v0")),
    )
    @example(
        existing_min=320,
        output_min=127,
        existing_avg=320,
        target="opus 128",
    )
    def test_verified_lossless_bypass_uses_concise_upgrade_copy(
        self,
        existing_min: int,
        output_min: int,
        existing_avg: int,
        target: str,
    ) -> None:
        actual_format = "opus" if target == "opus 128" else "mp3"
        result = classify_log_entry(_entry(
            outcome="success",
            was_converted=True,
            original_filetype="flac",
            actual_filetype=actual_format,
            actual_min_bitrate=output_min,
            existing_min_bitrate=existing_min,
            spectral_grade="genuine",
            import_result={
                "version": 2,
                "decision": "import",
                "comparison_basis": {
                    "verdict": "equivalent",
                    "branch": "cross_family_same_rank",
                    "new_rank": "transparent",
                    "existing_rank": "transparent",
                    "new_metric": "contract",
                    "existing_metric": "avg",
                    "new_value_kbps": 128,
                    "existing_value_kbps": existing_avg,
                    "new_format": target,
                    "existing_format": "mp3",
                    "spectral_clamped": False,
                    "tolerance_kbps": None,
                    "verified_lossless_bypass": True,
                },
            },
        ))
        assert_verified_lossless_upgrade_copy_is_concise(result.verdict)

    @given(
        source_id=st.integers(min_value=1, max_value=1_000),
        unrelated_offset=st.integers(min_value=1, max_value=1_000),
        outcome=st.sampled_from(("success", "force_import", "manual_import")),
        materialized_format=st.sampled_from(("Opus", "MP3", "FLAC")),
        materialized_min=st.integers(min_value=1, max_value=2_000),
        materialized_avg=st.integers(min_value=1, max_value=2_000),
    )
    @example(
        source_id=37120,
        unrelated_offset=8,
        outcome="force_import",
        materialized_format="Opus",
        materialized_min=118,
        materialized_avg=124,
    )
    def test_linked_materialized_output_follows_only_explicit_source_id(
        self,
        source_id: int,
        unrelated_offset: int,
        outcome: str,
        materialized_format: str,
        materialized_min: int,
        materialized_avg: int,
    ) -> None:
        unrelated_id = source_id + unrelated_offset
        successor_id = unrelated_id + 1
        items: list[dict[str, object]] = [
            {"id": source_id, "request_id": 42, "materialized_format": None},
            {"id": unrelated_id, "request_id": 42, "materialized_format": None},
        ]
        linked_successor = {
            "id": successor_id,
            "request_id": 42,
            "outcome": outcome,
            "source_download_log_id": source_id,
            "materialized_format": materialized_format,
            "materialized_min_bitrate": materialized_min,
            "materialized_avg_bitrate": materialized_avg,
        }
        _project_linked_import_evidence(items, [linked_successor])
        assert_only_explicit_source_receives_materialized_output(
            items,
            source_id=source_id,
            unrelated_id=unrelated_id,
            expected_format=materialized_format,
        )
        self.assertEqual(items[0]["materialized_min_bitrate"], materialized_min)
        self.assertEqual(items[0]["materialized_avg_bitrate"], materialized_avg)


if __name__ == "__main__":
    unittest.main()
