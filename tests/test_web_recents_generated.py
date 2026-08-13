"""Generated copy-policy checks for Recents rejection verdicts."""

from __future__ import annotations

import unittest
from typing import Protocol

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.pipeline_db.download_log import _DownloadLogMixin
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    QualityComparisonBasis,
    SpectralAnalysisDetail,
    SpectralDetail,
    V0ProbeEvidence,
)
from lib.quality.decisions import (
    AacLatticeProofLeg,
    UltrasonicProofLeg,
    mint_verified_lossless_proof,
)
from tests.test_web_recents import _entry
from web.classify import ClassifiedEntry, LogEntry, classify_log_entry
from web.download_history_view import (
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


class _CompositeTriageProjection(Protocol):
    verdict: str
    badge: str
    badge_class: str
    border_color: str
    summary: str
    actual_min_bitrate: int | None
    source_min_bitrate: int | None
    source_avg_bitrate: int | None
    source_median_bitrate: int | None
    spectral_grade: str | None
    spectral_bitrate: int | None
    v0_probe_kind: str | None
    v0_probe_min_bitrate: int | None
    v0_probe_avg_bitrate: int | None
    v0_probe_median_bitrate: int | None
    comparison_basis: dict[str, object] | None
    downloaded_label: str
    target_contract_format: str | None
    materialized_format: str | None
    materialized_min_bitrate: int | None
    materialized_avg_bitrate: int | None
    materialized_median_bitrate: int | None
    existing_format: str | None
    existing_min_bitrate: int | None
    existing_avg_bitrate: int | None


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
        if (badge != "Triaged · download deleted"
                or badge_class != "badge-rejected"):
            raise AssertionError("triaged deletion was styled as a successful library outcome")
    elif action.startswith("kept_"):
        if badge != "Triaged · download kept" or badge_class != "badge-warn":
            raise AssertionError("kept triage lost its primary amber badge")
    elif badge_class != "badge-rejected":
        raise AssertionError("triaged rejection lost its rejected badge")


def assert_composite_triage_projection(
    result: _CompositeTriageProjection,
    *,
    action: str,
    reason: str,
    uploader: str,
    distance: float,
    has_have: bool,
) -> None:
    """Check the compact contract for a persisted cleanup audit.

    This intentionally patrols the server-owned classifier rather than the
    Recents renderer: the exact same projection feeds list and history rows.
    """
    expected_object = (
        "download deleted" if action.startswith("deleted_") else "download kept"
    )
    expected_reason = reason.replace("_", " ")
    expected_verdict = f"Wrong match (dist {distance:.3f})"
    if result.verdict != expected_verdict:
        raise AssertionError("cleanup replaced the original match verdict")
    if expected_object not in result.badge:
        raise AssertionError("cleanup badge did not name the download object")
    if expected_object not in result.summary or expected_reason not in result.summary:
        raise AssertionError("compact summary hid cleanup disposition or reason")
    if uploader not in result.summary:
        raise AssertionError("compact summary lost uploader provenance")
    assert_triaged_rejection_style(
        action, result.badge, result.badge_class, result.border_color)
    if reason == "audio_corrupt":
        assert_corrupt_candidate_display_is_codec_only(result, has_have=has_have)


def assert_corrupt_candidate_display_is_codec_only(
    result: _CompositeTriageProjection,
    *,
    has_have: bool,
) -> None:
    """A corrupt candidate may retain its codec, never quality claims."""
    if any((
        result.actual_min_bitrate is not None,
        result.source_min_bitrate is not None,
        result.source_avg_bitrate is not None,
        result.source_median_bitrate is not None,
        result.spectral_grade is not None,
        result.spectral_bitrate is not None,
        result.v0_probe_kind is not None,
        result.v0_probe_min_bitrate is not None,
        result.v0_probe_avg_bitrate is not None,
        result.v0_probe_median_bitrate is not None,
        result.comparison_basis is not None,
        result.target_contract_format is not None,
        result.materialized_format is not None,
        result.materialized_min_bitrate is not None,
        result.materialized_avg_bitrate is not None,
        result.materialized_median_bitrate is not None,
    )):
        raise AssertionError("corrupt input leaked a candidate quality claim")
    if result.downloaded_label != "FLAC":
        raise AssertionError("corrupt input leaked a tier or conversion label")
    if has_have:
        if result.existing_format != "MP3" or result.existing_min_bitrate != 192:
            raise AssertionError("corrupt projection erased point-in-time HAVE")
        if result.existing_avg_bitrate != 224:
            raise AssertionError("corrupt projection changed point-in-time HAVE")


def _corrupt_import_result(
    decision: str | None,
    *,
    has_have: bool = False,
    typed_source: bool = True,
) -> str:
    """Full candidate quality world used to prove every display fallback dies."""
    basis = msgspec.convert({
        "verdict": "worse", "branch": "rank",
        "new_rank": "transparent", "existing_rank": "acceptable",
        "new_metric": "avg", "existing_metric": "avg",
        "new_value_kbps": 0, "existing_value_kbps": 224,
        "new_format": "FLAC", "existing_format": "MP3",
        "spectral_clamped": False, "tolerance_kbps": None,
        "verified_lossless_bypass": False,
    }, type=QualityComparisonBasis)
    return ImportResult(
        decision=decision,
        source_measurement=(
            AudioQualityMeasurement(
                min_bitrate_kbps=0, avg_bitrate_kbps=0,
                median_bitrate_kbps=0, format="FLAC",
            ) if typed_source else None
        ),
        current_measurement=(
            AudioQualityMeasurement(
                min_bitrate_kbps=192, avg_bitrate_kbps=224, format="MP3",
            ) if has_have else None
        ),
        materialized_measurement=AudioQualityMeasurement(
            min_bitrate_kbps=118, avg_bitrate_kbps=124,
            median_bitrate_kbps=122, format="Opus",
        ),
        spectral=SpectralDetail(candidate=SpectralAnalysisDetail(
            attempted=True, grade="genuine", bitrate_kbps=320,
        )),
        v0_probe=V0ProbeEvidence(
            kind="lossless_source_v0", min_bitrate_kbps=165,
            avg_bitrate_kbps=171, median_bitrate_kbps=170,
        ),
        comparison_basis=basis,
    ).to_json()


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


def assert_complete_have_snapshot_is_selected(
    item: dict[str, object],
    *,
    expected_format: str,
    expected_min: int,
    expected_avg: int,
    expected_median: int,
    expected_spectral: str | None,
    expected_v0: int | None,
) -> None:
    """Require one canonical HAVE snapshot, never a partial-field blend."""
    assert_current_library_have_is_projected(
        item,
        expected_format=expected_format,
        expected_min=expected_min,
        expected_avg=expected_avg,
        expected_median=expected_median,
    )
    if item.get("existing_spectral_grade") != expected_spectral:
        raise AssertionError("partial attempt spectral leaked into canonical HAVE")
    if item.get("existing_v0_probe_avg_bitrate") != expected_v0:
        raise AssertionError("partial attempt V0 leaked into canonical HAVE")
    if item.get("existing_spectral_attempted") is True:
        raise AssertionError("partial attempt failure leaked into canonical HAVE")
    if item.get("existing_spectral_error") is not None:
        raise AssertionError("partial attempt error leaked into canonical HAVE")


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


def assert_verified_lossless_proof_upgrade_names_basis(
    verdict: str,
    basis: QualityComparisonBasis,
) -> None:
    if not verdict.startswith("Proof upgrade: equivalent quality — "):
        raise AssertionError("verified-lossless import lost proof-upgrade grammar")
    if basis.new_format is None or basis.existing_format is None:
        raise AssertionError("proof-upgrade basis is missing a format")
    new_format = basis.new_format.upper()
    existing_format = basis.existing_format.upper()
    if f"{new_format} vs {existing_format}" not in verdict:
        raise AssertionError("proof upgrade lost the persisted format comparison")
    if (
        "MP3" in verdict
        and not any(
            "MP3" in format_name
            for format_name in (new_format, existing_format)
        )
    ):
        raise AssertionError("proof upgrade invented MP3 outside the basis")
    if "verified lossless" not in verdict:
        raise AssertionError("verified-lossless reason disappeared")


def minted_classifiers() -> tuple[str, ...]:
    """Every classifier the real minting policy can write (Rule C).

    Derived by driving ``mint_verified_lossless_proof`` over its three
    adjudicating leg combinations rather than restating its literals, so a
    new proof generation enters this property the moment the producer can
    mint one.
    """
    ultrasonic = UltrasonicProofLeg(
        outcome="passed", reason="deficit_below_threshold")
    lattice = AacLatticeProofLeg(outcome="passed", reason="adjudicated_clean")
    minted: list[str] = []
    for ultrasonic_leg, lattice_leg in (
        (None, None),
        (ultrasonic, None),
        (ultrasonic, lattice),
    ):
        proof = mint_verified_lossless_proof(
            True,
            was_converted_from="flac",
            detected_source_format="flac",
            spectral_grade="genuine",
            ultrasonic_leg=ultrasonic_leg,
            aac_lattice_leg=lattice_leg,
        )
        if proof is None:
            raise AssertionError("minting policy refused an adjudicated world")
        minted.append(proof.classifier)
    return tuple(dict.fromkeys(minted))


MINTED_CLASSIFIERS = minted_classifiers()


def assert_verified_lossless_claim_is_minted(
    verdict: str,
    summary: str,
    *,
    classifier: str | None,
    basis_bypass: bool,
) -> None:
    """The phrase is a report of a minted proof, never a re-derivation.

    ``classifier`` is the proof that actually REACHED the render path:
    ``album_quality_evidence.verified_lossless_classifier`` once the read
    seam's source-semantic gate has had its say, or the one this attempt's
    own ``ImportResult`` persisted. ``basis_bypass`` is the same decision
    persisted on the comparison basis. Those are the complete set of things
    that may put the phrase in front of an operator; container, conversion
    and spectral columns are what the decider CONSIDERED, not what it
    concluded, and a legacy-lineage evidence row's proof was minted from
    another attempt's bytes.
    """
    proved = classifier is not None or basis_bypass
    if proved:
        return
    for field_name, text in (("verdict", verdict), ("summary", summary)):
        if "verified lossless" in text.lower():
            raise AssertionError(
                f"{field_name} claims verified lossless with no minted "
                f"proof: {text!r}"
            )


#: A real v3 ``import_result`` blob whose ``source_measurement`` carries the
#: era's ``verified_lossless`` flag (live dl 36970 req 2937). Production's own
#: legacy reader projects it into ``ImportResult.verified_lossless_proof``, so
#: this is a row that genuinely HAS a persisted proof object — and must still
#: not move the verdict, because the projection stamps the same
#: ``legacy_import_result`` classifier on the v1/v2 heuristic flag.
_V3_PERSISTED_PROOF_BLOB: dict[str, object] = {
    "version": 3,
    "decision": "import",
    "source_measurement": {
        "format": "FLAC",
        "min_bitrate_kbps": 742,
        "verified_lossless": True,
    },
}


def _raw_download_log_row(
    *,
    lineage: int | None,
    classifier: str | None,
    **columns: object,
) -> dict[str, object]:
    """One joined ``download_log`` row exactly as ``get_log`` hands it over.

    The candidate-evidence aliases are the join's own names — the shape the
    read seam receives BEFORE ``_overlay_evidence_onto_download_log_row``
    adjudicates them.
    """
    entry = _entry(outcome="success", beets_scenario="strong_match", **columns)
    return {
        **entry.to_json_dict(),
        "verified_lossless_classifier": None,
        "_request_mb_release_id": "generated-recents-release",
        "_evidence_mb_release_id": "generated-recents-release",
        "_evidence_lineage_version": lineage,
        "_evidence_verified_lossless_classifier": classifier,
    }


def render_through_the_read_seam(row: dict[str, object]) -> ClassifiedEntry:
    """Raw joined row → production overlay → ``LogEntry`` → verdict."""
    overlaid = _DownloadLogMixin._overlay_evidence_onto_download_log_row(
        dict(row))
    return classify_log_entry(LogEntry.from_row(overlaid))


def assert_minted_proof_is_reported(verdict: str) -> None:
    """The must-still-work half: a proved row still says so.

    Verdict only. The collapsed-card summary for a new import (badge
    "Imported") is deliberately its own short format label rather than the
    verdict, and has never carried the phrase; requiring it there would be
    the checker inventing policy instead of patrolling it.
    """
    if "verified lossless" not in verdict.lower():
        raise AssertionError(
            f"verdict dropped a minted verified-lossless proof: {verdict!r}"
        )


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

    def test_triage_copy_uses_persisted_reason_not_stage_name(self) -> None:
        reasons = (
            "downgrade",
            "suspect_lossless_downgrade",
            "lossless_source_locked",
        )
        stages = (
            "stage0_spectral_gate:import",
            "stage1_spectral:skipped_vbr_high_avg",
            "stage1_spectral:import",
        )
        for reason in reasons:
            for stage in stages:
                with self.subTest(reason=reason, stage=stage):
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
                                "stage_chain": [
                                    stage,
                                    f"stage2_import:{reason}",
                                ],
                            },
                        },
                    ))
                    self.assertEqual(
                        result.badge,
                        "Triaged · download deleted",
                    )
                    assert_triaged_rejection_style(
                        "deleted_reject",
                        result.badge,
                        result.badge_class,
                        result.border_color,
                    )
                    assert_triage_summary_uses_persisted_reject(
                        result.wrong_match_triage_summary or "",
                        reason,
                    )

    def test_triage_checker_rejects_stage_inferred_copy(self) -> None:
        with self.assertRaisesRegex(AssertionError, "persisted reject reason"):
            assert_triage_summary_uses_persisted_reject(
                "deleted: spectral reject", "suspect_lossless_downgrade")

    def test_triage_style_checker_rejects_the_old_success_style(self) -> None:
        with self.assertRaisesRegex(AssertionError, "rejected row border"):
            assert_triaged_rejection_style(
                "deleted_reject",
                "Triaged · download deleted",
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

    @given(
        action_reason=st.sampled_from((
            ("deleted_reject", "audio_corrupt"),
            ("deleted_reject", "spectral_reject"),
            ("deleted_reject", "downgrade"),
            ("kept_would_import", "import"),
        )),
        uploader=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
            min_size=1,
            max_size=12,
        ),
        distance=st.floats(min_value=0.151, max_value=0.999,
                           allow_nan=False, allow_infinity=False),
        has_have=st.booleans(),
    )
    @example(
        action_reason=("deleted_reject", "audio_corrupt"), uploader="Korveck",
        distance=0.181, has_have=False,
    )
    def test_composite_triage_keeps_match_and_cleanup_facts(
        self,
        action_reason: tuple[str, str],
        uploader: str,
        distance: float,
        has_have: bool,
    ) -> None:
        action, reason = action_reason
        current = ({
            "format": "MP3", "min_bitrate_kbps": 192,
            "avg_bitrate_kbps": 224,
        } if has_have else None)
        audit: dict[str, object] = {
            "action": action,
            "outcome": "deleted" if action.startswith("deleted_") else "kept",
            "reason": reason,
            "preview_verdict": "confident_reject",
            "preview_decision": reason,
            "stage_chain": [f"stage2_import:{reason}"],
            "candidate_measurement": {
                "format": "FLAC", "min_bitrate_kbps": 0,
                "avg_bitrate_kbps": 0, "spectral_grade": "genuine",
            },
        }
        if current is not None:
            audit["current_measurement"] = current
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="high_distance",
            beets_distance=distance, soulseek_username=uploader,
            actual_min_bitrate=0,
            validation_result={"wrong_match_triage": audit},
        ))
        assert_composite_triage_projection(
            result, action=action, reason=reason, uploader=uploader,
            distance=distance, has_have=has_have,
        )

    def test_composite_triage_checker_rejects_hidden_cleanup_reason(self) -> None:
        class OldProjection:
            verdict: str = "Wrong match (dist 0.181)"
            badge: str = "Triaged · download deleted"
            badge_class: str = "badge-rejected"
            border_color: str = "#a33"
            summary: str = "Wrong match (dist 0.181) · Korveck"
            actual_min_bitrate: int | None = None
            source_min_bitrate: int | None = None
            source_avg_bitrate: int | None = None
            source_median_bitrate: int | None = None
            spectral_grade: str | None = None
            spectral_bitrate: int | None = None
            v0_probe_kind: str | None = None
            v0_probe_min_bitrate: int | None = None
            v0_probe_avg_bitrate: int | None = None
            v0_probe_median_bitrate: int | None = None
            comparison_basis: dict[str, object] | None = None
            downloaded_label: str = ""
            target_contract_format: str | None = None
            materialized_format: str | None = None
            materialized_min_bitrate: int | None = None
            materialized_avg_bitrate: int | None = None
            materialized_median_bitrate: int | None = None
            existing_format: str | None = None
            existing_min_bitrate: int | None = None
            existing_avg_bitrate: int | None = None

        with self.assertRaisesRegex(AssertionError, "hid cleanup"):
            assert_composite_triage_projection(
                OldProjection(), action="deleted_reject", reason="spectral_reject",
                uploader="Korveck", distance=0.181, has_have=False,
            )

    def test_corrupt_candidate_projection_clears_all_quality_fallbacks(
        self,
    ) -> None:
        for path in ("direct", "triaged"):
            for has_have in (False, True):
                for legacy_source in (False, True):
                    with self.subTest(
                        path=path,
                        has_have=has_have,
                        legacy_source=legacy_source,
                    ):
                        current = ({
                            "format": "MP3",
                            "min_bitrate_kbps": 192,
                            "avg_bitrate_kbps": 224,
                        } if has_have else None)
                        common: dict[str, object] = {
                            "outcome": "rejected",
                            "actual_min_bitrate": 0,
                            "was_converted": True,
                            "original_filetype": "flac",
                            "actual_filetype": "opus",
                            "final_format": "opus 128",
                            "source_format": (
                                None if legacy_source else "FLAC"
                            ),
                            "slskd_filetype": "FLAC",
                            "filetype": "mp3",
                            "import_result": _corrupt_import_result(
                                (
                                    "audio_corrupt"
                                    if path == "direct"
                                    else None
                                ),
                                has_have=has_have,
                                typed_source=not legacy_source,
                            ),
                        }
                        if path == "direct":
                            result = classify_log_entry(_entry(
                                **common,
                                beets_scenario="audio_corrupt",
                            ))
                        else:
                            audit: dict[str, object] = {
                                "action": "deleted_reject",
                                "outcome": "deleted",
                                "reason": "audio_corrupt",
                                "preview_verdict": "confident_reject",
                                "preview_decision": "audio_corrupt",
                                "stage_chain": [
                                    "preimport_audio:reject_corrupt",
                                ],
                                "candidate_measurement": {
                                    "format": (
                                        None
                                        if legacy_source
                                        else "FLAC"
                                    ),
                                    "min_bitrate_kbps": 0,
                                    "avg_bitrate_kbps": 0,
                                    "median_bitrate_kbps": 0,
                                    "spectral_grade": "genuine",
                                    "spectral_bitrate_kbps": 320,
                                },
                                "candidate_v0_probe": {
                                    "kind": "lossless_source_v0",
                                    "min_bitrate_kbps": 165,
                                    "avg_bitrate_kbps": 171,
                                    "median_bitrate_kbps": 170,
                                },
                            }
                            if current is not None:
                                audit["current_measurement"] = current
                            result = classify_log_entry(_entry(
                                **common,
                                beets_scenario="high_distance",
                                beets_distance=0.181,
                                validation_result={
                                    "wrong_match_triage": audit,
                                },
                            ))
                        assert_corrupt_candidate_display_is_codec_only(
                            result,
                            has_have=has_have,
                        )

    def test_corrupt_projection_checker_rejects_candidate_leaks_and_erased_have(self) -> None:
        result = classify_log_entry(_entry(
            outcome="rejected", beets_scenario="audio_corrupt",
            actual_min_bitrate=0, import_result=_corrupt_import_result("audio_corrupt"),
        ))
        leaked_v0 = msgspec.structs.replace(result, v0_probe_avg_bitrate=171)
        with self.assertRaisesRegex(AssertionError, "candidate quality claim"):
            assert_corrupt_candidate_display_is_codec_only(leaked_v0, has_have=False)
        leaked_label = msgspec.structs.replace(
            result, downloaded_label="FLAC → OPUS 128",
        )
        with self.assertRaisesRegex(AssertionError, "tier or conversion label"):
            assert_corrupt_candidate_display_is_codec_only(leaked_label, has_have=False)
        leaked_output = msgspec.structs.replace(
            result, materialized_format="Opus", materialized_avg_bitrate=124,
        )
        with self.assertRaisesRegex(AssertionError, "candidate quality claim"):
            assert_corrupt_candidate_display_is_codec_only(leaked_output, has_have=False)
        with_have = msgspec.structs.replace(
            result, existing_format="MP3", existing_min_bitrate=192,
            existing_avg_bitrate=224,
        )
        erased_have = msgspec.structs.replace(
            with_have, existing_format=None, existing_min_bitrate=None,
            existing_avg_bitrate=None,
        )
        with self.assertRaisesRegex(AssertionError, "erased point-in-time HAVE"):
            assert_corrupt_candidate_display_is_codec_only(erased_have, has_have=True)

    def test_every_triaged_rejection_stays_red(self) -> None:
        actions = (
            "deleted_reject",
            "deleted_verified_lossless_parent",
            "kept_would_import",
            "kept_uncertain",
            "skipped_current_evidence_missing",
        )
        for action in actions:
            with self.subTest(action=action):
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
    )
    @example(
        existing_format=None,
        existing_min=None,
        has_attempt_spectral=False,
    )
    @example(
        existing_format=None,
        existing_min=None,
        has_attempt_spectral=True,
    )
    def test_unproven_current_library_never_backfills_attempt_have(
        self,
        existing_format: str | None,
        existing_min: int | None,
        has_attempt_spectral: bool,
    ) -> None:
        item: dict[str, object] = {
            "existing_format": existing_format,
            "existing_min_bitrate": existing_min,
            "existing_spectral_grade": (
                "likely_transcode" if has_attempt_spectral else None
            ),
            "existing_spectral_bitrate": 160 if has_attempt_spectral else None,
        }
        _project_current_library_have(item, {})

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
        _project_current_library_have(item, row)

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
        card_kind=st.sampled_from((
            "deleted", "failed", "timeout", "measurement_failed",
        )),
        partial_kind=st.sampled_from((
            "v0_only",
            "spectral_only",
            "format_only",
            "minimum_only",
            "spectral_failure_only",
        )),
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
        card_kind="deleted",
        partial_kind="v0_only",
        current_format="MP3",
        current_min=320,
        current_avg=320,
        current_median=320,
        current_spectral="genuine",
        current_v0=268,
    )
    @example(
        card_kind="failed",
        partial_kind="minimum_only",
        current_format="Opus",
        current_min=93,
        current_avg=129,
        current_median=128,
        current_spectral="suspect",
        current_v0=256,
    )
    @example(
        card_kind="measurement_failed",
        partial_kind="spectral_only",
        current_format="Opus",
        current_min=90,
        current_avg=97,
        current_median=95,
        current_spectral="suspect",
        current_v0=None,
    )
    def test_partial_have_selects_one_complete_snapshot(
        self,
        card_kind: str,
        partial_kind: str,
        current_format: str,
        current_min: int,
        current_avg: int,
        current_median: int,
        current_spectral: str | None,
        current_v0: int | None,
    ) -> None:
        item: dict[str, object] = {
            "outcome": (
                "rejected" if card_kind == "deleted" else card_kind
            ),
            "wrong_match_triage_action": (
                "deleted_reject" if card_kind == "deleted" else None
            ),
            "existing_format": None,
            "existing_min_bitrate": None,
            "existing_avg_bitrate": None,
            "existing_median_bitrate": None,
            "existing_spectral_grade": None,
            "existing_spectral_bitrate": None,
            "existing_spectral_attempted": False,
            "existing_spectral_error": None,
            "existing_v0_probe_kind": None,
            "existing_v0_probe_min_bitrate": None,
            "existing_v0_probe_avg_bitrate": None,
            "existing_v0_probe_median_bitrate": None,
            "comparison_basis": None,
        }
        if partial_kind == "v0_only":
            item.update({
                "existing_v0_probe_kind": "on_disk_research_v0",
                "existing_v0_probe_min_bitrate": 245,
                "existing_v0_probe_avg_bitrate": 268,
                "existing_v0_probe_median_bitrate": 268,
            })
        elif partial_kind == "spectral_only":
            item.update({
                "existing_spectral_grade": "likely_transcode",
                "existing_spectral_bitrate": 96,
            })
        elif partial_kind == "format_only":
            item["existing_format"] = "Legacy"
        elif partial_kind == "minimum_only":
            item["existing_min_bitrate"] = 7
        else:
            item.update({
                "existing_spectral_attempted": True,
                "existing_spectral_error": "legacy decode failure",
            })

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
                "on_disk_research_v0" if current_v0 is not None else None
            ),
            "_current_evidence_v0_probe_min_bitrate": (
                current_v0 - 1 if current_v0 is not None else None
            ),
            "_current_evidence_v0_probe_avg_bitrate": current_v0,
            "_current_evidence_v0_probe_median_bitrate": current_v0,
        }
        _project_current_library_have(item, row)

        assert_complete_have_snapshot_is_selected(
            item,
            expected_format=current_format,
            expected_min=current_min,
            expected_avg=current_avg,
            expected_median=current_median,
            expected_spectral=current_spectral,
            expected_v0=current_v0,
        )

    def test_complete_snapshot_checker_rejects_partial_field_blends(self) -> None:
        with self.assertRaisesRegex(AssertionError, "current library format"):
            assert_complete_have_snapshot_is_selected(
                {
                    "existing_format": None,
                    "existing_min_bitrate": None,
                    "existing_avg_bitrate": None,
                    "existing_median_bitrate": None,
                    "existing_v0_probe_avg_bitrate": 268,
                },
                expected_format="MP3",
                expected_min=320,
                expected_avg=320,
                expected_median=320,
                expected_spectral=None,
                expected_v0=268,
            )

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
        })
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

    def test_verified_lossless_copy_checker_rejects_hardcoded_mp3_legacy_copy(
        self,
    ) -> None:
        basis = msgspec.convert({
            "verdict": "equivalent",
            "branch": "label_contract_same_rank",
            "new_rank": "transparent",
            "existing_rank": "transparent",
            "new_metric": "contract",
            "existing_metric": "avg",
            "new_value_kbps": 128,
            "existing_value_kbps": 141,
            "new_format": "opus 128",
            "existing_format": "opus",
            "spectral_clamped": False,
            "tolerance_kbps": None,
            "verified_lossless_bypass": True,
        }, type=QualityComparisonBasis)
        with self.assertRaisesRegex(AssertionError, "persisted format comparison"):
            assert_verified_lossless_proof_upgrade_names_basis(
                "Proof upgrade: equivalent quality — OPUS 128 vs MP3, "
                "from FLAC, verified lossless",
                basis,
            )

    @given(
        target=st.sampled_from((
            ("opus 128", 141),
            ("mp3 v0", 250),
        )),
        existing=st.sampled_from((
            ("opus", 141, 129),
            # 320, not the pre-#1145 250: the bypass only has something to
            # bypass when the installed copy is not beaten outright, and one
            # MP3 ladder puts 250 in ``good`` where the VBR table read it
            # ``transparent``.
            ("mp3", 320, 300),
            ("aac", 256, 240),
        )),
    )
    @example(
        target=("opus 128", 141),
        existing=("opus", 141, 129),
    )
    def test_verified_lossless_bypass_uses_persisted_existing_codec(
        self,
        target: tuple[str, int],
        existing: tuple[str, int, int],
    ) -> None:
        from lib.quality import (
            AudioQualityMeasurement,
            QualityRankConfig,
            import_quality_decision,
        )

        target_format, target_avg = target
        existing_format, existing_avg, existing_min = existing
        decision = import_quality_decision(
            AudioQualityMeasurement(
                avg_bitrate_kbps=target_avg,
                format=target_format,
            ),
            AudioQualityMeasurement(
                min_bitrate_kbps=existing_min,
                avg_bitrate_kbps=existing_avg,
                format=existing_format,
            ),
            cfg=QualityRankConfig.defaults(),
            verified_lossless_proof=True,
        )
        self.assertEqual(decision.decision, "import")
        self.assertIsNotNone(decision.basis)
        assert decision.basis is not None
        self.assertTrue(decision.basis.verified_lossless_bypass)

        actual_format = "opus" if target_format == "opus 128" else "mp3"
        result = classify_log_entry(_entry(
            outcome="success",
            was_converted=True,
            original_filetype="flac",
            actual_filetype=actual_format,
            actual_min_bitrate=target_avg,
            existing_min_bitrate=existing_min,
            spectral_grade="genuine",
            import_result={
                "version": 2,
                "decision": "import",
                "comparison_basis": msgspec.to_builtins(decision.basis),
            },
        ))
        assert_verified_lossless_proof_upgrade_names_basis(
            result.verdict, decision.basis)

    @staticmethod
    def _plain_basis(bypass: bool) -> dict[str, object] | None:
        """A real persisted basis for the same MP3 pair, with/without bypass."""
        from lib.quality import (
            AudioQualityMeasurement,
            QualityRankConfig,
            compare_quality,
            import_quality_decision,
        )

        # The installed copy sits at the MP3 transparent floor so the pair is
        # EQUIVALENT and the proof has something to bypass. Under the one MP3
        # ladder (#1145) the old 248 reads ``good``, which the V0 contract
        # beats outright — a "better" verdict needs no bypass and the world
        # would stop exercising this branch.
        new = AudioQualityMeasurement(avg_bitrate_kbps=250, format="mp3 v0")
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=300, avg_bitrate_kbps=320, format="mp3")
        if not bypass:
            return msgspec.to_builtins(
                compare_quality(new, existing, QualityRankConfig.defaults()))
        decision = import_quality_decision(
            new,
            existing,
            cfg=QualityRankConfig.defaults(),
            verified_lossless_proof=True,
        )
        if decision.basis is None or not decision.basis.verified_lossless_bypass:
            raise AssertionError("bypass world did not persist a bypass basis")
        return msgspec.to_builtins(decision.basis)

    @given(
        classifier=st.sampled_from((None, *MINTED_CLASSIFIERS)),
        was_converted=st.booleans(),
        original_filetype=st.sampled_from((None, "flac", "FLAC", "wav", "mp3")),
        spectral_grade=st.sampled_from(
            (None, "genuine", "suspect", "likely_transcode")),
        actual_filetype=st.sampled_from(("mp3", "opus", "flac")),
        actual_min_bitrate=st.integers(min_value=1, max_value=1_200),
        existing_min_bitrate=st.sampled_from((None, 0, 192, 320)),
        search_filetype_override=st.sampled_from(
            (None, "lossless", "flac,mp3 v0,mp3 320")),
        basis_mode=st.sampled_from(("none", "plain", "bypass")),
    )
    @example(  # live dl 39120 req 2066 — the shipped defect
        classifier=None,
        was_converted=True,
        original_filetype="flac",
        spectral_grade="genuine",
        actual_filetype="opus",
        actual_min_bitrate=93,
        existing_min_bitrate=192,
        search_filetype_override="lossless",
        basis_mode="none",
    )
    @example(  # live dl 39094 req 2147 — same world, MP3 target
        classifier=None,
        was_converted=True,
        original_filetype="flac",
        spectral_grade="genuine",
        actual_filetype="mp3",
        actual_min_bitrate=183,
        existing_min_bitrate=192,
        search_filetype_override="lossless",
        basis_mode="plain",
    )
    def test_verified_lossless_copy_never_outruns_the_minted_proof(
        self,
        classifier: str | None,
        was_converted: bool,
        original_filetype: str | None,
        spectral_grade: str | None,
        actual_filetype: str,
        actual_min_bitrate: int,
        existing_min_bitrate: int | None,
        search_filetype_override: str | None,
        basis_mode: str,
    ) -> None:
        """The phrase is a report of the decider's proof, never a guess.

        The world space is exactly the columns the retired heuristic keyed
        on — conversion, original filetype, spectral grade — crossed with
        every render branch a successful import can take.
        """
        basis = (
            None if basis_mode == "none"
            else self._plain_basis(basis_mode == "bypass")
        )
        result = classify_log_entry(_entry(
            outcome="success",
            beets_scenario="strong_match",
            was_converted=was_converted,
            original_filetype=original_filetype,
            spectral_grade=spectral_grade,
            actual_filetype=actual_filetype,
            actual_min_bitrate=actual_min_bitrate,
            existing_min_bitrate=existing_min_bitrate,
            search_filetype_override=search_filetype_override,
            verified_lossless_classifier=classifier,
            import_result=(
                None if basis is None
                else {"version": 2, "decision": "import",
                      "comparison_basis": basis}
            ),
        ))
        assert_verified_lossless_claim_is_minted(
            result.verdict,
            result.summary,
            classifier=classifier,
            basis_bypass=basis_mode == "bypass",
        )
        if classifier is not None and basis_mode != "bypass":
            assert_minted_proof_is_reported(result.verdict)

    def test_minted_proof_checker_rejects_the_retired_heuristic(self) -> None:
        """Known-bad: the pre-fix derivation, planted as its own output."""
        with self.assertRaisesRegex(AssertionError, "no minted proof"):
            assert_verified_lossless_claim_is_minted(
                "Replaced unverified CBR with OPUS 93k, from FLAC, "
                "verified lossless",
                "Replaced unverified CBR with OPUS 93k, from FLAC, "
                "verified lossless · algernon",
                classifier=None,
                basis_bypass=False,
            )

    def test_minted_proof_checker_rejects_a_summary_only_claim(self) -> None:
        """The claim survives in the OTHER renderer — issue #868's lesson."""
        with self.assertRaisesRegex(AssertionError, "summary claims"):
            assert_verified_lossless_claim_is_minted(
                "MP3 V0 - from FLAC",
                "MP3 V0 - from FLAC, verified lossless · algernon",
                classifier=None,
                basis_bypass=False,
            )

    def test_minted_proof_reporting_checker_rejects_a_dropped_proof(
        self,
    ) -> None:
        """Known-bad for the must-still-work half."""
        with self.assertRaisesRegex(AssertionError, "dropped a minted"):
            assert_minted_proof_is_reported("MP3 V0 - from FLAC")

    @given(
        lineage=st.sampled_from((None, 1, 2, 3, 4)),
        classifier=st.sampled_from((None, *MINTED_CLASSIFIERS)),
        was_converted=st.booleans(),
        original_filetype=st.sampled_from((None, "flac", "FLAC", "wav")),
        spectral_grade=st.sampled_from(
            (None, "genuine", "suspect", "likely_transcode")),
        actual_filetype=st.sampled_from(("mp3", "opus", "flac")),
        actual_min_bitrate=st.integers(min_value=1, max_value=1_200),
        existing_min_bitrate=st.sampled_from((None, 0, 192, 320)),
        search_filetype_override=st.sampled_from((None, "lossless")),
        import_result_proof=st.booleans(),
    )
    @example(  # the shipped defect's shape: a cross-walked legacy proof
        lineage=1,
        classifier=MINTED_CLASSIFIERS[0],
        was_converted=False,
        original_filetype=None,
        spectral_grade=None,
        actual_filetype="mp3",
        actual_min_bitrate=320,
        existing_min_bitrate=192,
        search_filetype_override=None,
        import_result_proof=False,
    )
    @example(  # the must-still-work twin: same proof, source-semantic row
        lineage=4,
        classifier=MINTED_CLASSIFIERS[0],
        was_converted=False,
        original_filetype=None,
        spectral_grade=None,
        actual_filetype="mp3",
        actual_min_bitrate=320,
        existing_min_bitrate=192,
        search_filetype_override=None,
        import_result_proof=False,
    )
    @example(  # live dl 36970 req 2937 — no evidence, proof on the blob only
        lineage=None,
        classifier=None,
        was_converted=True,
        original_filetype="flac",
        spectral_grade="genuine",
        actual_filetype="opus",
        actual_min_bitrate=112,
        existing_min_bitrate=192,
        search_filetype_override=None,
        import_result_proof=True,
    )
    def test_only_an_attributable_evidence_proof_reaches_the_verdict(
        self,
        lineage: int | None,
        classifier: str | None,
        was_converted: bool,
        original_filetype: str | None,
        spectral_grade: str | None,
        actual_filetype: str,
        actual_min_bitrate: int,
        existing_min_bitrate: int | None,
        search_filetype_override: str | None,
        import_result_proof: bool,
    ) -> None:
        """Composed patrol: the real read seam plus the real renderer.

        The gate and the copy live in different modules — the overlay
        decides whether a proof may speak for this attempt's bytes, the
        verdict decides whether to say so — and the defect lived in their
        composition: ``candidate_evidence_id`` was cross-walked by migration
        021 §6b, so a legacy-lineage row's proof belongs to a sibling
        attempt. Neither module alone can see that, so the property drives
        the raw joined row through BOTH.

        ``import_result_proof`` crosses the whole space with a row whose own
        audit blob DOES project a ``verified_lossless_proof``. It must never
        move the verdict: every historical projection mints the same
        ``legacy_import_result`` stamp out of the era's
        ``will_be_verified_lossless``, which on the converted path is
        ``converted_count > 0 and not is_transcode`` — the retired heuristic
        itself. An evidence row is the only thing that carries an
        attributable proof.
        """
        row = _raw_download_log_row(
            lineage=lineage,
            classifier=classifier,
            was_converted=was_converted,
            original_filetype=original_filetype,
            spectral_grade=spectral_grade,
            actual_filetype=actual_filetype,
            actual_min_bitrate=actual_min_bitrate,
            existing_min_bitrate=existing_min_bitrate,
            search_filetype_override=search_filetype_override,
            import_result=(
                _V3_PERSISTED_PROOF_BLOB if import_result_proof else None
            ),
        )
        result = render_through_the_read_seam(row)
        proved = classifier is not None and lineage in (3, 4)
        assert_verified_lossless_claim_is_minted(
            result.verdict,
            result.summary,
            classifier=classifier if proved else None,
            basis_bypass=False,
        )
        if proved:
            assert_minted_proof_is_reported(result.verdict)

    def test_the_composed_checker_trips_on_the_ungated_render(self) -> None:
        """Known-bad: re-feed a cross-walked lineage-1 classifier.

        No mutant needed — skipping the read seam IS the pre-fix path, so
        the real renderer over the real cross-walked row produces the
        shipped falsehood and the checker must name it.
        """
        row = _raw_download_log_row(
            lineage=1,
            classifier=MINTED_CLASSIFIERS[0],
            was_converted=False,
            original_filetype=None,
            spectral_grade=None,
            actual_filetype="mp3",
            actual_min_bitrate=320,
            existing_min_bitrate=192,
            search_filetype_override=None,
        )
        ungated = classify_log_entry(LogEntry.from_row(dict(row)))
        self.assertIn("verified lossless", ungated.verdict.lower())
        with self.assertRaisesRegex(AssertionError, "no minted proof"):
            assert_verified_lossless_claim_is_minted(
                ungated.verdict,
                ungated.summary,
                classifier=None,
                basis_bypass=False,
            )

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
