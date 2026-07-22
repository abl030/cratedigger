"""Generated contracts for source, probe, target, and output lineage."""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from hypothesis import example, given, strategies as st
import msgspec

from harness.import_one import projected_is_cbr_from_bitrates
from lib.beets_db import (
    CurrentBeetsUnique,
    release_identity_for_lookup,
)
from lib.import_evidence import ensure_current_evidence_for_action
from lib.measurement import PreimportMeasurement
from lib.import_preview import (
    EnrichmentPlan,
    enrich_current_v0_research_for_preview,
    enrich_incomplete_current_evidence_for_request,
    load_current_evidence_for_preview,
    prepare_current_evidence_for_failure,
    persist_exact_current_spectral_from_attempt,
    plan_current_evidence_enrichment,
)
from lib.pipeline_db.download_log import _DownloadLogMixin
from lib.quality import (
    AlbumQualityV0Metric,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    ImportResult,
    MeasuredImportDecisionInput,
    QualityRankConfig,
    SpectralAnalysisDetail,
    TargetQualityContract,
    V0ProbeEvidence,
    full_pipeline_decision,
    full_pipeline_decision_from_evidence,
    gate_rank,
    measured_import_decision,
    quality_gate_decision,
)
from lib.quality_evidence import (
    evidence_from_import_result,
    evidence_from_measurement,
    snapshot_audio_files,
)
from lib.wrong_match_cleanup_service import (
    OUTCOME_KEPT_UNCERTAIN,
    _cleanup_audit_payload,
    _result,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


def _coherent_three_track_metrics(
    first: int,
    second: int,
    third: int,
) -> tuple[int, int, int, bool]:
    """Turn generated item bitrates into production-derived album metrics."""

    minimum, median, maximum = sorted((first, second, third))
    average = int((minimum + median + maximum) / 3)
    return minimum, average, median, minimum == maximum
from web.classify import LogEntry, classify_log_entry


def assert_source_target_lineage(result: ImportResult) -> None:
    """Independent checker for the unambiguous v3 measurement shape."""

    source = result.source_measurement
    if source is not None:
        if source.format is not None and len(source.format.strip().split()) != 1:
            raise AssertionError("source measurement must use a bare codec label")
        if source.was_converted_from is not None:
            raise AssertionError("source measurement must not carry output lineage")
    output = result.materialized_measurement
    if (
        output is not None
        and output.format is not None
        and len(output.format.strip().split()) != 1
    ):
        raise AssertionError("output measurement must use a bare codec label")


def assert_research_v0_is_policy_neutral(
    before: dict[str, object],
    after: dict[str, object],
) -> None:
    if before != after:
        raise AssertionError("on-disk research V0 changed quality policy")


def assert_research_attempted_once_per_snapshot(
    *,
    probe_calls: int,
    evidence_attempted: bool,
) -> None:
    """Independent checker for the preview retry/idempotence invariant."""

    if probe_calls != 1 or not evidence_attempted:
        raise AssertionError(
            "unchanged evidence snapshot must record exactly one V0 research attempt"
        )


def assert_exact_current_spectral_persisted(
    *,
    expected_grade: str,
    expected_bitrate: int | None,
    actual_grade: str | None,
    actual_bitrate: int | None,
) -> None:
    if (actual_grade, actual_bitrate) != (expected_grade, expected_bitrate):
        raise AssertionError("attempt scan did not populate exact current evidence")


def assert_enrichment_plan_never_remeasures(evidence, plan) -> None:
    """Independent checker: enrichment only measures what is missing."""

    measurement = evidence.measurement
    if plan.spectral and (
        measurement.spectral_grade is not None
        or measurement.spectral_bitrate_kbps is not None
    ):
        raise AssertionError(
            "enrichment plan re-measures spectral evidence that already exists"
        )
    if plan.v0 and (
        evidence.v0_metric is not None
        or evidence.on_disk_v0_research_attempted
    ):
        raise AssertionError(
            "enrichment plan re-researches V0 evidence that already exists"
        )


def assert_failure_enrichment_matches_library_membership(
    *,
    album_present: bool,
    current_evidence_id: int | None,
    outcome: str,
) -> None:
    """An installed exact release must acquire a linked HAVE snapshot."""
    if album_present and current_evidence_id is None:
        raise AssertionError("installed release remained without linked HAVE")
    if album_present and outcome == "no_current_evidence":
        raise AssertionError("installed release was classified as absent HAVE")
    if not album_present and current_evidence_id is not None:
        raise AssertionError("absent release fabricated a HAVE snapshot")
    if not album_present and outcome != "no_current_evidence":
        raise AssertionError("absent release did not retain no-HAVE outcome")


def assert_failure_v1_refresh_phases(
    *,
    outcome: str,
    initial_lineage: int,
    prepared_lineage: int | None,
    refresh_status: str,
    final_lineage: int | None,
) -> None:
    """Failure preparation refreshes identity before later enrichment."""
    if outcome != "ready":
        raise AssertionError("installed current evidence was not prepared")
    if initial_lineage == 1 and prepared_lineage != 4:
        raise AssertionError("failure preparation retained ambiguous lineage")
    if refresh_status != "wanted":
        raise AssertionError("failure refresh ran before request became wanted")
    if final_lineage != 4:
        raise AssertionError("post-failure refresh retained ambiguous lineage")


def assert_import_attempt_uses_v3_current_evidence(
    *,
    initial_lineage: int,
    decision_lineage: int | None,
) -> None:
    """An actual import attempt may not decide from ambiguous v1 evidence."""
    if initial_lineage == 1 and decision_lineage != 4:
        raise AssertionError("import attempt retained ambiguous current lineage")


def assert_action_refresh_carries_only_source_facts(
    *,
    decision_lineage: int | None,
    v0_subject: str | None,
    v0_provenance: str | None,
    v0_average: int | None,
    spectral_grade: str | None,
    spectral_bitrate: int | None,
) -> None:
    """Lineage repair carries source facts and drops ambiguous on-disk facts."""
    if decision_lineage != 4:
        raise AssertionError("action did not rebuild current evidence as v4")
    if v0_subject != "source":
        raise AssertionError("action discarded lossless-source V0 evidence")
    if v0_provenance != "carried":
        raise AssertionError("action did not mark source V0 evidence carried")
    if v0_average != 195:
        raise AssertionError("action replaced exact-snapshot V0 evidence")
    if spectral_grade is not None or spectral_bitrate is not None:
        raise AssertionError("action retained ambiguous legacy spectral evidence")


class TestQualityLineagePins(unittest.TestCase):
    def test_research_once_checker_rejects_repeated_unmarked_probe(self):
        with self.assertRaisesRegex(AssertionError, "exactly one"):
            assert_research_attempted_once_per_snapshot(
                probe_calls=2,
                evidence_attempted=False,
            )

    def test_enrichment_plan_checker_rejects_planted_remeasure(self):
        graded = make_album_quality_evidence()  # default: genuine spectral
        with self.assertRaisesRegex(AssertionError, "spectral"):
            assert_enrichment_plan_never_remeasures(
                graded, EnrichmentPlan(spectral=True, v0=False),
            )
        attempted = make_album_quality_evidence(
            on_disk_v0_research_attempted=True,
        )
        with self.assertRaisesRegex(AssertionError, "V0"):
            assert_enrichment_plan_never_remeasures(
                attempted, EnrichmentPlan(spectral=False, v0=True),
            )

    def test_failure_enrichment_checker_rejects_unlinked_installed_release(self):
        with self.assertRaisesRegex(AssertionError, "without linked HAVE"):
            assert_failure_enrichment_matches_library_membership(
                album_present=True,
                current_evidence_id=None,
                outcome="no_current_evidence",
            )

    def test_failure_lineage_checker_rejects_retained_v1(self):
        with self.assertRaisesRegex(AssertionError, "ambiguous lineage"):
            assert_failure_v1_refresh_phases(
                outcome="ready",
                initial_lineage=1,
                prepared_lineage=1,
                refresh_status="wanted",
                final_lineage=4,
            )

    def test_import_attempt_checker_rejects_v1_decision_evidence(self):
        with self.assertRaisesRegex(AssertionError, "ambiguous current lineage"):
            assert_import_attempt_uses_v3_current_evidence(
                initial_lineage=1,
                decision_lineage=1,
            )

    def test_action_refresh_checker_rejects_lost_lossless_source_v0(self):
        with self.assertRaisesRegex(AssertionError, "discarded"):
            assert_action_refresh_carries_only_source_facts(
                decision_lineage=4,
                v0_subject=None,
                v0_provenance=None,
                v0_average=None,
                spectral_grade=None,
                spectral_bitrate=None,
            )

    def test_action_refresh_checker_rejects_legacy_spectral_carry(self):
        with self.assertRaisesRegex(AssertionError, "ambiguous legacy spectral"):
            assert_action_refresh_carries_only_source_facts(
                decision_lineage=4,
                v0_subject="source",
                v0_provenance="carried",
                v0_average=195,
                spectral_grade="likely_transcode",
                spectral_bitrate=96,
            )

    def test_enrichment_plan_pin_complete_row_plans_nothing(self):
        evidence = make_album_quality_evidence(
            on_disk_v0_research_attempted=True,
        )
        plan = plan_current_evidence_enrichment(evidence)
        self.assertFalse(plan.spectral)
        self.assertFalse(plan.v0)

    @given(
        grade=st.one_of(
            st.none(),
            st.sampled_from(("genuine", "suspect", "likely_transcode")),
        ),
        spectral_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=500),
        ),
        v0_present=st.booleans(),
        v0_attempted=st.booleans(),
    )
    @example(grade=None, spectral_bitrate=None,
             v0_present=False, v0_attempted=False)
    @example(grade="genuine", spectral_bitrate=None,
             v0_present=False, v0_attempted=True)
    def test_generated_enrichment_plan_measures_exactly_the_missing_pieces(
        self, grade, spectral_bitrate, v0_present, v0_attempted,
    ):
        evidence = make_album_quality_evidence(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade=grade,
                spectral_bitrate_kbps=spectral_bitrate,
            ),
            v0_metric=(
                AlbumQualityV0Metric(
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                    subject="installed",
                )
                if v0_present
                else None
            ),
            on_disk_v0_research_attempted=v0_attempted,
        )
        plan = plan_current_evidence_enrichment(evidence)
        assert_enrichment_plan_never_remeasures(evidence, plan)
        self.assertEqual(
            plan.spectral,
            grade is None and spectral_bitrate is None,
        )
        self.assertEqual(plan.v0, not v0_present and not v0_attempted)

    @given(
        album_present=st.booleans(),
        minimum=st.integers(min_value=32, max_value=320),
        average=st.integers(min_value=32, max_value=320),
        median=st.integers(min_value=32, max_value=320),
    )
    @example(album_present=True, minimum=183, average=190, median=191)
    def test_generated_failed_download_links_have_when_release_is_installed(
        self,
        album_present: bool,
        minimum: int,
        average: int,
        median: int,
    ) -> None:
        from lib.beets_db import AlbumInfo
        from tests.fakes import FakeBeetsDB

        minimum, average, median, is_cbr = _coherent_three_track_metrics(
            minimum, average, median,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            status="wanted",
        ))
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"generated current-library bytes")
            fake_beets = FakeBeetsDB()
            if album_present:
                fake_beets.set_album_info("mbid-42", AlbumInfo(
                    album_id=1,
                    track_count=3,
                    min_bitrate_kbps=minimum,
                    avg_bitrate_kbps=average,
                    median_bitrate_kbps=median,
                    is_cbr=is_cbr,
                    album_path=source,
                    format="MP3",
                ))
            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
                prepared = prepare_current_evidence_for_failure(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                )
                outcome = prepared
                if prepared == "ready":
                    outcome = enrich_incomplete_current_evidence_for_request(
                        db,
                        request_id=42,
                        mb_release_id="mbid-42",
                        quality_ranks=QualityRankConfig.defaults(),
                        beets_library_root=source,
                        spectral_analyzer=lambda _path: SpectralAnalysisDetail(
                            attempted=True,
                            grade="genuine",
                            bitrate_kbps=96,
                        ),
                        probe_fn=lambda _path: None,
                    )

        assert_failure_enrichment_matches_library_membership(
            album_present=album_present,
            current_evidence_id=db.get_request_current_evidence_id(42),
            outcome=outcome,
        )

    @given(
        minimum=st.integers(min_value=32, max_value=320),
        average=st.integers(min_value=32, max_value=320),
        median=st.integers(min_value=32, max_value=320),
    )
    @example(minimum=256, average=256, median=256)
    def test_generated_failed_download_rebuilds_v1_current_evidence(
        self,
        minimum: int,
        average: int,
        median: int,
    ) -> None:
        from lib.beets_db import AlbumInfo
        from tests.fakes import FakeBeetsDB

        minimum, average, median, is_cbr = _coherent_three_track_metrics(
            minimum, average, median,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            status="downloading",
        ))
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.m4a"), "wb") as handle:
                handle.write(b"generated legacy current-library bytes")
            legacy = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=minimum,
                    avg_bitrate_kbps=average,
                    median_bitrate_kbps=median,
                    format="AAC",
                    is_cbr=is_cbr,
                    spectral_grade="genuine",
                ),
                lineage_version=1,
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=259,
                    avg_bitrate_kbps=267,
                    median_bitrate_kbps=269,
                    subject="installed",
                ),
            )
            db.upsert_album_quality_evidence(legacy)
            stored = db.find_album_quality_evidence(
                mb_release_id=legacy.mb_release_id,
                snapshot_fingerprint=legacy.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)
            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=3,
                min_bitrate_kbps=minimum,
                avg_bitrate_kbps=average,
                median_bitrate_kbps=median,
                is_cbr=is_cbr,
                album_path=source,
                format="AAC",
            ))

            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
                prepared = prepare_current_evidence_for_failure(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                )

            current_id = db.get_request_current_evidence_id(42)
            prepared_current = db.load_album_quality_evidence_by_id(current_id)
            db.update_status(42, "wanted", expected_status="downloading")
            refresh_status = str(db.request(42)["status"])
            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
                enriched = enrich_incomplete_current_evidence_for_request(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                    spectral_analyzer=lambda _path: SpectralAnalysisDetail(
                        attempted=True,
                        grade="genuine",
                        bitrate_kbps=96,
                    ),
                    probe_fn=lambda _path: None,
                )

        current = db.load_album_quality_evidence_by_id(current_id)
        self.assertEqual(enriched, "enriched")
        assert_failure_v1_refresh_phases(
            outcome=prepared,
            initial_lineage=1,
            prepared_lineage=(
                prepared_current.lineage_version
                if prepared_current is not None
                else None
            ),
            refresh_status=refresh_status,
            final_lineage=(current.lineage_version if current is not None else None),
        )
        assert current is not None
        self.assertEqual(current.measurement.min_bitrate_kbps, minimum)
        self.assertEqual(current.measurement.avg_bitrate_kbps, average)
        self.assertEqual(current.measurement.median_bitrate_kbps, median)

    @given(
        minimum=st.integers(min_value=32, max_value=320),
        average=st.integers(min_value=32, max_value=320),
        median=st.integers(min_value=32, max_value=320),
    )
    @example(minimum=256, average=256, median=256)
    def test_generated_import_attempt_rebuilds_v1_current_evidence(
        self,
        minimum: int,
        average: int,
        median: int,
    ) -> None:
        from lib.beets_db import AlbumInfo
        from tests.fakes import FakeBeetsDB

        minimum, average, median, is_cbr = _coherent_three_track_metrics(
            minimum, average, median,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            status="downloading",
            current_spectral_grade="genuine",
            current_spectral_bitrate=None,
            current_lossless_source_v0_probe_min_bitrate=211,
            current_lossless_source_v0_probe_avg_bitrate=222,
            current_lossless_source_v0_probe_median_bitrate=220,
        ))
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.m4a"), "wb") as handle:
                handle.write(b"generated import-attempt library bytes")
            legacy = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=minimum,
                    avg_bitrate_kbps=average,
                    median_bitrate_kbps=median,
                    format="AAC",
                    is_cbr=is_cbr,
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=96,
                    was_converted_from="flac",
                ),
                lineage_version=1,
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=189,
                    avg_bitrate_kbps=195,
                    median_bitrate_kbps=195,
                    subject="source",
                ),
            )
            db.upsert_album_quality_evidence(legacy)
            stored = db.find_album_quality_evidence(
                mb_release_id=legacy.mb_release_id,
                snapshot_fingerprint=legacy.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)
            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=3,
                min_bitrate_kbps=minimum,
                avg_bitrate_kbps=average,
                median_bitrate_kbps=median,
                is_cbr=is_cbr,
                album_path=source,
                format="AAC",
            ))

            with patch(
                "lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets,
            ):
                result = load_current_evidence_for_preview(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                    preloaded_evidence=stored,
                )
            assert result.status == "ready"
            current = result.evidence
            assert current is not None

            action_db = FakePipelineDB()
            action_db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-42",
                status="downloading",
                current_spectral_grade="genuine",
                current_spectral_bitrate=None,
                current_lossless_source_v0_probe_min_bitrate=211,
                current_lossless_source_v0_probe_avg_bitrate=222,
                current_lossless_source_v0_probe_median_bitrate=220,
            ))
            action_db.upsert_album_quality_evidence(legacy)
            action_stored = action_db.find_album_quality_evidence(
                mb_release_id=legacy.mb_release_id,
                snapshot_fingerprint=legacy.snapshot_fingerprint,
            )
            assert action_stored is not None and action_stored.id is not None
            action_db.set_request_current_evidence(42, action_stored.id)
            identity = release_identity_for_lookup("mbid-42")
            assert identity is not None
            current_release = fake_beets.resolve_current_release(identity)
            assert isinstance(current_release, CurrentBeetsUnique)
            action = ensure_current_evidence_for_action(
                action_db,
                request_id=42,
                mb_release_id="mbid-42",
                current_release=current_release,
                album_info=AlbumInfo(
                    album_id=1,
                    track_count=3,
                    min_bitrate_kbps=minimum,
                    avg_bitrate_kbps=average,
                    median_bitrate_kbps=median,
                    is_cbr=is_cbr,
                    album_path=source,
                    format="AAC",
                ),
            )

        assert_import_attempt_uses_v3_current_evidence(
            initial_lineage=1,
            decision_lineage=(current.lineage_version if current is not None else None),
        )
        self.assertEqual(current.measurement.min_bitrate_kbps, minimum)
        self.assertEqual(current.measurement.avg_bitrate_kbps, average)
        self.assertEqual(current.measurement.median_bitrate_kbps, median)
        action_evidence = action.evidence
        assert_action_refresh_carries_only_source_facts(
            decision_lineage=(
                action_evidence.lineage_version
                if action_evidence is not None
                else None
            ),
            v0_subject=(
                action_evidence.v0_metric.subject
                if action_evidence is not None
                and action_evidence.v0_metric is not None
                else None
            ),
            v0_provenance=(
                action_evidence.v0_metric.provenance
                if action_evidence is not None
                and action_evidence.v0_metric is not None
                else None
            ),
            v0_average=(
                action_evidence.v0_metric.avg_bitrate_kbps
                if action_evidence is not None
                and action_evidence.v0_metric is not None
                else None
            ),
            spectral_grade=(
                action_evidence.measurement.spectral_grade
                if action_evidence is not None
                else None
            ),
            spectral_bitrate=(
                action_evidence.measurement.spectral_bitrate_kbps
                if action_evidence is not None
                else None
            ),
        )

    @given(
        projected_format=st.sampled_from(("OPUS 128", "MP3 V0", "FLAC")),
        source_min=st.integers(min_value=1, max_value=2000),
        source_avg=st.integers(min_value=1, max_value=2000),
        source_median=st.integers(min_value=1, max_value=2000),
        spectral_bitrate=st.integers(min_value=1, max_value=2000),
        v0_avg=st.integers(min_value=1, max_value=500),
    )
    @example(
        projected_format="OPUS 128",
        source_min=121,
        source_avg=128,
        source_median=127,
        spectral_bitrate=224,
        v0_avg=259,
    )
    def test_generated_v1_download_overlay_fails_closed_for_source_fields(
        self,
        projected_format: str,
        source_min: int,
        source_avg: int,
        source_median: int,
        spectral_bitrate: int,
        v0_avg: int,
    ) -> None:
        row = _DownloadLogMixin._overlay_evidence_onto_download_log_row({
            "_evidence_lineage_version": 1,
            "_evidence_source_format": projected_format,
            "_evidence_source_min_bitrate": source_min,
            "_evidence_source_avg_bitrate": source_avg,
            "_evidence_source_median_bitrate": source_median,
            "_evidence_spectral_grade": "likely_transcode",
            "_evidence_spectral_bitrate": spectral_bitrate,
            "_evidence_v0_probe_kind": "on_disk_research",
            "_evidence_v0_probe_min_bitrate": 201,
            "_evidence_v0_probe_avg_bitrate": v0_avg,
            "_evidence_v0_probe_median_bitrate": 255,
        })

        self.assertIsNone(row.get("source_format"))
        self.assertIsNone(row.get("source_min_bitrate"))
        self.assertIsNone(row.get("source_avg_bitrate"))
        self.assertIsNone(row.get("source_median_bitrate"))
        self.assertEqual(row["spectral_grade"], "likely_transcode")
        self.assertEqual(row["spectral_bitrate"], spectral_bitrate)
        self.assertEqual(row["v0_probe_kind"], "on_disk_research_v0")
        self.assertEqual(row["v0_probe_avg_bitrate"], v0_avg)

    @given(
        candidate_lineage=st.sampled_from((1, 3)),
        current_lineage=st.sampled_from((1, 3)),
        candidate_avg=st.integers(min_value=32, max_value=500),
        current_avg=st.integers(min_value=32, max_value=500),
        candidate_spectral=st.integers(min_value=32, max_value=500),
        current_spectral=st.integers(min_value=32, max_value=500),
    )
    @example(
        candidate_lineage=1,
        current_lineage=3,
        candidate_avg=128,
        current_avg=320,
        candidate_spectral=96,
        current_spectral=160,
    )
    @example(
        candidate_lineage=3,
        current_lineage=3,
        candidate_avg=288,
        current_avg=196,
        candidate_spectral=224,
        current_spectral=160,
    )
    def test_generated_cleanup_audit_classify_respects_lineage_version(
        self,
        candidate_lineage: int,
        current_lineage: int,
        candidate_avg: int,
        current_avg: int,
        candidate_spectral: int,
        current_spectral: int,
    ) -> None:
        candidate = make_album_quality_evidence(
            mb_release_id="generated-candidate",
            lineage_version=candidate_lineage,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=candidate_avg,
                avg_bitrate_kbps=candidate_avg,
                median_bitrate_kbps=candidate_avg,
                format="MP3",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=candidate_spectral,
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                subject="installed",
            ),
        )
        current = make_album_quality_evidence(
            mb_release_id="generated-current",
            lineage_version=current_lineage,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=current_avg,
                avg_bitrate_kbps=current_avg,
                median_bitrate_kbps=current_avg,
                format="Opus",
                spectral_grade="genuine",
                spectral_bitrate_kbps=current_spectral,
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=202,
                avg_bitrate_kbps=260,
                median_bitrate_kbps=256,
                subject="installed",
            ),
        )
        outcome = _result(
            1,
            OUTCOME_KEPT_UNCERTAIN,
            decision={
                "comparison_basis": {
                    "verdict": "better",
                    "branch": "metric_tiebreak",
                    "new_rank": "transparent",
                    "existing_rank": "good",
                    "new_metric": "avg",
                    "existing_metric": "avg",
                    "new_value_kbps": candidate_avg,
                    "existing_value_kbps": current_avg,
                    "new_format": "MP3",
                    "existing_format": "Opus",
                },
            },
            candidate_evidence=candidate,
            current_evidence=current,
        )
        audit = _cleanup_audit_payload(outcome)
        classified = classify_log_entry(LogEntry(
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "wrong_match_triage": msgspec.to_builtins(audit),
            },
        ))

        self.assertEqual(
            classified.source_format,
            "MP3" if candidate_lineage == 3 else None,
        )
        self.assertEqual(
            classified.source_avg_bitrate,
            candidate_avg if candidate_lineage == 3 else None,
        )
        self.assertEqual(
            classified.existing_format,
            "Opus" if current_lineage == 3 else None,
        )
        self.assertEqual(
            classified.existing_min_bitrate,
            current_avg if current_lineage == 3 else None,
        )
        self.assertEqual(classified.spectral_bitrate, candidate_spectral)
        self.assertEqual(
            classified.existing_spectral_bitrate, current_spectral)
        self.assertEqual(classified.v0_probe_avg_bitrate, 259)
        self.assertEqual(classified.existing_v0_probe_avg_bitrate, 260)
        self.assertEqual(
            classified.comparison_basis is not None,
            candidate_lineage == 3 and current_lineage == 3,
        )

    @given(
        repeats=st.integers(min_value=2, max_value=8),
        payload_size=st.integers(min_value=1, max_value=128),
    )
    @example(repeats=5, payload_size=27)
    def test_generated_failed_v0_research_runs_once_per_snapshot(
        self,
        repeats: int,
        payload_size: int,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="generated-have"))
        probe_calls = 0
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"x" * payload_size)
            evidence = make_album_quality_evidence(
                mb_release_id="generated-have",
                source_path=source,
                files=snapshot_audio_files(source),
                v0_metric=None,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            def failed_probe(_path: str):
                nonlocal probe_calls
                probe_calls += 1
                return None

            result = None
            for _ in range(repeats):
                result = enrich_current_v0_research_for_preview(
                    db,
                    request_id=42,
                    expected_evidence_id=stored.id,
                    expected_snapshot_fingerprint=stored.snapshot_fingerprint,
                    current_album_path=source,
                    probe_fn=failed_probe,
                )

            assert result is not None and result.evidence is not None
            assert_research_attempted_once_per_snapshot(
                probe_calls=probe_calls,
                evidence_attempted=(
                    result.evidence.on_disk_v0_research_attempted
                ),
            )

    def test_on_disk_research_v0_does_not_change_quality_decision(self):
        candidate = make_album_quality_evidence(
            mb_release_id="candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                format="MP3",
                is_cbr=True,
            ),
        )
        current = make_album_quality_evidence(
            mb_release_id="current",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
            ),
        )
        enriched = msgspec.structs.replace(
            current,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                subject="installed",
            ),
        )
        assert_research_v0_is_policy_neutral(
            full_pipeline_decision_from_evidence(candidate, current),
            full_pipeline_decision_from_evidence(candidate, enriched),
        )

    def test_policy_neutral_checker_rejects_a_changed_decision(self):
        with self.assertRaisesRegex(AssertionError, "changed quality policy"):
            assert_research_v0_is_policy_neutral(
                {"stage2_import": "downgrade"},
                {"stage2_import": "import"},
            )

    @given(
        candidate_kbps=st.integers(min_value=32, max_value=500),
        current_kbps=st.integers(min_value=32, max_value=500),
        probe_min=st.integers(min_value=32, max_value=320),
        probe_avg=st.integers(min_value=32, max_value=320),
    )
    @example(
        candidate_kbps=320,
        current_kbps=320,
        probe_min=201,
        probe_avg=259,
    )
    def test_generated_on_disk_research_v0_is_policy_neutral(
        self,
        candidate_kbps: int,
        current_kbps: int,
        probe_min: int,
        probe_avg: int,
    ) -> None:
        candidate = make_album_quality_evidence(
            mb_release_id="candidate-generated",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=candidate_kbps,
                avg_bitrate_kbps=candidate_kbps,
                format="MP3",
                is_cbr=True,
            ),
        )
        current = make_album_quality_evidence(
            mb_release_id="current-generated",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=current_kbps,
                avg_bitrate_kbps=current_kbps,
                format="MP3",
                is_cbr=True,
            ),
        )
        enriched = msgspec.structs.replace(
            current,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=probe_min,
                avg_bitrate_kbps=probe_avg,
                median_bitrate_kbps=probe_avg,
                subject="installed",
            ),
        )
        assert_research_v0_is_policy_neutral(
            full_pipeline_decision_from_evidence(candidate, current),
            full_pipeline_decision_from_evidence(candidate, enriched),
        )
    def test_flac_to_opus_keeps_four_facts_separate(self):
        source = AudioQualityMeasurement(
            min_bitrate_kbps=742,
            avg_bitrate_kbps=811,
            median_bitrate_kbps=803,
            format="FLAC",
        )
        probe = V0ProbeEvidence(
            kind="lossless_source_v0",
            min_bitrate_kbps=191,
            avg_bitrate_kbps=224,
            median_bitrate_kbps=237,
        )
        output = AudioQualityMeasurement(
            min_bitrate_kbps=121,
            avg_bitrate_kbps=128,
            median_bitrate_kbps=127,
            format="Opus",
            was_converted_from="flac",
        )
        result = ImportResult(
            source_measurement=source,
            v0_probe=probe,
            target_quality_contract=TargetQualityContract.from_explicit_label(
                "opus 128"
            ),
            materialized_measurement=output,
        )

        assert_source_target_lineage(result)
        decoded = ImportResult.from_json(result.to_json())
        self.assertEqual(decoded.version, 4)
        self.assertEqual(decoded.source_measurement, source)
        self.assertEqual(decoded.v0_probe, probe)
        self.assertIsNotNone(decoded.target_quality_contract)
        assert decoded.target_quality_contract is not None
        self.assertEqual(decoded.target_quality_contract.format, "opus 128")
        self.assertEqual(decoded.materialized_measurement, output)

    def test_flac_to_mp3_v_level_keeps_contract_out_of_measurement(self):
        result = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=706,
                avg_bitrate_kbps=768,
                median_bitrate_kbps=755,
                format="FLAC",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=188,
                avg_bitrate_kbps=230,
                median_bitrate_kbps=232,
            ),
            target_quality_contract=TargetQualityContract.from_explicit_label(
                "mp3 v2"
            ),
        )

        assert_source_target_lineage(result)
        self.assertIsNotNone(result.source_measurement)
        self.assertIsNotNone(result.target_quality_contract)
        assert result.source_measurement is not None
        assert result.target_quality_contract is not None
        self.assertEqual(result.source_measurement.format, "FLAC")
        self.assertEqual(result.target_quality_contract.format, "mp3 v2")

    def test_native_lossy_research_probe_does_not_replace_source(self):
        source = AudioQualityMeasurement(
            min_bitrate_kbps=117,
            avg_bitrate_kbps=126,
            median_bitrate_kbps=125,
            format="Opus",
        )
        result = ImportResult(
            source_measurement=source,
            v0_probe=V0ProbeEvidence(
                kind="native_lossy_research_v0",
                min_bitrate_kbps=180,
                avg_bitrate_kbps=211,
                median_bitrate_kbps=214,
            ),
        )

        assert_source_target_lineage(result)
        self.assertEqual(result.source_measurement, source)
        self.assertIsNone(result.target_quality_contract)

    def test_keep_lossless_contract_can_name_same_bare_source_codec(self):
        result = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=742,
                avg_bitrate_kbps=811,
                format="FLAC",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=191,
                avg_bitrate_kbps=224,
            ),
            target_quality_contract=TargetQualityContract.from_explicit_label(
                "flac"
            ),
        )

        self.assertEqual(ImportResult.from_json(result.to_json()), result)

    def test_target_contract_preserves_comparison_and_gate_verdicts(self):
        cfg = QualityRankConfig.defaults()
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=128,
            avg_bitrate_kbps=130,
            format="Opus",
        )
        probe = V0ProbeEvidence(
            kind="lossless_source_v0",
            min_bitrate_kbps=191,
            avg_bitrate_kbps=224,
        )
        contract = TargetQualityContract.from_explicit_label("opus 128")
        legacy = AudioQualityMeasurement(
            min_bitrate_kbps=191,
            avg_bitrate_kbps=224,
            format="opus 128",
        )
        source = AudioQualityMeasurement(
            min_bitrate_kbps=742,
            avg_bitrate_kbps=811,
            format="FLAC",
        )

        old_decision = measured_import_decision(
            MeasuredImportDecisionInput(legacy, existing), cfg=cfg
        )
        new_decision = measured_import_decision(
            MeasuredImportDecisionInput(
                source, existing, False, contract, probe
            ),
            cfg=cfg,
        )
        self.assertEqual(new_decision, old_decision)
        output = AudioQualityMeasurement(
            min_bitrate_kbps=121,
            avg_bitrate_kbps=128,
            format="Opus",
        )
        self.assertEqual(
            quality_gate_decision(output, cfg=cfg, target_contract=contract),
            quality_gate_decision(
                AudioQualityMeasurement(
                    min_bitrate_kbps=121,
                    avg_bitrate_kbps=128,
                    format="opus 128",
                ),
                cfg=cfg,
            ),
        )

    def test_single_track_bare_mp3_preserves_legacy_cbr_projection(self):
        projected_bitrates = [128]
        projected_is_cbr = projected_is_cbr_from_bitrates(projected_bitrates)
        contract = TargetQualityContract.from_projection(
            "MP3", projected_is_cbr=projected_is_cbr
        )
        self.assertTrue(contract.is_cbr)
        source = AudioQualityMeasurement(
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            format="FLAC",
            is_cbr=True,
        )
        current = AudioQualityMeasurement(
            min_bitrate_kbps=123,
            avg_bitrate_kbps=123,
            format="MP3",
            is_cbr=False,
        )
        proxy = AudioQualityMeasurement(
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            format="MP3",
            is_cbr=projected_is_cbr,
        )
        cfg = QualityRankConfig.defaults()

        projected = measured_import_decision(
            MeasuredImportDecisionInput(source, current, True, contract, None),
            cfg=cfg,
        )
        legacy = measured_import_decision(
            MeasuredImportDecisionInput(proxy, current, True), cfg=cfg
        )
        self.assertEqual(projected, legacy)
        self.assertEqual(projected.decision, "transcode_upgrade")
        with self.assertRaisesRegex(ValueError, "bare MP3"):
            TargetQualityContract.from_explicit_label("MP3")
        self.assertFalse(hasattr(TargetQualityContract, "from_format"))

        pipeline = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=False,
            existing_min_bitrate=123,
            existing_avg_bitrate=123,
            existing_format="MP3",
            existing_is_cbr=False,
            post_conversion_min_bitrate=128,
            post_conversion_is_cbr=projected_is_cbr,
            converted_count=1,
        )
        self.assertEqual(pipeline["stage2_import"], "transcode_upgrade")

    def test_projection_mode_covers_one_multi_same_and_multi_different(self):
        cases = (
            ([128], True),
            ([128, 128], True),
            ([128, 129], False),
        )
        for bitrates, expected in cases:
            with self.subTest(bitrates=bitrates):
                mode = projected_is_cbr_from_bitrates(bitrates)
                contract = TargetQualityContract.from_projection(
                    "MP3", projected_is_cbr=mode
                )
                self.assertEqual(mode, expected)
                self.assertEqual(contract.is_cbr, expected)

    def test_bare_mp3_requires_mode_after_case_and_whitespace_normalization(self):
        for label in ("MP3", " mp3 ", "\tMp3\n"):
            with self.subTest(label=label), self.assertRaisesRegex(
                ValueError, "bare MP3"
            ):
                TargetQualityContract.from_explicit_label(label)

    def test_numeric_mp3_target_is_explicitly_cbr(self):
        self.assertTrue(
            TargetQualityContract.from_explicit_label("mp3 192").is_cbr
        )
        self.assertFalse(
            TargetQualityContract.from_explicit_label("mp3 v2").is_cbr
        )

    def test_explicit_mp3_labels_ignore_contradictory_projected_modes(self):
        cfg = QualityRankConfig.defaults()
        v0_output = AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=245,
            format="MP3",
            is_cbr=True,
        )
        cbr_output = AudioQualityMeasurement(
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            format="MP3",
            is_cbr=False,
        )
        for supplied_mode in (False, True):
            with self.subTest(label="mp3 v0", supplied_mode=supplied_mode):
                contract = TargetQualityContract.from_projection(
                    "mp3 v0", projected_is_cbr=supplied_mode
                )
                self.assertFalse(contract.is_cbr)
                self.assertEqual(
                    gate_rank(v0_output, cfg, target_contract=contract).name,
                    "TRANSPARENT",
                )
                self.assertEqual(
                    quality_gate_decision(
                        v0_output, cfg=cfg, target_contract=contract
                    ),
                    "requeue_upgrade",
                )
            with self.subTest(label="mp3 320", supplied_mode=supplied_mode):
                contract = TargetQualityContract.from_projection(
                    "mp3 320", projected_is_cbr=supplied_mode
                )
                self.assertTrue(contract.is_cbr)
                self.assertEqual(
                    gate_rank(cbr_output, cfg, target_contract=contract).name,
                    "TRANSPARENT",
                )
                self.assertEqual(
                    quality_gate_decision(
                        cbr_output, cfg=cfg, target_contract=contract
                    ),
                    "requeue_upgrade",
                )

    def test_early_downgrade_keeps_projected_target_for_dispatch_audit(self):
        decision = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=True,
            avg_bitrate=820,
            spectral_grade="genuine",
            existing_min_bitrate=900,
            existing_avg_bitrate=900,
            existing_format="FLAC",
            converted_count=1,
            post_conversion_min_bitrate=220,
            verified_lossless_target="opus 128",
            candidate_v0_probe_min=220,
            candidate_v0_probe_avg=240,
        )

        self.assertEqual(decision["stage2_import"], "downgrade")
        self.assertEqual(decision["target_final_format"], "opus 128")


class TestQualityLineageGenerated(unittest.TestCase):
    @given(
        grade=st.sampled_from(("genuine", "suspect", "likely_transcode")),
        bitrate=st.one_of(st.none(), st.integers(min_value=32, max_value=500)),
        payload_size=st.integers(min_value=1, max_value=128),
    )
    @example(grade="genuine", bitrate=96, payload_size=27)
    def test_attempt_scan_populates_only_the_exact_current_snapshot(
        self,
        grade: str,
        bitrate: int | None,
        payload_size: int,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="generated-have"))
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"x" * payload_size)
            evidence = make_album_quality_evidence(
                mb_release_id="generated-have",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=stored,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade=grade,
                    bitrate_kbps=bitrate,
                ),
                measured_existing_path=source,
            )

            assert result.evidence is not None
            assert_exact_current_spectral_persisted(
                expected_grade=grade,
                expected_bitrate=bitrate,
                actual_grade=result.evidence.measurement.spectral_grade,
                actual_bitrate=(
                    result.evidence.measurement.spectral_bitrate_kbps
                ),
            )

    @given(
        stale_grade=st.sampled_from(("genuine", "suspect", "likely_transcode")),
        stale_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=500)),
        fresh_grade=st.sampled_from(("genuine", "suspect", "likely_transcode")),
        fresh_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=500)),
        payload_size=st.integers(min_value=1, max_value=128),
    )
    @example(
        stale_grade="likely_transcode", stale_bitrate=128,
        fresh_grade="genuine", fresh_bitrate=160, payload_size=27,
    )
    def test_fresh_audit_overwrites_stale_landmine_grade(
        self,
        stale_grade: str,
        stale_bitrate: int | None,
        fresh_grade: str,
        fresh_bitrate: int | None,
        payload_size: int,
    ) -> None:
        """Issue #815 fresh-audit-wins property. A legacy landmine — an
        installed-subject evidence row whose spectral grade disagrees with a
        fresh audit of its matched-fingerprint bytes (a state a clean forward
        run can never produce) — is always overwritten by a successful fresh
        measured audit; the stale grade never survives."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="landmine-have"))
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"y" * payload_size)
            evidence = make_album_quality_evidence(
                mb_release_id="landmine-have",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=192,
                    avg_bitrate_kbps=192,
                    median_bitrate_kbps=192,
                    format="MP3",
                    spectral_grade=stale_grade,
                    spectral_bitrate_kbps=stale_bitrate,
                    spectral_subject="installed",
                    spectral_provenance="measured",
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=stored,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade=fresh_grade,
                    bitrate_kbps=fresh_bitrate,
                ),
                measured_existing_path=source,
            )

            assert result.evidence is not None
            # The fresh audit wins on the returned row...
            assert_exact_current_spectral_persisted(
                expected_grade=fresh_grade,
                expected_bitrate=fresh_bitrate,
                actual_grade=result.evidence.measurement.spectral_grade,
                actual_bitrate=(
                    result.evidence.measurement.spectral_bitrate_kbps
                ),
            )
            # ...and durably, not just in the returned struct.
            reloaded = db.load_album_quality_evidence_by_id(stored.id)
            assert reloaded is not None
            assert_exact_current_spectral_persisted(
                expected_grade=fresh_grade,
                expected_bitrate=fresh_bitrate,
                actual_grade=reloaded.measurement.spectral_grade,
                actual_bitrate=reloaded.measurement.spectral_bitrate_kbps,
            )

    @given(
        label=st.sampled_from(("mp3 v0", "MP3 V0", "mp3 320", " MP3 320 ")),
        supplied_mode=st.booleans(),
    )
    @example(label="mp3 v0", supplied_mode=True)
    @example(label="mp3 320", supplied_mode=False)
    def test_explicit_mp3_label_owns_mode_and_gate_policy(
        self,
        label: str,
        supplied_mode: bool,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        expected_cbr = label.strip().lower() == "mp3 320"
        bitrate = 320 if expected_cbr else 245
        contract = TargetQualityContract.from_projection(
            label,
            projected_is_cbr=supplied_mode,
        )
        output = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format="MP3",
            is_cbr=not expected_cbr,
        )

        self.assertEqual(contract.is_cbr, expected_cbr)
        self.assertEqual(
            gate_rank(output, cfg, target_contract=contract).name,
            "TRANSPARENT",
        )
        self.assertEqual(
            quality_gate_decision(output, cfg=cfg, target_contract=contract),
            "requeue_upgrade",
        )

    @given(
        reject_fact=st.sampled_from(("audio_corrupt", "bad_hash", "nested", "empty")),
        bitrate=st.integers(min_value=1, max_value=320),
    )
    @example(reject_fact="audio_corrupt", bitrate=128)
    def test_measurement_only_rejects_never_invent_target_policy(
        self,
        reject_fact: str,
        bitrate: int,
    ) -> None:
        files = [] if reject_fact == "empty" else [
            AlbumQualityEvidenceFile(
                relative_path="01.mp3",
                size_bytes=1,
                mtime_ns=1,
                extension="mp3",
                container="mp3",
                codec="mp3",
            )
        ]
        measurement = PreimportMeasurement(
            audio_corrupt=reject_fact == "audio_corrupt",
            corrupt_files=(
                ["01.mp3"] if reject_fact == "audio_corrupt" else []
            ),
            matched_bad_hash_id=(1 if reject_fact == "bad_hash" else None),
            matched_bad_track_path=(
                "01.mp3" if reject_fact == "bad_hash" else None
            ),
            folder_layout="nested" if reject_fact == "nested" else "flat",
            audio_file_count=0 if reject_fact == "empty" else 1,
            filetype_band="mp3",
            min_bitrate_kbps=bitrate,
            is_vbr=False,
        )

        built = evidence_from_measurement(
            mb_release_id="generated-early-reject",
            source_path="/generated/source",
            measurement=measurement,
            files=files,
        )

        self.assertEqual(built.status, "ready")
        assert built.evidence is not None
        self.assertIsNone(built.evidence.target_format)
        self.assertIsNone(built.evidence.target_is_cbr)

    @given(
        prefix=st.sampled_from(("", " ", "\t", "\n ")),
        spelling=st.sampled_from(("mp3", "MP3", "Mp3", "mP3")),
        suffix=st.sampled_from(("", " ", "\t", " \n")),
    )
    @example(prefix=" ", spelling="MP3", suffix=" ")
    def test_bare_mp3_always_requires_an_explicit_mode(
        self,
        prefix: str,
        spelling: str,
        suffix: str,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "bare MP3"):
            TargetQualityContract.from_explicit_label(
                prefix + spelling + suffix
            )

    @given(
        prefix=st.sampled_from(("", " ", "\t", "\n ")),
        spelling=st.sampled_from(("mp3", "MP3", "Mp3", "mP3")),
        suffix=st.sampled_from(("", " ", "\t", " \n")),
        projected_is_cbr=st.booleans(),
    )
    @example(
        prefix=" ",
        spelling="MP3",
        suffix=" ",
        projected_is_cbr=False,
    )
    @example(
        prefix="\t",
        spelling="Mp3",
        suffix="\n",
        projected_is_cbr=True,
    )
    def test_projection_api_preserves_required_bare_mp3_mode(
        self,
        prefix: str,
        spelling: str,
        suffix: str,
        projected_is_cbr: bool,
    ) -> None:
        contract = TargetQualityContract.from_projection(
            prefix + spelling + suffix,
            projected_is_cbr=projected_is_cbr,
        )
        self.assertEqual(contract.is_cbr, projected_is_cbr)

    @given(
        projected_bitrates=st.lists(
            st.integers(min_value=32, max_value=200),
            min_size=1,
            max_size=8,
        ),
        existing=st.integers(min_value=32, max_value=320),
        existing_is_cbr=st.booleans(),
    )
    @example(
        projected_bitrates=[128],
        existing=123,
        existing_is_cbr=False,
    )
    @example(
        projected_bitrates=[128, 128],
        existing=123,
        existing_is_cbr=False,
    )
    @example(
        projected_bitrates=[128, 129],
        existing=123,
        existing_is_cbr=False,
    )
    def test_full_pipeline_preserves_legacy_projection_mode(
        self,
        projected_bitrates: list[int],
        existing: int,
        existing_is_cbr: bool,
    ) -> None:
        projected_min = min(projected_bitrates)
        projected_is_cbr = projected_is_cbr_from_bitrates(projected_bitrates)
        result = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=False,
            existing_min_bitrate=existing,
            existing_avg_bitrate=existing,
            existing_format="MP3",
            existing_is_cbr=existing_is_cbr,
            post_conversion_min_bitrate=projected_min,
            post_conversion_is_cbr=projected_is_cbr,
            converted_count=len(projected_bitrates),
        )
        legacy = measured_import_decision(
            MeasuredImportDecisionInput(
                AudioQualityMeasurement(
                    min_bitrate_kbps=projected_min,
                    avg_bitrate_kbps=projected_min,
                    format="MP3",
                    is_cbr=projected_is_cbr,
                ),
                AudioQualityMeasurement(
                    min_bitrate_kbps=existing,
                    avg_bitrate_kbps=existing,
                    format="MP3",
                    is_cbr=existing_is_cbr,
                ),
                True,
            ),
            cfg=QualityRankConfig.defaults(),
        )
        self.assertEqual(result["stage2_import"], legacy.decision)

    @given(
        source_min=st.integers(min_value=1, max_value=5000),
        source_avg=st.integers(min_value=1, max_value=5000),
        probe_min=st.integers(min_value=1, max_value=500),
        probe_avg=st.integers(min_value=1, max_value=500),
        target=st.sampled_from(["opus 128", "mp3 v0", "mp3 v2"]),
    )
    def test_lossless_source_probe_and_target_remain_disjoint(
        self,
        source_min: int,
        source_avg: int,
        probe_min: int,
        probe_avg: int,
        target: str,
    ) -> None:
        result = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=source_min,
                avg_bitrate_kbps=source_avg,
                format="FLAC",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=probe_min,
                avg_bitrate_kbps=probe_avg,
            ),
            target_quality_contract=(
                TargetQualityContract.from_explicit_label(target)
            ),
        )

        decoded = ImportResult.from_json(result.to_json())
        assert_source_target_lineage(decoded)
        assert decoded.source_measurement is not None
        assert decoded.v0_probe is not None
        self.assertEqual(decoded.source_measurement.min_bitrate_kbps, source_min)
        self.assertEqual(decoded.source_measurement.avg_bitrate_kbps, source_avg)
        self.assertEqual(decoded.v0_probe.min_bitrate_kbps, probe_min)
        self.assertEqual(decoded.v0_probe.avg_bitrate_kbps, probe_avg)
        built = evidence_from_import_result(
            mb_release_id="generated-mbid",
            source_path="/generated/source",
            import_result=decoded,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=source_avg,
                    mtime_ns=source_min,
                    extension="flac",
                    container="flac",
                    codec="flac",
                )
            ],
        )
        self.assertEqual(built.status, "ready")
        assert built.evidence is not None
        self.assertEqual(built.evidence.measurement, decoded.source_measurement)
        assert decoded.target_quality_contract is not None
        self.assertEqual(
            built.evidence.target_is_cbr,
            decoded.target_quality_contract.is_cbr,
        )
        assert built.evidence.v0_metric is not None
        self.assertEqual(built.evidence.v0_metric.min_bitrate_kbps, probe_min)
        self.assertEqual(built.evidence.v0_metric.avg_bitrate_kbps, probe_avg)

    @given(
        source_min=st.integers(min_value=1, max_value=500),
        source_avg=st.integers(min_value=1, max_value=500),
        probe_min=st.integers(min_value=1, max_value=500),
        probe_avg=st.integers(min_value=1, max_value=500),
        codec=st.sampled_from(["MP3", "Opus", "AAC"]),
    )
    def test_native_lossy_research_probe_never_changes_decision(
        self,
        source_min: int,
        source_avg: int,
        probe_min: int,
        probe_avg: int,
        codec: str,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        source = AudioQualityMeasurement(
            min_bitrate_kbps=source_min,
            avg_bitrate_kbps=source_avg,
            format=codec,
        )
        current = AudioQualityMeasurement(
            min_bitrate_kbps=192,
            avg_bitrate_kbps=224,
            format="MP3",
        )
        research = V0ProbeEvidence(
            kind="native_lossy_research_v0",
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
        )
        baseline = measured_import_decision(
            MeasuredImportDecisionInput(source, current), cfg=cfg
        )
        with_research = measured_import_decision(
            MeasuredImportDecisionInput(
                source, current, False, None, research
            ),
            cfg=cfg,
        )
        self.assertEqual(with_research, baseline)

    @given(
        proxy_min=st.integers(min_value=1, max_value=500),
        proxy_avg=st.integers(min_value=1, max_value=500),
        target=st.sampled_from(["opus 128", "mp3 v0", "mp3 v2"]),
    )
    def test_new_wire_rows_reject_target_labelled_proxy_measurements(
        self,
        proxy_min: int,
        proxy_avg: int,
        target: str,
    ) -> None:
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=proxy_min,
                avg_bitrate_kbps=proxy_avg,
                format=target,
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=proxy_min,
                avg_bitrate_kbps=proxy_avg,
            ),
            target_quality_contract=(
                TargetQualityContract.from_explicit_label(target)
            ),
        )
        with self.assertRaisesRegex(ValueError, "bare measured codec label"):
            planted_bad.to_json()

    @given(
        source_min=st.integers(min_value=300, max_value=5000),
        source_avg=st.integers(min_value=300, max_value=5000),
        probe_min=st.integers(min_value=80, max_value=320),
        probe_avg=st.integers(min_value=80, max_value=320),
        existing_min=st.integers(min_value=32, max_value=400),
        existing_avg=st.integers(min_value=32, max_value=400),
        target=st.sampled_from(["opus 128", "mp3 v0", "mp3 v2"]),
    )
    def test_contract_projection_preserves_old_decision_and_gate_policy(
        self,
        source_min: int,
        source_avg: int,
        probe_min: int,
        probe_avg: int,
        existing_min: int,
        existing_avg: int,
        target: str,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=existing_min,
            avg_bitrate_kbps=existing_avg,
            format="MP3",
        )
        proxy = AudioQualityMeasurement(
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
            format=target,
        )
        source = AudioQualityMeasurement(
            min_bitrate_kbps=source_min,
            avg_bitrate_kbps=source_avg,
            format="FLAC",
        )
        probe = V0ProbeEvidence(
            kind="lossless_source_v0",
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
        )
        contract = TargetQualityContract.from_explicit_label(target)

        old_decision = measured_import_decision(
            MeasuredImportDecisionInput(proxy, existing), cfg=cfg
        )
        new_decision = measured_import_decision(
            MeasuredImportDecisionInput(
                source, existing, False, contract, probe
            ),
            cfg=cfg,
        )
        self.assertEqual(new_decision, old_decision)

        output = AudioQualityMeasurement(
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
            format=target.split()[0],
        )
        self.assertEqual(
            quality_gate_decision(output, cfg=cfg, target_contract=contract),
            quality_gate_decision(proxy, cfg=cfg),
        )

    @given(
        source_is_cbr=st.booleans(),
        output_is_cbr=st.booleans(),
        projected_bitrates=st.lists(
            st.integers(min_value=32, max_value=320),
            min_size=1,
            max_size=8,
        ),
        existing=st.integers(min_value=32, max_value=320),
    )
    def test_bare_mp3_projection_mode_is_independent_of_source_and_output(
        self,
        source_is_cbr: bool,
        output_is_cbr: bool,
        projected_bitrates: list[int],
        existing: int,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        bitrate = min(projected_bitrates)
        projected_is_cbr = projected_is_cbr_from_bitrates(projected_bitrates)
        contract = TargetQualityContract.from_projection(
            "MP3", projected_is_cbr=projected_is_cbr
        )
        source = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format="FLAC",
            is_cbr=source_is_cbr,
        )
        current = AudioQualityMeasurement(
            min_bitrate_kbps=existing,
            avg_bitrate_kbps=existing,
            format="MP3",
            is_cbr=False,
        )
        legacy_projection = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format="MP3",
            is_cbr=projected_is_cbr,
        )
        self.assertEqual(
            measured_import_decision(
                MeasuredImportDecisionInput(
                    source, current, False, contract, None
                ),
                cfg=cfg,
            ),
            measured_import_decision(
                MeasuredImportDecisionInput(legacy_projection, current),
                cfg=cfg,
            ),
        )
        output = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format="MP3",
            is_cbr=output_is_cbr,
        )
        self.assertEqual(
            quality_gate_decision(output, cfg=cfg, target_contract=contract),
            quality_gate_decision(legacy_projection, cfg=cfg),
        )

    @given(
        explicit_label=st.sampled_from(
            ["opus 128", "mp3 v0", "mp3 192", "aac 128"]
        ),
        bitrate=st.integers(min_value=1, max_value=500),
    )
    def test_target_absence_never_allows_explicit_source_measurement(
        self, explicit_label: str, bitrate: int
    ) -> None:
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=bitrate,
                format=explicit_label,
            )
        )
        with self.assertRaisesRegex(ValueError, "bare measured codec label"):
            planted_bad.to_json()
        built = evidence_from_import_result(
            mb_release_id="bad-target-absent",
            source_path="/bad",
            import_result=planted_bad,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.mp3",
                    size_bytes=1,
                    mtime_ns=1,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                )
            ],
        )
        self.assertEqual(built.status, "incomplete")

    @given(source_codec=st.sampled_from(["FLAC", "WAV", "ALAC"]))
    def test_source_measurement_never_carries_output_lineage(
        self, source_codec: str
    ) -> None:
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                format=source_codec,
                was_converted_from=source_codec.lower(),
            )
        )
        with self.assertRaisesRegex(ValueError, "was_converted_from"):
            planted_bad.to_json()
        built = evidence_from_import_result(
            mb_release_id="bad-source-lineage",
            source_path="/bad",
            import_result=planted_bad,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=1,
                    mtime_ns=1,
                    extension="flac",
                    container="flac",
                    codec="flac",
                )
            ],
        )
        self.assertEqual(built.status, "incomplete")


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_exact_current_spectral_checker_rejects_a_blank_have(self):
        with self.assertRaisesRegex(AssertionError, "exact current evidence"):
            assert_exact_current_spectral_persisted(
                expected_grade="genuine",
                expected_bitrate=96,
                actual_grade=None,
                actual_bitrate=None,
            )

    def test_exact_current_spectral_checker_rejects_surviving_landmine(self):
        # #815 fresh-audit-wins: a stale likely_transcode/128 grade that
        # survived a fresh genuine/160 audit must trip the checker.
        with self.assertRaisesRegex(AssertionError, "exact current evidence"):
            assert_exact_current_spectral_persisted(
                expected_grade="genuine",
                expected_bitrate=160,
                actual_grade="likely_transcode",
                actual_bitrate=128,
            )

    def test_source_target_checker_rejects_target_labelled_proxy(self):
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=191,
                avg_bitrate_kbps=224,
                format="opus 128",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=191,
                avg_bitrate_kbps=224,
            ),
            target_quality_contract=TargetQualityContract.from_explicit_label(
                "opus 128"
            ),
        )

        with self.assertRaisesRegex(AssertionError, "bare codec"):
            assert_source_target_lineage(planted_bad)

    def test_checker_rejects_target_absent_explicit_source(self):
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=191,
                format="opus 128",
            )
        )
        with self.assertRaisesRegex(AssertionError, "bare codec"):
            assert_source_target_lineage(planted_bad)

    def test_checker_rejects_source_output_lineage(self):
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                format="FLAC",
                was_converted_from="flac",
            )
        )
        with self.assertRaisesRegex(AssertionError, "output lineage"):
            assert_source_target_lineage(planted_bad)
