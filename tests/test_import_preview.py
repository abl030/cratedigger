"""Tests for unified import preview service."""

import configparser
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any, TYPE_CHECKING
from unittest.mock import patch

from lib.config import CratediggerConfig
from lib.dispatch.types import ImportOneRun
from lib.import_preview import (
    ImportPreviewValues,
    _lossless_candidate_spectral_failure,
    _prefer_successful_spectral_detail,
    compose_attempt_spectral_audit,
    enrich_current_v0_research_for_preview,
    enrich_incomplete_current_evidence_for_request,
    prepare_current_evidence_for_failure,
    persist_exact_current_spectral_from_attempt,
    load_persisted_existing_spectral,
    measure_and_persist_candidate_evidence,
    preview_import_from_path,
    preview_import_from_values,
)
from lib.measurement import (
    AudioCodecProbeError,
    LocalFileInspection,
    PreimportMeasurement,
)
from lib.quality import (
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    ImportResult,
    QualityRankConfig,
    SpectralAnalysisDetail,
    SpectralDetail,
    TargetQualityContract,
    V0ProbeEvidence,
    full_pipeline_decision,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    snapshot_audio_files,
    snapshot_fingerprint,
)

from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    make_album_quality_evidence,
    make_audio_corrupt_validation_report,
    make_request_row,
)


def _preview_config() -> CratediggerConfig:
    ini = configparser.ConfigParser()
    ini["Beets Validation"] = {
        "harness_path": "/fake/harness/run_beets_harness.sh",
        "audio_check": "off",
    }
    ini["Pipeline DB"] = {"enabled": "true"}
    return CratediggerConfig.from_ini(ini)


class TestSpectralAuditMerge(unittest.TestCase):
    def test_lossless_candidate_requires_a_successful_usable_grade(self):
        cases = (
            ("absent", None),
            ("not_attempted", SpectralAnalysisDetail(attempted=False)),
            (
                "error",
                SpectralAnalysisDetail(
                    attempted=True,
                    error="RuntimeError: decoder failed",
                ),
            ),
            ("grade_none", SpectralAnalysisDetail(attempted=True, grade=None)),
            ("grade_error", SpectralAnalysisDetail(attempted=True, grade="error")),
        )
        for name, candidate in cases:
            with self.subTest(name=name):
                failure = _lossless_candidate_spectral_failure(
                    PreimportMeasurement(
                        lossless_candidate=True,
                        spectral_audit=SpectralDetail(candidate=candidate),
                    ),
                    lossless_candidate=True,
                )
                self.assertIsNotNone(failure)

    def test_wav_conversion_preserves_source_spectral(self):
        """WAV→Opus is a lossless-source derivative, just like FLAC→Opus."""
        from lib.import_preview import preserve_existing_source_spectral

        evidence = make_album_quality_evidence(
            mb_release_id="wav-derived",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="Opus",
                spectral_grade="genuine",
                was_converted_from="wav",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )

        self.assertTrue(preserve_existing_source_spectral(evidence))

    def test_source_anchor_alone_preserves_source_spectral(self):
        """R19: an enrichment-born provisional row (no was_converted_from)
        is still lossless-sourced — its source-subject anchor proves it.
        The 2026-07-17 deploy-night rows were minted genuine/installed
        because this predicate could not see anchor-only lineage.
        """
        from lib.import_preview import preserve_existing_source_spectral
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityV0Metric

        evidence = make_album_quality_evidence(
            mb_release_id="anchor-only",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=129,
                avg_bitrate_kbps=129,
                median_bitrate_kbps=129,
                format="Opus",
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=187,
                avg_bitrate_kbps=213,
                median_bitrate_kbps=210,
                subject=EVIDENCE_SUBJECT_SOURCE,
                provenance="carried",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.assertTrue(preserve_existing_source_spectral(evidence))

    def test_proof_alone_preserves_source_spectral(self):
        """R19: verified-lossless proof is lossless lineage by definition."""
        from lib.import_preview import preserve_existing_source_spectral
        from lib.quality import VerifiedLosslessProof

        evidence = make_album_quality_evidence(
            mb_release_id="proof-only",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=129,
                avg_bitrate_kbps=129,
                median_bitrate_kbps=129,
                format="Opus",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier="request_seed",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.assertTrue(preserve_existing_source_spectral(evidence))

    def test_native_row_without_lineage_is_not_preserved(self):
        """A native copy with no lossless lineage is scanned normally."""
        from lib.import_preview import preserve_existing_source_spectral

        evidence = make_album_quality_evidence(
            mb_release_id="native-mp3",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
            ),
        )
        self.assertFalse(preserve_existing_source_spectral(evidence))

    def test_candidate_measured_error_yields_to_harness_success(self):
        measured = SpectralAnalysisDetail(
            attempted=True, error="RuntimeError: measured failed")
        harness = SpectralAnalysisDetail(
            attempted=True, grade="genuine", suspect_pct=0.0)

        self.assertIs(
            _prefer_successful_spectral_detail(measured, harness), harness)

    def test_composition_keeps_preview_have_over_harness_duplicate(self):
        measured = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="likely_transcode", bitrate_kbps=224),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="likely_transcode", bitrate_kbps=224),
        )
        harness = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=228),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=122),
        )

        composed = compose_attempt_spectral_audit(measured, harness)

        assert composed.existing is not None
        self.assertEqual(composed.existing.grade, "likely_transcode")
        self.assertEqual(composed.existing.bitrate_kbps, 224)

    def test_authoritative_empty_evidence_does_not_revive_stale_scalars(self):
        db = FakePipelineDB()
        req = make_request_row(
            id=42,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=224,
        )
        db.seed_request(req)
        evidence = make_album_quality_evidence(
            mb_release_id=req["mb_release_id"],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=122,
                avg_bitrate_kbps=127,
                median_bitrate_kbps=127,
                format="Opus",
                spectral_grade=None,
                spectral_bitrate_kbps=None,
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        loaded, detail, authoritative = load_persisted_existing_spectral(
            db, 42)

        self.assertIsNotNone(loaded)
        self.assertTrue(authoritative)
        self.assertFalse(detail.attempted)
        self.assertIsNone(detail.grade)
        self.assertIsNone(detail.bitrate_kbps)

    def test_linked_missing_or_unreadable_evidence_does_not_use_scalars(self):
        req = make_request_row(
            id=42,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=224,
        )
        for load_side_effect in (None, RuntimeError("evidence unavailable")):
            with self.subTest(load_side_effect=load_side_effect):
                db = FakePipelineDB()
                db.seed_request(req)
                db.set_request_current_evidence(42, 999)
                context = (
                    patch.object(
                        db,
                        "load_album_quality_evidence_by_id",
                        side_effect=load_side_effect,
                    )
                    if load_side_effect is not None
                    else patch.object(
                        db,
                        "load_album_quality_evidence_by_id",
                        return_value=None,
                    )
                )
                with context:
                    loaded, detail, authoritative = (
                        load_persisted_existing_spectral(db, 42)
                    )

                self.assertIsNone(loaded)
                self.assertTrue(authoritative)
                self.assertFalse(detail.attempted)
                self.assertIsNone(detail.grade)
                self.assertIsNone(detail.bitrate_kbps)

    def test_existing_measured_error_yields_to_harness_success(self):
        measured = SpectralAnalysisDetail(
            attempted=True, grade="suspect",
            error="TypeError: malformed track detail")
        harness = SpectralAnalysisDetail(
            attempted=True, grade="suspect", bitrate_kbps=128,
            suspect_pct=60.0)

        self.assertIs(
            _prefer_successful_spectral_detail(measured, harness), harness)


class TestImportPreviewValues(unittest.TestCase):
    def test_existing_spectral_grade_field_preserves_struct_positional_order(self):
        fields = list(ImportPreviewValues.__struct_fields__)
        self.assertLess(
            fields.index("existing_spectral_bitrate"),
            fields.index("existing_spectral_grade"),
        )

    def test_values_preview_delegates_to_full_pipeline_shape(self):
        values = ImportPreviewValues(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            is_vbr=True,
        )

        preview = preview_import_from_values(values)

        direct = full_pipeline_decision(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            is_vbr=True,
        )
        self.assertEqual(preview.simulation, direct)
        self.assertEqual(preview.verdict, "would_import")
        self.assertIn("stage2_import:import", preview.stage_chain)

    def test_values_preview_classifies_spectral_reject_as_confident(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=False,
                min_bitrate=320,
                is_cbr=True,
                spectral_grade="suspect",
                spectral_bitrate=96,
                existing_spectral_bitrate=128,
            )
        )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertTrue(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "spectral_reject")

    def test_values_preview_keeps_import_that_quality_gate_would_requeue(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=False,
                min_bitrate=160,
                avg_bitrate=160,
                is_cbr=False,
                is_vbr=True,
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertTrue(preview.would_import)
        self.assertFalse(preview.confident_reject)
        self.assertFalse(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "requeue_upgrade")
        self.assertEqual(
            preview.stage_chain,
            [
                "preimport_nested:pass",
                "preimport_audio:pass",
                "stage0_spectral_gate:would_run",
                "stage2_import:import",
                "stage3_quality_gate:requeue_upgrade",
            ],
        )

    def test_values_preview_classifies_provisional_lossless_upgrade(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="suspect",
                spectral_bitrate=160,
                post_conversion_min_bitrate=228,
                post_conversion_is_cbr=False,
                converted_count=12,
                candidate_v0_probe_avg=228,
                existing_v0_probe_avg=171,
                verified_lossless_target="opus 128",
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertEqual(preview.reason, "provisional_lossless_upgrade")
        self.assertIn(
            "stage2_import:provisional_lossless_upgrade",
            preview.stage_chain,
        )
        assert preview.simulation is not None
        self.assertEqual(preview.simulation["target_final_format"], "opus 128")

    def test_values_preview_high_v0_override_imports_verified(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="likely_transcode",
                spectral_bitrate=160,
                post_conversion_min_bitrate=237,
                post_conversion_is_cbr=False,
                converted_count=12,
                candidate_v0_probe_avg=276,
                candidate_v0_probe_min=237,
                verified_lossless_target="opus 128",
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertEqual(preview.reason, "import")
        self.assertIn("stage2_import:import", preview.stage_chain)
        assert preview.simulation is not None
        self.assertTrue(preview.simulation["verified_lossless"])
        self.assertEqual(preview.simulation["final_status"], "imported")
        self.assertFalse(preview.simulation["keep_searching"])

    def test_values_preview_prefers_provisional_over_stage1_reject(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="likely_transcode",
                spectral_bitrate=128,
                existing_spectral_bitrate=160,
                post_conversion_min_bitrate=228,
                post_conversion_is_cbr=False,
                converted_count=12,
                existing_v0_probe_avg=171,
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertFalse(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "provisional_lossless_upgrade")
        self.assertIn("stage1_spectral:reject", preview.stage_chain)
        self.assertIn(
            "stage2_import:provisional_lossless_upgrade",
            preview.stage_chain,
        )

    def test_values_preview_classifies_suspect_lossless_downgrade(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="suspect",
                spectral_bitrate=160,
                post_conversion_min_bitrate=175,
                post_conversion_is_cbr=False,
                converted_count=12,
                candidate_v0_probe_avg=175,
                existing_v0_probe_avg=171,
            )
        )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertTrue(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "suspect_lossless_downgrade")

    def test_values_preview_classifies_lossless_source_locked(self):
        # Lossy candidate (is_flac=False) facing existing with comparable
        # lossless-source V0 probe — preview must classify as confident
        # reject so the importer never schedules it. Parallel to the
        # suspect_lossless_downgrade case above.
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=False,
                is_cbr=False,
                is_vbr=True,
                min_bitrate=176,
                avg_bitrate=205,
                spectral_grade="likely_transcode",
                spectral_bitrate=128,
                existing_min_bitrate=116,
                existing_avg_bitrate=131,
                existing_format="opus",
                existing_v0_probe_avg=240,
            )
        )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertTrue(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "lossless_source_locked")


class TestImportPreviewPath(unittest.TestCase):
    def _db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            status="unsearchable",
            min_bitrate=180,
            current_lossless_source_v0_probe_min_bitrate=128,
            current_lossless_source_v0_probe_avg_bitrate=171,
            current_lossless_source_v0_probe_median_bitrate=169,
            artist_name="Artist",
            album_title="Album",
        ))
        return db

    def _source_dir(self) -> str:
        source = tempfile.mkdtemp()
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"not real audio but never inspected in this test")
        return source

    def _beets_current(self, source: str) -> FakeBeetsDB:
        from lib.beets_db import AlbumInfo

        beets = FakeBeetsDB(library_root=source)
        beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            is_cbr=True,
            album_path=source,
            format="MP3",
        ))
        return beets

    def _seed_current_without_v0(
        self,
        db: FakePipelineDB,
        source: str,
    ):
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
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
        return stored

    def test_aac_m4a_preview_entrypoints_reuse_measurement_codec_probe(self):
        """Neither preview path may repeat M4A classification after measure."""
        from lib.dispatch.types import ImportOneRun
        from lib.measurement import ExistingSpectralAuditLookup

        run = ImportOneRun(
            command=("import_one",),
            returncode=0,
            stdout="",
            stderr="",
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=256,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=256,
                    format="AAC",
                ),
            ),
        )
        for entrypoint in ("worker", "direct"):
            with self.subTest(entrypoint=entrypoint):
                db = self._db()
                source = tempfile.mkdtemp()
                try:
                    with open(os.path.join(source, "01.m4a"), "wb") as handle:
                        handle.write(b"aac")
                    fake_beets = FakeBeetsDB()
                    with patch(
                        "lib.config.read_runtime_config",
                        return_value=_preview_config(),
                    ), patch(
                        "lib.beets_db.BeetsDB",
                        lambda **_kwargs: fake_beets,
                    ), patch(
                        "lib.measurement.ffprobe_audio_codec_name",
                        return_value="aac",
                    ) as codec_probe, patch(
                        "lib.import_preview.run_import_one",
                        return_value=run,
                    ):
                        if entrypoint == "worker":
                            result = measure_and_persist_candidate_evidence(
                                db,
                                request_id=42,
                                path=source,
                                run_import_fn=lambda **_kwargs: run,
                                existing_spectral_resolver=(
                                    lambda _release_id: ExistingSpectralAuditLookup()
                                ),
                            )
                        else:
                            result = preview_import_from_path(
                                db,
                                request_id=42,
                                path=source,
                            )

                    self.assertNotEqual(result.decision, "spectral_analysis_failed")
                    self.assertEqual(codec_probe.call_count, 1)
                finally:
                    shutil.rmtree(source, ignore_errors=True)

    def test_m4a_codec_probe_failure_is_measurement_failed_before_harness(self):
        from lib.measurement import ExistingSpectralAuditLookup

        db = self._db()
        source = tempfile.mkdtemp()
        harness_called = False
        try:
            with open(os.path.join(source, "01.m4a"), "wb") as handle:
                handle.write(b"unknown-codec")

            def run_import(**_kwargs: Any):
                nonlocal harness_called
                harness_called = True
                raise AssertionError("harness must not run after codec probe failure")

            fake_beets = FakeBeetsDB()
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ), patch(
                "lib.measurement.ffprobe_audio_codec_name",
                return_value=None,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    run_import_fn=run_import,
                    existing_spectral_resolver=(
                        lambda _release_id: ExistingSpectralAuditLookup()
                    ),
                )

            self.assertEqual(result.verdict, "measurement_failed")
            self.assertEqual(result.decision, "measurement_crashed")
            self.assertIn("codec probe", result.detail or "")
            self.assertFalse(harness_called)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def _crashed_run(self):
        """The live 2026-07-18 shape: a stage-2 crash mid-mint left a partial
        ImportResult with a real source_measurement but no proof/target."""
        from lib.dispatch.types import ImportOneRun

        return ImportOneRun(
            command=("import_one",),
            returncode=99,
            stdout="",
            stderr="",
            import_result=ImportResult(
                exit_code=99,
                decision="crash",
                error="AttributeError: 'Namespace' object has no attribute 'filetype'",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=767,
                    avg_bitrate_kbps=851,
                    median_bitrate_kbps=847,
                    format="FLAC",
                    spectral_grade="genuine",
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
            ),
        )

    def _assert_nothing_persisted(self, db: FakePipelineDB, source: str) -> None:
        stored = db.find_album_quality_evidence(
            mb_release_id="mbid-42",
            snapshot_fingerprint=snapshot_fingerprint(
                snapshot_audio_files(source)),
        )
        self.assertIsNone(
            stored, "a crashed ImportResult must never persist evidence")

    def test_worker_stage2_crash_is_measurement_failed_not_evidence_ready(self):
        from lib.measurement import ExistingSpectralAuditLookup

        db = self._db()
        source = self._source_dir()
        run = self._crashed_run()
        try:
            fake_beets = FakeBeetsDB()
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    import_job_id=7,
                    run_import_fn=lambda **_kwargs: run,
                    existing_spectral_resolver=(
                        lambda _release_id: ExistingSpectralAuditLookup()
                    ),
                )

            self.assertEqual(result.verdict, "measurement_failed")
            self.assertEqual(result.decision, "crash")
            self.assertIn("filetype", result.detail or "")
            self._assert_nothing_persisted(db, source)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_path_preview_stage2_crash_never_persists_evidence(self):
        db = self._db()
        source = self._source_dir()
        run = self._crashed_run()
        try:
            fake_beets = FakeBeetsDB()
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=run,
            ):
                result = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    import_job_id=7,
                    persist_candidate_evidence=True,
                )

            self.assertNotEqual(result.verdict, "evidence_ready")
            self._assert_nothing_persisted(db, source)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_loader_rebuilds_blank_source_path_current_evidence(self):
        """A blank-path HAVE row must be rebuilt, not reused authoritatively.

        download_log 37206 (French Quarter): the linked current evidence was
        a legacy backfill with ``source_path=''``; every enrichment guard
        refused it, so preview kept handing the importer a spectrally blind
        HAVE side. The preview loader must rebuild such rows from beets so
        the same preview's enrichment can complete them.
        """
        from lib.beets_db import AlbumInfo
        from lib.import_preview import load_current_evidence_for_preview
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path="",
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=186,
                    avg_bitrate_kbps=194,
                    median_bitrate_kbps=194,
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

            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=3,
                min_bitrate_kbps=186,
                avg_bitrate_kbps=194,
                median_bitrate_kbps=194,
                is_cbr=False,
                album_path=source,
                format="MP3",
            ))
            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
                result = load_current_evidence_for_preview(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root="",
                    preloaded_evidence=stored,
                )

            self.assertEqual(result.status, "ready")
            current = result.evidence
            assert current is not None
            self.assertEqual(current.source_path, source)
            linked_id = db.get_request_current_evidence_id(42)
            self.assertEqual(linked_id, stored.id)
            linked = db.load_album_quality_evidence_by_id(linked_id)
            assert linked is not None
            self.assertEqual(linked.source_path, source)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_loader_rebuilds_v1_current_evidence_for_import_attempt(self):
        """An actual import attempt must decide from a fresh v4 HAVE row."""
        from lib.beets_db import AlbumInfo
        from lib.import_preview import load_current_evidence_for_preview
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=256,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=256,
                    format="AAC",
                    is_cbr=True,
                ),
                lineage_version=1,
                on_disk_v0_research_attempted=True,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                is_cbr=True,
                album_path=source,
                format="AAC",
            ))
            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
                result = load_current_evidence_for_preview(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                    preloaded_evidence=stored,
                )

            self.assertEqual(result.status, "ready")
            current = result.evidence
            assert current is not None
            self.assertEqual(current.id, stored.id)
            self.assertEqual(current.lineage_version, 4)
            self.assertEqual(current.measurement.format, "AAC")
            self.assertEqual(current.measurement.avg_bitrate_kbps, 256)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_persists_qigong_current_spectral_snapshot(self):
        """Qigong: the exact installed HAVE scan becomes durable evidence."""
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
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
                lineage_version=1,
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
                    grade="genuine",
                    bitrate_kbps=96,
                    suspect_pct=52.17,
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "ready")
            assert result.evidence is not None
            self.assertEqual(result.evidence.measurement.spectral_grade, "genuine")
            self.assertEqual(
                result.evidence.measurement.spectral_bitrate_kbps,
                96,
            )
            self.assertEqual(result.evidence.id, stored.id)
            self.assertEqual(
                result.evidence.snapshot_fingerprint,
                stored.snapshot_fingerprint,
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_fresh_have_audit_overwrites_stale_installed_grade(self):
        """Issue #815 fresh-audit-wins pin (Shugo Tokumaru EXIT, request 4351).

        An installed-subject evidence row carrying a STALE likely_transcode/128
        (a legacy landmine seeded on a matched fingerprint — a state a clean
        forward run can never produce) is re-persisted to the fresh genuine/160
        audit of the exact same bytes. Pre-#815 the fill-only-if-NULL early
        return discarded the fresh audit; the stale 128 then decided the
        dl 37742 import and a fake-320 replaced the genuine 192 copy.
        """
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=192,
                    avg_bitrate_kbps=192,
                    median_bitrate_kbps=192,
                    format="MP3",
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=128,
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
                    grade="genuine",
                    bitrate_kbps=160,
                    suspect_pct=30.0,
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "ready")
            assert result.evidence is not None
            self.assertEqual(
                result.evidence.measurement.spectral_grade, "genuine")
            self.assertEqual(
                result.evidence.measurement.spectral_bitrate_kbps, 160)
            # The overwrite is durable and stamped measured/installed.
            reloaded = db.load_album_quality_evidence_by_id(stored.id)
            assert reloaded is not None
            self.assertEqual(reloaded.measurement.spectral_grade, "genuine")
            self.assertEqual(reloaded.measurement.spectral_bitrate_kbps, 160)
            self.assertEqual(
                reloaded.measurement.spectral_subject, "installed")
            self.assertEqual(
                reloaded.measurement.spectral_provenance, "measured")
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_fresh_audit_never_overwrites_lossless_source_carried_grade(self):
        """R19 must-still-work: a lossless-sourced row that already carries a
        source-subject grade is NEVER overwritten by an installed-derivative
        fresh audit, even under #815 fresh-audit-wins."""
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityV0Metric

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=129,
                    avg_bitrate_kbps=129,
                    median_bitrate_kbps=129,
                    format="Opus",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=140,
                    spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                    spectral_provenance="carried",
                ),
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=187,
                    avg_bitrate_kbps=213,
                    median_bitrate_kbps=210,
                    subject=EVIDENCE_SUBJECT_SOURCE,
                    provenance="carried",
                ),
                codec="opus",
                container="opus",
                storage_format="Opus",
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
                    grade="genuine",
                    bitrate_kbps=200,
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "skipped")
            reloaded = db.load_album_quality_evidence_by_id(stored.id)
            assert reloaded is not None
            self.assertEqual(reloaded.measurement.spectral_grade, "suspect")
            self.assertEqual(
                reloaded.measurement.spectral_subject, EVIDENCE_SUBJECT_SOURCE)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_never_persists_onto_lossless_sourced_row(self):
        """R19 guard: a lossless-sourced row (source anchor, empty spectral)
        must refuse the installed-derivative scan — the source grade
        governs; the slot stays empty until it is carried in. Reproduces
        the 2026-07-17 deploy-night minting exactly.
        """
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityV0Metric

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-6108",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=129,
                    avg_bitrate_kbps=129,
                    median_bitrate_kbps=129,
                    format="Opus",
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                ),
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=187,
                    avg_bitrate_kbps=213,
                    median_bitrate_kbps=210,
                    subject=EVIDENCE_SUBJECT_SOURCE,
                    provenance="carried",
                ),
                codec="opus",
                container="opus",
                storage_format="Opus",
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
                    grade="genuine",
                    bitrate_kbps=128,
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "skipped")
            refreshed = db.load_album_quality_evidence_by_id(stored.id)
            assert refreshed is not None
            self.assertIsNone(refreshed.measurement.spectral_grade)
            self.assertIsNone(refreshed.measurement.spectral_subject)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_accepts_moved_path_with_the_exact_fingerprint(self):
        db = self._db()
        source = self._source_dir()
        other = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
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
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)
            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                ),
                measured_existing_path=other,
            )

            self.assertEqual(result.status, "ready")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertEqual(persisted.measurement.spectral_grade, "genuine")
            self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 96)
            self.assertEqual(persisted.source_path, source)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)
            shutil.rmtree(other, ignore_errors=True)

    def test_fresh_have_failure_overrides_stored_spectral_success(self):
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade="genuine",
                    spectral_subject="installed",
                    spectral_provenance="measured",
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    error="RuntimeError: fresh HAVE scan failed",
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "incomplete")
            self.assertIn("fresh HAVE scan failed", result.reason or "")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_worker_wires_have_scan_into_current_evidence(self):
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
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
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)
            measurement = PreimportMeasurement(
                audio_corrupt=True,
                corrupt_files=["01.mp3"],
                audio_validation=make_audio_corrupt_validation_report(
                    "01.mp3",
                ),
                folder_layout="flat",
                audio_file_count=1,
                existing_spectral_path=source,
                spectral_audit=SpectralDetail(
                    candidate=SpectralAnalysisDetail(
                        attempted=True,
                        grade="likely_transcode",
                        bitrate_kbps=96,
                    ),
                    existing=SpectralAnalysisDetail(
                        attempted=True,
                        grade="genuine",
                        bitrate_kbps=96,
                    ),
                ),
            )
            candidate = make_album_quality_evidence(
                mb_release_id="mbid-42-candidate"
            )
            with patch(
                "lib.beets_db.BeetsDB",
                return_value=self._beets_current(source),
            ), patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(filetype="mp3"),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=measurement,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    persist_measurement_fn=(
                        lambda *args, **kwargs: EvidenceBuildResult(
                            candidate,
                            "ready",
                        )
                    ),
                )

            self.assertEqual(result.verdict, "evidence_ready")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertEqual(persisted.measurement.spectral_grade, "genuine")
            self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 96)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_attempt_is_persisted_once_after_failure(self):
        db = self._db()
        source = self._source_dir()
        calls: list[str] = []
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def failed_probe(path: str):
                calls.append(path)
                raise RuntimeError("ffmpeg failed")

            first = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=failed_probe,
            )
            second = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=failed_probe,
            )

            self.assertEqual(first.status, "ready")
            self.assertEqual(second.status, "ready")
            self.assertEqual(calls, [source])
            assert second.evidence is not None
            self.assertTrue(second.evidence.on_disk_v0_research_attempted)
            self.assertIsNone(second.evidence.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_persists_neutral_metric(self):
        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None
            result = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=lambda _path: V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                ),
            )

            self.assertEqual(result.status, "ready")
            assert result.evidence is not None
            self.assertTrue(result.evidence.on_disk_v0_research_attempted)
            assert result.evidence.v0_metric is not None
            self.assertEqual(
                result.evidence.v0_metric.subject,
                "installed",
            )
            self.assertEqual(result.evidence.v0_metric.provenance, "measured")
            self.assertEqual(result.evidence.v0_metric.avg_bitrate_kbps, 259)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_requires_exact_current_snapshot(self):
        db = self._db()
        source = self._source_dir()
        calls: list[str] = []
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def probe(path: str):
                calls.append(path)
                return None

            wrong_id = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id + 1,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=probe,
            )
            with open(os.path.join(source, "01.mp3"), "ab") as handle:
                handle.write(b"changed")
            stale = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=probe,
            )

            self.assertEqual(wrong_id.status, "stale")
            self.assertEqual(stale.status, "stale")
            self.assertEqual(calls, [])
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_releases_claim_when_probe_changes_files(self):
        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def mutating_probe(path: str) -> V0ProbeEvidence:
                with open(os.path.join(path, "01.mp3"), "ab") as handle:
                    handle.write(b"changed during probe")
                return V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                )

            result = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=mutating_probe,
            )

            self.assertEqual(result.status, "stale")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertFalse(persisted.on_disk_v0_research_attempted)
            self.assertIsNone(persisted.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_loader_rejects_have_when_v0_probe_changes_files(self):
        """A stale enrichment result must invalidate the whole preview HAVE."""
        from lib.import_preview import load_current_evidence_for_preview

        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)

            def mutating_probe(path: str) -> V0ProbeEvidence:
                with open(os.path.join(path, "01.mp3"), "ab") as handle:
                    handle.write(b"changed during wrapper probe")
                return V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                )

            def mutating_enrichment(*args: Any, **kwargs: Any):
                return enrich_current_v0_research_for_preview(
                    *args,
                    **kwargs,
                    probe_fn=mutating_probe,
                )

            with patch(
                "lib.beets_db.BeetsDB",
                return_value=self._beets_current(source),
            ):
                result = load_current_evidence_for_preview(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                    preloaded_evidence=current,
                    enrich_current_fn=mutating_enrichment,
                )

            self.assertEqual(result.status, "stale")
            self.assertIsNone(result.evidence)
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertFalse(persisted.on_disk_v0_research_attempted)
            self.assertIsNone(persisted.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_releases_claim_when_current_link_changes(self):
        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def relinking_probe(_path: str) -> V0ProbeEvidence:
                db.set_request_current_evidence(42, None)
                return V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                )

            result = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=relinking_probe,
            )

            self.assertEqual(result.status, "stale")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertFalse(persisted.on_disk_v0_research_attempted)
            self.assertIsNone(persisted.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def _direct_preview_override(self, db: FakePipelineDB) -> int | None:
        source = self._source_dir()
        run = SimpleNamespace(
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=245,
                    median_bitrate_kbps=245,
                    format="MP3",
                ),
            )
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="flac",
                    min_bitrate_bps=900000,
                    is_vbr=False,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                ),
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=run,
            ) as mock_run:
                preview_import_from_path(db, request_id=42, path=source)
            return mock_run.call_args.kwargs["override_min_bitrate"]
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_direct_preview_no_fk_ignores_request_spectral_floor(self):
        db = self._db()
        db.request(42).update(
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=96,
        )

        self.assertIsNone(self._direct_preview_override(db))

    def test_direct_preview_authoritative_empty_ignores_stale_scalars(self):
        db = self._db()
        db.request(42).update(
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=96,
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade=None,
                spectral_bitrate_kbps=None,
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        # Exact absence discards the stale linked row wholesale: no HAVE
        # bitrate/spectral/V0/override input may survive into the dry run.
        self.assertIsNone(self._direct_preview_override(db))

    def test_direct_preview_ambiguous_current_fails_before_measurement(self):
        db = self._db()
        source = self._source_dir()
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_ids_for_release("mbid-42", [1, 2])
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ), patch(
                "lib.import_preview.measure_preimport_state",
            ) as mock_measure:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            self.assertEqual(preview.decision, "current_evidence_failed")
            self.assertIn("ambiguous_current", preview.detail or "")
            mock_measure.assert_not_called()
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_worker_stale_have_enrichment_fails_before_measurement(
        self,
    ):
        """A lost HAVE authority cannot degrade into an absent comparison."""
        db = self._db()
        source = self._source_dir()
        try:
            def stale_current(*_args: Any, **_kwargs: Any) -> EvidenceBuildResult:
                return EvidenceBuildResult(
                    None,
                    "stale",
                    "current files changed during V0 probe",
                )

            with patch(
                "lib.import_preview.inspect_local_files",
            ) as inspect, patch(
                "lib.import_preview.run_import_one",
            ) as run_import:
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    current_evidence_loader=stale_current,
                )

            self.assertEqual(result.verdict, "measurement_failed")
            self.assertEqual(result.decision, "current_evidence_failed")
            self.assertIn("stale", result.detail or "")
            inspect.assert_not_called()
            run_import.assert_not_called()
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_direct_preview_rebuilds_changed_or_poisoned_link_before_use(self):
        for poisoned_identity in (False, True):
            with self.subTest(poisoned_identity=poisoned_identity):
                db = self._db()
                candidate = self._source_dir()
                current = self._source_dir()
                fake_beets = FakeBeetsDB()
                try:
                    linked = make_album_quality_evidence(
                        mb_release_id=(
                            "other-exact-release"
                            if poisoned_identity
                            else "mbid-42"
                        ),
                        files=snapshot_audio_files(current),
                        measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=320,
                            avg_bitrate_kbps=320,
                            format="MP3",
                            spectral_grade="likely_transcode",
                            spectral_bitrate_kbps=96,
                            spectral_subject="source",
                            spectral_provenance="measured",
                        ),
                    )
                    db.upsert_album_quality_evidence(linked)
                    stored = db.find_album_quality_evidence(
                        mb_release_id=linked.mb_release_id,
                        snapshot_fingerprint=linked.snapshot_fingerprint,
                    )
                    assert stored is not None and stored.id is not None
                    db.set_request_current_evidence(42, stored.id)
                    if not poisoned_identity:
                        with open(os.path.join(current, "01.mp3"), "ab") as fh:
                            fh.write(b"changed-current-bytes")
                    from lib.beets_db import AlbumInfo
                    fake_beets.set_album_info(
                        "mbid-42",
                        AlbumInfo(
                            album_id=1,
                            track_count=1,
                            min_bitrate_kbps=128,
                            avg_bitrate_kbps=128,
                            median_bitrate_kbps=128,
                            is_cbr=True,
                            album_path=current,
                            format="MP3",
                        ),
                    )
                    run = SimpleNamespace(import_result=ImportResult(
                        decision="import",
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=245,
                            avg_bitrate_kbps=245,
                            format="MP3",
                        ),
                    ))
                    with patch(
                        "lib.config.read_runtime_config",
                        return_value=CratediggerConfig(
                            beets_harness_path="/fake/harness/run_beets_harness.sh",
                            pipeline_db_enabled=True,
                        ),
                    ), patch(
                        "lib.beets_db.BeetsDB",
                        lambda **_kwargs: fake_beets,
                    ), patch(
                        "lib.import_preview.inspect_local_files",
                        return_value=LocalFileInspection(filetype="mp3"),
                    ), patch(
                        "lib.import_preview.measure_preimport_state",
                        return_value=PreimportMeasurement(
                            folder_layout="flat",
                            audio_file_count=1,
                        ),
                    ) as mock_measure, patch(
                        "lib.import_preview.run_import_one",
                        return_value=run,
                    ) as mock_run:
                        preview_import_from_path(
                            db,
                            request_id=42,
                            path=candidate,
                        )

                    measurement_args = mock_measure.call_args.kwargs
                    self.assertFalse(
                        measurement_args["preserve_existing_source_spectral"]
                    )
                    if poisoned_identity:
                        self.assertIsNone(
                            measurement_args["existing_spectral_evidence"].grade
                        )
                    refreshed = db.load_album_quality_evidence_by_id(
                        db.get_request_current_evidence_id(42)
                    )
                    assert refreshed is not None
                    self.assertEqual(refreshed.mb_release_id, "mbid-42")
                    self.assertEqual(refreshed.source_path, current)
                    self.assertEqual(
                        refreshed.snapshot_fingerprint,
                        snapshot_fingerprint(snapshot_audio_files(current)),
                    )
                    self.assertNotEqual(
                        mock_run.call_args.kwargs["override_min_bitrate"],
                        320,
                    )
                    self.assertIsNone(
                        mock_run.call_args.kwargs["existing_v0_probe"]
                    )
                finally:
                    shutil.rmtree(candidate, ignore_errors=True)
                    shutil.rmtree(current, ignore_errors=True)

    def test_real_path_preview_runs_harness_dry_run_without_db_writes(self):
        db = self._db()
        source = self._source_dir()
        before = sorted(os.listdir(source))
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           beets_directory="/srv/music/Beets",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                           spectral_audit=SpectralDetail(
                               candidate=SpectralAnalysisDetail(
                                   attempted=True,
                                   grade="likely_transcode",
                                   bitrate_kbps=224,
                               ),
                               existing=SpectralAnalysisDetail(
                                   attempted=True,
                                   grade="likely_transcode",
                                   bitrate_kbps=224,
                               ),
                           ),
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=245,
                                   avg_bitrate_kbps=245,
                                   median_bitrate_kbps=245,
                                   format="MP3",
                               ),
                               spectral=SpectralDetail(
                                   candidate=SpectralAnalysisDetail(
                                       attempted=True,
                                       grade="genuine",
                                       bitrate_kbps=228,
                                   ),
                                   existing=SpectralAnalysisDetail(
                                       attempted=True,
                                       grade="genuine",
                                       bitrate_kbps=122,
                                   ),
                               ),
                           )
                       )) as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "would_import")
            self.assertEqual(sorted(os.listdir(source)), before)
            self.assertEqual(db.download_logs, [])
            self.assertEqual(db.denylist, [])
            self.assertTrue(mock_run.call_args.kwargs["dry_run"])
            self.assertIsNone(mock_run.call_args.kwargs["request_id"])
            self.assertNotIn("beets_library_root", mock_run.call_args.kwargs)
            assert preview.import_result is not None
            assert preview.import_result.spectral.existing is not None
            self.assertEqual(
                preview.import_result.spectral.existing.grade,
                "likely_transcode",
            )
            self.assertEqual(
                preview.import_result.spectral.existing.bitrate_kbps,
                224,
            )
            # The request-row V0 stamps are audit-only.  With no linked
            # current evidence, preview must not reconstruct a policy probe
            # from those legacy scalars.
            self.assertIsNone(mock_run.call_args.kwargs["existing_v0_probe"])
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_path_preview_persists_candidate_evidence_for_job_owner(self):
        """Post-migration 021: preview persists candidate evidence and wires
        the ``import_jobs.candidate_evidence_id`` FK. Loading via the FK
        chain returns the persisted row.
        """
        db = self._db()
        job = db.enqueue_import_job(
            "force_import",
            request_id=42,
            dedupe_key="force:42:/tmp/source",
            payload={"failed_path": "/tmp/source"},
        )
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=245,
                                   avg_bitrate_kbps=245,
                                   median_bitrate_kbps=245,
                                   format="MP3",
                               ),
                           )
                       )):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=False,
                    import_job_id=job.id,
                    persist_candidate_evidence=True,
                )

            self.assertEqual(preview.verdict, "would_import")
            evidence_id = db.get_import_job_candidate_evidence_id(job.id)
            self.assertIsNotNone(evidence_id)
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertEqual(loaded.measurement.avg_bitrate_kbps, 245)
            self.assertEqual([f.relative_path for f in loaded.files], ["01.mp3"])
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_path_preview_persists_candidate_evidence_for_download_log_owner(self):
        db = self._db()
        download_log_id = db.log_download(
            request_id=42,
            outcome="rejected",
            validation_result={"failed_path": "/tmp/failed"},
        )
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=245,
                                   avg_bitrate_kbps=245,
                                   median_bitrate_kbps=245,
                                   format="MP3",
                               ),
                           )
                       )):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=True,
                    download_log_id=download_log_id,
                    persist_candidate_evidence=True,
                )

            self.assertEqual(preview.verdict, "would_import")
            evidence_id = db.get_download_log_candidate_evidence_id(
                download_log_id
            )
            self.assertIsNotNone(evidence_id)
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertEqual(loaded.measurement.avg_bitrate_kbps, 245)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_configured_target_round_trips_when_request_target_is_null(self):
        db = self._db()
        download_log_id = db.log_download(
            request_id=42,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": "/tmp/config-target",
            },
        )
        source = tempfile.mkdtemp()
        with open(os.path.join(source, "01.flac"), "wb") as handle:
            handle.write(b"flac")
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                    verified_lossless_target="opus 128",
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="flac",
                    min_bitrate_bps=800000,
                    is_vbr=False,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                ),
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=SimpleNamespace(
                    import_result=ImportResult(
                        decision="downgrade",
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=800,
                            avg_bitrate_kbps=820,
                            median_bitrate_kbps=810,
                            format="FLAC",
                        ),
                        target_quality_contract=(
                            TargetQualityContract.from_explicit_label(
                                "opus 128"
                            )
                        ),
                    )
                ),
            ):
                preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=True,
                    download_log_id=download_log_id,
                    persist_candidate_evidence=True,
                )

            evidence_id = db.get_download_log_candidate_evidence_id(
                download_log_id
            )
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertEqual(loaded.measurement.format, "FLAC")
            self.assertEqual(loaded.target_format, "opus 128")
            self.assertFalse(loaded.target_is_cbr)
            self.assertEqual(loaded.lineage_version, 4)
            wrong_match = db.get_wrong_matches()[0]
            self.assertEqual(
                wrong_match["evidence_target_format"], "opus 128"
            )
            self.assertFalse(wrong_match["evidence_target_is_cbr"])
            self.assertEqual(wrong_match["evidence_lineage_version"], 4)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_audit_survives_evidence_persistence_failure(self):
        db = self._db()
        source = self._source_dir()
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="suspect", bitrate_kbps=128),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="genuine"),
        )
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True)), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(filetype="mp3")), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           audio_corrupt=True,
                           corrupt_files=["01.mp3"],
                           audio_validation=(
                               make_audio_corrupt_validation_report("01.mp3")
                           ),
                           folder_layout="flat",
                           audio_file_count=1,
                           spectral_audit=audit,
                       )):
                preview = measure_and_persist_candidate_evidence(
                    db, request_id=42, path=source,
                    persist_measurement_fn=(
                        lambda *args, **kwargs: (_ for _ in ()).throw(
                            RuntimeError("database unavailable"))
                    ),
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            self.assertEqual(preview.decision, "evidence_persist_failed")
            assert preview.import_result is not None
            self.assertEqual(preview.import_result.spectral, audit)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_badlands_corruption_outranks_lossless_spectral_failure(self):
        """dl 37604: a corrupt FLAC candidate is completed integrity
        evidence, not an infrastructure-class spectral measurement failure.
        """
        db = self._db()
        job = db.enqueue_import_job(
            "automation_import",
            request_id=42,
            dedupe_key="automation_import:request:42",
            payload={},
        )
        source = tempfile.mkdtemp()
        with open(os.path.join(source, "01.flac"), "wb") as handle:
            handle.write(b"truncated lossless bytes")
        decode_error = (
            "01.flac: Cannot determine format of input 0:0 after EOF; "
            "Invalid data found when processing input"
        )
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True,
                grade="error",
                error="ffmpeg could not decode corrupt source",
            ),
            existing=SpectralAnalysisDetail(
                attempted=True,
                grade="suspect",
            ),
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="flac",
                    min_bitrate_bps=900_000,
                    is_vbr=False,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    audio_corrupt=True,
                    corrupt_files=["01.flac"],
                    audio_validation=make_audio_corrupt_validation_report(
                        "01.flac",
                        detail=decode_error,
                    ),
                    audio_error=decode_error,
                    folder_layout="flat",
                    audio_file_count=1,
                    filetype_band="flac",
                    lossless_candidate=True,
                    min_bitrate_kbps=900,
                    is_vbr=False,
                    spectral_audit=audit,
                ),
            ), patch("lib.import_preview.run_import_one") as mock_run:
                preview = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    import_job_id=job.id,
                )

            self.assertEqual(preview.verdict, "evidence_ready")
            self.assertEqual(preview.decision, "audio_corrupt")
            mock_run.assert_not_called()
            evidence_id = db.get_import_job_candidate_evidence_id(job.id)
            self.assertIsNotNone(evidence_id)
            evidence = db.load_album_quality_evidence_by_id(evidence_id)
            assert evidence is not None
            self.assertTrue(evidence.audio_corrupt)
            self.assertEqual(evidence.audio_error, decode_error)
            self.assertEqual(
                [(file.relative_path, file.decode_ok) for file in evidence.files],
                [("01.flac", False)],
            )
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_production_preview_prepares_have_before_candidate_ready(self):
        order: list[str] = []

        class RecordingPipelineDB(FakePipelineDB):
            def claim_current_v0_research_attempt(
                self,
                *,
                request_id: int,
                expected_evidence_id: int,
                expected_snapshot_fingerprint: str,
            ) -> bool:
                order.append("prepare_have")
                super().claim_current_v0_research_attempt(
                    request_id=request_id,
                    expected_evidence_id=expected_evidence_id,
                    expected_snapshot_fingerprint=expected_snapshot_fingerprint,
                )
                # Model a concurrent preview winning the claim. The loader
                # must reload that committed marker without running ffmpeg;
                # this test is about orchestration order, not probe behavior.
                return False

        db = RecordingPipelineDB()
        request = self._db().get_request(42)
        assert request is not None
        db.seed_request(request)
        source = self._source_dir()

        def measure(*args, **kwargs):
            order.append("measure_candidate")
            return PreimportMeasurement(
                audio_corrupt=True,
                corrupt_files=["01.mp3"],
                audio_validation=make_audio_corrupt_validation_report(
                    "01.mp3",
                ),
                folder_layout="flat",
                audio_file_count=1,
            )

        def persist(*args, **kwargs):
            order.append("persist_candidate")
            return EvidenceBuildResult(
                make_album_quality_evidence(mb_release_id="candidate-ready"),
                "ready",
            )

        try:
            self._seed_current_without_v0(db, source)
            with patch(
                "lib.beets_db.BeetsDB",
                return_value=self._beets_current(source),
            ), patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(filetype="mp3"),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                side_effect=measure,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    persist_measurement_fn=persist,
                )

            self.assertEqual(result.verdict, "evidence_ready")
            self.assertEqual(
                order,
                ["prepare_have", "measure_candidate", "persist_candidate"],
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_audit_survives_harness_crash_and_no_json(self):
        from lib.dispatch.types import ImportOneRun
        db = self._db()
        source = self._source_dir()
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="suspect", bitrate_kbps=128),
            existing=SpectralAnalysisDetail(
                attempted=True, error="existing decode failed"),
        )
        measurement = PreimportMeasurement(
            folder_layout="flat", audio_file_count=1,
            spectral_audit=audit,
        )
        common = (
            patch("lib.config.read_runtime_config", return_value=CratediggerConfig(
                beets_harness_path="/fake/harness/run_beets_harness.sh",
                pipeline_db_enabled=True)),
            patch("lib.import_preview.inspect_local_files",
                  return_value=LocalFileInspection(filetype="mp3")),
            patch("lib.import_preview.measure_preimport_state",
                  return_value=measurement),
        )
        try:
            for decision, run_value in (
                ("harness_crashed", RuntimeError("harness exploded")),
                ("no_json_result", ImportOneRun(
                    command=(), returncode=1, stdout="",
                    stderr="no sentinel", import_result=None)),
            ):
                def run_import(*args: Any, **kwargs: Any) -> ImportOneRun:
                    if isinstance(run_value, Exception):
                        raise run_value
                    return run_value

                with common[0], common[1], common[2]:
                    preview = measure_and_persist_candidate_evidence(
                        db, request_id=42, path=source,
                        run_import_fn=run_import,
                    )
                self.assertEqual(preview.decision, decision)
                assert preview.import_result is not None
                self.assertEqual(preview.import_result.spectral, audit)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_source_change_during_preview_does_not_persist_candidate_evidence(self):
        db = self._db()
        job = db.enqueue_import_job(
            "force_import",
            request_id=42,
            dedupe_key="force:42:/tmp/source",
            payload={"failed_path": "/tmp/source"},
        )
        source = self._source_dir()

        def run_preview(*args, **kwargs):
            with open(os.path.join(source, "01.mp3"), "ab") as handle:
                handle.write(b"changed")
            return SimpleNamespace(
                import_result=ImportResult(
                    decision="import",
                    source_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=245,
                        avg_bitrate_kbps=245,
                        median_bitrate_kbps=245,
                        format="MP3",
                    ),
                )
            )

        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       side_effect=run_preview):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=False,
                    import_job_id=job.id,
                    persist_candidate_evidence=True,
                )

            self.assertEqual(preview.verdict, "uncertain")
            self.assertEqual(preview.decision, "source_changed_during_preview")
            # Source mutated mid-flight: preview must NOT wire the candidate
            # FK on the import_job row.
            self.assertIsNone(db.get_import_job_candidate_evidence_id(job.id))
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_audio_corrupt_is_confident_reject_without_denylist_side_effects(self):
        """U6: preview surfaces the five folder/audio-integrity facts as a
        confident_reject. Spectral / codec rank / V0 are NEVER decided in
        preview — those live in the importer's
        ``full_pipeline_decision_from_evidence``. Preview must also NEVER
        touch the denylist (importer owns that on reject via U11).
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=128000,
                           is_vbr=False,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           audio_corrupt=True,
                           corrupt_files=["01.mp3"],
                           audio_validation=(
                               make_audio_corrupt_validation_report("01.mp3")
                           ),
                           folder_layout="flat",
                           audio_file_count=0,
                       )), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "confident_reject")
            self.assertTrue(preview.cleanup_eligible)
            self.assertEqual(preview.decision, "audio_corrupt")
            self.assertEqual(db.denylist, [])
            mock_run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_bad_audio_hash_is_confident_reject_without_denylist_side_effects(self):
        """U6: preview must surface ``bad_audio_hash`` as confident_reject
        without writing to the denylist. The importer's unified reject path
        (U11) owns the denylist write.
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=128000,
                           is_vbr=False,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           matched_bad_hash_id=7,
                           matched_bad_track_path="01.mp3",
                           folder_layout="flat",
                           audio_file_count=0,
                       )), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "confident_reject")
            self.assertTrue(preview.cleanup_eligible)
            self.assertEqual(preview.decision, "bad_audio_hash")
            self.assertEqual(db.denylist, [])
            mock_run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_crash_degrades_to_uncertain_instead_of_raising(self):
        """A crashing measurement (e.g. an unprobeable .m4a raising
        AudioCodecProbeError) must degrade to the measurement_failed
        preview verdict like the worker path — never escape as an
        exception the web route would surface as a 500.
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="m4a",
                           min_bitrate_bps=256000,
                           is_vbr=False,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       side_effect=AudioCodecProbeError(
                           "ffprobe could not read 01.m4a")), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            self.assertEqual(preview.decision, "measurement_crashed")
            self.assertIn("AudioCodecProbeError", preview.detail or "")
            mock_run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_conversion_source_corruption_becomes_persisted_evidence(self):
        """A conversion-time decode failure rejoins the unified decider path."""
        db = self._db()
        download_log_id = db.log_download(request_id=42, outcome="failed")
        source = self._source_dir()
        report = AudioValidationReport(
            outcome="audio_corrupt",
            files_checked=1,
            files_failed=1,
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="01.mp3",
                    category="decode_error",
                    return_code=69,
                    stderr_excerpt="invalid frame",
                ),
            ],
        )
        result = ImportResult(
            decision="conversion_failed",
            error="1 conversion failed",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                format="MP3",
            ),
        )
        result.conversion.failed = 1
        result.conversion.source_validation = report
        result.conversion.source_validation_failed_paths = ["01.mp3"]
        run = ImportOneRun(
            command=("import_one.py",),
            returncode=1,
            stdout=result.to_sentinel_line(),
            stderr="one concise conversion summary",
            import_result=result,
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="mp3",
                    min_bitrate_bps=256000,
                    is_vbr=True,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                    filetype_band="mp3",
                    min_bitrate_kbps=256,
                ),
            ):
                preview = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    download_log_id=download_log_id,
                    run_import_fn=lambda **_kwargs: run,
                )

            self.assertEqual(preview.verdict, "evidence_ready")
            self.assertEqual(preview.decision, "audio_corrupt")
            evidence_id = db.get_download_log_candidate_evidence_id(
                download_log_id
            )
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertTrue(loaded.audio_corrupt)
            self.assertEqual(loaded.audio_validation, report)
            self.assertFalse(loaded.files[0].decode_ok)
            self.assertEqual(db.denylist, [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_conversion_world_failure_keeps_typed_measurement_audit(self):
        """An unavailable decoder is not persisted as bad content."""
        db = self._db()
        source = self._source_dir()
        report = AudioValidationReport(
            outcome="measurement_failed",
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="01.mp3",
                    category="process_unavailable",
                    stderr_excerpt="ffmpeg missing",
                ),
            ],
        )
        result = ImportResult(
            decision="conversion_failed",
            error="conversion failed",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                format="MP3",
            ),
        )
        result.conversion.failed = 1
        result.conversion.source_validation = report
        run = ImportOneRun(
            command=("import_one.py",),
            returncode=1,
            stdout=result.to_sentinel_line(),
            stderr="one concise conversion summary",
            import_result=result,
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="mp3",
                    min_bitrate_bps=256000,
                    is_vbr=True,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                    filetype_band="mp3",
                    min_bitrate_kbps=256,
                ),
            ):
                preview = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    run_import_fn=lambda **_kwargs: run,
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            assert preview.failure is not None
            self.assertEqual(preview.failure.audio_validation, report)
            self.assertEqual(db.album_quality_evidence, {})
            self.assertEqual(db.denylist, [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_legacy_path_does_not_call_run_preimport_gates(self):
        """U6/U8 anti-regression: the legacy ``run_preimport_gates`` shim
        was deleted in U8. If a future change reintroduces it (in
        lib.measurement or as a re-export from lib.import_preview), this
        guard fires.
        """
        import lib.import_preview as ip
        import lib.measurement as pi
        self.assertFalse(
            hasattr(ip, "run_preimport_gates"),
            "lib.import_preview must not re-export run_preimport_gates — "
            "preview measures only",
        )
        self.assertFalse(
            hasattr(pi, "run_preimport_gates"),
            "lib.measurement must not export run_preimport_gates — the shim "
            "was deleted in U8",
        )

    def test_missing_path_is_uncertain_not_cleanup_eligible(self):
        preview = preview_import_from_path(
            self._db(),
            request_id=42,
            path="/tmp/definitely-missing-cratedigger-preview",
        )

        self.assertEqual(preview.verdict, "uncertain")
        self.assertEqual(preview.decision, "path_missing")
        self.assertFalse(preview.cleanup_eligible)

    def test_path_preview_keeps_import_that_quality_gate_would_requeue(self):
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=160000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=160,
                                   avg_bitrate_kbps=160,
                                   median_bitrate_kbps=160,
                                   format="mp3",
                               ),
                           )
                       )):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "would_import")
            self.assertEqual(preview.decision, "import")
            self.assertEqual(preview.reason, "requeue_upgrade")
            self.assertFalse(preview.cleanup_eligible)
            self.assertEqual(
                preview.stage_chain,
                ["stage2_import:import", "stage3_quality_gate:requeue_upgrade"],
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)


if TYPE_CHECKING:
    from typing import cast

    from lib.import_preview import ImportPreviewDB as _PreviewDB
    from lib.pipeline_db import PipelineDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_preview_protocol: _PreviewDB = cast("PipelineDB", None)
    _fake_db_satisfies_preview_protocol: _PreviewDB = cast("FakePipelineDB", None)


class TestEnrichIncompleteCurrentEvidence(unittest.TestCase):
    """Failure-point HAVE enrichment fills only what's missing, once."""

    def _db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        return db

    def _source_dir(self) -> str:
        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"not real audio but never inspected in this test")
        return source

    def _seed_current(
        self,
        db: FakePipelineDB,
        source: str,
        *,
        spectral_present: bool,
        v0_attempted: bool = False,
    ):
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade="genuine" if spectral_present else None,
                spectral_bitrate_kbps=96 if spectral_present else None,
            ),
            v0_metric=None,
            on_disk_v0_research_attempted=v0_attempted,
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(42, stored.id)
        return stored

    def _spectral_recorder(self, detail: SpectralAnalysisDetail):
        calls: list[str] = []

        def analyzer(path: str) -> SpectralAnalysisDetail:
            calls.append(path)
            return detail

        return analyzer, calls

    def _probe_recorder(self):
        calls: list[str] = []

        def probe(path: str) -> V0ProbeEvidence:
            calls.append(path)
            return V0ProbeEvidence(
                kind="on_disk_research_v0",
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
            )

        return probe, calls

    def _good_scan(self) -> SpectralAnalysisDetail:
        return SpectralAnalysisDetail(
            attempted=True, grade="genuine", bitrate_kbps=96,
        )

    def _enrich(self, db, analyzer, probe):
        def load_current(db_arg, **_kwargs):
            evidence_id = db_arg.get_request_current_evidence_id(42)
            evidence = db_arg.load_album_quality_evidence_by_id(evidence_id)
            if evidence is None:
                return EvidenceBuildResult(
                    None,
                    "empty_current",
                    "exact album not in beets",
                )
            return EvidenceBuildResult(
                evidence,
                "ready",
                current_album_path=evidence.source_path,
            )

        return enrich_incomplete_current_evidence_for_request(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="",
            spectral_analyzer=analyzer,
            probe_fn=probe,
            load_fn=load_current,
        )

    def test_complete_row_skips_all_measurement(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(db, source, spectral_present=True, v0_attempted=True)
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, "complete")
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [])

    def test_preparation_preserves_an_existing_complete_current_row(self):
        db = self._db()
        source = self._source_dir()
        before = self._seed_current(
            db,
            source,
            spectral_present=True,
            v0_attempted=True,
        )

        calls: list[object] = []

        def load_current(*_args, **_kwargs):
            calls.append(_kwargs.get("preloaded_evidence"))
            return EvidenceBuildResult(before, "ready")

        outcome = prepare_current_evidence_for_failure(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root=source,
            load_fn=load_current,
        )

        current_id = db.get_request_current_evidence_id(42)
        self.assertEqual(outcome, "ready")
        self.assertEqual(calls, [before])
        self.assertEqual(current_id, before.id)
        self.assertEqual(
            db.load_album_quality_evidence_by_id(current_id),
            before,
        )

    def test_failure_refreshes_complete_v1_through_current_beets(self):
        from lib.beets_db import AlbumInfo
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            source_path=source,
            files=snapshot_audio_files(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                format="AAC",
                is_cbr=True,
                spectral_grade="genuine",
            ),
            lineage_version=1,
            on_disk_v0_research_attempted=True,
        )
        db.upsert_album_quality_evidence(evidence)
        before = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert before is not None and before.id is not None
        db.set_request_current_evidence(42, before.id)
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=256,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=256,
            is_cbr=True,
            album_path=source,
            format="AAC",
        ))

        db.update_status(42, "downloading", expected_status="wanted")
        with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
            prepared = prepare_current_evidence_for_failure(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
            )

        self.assertEqual(prepared, "ready")
        current_id = db.get_request_current_evidence_id(42)
        self.assertEqual(current_id, before.id)
        current = db.load_album_quality_evidence_by_id(current_id)
        assert current is not None
        self.assertEqual(current.lineage_version, 4)
        self.assertEqual(db.request(42)["status"], "downloading")

        db.update_status(42, "wanted", expected_status="downloading")
        with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
            enriched = enrich_incomplete_current_evidence_for_request(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
                spectral_analyzer=lambda _path: self._good_scan(),
                probe_fn=lambda _path: None,
            )

        self.assertEqual(enriched, "enriched")
        self.assertEqual(db.request(42)["status"], "wanted")
        current = db.load_album_quality_evidence_by_id(current_id)
        assert current is not None
        self.assertEqual(current.lineage_version, 4)
        self.assertEqual(current.measurement.format, "AAC")
        self.assertEqual(current.measurement.avg_bitrate_kbps, 256)

    def test_fills_both_missing_pieces(self):
        db = self._db()
        source = self._source_dir()
        stored = self._seed_current(db, source, spectral_present=False)
        assert stored.id is not None
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, "enriched")
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [source])
        persisted = db.load_album_quality_evidence_by_id(stored.id)
        assert persisted is not None
        self.assertEqual(persisted.measurement.spectral_grade, "genuine")
        self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 96)
        self.assertTrue(persisted.on_disk_v0_research_attempted)
        assert persisted.v0_metric is not None
        self.assertEqual(persisted.v0_metric.avg_bitrate_kbps, 259)

    def test_fills_v0_only_when_spectral_present(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(db, source, spectral_present=True)
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, "enriched")
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [source])

    def test_fills_spectral_only_when_v0_already_attempted(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(
            db, source, spectral_present=False, v0_attempted=True,
        )
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, "enriched")
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [])

    def test_stale_snapshot_measures_nothing(self):
        db = self._db()
        source = self._source_dir()
        self._seed_current(db, source, spectral_present=False)
        with open(os.path.join(source, "01.mp3"), "ab") as handle:
            handle.write(b"changed after snapshot")
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, "stale")
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [])

    def test_without_current_evidence_returns_no_current_evidence(self):
        db = self._db()
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, "no_current_evidence")
        self.assertEqual(spectral_calls, [])
        self.assertEqual(probe_calls, [])

    def test_failed_backfill_is_not_classified_as_absent_library_copy(self):
        db = self._db()

        outcome = prepare_current_evidence_for_failure(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="",
            load_fn=lambda *_args, **_kwargs: EvidenceBuildResult(
                None,
                "failed",
                "beets library unreadable",
            ),
        )

        self.assertEqual(outcome, "failed")

    def test_backfill_exception_is_not_classified_as_absent_library_copy(self):
        db = self._db()

        def broken_loader(*_args, **_kwargs):
            raise RuntimeError("beets adapter crashed")

        outcome = prepare_current_evidence_for_failure(
            db,
            request_id=42,
            mb_release_id="mbid-42",
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="",
            load_fn=broken_loader,
        )

        self.assertEqual(outcome, "failed")

    def test_failed_download_backfills_unlinked_seabear_have(self):
        """We Built a Fire: an installed album cannot stay HAVE-less."""
        from lib.beets_db import AlbumInfo
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1,
            track_count=17,
            min_bitrate_kbps=183,
            avg_bitrate_kbps=190,
            median_bitrate_kbps=191,
            is_cbr=False,
            album_path=source,
            format="MP3",
        ))
        analyzer, spectral_calls = self._spectral_recorder(self._good_scan())
        probe, probe_calls = self._probe_recorder()

        with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
            prepared = prepare_current_evidence_for_failure(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
            )
            outcome = enrich_incomplete_current_evidence_for_request(
                db,
                request_id=42,
                mb_release_id="mbid-42",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root=source,
                spectral_analyzer=analyzer,
                probe_fn=probe,
            )

        self.assertEqual(prepared, "ready")
        self.assertEqual(outcome, "enriched")
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [source])
        evidence_id = db.get_request_current_evidence_id(42)
        self.assertIsNotNone(evidence_id)
        persisted = db.load_album_quality_evidence_by_id(evidence_id)
        assert persisted is not None
        self.assertEqual(persisted.measurement.format, "MP3")
        self.assertEqual(persisted.measurement.avg_bitrate_kbps, 190)
        self.assertEqual(persisted.measurement.spectral_grade, "genuine")
        assert persisted.v0_metric is not None
        self.assertEqual(persisted.v0_metric.avg_bitrate_kbps, 259)

    def test_failed_spectral_scan_reports_partial(self):
        db = self._db()
        source = self._source_dir()
        stored = self._seed_current(
            db, source, spectral_present=False, v0_attempted=True,
        )
        assert stored.id is not None
        analyzer, spectral_calls = self._spectral_recorder(
            SpectralAnalysisDetail(attempted=True, error="sox exploded"),
        )
        probe, probe_calls = self._probe_recorder()

        outcome = self._enrich(db, analyzer, probe)

        self.assertEqual(outcome, "partial")
        self.assertEqual(spectral_calls, [source])
        self.assertEqual(probe_calls, [])
        persisted = db.load_album_quality_evidence_by_id(stored.id)
        assert persisted is not None
        self.assertIsNone(persisted.measurement.spectral_grade)
        self.assertIsNone(persisted.measurement.spectral_bitrate_kbps)


class TestPreviewDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy ImportPreviewDB."""

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.import_preview import ImportPreviewDB
        from lib.pipeline_db import PipelineDB

        self.assertTrue(issubclass(PipelineDB, ImportPreviewDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.import_preview import ImportPreviewDB

        self.assertTrue(issubclass(FakePipelineDB, ImportPreviewDB))

    def test_preview_protocol_extends_evidence_protocol(self) -> None:
        """Preview forwards its handle into the evidence persisters."""
        from lib.import_preview import ImportPreviewDB
        from lib.quality_evidence import QualityEvidenceDB

        self.assertTrue(issubclass(ImportPreviewDB, QualityEvidenceDB))


if __name__ == "__main__":
    unittest.main()
