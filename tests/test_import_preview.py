"""Tests for unified import preview service."""

import os
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any, TYPE_CHECKING
from unittest.mock import patch

from lib.config import CratediggerConfig
from lib.import_preview import (
    ImportPreviewValues,
    _prefer_successful_spectral_detail,
    compose_attempt_spectral_audit,
    load_persisted_existing_spectral,
    measure_and_persist_candidate_evidence,
    preview_import_from_path,
    preview_import_from_values,
)
from lib.measurement import LocalFileInspection, PreimportMeasurement
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    SpectralAnalysisDetail,
    SpectralDetail,
    full_pipeline_decision,
)

from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


class TestSpectralAuditMerge(unittest.TestCase):
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
            db, 42, req)

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
                        load_persisted_existing_spectral(db, 42, req)
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
                "preimport_nested:skipped_auto",
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
            status="manual",
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

    def _direct_preview_override(self, db: FakePipelineDB) -> int | None:
        source = self._source_dir()
        run = SimpleNamespace(
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=245,
                    median_bitrate_kbps=245,
                    format="mp3 v0",
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

    def test_direct_preview_legacy_no_fk_uses_spectral_floor(self):
        db = self._db()
        db.request(42).update(
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=96,
        )

        self.assertEqual(self._direct_preview_override(db), 96)

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
                format="MP3 320",
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

        self.assertEqual(self._direct_preview_override(db), 320)

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
                                   format="mp3 v0",
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
            self.assertEqual(
                mock_run.call_args.kwargs["existing_v0_probe"].avg_bitrate_kbps,
                171,
            )
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
            "manual_import",
            request_id=42,
            dedupe_key="manual:42:/tmp/source",
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
                                   format="mp3 v0",
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
                                   format="mp3 v0",
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
            "manual_import",
            request_id=42,
            dedupe_key="manual:42:/tmp/source",
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
                        format="mp3 v0",
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
        """U6: preview surfaces the four folder/audio-integrity facts as a
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
