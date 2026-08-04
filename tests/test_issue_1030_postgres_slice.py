"""Disposable-PostgreSQL outer-composition regression for issue #1030."""

from __future__ import annotations

import configparser
import json
import os
import tempfile
import unittest
from functools import partial
from unittest.mock import patch

import msgspec

from lib.config import CratediggerConfig
from lib.import_evidence import ensure_candidate_evidence_for_action
from lib.import_preview import measure_and_persist_candidate_evidence
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.migrator import apply_migrations
from lib.pipeline_db import PipelineDB
from lib.quality import (
    AudioQualityMeasurement,
    AudioValidationResult,
    SpectralAnalysisDetail,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
)
from lib.quality_evidence import (
    CandidateEvidencePersistenceReceipt,
    candidate_evidence_persistence_receipt_semantic_error,
    snapshot_audio_files,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from scripts import import_preview_worker
from tests.ephemeral_pg import EphemeralPostgres
from tests.helpers import (
    claim_next_import_preview_job,
    hermetic_beets_config_defaults,
    make_album_quality_evidence,
    make_audio_corrupt_validation_report,
)


class TestBackToMonoPostgresOuterComposition(unittest.TestCase):
    def test_stale_same_address_reaches_corrupt_decider_not_measurement_failed(
        self,
    ) -> None:
        """Back to Mono: outer preview persists the fresh exact-attempt receipt."""
        with (
            EphemeralPostgres() as postgres,
            tempfile.TemporaryDirectory(prefix="back-to-mono-1030-") as root,
            hermetic_beets_config_defaults(),
        ):
            assert postgres.dsn is not None
            apply_migrations(postgres.dsn)
            db = PipelineDB(postgres.dsn)
            self.addCleanup(db.close)

            staging = os.path.join(root, "Incoming")
            processing = os.path.join(root, "processing")
            source = os.path.join(staging, "failed_imports", "Back to Mono")
            os.makedirs(source)
            os.makedirs(os.path.join(root, "slskd"))
            os.makedirs(os.path.join(processing, "albums"), mode=0o700)
            os.makedirs(os.path.join(processing, "preview"), mode=0o700)
            os.chmod(processing, 0o700)
            with open(os.path.join(source, "01 - Back to Mono.flac"), "wb") as handle:
                handle.write(b"corrupt flac bytes")

            request_id = db.add_request(
                artist_name="The Mono Set",
                album_title="Back to Mono",
                source="request",
                mb_release_id="back-to-mono-release",
            )
            download_log_id = db.log_download(
                request_id=request_id,
                outcome="rejected",
                validation_result=json.dumps({"failed_path": source}),
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="archive-peer",
                ),
            )

            files = snapshot_audio_files(source)
            stale = make_album_quality_evidence(
                mb_release_id="back-to-mono-release",
                source_path=source,
                files=files,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=900,
                    avg_bitrate_kbps=900,
                    median_bitrate_kbps=900,
                    format="FLAC",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=96,
                    spectral_subject="source",
                    spectral_provenance="measured",
                    spectral_measurement_version=None,
                ),
                preserve_spectral_measurement_version=True,
                codec="flac",
                container="flac",
                storage_format="FLAC",
            )
            db.upsert_album_quality_evidence(stale)
            stored_stale = db.find_album_quality_evidence(
                mb_release_id=stale.mb_release_id,
                snapshot_fingerprint=stale.snapshot_fingerprint,
            )
            assert stored_stale is not None and stored_stale.id is not None
            self.assertTrue(
                db.set_import_job_candidate_evidence(job.id, stored_stale.id)
            )
            db.set_download_log_candidate_evidence(
                download_log_id,
                stored_stale.id,
                direct_attribution=True,
            )

            claimed = claim_next_import_preview_job(db, worker_id="preview-1030")
            assert claimed is not None and claimed.id == job.id
            detail = SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
                bitrate_kbps=96,
                cliff_hz=21_000,
                codec_family="lossless",
                ultrasonic_deficit_db=1.0,
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            )
            audio_result = AudioValidationResult(
                make_audio_corrupt_validation_report(
                    "01 - Back to Mono.flac"
                ),
                failed_paths=("01 - Back to Mono.flac",),
            )

            ini = configparser.ConfigParser()
            ini["Beets Validation"] = {
                "harness_path": "/nix/store/fake/harness/run_beets_harness.sh",
                "audio_check": "normal",
                "staging_dir": staging,
            }
            ini["Slskd"] = {"download_dir": os.path.join(root, "slskd")}
            ini["Paths"] = {"processing_dir": processing}
            ini["Pipeline DB"] = {"enabled": "true"}
            cfg = CratediggerConfig.from_ini(ini)

            analyzer_calls: list[str] = []

            def analyzer(path: str) -> SpectralAnalysisDetail:
                analyzer_calls.append(path)
                return detail

            with patch(
                "lib.measurement.validate_audio",
                return_value=audio_result,
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    spectral_detail_analyzer=analyzer,
                    candidate_measurement_fn=partial(
                        measure_and_persist_candidate_evidence,
                        spectral_detail_analyzer=analyzer,
                    ),
                    runtime_config=cfg,
                )

            assert updated is not None
            self.assertEqual(
                updated.preview_status,
                "evidence_ready",
                f"{updated.preview_error}; {updated.preview_result}",
            )
            self.assertNotEqual(updated.preview_status, "measurement_failed")
            reloaded_job = db.get_import_job(job.id)
            assert reloaded_job is not None and reloaded_job.preview_result is not None
            raw_receipt = reloaded_job.preview_result.get(
                "candidate_evidence_receipt"
            )
            receipt = msgspec.convert(
                raw_receipt,
                type=CandidateEvidencePersistenceReceipt,
            )
            self.assertIsNone(
                candidate_evidence_persistence_receipt_semantic_error(receipt)
            )
            self.assertEqual(receipt.evidence_id, stored_stale.id)
            self.assertEqual(
                db.get_import_job_candidate_evidence_id(job.id),
                receipt.evidence_id,
            )
            self.assertEqual(receipt.spectral_outcome, "measured")
            self.assertEqual(receipt.spectral_grade, "genuine")
            self.assertEqual(
                receipt.spectral_measurement_version,
                SPECTRAL_MEASUREMENT_VERSION,
            )

            action_path = str(
                reloaded_job.preview_result.get("action_path") or source
            )
            admitted = ensure_candidate_evidence_for_action(
                db,
                source_path=action_path,
                import_job_id=job.id,
            )
            self.assertTrue(admitted.available, admitted.provenance)
            assert admitted.evidence is not None
            decision = full_pipeline_decision_from_evidence(admitted.evidence)
            self.assertEqual(evidence_decision_name(decision), "audio_corrupt")
            self.assertEqual(decision["preimport_audio"], "reject_corrupt")
            self.assertNotEqual(evidence_decision_name(decision), "measurement_failed")
            canonical = db.load_album_quality_evidence_by_id(receipt.evidence_id)
            assert canonical is not None
            self.assertTrue(canonical.audio_corrupt)
            self.assertEqual(canonical.measurement.spectral_grade, "genuine")
            self.assertEqual(len(analyzer_calls), 1)
            self.assertNotEqual(analyzer_calls[0], source)
