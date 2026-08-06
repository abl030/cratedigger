"""Disposable-PostgreSQL outer-composition regression for issue #1030."""

from __future__ import annotations

import configparser
import json
import os
import tempfile
import unittest
from functools import partial
from typing import Self
from unittest.mock import patch

import msgspec

from lib.beets_db import (
    CurrentBeetsItem,
    CurrentBeetsUnique,
    release_identity_for_lookup,
)
from lib.config import CratediggerConfig
from lib.ephemeral_postgres import EphemeralPostgres
from lib.import_evidence import (
    ensure_candidate_evidence_for_action,
    ensure_current_evidence_for_action,
)
from lib.import_preview import measure_and_persist_candidate_evidence
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.measurement import PreimportMeasurement
from lib.migrator import apply_migrations
from lib.pipeline_db import PipelineDB
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    AudioValidationResult,
    SpectralAnalysisDetail,
    SpectralDetail,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
)
from lib.quality_evidence import (
    CandidateEvidencePersistenceReceipt,
    candidate_evidence_persistence_receipt_semantic_error,
    persist_candidate_evidence_from_measurement,
    snapshot_audio_files,
)
from lib.release_identity import ReleaseIdentity
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from scripts import import_preview_worker
from tests.helpers import (
    claim_next_import_preview_job,
    hermetic_beets_config_defaults,
    make_album_quality_evidence,
    make_audio_corrupt_validation_report,
)


class TestBackToMonoPostgresOuterComposition(unittest.TestCase):
    def test_invalid_same_address_attempt_has_no_canonical_or_fk_effect(self) -> None:
        """A fail-closed receipt cannot mutate PG before it is rejected."""
        with (
            EphemeralPostgres() as postgres,
            tempfile.TemporaryDirectory(prefix="receipt-no-effect-1030-") as root,
        ):
            assert postgres.dsn is not None
            apply_migrations(postgres.dsn)
            db = PipelineDB(postgres.dsn)
            try:
                source = os.path.join(root, "candidate")
                os.mkdir(source)
                with open(os.path.join(source, "01.mp3"), "wb") as handle:
                    handle.write(b"same-address")
                request_id = db.add_request(
                    artist_name="Receipt no-effect",
                    album_title="Same Address",
                    source="request",
                    mb_release_id="receipt-no-effect-release",
                )
                download_log_id = db.log_download(
                    request_id=request_id,
                    outcome="rejected",
                )
                files = snapshot_audio_files(source)
                canonical = make_album_quality_evidence(
                    mb_release_id="receipt-no-effect-release",
                    source_path=source,
                    files=files,
                    measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=128,
                        format="MP3",
                        spectral_grade="suspect",
                        spectral_subject="source",
                        spectral_provenance="measured",
                        spectral_measurement_version=(
                            SPECTRAL_MEASUREMENT_VERSION
                        ),
                    ),
                )
                db.upsert_album_quality_evidence(canonical)
                before = db.find_album_quality_evidence(
                    mb_release_id=canonical.mb_release_id,
                    snapshot_fingerprint=canonical.snapshot_fingerprint,
                )
                assert before is not None and before.id is not None
                db.set_download_log_candidate_evidence(
                    download_log_id,
                    before.id,
                )
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=request_id,
                    payload=force_import_payload(
                        download_log_id=download_log_id,
                        failed_path=source,
                    ),
                )
                self.assertIsNone(
                    db.get_import_job_candidate_evidence_id(job.id)
                )

                refused = persist_candidate_evidence_from_measurement(
                    db,
                    mb_release_id=canonical.mb_release_id,
                    source_path=source,
                    measurement=PreimportMeasurement(
                        min_bitrate_kbps=128,
                        audio_file_count=len(files),
                        filetype_band="mp3",
                        spectral_audit=SpectralDetail(
                            candidate=SpectralAnalysisDetail(
                                attempted=True,
                                grade="alien",
                                spectral_measurement_version=(
                                    SPECTRAL_MEASUREMENT_VERSION
                                ),
                            )
                        ),
                    ),
                    download_log_id=download_log_id,
                    import_job_id=job.id,
                    files=files,
                )

                self.assertEqual(refused.status, "failed")
                self.assertIn("receipt semantic", refused.reason or "")
                after = db.find_album_quality_evidence(
                    mb_release_id=canonical.mb_release_id,
                    snapshot_fingerprint=canonical.snapshot_fingerprint,
                )
                assert after is not None
                self.assertEqual(after.id, before.id)
                self.assertEqual(after.measurement.spectral_grade, "suspect")
                self.assertEqual(
                    db.get_download_log_candidate_evidence_id(download_log_id),
                    before.id,
                )
                self.assertIsNone(
                    db.get_import_job_candidate_evidence_id(job.id)
                )
            finally:
                db.close()

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

            current_path = os.path.join(root, "Music", "Back to Mono")
            os.makedirs(current_path)
            current_track = os.path.join(current_path, "01 - Back to Mono.opus")
            with open(current_track, "wb") as handle:
                handle.write(b"installed opus bytes")

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

            current_files = snapshot_audio_files(current_path)
            current = make_album_quality_evidence(
                mb_release_id="back-to-mono-release",
                source_path=current_path,
                files=current_files,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=160,
                    avg_bitrate_kbps=160,
                    median_bitrate_kbps=160,
                    format="Opus",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=96,
                    spectral_subject="source",
                    spectral_provenance="carried",
                    cliff_hz=15_500,
                    codec_family="lossless",
                    ultrasonic_deficit_db=9.0,
                    spectral_measurement_version=None,
                    was_converted_from="flac",
                ),
                v0_metric=AlbumQualityV0Metric(
                    subject="source",
                    provenance="carried",
                    avg_bitrate_kbps=96,
                ),
                preserve_spectral_measurement_version=True,
                codec="opus",
                container="opus",
                storage_format="Opus",
            )
            db.upsert_album_quality_evidence(current)
            stored_current = db.find_album_quality_evidence(
                mb_release_id=current.mb_release_id,
                snapshot_fingerprint=current.snapshot_fingerprint,
            )
            assert stored_current is not None and stored_current.id is not None
            self.assertTrue(
                db.set_request_current_evidence(request_id, stored_current.id)
            )
            current_tuple = (
                stored_current.measurement.spectral_grade,
                stored_current.measurement.spectral_bitrate_kbps,
                stored_current.measurement.spectral_subject,
                stored_current.measurement.spectral_provenance,
                stored_current.measurement.cliff_hz,
                stored_current.measurement.codec_family,
                stored_current.measurement.ultrasonic_deficit_db,
                stored_current.measurement.spectral_measurement_version,
                stored_current.measurement.was_converted_from,
            )

            identity = release_identity_for_lookup("back-to-mono-release")
            assert identity is not None
            current_release = CurrentBeetsUnique(
                identity=identity,
                album_id=1030,
                album_path=current_path,
                items=(CurrentBeetsItem(
                    id=1030,
                    path=current_track,
                    format="Opus",
                    bitrate=160_000,
                ),),
                selectors=("mb_albumid:back-to-mono-release",),
            )

            class _CurrentBeetsLeaf:
                def __enter__(self) -> Self:
                    return self

                def __exit__(self, *_args: object) -> None:
                    return None

                def resolve_current_releases(
                    self,
                    requested: list[ReleaseIdentity],
                ) -> dict[ReleaseIdentity, CurrentBeetsUnique]:
                    # Batched, because the request resolves over its identity
                    # union (#1059). This row has no stored merge survivor,
                    # so the union is the acquisition identity alone.
                    if requested != [identity]:
                        raise AssertionError("unexpected current identity")
                    return {identity: current_release}
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
            ), patch(
                "lib.beets_db.BeetsDB",
                return_value=_CurrentBeetsLeaf(),
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
            admitted_current = ensure_current_evidence_for_action(
                db,
                request_id=request_id,
                mb_release_id="back-to-mono-release",
                current_release=current_release,
                quality_ranks=cfg.quality_ranks,
            )
            self.assertTrue(admitted_current.available, admitted_current.provenance)
            assert admitted_current.evidence is not None
            decision = full_pipeline_decision_from_evidence(
                admitted.evidence,
                admitted_current.evidence,
            )
            self.assertEqual(evidence_decision_name(decision), "audio_corrupt")
            self.assertEqual(decision["preimport_audio"], "reject_corrupt")
            self.assertNotEqual(evidence_decision_name(decision), "measurement_failed")
            canonical = db.load_album_quality_evidence_by_id(receipt.evidence_id)
            assert canonical is not None
            self.assertTrue(canonical.audio_corrupt)
            self.assertEqual(canonical.measurement.spectral_grade, "genuine")
            reloaded_current = db.load_album_quality_evidence_by_id(
                stored_current.id
            )
            assert reloaded_current is not None
            self.assertEqual(
                (
                    reloaded_current.measurement.spectral_grade,
                    reloaded_current.measurement.spectral_bitrate_kbps,
                    reloaded_current.measurement.spectral_subject,
                    reloaded_current.measurement.spectral_provenance,
                    reloaded_current.measurement.cliff_hz,
                    reloaded_current.measurement.codec_family,
                    reloaded_current.measurement.ultrasonic_deficit_db,
                    reloaded_current.measurement.spectral_measurement_version,
                    reloaded_current.measurement.was_converted_from,
                ),
                current_tuple,
            )
            self.assertEqual(len(analyzer_calls), 1)
            self.assertNotEqual(analyzer_calls[0], source)
            self.assertNotEqual(analyzer_calls[0], current_path)
