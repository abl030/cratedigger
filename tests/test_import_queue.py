"""Tests for the shared import queue worker."""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

from lib.config import CratediggerConfig
from lib.import_dispatch import DispatchOutcome
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    automation_import_dedupe_key,
    force_import_dedupe_key,
    force_import_payload,
    manual_import_dedupe_key,
    manual_import_payload,
)
from lib.import_preview import ImportPreviewResult
from lib.quality import (
    ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
    ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
    ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
    AudioQualityMeasurement,
    ImportResult,
)
from lib.quality_evidence import snapshot_audio_files
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_album_quality_evidence,
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
)


class TestWrongMatchCleanupDecision(unittest.TestCase):
    def _patch_beets_album(self, album_path: str | None, *, min_bitrate: int):
        from lib.beets_db import AlbumInfo

        beets = MagicMock()
        beets.__enter__.return_value = beets
        beets.__exit__.return_value = None
        beets.get_album_info.return_value = (
            None
            if album_path is None
            else AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=min_bitrate,
                avg_bitrate_kbps=min_bitrate,
                median_bitrate_kbps=min_bitrate,
                is_cbr=False,
                album_path=album_path,
                format="MP3",
            )
        )
        return patch("lib.beets_db.BeetsDB", return_value=beets)

    def _seed_cleanup_evidence(
        self,
        db: FakePipelineDB,
        *,
        source_path: str,
        candidate_min: int,
        current_min: int,
    ) -> int:
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))
        log_id = db.log_download(
            42,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": source_path,
            },
        )
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            owner_id=log_id,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=candidate_min,
                avg_bitrate_kbps=candidate_min,
                median_bitrate_kbps=candidate_min,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        ))
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            owner_id=42,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=current_min,
                avg_bitrate_kbps=current_min,
                median_bitrate_kbps=current_min,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        ))
        return log_id

    def test_decision_reuses_download_log_evidence_without_preview(self):
        from lib.wrong_match_cleanup_decision import (
            CLEANUP_DECISION_PROVENANCE,
            decide_wrong_match_cleanup,
        )

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            log_id = self._seed_cleanup_evidence(
                db,
                source_path=source,
                candidate_min=128,
                current_min=245,
            )

            with self._patch_beets_album(source, min_bitrate=245):
                decision = decide_wrong_match_cleanup(
                    db,
                    log_id,
                    cfg=CratediggerConfig(),
                )

        self.assertTrue(decision.delete_allowed)
        self.assertFalse(decision.uncertain)
        self.assertEqual(decision.provenance, CLEANUP_DECISION_PROVENANCE)
        self.assertEqual(decision.preview_decision, "downgrade")

    def test_decision_blocks_uncertain_when_preview_cannot_build_evidence(self):
        from lib.wrong_match_cleanup_decision import decide_wrong_match_cleanup

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))
        log_id = db.log_download(
            42,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": "/tmp/missing-evidence",
            },
        )
        preview = ImportPreviewResult(
            mode="download_log",
            verdict="uncertain",
            uncertain=True,
            cleanup_eligible=False,
            reason="fresh evidence unavailable",
            download_log_id=7,
        )

        decision = decide_wrong_match_cleanup(
            db,
            log_id,
            preview_builder=lambda _db, _log_id: preview,
            cfg=CratediggerConfig(),
        )

        self.assertFalse(decision.delete_allowed)
        self.assertTrue(decision.uncertain)
        self.assertIn("fresh evidence unavailable", decision.skip_reason)

    def test_cleanup_preview_must_publish_evidence_before_cleanup_decision(self):
        from lib.wrong_match_cleanup_decision import decide_wrong_match_cleanup

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="manual",
            ))
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "scenario": "high_distance",
                    "failed_path": source,
                },
            )
            preview = ImportPreviewResult(
                mode="download_log",
                verdict="would_import",
                would_import=True,
                decision="import",
                reason="import",
                source_path=source,
                import_result=ImportResult(
                    decision="import",
                    new_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=245,
                        avg_bitrate_kbps=256,
                        median_bitrate_kbps=252,
                        format="MP3 V0",
                        spectral_grade="genuine",
                    ),
                ),
            )

            with patch(
                "lib.quality_evidence.persist_candidate_evidence_from_import_result",
                side_effect=AssertionError(
                    "cleanup decision must rely on preview's guarded persistence"
                ),
            ) as persist:
                decision = decide_wrong_match_cleanup(
                    db,
                    log_id,
                    preview_builder=lambda _db, _log_id: preview,
                    cfg=CratediggerConfig(),
                )

        persist.assert_not_called()
        self.assertFalse(decision.delete_allowed)
        self.assertTrue(decision.uncertain)
        self.assertEqual(decision.verdict, "uncertain")

    def test_decision_blocks_importable_candidate_even_with_old_reject_row(self):
        from lib.wrong_match_cleanup_decision import decide_wrong_match_cleanup

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            log_id = self._seed_cleanup_evidence(
                db,
                source_path=source,
                candidate_min=245,
                current_min=128,
            )

            with self._patch_beets_album(source, min_bitrate=128):
                decision = decide_wrong_match_cleanup(
                    db,
                    log_id,
                    cfg=CratediggerConfig(),
                )

        self.assertFalse(decision.delete_allowed)
        self.assertFalse(decision.uncertain)
        self.assertFalse(decision.confident_reject)
        self.assertFalse(decision.cleanup_eligible)
        self.assertEqual(decision.preview_decision, "import")

    def test_no_current_album_does_not_use_stale_current_evidence_for_delete(self):
        from lib.wrong_match_cleanup_decision import decide_wrong_match_cleanup

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            log_id = self._seed_cleanup_evidence(
                db,
                source_path=source,
                candidate_min=128,
                current_min=320,
            )

            with self._patch_beets_album(None, min_bitrate=320):
                decision = decide_wrong_match_cleanup(
                    db,
                    log_id,
                    cfg=CratediggerConfig(),
                )

        self.assertFalse(decision.delete_allowed)
        self.assertFalse(decision.uncertain)
        self.assertEqual(decision.verdict, "would_import")
        self.assertEqual(decision.preview_decision, "import")


class TestAutomationEvidenceReuse(unittest.TestCase):
    def test_previewed_automation_job_skips_preimport_gates(self):
        from lib.download import _process_beets_validation
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="downloading",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"audio")
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                owner_id=job.id,
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3 V0",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="mp3 v0",
            ))
            cfg = CratediggerConfig(
                beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=tmpdir,
                pipeline_db_enabled=True,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album_data = make_grab_list_entry(
                album_id=42,
                artist="Artist",
                title="Album",
                mb_release_id="mbid-123",
                db_source="request",
                db_request_id=42,
            )
            staged_album = StagedAlbum(current_path=tmpdir, request_id=42)

            with patch("lib.beets.beets_validate", return_value=ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )), \
                 patch(
                     "lib.preimport.run_preimport_gates",
                     side_effect=AssertionError(
                         "valid preview evidence must skip preimport gates"),
                 ) as gates, \
                 patch(
                     "lib.download._handle_valid_result",
                     return_value=DispatchOutcome(True, "imported"),
                 ) as handle_valid:
                result = _process_beets_validation(
                    album_data,
                    staged_album,
                    ctx,
                    import_job_id=job.id,
                )

        assert result is not None
        self.assertTrue(result.success)
        gates.assert_not_called()
        handle_valid.assert_called_once()
        self.assertEqual(handle_valid.call_args.kwargs["import_job_id"], job.id)

    def test_stale_previewed_automation_evidence_fails_before_preimport(self):
        from lib.download import _process_beets_validation
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="downloading",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            track = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track, "wb") as handle:
                handle.write(b"audio")
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                owner_id=job.id,
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3 V0",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="mp3 v0",
            ))
            with open(track, "ab") as handle:
                handle.write(b" changed")
            cfg = CratediggerConfig(
                beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=tmpdir,
                pipeline_db_enabled=True,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album_data = make_grab_list_entry(
                album_id=42,
                artist="Artist",
                title="Album",
                mb_release_id="mbid-123",
                db_source="request",
                db_request_id=42,
            )
            staged_album = StagedAlbum(current_path=tmpdir, request_id=42)

            with patch("lib.beets.beets_validate", return_value=ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )), \
                 patch("lib.preimport.run_preimport_gates") as gates, \
                 patch("lib.download._handle_valid_result") as handle_valid:
                result = _process_beets_validation(
                    album_data,
                    staged_album,
                    ctx,
                    import_job_id=job.id,
                )

        assert result is not None
        self.assertFalse(result.success)
        self.assertIn("Candidate quality evidence unavailable", result.message)
        gates.assert_not_called()
        handle_valid.assert_not_called()


class TestImporterWorker(unittest.TestCase):
    def _patch_beets_album(self, album_path: str, *, min_bitrate: int):
        from lib.beets_db import AlbumInfo

        beets = MagicMock()
        beets.__enter__.return_value = beets
        beets.__exit__.return_value = None
        beets.get_album_info.return_value = AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=min_bitrate,
            median_bitrate_kbps=min_bitrate,
            is_cbr=False,
            album_path=album_path,
            format="MP3",
        )
        return patch("lib.beets_db.BeetsDB", return_value=beets)

    def _mark_importable(
        self,
        db: FakePipelineDB,
        job,
        *,
        preview_result: dict[str, Any] | None = None,
    ):
        updated = db.mark_import_job_preview_importable(
            job.id,
            preview_result=preview_result or {"verdict": "would_import"},
            message="ready",
        )
        assert updated is not None
        return updated

    def _result(self, job: Any) -> dict[str, Any]:
        assert job.result is not None
        return job.result

    def _log_wrong_match(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 42,
        failed_path: str,
        username: str = "alice",
    ) -> int:
        db.log_download(
            request_id,
            soulseek_username=username,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": failed_path,
            },
        )
        return db.download_logs[-1].id

    def _cleanup_preview(
        self,
        log_id: int,
        *,
        verdict: str = "confident_reject",
        cleanup_eligible: bool = True,
        reason: str = "fresh cleanup-safe reject",
    ) -> ImportPreviewResult:
        return ImportPreviewResult(
            mode="download_log",
            verdict=verdict,
            would_import=verdict == "would_import",
            confident_reject=verdict == "confident_reject",
            uncertain=verdict == "uncertain",
            cleanup_eligible=cleanup_eligible,
            decision=reason,
            reason=reason,
            download_log_id=log_id,
        )

    def _seed_cleanup_reject_evidence(
        self,
        db: FakePipelineDB,
        *,
        log_id: int,
        source_path: str,
        request_id: int = 42,
    ) -> None:
        if request_id not in db._requests:  # type: ignore[attr-defined]
            db.seed_request(make_request_row(
                id=request_id,
                mb_release_id="mbid-123",
                status="manual",
            ))
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            owner_id=log_id,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        ))
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            owner_id=request_id,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        ))

    def test_force_import_job_calls_existing_dispatch_and_completes(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            updated = importer.process_claimed_job(cast(Any, db), claimed)

        dispatch.assert_called_once_with(
            db,
            request_id=42,
            failed_path="/tmp/failed",
            force=True,
            outcome_label=IMPORT_JOB_FORCE,
            source_username="alice",
            source_dirs=None,
            import_job_id=claimed.id,
            download_log_id=7,
        )
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(self._result(updated)["success"], True)
        self.assertEqual(job.id, updated.id)

    def test_force_import_job_forwards_source_dirs(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
                source_dirs=["alice\\Artist\\Album", "alice\\Artist\\Album\\CD2"],
            ),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        dispatch.assert_called_once_with(
            db,
            request_id=42,
            failed_path="/tmp/failed",
            force=True,
            outcome_label=IMPORT_JOB_FORCE,
            source_username="alice",
            source_dirs=["alice\\Artist\\Album", "alice\\Artist\\Album\\CD2"],
            import_job_id=claimed.id,
            download_log_id=7,
        )

    def test_force_import_job_does_not_forward_preview_import_result(self):
        from scripts import importer

        preview_ir = ImportResult(
            decision="import",
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=245),
        )
        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(
            db,
            job,
            preview_result={
                "verdict": "would_import",
                "import_result": json.loads(preview_ir.to_json()),
            },
        )
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        self.assertNotIn("preview_import_result", dispatch.call_args.kwargs)

    def test_force_import_job_does_not_forward_stale_preview_import_result_as_authority(self):
        from scripts import importer

        preview_ir = ImportResult(
            decision="import",
            already_in_beets=False,
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=141),
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="manual",
            min_bitrate=116,
            verified_lossless=False,
            current_spectral_grade="likely_transcode",
            current_lossless_source_v0_probe_avg_bitrate=240,
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(
            db,
            job,
            preview_result={
                "verdict": "would_import",
                "import_result": json.loads(preview_ir.to_json()),
            },
        )
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        self.assertNotIn(
            "preview_import_result",
            dispatch.call_args.kwargs,
            "Stored preview ImportResult is audit/evidence input only; force "
            "import must recompute the action decision against current evidence.",
        )

    def test_manual_import_failure_marks_job_failed(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(False, "quality gate rejected"),
        ):
            updated = importer.process_claimed_job(cast(Any, db), claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.error, "quality gate rejected")
        self.assertEqual(self._result(updated)["success"], False)

    def test_failed_force_import_job_cleans_wrong_match_source(self):
        from scripts import importer

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            self._seed_cleanup_reject_evidence(
                db,
                log_id=log_id,
                source_path=source,
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(False, "Pre-import gate rejected"),
            ), self._patch_beets_album(source, min_bitrate=245):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
            result = self._result(updated)
            self.assertEqual(result["cleanup"]["success"], True)
            self.assertEqual(result["cleanup"]["cleared_rows"], 1)
            self.assertEqual(
                result["cleanup"]["cleanup_decision"]["delete_allowed"],
                True,
            )
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_failed_force_import_job_skips_cleanup_when_fresh_evidence_uncertain(self):
        from scripts import importer

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="manual",
            ))
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(
                db,
                job,
                preview_result={
                    "verdict": "confident_reject",
                    "confident_reject": True,
                    "cleanup_eligible": True,
                },
            )
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(False, "Pre-import gate rejected"),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            result = self._result(updated)
            self.assertEqual(result["cleanup"]["success"], False)
            self.assertTrue(result["cleanup"]["skipped"])
            self.assertEqual(
                result["cleanup"]["cleanup_decision"]["delete_allowed"],
                False,
            )
            self.assertEqual(
                result["cleanup"]["cleanup_decision"]["uncertain"],
                True,
            )
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_force_import_requeued_for_preview_does_not_mark_failed(self):
        """U2: when dispatch returns DISPATCH_CODE_REQUEUED_FOR_PREVIEW the
        importer does NOT write a terminal failed status and does NOT run
        the wrong-match cleanup path. The dispatch-side requeue has already
        flipped the row back to queued/waiting; the importer just logs and
        yields."""
        from scripts import importer
        from lib.import_dispatch import DISPATCH_CODE_REQUEUED_FOR_PREVIEW

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None
            claimed_attempts = claimed.attempts

            def fake_dispatch(*_args, **_kwargs):
                # Simulate the dispatch-side requeue.
                db.requeue_import_job_for_preview(
                    job.id,
                    reason="candidate evidence missing",
                )
                return DispatchOutcome(
                    False,
                    "Candidate evidence unavailable; requeued for preview",
                    code=DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
                )

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                side_effect=fake_dispatch,
            ), patch(
                "lib.wrong_match_cleanup_decision.decide_wrong_match_cleanup",
            ) as decision, patch(
                "lib.wrong_matches.cleanup_wrong_match_source",
            ) as cleanup:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            # Importer must NOT have written a terminal status.
            decision.assert_not_called()
            cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(source))
            # Job row is queued/waiting after the requeue.
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["preview_status"], "waiting")
            # Importer did not retry-count: row attempts not bumped beyond
            # the original claim.
            self.assertEqual(row["attempts"], claimed_attempts)
            # process_claimed_job returns the job ImportJob (current state),
            # not a terminal failure. The job should not be in 'failed'.
            if updated is not None:
                self.assertNotEqual(updated.status, "failed")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_force_import_requeue_failed_leaves_job_running(self):
        """U2: when dispatch returns DISPATCH_CODE_REQUEUE_FAILED (its
        requeue UPDATE itself raised), the importer must not write
        terminal failure — the job stays 'running' so
        requeue_running_import_jobs on next worker boot recovers it."""
        from scripts import importer
        from lib.import_dispatch import DISPATCH_CODE_REQUEUE_FAILED

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None
            claimed_attempts = claimed.attempts

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "requeue UPDATE failed: boom",
                    code=DISPATCH_CODE_REQUEUE_FAILED,
                ),
            ), patch(
                "lib.wrong_match_cleanup_decision.decide_wrong_match_cleanup",
            ) as decision, patch(
                "lib.wrong_matches.cleanup_wrong_match_source",
            ) as cleanup:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            decision.assert_not_called()
            cleanup.assert_not_called()
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            # Stuck in running for startup recovery (requeue_running_import_jobs).
            self.assertEqual(row["status"], "running")
            self.assertEqual(row["attempts"], claimed_attempts)
            self.assertTrue(os.path.isdir(source))
            if updated is not None:
                self.assertNotEqual(updated.status, "failed")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_failed_force_import_job_clears_newer_duplicate_rejection(self):
        from scripts import importer

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source, username="old")
            self._seed_cleanup_reject_evidence(
                db,
                log_id=log_id,
                source_path=source,
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            def reject_again(*_args, **kwargs):
                db.log_download(
                    kwargs["request_id"],
                    soulseek_username="new",
                    outcome="rejected",
                    validation_result={
                        "scenario": "quality_downgrade",
                        "failed_path": kwargs["failed_path"],
                    },
                )
                return DispatchOutcome(False, "Rejected: quality_downgrade")

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                side_effect=reject_again,
            ), self._patch_beets_album(source, min_bitrate=245):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(self._result(updated)["cleanup"]["cleared_rows"], 2)
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_manual_import_failure_preserves_source_and_wrong_match(self):
        from scripts import importer

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, source),
                payload=manual_import_payload(failed_path=source),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(False, "manual import failed"),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertNotIn("cleanup", self._result(updated))
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_deferred_force_import_preserves_source_and_wrong_match(self):
        from scripts import importer

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "Another import is already in progress",
                    deferred=True,
                ),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertNotIn("cleanup", self._result(updated))
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_startup_requeues_abandoned_running_job_for_retry(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="old-worker")
        assert claimed is not None

        recovered = importer.recover_abandoned_running_jobs(cast(Any, db))

        self.assertEqual([job.id for job in recovered], [claimed.id])
        self.assertEqual(recovered[0].status, "queued")
        self.assertIsNone(recovered[0].worker_id)
        self.assertIsNone(recovered[0].heartbeat_at)

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported on retry"),
        ):
            updated = importer.run_once(cast(Any, db), worker_id="new-worker")

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        retried = db.get_import_job(claimed.id)
        assert retried is not None
        self.assertEqual(retried.attempts, 2)

    def test_importer_does_not_claim_job_waiting_for_preview(self):
        from scripts import importer

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )

        self.assertIsNone(importer.run_once(cast(Any, db), worker_id="worker"))

    def test_automation_job_reconstructs_active_state_and_uses_processing_path(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=True,
        ) as processing:
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

        processing.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Automation import processing completed")

    def test_automation_job_completes_from_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=DispatchOutcome(True, "Imported by dispatch"),
        ) as processing:
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

        self.assertEqual(processing.call_args.kwargs["import_job_id"], job.id)
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Imported by dispatch")
        self.assertEqual(self._result(updated)["success"], True)

    def test_automation_job_fails_from_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=DispatchOutcome(False, "Pre-import gate rejected"),
        ):
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.message, "Pre-import gate rejected")
        self.assertEqual(updated.error, "Pre-import gate rejected")
        self.assertEqual(self._result(updated)["success"], False)

    def test_requeued_automation_job_abandons_interrupted_auto_import(self):
        from scripts import importer
        from lib.processing_paths import stage_to_ai_path

        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            slskd_root = os.path.join(tmpdir, "slskd")
            os.makedirs(staging_root)
            os.makedirs(slskd_root)
            staged_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=42,
                auto_import=True,
            )
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.opus"), "w") as fp:
                fp.write("converted audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                artist_name="Test Artist",
                album_title="Test Album",
                year=2020,
                mb_release_id="test-mbid",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "processing_started_at": "2026-04-25T00:10:00+00:00",
                    "import_subprocess_started_at": "2026-04-25T00:11:00+00:00",
                    "current_path": staged_path,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="old-worker")
            assert claimed is not None

            recovered = importer.recover_abandoned_running_jobs(cast(Any, db))
            self.assertEqual([job.id for job in recovered], [claimed.id])

            cfg = type("Cfg", (), {
                "slskd_download_dir": slskd_root,
                "beets_staging_dir": staging_root,
                "beets_validation_enabled": False,
            })()
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            updated = importer.run_once(cast(Any, db), worker_id="new-worker", ctx=ctx)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(db.request(42)["status"], "wanted")
            self.assertEqual(db.get_active_import_job_for_request(42), None)
            self.assertFalse(os.path.exists(staged_path))
            failed_parent = os.path.join(os.path.dirname(staged_path), "failed_imports")
            moved = os.listdir(failed_parent)
            self.assertEqual(len(moved), 1)
            self.assertTrue(moved[0].startswith("abandoned_auto_import"))
            self.assertTrue(os.path.exists(os.path.join(
                failed_parent,
                moved[0],
                "01.opus",
            )))
            self.assertEqual(len(db.download_logs), 1)
            db.assert_log(
                self,
                0,
                outcome="failed",
                beets_scenario="abandoned_auto_import",
            )
            self.assertEqual(db.denylist, [])
            self.assertEqual(db.cooldowns_applied, [])


class TestImportPreviewWorker(unittest.TestCase):
    def _preview(
        self,
        verdict: str,
        *,
        reason: str | None = None,
        source_path: str | None = None,
    ) -> ImportPreviewResult:
        return ImportPreviewResult(
            mode="path",
            verdict=verdict,
            would_import=verdict == "would_import",
            confident_reject=verdict == "confident_reject",
            uncertain=verdict == "uncertain",
            decision=reason,
            reason=reason,
            stage_chain=[f"preview:{reason or verdict}"],
            source_path=source_path,
        )

    def _seed_job_candidate_evidence(
        self,
        db: FakePipelineDB,
        job_id: int,
        source_path: str,
    ) -> None:
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
            owner_id=job_id,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3 V0",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
        ))

    def test_force_job_preview_would_import_marks_importable(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(7),
                payload=force_import_payload(
                    download_log_id=7,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview measures + persists evidence.
                self._seed_job_candidate_evidence(db, claimed.id, source)
                return preview_result

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_called_once_with(
            db,
            request_id=42,
            path=source,
            force=True,
            source_username="alice",
            download_log_id=7,
            import_job_id=claimed.id,
            persist_candidate_evidence=True,
        )
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(updated.preview_result["verdict"], "would_import")
        self.assertIsNotNone(updated.importable_at)

    def test_manual_job_preview_uses_non_force_semantics(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, source),
                payload=manual_import_payload(failed_path=source),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                self._seed_job_candidate_evidence(db, claimed.id, source)
                return preview_result

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_called_once_with(
            db,
            request_id=42,
            path=source,
            force=False,
            source_username=None,
            download_log_id=None,
            import_job_id=claimed.id,
            persist_candidate_evidence=True,
        )
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")

    def test_automation_job_preview_uses_active_download_current_path(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=staged,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                self._seed_job_candidate_evidence(db, claimed.id, staged)
                return preview_result

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(db, claimed)

            preview.assert_called_once_with(
                db,
                request_id=42,
                path=staged,
                force=False,
                source_username="alice",
                download_log_id=None,
                import_job_id=claimed.id,
                persist_candidate_evidence=True,
            )
            assert updated is not None
            self.assertEqual(updated.preview_status, "evidence_ready")

    def test_automation_preview_reject_with_evidence_marks_ready_for_dispatch(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            self._seed_job_candidate_evidence(db, claimed.id, staged)

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                return_value=self._preview(
                    "confident_reject",
                    reason="spectral_reject",
                    source_path=staged,
                ),
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

            assert updated is not None
            self.assertEqual(updated.status, "queued")
            self.assertEqual(updated.preview_status, "evidence_ready")
            self.assertIsNone(updated.preview_error)
            self.assertIsNotNone(db.get_active_import_job_for_request(42))
            claimed_for_import = db.claim_next_import_job(worker_id="importer")
            assert claimed_for_import is not None
            self.assertEqual(claimed_for_import.id, updated.id)

    def test_run_once_heartbeats_while_preview_is_running(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            setattr(db, "dsn", "postgresql://fake")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(7),
                payload=force_import_payload(
                    download_log_id=7,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            initial_claim = db.claim_next_import_preview_job(worker_id="peek")
            assert initial_claim is not None
            assert initial_claim.preview_heartbeat_at is not None
            db.requeue_stale_import_preview_jobs(
                older_than=timedelta(seconds=-1),
                message="test reset",
            )
            heartbeat_seen = threading.Event()

            def preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                baseline = db.get_import_job(initial_claim.id)
                assert baseline is not None
                baseline_heartbeat = baseline.preview_heartbeat_at
                assert baseline_heartbeat is not None
                deadline = time.monotonic() + 0.5
                while time.monotonic() < deadline:
                    current = db.get_import_job(initial_claim.id)
                    assert current is not None
                    if (
                        current.preview_heartbeat_at is not None
                        and current.preview_heartbeat_at > baseline_heartbeat
                    ):
                        heartbeat_seen.set()
                        break
                    time.sleep(0.005)
                # Simulate production: preview persists evidence as a
                # side-effect so the post-measurement gate sees it.
                self._seed_job_candidate_evidence(db, initial_claim.id, source)
                return self._preview(
                    "would_import",
                    reason="import",
                    source_path=source,
                )

            with (
                patch("scripts.import_preview_worker.PipelineDB",
                      side_effect=lambda dsn: db),
                patch(
                    "scripts.import_preview_worker.preview_import_from_path",
                    side_effect=preview,
                ),
            ):
                updated = import_preview_worker.run_once(
                    cast(Any, db),
                    worker_id="preview",
                    heartbeat_interval=0.01,
                )

            assert updated is not None
            self.assertEqual(updated.preview_status, "evidence_ready")
            self.assertTrue(heartbeat_seen.is_set())

    def test_preview_recovery_loop_requeues_abandoned_running_rows(self):
        from scripts import import_preview_worker

        db = FakePipelineDB()
        dsn = "postgresql://fake"
        setattr(db, "dsn", dsn)
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="dead-worker")
        assert claimed is not None
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        for row in db._import_jobs:
            if row["id"] == claimed.id:
                row["preview_started_at"] = old
                row["preview_heartbeat_at"] = old
                row["updated_at"] = old

        stop = threading.Event()
        thread = threading.Thread(
            target=import_preview_worker.preview_recovery_loop,
            kwargs={
                "dsn": dsn,
                "stop": stop,
                "interval": 0.01,
                "db_factory": lambda dsn: db,
            },
        )
        thread.start()
        try:
            deadline = time.monotonic() + 0.5
            recovered = None
            while time.monotonic() < deadline:
                recovered = db.get_import_job(claimed.id)
                if recovered is not None and recovered.preview_status == "waiting":
                    break
                time.sleep(0.005)
        finally:
            stop.set()
            thread.join(timeout=1.0)

        assert recovered is not None
        self.assertEqual(recovered.preview_status, "waiting")
        self.assertEqual(
            recovered.preview_message,
            import_preview_worker.STALE_PREVIEW_MESSAGE,
        )

    def test_confident_reject_fails_job_without_denylisting_source(self):
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=self._preview("confident_reject", reason="spectral_reject"),
        ):
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "uncertain")
        self.assertEqual(updated.preview_error, "spectral_reject")
        self.assertEqual(db.get_denylisted_users(42), [])

    def test_uncertain_preview_fails_without_denylisting(self):
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=self._preview("uncertain", reason="path_missing"),
        ):
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "uncertain")
        self.assertEqual(updated.preview_error, "path_missing")
        self.assertEqual(db.get_denylisted_users(42), [])

    def test_threaded_worker_exits_nonzero_when_worker_thread_crashes(self):
        from scripts import import_preview_worker

        class ThreadDB:
            def close(self):
                pass

        calls = 0
        calls_lock = threading.Lock()

        def run_once(db, *, worker_id):
            nonlocal calls
            with calls_lock:
                calls += 1
                if calls == 1:
                    raise RuntimeError("db connection died")
            return None

        with (
            patch("scripts.import_preview_worker.PipelineDB",
                  side_effect=lambda dsn: ThreadDB()),
            patch("scripts.import_preview_worker.run_once",
                  side_effect=run_once),
            patch("scripts.import_preview_worker.logger.exception"),
            patch("scripts.import_preview_worker.logger.error"),
        ):
            exit_code = import_preview_worker.run_threaded_workers(
                dsn="postgresql://example",
                worker_id="preview-test",
                worker_count=2,
                poll_interval=60.0,
            )

        self.assertEqual(exit_code, 1)


class TestImportPreviewWorkerFrontGate(unittest.TestCase):
    """U1: worker short-circuits measurement when stored candidate evidence
    already passes the snapshot guard.

    Covers AE4 (re-claim of valid evidence skips measurement) for both
    force/manual and automation job types.
    """

    def _seed_evidence_for_job(
        self,
        db: FakePipelineDB,
        job_id: int,
        source_path: str,
    ) -> None:
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
            owner_id=job_id,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3 V0",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
        ))

    def _seed_evidence_for_download_log(
        self,
        db: FakePipelineDB,
        download_log_id: int,
        source_path: str,
    ) -> None:
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            owner_id=download_log_id,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3 V0",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
        ))

    def test_force_job_valid_evidence_skips_measurement(self):
        """AE4 force/manual: matching snapshot + valid evidence → no measurement."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # Seed download_log_candidate evidence — force/manual path uses it.
            self._seed_evidence_for_download_log(db, download_log_id, source)

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
            ) as preview, patch(
                "lib.preimport.run_preimport_gates",
            ) as preimport, patch(
                "lib.spectral_check.analyze_album",
            ) as spectral:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        spectral.assert_not_called()
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )
        self.assertIsNotNone(updated.importable_at)

    def test_manual_job_valid_evidence_skips_measurement(self):
        """AE4 manual: matching snapshot + valid evidence → no measurement."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, source),
                payload=manual_import_payload(failed_path=source),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # Manual jobs have no download_log; seed import_job_candidate.
            self._seed_evidence_for_job(db, claimed.id, source)

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
            ) as preview, patch(
                "lib.preimport.run_preimport_gates",
            ) as preimport:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_automation_job_valid_evidence_skips_measurement_and_materialization(self):
        """AE4 automation: matching snapshot + valid evidence → no measurement.

        Crucially, no materialization either: the path-derivation helper must
        not invoke _materialize_processing_dir.
        """
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_job(db, claimed.id, staged)

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
            ) as preview, patch(
                "lib.preimport.run_preimport_gates",
            ) as preimport, patch(
                "lib.download._materialize_processing_dir",
            ) as materialize:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        materialize.assert_not_called()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_missing_evidence_falls_through_to_full_measurement(self):
        """No evidence row → worker runs full preview measurement (legacy path)."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # No evidence seeded → front-gate misses → measurement runs.

            preview_result = ImportPreviewResult(
                mode="path",
                verdict="would_import",
                would_import=True,
                decision="import",
                reason="import",
                stage_chain=["preview:import"],
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview persists candidate evidence.
                db.upsert_album_quality_evidence(make_album_quality_evidence(
                    owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                    owner_id=download_log_id,
                    files=snapshot_audio_files(source),
                ))
                return preview_result

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        # Front-gate misses (no evidence) → preview is called.
        preview.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        # Provenance reflects the measured path, not the reused path.
        assert updated.preview_result is not None
        self.assertNotEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_snapshot_mismatch_falls_through_to_full_measurement(self):
        """Stale snapshot → measurement runs; new evidence replaces stale row."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # Seed evidence with files that don't match the on-disk snapshot.
            from lib.quality import AlbumQualityEvidenceFile
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                owner_id=download_log_id,
                files=[AlbumQualityEvidenceFile(
                    relative_path="stale.mp3",
                    size_bytes=999,
                    mtime_ns=1,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                )],
            ))

            preview_result = ImportPreviewResult(
                mode="path",
                verdict="would_import",
                would_import=True,
                decision="import",
                reason="import",
                stage_chain=["preview:import"],
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview re-measures and persists fresh
                # evidence with the actual on-disk snapshot.
                db.upsert_album_quality_evidence(make_album_quality_evidence(
                    owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                    owner_id=download_log_id,
                    files=snapshot_audio_files(source),
                ))
                return preview_result

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        # Snapshot mismatch → front-gate misses → preview ran.
        preview.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        # The stale evidence row was replaced.
        from lib.quality import AlbumQualityEvidenceOwner
        evidence = db.load_album_quality_evidence(AlbumQualityEvidenceOwner(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            owner_id=download_log_id,
        ))
        assert evidence is not None
        self.assertEqual(len(evidence.files), 1)
        self.assertEqual(evidence.files[0].relative_path, "01.mp3")
