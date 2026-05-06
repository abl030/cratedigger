"""Tests for the shared import queue worker."""

import os
import shutil
import tempfile
import unittest
from typing import Any, cast
from unittest.mock import patch

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
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_request_row


class TestImporterWorker(unittest.TestCase):
    def _mark_importable(self, db: FakePipelineDB, job):
        updated = db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
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
            preview_enabled=True,
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
        )
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(self._result(updated)["success"], True)
        self.assertEqual(job.id, updated.id)

    def test_manual_import_failure_marks_job_failed(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
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
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
                preview_enabled=True,
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(False, "Pre-import gate rejected"),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
            result = self._result(updated)
            self.assertEqual(result["cleanup"]["success"], True)
            self.assertEqual(result["cleanup"]["cleared_rows"], 1)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_failed_force_import_job_clears_newer_duplicate_rejection(self):
        from scripts import importer

        db = FakePipelineDB()
        source = tempfile.mkdtemp()
        try:
            log_id = self._log_wrong_match(db, failed_path=source, username="old")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
                preview_enabled=True,
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
            ):
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
                preview_enabled=True,
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
                preview_enabled=True,
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
            preview_enabled=True,
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

    def test_importer_claims_job_immediately_when_preview_disabled_by_default(self):
        db = FakePipelineDB()
        with patch.dict(os.environ, {}, clear=True):
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
                payload=manual_import_payload(failed_path="/tmp/manual"),
            )

        claimed = db.claim_next_import_job(worker_id="worker")

        assert claimed is not None
        self.assertEqual(claimed.id, job.id)
        self.assertEqual(claimed.preview_status, "would_import")
        self.assertEqual(claimed.preview_message, "Preview gate disabled")

    def test_importer_does_not_claim_job_waiting_for_preview(self):
        from scripts import importer

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
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
            preview_enabled=True,
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
            preview_enabled=False,
        )
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=DispatchOutcome(True, "Imported by dispatch"),
        ):
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

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
        db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
            preview_enabled=False,
        )
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
                preview_enabled=False,
            )
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
        )

    def test_force_job_preview_would_import_marks_importable(self):
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
            preview_enabled=True,
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=self._preview("would_import", reason="import"),
        ) as preview:
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        preview.assert_called_once_with(
            db,
            request_id=42,
            path="/tmp/failed",
            force=True,
            source_username="alice",
            download_log_id=7,
        )
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "would_import")
        assert updated.preview_result is not None
        self.assertEqual(updated.preview_result["verdict"], "would_import")
        self.assertIsNotNone(updated.importable_at)

    def test_manual_job_preview_uses_non_force_semantics(self):
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=self._preview("would_import", reason="import"),
        ) as preview:
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        preview.assert_called_once_with(
            db,
            request_id=42,
            path="/tmp/manual",
            force=False,
            source_username=None,
            download_log_id=None,
        )
        assert updated is not None
        self.assertEqual(updated.preview_status, "would_import")

    def test_automation_job_preview_uses_active_download_current_path(self):
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "current_path": "/tmp/staged",
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
            preview_enabled=True,
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=self._preview("would_import", reason="import"),
        ) as preview:
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        preview.assert_called_once_with(
            db,
            request_id=42,
            path="/tmp/staged",
            force=False,
            source_username="alice",
            download_log_id=None,
        )
        assert updated is not None
        self.assertEqual(updated.preview_status, "would_import")

    def test_confident_reject_fails_job_and_denylists_source(self):
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
            preview_enabled=True,
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
        self.assertEqual(updated.preview_status, "confident_reject")
        self.assertEqual(updated.preview_error, "spectral_reject")
        self.assertEqual(db.get_denylisted_users(42)[0]["username"], "alice")

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
            preview_enabled=True,
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
