"""Tests for the shared import queue worker."""

import unittest
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
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestImporterWorker(unittest.TestCase):
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
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            updated = importer.process_claimed_job(db, claimed)

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
        self.assertEqual(updated.result["success"], True)
        self.assertEqual(job.id, updated.id)

    def test_manual_import_failure_marks_job_failed(self):
        from scripts import importer

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(False, "quality gate rejected"),
        ):
            updated = importer.process_claimed_job(db, claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.error, "quality gate rejected")
        self.assertEqual(updated.result["success"], False)

    def test_startup_requeues_abandoned_running_job_for_retry(self):
        from scripts import importer

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        claimed = db.claim_next_import_job(worker_id="old-worker")
        assert claimed is not None

        recovered = importer.recover_abandoned_running_jobs(db)

        self.assertEqual([job.id for job in recovered], [claimed.id])
        self.assertEqual(recovered[0].status, "queued")
        self.assertIsNone(recovered[0].worker_id)
        self.assertIsNone(recovered[0].heartbeat_at)

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported on retry"),
        ):
            updated = importer.run_once(db, worker_id="new-worker")

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        retried = db.get_import_job(claimed.id)
        assert retried is not None
        self.assertEqual(retried.attempts, 2)

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
        db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=True,
        ) as processing:
            updated = importer.process_claimed_job(
                db,
                claimed,
                ctx=object(),
            )

        processing.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Automation import processing completed")
