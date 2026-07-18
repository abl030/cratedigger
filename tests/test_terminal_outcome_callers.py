"""Caller contracts for the DB-owned terminal outcome boundary."""

from __future__ import annotations

import unittest
from typing import Any, cast

from lib import transitions
from lib.dispatch import DispatchOutcome
from lib.import_preview import ImportPreviewResult
from lib.import_queue import (
    IMPORT_JOB_FORCE,
)
from lib.quality import MeasurementFailure
from lib.terminal_outcomes import (
    PendingImportTerminalOutcome,
    TerminalDownloadAudit,
)
from scripts import import_preview_worker, importer
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _seed_request(db: FakePipelineDB) -> None:
    db.seed_request(make_request_row(
        id=42,
        status="downloading",
        active_download_state={"files": []},
    ))


class TestTerminalOutcomeCallers(unittest.TestCase):
    def test_importer_consumes_pending_bundle_without_double_finalization(self) -> None:
        db = FakePipelineDB()
        _seed_request(db)
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"failed_path": "/tmp/atomic"},
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="atomic")
        assert claimed is not None
        pending = PendingImportTerminalOutcome(
            request_id=42,
            import_job_id=claimed.id,
            initial_transition=transitions.RequestTransition.to_wanted(
                attempt_type="validation"
            ),
            audit=TerminalDownloadAudit(
                outcome="rejected",
                validation_result='{"scenario":"atomic_reject"}',
            ),
        )

        def execute_job(*_args: object, **_kwargs: object) -> DispatchOutcome:
            return DispatchOutcome(
                success=False,
                message="atomic reject",
                terminal_outcome=pending,
            )

        updated = importer.process_claimed_job(
            cast(Any, db),
            claimed,
            execute_fn=execute_job,
        )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(len(db.persist_import_terminal_outcome_calls), 1)
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.request(42)["status"], "wanted")

    def test_preview_worker_uses_one_terminal_persistence_call(self) -> None:
        db = FakePipelineDB()
        _seed_request(db)
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"failed_path": "/tmp/atomic"},
        )
        claimed = db.claim_next_import_preview_job(worker_id="atomic-preview")
        assert claimed is not None
        failure = MeasurementFailure(
            reason="source_vanished",
            detail="source disappeared",
            source_path="/tmp/atomic",
        )
        result = ImportPreviewResult(
            mode="path",
            verdict="measurement_failed",
            reason=failure.reason,
            detail=failure.detail,
            source_path=failure.source_path,
            failure=failure,
        )

        updated = import_preview_worker.process_claimed_preview_job(
            db,
            claimed,
            preview_fn=lambda _db, _job: result,
        )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(len(db.persist_preview_terminal_outcome_calls), 1)
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.request(42)["status"], "wanted")


if __name__ == "__main__":
    unittest.main()
