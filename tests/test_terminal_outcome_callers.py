"""Caller contracts for the DB-owned terminal outcome boundary."""

from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, cast
from unittest import mock

from lib import transitions
from lib.config import CratediggerConfig
from lib.dispatch import DispatchOutcome
from lib.dispatch.post_import import _run_or_stage_quality_gate
from lib.dispatch.quality_gate import QualityGatePlan
from lib.import_preview import ImportPreviewResult
from lib.import_queue import (
    IMPORT_JOB_FORCE,
)
from lib.quality import MeasurementFailure
from lib.media_readiness import MediaReadinessError
from lib.terminal_outcomes import (
    ImportJobTerminal,
    PendingImportTerminalOutcome,
    TerminalDownloadAudit,
)
from scripts import import_preview_worker, importer
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    make_request_row,
)


def _seed_request(db: FakePipelineDB) -> None:
    db.seed_request(make_request_row(
        id=42,
        status="downloading",
        active_download_state={"files": []},
    ))


class TestTerminalOutcomeCallers(unittest.TestCase):
    def test_rejection_cannot_claim_successful_terminal_acceptance(self) -> None:
        pending = PendingImportTerminalOutcome(
            request_id=42,
            import_job_id=7,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="rejected"),
        ).mark_successful_terminal_acceptance()

        with self.assertRaisesRegex(
            ValueError,
            "successful terminal acceptance requires",
        ):
            pending.with_job(ImportJobTerminal(
                status="failed",
                error="rejected",
                result={"success": False},
                message="rejected",
            ))

    def test_quality_gate_acceptance_marks_pending_terminal_bundle(self) -> None:
        pending = PendingImportTerminalOutcome(
            request_id=42,
            import_job_id=7,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="success"),
        )

        def accepted_plan(**_kwargs: object) -> QualityGatePlan:
            return QualityGatePlan(
                transition=transitions.RequestTransition.to_imported(
                    from_status="imported",
                ),
                successful_terminal_acceptance=True,
            )

        result = _run_or_stage_quality_gate(
            accepted_plan,
            pending,
            db=cast(Any, FakePipelineDB()),
            request_id=42,
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.successful_terminal_acceptance)

    def test_importer_consumes_pending_bundle_without_double_finalization(self) -> None:
        db = FakePipelineDB()
        _seed_request(db)
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/atomic"},
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = claim_next_import_job(db, worker_id="atomic")
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
        db.seed_request(make_request_row(id=42, status="unsearchable"))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/atomic"},
        )
        claimed = claim_next_import_preview_job(db, worker_id="atomic-preview")
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
        self.assertEqual(db.request(42)["status"], "unsearchable")
        command = db.persist_preview_terminal_outcome_calls[0]
        self.assertIsNone(command.request_transition)

    def test_force_probe_failure_does_not_escape_into_preview_retry(self) -> None:
        """Best-effort readiness failures reach force measurement terminalization."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="unsearchable"))
        with tempfile.TemporaryDirectory() as tmp:
            downloads = os.path.join(tmp, "downloads")
            processing = os.path.join(tmp, "processing")
            source = os.path.join(downloads, "failed_imports", "Five Suns")
            os.makedirs(source)
            os.mkdir(processing, 0o700)
            os.mkdir(os.path.join(processing, "albums"), 0o700)
            os.mkdir(os.path.join(processing, "preview"), 0o700)
            # Valid FLAC metadata envelope with a zero total-sample field:
            # enough to choose the readiness recovery path; decode is mocked
            # so this caller test isolates a deterministic probe failure.
            with open(os.path.join(source, "01.flac"), "wb") as handle:
                handle.write(b"fLaC\0\0\0\x22" + b"\0" * 34)
            cfg = CratediggerConfig(
                slskd_download_dir=downloads,
                processing_dir=processing,
                audio_check_mode="off",
            )
            download_log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={"failed_path": source},
            )
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": download_log_id, "failed_path": source},
            )
            claimed = claim_next_import_preview_job(db, worker_id="probe-failure-preview")
            assert claimed is not None

            def failed_measurement(_db: object, **kwargs: object) -> ImportPreviewResult:
                source_path = str(kwargs["source_display_path"])
                return ImportPreviewResult(
                    mode="path",
                    verdict="measurement_failed",
                    reason="measurement_crashed",
                    detail="ffprobe unavailable",
                    source_path=source_path,
                    failure=MeasurementFailure(
                        reason="measurement_crashed",
                        detail="ffprobe unavailable",
                        source_path=source_path,
                    ),
                )

            def no_current_evidence(*_args: object, **_kwargs: object) -> str:
                return "no_current_evidence"

            with mock.patch("lib.media_readiness._strict_decode"), mock.patch(
                "lib.media_readiness._ffprobe_readiness",
                side_effect=MediaReadinessError("measurement_failed", "ffprobe unavailable"),
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                    runtime_config=cfg,
                    candidate_measurement_fn=failed_measurement,
                    prepare_failure_have_fn=no_current_evidence,
                )

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(updated.preview_status, "measurement_failed")
            self.assertEqual(updated.preview_error, "measurement_crashed")
            self.assertEqual(len(db.persist_preview_terminal_outcome_calls), 1)
            self.assertEqual(db.request(42)["status"], "unsearchable")


if __name__ == "__main__":
    unittest.main()
