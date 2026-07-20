"""Import-job launch authorization and crash-recovery contracts (#703)."""

from __future__ import annotations

from typing import Any, cast
import unittest
from unittest.mock import patch

from lib.dispatch import DispatchOutcome
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_RECOVERY_REQUIRED,
    automation_import_dedupe_key,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.pipeline_db import PipelineDB
from lib.import_job_recovery_service import resolve_import_job_recovery
from lib.terminal_outcomes import (
    ImportJobTerminal,
    ImportTerminalOutcome,
    PendingImportTerminalOutcome,
    TerminalDownloadAudit,
    TerminalOutcomeResult,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


def _seed_candidate(
    db: FakePipelineDB,
    job_id: int,
    *,
    release_id: str,
    source_path: str,
) -> str:
    evidence = make_album_quality_evidence(
        mb_release_id=release_id,
        source_path=source_path,
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_import_job_candidate_evidence(job_id, persisted.id)
    return evidence.snapshot_fingerprint


class TestImportOperationFence(unittest.TestCase):
    def _force_recovery_job(self) -> tuple[FakePipelineDB, Any]:
        db = FakePipelineDB()
        source_path = "/tmp/recovery-force"
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="release-42",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:recovery",
            payload={"failed_path": source_path},
        )
        _seed_candidate(
            db,
            job.id,
            release_id="release-42",
            source_path=source_path,
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None
        launched = db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-42",
            source_path=source_path,
            expected_request_status="wanted",
        )
        assert launched is not None
        recovery = db.mark_import_job_recovery_required(
            claimed.id,
            reason="worker disappeared",
        )
        assert recovery is not None
        return db, recovery

    def test_stale_release_authority_refuses_launch_before_beets(self) -> None:
        db = FakePipelineDB()
        source_path = "/tmp/operator-copy"
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="release-new",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7003),
            payload=force_import_payload(
                download_log_id=7003,
                failed_path=source_path,
            ),
        )
        _seed_candidate(
            db,
            job.id,
            release_id="release-old",
            source_path=source_path,
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        beets_invocations: list[int] = []
        authorized = db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-old",
            source_path=source_path,
            expected_request_status="wanted",
        )
        if authorized is not None:
            beets_invocations.append(claimed.id)

        self.assertIsNone(authorized)
        self.assertEqual(beets_invocations, [])
        current = db.get_import_job(claimed.id)
        assert current is not None
        self.assertIsNone(current.beets_launch_authorized_at)

    def test_startup_requeues_only_jobs_proven_not_started(self) -> None:
        from scripts import importer

        db = FakePipelineDB()
        for request_id, source_path in ((1, "/tmp/one"), (2, "/tmp/two")):
            db.seed_request(make_request_row(
                id=request_id,
                mb_release_id=f"release-{request_id}",
                status="wanted",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=f"force:{request_id}",
                payload={"failed_path": source_path},
            )
            _seed_candidate(
                db,
                job.id,
                release_id=f"release-{request_id}",
                source_path=source_path,
            )
            db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})

        first = db.claim_next_import_job(worker_id="old-worker")
        assert first is not None
        second = db.claim_next_import_job(worker_id="old-worker")
        assert second is not None
        authorized = db.authorize_import_job_launch(
            second.id,
            request_id=2,
            release_id="release-2",
            source_path="/tmp/two",
            expected_request_status="wanted",
        )
        assert authorized is not None

        recovered = importer.recover_abandoned_running_jobs(cast(Any, db))
        by_id = {job.id: job for job in recovered}

        self.assertEqual(by_id[first.id].status, "queued")
        self.assertEqual(
            by_id[second.id].status,
            IMPORT_JOB_RECOVERY_REQUIRED,
        )
        retry = db.claim_next_import_job(worker_id="new-worker-1")
        assert retry is not None
        self.assertEqual(retry.id, first.id)
        self.assertIsNone(db.claim_next_import_job(worker_id="new-worker-2"))

    def test_launched_exception_becomes_recovery_required_not_failed(self) -> None:
        from scripts import importer

        db = FakePipelineDB()
        source_path = "/tmp/force"
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="release-42",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:exception",
            payload={"failed_path": source_path},
        )
        fingerprint = _seed_candidate(
            db,
            job.id,
            release_id="release-42",
            source_path=source_path,
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None
        authorized = db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-42",
            source_path=source_path,
            expected_request_status="wanted",
        )
        assert authorized is not None

        def crash(*_args: object, **_kwargs: object) -> Any:
            raise RuntimeError("lost subprocess acknowledgement")

        recovered = importer.process_claimed_job(
            cast(Any, db),
            authorized,
            execute_fn=crash,
        )

        assert recovered is not None
        self.assertEqual(recovered.status, IMPORT_JOB_RECOVERY_REQUIRED)
        self.assertEqual(recovered.beets_launch_release_id, "release-42")
        self.assertEqual(recovered.beets_launch_source_path, source_path)
        self.assertEqual(recovered.beets_launch_snapshot_fingerprint, fingerprint)
        self.assertIn("lost subprocess acknowledgement", recovered.message or "")
        self.assertIsNone(db.claim_next_import_job(worker_id="replay-worker"))

    def test_terminal_acknowledgement_prevents_recovery_replay(self) -> None:
        db = FakePipelineDB()
        source_path = "/tmp/acknowledged-force"
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="release-42",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:acknowledged",
            payload={"failed_path": source_path},
        )
        _seed_candidate(
            db,
            job.id,
            release_id="release-42",
            source_path=source_path,
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None
        assert db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-42",
            source_path=source_path,
            expected_request_status="wanted",
        ) is not None
        terminal = PendingImportTerminalOutcome(
            request_id=42,
            import_job_id=claimed.id,
            initial_transition=None,
            audit=TerminalDownloadAudit(outcome="force_import"),
        ).with_job(ImportJobTerminal(
            status="completed",
            result={"success": True},
            message="acknowledged",
        ))

        db.persist_import_terminal_outcome(terminal)
        recovered = db.recover_running_import_jobs(
            requeue_message="safe retry",
            recovery_message="operator recovery required",
        )

        self.assertEqual(recovered, [])
        completed = db.get_import_job(claimed.id)
        assert completed is not None
        self.assertEqual(completed.status, "completed")
        self.assertIsNone(db.claim_next_import_job(worker_id="replay"))

    def test_automatic_launch_binds_current_request_source(self) -> None:
        db = FakePipelineDB()
        source_path = "/incoming/Artist - Album [request-42]"
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="release-42",
            status="downloading",
            active_download_state={"current_path": source_path, "files": []},
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        _seed_candidate(
            db,
            job.id,
            release_id="release-42",
            source_path=source_path,
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        authorized = db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-42",
            source_path=source_path,
            expected_request_status="downloading",
        )

        assert authorized is not None
        self.assertEqual(authorized.beets_launch_request_status, "downloading")

    def test_operator_retry_closes_ambiguous_operation_and_mints_new_job(self) -> None:
        db, recovery = self._force_recovery_job()

        result = resolve_import_job_recovery(
            db,
            recovery.id,
            resolution="retry",
            reason="Checked Beets DB and source; mutation was not applied",
        )

        self.assertEqual(result.outcome, "retry_queued")
        assert result.job is not None and result.retry_job is not None
        self.assertEqual(result.job.status, "failed")
        self.assertNotEqual(result.retry_job.id, recovery.id)
        self.assertEqual(result.retry_job.status, "queued")
        self.assertIsNone(result.retry_job.beets_launch_authorized_at)
        resolution_result = result.job.result
        assert resolution_result is not None
        self.assertEqual(
            resolution_result["recovery_resolution"]["resolution"],
            "retry",
        )

    def test_operator_close_never_schedules_replay(self) -> None:
        db, recovery = self._force_recovery_job()

        result = resolve_import_job_recovery(
            db,
            recovery.id,
            resolution="close",
            reason="Library and request were reconciled manually",
        )

        self.assertEqual(result.outcome, "closed")
        self.assertIsNone(result.retry_job)
        self.assertEqual(len(db.list_import_jobs()), 1)
        self.assertIsNone(db.claim_next_import_job(worker_id="replay"))

    def test_operator_retry_refuses_authority_changed_during_inspection(self) -> None:
        db, recovery = self._force_recovery_job()
        db.request(42)["status"] = "unsearchable"

        result = resolve_import_job_recovery(
            db,
            recovery.id,
            resolution="retry",
            reason="Inspection started before the request changed",
        )

        self.assertEqual(result.outcome, "authority_changed")
        current = db.get_import_job(recovery.id)
        assert current is not None
        self.assertEqual(current.status, IMPORT_JOB_RECOVERY_REQUIRED)
        self.assertEqual(len(db.list_import_jobs()), 1)

    def test_destructive_cleanup_waits_for_terminal_acknowledgement(self) -> None:
        from scripts import importer

        class TerminalFailureDB(FakePipelineDB):
            def persist_import_terminal_outcome(
                self,
                command: ImportTerminalOutcome,
            ) -> TerminalOutcomeResult:
                del command
                raise RuntimeError("terminal acknowledgement failed")

        db = TerminalFailureDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:cleanup-order",
            payload={"failed_path": "/tmp/operator-copy"},
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None
        pending = PendingImportTerminalOutcome(
            request_id=42,
            import_job_id=claimed.id,
            initial_transition=None,
            audit=TerminalDownloadAudit(outcome="rejected"),
        )

        def rejected(*_args: object, **_kwargs: object) -> DispatchOutcome:
            return DispatchOutcome(
                success=False,
                message="rejected",
                terminal_outcome=pending,
            )

        with patch.object(importer, "_cleanup_failed_force_import") as cleanup:
            with self.assertRaisesRegex(
                RuntimeError,
                "terminal acknowledgement failed",
            ):
                importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    execute_fn=rejected,
                )
        cleanup.assert_not_called()

    def test_automation_retry_clears_legacy_request_launch_guard(self) -> None:
        db = FakePipelineDB()
        source_path = "/incoming/Artist - Album [request-42]"
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="release-42",
            status="downloading",
            active_download_state={
                "current_path": source_path,
                "files": [],
                "import_subprocess_started_at": "2026-07-20T01:02:03+00:00",
            },
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        _seed_candidate(
            db,
            job.id,
            release_id="release-42",
            source_path=source_path,
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None
        assert db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-42",
            source_path=source_path,
            expected_request_status="downloading",
        ) is not None
        recovery = db.mark_import_job_recovery_required(
            claimed.id,
            reason="crash",
        )
        assert recovery is not None

        result = resolve_import_job_recovery(
            db,
            recovery.id,
            resolution="retry",
            reason="Confirmed Beets did not apply the import",
        )

        self.assertEqual(result.outcome, "retry_queued")
        state = db.request(42)["active_download_state"]
        self.assertNotIn("import_subprocess_started_at", state)


@requires_postgres
class TestImportOperationFencePostgres(unittest.TestCase):
    def test_launch_marker_survives_connection_loss_and_blocks_replay(self) -> None:
        db = make_db()
        self.addCleanup(db.close)
        source_path = "/tmp/postgres-force"
        request_id = db.add_request(
            artist_name="Fence",
            album_title="Postgres",
            source="request",
            mb_release_id="release-pg",
            status="wanted",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:postgres-fence",
            payload={"failed_path": source_path},
        )
        evidence = make_album_quality_evidence(
            mb_release_id="release-pg",
            source_path=source_path,
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="postgres-worker")
        assert claimed is not None

        launched = db.authorize_import_job_launch(
            claimed.id,
            request_id=request_id,
            release_id="release-pg",
            source_path=source_path,
            expected_request_status="wanted",
        )
        assert launched is not None
        db.close()

        observer = PipelineDB(db.dsn)
        self.addCleanup(observer.close)
        recovered = observer.recover_running_import_jobs(
            requeue_message="safe retry",
            recovery_message="operator recovery required",
        )

        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].status, IMPORT_JOB_RECOVERY_REQUIRED)
        self.assertEqual(
            recovered[0].beets_launch_snapshot_fingerprint,
            evidence.snapshot_fingerprint,
        )
        self.assertIsNone(observer.claim_next_import_job(worker_id="replay"))

        resolution = resolve_import_job_recovery(
            observer,
            recovered[0].id,
            resolution="retry",
            reason="Real PostgreSQL check confirmed no Beets mutation",
        )
        self.assertEqual(resolution.outcome, "retry_queued")
        assert resolution.job is not None and resolution.retry_job is not None
        self.assertEqual(resolution.job.status, "failed")
        self.assertNotEqual(resolution.retry_job.id, recovered[0].id)
        self.assertEqual(resolution.retry_job.status, "queued")

    def test_unlaunched_running_job_is_requeued(self) -> None:
        db = make_db()
        self.addCleanup(db.close)
        request_id = db.add_request(
            artist_name="Fence",
            album_title="Never Started",
            source="request",
            mb_release_id="release-never-started",
            status="wanted",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:postgres-unlaunched",
            payload={"failed_path": "/tmp/unlaunched"},
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="postgres-worker")
        assert claimed is not None

        recovered = db.recover_running_import_jobs(
            requeue_message="safe retry",
            recovery_message="operator recovery required",
        )

        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].status, "queued")
        retry = db.claim_next_import_job(worker_id="retry")
        assert retry is not None
        self.assertEqual(retry.id, claimed.id)

    def test_terminal_acknowledgement_rollback_preserves_launch_marker(self) -> None:
        from tests.test_terminal_outcomes import (
            FaultInjectingPipelineDB,
            InjectedTerminalWriteFailure,
            _searching_import_outcome,
        )

        assert TEST_DSN is not None
        db = make_db()
        self.addCleanup(db.close)
        source_path = "/tmp/postgres-terminal-rollback"
        request_id = db.add_request(
            artist_name="Fence",
            album_title="Terminal rollback",
            source="request",
            mb_release_id="release-terminal-rollback",
            status="wanted",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:postgres-terminal-rollback",
            payload={"failed_path": source_path},
        )
        evidence = make_album_quality_evidence(
            mb_release_id="release-terminal-rollback",
            source_path=source_path,
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="postgres-worker")
        assert claimed is not None
        assert db.authorize_import_job_launch(
            claimed.id,
            request_id=request_id,
            release_id="release-terminal-rollback",
            source_path=source_path,
            expected_request_status="wanted",
        ) is not None

        failing = FaultInjectingPipelineDB(TEST_DSN, fail_after_write=1)
        try:
            with self.assertRaises(InjectedTerminalWriteFailure):
                failing.persist_import_terminal_outcome(
                    _searching_import_outcome(request_id, claimed.id)
                )
        finally:
            failing.close()

        observer = PipelineDB(TEST_DSN)
        self.addCleanup(observer.close)
        still_running = observer.get_import_job(claimed.id)
        assert still_running is not None
        self.assertEqual(still_running.status, "running")
        self.assertIsNotNone(still_running.beets_launch_authorized_at)
        recovered = observer.recover_running_import_jobs(
            requeue_message="safe retry",
            recovery_message="operator recovery required",
        )
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].status, IMPORT_JOB_RECOVERY_REQUIRED)
        self.assertIsNone(observer.claim_next_import_job(worker_id="replay"))


if __name__ == "__main__":
    unittest.main()
