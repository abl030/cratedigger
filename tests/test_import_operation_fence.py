"""Import-job launch authorization and crash-recovery contracts (#703)."""

from __future__ import annotations

import os
import signal
import tempfile
import threading
import time
import unittest
from typing import Any, cast
from unittest.mock import MagicMock, patch

from lib.config import CratediggerConfig
from lib.dispatch import DispatchOutcome
from lib.dispatch.types import PostCommitCleanup
from lib.import_evidence import ensure_candidate_evidence_for_action
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    MonitoredProcessGroup,
    ProcessIdentity,
)
from lib.import_job_recovery_service import resolve_import_job_recovery
from lib.import_preview import force_action_copy_path
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_RECOVERY_REQUIRED,
    IMPORT_JOB_YOUTUBE,
    ForceImportPayload,
    force_import_dedupe_key,
    force_import_payload,
    youtube_import_payload,
)
from lib.pipeline_db import PipelineDB
from lib.quality_evidence import snapshot_audio_files
from lib.terminal_outcomes import (
    ImportJobTerminal,
    ImportTerminalOutcome,
    PendingImportTerminalOutcome,
    TerminalDownloadAudit,
    TerminalOutcomeResult,
)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    handoff_automation_owner,
    make_album_quality_evidence,
    make_request_row,
)
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


class TestOwnedImportSubprocessRunner(unittest.TestCase):
    def test_child_identity_callback_completes_before_wait(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        token = CancellationToken()
        events: list[str] = []
        monitored = MagicMock()
        monitored.pid = 4321

        def wait(
            observed_token: CancellationToken,
            *,
            owner_session_probe: Any,
        ) -> int:
            self.assertIs(observed_token, token)
            self.assertEqual(events, ["spawn:4321"])
            self.assertTrue(owner_session_probe())
            events.append("wait")
            return 0

        monitored.wait.side_effect = wait
        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen"),
            patch(
                "lib.dispatch.subprocess_runner.MonitoredProcessGroup",
                return_value=monitored,
            ),
        ):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                cancellation_token=token,
                on_spawn=lambda pid: events.append(f"spawn:{pid}"),
                owner_session_probe=lambda: True,
            )

        self.assertEqual(events, ["spawn:4321", "wait"])

    def test_spawn_callback_failure_terminates_before_propagating(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        monitored = MagicMock()
        monitored.pid = 4321

        def reject_child(_pid: int) -> None:
            raise RuntimeError("child lease CAS rejected")

        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen"),
            patch(
                "lib.dispatch.subprocess_runner.MonitoredProcessGroup",
                return_value=monitored,
            ),
            self.assertRaisesRegex(RuntimeError, "child lease CAS rejected"),
        ):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                cancellation_token=CancellationToken(),
                on_spawn=reject_child,
            )

        monitored.terminate_and_wait.assert_called_once_with()
        monitored.wait.assert_not_called()

    def test_owner_cancellation_wins_when_timeout_also_fires(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        token = CancellationToken()
        monitored = MagicMock()
        monitored.pid = 4321

        def wait(*_args: Any, **_kwargs: Any) -> int:
            token.cancel("owner_session_lost")
            time.sleep(0.02)
            return -15

        monitored.wait.side_effect = wait
        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen"),
            patch(
                "lib.dispatch.subprocess_runner.MonitoredProcessGroup",
                return_value=monitored,
            ),
            self.assertRaisesRegex(ExecutionCancelled, "owner_session_lost"),
        ):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                timeout=0,
                cancellation_token=token,
            )

        monitored.terminate_and_wait.assert_called()

    def test_concurrent_termination_is_serialized_and_idempotent(self) -> None:
        process = MagicMock()
        process.pid = 4321
        process.poll.return_value = -15
        process.wait.return_value = -15
        monitored = MonitoredProcessGroup(process)
        barrier = threading.Barrier(3)
        results: list[int] = []

        def terminate() -> None:
            barrier.wait()
            results.append(monitored.terminate_and_wait())

        threads = [threading.Thread(target=terminate) for _ in range(2)]
        for thread in threads:
            thread.start()
        with patch(
            "lib.import_execution.os.killpg",
            side_effect=ProcessLookupError,
        ) as killpg:
            barrier.wait()
            for thread in threads:
                thread.join(timeout=1.0)

        self.assertEqual(results, [-15, -15])
        term_calls = [
            call for call in killpg.call_args_list
            if call.args == (4321, signal.SIGTERM)
        ]
        self.assertEqual(len(term_calls), 1)
        process.wait.assert_called_once()


def _seed_candidate(
    db: FakePipelineDB,
    job_id: int,
    *,
    release_id: str,
    source_path: str,
    expected_execution_lease: ExecutionLeaseSnapshot | None = None,
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
    if expected_execution_lease is None:
        db.set_import_job_candidate_evidence(job_id, persisted.id)
    else:
        db.set_import_job_candidate_evidence(
            job_id,
            persisted.id,
            expected_execution_lease=expected_execution_lease,
        )
    return evidence.snapshot_fingerprint


def _claim_automation_job(
    db: FakePipelineDB,
    *,
    release_id: str,
    source_path: str,
    preview_result: dict[str, Any] | None = None,
) -> tuple[Any, ExecutionLeaseSnapshot]:
    request = db.request(42)
    active_state = dict(request.get("active_download_state") or {})
    active_state.update({
        "current_path": source_path,
        "filetype": active_state.get("filetype") or "flac",
        "enqueued_at": (
            active_state.get("enqueued_at")
            or "2026-07-29T00:00:00+00:00"
        ),
        "files": active_state.get("files") or [],
    })
    db.seed_request({
        **request,
        "status": "wanted",
        "active_download_state": None,
        "active_automation_import_job_id": None,
    })
    job = handoff_automation_owner(
        db,
        42,
        state=active_state,
        canonical_path=source_path,
    )
    preview_lease = ExecutionLeaseSnapshot(
        host_boot_id="operation-fence-boot",
        invocation_id=f"operation-preview-{job.id}",
        systemd_unit="cratedigger-import-preview-worker.service",
        worker=ProcessIdentity(8501, 85001),
    )
    assert db.claim_next_import_preview_job(
        worker_id="preview",
        execution_lease=preview_lease,
    ) is not None
    _seed_candidate(
        db,
        job.id,
        release_id=release_id,
        source_path=source_path,
        expected_execution_lease=preview_lease,
    )
    assert db.mark_import_job_preview_importable(
        job.id,
        preview_result=preview_result or {"ready": True},
        expected_execution_lease=preview_lease,
    ) is not None
    importer_lease = ExecutionLeaseSnapshot(
        host_boot_id="operation-fence-boot",
        invocation_id=f"operation-importer-{job.id}",
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(8502, 85002),
    )
    claimed = db.claim_next_import_job(
        worker_id="worker",
        execution_lease=importer_lease,
    )
    assert claimed is not None
    return claimed, importer_lease


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
            payload={"download_log_id": 1, "failed_path": source_path},
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
        )
        if authorized is not None:
            beets_invocations.append(claimed.id)

        self.assertIsNone(authorized)
        self.assertEqual(beets_invocations, [])
        current = db.get_import_job(claimed.id)
        assert current is not None
        self.assertIsNone(current.beets_launch_authorized_at)

    def test_relocated_evidence_uses_job_path_as_launch_authority(self) -> None:
        """Moved bytes use owned job path without rewriting evidence metadata."""

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as action_path:
            with open(os.path.join(action_path, "01.mp3"), "wb") as handle:
                handle.write(b"moved-but-identical")
            capture_path = "/pre-quarantine/operator-copy"
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="release-42",
                status="wanted",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(7004),
                payload=force_import_payload(
                    download_log_id=7004,
                    failed_path=action_path,
                ),
            )
            evidence = make_album_quality_evidence(
                mb_release_id="release-42",
                source_path=capture_path,
                files=snapshot_audio_files(action_path),
            )
            db.upsert_album_quality_evidence(evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db.set_import_job_candidate_evidence(job.id, persisted.id)
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            candidate = ensure_candidate_evidence_for_action(
                db,
                source_path=action_path,
                import_job_id=claimed.id,
            )
            self.assertTrue(candidate.available)
            assert candidate.evidence is not None
            self.assertEqual(candidate.evidence.source_path, capture_path)

            assert isinstance(claimed.payload, ForceImportPayload)
            active_job_path = claimed.payload.failed_path
            authorized = db.authorize_import_job_launch(
                claimed.id,
                request_id=42,
                release_id="release-42",
                source_path=active_job_path,
            )

            assert authorized is not None
            self.assertEqual(authorized.beets_launch_source_path, action_path)
            self.assertEqual(
                authorized.beets_launch_snapshot_fingerprint,
                evidence.snapshot_fingerprint,
            )
            unchanged = db.load_album_quality_evidence_by_id(persisted.id)
            assert unchanged is not None
            self.assertEqual(unchanged.source_path, capture_path)

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
                payload={"download_log_id": 1, "failed_path": source_path},
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
            payload={"download_log_id": 1, "failed_path": source_path},
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
            payload={"download_log_id": 1, "failed_path": source_path},
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
        claimed, execution_lease = _claim_automation_job(
            db,
            release_id="release-42",
            source_path=source_path,
        )

        authorized = db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-42",
            source_path=source_path,
            expected_execution_lease=execution_lease,
        )

        assert authorized is not None
        self.assertEqual(authorized.beets_launch_request_status, "processing")

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

    def test_recovery_retry_resets_only_force_preview_state(self) -> None:
        """#853: only force retries lost a disposable action copy."""
        cases = (
            (IMPORT_JOB_AUTOMATION, "downloading", "/incoming/automation", {}),
            (
                IMPORT_JOB_YOUTUBE,
                "wanted",
                "/incoming/youtube",
                youtube_import_payload(
                    staged_path="/incoming/youtube",
                    request_id=42,
                    browse_id="MPREb_recovery",
                    download_log_id=77,
                ),
            ),
        )
        for job_type, status, source_path, payload in cases:
            with self.subTest(job_type=job_type):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=42,
                    mb_release_id="release-42",
                    status=status,
                    active_download_state=(
                        {"current_path": source_path, "files": []}
                        if job_type == IMPORT_JOB_AUTOMATION else None
                    ),
                ))
                preview_result = {"verdict": "would_import", "sentinel": job_type}
                execution_lease: ExecutionLeaseSnapshot | None = None
                if job_type == IMPORT_JOB_AUTOMATION:
                    claimed, execution_lease = _claim_automation_job(
                        db,
                        release_id="release-42",
                        source_path=source_path,
                        preview_result=preview_result,
                    )
                else:
                    job = db.enqueue_import_job(
                        job_type,
                        request_id=42,
                        dedupe_key=f"{job_type}:recovery-preview",
                        payload=payload,
                    )
                    _seed_candidate(
                        db,
                        job.id,
                        release_id="release-42",
                        source_path=source_path,
                    )
                    db.mark_import_job_preview_importable(
                        job.id, preview_result=preview_result,
                    )
                    claimed = db.claim_next_import_job(worker_id="worker")
                    assert claimed is not None
                candidate_evidence_id = claimed.candidate_evidence_id
                assert db.authorize_import_job_launch(
                    claimed.id,
                    request_id=42,
                    release_id="release-42",
                    source_path=source_path,
                    expected_execution_lease=execution_lease,
                ) is not None
                recovery = db.mark_import_job_recovery_required(
                    claimed.id,
                    reason="ambiguous operation",
                    expected_execution_lease=execution_lease,
                )
                assert recovery is not None

                result = resolve_import_job_recovery(
                    db,
                    recovery.id,
                    resolution="retry",
                    reason="Operator reconciled external mutation",
                )

                if job_type == IMPORT_JOB_AUTOMATION:
                    self.assertEqual(result.outcome, "authority_changed")
                    self.assertIsNone(result.retry_job)
                    continue
                assert result.retry_job is not None
                self.assertEqual(result.retry_job.preview_result, preview_result)
                self.assertEqual(result.retry_job.candidate_evidence_id, candidate_evidence_id)
                self.assertEqual(
                    result.retry_job.beets_launch_snapshot_fingerprint,
                    None,
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

    def test_recovery_resolution_discards_old_force_action_before_close_or_retry(self) -> None:
        """#853: recovery retains a copy only while reconciliation is pending."""
        for resolution in ("close", "retry"):
            with self.subTest(resolution=resolution), tempfile.TemporaryDirectory() as root:
                downloads = os.path.join(root, "downloads")
                processing = os.path.join(root, "processing")
                os.mkdir(downloads, 0o700)
                os.mkdir(processing, 0o700)
                os.mkdir(os.path.join(processing, "albums"), 0o700)
                os.mkdir(os.path.join(processing, "preview"), 0o700)
                cfg = CratediggerConfig(
                    slskd_download_dir=downloads,
                    processing_dir=processing,
                    audio_check_mode="off",
                )
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=42, mb_release_id="release-42", status="wanted",
                ))
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    dedupe_key=f"force:recovery-action:{resolution}",
                    payload={"download_log_id": 1, "failed_path": "/operator/raw"},
                )
                action_path = force_action_copy_path(cfg, job.id)
                os.mkdir(action_path, 0o700)
                with open(os.path.join(action_path, "01.mp3"), "wb") as handle:
                    handle.write(b"action bytes")
                _seed_candidate(
                    db, job.id, release_id="release-42", source_path=action_path,
                )
                db.mark_import_job_preview_importable(
                    job.id, preview_result={"action_path": action_path},
                )
                claimed = db.claim_next_import_job(worker_id="worker")
                assert claimed is not None
                assert db.authorize_import_job_launch(
                    claimed.id,
                    request_id=42,
                    release_id="release-42",
                    source_path="/operator/raw",
                ) is not None
                recovery = db.mark_import_job_recovery_required(
                    claimed.id, reason="ambiguous Beets result",
                )
                assert recovery is not None

                with patch("lib.config.read_runtime_config", return_value=cfg):
                    result = resolve_import_job_recovery(
                        db,
                        recovery.id,
                        resolution=resolution,
                        reason="Operator reconciled Beets and raw source",
                    )

                self.assertFalse(os.path.exists(action_path))
                if resolution == "retry":
                    assert result.retry_job is not None
                    self.assertIsNone(result.retry_job.preview_result)

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
            payload={"download_log_id": 1, "failed_path": "/tmp/operator-copy"},
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
                post_commit_cleanup=PostCommitCleanup(
                    staged_path="/tmp/operator-copy",
                ),
            )

        with patch.object(importer, "_cleanup_failed_force_import") as cleanup, \
             patch.object(importer, "_run_post_commit_cleanup") as post_cleanup, self.assertRaisesRegex(
            RuntimeError,
            "terminal acknowledgement failed",
        ):
            importer.process_claimed_job(
                cast(Any, db),
                claimed,
                execute_fn=rejected,
            )
        cleanup.assert_not_called()
        post_cleanup.assert_not_called()

    def test_post_commit_cleanup_runs_only_after_terminal_persistence(self) -> None:
        from scripts import importer

        events: list[str] = []

        class OrderingDB(FakePipelineDB):
            def persist_import_terminal_outcome(
                self,
                command: ImportTerminalOutcome,
            ) -> TerminalOutcomeResult:
                result = super().persist_import_terminal_outcome(command)
                events.append("terminal")
                return result

        db = OrderingDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/operator-copy"},
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
                post_commit_cleanup=PostCommitCleanup(
                    staged_path="/tmp/operator-copy",
                ),
            )

        def record_cleanup(
            _db: object,
            _outcome: DispatchOutcome,
            *,
            download_log_id: int | None = None,
        ) -> dict[str, object]:
            self.assertEqual(download_log_id, 1)
            events.append("cleanup")
            return {"staged_path": {"success": True}}

        with patch.object(
            importer,
            "_run_post_commit_cleanup",
            side_effect=record_cleanup,
        ):
            importer.process_claimed_job(
                cast(Any, db),
                claimed,
                execute_fn=rejected,
            )

        self.assertEqual(events, ["terminal", "cleanup"])

    def test_unlaunched_youtube_cleanup_waits_for_job_acknowledgement(self) -> None:
        from scripts import importer

        with tempfile.TemporaryDirectory() as tmpdir:
            staged_path = os.path.join(tmpdir, "youtube-staged")
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.opus"), "wb") as handle:
                handle.write(b"operator recovery evidence")

            observations: list[tuple[str, bool]] = []

            class OrderingDB(FakePipelineDB):
                def mark_import_job_failed(
                    self,
                    job_id: int,
                    *,
                    error: str,
                    result: dict[str, Any] | None = None,
                    message: str | None = None,
                ):
                    observations.append(("terminal", os.path.exists(staged_path)))
                    return super().mark_import_job_failed(
                        job_id,
                        error=error,
                        result=result,
                        message=message,
                    )

            db = OrderingDB()
            db.seed_request(make_request_row(id=42, status="wanted"))
            job = db.enqueue_import_job(
                IMPORT_JOB_YOUTUBE,
                request_id=42,
                payload=youtube_import_payload(
                    staged_path=staged_path,
                    request_id=42,
                    browse_id="MPREb_fence",
                    download_log_id=600,
                ),
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None
            db.request(42)["status"] = "imported"

            terminal = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

            assert terminal is not None
            self.assertEqual(terminal.status, "failed")
            self.assertEqual(observations, [("terminal", True)])
            self.assertFalse(os.path.exists(staged_path))

    def test_unlaunched_youtube_ack_failure_preserves_staged_source(self) -> None:
        from scripts import importer

        with tempfile.TemporaryDirectory() as tmpdir:
            staged_path = os.path.join(tmpdir, "youtube-staged")
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.opus"), "wb") as handle:
                handle.write(b"operator recovery evidence")

            class FailingDB(FakePipelineDB):
                def mark_import_job_failed(
                    self,
                    job_id: int,
                    *,
                    error: str,
                    result: dict[str, Any] | None = None,
                    message: str | None = None,
                ):
                    del job_id, error, result, message
                    raise RuntimeError("terminal acknowledgement failed")

            db = FailingDB()
            db.seed_request(make_request_row(id=42, status="wanted"))
            job = db.enqueue_import_job(
                IMPORT_JOB_YOUTUBE,
                request_id=42,
                payload=youtube_import_payload(
                    staged_path=staged_path,
                    request_id=42,
                    browse_id="MPREb_fence",
                    download_log_id=651,
                ),
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None
            db.request(42)["status"] = "imported"

            with self.assertRaisesRegex(
                RuntimeError,
                "terminal acknowledgement failed",
            ):
                importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

            self.assertTrue(os.path.exists(staged_path))

    def test_automation_recovery_stays_attached_to_exact_owner(self) -> None:
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
        claimed, execution_lease = _claim_automation_job(
            db,
            release_id="release-42",
            source_path=source_path,
        )
        assert db.authorize_import_job_launch(
            claimed.id,
            request_id=42,
            release_id="release-42",
            source_path=source_path,
            expected_execution_lease=execution_lease,
        ) is not None
        recovery = db.mark_import_job_recovery_required(
            claimed.id,
            reason="crash",
            expected_execution_lease=execution_lease,
        )
        assert recovery is not None

        result = resolve_import_job_recovery(
            db,
            recovery.id,
            resolution="retry",
            reason="Confirmed Beets did not apply the import",
        )

        self.assertEqual(result.outcome, "authority_changed")
        self.assertIsNone(result.retry_job)
        state = db.request(42)["active_download_state"]
        self.assertIn("import_subprocess_started_at", state)


@requires_postgres
class TestImportOperationFencePostgres(unittest.TestCase):
    def test_relocated_evidence_path_does_not_override_job_authority(self) -> None:
        db = make_db()
        self.addCleanup(db.close)
        source_path = "/failed_imports/postgres-force"
        request_id = db.add_request(
            artist_name="Fence",
            album_title="Relocated PostgreSQL evidence",
            source="request",
            mb_release_id="release-pg-relocated",
            status="wanted",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:postgres-relocated",
            payload={"download_log_id": 1, "failed_path": source_path},
        )
        evidence = make_album_quality_evidence(
            mb_release_id="release-pg-relocated",
            source_path="/pre-quarantine/postgres-force",
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
            release_id="release-pg-relocated",
            source_path=source_path,
        )

        assert launched is not None
        self.assertEqual(launched.beets_launch_source_path, source_path)
        self.assertEqual(
            launched.beets_launch_snapshot_fingerprint,
            evidence.snapshot_fingerprint,
        )

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
            payload={"download_log_id": 1, "failed_path": source_path},
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
            payload={"download_log_id": 1, "failed_path": "/tmp/unlaunched"},
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
            payload={"download_log_id": 1, "failed_path": source_path},
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
