"""Import-job launch authorization and crash-recovery contracts (#703)."""

from __future__ import annotations

import os
import signal
import tempfile
import threading
import time
import unittest
from collections.abc import Callable, Mapping
from typing import Any, cast
from unittest.mock import MagicMock, patch

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
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_RECOVERY_REQUIRED,
    ForceImportPayload,
    ImportJob,
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
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_automation_startup_recovery import (
    _no_debris_removal,
    _no_force_action_copy_path,
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
            owner_session_probe: Callable[[], bool],
        ) -> int:
            self.assertIs(observed_token, token)
            self.assertEqual(events, ["spawn:4321"])
            self.assertTrue(owner_session_probe())
            events.append("wait")
            return 0

        monitored.wait.side_effect = wait
        with patch("lib.dispatch.subprocess_runner.sp.Popen"):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                cancellation_token=token,
                on_spawn=lambda pid: events.append(f"spawn:{pid}"),
                owner_session_probe=lambda: True,
                process_group_factory=lambda _process: monitored,
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
            self.assertRaisesRegex(RuntimeError, "child lease CAS rejected"),
        ):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                cancellation_token=CancellationToken(),
                on_spawn=reject_child,
                process_group_factory=lambda _process: monitored,
            )

        monitored.terminate_and_wait.assert_called_once_with()
        monitored.wait.assert_not_called()

    def test_owner_cancellation_wins_when_timeout_also_fires(self) -> None:
        from lib.dispatch.subprocess_runner import run_import_one

        token = CancellationToken()
        monitored = MagicMock()
        monitored.pid = 4321

        def wait(
            _observed_token: CancellationToken,
            *,
            owner_session_probe: Callable[[], bool] | None = None,
        ) -> int:
            del owner_session_probe
            token.cancel("owner_session_lost")
            time.sleep(0.02)
            return -15

        monitored.wait.side_effect = wait
        with (
            patch("lib.dispatch.subprocess_runner.sp.Popen"),
            self.assertRaisesRegex(ExecutionCancelled, "owner_session_lost"),
        ):
            run_import_one(
                path="/tmp/source",
                mb_release_id="release-1",
                beets_harness_path="/tmp/harness/run",
                timeout=0,
                cancellation_token=token,
                process_group_factory=lambda _process: monitored,
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
    preview_result: Mapping[str, object] | None = None,
) -> tuple[ImportJob, ExecutionLeaseSnapshot]:
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
    assert claim_next_import_preview_job(db, worker_id="preview",
    execution_lease=preview_lease,) is not None
    _seed_candidate(
        db,
        job.id,
        release_id=release_id,
        source_path=source_path,
        expected_execution_lease=preview_lease,
    )
    assert db.mark_import_job_preview_importable(
        job.id,
        preview_result=(
            dict(preview_result)
            if preview_result is not None
            else {"ready": True}
        ),
        expected_execution_lease=preview_lease,
    ) is not None
    importer_lease = ExecutionLeaseSnapshot(
        host_boot_id="operation-fence-boot",
        invocation_id=f"operation-importer-{job.id}",
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(8502, 85002),
    )
    claimed = claim_next_import_job(db, worker_id="worker",
    execution_lease=importer_lease,)
    assert claimed is not None
    return claimed, importer_lease


class TestImportOperationFence(unittest.TestCase):

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
        claimed = claim_next_import_job(db, worker_id="worker")
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
            claimed = claim_next_import_job(db, worker_id="worker")
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
            source_download_log_id = db.log_download(
                request_id,
                outcome="rejected",
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=f"force:{request_id}",
                payload={
                    "download_log_id": source_download_log_id,
                    "failed_path": source_path,
                },
            )
            _seed_candidate(
                db,
                job.id,
                release_id=f"release-{request_id}",
                source_path=source_path,
            )
            db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})

        first = claim_next_import_job(db, worker_id="old-worker")
        assert first is not None
        second = claim_next_import_job(db, worker_id="old-worker")
        assert second is not None
        authorized = db.authorize_import_job_launch(
            second.id,
            request_id=2,
            release_id="release-2",
            source_path="/tmp/two",
        )
        assert authorized is not None

        recovered = importer.recover_abandoned_running_jobs(
            cast(Any, db),
            debris_removal_fn=_no_debris_removal,
            force_action_copy_path_fn=_no_force_action_copy_path,
        )
        by_id = {job.id: job for job in recovered}

        self.assertEqual(by_id[first.id].status, "queued")
        self.assertEqual(by_id[second.id].status, "failed")
        self.assertIsNotNone(by_id[second.id].completed_at)
        retry = claim_next_import_job(db, worker_id="new-worker-1")
        assert retry is not None
        self.assertEqual(retry.id, first.id)
        self.assertIsNone(claim_next_import_job(db, worker_id="new-worker-2"))

    def test_launched_exception_self_heals_the_search_pool(self) -> None:
        """A launched automation exception self-heals; it no longer parks.

        Beets was launched (``authorize_import_job_launch`` succeeded) when
        the execution crashed, so whether Beets already mutated the library
        is unknowable from here. The distinction from an ordinary failure
        still matters: the old policy parked this exact case as
        ``recovery_required`` for a human to adjudicate the ambiguity; the
        current policy (#933, "nothing is ever parked") instead self-heals
        it in the same frame — a ``download_log`` audit row records the
        diagnostic (so it reads in Recents), the exact processing owner is
        cleared, and the request returns to ``wanted`` so the next cycle
        re-derives the truth. The job itself still ends terminally
        ``failed`` and can never be replayed.
        """
        from scripts import importer

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

        def crash(*_args: object, **_kwargs: object) -> Any:
            raise RuntimeError("lost subprocess acknowledgement")

        token = CancellationToken()
        with db._pin_owner_session(token) as owner_session_identity:
            recovered = importer.process_claimed_job(
                cast(Any, db),
                authorized,
                execute_fn=crash,
                execution_lease=execution_lease,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
            )

        assert recovered is not None
        self.assertEqual(recovered.status, "failed")
        self.assertNotEqual(recovered.status, IMPORT_JOB_RECOVERY_REQUIRED)
        self.assertIn("lost subprocess acknowledgement", recovered.message or "")
        request = db.request(42)
        self.assertEqual(request["status"], "wanted")
        self.assertIsNone(request["active_automation_import_job_id"])
        self.assertIsNone(claim_next_import_job(db, worker_id="replay-worker"))
        audit_rows = [row for row in db.download_logs if row.outcome == "failed"]
        self.assertTrue(audit_rows, "no download_log row recorded the world failure")
        audit_message = audit_rows[-1].error_message or ""
        self.assertIn(importer._WORLD_FAILURE_AUDIT_PREFIX, audit_message)
        self.assertIn("lost subprocess acknowledgement", audit_message)

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
        claimed = claim_next_import_job(db, worker_id="worker")
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
        self.assertIsNone(claim_next_import_job(db, worker_id="replay"))

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
        claimed = claim_next_import_job(db, worker_id="worker")
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
        claimed = claim_next_import_job(db, worker_id="worker")
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

        def record_cleanup(_outcome: DispatchOutcome) -> dict[str, object]:
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
            source_download_log_id = db.insert_youtube_running(
                request_id=42,
                browse_id="MPREb_fence",
                audio_playlist_id=None,
                yt_url="https://music.youtube.com/watch?v=fence",
                expected_track_count=1,
            )
            job = db.enqueue_youtube_import_and_mark_success(
                download_log_id=source_download_log_id,
                request_id=42,
                dedupe_key="youtube:cleanup-order",
                payload=youtube_import_payload(
                    staged_path=staged_path,
                    request_id=42,
                    browse_id="MPREb_fence",
                    download_log_id=source_download_log_id,
                ),
                message="youtube staged",
                terminal_metadata={},
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = claim_next_import_job(db, worker_id="worker")
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
            source_download_log_id = db.insert_youtube_running(
                request_id=42,
                browse_id="MPREb_fence",
                audio_playlist_id=None,
                yt_url="https://music.youtube.com/watch?v=fence",
                expected_track_count=1,
            )
            job = db.enqueue_youtube_import_and_mark_success(
                download_log_id=source_download_log_id,
                request_id=42,
                dedupe_key="youtube:cleanup-ack-failure",
                payload=youtube_import_payload(
                    staged_path=staged_path,
                    request_id=42,
                    browse_id="MPREb_fence",
                    download_log_id=source_download_log_id,
                ),
                message="youtube staged",
                terminal_metadata={},
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = claim_next_import_job(db, worker_id="worker")
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



@requires_postgres
class TestImportOperationFencePostgres(unittest.TestCase):
    def test_startup_recovery_removes_launched_force_action_copy(self) -> None:
        from lib.config import CratediggerConfig
        from lib.preview_snapshot import force_action_copy_path
        from scripts import importer

        db = make_db()
        self.addCleanup(db.close)
        with tempfile.TemporaryDirectory() as root:
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
            request_id = db.add_request(
                artist_name="Fence",
                album_title="Startup force cleanup",
                source="request",
                mb_release_id="release-pg-startup-force",
                status="wanted",
            )
            source_download_log_id = db.log_download(
                request_id,
                outcome="rejected",
                error_message="startup force source",
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key="force:postgres-startup-cleanup",
                payload={
                    "download_log_id": source_download_log_id,
                    "failed_path": "/failed-imports/startup-force",
                },
            )
            action_path = force_action_copy_path(cfg, job.id)
            os.mkdir(action_path, 0o700)
            with open(os.path.join(action_path, "01.mp3"), "wb") as handle:
                handle.write(b"private action copy")
            evidence = make_album_quality_evidence(
                mb_release_id="release-pg-startup-force",
                source_path=action_path,
                files=snapshot_audio_files(action_path),
            )
            db.upsert_album_quality_evidence(evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            self.assertTrue(db.set_import_job_candidate_evidence(
                job.id,
                persisted.id,
            ))
            self.assertIsNotNone(db.mark_import_job_preview_importable(
                job.id,
                preview_result={
                    "verdict": "evidence_ready",
                    "action_path": action_path,
                },
            ))
            claimed = claim_next_import_job(db, worker_id="postgres-worker")
            assert claimed is not None
            self.assertIsNotNone(db.authorize_import_job_launch(
                claimed.id,
                request_id=request_id,
                release_id="release-pg-startup-force",
                source_path="/failed-imports/startup-force",
            ))
            terminalized = db.recover_running_import_jobs(
                requeue_message="safe retry",
                recovery_message="startup ambiguity",
                debris_removal_fn=_no_debris_removal,
                force_action_copy_path_fn=_no_force_action_copy_path,
            )
            self.assertEqual([item.id for item in terminalized], [job.id])
            self.assertEqual(terminalized[0].status, "failed")
            history = db.get_download_history(request_id)
            self.assertEqual(len(history), 2)
            self.assertEqual(history[0]["outcome"], "failed")
            self.assertEqual(
                history[0]["source_download_log_id"],
                source_download_log_id,
            )
            request = db.get_request(request_id)
            assert request is not None
            self.assertEqual(request["status"], "wanted")
            self.assertTrue(os.path.exists(action_path))

            with patch("lib.config.read_runtime_config", return_value=cfg):
                recovered = importer.recover_abandoned_running_jobs(db)

            self.assertEqual(recovered, [])
            self.assertFalse(os.path.exists(action_path))
            stored = db.get_import_job(job.id)
            assert stored is not None and stored.result is not None
            cleanup = stored.result["force_action_cleanup"]
            assert isinstance(cleanup, dict)
            self.assertEqual(cleanup["action_path"], action_path)
            self.assertTrue(cleanup["removed"])

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
        source_download_log_id = db.log_download(
            request_id,
            outcome="rejected",
            error_message="launch marker source",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:postgres-relocated",
            payload={
                "download_log_id": source_download_log_id,
                "failed_path": source_path,
            },
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
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
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
        source_download_log_id = db.log_download(
            request_id,
            outcome="rejected",
            error_message="terminal rollback source",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:postgres-fence",
            payload={
                "download_log_id": source_download_log_id,
                "failed_path": source_path,
            },
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
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
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
        self.assertEqual(recovered[0].status, "failed")
        self.assertEqual(
            recovered[0].beets_launch_snapshot_fingerprint,
            evidence.snapshot_fingerprint,
        )
        self.assertIsNone(claim_next_import_job(observer, worker_id="replay"))
        self.assertIsNotNone(recovered[0].completed_at)
        history = observer.get_download_history(request_id)
        self.assertEqual(history[0]["outcome"], "failed")
        self.assertEqual(
            history[0]["source_download_log_id"],
            source_download_log_id,
        )

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
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
        assert claimed is not None

        recovered = db.recover_running_import_jobs(
            requeue_message="safe retry",
            recovery_message="operator recovery required",
        )

        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].status, "queued")
        retry = claim_next_import_job(db, worker_id="retry")
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
        source_download_log_id = db.log_download(
            request_id,
            outcome="rejected",
            error_message="terminal rollback source",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:postgres-terminal-rollback",
            payload={
                "download_log_id": source_download_log_id,
                "failed_path": source_path,
            },
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
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
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
        self.assertEqual(recovered[0].status, "failed")
        self.assertIsNotNone(recovered[0].completed_at)
        self.assertIsNone(claim_next_import_job(observer, worker_id="replay"))

    def test_non_automation_failure_terminal_is_atomic_and_fake_real_parity(
        self,
    ) -> None:
        """The new audit+job command is all-or-nothing in both DB adapters."""
        from lib.terminal_outcomes import non_automation_failure_terminal_outcome
        from tests.test_terminal_outcomes import (
            FaultInjectingPipelineDB,
            InjectedTerminalWriteFailure,
        )
        from web.download_history_view import build_recents_download_log_rows

        assert TEST_DSN is not None
        db = make_db()
        self.addCleanup(db.close)
        request_id = db.add_request(
            artist_name="Fence",
            album_title="Visible terminal rollback",
            source="request",
            mb_release_id="release-visible-terminal",
            status="wanted",
        )
        source_download_log_id = db.log_download(
            request_id,
            outcome="rejected",
            error_message="force source for terminal rollback",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:visible-terminal-rollback",
            payload={
                "download_log_id": source_download_log_id,
                "failed_path": "/tmp/visible-terminal-rollback",
            },
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
        assert claimed is not None
        command = non_automation_failure_terminal_outcome(
            claimed,
            error="RuntimeError",
            message="force executor crashed after launch authority",
            result={"success": False, "kind": "crash"},
        )

        failing = FaultInjectingPipelineDB(TEST_DSN, fail_after_write=1)
        try:
            with self.assertRaises(InjectedTerminalWriteFailure):
                failing.persist_import_terminal_outcome(command)
        finally:
            failing.close()

        observer = PipelineDB(TEST_DSN)
        self.addCleanup(observer.close)
        still_running = observer.get_import_job(claimed.id)
        assert still_running is not None
        self.assertEqual(still_running.status, "running")
        self.assertEqual(len(observer.get_download_history(request_id)), 1)
        request = observer.get_request(request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")

        terminal = observer.persist_import_terminal_outcome(command)
        self.assertEqual(terminal.job.status, "failed")
        audit = observer.get_download_log_entry(terminal.download_log_id)
        assert audit is not None
        self.assertEqual(audit["source_download_log_id"], source_download_log_id)
        self.assertEqual(audit["source"], "slskd")
        self.assertEqual(
            build_recents_download_log_rows([dict(audit)])[0]["verdict"],
            "Force import attempt failed: force executor crashed after launch "
            "authority",
        )

        fake = FakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            mb_release_id="fake-visible-terminal",
            status="wanted",
        ))
        fake_source = fake.log_download(42, outcome="rejected")
        fake_job = fake.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:fake-visible-terminal",
            payload={
                "download_log_id": fake_source,
                "failed_path": "/tmp/fake-visible-terminal",
            },
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = claim_next_import_job(fake, worker_id="fake-worker")
        assert fake_claimed is not None
        fake_terminal = fake.persist_import_terminal_outcome(
            non_automation_failure_terminal_outcome(
                fake_claimed,
                error="RuntimeError",
                message="force executor crashed after launch authority",
                result={"success": False, "kind": "crash"},
            )
        )
        fake_audit = fake.get_download_log_entry(fake_terminal.download_log_id)
        assert fake_audit is not None
        self.assertEqual(fake_terminal.job.status, terminal.job.status)
        self.assertEqual(
            fake_audit["source_download_log_id"],
            fake_source,
        )
        self.assertEqual(fake_audit["source"], "slskd")
        self.assertEqual(fake.request(42)["status"], "wanted")

    def test_youtube_failure_terminal_inherits_source_and_rolls_back_atomically(
        self,
    ) -> None:
        """A linked YT failure stays YT; its audit cannot half-commit."""
        from lib.terminal_outcomes import non_automation_failure_terminal_outcome
        from tests.test_terminal_outcomes import (
            FaultInjectingPipelineDB,
            InjectedTerminalWriteFailure,
        )

        assert TEST_DSN is not None
        db = make_db()
        self.addCleanup(db.close)
        request_id = db.add_request(
            artist_name="Fence",
            album_title="YouTube terminal provenance",
            source="request",
            mb_release_id="release-youtube-terminal-provenance",
            status="wanted",
        )
        source_download_log_id = db.insert_youtube_running(
            request_id=request_id,
            browse_id="MPREb_terminal_provenance",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/watch?v=terminal-provenance",
            expected_track_count=1,
        )
        job = db.enqueue_youtube_import_and_mark_success(
            download_log_id=source_download_log_id,
            request_id=request_id,
            dedupe_key="youtube:terminal-provenance",
            payload=youtube_import_payload(
                staged_path="/tmp/youtube-terminal-provenance",
                request_id=request_id,
                browse_id="MPREb_terminal_provenance",
                download_log_id=source_download_log_id,
            ),
            message="youtube rescue staged",
            terminal_metadata={},
        )
        origin = db.get_download_log_entry(source_download_log_id)
        assert origin is not None
        self.assertEqual(origin["outcome"], "youtube_success")
        db.mark_import_job_preview_importable(job.id, preview_result={})
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
        assert claimed is not None
        command = non_automation_failure_terminal_outcome(
            claimed,
            error="RuntimeError: youtube executor crashed",
            message="Executor crashed: RuntimeError: youtube executor crashed",
            result={"success": False, "kind": "crash"},
        )

        failing = FaultInjectingPipelineDB(TEST_DSN, fail_after_write=1)
        try:
            with self.assertRaises(InjectedTerminalWriteFailure):
                failing.persist_import_terminal_outcome(command)
        finally:
            failing.close()

        observer = PipelineDB(TEST_DSN)
        self.addCleanup(observer.close)
        self.assertEqual(len(observer.get_download_history(request_id)), 1)
        still_running = observer.get_import_job(claimed.id)
        assert still_running is not None
        self.assertEqual(still_running.status, "running")

        terminal = observer.persist_import_terminal_outcome(command)
        audit = observer.get_download_log_entry(terminal.download_log_id)
        assert audit is not None
        self.assertEqual(audit["source_download_log_id"], source_download_log_id)
        self.assertEqual(audit["source"], "youtube")
        self.assertEqual(terminal.job.status, "failed")

        retry_source = observer.insert_youtube_running(
            request_id=request_id,
            browse_id="MPREb_terminal_provenance_retry",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/watch?v=terminal-provenance-retry",
            expected_track_count=1,
        )
        retry = observer.enqueue_youtube_import_and_mark_success(
            download_log_id=retry_source,
            request_id=request_id,
            dedupe_key="youtube:terminal-provenance-retry",
            payload=youtube_import_payload(
                staged_path="/tmp/youtube-terminal-provenance-retry",
                request_id=request_id,
                browse_id="MPREb_terminal_provenance_retry",
                download_log_id=retry_source,
            ),
            message="operator requested YouTube retry",
            terminal_metadata={},
        )
        self.assertFalse(retry.deduped)
        self.assertNotEqual(retry.id, job.id)
        retry_origin = observer.get_download_log_entry(retry_source)
        assert retry_origin is not None
        self.assertEqual(retry_origin["outcome"], "youtube_success")

        fake = FakePipelineDB()
        fake.seed_request(make_request_row(
            id=42,
            mb_release_id="fake-youtube-terminal",
            status="wanted",
        ))
        fake_source = fake.insert_youtube_running(
            request_id=42,
            browse_id="MPREb_fake_terminal",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/watch?v=fake-terminal",
            expected_track_count=1,
        )
        fake_job = fake.enqueue_youtube_import_and_mark_success(
            download_log_id=fake_source,
            request_id=42,
            dedupe_key="youtube:fake-terminal-provenance",
            payload=youtube_import_payload(
                staged_path="/tmp/fake-youtube-terminal",
                request_id=42,
                browse_id="MPREb_fake_terminal",
                download_log_id=fake_source,
            ),
            message="youtube rescue staged",
            terminal_metadata={},
        )
        fake_origin = fake.get_download_log_entry(fake_source)
        assert fake_origin is not None
        self.assertEqual(fake_origin["outcome"], "youtube_success")
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = claim_next_import_job(fake, worker_id="fake-worker")
        assert fake_claimed is not None
        fake_terminal = fake.persist_import_terminal_outcome(
            non_automation_failure_terminal_outcome(
                fake_claimed,
                error="RuntimeError: youtube executor crashed",
                message="Executor crashed: RuntimeError: youtube executor crashed",
                result={"success": False, "kind": "crash"},
            )
        )
        fake_audit = fake.get_download_log_entry(fake_terminal.download_log_id)
        assert fake_audit is not None
        self.assertEqual(fake_audit["source_download_log_id"], fake_source)
        self.assertEqual(fake_audit["source"], "youtube")

    def test_non_automation_terminal_unlinks_invalid_source_origin(self) -> None:
        """Broken origin provenance is visible but cannot park the job."""
        from lib.failure_presentation import MAX_DIAGNOSTIC_CHARS
        from lib.terminal_outcomes import non_automation_failure_terminal_outcome

        db = make_db()
        self.addCleanup(db.close)
        request_id = db.add_request(
            artist_name="Fence",
            album_title="Terminal origin owner",
            source="request",
            mb_release_id="release-terminal-origin-owner",
            status="wanted",
        )
        other_request_id = db.add_request(
            artist_name="Fence",
            album_title="Other terminal origin",
            source="request",
            mb_release_id="release-other-terminal-origin",
            status="wanted",
        )
        foreign_source = db.log_download(other_request_id, outcome="rejected")
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key="force:cross-request-terminal-origin",
            payload={
                "download_log_id": foreign_source,
                "failed_path": "/tmp/cross-request-terminal-origin",
            },
        )
        db.mark_import_job_preview_importable(job.id, preview_result={})
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
        assert claimed is not None
        command = non_automation_failure_terminal_outcome(
            claimed,
            error="RuntimeError: source ownership failed",
            message="Executor crashed: " + "source ownership failed " * 30,
            result={"success": False},
        )
        terminal = db.persist_import_terminal_outcome(command)
        self.assertEqual(terminal.job.status, "failed")
        audit = db.get_download_log_entry(terminal.download_log_id)
        assert audit is not None
        self.assertIsNone(audit["source_download_log_id"])
        self.assertEqual(audit["source"], "slskd")
        self.assertIn(
            "Source provenance link was unavailable or refused",
            audit["error_message"] or "",
        )
        self.assertLessEqual(len(audit["error_message"] or ""), MAX_DIAGNOSTIC_CHARS)
        current = db.get_import_job(job.id)
        assert current is not None
        self.assertEqual(current.status, "failed")
        self.assertEqual(
            db.recover_running_import_jobs(
                requeue_message="retry",
                recovery_message="startup recovery",
            ),
            [],
        )

        fake = FakePipelineDB()
        fake.seed_request(make_request_row(id=42, status="wanted"))
        fake.seed_request(make_request_row(id=43, status="wanted"))
        fake_foreign_source = fake.log_download(43, outcome="rejected")
        fake_job = fake.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:fake-cross-request-terminal-origin",
            payload={
                "download_log_id": fake_foreign_source,
                "failed_path": "/tmp/fake-cross-request-terminal-origin",
            },
        )
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = claim_next_import_job(fake, worker_id="fake-worker")
        assert fake_claimed is not None
        fake_terminal = fake.persist_import_terminal_outcome(
            non_automation_failure_terminal_outcome(
                fake_claimed,
                error="RuntimeError: source ownership failed",
                message="Executor crashed: " + "source ownership failed " * 30,
                result={"success": False},
            )
        )
        fake_audit = fake.get_download_log_entry(fake_terminal.download_log_id)
        assert fake_audit is not None
        self.assertIsNone(fake_audit["source_download_log_id"])
        self.assertEqual(fake_audit["source"], "slskd")
        self.assertIn(
            "Source provenance link was unavailable or refused",
            fake_audit["error_message"] or "",
        )
        self.assertLessEqual(
            len(fake_audit["error_message"] or ""),
            MAX_DIAGNOSTIC_CHARS,
        )
        fake_current = fake.get_import_job(fake_job.id)
        assert fake_current is not None
        self.assertEqual(fake_current.status, "failed")
        self.assertEqual(
            fake.recover_running_import_jobs(
                requeue_message="retry",
                recovery_message="startup recovery",
            ),
            [],
        )

    def test_youtube_terminal_unlinks_missing_source_with_job_type_source(self) -> None:
        """A corrupt YT payload cannot revive the running-row uniqueness guard."""
        from lib.terminal_outcomes import non_automation_failure_terminal_outcome

        def enqueue_corrupt_youtube(db: PipelineDB | FakePipelineDB, request_id: int):
            source = db.insert_youtube_running(
                request_id=request_id,
                browse_id="MPREb_missing_origin",
                audio_playlist_id=None,
                yt_url="https://music.youtube.com/watch?v=missing-origin",
                expected_track_count=1,
            )
            payload = youtube_import_payload(
                staged_path="/tmp/missing-youtube-origin",
                request_id=request_id,
                browse_id="MPREb_missing_origin",
                download_log_id=source + 99_999,
            )
            return db.enqueue_youtube_import_and_mark_success(
                download_log_id=source,
                request_id=request_id,
                dedupe_key=f"youtube:missing-origin:{request_id}",
                payload=payload,
                message="youtube rescue staged",
                terminal_metadata={},
            )

        db = make_db()
        self.addCleanup(db.close)
        request_id = db.add_request(
            artist_name="Fence",
            album_title="Missing YouTube terminal origin",
            source="request",
            mb_release_id="release-missing-youtube-terminal-origin",
            status="wanted",
        )
        job = enqueue_corrupt_youtube(db, request_id)
        db.mark_import_job_preview_importable(job.id, preview_result={})
        claimed = claim_next_import_job(db, worker_id="postgres-worker")
        assert claimed is not None
        terminal = db.persist_import_terminal_outcome(
            non_automation_failure_terminal_outcome(
                claimed,
                error="RuntimeError: missing source audit",
                message="Executor crashed: RuntimeError: missing source audit",
                result={"success": False},
            )
        )
        audit = db.get_download_log_entry(terminal.download_log_id)
        assert audit is not None
        self.assertEqual(terminal.job.status, "failed")
        self.assertEqual(audit["source"], "youtube")
        self.assertIsNone(audit["source_download_log_id"])
        self.assertIn(
            "Source provenance link was unavailable or refused",
            audit["error_message"] or "",
        )
        self.assertEqual(
            db.recover_running_import_jobs(
                requeue_message="retry",
                recovery_message="startup recovery",
            ),
            [],
        )

        fake = FakePipelineDB()
        fake.seed_request(make_request_row(id=42, status="wanted"))
        fake_job = enqueue_corrupt_youtube(fake, 42)
        fake.mark_import_job_preview_importable(fake_job.id, preview_result={})
        fake_claimed = claim_next_import_job(fake, worker_id="fake-worker")
        assert fake_claimed is not None
        fake_terminal = fake.persist_import_terminal_outcome(
            non_automation_failure_terminal_outcome(
                fake_claimed,
                error="RuntimeError: missing source audit",
                message="Executor crashed: RuntimeError: missing source audit",
                result={"success": False},
            )
        )
        fake_audit = fake.get_download_log_entry(fake_terminal.download_log_id)
        assert fake_audit is not None
        self.assertEqual(fake_terminal.job.status, "failed")
        self.assertEqual(fake_audit["source"], "youtube")
        self.assertIsNone(fake_audit["source_download_log_id"])
        self.assertIn(
            "Source provenance link was unavailable or refused",
            fake_audit["error_message"] or "",
        )
        self.assertEqual(
            fake.recover_running_import_jobs(
                requeue_message="retry",
                recovery_message="startup recovery",
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
