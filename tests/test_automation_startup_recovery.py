"""Exact-owner startup recovery contracts for automation imports."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from typing import Literal

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessEvidence,
    ProcessIdentity,
)
from lib.import_queue import IMPORT_JOB_FORCE, ImportJob
from lib.pipeline_db import PipelineDB
from lib.pipeline_db.cleanup_journal import CleanupJournalIntent
from lib.pipeline_db.import_jobs import (
    AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX,
    AutomationRecoveryCAS,
)
from lib.pipeline_db.rows import AlbumRequestRow, DownloadLogWithRequestRow
from lib.processing_cleanup import (
    PROCESSING_CLEANUP_REMOVE_SOURCE,
    cleanup_manifest_builtins,
    execute_processing_cleanup,
    inspect_processing_cleanup_source,
)
from scripts import importer
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
    make_album_quality_evidence,
    make_request_row,
)

TEST_DSN = os.environ["TEST_DB_DSN"]


def _lease(lane: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="boot-before-restart",
        invocation_id=f"{lane}-before-restart",
        systemd_unit=f"cratedigger-{lane}.service",
        worker=ProcessIdentity(pid=731, start_ticks=7301),
    )


def _evidence(lease: ExecutionLeaseSnapshot) -> ExecutionLivenessEvidence:
    return ExecutionLivenessEvidence(
        lease=lease,
        current_host_boot_id="boot-after-restart",
        boot_error=None,
        worker=None,
        beets=None,
        invocation=None,
        cgroup=None,
    )


def _dead(lease: ExecutionLeaseSnapshot) -> ExecutionLivenessDecision:
    return ExecutionLivenessDecision(
        status="dead",
        reason="prior boot ended",
        evidence=_evidence(lease),
    )


def _unprovable(
    lease: ExecutionLeaseSnapshot,
    status: Literal["live", "unknown"],
) -> ExecutionLivenessDecision:
    return ExecutionLivenessDecision(
        status=status,
        reason="probe could not settle",
        evidence=_evidence(lease),
    )


def _seed_unrelated_importable_jobs(
    db: FakePipelineDB | PipelineDB,
    *,
    request_id: int,
    count: int,
) -> None:
    for index in range(count):
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=f"force:startup-clutter:{index}",
            payload={
                "download_log_id": index + 1,
                "failed_path": f"/tmp/startup-clutter-{index}",
            },
        )
        marked = db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
            message="unrelated importable job",
        )
        if marked is None:
            raise AssertionError("failed to seed unrelated importable job")


def _no_checkpoint() -> None:
    """No owner can change under a single-threaded test."""


def _crash_cleanup_after_first_file(
    db: FakePipelineDB | PipelineDB,
    *,
    request_id: int,
    job_id: int,
) -> None:
    """Leave a real, partially executed cleanup journal behind.

    Rule C: the resume pin's world comes from the real executor stopping
    part-way, never from a hand-written "partial" journal that no producer
    could ever write.
    """

    class _Crash(RuntimeError):
        pass

    seen: list[str] = []

    def after_boundary(label: str) -> None:
        seen.append(label)
        if len([one for one in seen if one.startswith("checkpointed:")]) == 1:
            raise _Crash("simulated crash mid-cleanup")

    journal = db.get_processing_cleanup_journal(
        request_id=request_id,
        job_id=job_id,
    )
    assert journal is not None
    try:
        execute_processing_cleanup(
            db,
            journal,
            owner_checkpoint=_no_checkpoint,
            after_boundary=after_boundary,
        )
    except _Crash:
        return
    raise AssertionError("cleanup did not stop part-way")


class _StartupRecoveryContract:
    """One contract, run against the fake and real PostgreSQL alike.

    CLAUDE.md invariant 11 is the whole subject: a proven-dead owner either
    becomes runnable again in its own lane, or its request goes back into the
    search pool with audit evidence. Nothing rests anywhere only an operator
    can reach.
    """

    db: FakePipelineDB | PipelineDB
    request_id: int
    mb_release_id: str

    def _case(self) -> unittest.TestCase:
        assert isinstance(self, unittest.TestCase)
        return self

    def _request_row(self) -> AlbumRequestRow:
        row = self.db.get_request(self.request_id)
        assert row is not None
        return row

    def _audit_rows(self) -> list[DownloadLogWithRequestRow]:
        return [
            row
            for row in self.db.get_log(limit=50)
            if row["request_id"] == self.request_id
        ]

    # --- world builders -------------------------------------------------

    def _album_dir(self, name: str, *files: str) -> str:
        root = tempfile.mkdtemp(prefix="startup-recovery-")
        self._case().addCleanup(shutil.rmtree, root, ignore_errors=True)
        path = os.path.join(root, "albums", name)
        os.makedirs(path)
        for filename in files or ("01 - Track.mp3",):
            with open(os.path.join(path, filename), "wb") as handle:
                handle.write(b"audio")
        return path

    def _owner(self, canonical_path: str | None = None) -> ImportJob:
        return handoff_automation_owner(
            self.db,
            self.request_id,
            canonical_path=(
                canonical_path or "/processing/albums/startup-owner"
            ),
        )

    def _preview_owner(
        self,
        canonical_path: str | None = None,
    ) -> tuple[ImportJob, ExecutionLeaseSnapshot]:
        owner = self._owner(canonical_path)
        lease = _lease("import-preview")
        claimed = claim_next_import_preview_job(
            self.db,
            worker_id="preview-before-restart",
            execution_lease=lease,
        )
        assert claimed is not None
        return owner, lease

    def _launched_owner(
        self,
        canonical_path: str,
    ) -> tuple[ImportJob, ExecutionLeaseSnapshot]:
        """Drive one owner all the way to an authorized Beets launch."""
        owner, preview_lease = self._preview_owner(canonical_path)
        evidence = make_album_quality_evidence(
            mb_release_id=self.mb_release_id,
            source_path=canonical_path,
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        assert self.db.set_import_job_candidate_evidence(
            owner.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        )
        assert self.db.mark_import_job_preview_importable(
            owner.id,
            expected_execution_lease=preview_lease,
        ) is not None
        import_lease = _lease("importer")
        assert claim_next_import_job(
            self.db,
            worker_id="importer-before-restart",
            execution_lease=import_lease,
        ) is not None
        assert self.db.authorize_import_job_launch(
            owner.id,
            request_id=self.request_id,
            release_id=self.mb_release_id,
            source_path=canonical_path,
            expected_execution_lease=import_lease,
        ) is not None
        return owner, import_lease

    def _journal(self, job_id: int, source_path: str) -> None:
        inspection = inspect_processing_cleanup_source(source_path)
        assert inspection.manifest_hash is not None
        self.db.create_processing_cleanup_journal(
            request_id=self.request_id,
            job_id=job_id,
            intent=CleanupJournalIntent(
                action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                source_path=source_path,
                source_manifest=cleanup_manifest_builtins(
                    inspection.manifest
                ),
                source_manifest_hash=inspection.manifest_hash,
            ),
        )

    def _recovery_cas(
        self,
        job: ImportJob,
        canonical_path: str,
    ) -> AutomationRecoveryCAS:
        journal = self.db.get_processing_cleanup_journal(
            request_id=self.request_id,
            job_id=job.id,
        )
        assert journal is not None
        return AutomationRecoveryCAS(
            request_id=self.request_id,
            job_id=job.id,
            job_status=job.status,
            preview_status=job.preview_status,
            canonical_path=canonical_path,
            beets_launch_authorized_at=job.beets_launch_authorized_at,
            beets_launch_release_id=job.beets_launch_release_id,
            beets_launch_source_path=job.beets_launch_source_path,
            beets_launch_request_status=job.beets_launch_request_status,
            beets_launch_snapshot_fingerprint=(
                job.beets_launch_snapshot_fingerprint
            ),
            execution_invocation_id=job.execution_invocation_id,
            execution_host_boot_id=job.execution_host_boot_id,
            execution_systemd_unit=job.execution_systemd_unit,
            execution_worker_pid=job.execution_worker_pid,
            execution_worker_start_ticks=job.execution_worker_start_ticks,
            execution_beets_pid=job.execution_beets_pid,
            execution_beets_start_ticks=job.execution_beets_start_ticks,
            cleanup_job_id=job.id,
            cleanup_request_id=self.request_id,
            cleanup_revision=journal["revision"],
            cleanup_progress=dict(journal["step_progress"]),
        )

    def _recover(
        self,
        job_id: int,
        lease: ExecutionLeaseSnapshot,
        decision: ExecutionLivenessDecision | None = None,
    ) -> ImportJob | None:
        return self.db.recover_automation_import_job(
            job_id,
            expected_execution_lease=lease,
            decision=decision or _dead(lease),
            requeue_message="safe to replay",
            recovery_message="importer restarted mid-import",
        )

    # --- assertions -----------------------------------------------------

    def _assert_returned_to_search_pool(self, job_id: int) -> None:
        case = self._case()
        row = self._request_row()
        case.assertEqual(row["status"], "wanted")
        case.assertIsNone(row["active_automation_import_job_id"])
        case.assertIsNone(row["active_download_state"])
        job = self.db.get_import_job(job_id)
        assert job is not None
        case.assertEqual(job.status, "failed")
        case.assertIsNone(self.db.get_processing_cleanup_journal(
            request_id=self.request_id,
            job_id=job_id,
        ))
        logs = self._audit_rows()
        case.assertTrue(logs, "recovery recorded no audit evidence")
        latest = logs[0]
        case.assertEqual(latest["outcome"], "failed")
        case.assertIn(
            AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX,
            str(latest["error_message"]),
        )
        # Retained counters give growing backoff instead of a hot loop.
        case.assertEqual(row["validation_attempts"], 1)

    # --- contract -------------------------------------------------------

    def test_exact_owner_query_is_not_hidden_by_mixed_timeline_limit(
        self,
    ) -> None:
        owner, _unused = self._preview_owner()
        _seed_unrelated_importable_jobs(
            self.db,
            request_id=self.request_id,
            count=60,
        )

        owners = self.db.list_automation_import_jobs_for_startup_recovery()

        assert [job.id for job in owners] == [owner.id]

    def test_proven_unstarted_owner_is_requeued_with_owner_intact(
        self,
    ) -> None:
        """Nothing reached Beets and nothing was journaled: just replay it."""
        case = self._case()
        owner, lease = self._preview_owner()

        recovered = self._recover(owner.id, lease)

        assert recovered is not None
        case.assertEqual(recovered.status, "queued")
        case.assertEqual(recovered.preview_status, "waiting")
        case.assertIsNone(recovered.execution_invocation_id)
        row = self._request_row()
        # The owner is runnable again, so it legitimately keeps the request.
        case.assertEqual(row["status"], "processing")
        case.assertEqual(row["active_automation_import_job_id"], owner.id)
        case.assertFalse(self._audit_rows())

    def test_incomplete_cleanup_journal_resumes_and_reopens_the_search(
        self,
    ) -> None:
        """A half-finished cleanup is finished, not parked for an operator.

        Renamed from ``..._requires_operator_recovery``: that expectation was
        the invariant-11 violation itself. The journal's own pre-checkpoints
        are resumed to completion, and only then is the owner released.
        """
        case = self._case()
        path = self._album_dir(
            "resumable",
            "01 - A.mp3",
            "02 - B.mp3",
            "03 - C.mp3",
        )
        owner, lease = self._preview_owner(path)
        self._journal(owner.id, path)
        _crash_cleanup_after_first_file(
            self.db,
            request_id=self.request_id,
            job_id=owner.id,
        )
        # Precondition: a genuinely PARTIAL cleanup, mid-manifest.
        case.assertEqual(
            sorted(os.listdir(path)),
            ["02 - B.mp3", "03 - C.mp3"],
        )

        recovered = self._recover(owner.id, lease)

        assert recovered is not None
        case.assertNotEqual(recovered.status, "recovery_required")
        case.assertFalse(
            os.path.exists(path),
            "recovery abandoned the journaled cleanup instead of resuming it",
        )
        self._assert_returned_to_search_pool(owner.id)

    def test_abandoned_launched_owner_reopens_the_search(self) -> None:
        """The canonical invariant-11 case: Beets may have already run."""
        case = self._case()
        path = self._album_dir("launched")
        owner, lease = self._launched_owner(path)

        recovered = self._recover(owner.id, lease)

        assert recovered is not None
        case.assertNotEqual(
            recovered.status,
            "recovery_required",
            "a launched owner was parked behind an operator command",
        )
        self._assert_returned_to_search_pool(owner.id)
        case.assertFalse(os.path.exists(path))

    def test_owner_stranded_mid_close_is_driven_to_completion(self) -> None:
        """A crash inside the one-frame close window must not become a park."""
        case = self._case()
        path = self._album_dir("stranded")
        owner, lease = self._launched_owner(path)
        staged = self.db.mark_import_job_recovery_required(
            owner.id,
            reason="crashed inside the close window",
            expected_execution_lease=lease,
        )
        assert staged is not None
        case.assertEqual(staged.status, "recovery_required")

        # The recovery query must still SEE it, or the park is permanent.
        selected = self.db.list_automation_import_jobs_for_startup_recovery()
        case.assertEqual([job.id for job in selected], [owner.id])

        recovered = self._recover(owner.id, lease)

        assert recovered is not None
        self._assert_returned_to_search_pool(owner.id)

    def test_operator_retry_replacement_converges_without_a_human(
        self,
    ) -> None:
        """The last park: a retry's leaseless replacement still self-heals.

        ``retry_automation_import_recovery`` mints the replacement owner at
        ``recovery_required`` when it retargets an unresolved cleanup journal.
        Rule C — the world here is produced by that real command, not staged by
        hand — and the re-probe must converge it, because ``never_claimed`` is a
        real death proof for an owner no execution ever took.
        """
        case = self._case()
        path = self._album_dir("retry-replacement")
        owner, lease = self._launched_owner(path)
        self._journal(owner.id, path)
        staged = self.db.mark_import_job_recovery_required(
            owner.id,
            reason="ambiguous launched operation",
            expected_execution_lease=lease,
        )
        assert staged is not None
        applied = self.db.retry_automation_import_recovery(
            self._recovery_cas(staged, path),
            expected_execution_lease=lease,
            liveness=_dead(lease),
            reason="operator authorized a fresh retry",
            evidence_revision="evidence-revision-1",
        )
        assert applied is not None
        replacement = applied.retry
        # Precondition: a leaseless owner resting where only a human reaches it.
        case.assertEqual(replacement.status, "recovery_required")
        case.assertIsNone(replacement.execution_invocation_id)

        recovered = importer.recover_abandoned_automation_owners(self.db)

        case.assertEqual([job.id for job in recovered], [replacement.id])
        self._assert_returned_to_search_pool(replacement.id)
        case.assertFalse(
            os.path.exists(path),
            "the retargeted cleanup journal was not resumed",
        )

    def test_unprovable_execution_is_never_recovered(self) -> None:
        """Must-still-work: liveness ambiguity may not steal a live import.

        This is the guard on the fix's fail-closed half. An unproven execution
        is not a broken world — it is an unfinished observation — so the row
        must be left exactly as it was for the next re-probe.
        """
        case = self._case()
        path = self._album_dir("still-running")
        owner, lease = self._launched_owner(path)

        statuses: tuple[Literal["live", "unknown"], ...] = ("live", "unknown")
        for status in statuses:
            with case.subTest(liveness=status):
                case.assertIsNone(self._recover(
                    owner.id,
                    lease,
                    _unprovable(lease, status),
                ))
                row = self._request_row()
                case.assertEqual(row["status"], "processing")
                case.assertEqual(
                    row["active_automation_import_job_id"],
                    owner.id,
                )
                job = self.db.get_import_job(owner.id)
                assert job is not None
                case.assertEqual(job.status, "running")
                case.assertTrue(
                    os.path.exists(path),
                    "an unproven execution had its canonical album deleted",
                )
                case.assertFalse(self._audit_rows())

    def test_dead_verdict_about_another_lease_is_never_recovered(self) -> None:
        """Must-still-work: the proof has to be about THIS exact lease."""
        case = self._case()
        path = self._album_dir("other-lease")
        owner, lease = self._launched_owner(path)
        other = ExecutionLeaseSnapshot(
            host_boot_id=lease.host_boot_id,
            invocation_id="a-different-invocation",
            systemd_unit=lease.systemd_unit,
            worker=lease.worker,
        )

        case.assertIsNone(self._recover(owner.id, lease, _dead(other)))

        row = self._request_row()
        case.assertEqual(row["status"], "processing")
        case.assertEqual(row["active_automation_import_job_id"], owner.id)


class TestAutomationStartupRecoveryFake(
    _StartupRecoveryContract,
    unittest.TestCase,
):
    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.request_id = 42
        self.mb_release_id = "startup-recovery-fake"
        self.db.seed_request(make_request_row(
            id=self.request_id,
            mb_release_id=self.mb_release_id,
            status="wanted",
        ))


class TestAutomationStartupRecoveryPostgres(
    _StartupRecoveryContract,
    unittest.TestCase,
):
    def setUp(self) -> None:
        self.db = PipelineDB(TEST_DSN)
        self.db._execute("TRUNCATE album_requests CASCADE")
        self.db.conn.commit()
        self.mb_release_id = "startup-recovery-real"
        self.request_id = self.db.add_request(
            "Startup Recovery",
            "Exact Owner",
            "request",
            mb_release_id=self.mb_release_id,
        )

    def tearDown(self) -> None:
        self.db.close()
