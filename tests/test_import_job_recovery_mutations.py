"""Owner-atomic automation recovery retry and close contracts (#898 U4)."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from typing import Protocol
from unittest.mock import patch

import msgspec

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    ExecutionLivenessProbe,
    OwnerSessionIdentity,
    OwnerSessionProbe,
    ProcessIdentity,
)
from lib.import_job_recovery_service import (
    AutomationRecoveryMutationDB,
    apply_import_job_recovery,
    get_automation_recovery_detail,
)
from lib.pipeline_db.cleanup_journal import CleanupJournalIntent
from lib.pipeline_db.terminal_outcomes import ImportJobTerminalConflict
from lib.processing_cleanup import (
    cleanup_manifest_hash,
)
from tests.helpers import handoff_automation_owner

TEST_DSN = os.environ["TEST_DB_DSN"]
_RELEASE_ID = "75dbf62e-7dd2-4ddc-b57b-9bad1758b6b0"


class _RawRecoveryDB(AutomationRecoveryMutationDB, Protocol):
    def _execute(
        self,
        sql: str,
        params: tuple[object, ...] = (),
    ) -> object: ...


def _replacement_lease(label: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="replacement-boot",
        invocation_id=f"replacement-{label}",
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(pid=909, start_ticks=9009),
    )


class _ChangedBootProbe:
    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-after-restart",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )


class _MutatingChangedBootProbe(_ChangedBootProbe):
    def __init__(self, db: _RawRecoveryDB, job_id: int) -> None:
        self.db = db
        self.job_id = job_id

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        self.db._execute(
            """
            UPDATE import_jobs
            SET execution_invocation_id = 'fresh-claim'
            WHERE id = %s
            """,
            (self.job_id,),
        )
        return super().observe(lease)


class _UnknownProbe:
    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        del lease
        raise OSError("liveness probe unavailable")


class TestAutomationRecoveryMutationsFakeParity(unittest.TestCase):
    def _fake_owner(self, *, canonical_path: str):
        from tests.fakes import FakePipelineDB
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        job = handoff_automation_owner(
            db,
            42,
            canonical_path=canonical_path,
        )
        return db, job

    def test_fake_retry_matches_real_owner_and_journal_swap(self) -> None:
        db, job = self._fake_owner(canonical_path="/processing/fake-retry")
        row = next(item for item in db._import_jobs if item["id"] == job.id)
        row.update({
            "status": "recovery_required",
            "execution_invocation_id": "old-claim",
            "execution_host_boot_id": "boot-before-restart",
            "execution_systemd_unit": "cratedigger-importer.service",
            "execution_worker_pid": 101,
            "execution_worker_start_ticks": 1001,
        })
        journal = db.create_processing_cleanup_journal(
            request_id=42,
            job_id=job.id,
            intent=CleanupJournalIntent(
                action="no_op",
                source_path="/processing/fake-retry",
                source_manifest=(),
                source_manifest_hash=cleanup_manifest_hash(()),
            ),
        )
        probe = _ChangedBootProbe()
        detail = get_automation_recovery_detail(
            db,
            None,
            job.id,
            liveness_probe=probe,
        )
        assert detail.detail is not None

        result = apply_import_job_recovery(
            db,
            None,
            job.id,
            action="retry",
            reason="fake parity",
            evidence_revision=detail.detail.evidence_revision,
            liveness_probe=probe,
        )

        self.assertEqual(result.outcome, "retry_recovery_required")
        assert result.retry_job is not None
        self.assertEqual(result.retry_job.status, "recovery_required")
        self.assertIsNone(db.claim_next_import_preview_job(
            worker_id="must-not-replay-preview",
            execution_lease=_replacement_lease("fake-preview"),
        ))
        self.assertIsNone(db.claim_next_import_job(
            worker_id="must-not-replay-import",
            execution_lease=_replacement_lease("fake-import"),
        ))
        replacement_detail = get_automation_recovery_detail(
            db,
            None,
            result.retry_job.id,
        )
        assert replacement_detail.detail is not None
        self.assertEqual(
            replacement_detail.detail.execution_liveness.status,
            "dead",
        )
        self.assertEqual(
            replacement_detail.detail.execution_liveness.reason,
            "never_claimed",
        )
        self.assertEqual(db.request(42)["status"], "processing")
        self.assertEqual(
            db.request(42)["active_automation_import_job_id"],
            result.retry_job.id,
        )
        moved = db.get_processing_cleanup_journal(
            request_id=42,
            job_id=result.retry_job.id,
        )
        assert moved is not None
        self.assertEqual(moved["source_path"], journal["source_path"])
        self.assertEqual(moved["step_progress"], journal["step_progress"])
        self.assertEqual(moved["revision"], journal["revision"] + 1)

    def test_fake_close_matches_real_explicit_imported_no_op(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "already-gone")
            db, job = self._fake_owner(canonical_path=canonical)
            detail = get_automation_recovery_detail(
                db,
                None,
                job.id,
            )
            assert detail.detail is not None
            result = apply_import_job_recovery(
                db,
                None,
                job.id,
                action="close",
                reason="fake explicit imported",
                evidence_revision=detail.detail.evidence_revision,
                result_status="imported",
            )
            self.assertEqual(result.outcome, "closed")
            self.assertEqual(db.request(42)["status"], "imported")
            closed = db.get_import_job(job.id)
            assert closed is not None
            self.assertEqual(closed.status, "failed")
            assert closed.result is not None
            cleanup = msgspec.convert(
                closed.result["processing_cleanup"],
                type=dict[str, object],
            )
            self.assertEqual(cleanup["outcome"], "no_op")

    def test_unknown_execution_blocks_action_without_mutation(self) -> None:
        db, job = self._fake_owner(canonical_path="/processing/unknown")
        row = next(item for item in db._import_jobs if item["id"] == job.id)
        row.update({
            "status": "recovery_required",
            "execution_invocation_id": "old-claim",
            "execution_host_boot_id": "boot-before-restart",
            "execution_systemd_unit": "cratedigger-importer.service",
            "execution_worker_pid": 101,
            "execution_worker_start_ticks": 1001,
        })
        probe = _UnknownProbe()
        detail = get_automation_recovery_detail(
            db,
            None,
            job.id,
            liveness_probe=probe,
        )
        assert detail.detail is not None
        result = apply_import_job_recovery(
            db,
            None,
            job.id,
            action="retry",
            reason="must fail closed",
            evidence_revision=detail.detail.evidence_revision,
            liveness_probe=probe,
        )
        self.assertEqual(result.outcome, "execution_unknown")
        current = db.get_import_job(job.id)
        assert current is not None
        self.assertEqual(current.status, "recovery_required")
        self.assertEqual(
            db.request(42)["active_automation_import_job_id"],
            job.id,
        )


class TestAutomationRecoveryMutationsPostgres(unittest.TestCase):
    def setUp(self) -> None:
        from lib.pipeline_db import PipelineDB

        self.db = PipelineDB(TEST_DSN)
        self.db._execute("TRUNCATE album_requests CASCADE")
        self.db.conn.commit()

    def tearDown(self) -> None:
        self.db.close()

    def _owner(self, canonical_path: str):
        request_id = self.db.add_request(
            "Recovery Artist",
            "Recovery Album",
            "request",
            mb_release_id=_RELEASE_ID,
        )
        job = handoff_automation_owner(
            self.db,
            request_id,
            canonical_path=canonical_path,
        )
        return request_id, job

    def _make_recovery_required(self, job_id: int) -> None:
        self.db._execute(
            """
            UPDATE import_jobs
            SET status = 'recovery_required',
                execution_invocation_id = 'old-claim',
                execution_host_boot_id = 'boot-before-restart',
                execution_systemd_unit = 'cratedigger-importer.service',
                execution_worker_pid = 101,
                execution_worker_start_ticks = 1001
            WHERE id = %s
            """,
            (job_id,),
        )

    def _revision(
        self,
        job_id: int,
        *,
        probe: ExecutionLivenessProbe | None = None,
    ) -> str:
        result = get_automation_recovery_detail(
            self.db,
            None,
            job_id,
            liveness_probe=probe,
        )
        assert result.detail is not None
        return result.detail.evidence_revision

    def test_retry_swaps_owner_and_retargets_journal_atomically(self) -> None:
        request_id, job = self._owner("/processing/retry")
        self._make_recovery_required(job.id)
        journal = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job.id,
            intent=CleanupJournalIntent(
                action="no_op",
                source_path="/processing/retry",
                source_manifest=(),
                source_manifest_hash=cleanup_manifest_hash(()),
            ),
        )
        probe = _ChangedBootProbe()
        revision = self._revision(job.id, probe=probe)

        result = apply_import_job_recovery(
            self.db,
            None,
            job.id,
            action="retry",
            reason="operator confirmed Beets did not apply",
            evidence_revision=revision,
            liveness_probe=probe,
        )

        self.assertEqual(result.outcome, "retry_recovery_required")
        assert result.retry_job is not None
        old = self.db.get_import_job(job.id)
        request = self.db.get_request(request_id)
        moved = self.db.get_processing_cleanup_journal(
            request_id=request_id,
            job_id=result.retry_job.id,
        )
        assert old is not None and request is not None and moved is not None
        self.assertEqual(old.status, "failed")
        self.assertEqual(result.retry_job.status, "recovery_required")
        self.assertIsNone(self.db.claim_next_import_preview_job(
            worker_id="must-not-replay-preview",
            execution_lease=_replacement_lease("real-preview"),
        ))
        self.assertIsNone(self.db.claim_next_import_job(
            worker_id="must-not-replay-import",
            execution_lease=_replacement_lease("real-import"),
        ))
        replacement_detail = get_automation_recovery_detail(
            self.db,
            None,
            result.retry_job.id,
        )
        assert replacement_detail.detail is not None
        self.assertEqual(
            replacement_detail.detail.execution_liveness.status,
            "dead",
        )
        self.assertEqual(
            replacement_detail.detail.execution_liveness.reason,
            "never_claimed",
        )
        self.assertEqual(request["status"], "processing")
        self.assertEqual(
            request["active_automation_import_job_id"],
            result.retry_job.id,
        )
        self.assertIsNone(
            self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job.id,
            )
        )
        self.assertEqual(moved["action"], journal["action"])
        self.assertEqual(moved["source_path"], journal["source_path"])
        self.assertEqual(moved["step_progress"], journal["step_progress"])
        self.assertEqual(moved["revision"], journal["revision"] + 1)

    def test_retry_fault_and_fresh_claim_never_partially_swap_owner(self) -> None:
        request_id, job = self._owner("/processing/retry-race")
        self._make_recovery_required(job.id)
        probe = _ChangedBootProbe()
        revision = self._revision(job.id, probe=probe)

        def fail_after_insert(index: int, label: str) -> None:
            if index == 2:
                raise RuntimeError(label)

        self.db._automation_recovery_write_boundary = fail_after_insert
        with self.assertRaises(RuntimeError):
            apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="retry",
                reason="fault injection",
                evidence_revision=revision,
                liveness_probe=probe,
            )
        del self.db._automation_recovery_write_boundary
        request = self.db.get_request(request_id)
        original = self.db.get_import_job(job.id)
        assert request is not None and original is not None
        self.assertEqual(request["active_automation_import_job_id"], job.id)
        self.assertEqual(original.status, "recovery_required")
        active = [
            item
            for item in self.db.list_import_jobs(limit=20)
            if item.request_id == request_id
            and item.status in {"queued", "running", "recovery_required"}
        ]
        self.assertEqual([item.id for item in active], [job.id])

        changed = apply_import_job_recovery(
            self.db,
            None,
            job.id,
            action="retry",
            reason="claim raced observation",
            evidence_revision=revision,
            liveness_probe=_MutatingChangedBootProbe(self.db, job.id),
        )
        self.assertEqual(changed.outcome, "evidence_changed")
        request = self.db.get_request(request_id)
        original = self.db.get_import_job(job.id)
        assert request is not None and original is not None
        self.assertEqual(request["active_automation_import_job_id"], job.id)
        self.assertEqual(original.status, "recovery_required")

    def test_close_cleans_exact_tree_then_applies_explicit_wanted(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            with open(os.path.join(canonical, "track.flac"), "wb") as handle:
                handle.write(b"audio")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)

            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="operator reconciled ambiguous queued owner",
                evidence_revision=revision,
                result_status="wanted",
            )

            self.assertEqual(result.outcome, "closed")
            self.assertFalse(os.path.exists(canonical))
            request = self.db.get_request(request_id)
            closed = self.db.get_import_job(job.id)
            assert request is not None and closed is not None
            self.assertEqual(request["status"], "wanted")
            self.assertIsNone(request["active_automation_import_job_id"])
            self.assertIsNone(request["active_download_state"])
            self.assertEqual(closed.status, "failed")
            self.assertIsNone(
                self.db.get_processing_cleanup_journal(
                    request_id=request_id,
                    job_id=job.id,
                )
            )
            assert closed.result is not None
            resolution = msgspec.convert(
                closed.result["recovery_resolution"],
                type=dict[str, object],
            )
            self.assertEqual(resolution["result_status"], "wanted")
            self.assertEqual(resolution["reason"], (
                "operator reconciled ambiguous queued owner"
            ))

    def test_close_session_loss_leaves_owner_and_journal_for_resume(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            with open(os.path.join(canonical, "track.flac"), "wb") as handle:
                handle.write(b"audio")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)
            probes = 0

            def lose_session_before_mutation(
                identity: OwnerSessionIdentity,
            ) -> OwnerSessionProbe:
                nonlocal probes
                probes += 1
                return OwnerSessionProbe(
                    live=probes < 4,
                    reason=(
                        "exact_backend_alive"
                        if probes < 4
                        else "fault_injected_session_loss"
                    ),
                    expected=identity,
                    observed_backend_pid=(
                        identity.backend_pid if probes < 4 else None
                    ),
                )

            with (
                patch(
                    "lib.import_execution.threading.Thread",
                ),
                patch.object(
                    self.db,
                    "_probe_owner_session",
                    side_effect=lose_session_before_mutation,
                ),
            ):
                result = apply_import_job_recovery(
                    self.db,
                    None,
                    job.id,
                    action="close",
                    reason="session loss must fail stop",
                    evidence_revision=revision,
                    result_status="wanted",
                )

            self.assertEqual(result.outcome, "cleanup_failed")
            self.assertIn("fault_injected_session_loss", result.message)
            self.assertTrue(os.path.exists(canonical))
            request = self.db.get_request(request_id)
            current = self.db.get_import_job(job.id)
            journal = self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job.id,
            )
            assert request is not None and current is not None
            self.assertEqual(request["status"], "processing")
            self.assertEqual(
                request["active_automation_import_job_id"],
                job.id,
            )
            self.assertEqual(current.status, "queued")
            self.assertIsNotNone(journal)
            assert journal is not None
            self.assertIsNone(journal["completed_receipt"])

    def test_stale_revision_and_invalid_close_leave_owner_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            request_id, job = self._owner(canonical)

            with self.assertRaises(ValueError):
                apply_import_job_recovery(
                    self.db,
                    None,
                    job.id,
                    action="close",
                    reason="missing explicit result",
                    evidence_revision="sha256:stale",
                )
            stale = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="stale observation",
                evidence_revision="sha256:stale",
                result_status="imported",
            )
            self.assertEqual(stale.outcome, "evidence_changed")
            self.assertTrue(os.path.isdir(canonical))
            request = self.db.get_request(request_id)
            current = self.db.get_import_job(job.id)
            assert request is not None and current is not None
            self.assertEqual(request["status"], "processing")
            self.assertEqual(request["active_automation_import_job_id"], job.id)
            self.assertEqual(current.status, "queued")

    def test_close_proven_missing_source_applies_explicit_imported_no_op(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "already-gone")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)

            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="operator verified exact release in Beets",
                evidence_revision=revision,
                result_status="imported",
            )

            self.assertEqual(result.outcome, "closed")
            request = self.db.get_request(request_id)
            closed = self.db.get_import_job(job.id)
            assert request is not None and closed is not None
            self.assertEqual(request["status"], "imported")
            self.assertIsNone(request["active_automation_import_job_id"])
            self.assertEqual(closed.status, "failed")
            assert closed.result is not None
            cleanup = msgspec.convert(
                closed.result["processing_cleanup"],
                type=dict[str, object],
            )
            self.assertEqual(cleanup["outcome"], "no_op")
            resolution = msgspec.convert(
                closed.result["recovery_resolution"],
                type=dict[str, object],
            )
            self.assertEqual(resolution["result_status"], "imported")

    def test_close_resumes_completed_cleanup_after_terminal_conflict(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "album")
            os.mkdir(canonical)
            with open(os.path.join(canonical, "track.flac"), "wb") as handle:
                handle.write(b"audio")
            request_id, job = self._owner(canonical)
            revision = self._revision(job.id)

            with patch.object(
                self.db,
                "persist_import_terminal_outcome",
                side_effect=ImportJobTerminalConflict("fault after cleanup"),
            ):
                first = apply_import_job_recovery(
                    self.db,
                    None,
                    job.id,
                    action="close",
                    reason="resume exact cleanup declaration",
                    evidence_revision=revision,
                    result_status="wanted",
                )

            self.assertEqual(first.outcome, "evidence_changed")
            self.assertFalse(os.path.exists(canonical))
            assert first.detail is not None
            self.assertEqual(first.detail.cleanup_journal.status, "completed")
            request = self.db.get_request(request_id)
            assert request is not None
            self.assertEqual(request["status"], "processing")
            resumed = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="resume exact cleanup declaration",
                evidence_revision=first.detail.evidence_revision,
                result_status="wanted",
            )
            self.assertEqual(resumed.outcome, "closed")
            request = self.db.get_request(request_id)
            assert request is not None
            self.assertEqual(request["status"], "wanted")

    def test_close_blocks_uninspectable_source_without_journal_or_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            held_source = os.path.join(root, "held-source")
            os.mkdir(held_source)
            source = os.path.join(root, "uninspectable")
            os.symlink(held_source, source, target_is_directory=True)
            request_id, job = self._owner(source)
            revision = self._revision(job.id)
            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="must not infer absence",
                evidence_revision=revision,
                result_status="wanted",
            )
            self.assertEqual(result.outcome, "cleanup_uninspectable")
            request = self.db.get_request(request_id)
            current = self.db.get_import_job(job.id)
            assert request is not None and current is not None
            self.assertEqual(request["status"], "processing")
            self.assertEqual(current.status, "queued")
            self.assertIsNone(
                self.db.get_processing_cleanup_journal(
                    request_id=request_id,
                    job_id=job.id,
                )
            )

    def test_close_is_break_glass_for_proven_dead_wedged_running_owner(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            canonical = os.path.join(root, "already-gone")
            request_id, job = self._owner(canonical)
            self.db._execute(
                """
                UPDATE import_jobs
                SET status = 'running',
                    execution_invocation_id = 'old-claim',
                    execution_host_boot_id = 'boot-before-restart',
                    execution_systemd_unit = 'cratedigger-importer.service',
                    execution_worker_pid = 101,
                    execution_worker_start_ticks = 1001
                WHERE id = %s
                """,
                (job.id,),
            )
            probe = _ChangedBootProbe()
            revision = self._revision(job.id, probe=probe)
            result = apply_import_job_recovery(
                self.db,
                None,
                job.id,
                action="close",
                reason="wedged owner is proven dead",
                evidence_revision=revision,
                result_status="wanted",
                liveness_probe=probe,
            )
            self.assertEqual(result.outcome, "closed")
            request = self.db.get_request(request_id)
            closed = self.db.get_import_job(job.id)
            assert request is not None and closed is not None
            self.assertEqual(request["status"], "wanted")
            self.assertEqual(closed.status, "failed")


if __name__ == "__main__":
    unittest.main()
