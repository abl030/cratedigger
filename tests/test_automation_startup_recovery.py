"""Exact-owner startup recovery contracts for automation imports."""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessEvidence,
    ProcessIdentity,
)
from lib.import_queue import IMPORT_JOB_FORCE
from lib.pipeline_db.cleanup_journal import CleanupJournalIntent
from lib.processing_cleanup import cleanup_manifest_hash
from tests.fakes import FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row

TEST_DSN = os.environ["TEST_DB_DSN"]


def _lease(lane: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="boot-before-restart",
        invocation_id=f"{lane}-before-restart",
        systemd_unit=f"cratedigger-{lane}.service",
        worker=ProcessIdentity(pid=731, start_ticks=7301),
    )


def _dead(lease: ExecutionLeaseSnapshot) -> ExecutionLivenessDecision:
    return ExecutionLivenessDecision(
        status="dead",
        reason="prior boot ended",
        evidence=ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-after-restart",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        ),
    )


def _seed_unrelated_importable_jobs(
    db: Any,
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


class _StartupRecoveryContract:
    db: Any
    request_id: int

    def _owner(self):
        return handoff_automation_owner(
            self.db,
            self.request_id,
            canonical_path="/processing/albums/startup-owner",
        )

    def test_exact_owner_query_is_not_hidden_by_mixed_timeline_limit(
        self,
    ) -> None:
        owner = self._owner()
        lease = _lease("import-preview")
        claimed = self.db.claim_next_import_preview_job(
            worker_id="preview-before-restart",
            execution_lease=lease,
        )
        assert claimed is not None
        _seed_unrelated_importable_jobs(
            self.db,
            request_id=self.request_id,
            count=60,
        )

        owners = self.db.list_automation_import_jobs_for_startup_recovery()

        assert [job.id for job in owners] == [owner.id]

    def test_incomplete_cleanup_journal_requires_operator_recovery(
        self,
    ) -> None:
        owner = self._owner()
        lease = _lease("import-preview")
        claimed = self.db.claim_next_import_preview_job(
            worker_id="preview-before-restart",
            execution_lease=lease,
        )
        assert claimed is not None
        journal = self.db.create_processing_cleanup_journal(
            request_id=self.request_id,
            job_id=owner.id,
            intent=CleanupJournalIntent(
                action="remove",
                source_path="/processing/albums/startup-owner",
                source_manifest=(),
                source_manifest_hash=cleanup_manifest_hash(()),
            ),
        )

        recovered = self.db.recover_automation_import_job(
            owner.id,
            expected_execution_lease=lease,
            decision=_dead(lease),
            requeue_message="safe to replay",
            recovery_message="operator recovery required",
        )

        assert recovered is not None
        assert recovered.status == "recovery_required"
        assert recovered.preview_status == "running"
        assert recovered.execution_invocation_id == lease.invocation_id
        assert (
            self.db.get_processing_cleanup_journal(
                request_id=self.request_id,
                job_id=owner.id,
            )
            == journal
        )


class TestAutomationStartupRecoveryFake(
    _StartupRecoveryContract,
    unittest.TestCase,
):
    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.request_id = 42
        self.db.seed_request(make_request_row(
            id=self.request_id,
            mb_release_id="startup-recovery-fake",
            status="wanted",
        ))


class TestAutomationStartupRecoveryPostgres(
    _StartupRecoveryContract,
    unittest.TestCase,
):
    def setUp(self) -> None:
        from lib.pipeline_db import PipelineDB

        self.db = PipelineDB(TEST_DSN)
        self.db._execute("TRUNCATE album_requests CASCADE")
        self.db.conn.commit()
        self.request_id = self.db.add_request(
            "Startup Recovery",
            "Exact Owner",
            "request",
            mb_release_id="startup-recovery-real",
        )

    def tearDown(self) -> None:
        self.db.close()
