"""Generated startup-recovery coverage for mixed import-queue worlds."""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any, Literal, cast

from hypothesis import example, given
from hypothesis import strategies as st

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    ExecutionLivenessProbe,
    ProcessIdentity,
)
from lib.import_queue import IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE
from tests.fakes import FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row

Lane = Literal["preview", "import"]


def _lease(lane: Lane) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-old-boot",
        invocation_id=f"generated-{lane}-invocation",
        systemd_unit=f"cratedigger-{lane}.service",
        worker=ProcessIdentity(pid=811, start_ticks=8101),
    )


class _DeadProbe:
    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="generated-new-boot",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )


def _owner_with_dead_execution(
    lane: Lane,
) -> tuple[FakePipelineDB, int]:
    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42,
        mb_release_id="generated-startup-owner",
        status="wanted",
    ))
    owner = handoff_automation_owner(
        db,
        42,
        canonical_path="/processing/albums/generated-startup-owner",
    )
    preview_lease = _lease("preview")
    claimed = db.claim_next_import_preview_job(
        worker_id="generated-preview",
        execution_lease=preview_lease,
    )
    if claimed is None:
        raise AssertionError("generated preview owner was not claimable")
    if lane == "preview":
        return db, owner.id
    if db.mark_import_job_preview_importable(
        owner.id,
        preview_result={"ready": True},
        expected_execution_lease=preview_lease,
    ) is None:
        raise AssertionError("generated owner did not finish preview")
    claimed = db.claim_next_import_job(
        worker_id="generated-importer",
        execution_lease=_lease("import"),
    )
    if claimed is None:
        raise AssertionError("generated import owner was not claimable")
    return db, owner.id


def _add_higher_priority_clutter(
    db: FakePipelineDB,
    *,
    count: int,
) -> None:
    for index in range(count):
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=f"force:generated-startup-clutter:{index}",
            payload={
                "download_log_id": index + 1,
                "failed_path": f"/tmp/generated-clutter-{index}",
            },
        )
        if db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
        ) is None:
            raise AssertionError("generated clutter did not become importable")


class TestAutomationStartupRecoveryGenerated(unittest.TestCase):
    @given(
        lane=st.sampled_from(("preview", "import")),
        clutter_count=st.integers(min_value=50, max_value=80),
    )
    @example(lane="preview", clutter_count=50)
    @example(lane="import", clutter_count=50)
    def test_exact_owner_survives_every_mixed_timeline_cutoff(
        self,
        *,
        lane: Lane,
        clutter_count: int,
    ) -> None:
        from scripts import import_preview_worker, importer

        db, owner_id = _owner_with_dead_execution(lane)
        _add_higher_priority_clutter(db, count=clutter_count)

        # Known-bad implementation: limit a mixed timeline, then filter.
        mutant = [
            job
            for job in db.list_import_job_timeline(limit=50)
            if job.job_type == IMPORT_JOB_AUTOMATION
        ]
        self.assertEqual(mutant, [])

        if lane == "preview":
            recovered = import_preview_worker.recover_running_preview_jobs(
                cast(Any, db),
                liveness_probe=cast(ExecutionLivenessProbe, _DeadProbe()),
            )
        else:
            recovered = importer.recover_abandoned_running_jobs(
                cast(Any, db),
                liveness_probe=cast(ExecutionLivenessProbe, _DeadProbe()),
            )

        self.assertEqual([job.id for job in recovered], [owner_id])
