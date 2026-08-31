"""Generated exact-owner patrols for automation import DB commands."""

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
from tests.dispatch_helpers import (
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _lease(invocation_id: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-boot",
        invocation_id=invocation_id,
        systemd_unit="cratedigger-import-preview.service",
        worker=ProcessIdentity(pid=701, start_ticks=7001),
    )


def _owner_blind_mutant_accepts(
    *,
    stage_is_exact: bool,
    lease_is_exact: bool,
) -> bool:
    """Known-bad model that forgets the request's processing owner."""
    return stage_is_exact and lease_is_exact


class TestProcessingOwnerCommandsGenerated(unittest.TestCase):
    @given(
        owner_is_exact=st.booleans(),
        stage_is_exact=st.booleans(),
        lease_is_exact=st.booleans(),
    )
    @example(
        owner_is_exact=False,
        stage_is_exact=True,
        lease_is_exact=True,
    )
    def test_candidate_evidence_binding_needs_all_three_witnesses(
        self,
        *,
        owner_is_exact: bool,
        stage_is_exact: bool,
        lease_is_exact: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=71,
            mb_release_id="generated-owner",
            status="wanted",
        ))
        job = handoff_automation_owner(db, 71)
        exact_lease = _lease("generated-preview")
        claimed = claim_next_import_preview_job(db, worker_id="generated-preview",
        execution_lease=exact_lease,)
        assert claimed is not None
        if not owner_is_exact:
            db._requests[71]["active_automation_import_job_id"] = job.id + 1
        if not stage_is_exact:
            row = next(item for item in db._import_jobs if item["id"] == job.id)
            row["preview_status"] = "waiting"
        supplied_lease = (
            exact_lease
            if lease_is_exact
            else _lease("generated-stale-preview")
        )

        persisted = db.set_import_job_candidate_evidence(
            job.id,
            991,
            expected_execution_lease=supplied_lease,
        )

        self.assertEqual(
            persisted,
            owner_is_exact and stage_is_exact and lease_is_exact,
        )

    def test_known_bad_owner_blind_binding_is_detected(self) -> None:
        self.assertTrue(_owner_blind_mutant_accepts(
            stage_is_exact=True,
            lease_is_exact=True,
        ))
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=72,
            mb_release_id="known-bad-owner",
            status="wanted",
        ))
        job = handoff_automation_owner(db, 72)
        lease = _lease("known-bad-preview")
        assert claim_next_import_preview_job(db, execution_lease=lease,) is not None
        db._requests[72]["active_automation_import_job_id"] = job.id + 1

        self.assertFalse(db.set_import_job_candidate_evidence(
            job.id,
            992,
            expected_execution_lease=lease,
        ))


if __name__ == "__main__":
    unittest.main()
