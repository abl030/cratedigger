#!/usr/bin/env python3
"""Generated all-or-none laws for terminal import/preview outcomes."""

from __future__ import annotations

import copy
import unittest
from dataclasses import dataclass

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    IMPORT_JOB_YOUTUBE,
    automation_import_payload,
    force_import_payload,
    manual_import_payload,
    youtube_import_payload,
)
from lib.terminal_outcomes import (
    TerminalOutcomeBoundary,
    TerminalOutcomeConflict,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_terminal_outcomes import _preview_failure, _rejection, _success


@dataclass(frozen=True)
class TerminalState:
    request: object
    job: object
    logs: tuple[object, ...]
    denylist: tuple[object, ...]


def snapshot_terminal_state(
    db: FakePipelineDB,
    *,
    request_id: int,
    job_id: int,
) -> TerminalState:
    return TerminalState(
        request=copy.deepcopy(db.get_request(request_id)),
        job=copy.deepcopy(db.get_import_job(job_id)),
        logs=tuple(copy.deepcopy(db.download_logs)),
        denylist=tuple(copy.deepcopy(db.denylist)),
    )


def assert_terminal_outcome_all_or_none(
    before: TerminalState,
    after: TerminalState,
    *,
    committed: bool,
) -> None:
    """A failed terminal bundle must be byte-equivalent to its prior state."""
    if not committed and after != before:
        raise AssertionError("failed terminal outcome left partial persisted state")
    if committed:
        if len(after.logs) != len(before.logs) + 1:
            raise AssertionError("committed terminal outcome lacks exactly one audit row")
        if after.job == before.job:
            raise AssertionError("committed terminal outcome did not finalize its job")


def _prepare(job_type: str, *, preview: bool) -> tuple[FakePipelineDB, int, int]:
    db = FakePipelineDB()
    request_id = 42
    status = "manual" if job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_MANUAL) else "downloading"
    db.seed_request(make_request_row(id=request_id, status=status))
    if job_type == IMPORT_JOB_FORCE:
        payload = force_import_payload(download_log_id=7, failed_path="/tmp/force")
    elif job_type == IMPORT_JOB_MANUAL:
        payload = manual_import_payload(failed_path="/tmp/manual")
    elif job_type == IMPORT_JOB_YOUTUBE:
        payload = youtube_import_payload(
            staged_path="/tmp/youtube",
            request_id=request_id,
            browse_id="MPREb_test",
            download_log_id=8,
        )
    else:
        payload = automation_import_payload()
    job = db.enqueue_import_job(job_type, request_id=request_id, payload=payload)
    if preview:
        claimed = db.claim_next_import_preview_job(worker_id="preview")
    else:
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "evidence_ready"},
        )
        claimed = db.claim_next_import_job(worker_id="importer")
    assert claimed is not None
    return db, request_id, claimed.id


JOB_TYPES = st.sampled_from((
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    IMPORT_JOB_YOUTUBE,
))
OUTCOME_KINDS = st.sampled_from(("success", "rejection", "preview_failure"))
FAIL_BOUNDARIES = st.one_of(
    st.none(),
    st.sampled_from(tuple(TerminalOutcomeBoundary)),
)


class TestGeneratedTerminalOutcomeAtomicity(unittest.TestCase):
    @example(
        job_type=IMPORT_JOB_AUTOMATION,
        outcome_kind="success",
        fail_boundary=TerminalOutcomeBoundary.audit,
        replace_first=False,
    )
    @example(
        job_type=IMPORT_JOB_FORCE,
        outcome_kind="rejection",
        fail_boundary=TerminalOutcomeBoundary.denylist,
        replace_first=False,
    )
    @example(
        job_type=IMPORT_JOB_YOUTUBE,
        outcome_kind="preview_failure",
        fail_boundary=TerminalOutcomeBoundary.job,
        replace_first=True,
    )
    @given(
        job_type=JOB_TYPES,
        outcome_kind=OUTCOME_KINDS,
        fail_boundary=FAIL_BOUNDARIES,
        replace_first=st.booleans(),
    )
    def test_terminal_bundles_are_all_or_none_across_job_types_and_races(
        self,
        job_type: str,
        outcome_kind: str,
        fail_boundary: TerminalOutcomeBoundary | None,
        replace_first: bool,
    ) -> None:
        preview = outcome_kind == "preview_failure"
        db, request_id, job_id = _prepare(job_type, preview=preview)
        if replace_first:
            db.request(request_id)["status"] = "replaced"
        db.set_terminal_outcome_fault_after(fail_boundary)
        before = snapshot_terminal_state(db, request_id=request_id, job_id=job_id)
        committed = False
        try:
            if outcome_kind == "success":
                db.persist_import_success(_success(request_id, job_id))
            elif outcome_kind == "rejection":
                db.persist_importer_rejection(_rejection(request_id, job_id))
            else:
                db.persist_preview_measurement_failure(
                    _preview_failure(request_id, job_id)
                )
            committed = True
        except (RuntimeError, TerminalOutcomeConflict):
            committed = False
        after = snapshot_terminal_state(db, request_id=request_id, job_id=job_id)

        assert_terminal_outcome_all_or_none(before, after, committed=committed)
        if replace_first:
            self.assertFalse(committed)


class TestTerminalOutcomeInvariantCheckersTrip(unittest.TestCase):
    def test_all_or_none_checker_rejects_planted_partial_audit(self) -> None:
        db, request_id, job_id = _prepare(IMPORT_JOB_AUTOMATION, preview=False)
        before = snapshot_terminal_state(db, request_id=request_id, job_id=job_id)
        db.log_download(request_id=request_id, outcome="failed")
        after = snapshot_terminal_state(db, request_id=request_id, job_id=job_id)

        with self.assertRaisesRegex(AssertionError, "partial persisted state"):
            assert_terminal_outcome_all_or_none(before, after, committed=False)

    def test_committed_checker_rejects_planted_missing_job_finalization(self) -> None:
        db, request_id, job_id = _prepare(IMPORT_JOB_AUTOMATION, preview=False)
        before = snapshot_terminal_state(db, request_id=request_id, job_id=job_id)
        db.log_download(request_id=request_id, outcome="failed")
        after = snapshot_terminal_state(db, request_id=request_id, job_id=job_id)

        with self.assertRaisesRegex(AssertionError, "did not finalize its job"):
            assert_terminal_outcome_all_or_none(before, after, committed=True)


if __name__ == "__main__":
    unittest.main()
