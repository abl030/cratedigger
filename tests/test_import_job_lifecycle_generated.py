"""Generated runnable-import-job lifecycle boundary for issue #663."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_STATUSES,
)
import tests._hypothesis_profiles  # noqa: F401
from tests.fakes import FakePipelineDB


def assert_only_evidence_ready_is_claimable(
    preview_status: str,
    claimed: bool,
) -> None:
    """The importer lane begins only after neutral persisted evidence."""
    expected = preview_status == IMPORT_JOB_PREVIEW_EVIDENCE_READY
    if claimed != expected:
        raise AssertionError(
            f"preview_status={preview_status!r} claimed={claimed}; "
            f"expected {expected}",
        )


def _claimed_for(preview_status: str) -> bool:
    db = FakePipelineDB()
    db.enqueue_import_job(
        IMPORT_JOB_FORCE,
        request_id=663,
        dedupe_key=f"issue-663:{preview_status}",
        payload={"download_log_id": 663, "failed_path": "/tmp/663"},
    )
    db._import_jobs[0]["preview_status"] = preview_status
    return db.claim_next_import_job(worker_id="generated") is not None


class TestImportJobRunnableLifecycleGenerated(unittest.TestCase):
    def test_checker_rejects_the_removed_would_import_compatibility(self) -> None:
        with self.assertRaisesRegex(AssertionError, "would_import"):
            assert_only_evidence_ready_is_claimable("would_import", True)

    @given(preview_status=st.sampled_from(sorted(IMPORT_JOB_PREVIEW_STATUSES)))
    @example(preview_status="would_import")
    @example(preview_status="evidence_ready")
    def test_only_evidence_ready_preview_status_is_claimable(
        self, preview_status: str,
    ) -> None:
        assert_only_evidence_ready_is_claimable(
            preview_status,
            _claimed_for(preview_status),
        )


if __name__ == "__main__":
    unittest.main()
