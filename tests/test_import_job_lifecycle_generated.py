"""Generated runnable-import-job lifecycle boundary for issue #663."""

from __future__ import annotations

import unittest

from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_STATUSES,
)
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

    def test_only_evidence_ready_preview_status_is_claimable(self) -> None:
        # Exhaustive finite status vocabulary, including the removed
        # would_import compatibility and the evidence_ready success world.
        for preview_status in sorted(IMPORT_JOB_PREVIEW_STATUSES):
            with self.subTest(preview_status=preview_status):
                assert_only_evidence_ready_is_claimable(
                    preview_status,
                    _claimed_for(preview_status),
                )


if __name__ == "__main__":
    unittest.main()
