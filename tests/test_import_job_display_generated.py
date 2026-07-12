"""Generated import-job display lifecycle property for issue #575 PR5."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from hypothesis import example, given, strategies as st

from lib.import_queue import (
    IMPORT_JOB_PREVIEW_STATUSES,
    ImportJob,
)
import tests._hypothesis_profiles  # noqa: F401
from web.classify import ImportJobDisplay, classify_import_job_display


def _job(status: str, preview_status: str | None, text: str) -> ImportJob:
    now = datetime(2026, 7, 13, tzinfo=timezone.utc)
    return ImportJob(
        id=575,
        job_type="force_import",
        status=status,
        request_id=100,
        dedupe_key="force_import:download_log:575",
        payload={"failed_path": "/tmp/album"},
        result=None,
        message=text,
        error=None,
        attempts=0,
        worker_id=None,
        created_at=now,
        updated_at=now,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
        preview_status=preview_status,
        preview_message=f"preview {text}",
        preview_error=f"preview error {text}",
    )


def assert_import_job_display_contract(display: object) -> None:
    if not isinstance(display, ImportJobDisplay):
        raise AssertionError("classifier did not return ImportJobDisplay")
    if not display.badge or not display.badge_class or not display.border_color:
        raise AssertionError("classifier returned an incomplete display contract")


class TestGeneratedImportJobDisplay(unittest.TestCase):
    @given(
        status=st.sampled_from(["queued", "running"]),
        preview_status=st.one_of(
            st.none(), st.sampled_from(sorted(IMPORT_JOB_PREVIEW_STATUSES)),
        ),
        queue_position=st.integers(min_value=0, max_value=49),
        text=st.text(min_size=1, max_size=40),
    )
    @example(
        status="running",
        preview_status="measurement_failed",
        queue_position=49,
        text="importer owns this job",
    )
    def test_every_active_lifecycle_world_has_one_complete_display_contract(
        self,
        status: str,
        preview_status: str | None,
        queue_position: int,
        text: str,
    ) -> None:
        display = classify_import_job_display(
            _job(status, preview_status, text),
            queue_position=queue_position,
        )
        assert_import_job_display_contract(display)
        if status == "running":
            self.assertEqual(display.badge, "Importing")
            self.assertEqual(display.summary, text)

    def test_contract_checker_rejects_the_old_tuple_shape(self) -> None:
        with self.assertRaisesRegex(AssertionError, "ImportJobDisplay"):
            assert_import_job_display_contract(
                ("next check", "badge-new", "#1a4a2a"),
            )


if __name__ == "__main__":
    unittest.main()
