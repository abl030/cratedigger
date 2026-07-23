"""Deterministic pins for import-job timeline display classification."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

import msgspec

from lib.import_queue import ForceImportPayload, ImportJob
from web.classify import ImportJobDisplay, classify_import_job_display


def _job(
    *,
    status: str = "queued",
    preview_status: str | None = "waiting",
    message: str | None = None,
    error: str | None = None,
    preview_message: str | None = None,
    preview_error: str | None = None,
) -> ImportJob:
    now = datetime(2026, 7, 13, tzinfo=timezone.utc)
    return ImportJob(
        id=575,
        job_type="force_import",
        status=status,
        request_id=100,
        dedupe_key="force_import:download_log:575",
        payload=ForceImportPayload(failed_path="/tmp/album"),
        result=None,
        message=message,
        error=error,
        attempts=0,
        worker_id=None,
        created_at=now,
        updated_at=now,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
        preview_status=preview_status,
        preview_message=preview_message,
        preview_error=preview_error,
    )


class TestImportJobDisplay(unittest.TestCase):
    def test_evidence_ready_first_row_is_the_next_check(self) -> None:
        display = classify_import_job_display(
            _job(
                preview_status="evidence_ready",
                preview_message="Evidence ready for final check: import",
            ),
            queue_position=0,
        )
        self.assertEqual(display.badge, "Next check")
        self.assertEqual(display.badge_class, "badge-new")
        self.assertEqual(display.border_color, "#1a4a2a")
        self.assertEqual(display.summary, "Evidence ready for final check: import")

    def test_running_import_wins_over_stale_preview_state(self) -> None:
        display = classify_import_job_display(
            _job(
                status="running",
                preview_status="measurement_failed",
                preview_error="stale preview failure",
                message="Importer owns this job",
            ),
            queue_position=4,
        )
        self.assertEqual(display.badge, "Importing")
        self.assertEqual(display.badge_class, "badge-force")
        self.assertEqual(display.border_color, "#36c")
        self.assertEqual(display.summary, "Importer owns this job")

    def test_measurement_failure_is_one_server_classified_contract(self) -> None:
        display = classify_import_job_display(
            _job(
                preview_status="measurement_failed",
                preview_error="snapshot stale",
            ),
            queue_position=3,
        )
        self.assertEqual(display.badge, "Measurement failed")
        self.assertEqual(display.badge_class, "badge-failed")
        self.assertEqual(display.border_color, "#a33")
        self.assertEqual(display.summary, "snapshot stale")

    def test_display_is_a_strict_wire_type(self) -> None:
        self.assertTrue(issubclass(ImportJobDisplay, msgspec.Struct))
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert({
                "badge": 42,
                "badge_class": "badge-new",
                "border_color": "#1a4a2a",
                "summary": "ready",
            }, type=ImportJobDisplay)

    def test_terminal_job_is_rejected_from_the_active_timeline_classifier(self) -> None:
        with self.assertRaisesRegex(ValueError, "active import job"):
            classify_import_job_display(
                _job(status="failed", preview_status="evidence_ready"),
                queue_position=0,
            )


if __name__ == "__main__":
    unittest.main()
