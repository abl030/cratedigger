"""Generated outer-boundary requeue lifecycle properties for issue #1166."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import timedelta
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.config import CratediggerConfig
from lib.dispatch import (
    DISPATCH_CODE_REQUEUE_EXHAUSTED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
)
from lib.dispatch.evidence_gate import _requeue_import_job_to_preview
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    ProcessIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.quality import ActiveDownloadState
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
    make_request_row,
)

_EXPECTED_REQUEUE_DELAYS_SECONDS = {
    1: 60,
    2: 120,
    5: 960,
    6: 1800,
    2454: 1800,
}


class TestGeneratedImportPreviewRequeue(unittest.TestCase):
    @given(
        job_type=st.sampled_from((IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE)),
        age_seconds=st.sampled_from((0, 1, 3599, 3600, 3601)),
        attempts=st.sampled_from((1, 2, 5, 6, 2454)),
        preview_attempts=st.sampled_from((1, 2, 5, 6, 2463)),
        reason=st.sampled_from(("missing", "stale", "incomplete")),
    )
    def test_requeue_budget_composes_with_each_reachable_owner_kind(
        self,
        job_type: str,
        age_seconds: int,
        attempts: int,
        preview_attempts: int,
        reason: str,
    ) -> None:
        """Candidate gate, exact owner, and non-owner terminal paths agree."""
        if job_type == IMPORT_JOB_AUTOMATION:
            self._assert_automation_world(
                age_seconds, attempts, preview_attempts, reason,
            )
            return
        self._assert_force_world(
            age_seconds, attempts, preview_attempts, reason,
        )

    def _assert_automation_world(
        self,
        age_seconds: int,
        attempts: int,
        preview_attempts: int,
        reason: str,
    ) -> None:
        from scripts import importer

        with tempfile.TemporaryDirectory() as canonical:
            with open(os.path.join(canonical, "01.flac"), "wb") as handle:
                handle.write(b"generated canonical owner")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, status="wanted"))
            job = handoff_automation_owner(
                db,
                42,
                state=ActiveDownloadState(
                    filetype="flac",
                    enqueued_at="2026-08-17T00:00:00+00:00",
                    files=[],
                    current_path=canonical,
                ).to_json(),
                canonical_path=canonical,
            )
            preview_lease = self._lease("preview", job.id)
            assert claim_next_import_preview_job(
                db,
                worker_id="preview",
                execution_lease=preview_lease,
            ) is not None
            assert db.mark_import_job_preview_importable(
                job.id,
                preview_result={"verdict": "evidence_ready"},
                expected_execution_lease=preview_lease,
            ) is not None
            lease = self._lease("importer", job.id)
            claimed = claim_next_import_job(
                db,
                worker_id="importer",
                execution_lease=lease,
            )
            assert claimed is not None
            row = next(row for row in db._import_jobs if row["id"] == claimed.id)
            row["created_at"] -= timedelta(seconds=age_seconds)
            row["attempts"] = attempts
            row["preview_attempts"] = preview_attempts
            outcome = _requeue_import_job_to_preview(
                db,
                import_job_id=claimed.id,
                reason=reason,
                expected_execution_lease=lease,
            )

            if age_seconds < 3600:
                self.assertEqual(outcome.code, DISPATCH_CODE_REQUEUED_FOR_PREVIEW)
                self._assert_requeue_cadence(
                    db,
                    claimed.id,
                    attempts,
                    execution_lease=self._lease("retry-preview", claimed.id),
                )
                return

            self.assertEqual(outcome.code, DISPATCH_CODE_REQUEUE_EXHAUSTED)
            token = CancellationToken()
            with db._pin_owner_session(token) as owner_session_identity:
                updated = importer.process_claimed_job(
                    db,  # pyright: ignore[reportArgumentType]
                    claimed,
                    execute_fn=lambda *_args, **_kwargs: outcome,
                    execution_lease=lease,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                )
            assert updated is not None
            self.assertEqual(updated.status, "failed")
            request = db.request(42)
            self.assertEqual(request["status"], "wanted")
            self.assertIsNone(request["active_automation_import_job_id"])
            self.assertTrue(any(
                row.outcome == "failed" and outcome.message in (row.error_message or "")
                for row in db.download_logs
            ))

    def _assert_force_world(
        self,
        age_seconds: int,
        attempts: int,
        preview_attempts: int,
        reason: str,
    ) -> None:
        from lib.import_preview import force_action_copy_path
        from scripts import importer

        with tempfile.TemporaryDirectory() as root:
            downloads = os.path.join(root, "downloads")
            processing = os.path.join(root, "processing")
            os.mkdir(downloads, 0o700)
            os.mkdir(processing, 0o700)
            os.mkdir(os.path.join(processing, "albums"), 0o700)
            cfg = CratediggerConfig(
                slskd_download_dir=downloads,
                processing_dir=processing,
                audio_check_mode="off",
            )
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, status="wanted"))
            download_log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "scenario": "high_distance",
                    "failed_path": "/tmp/generated-wrong-match",
                },
            )
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path="/tmp/generated-wrong-match",
                ),
            )
            preview_claim = claim_next_import_preview_job(db, worker_id="preview")
            assert preview_claim is not None
            action_path = force_action_copy_path(cfg, preview_claim.id)
            os.mkdir(action_path, 0o700)
            assert db.mark_import_job_preview_importable(
                preview_claim.id,
                preview_result={"ready": True, "action_path": action_path},
            ) is not None
            claimed = claim_next_import_job(db, worker_id="importer")
            assert claimed is not None
            row = next(row for row in db._import_jobs if row["id"] == claimed.id)
            row["created_at"] -= timedelta(seconds=age_seconds)
            row["attempts"] = attempts
            row["preview_attempts"] = preview_attempts
            outcome = _requeue_import_job_to_preview(
                db,
                import_job_id=claimed.id,
                reason=reason,
            )

            if age_seconds < 3600:
                self.assertEqual(outcome.code, DISPATCH_CODE_REQUEUED_FOR_PREVIEW)
                self._assert_requeue_cadence(db, claimed.id, attempts)
                return

            self.assertEqual(outcome.code, DISPATCH_CODE_REQUEUE_EXHAUSTED)
            with patch("lib.config.read_runtime_config", return_value=cfg):
                updated = importer.process_claimed_job(
                    db,  # pyright: ignore[reportArgumentType]
                    claimed,
                    execute_fn=lambda *_args, **_kwargs: outcome,
                )
            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(db.request(42)["status"], "wanted")
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertFalse(os.path.exists(action_path))
            assert updated.result is not None
            self.assertNotIn("cleanup", updated.result)
            self.assertTrue(updated.result["force_action_cleanup"]["removed"])

    def _assert_requeue_cadence(
        self,
        db: FakePipelineDB,
        job_id: int,
        attempts: int,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> None:
        """Candidate selection and claim observe the one shared delay owner."""
        self.assertIsNone(claim_next_import_preview_job(
            db,
            worker_id="preview-immediate",
            execution_lease=execution_lease,
        ))
        row = next(row for row in db._import_jobs if row["id"] == job_id)
        delay = timedelta(seconds=_EXPECTED_REQUEUE_DELAYS_SECONDS[attempts])
        row["updated_at"] -= delay - timedelta(seconds=1)
        self.assertIsNone(claim_next_import_preview_job(
            db,
            worker_id="preview-too-early",
            execution_lease=execution_lease,
        ))
        row["updated_at"] -= timedelta(seconds=1)
        self.assertIsNotNone(claim_next_import_preview_job(
            db,
            worker_id="preview-on-time",
            execution_lease=execution_lease,
        ))

    @staticmethod
    def _lease(role: str, job_id: int) -> ExecutionLeaseSnapshot:
        return ExecutionLeaseSnapshot(
            host_boot_id="generated-boot",
            invocation_id=f"generated-{role}-{job_id}",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(pid=7000 + job_id, start_ticks=8000 + job_id),
        )


if __name__ == "__main__":
    unittest.main()
