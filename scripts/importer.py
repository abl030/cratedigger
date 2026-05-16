#!/usr/bin/env python3
"""Drain the shared import queue through one beets-mutating lane."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import time
from typing import Any

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.import_dispatch import (
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DISPATCH_CODE_REQUEUE_FAILED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    DispatchOutcome,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    ImportJob,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORTER,
    DEFAULT_DSN,
    PipelineDB,
)
from lib.quality import ActiveDownloadState

logger = logging.getLogger("cratedigger-importer")
RESTART_REQUEUE_MESSAGE = "Importer restarted while job was running; retry queued"


def _job_result(outcome: DispatchOutcome) -> dict[str, Any]:
    return {
        "success": outcome.success,
        "message": outcome.message,
        "deferred": outcome.deferred,
        "code": outcome.code,
    }


def _force_job_wrong_match_payload(job: ImportJob) -> tuple[int, str | None] | None:
    if job.job_type != IMPORT_JOB_FORCE:
        return None
    payload = job.payload or {}
    download_log_id = payload.get("download_log_id")
    if not isinstance(download_log_id, int):
        return None
    failed_path = payload.get("failed_path")
    return download_log_id, failed_path if isinstance(failed_path, str) else None


def _cleanup_failed_force_import(
    db: PipelineDB,
    job: ImportJob,
    outcome: DispatchOutcome,
) -> dict[str, object] | None:
    if outcome.deferred:
        return None
    force_payload = _force_job_wrong_match_payload(job)
    if force_payload is None:
        return None
    download_log_id, failed_path_hint = force_payload
    if outcome.code != DISPATCH_CODE_QUALITY_PIPELINE_REJECTED:
        return {
            "success": False,
            "download_log_id": download_log_id,
            "failed_path_hint": failed_path_hint,
            "outcome": "skipped_non_quality_pipeline_failure",
            "skipped": True,
            "dispatch_code": outcome.code,
            "dispatch_message": outcome.message,
        }
    try:
        from lib.wrong_match_delete_service import delete_wrong_match

        # Dispatch already made the canonical import decision via
        # full_pipeline_decision_from_evidence; this step only removes the
        # reviewed source from Wrong Matches.
        payload = job.payload or {}
        source_dirs = payload.get("source_dirs")
        result = delete_wrong_match(
            db,
            download_log_id,
            failed_path_hint=failed_path_hint,
            source_dirs_hint=(
                source_dirs if isinstance(source_dirs, list) else ()
            ),
            ignore_import_job_id=job.id,
            require_visible=False,
        )
        data = result.to_dict()
        data["dispatch_code"] = outcome.code
        data["dispatch_message"] = outcome.message
        if result.success:
            data["reason"] = "quality_pipeline_rejected"
        return data
    except Exception as exc:
        logger.exception(
            "Failed to clean wrong-match source for import job %s",
            job.id,
        )
        return {
            "success": False,
            "download_log_id": download_log_id,
            "failed_path_hint": failed_path_hint,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _dismiss_successful_force_import(
    db: PipelineDB,
    job: ImportJob,
) -> dict[str, object] | None:
    force_payload = _force_job_wrong_match_payload(job)
    if force_payload is None:
        return None
    download_log_id, failed_path_hint = force_payload
    try:
        from lib.wrong_matches import dismiss_wrong_match_source

        return dismiss_wrong_match_source(
            db,
            download_log_id,
            failed_path_hint=failed_path_hint,
        ).to_dict()
    except Exception as exc:
        logger.exception(
            "Failed to dismiss wrong-match source for import job %s",
            job.id,
        )
        return {
            "success": False,
            "download_log_id": download_log_id,
            "failed_path_hint": failed_path_hint,
            "error": f"{type(exc).__name__}: {exc}",
        }


def execute_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
) -> DispatchOutcome:
    """Execute one claimed import job without mutating job status."""
    if job.request_id is None:
        return DispatchOutcome(
            success=False,
            message="Import job has no request_id",
        )

    if job.job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_MANUAL):
        from lib.import_dispatch import dispatch_import_from_db

        payload = job.payload
        failed_path = str(payload.get("failed_path") or "")
        if not failed_path:
            return DispatchOutcome(
                success=False,
                message="Import job payload is missing failed_path",
            )
        source_username = payload.get("source_username")
        source_dirs = payload.get("source_dirs")
        download_log_id = payload.get("download_log_id")
        return dispatch_import_from_db(
            db,
            request_id=job.request_id,
            failed_path=failed_path,
            force=job.job_type == IMPORT_JOB_FORCE,
            outcome_label=job.job_type,
            source_username=(
                str(source_username)
                if source_username is not None
                else None
            ),
            source_dirs=(
                [str(source_dir) for source_dir in source_dirs if source_dir]
                if isinstance(source_dirs, list)
                else None
            ),
            import_job_id=job.id,
            download_log_id=(
                int(download_log_id)
                if isinstance(download_log_id, int)
                else None
            ),
        )

    if job.job_type == IMPORT_JOB_AUTOMATION:
        return execute_automation_import_job(db, job, ctx=ctx)

    return DispatchOutcome(
        success=False,
        message=f"Unsupported import job type: {job.job_type}",
    )


def _build_runtime_context(db: PipelineDB):
    """Build the minimal CratediggerContext needed by download processing."""
    from album_source import DatabaseSource
    from lib.config import read_runtime_config
    from lib.context import CratediggerContext

    cfg = read_runtime_config()
    source = DatabaseSource(db.dsn)
    return CratediggerContext(cfg=cfg, slskd=None, pipeline_db_source=source)


def execute_automation_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
) -> DispatchOutcome:
    """Run completed-download processing from an automation queue job."""
    from lib.download import _run_completed_processing, reconstruct_grab_list_entry

    request_id = job.request_id
    if request_id is None:
        return DispatchOutcome(False, "Automation import job has no request_id")

    row = db.get_request(request_id)
    if not row:
        return DispatchOutcome(False, f"Album request {request_id} not found")

    raw_state = row.get("active_download_state")
    if not raw_state:
        return DispatchOutcome(
            False,
            f"Album request {request_id} has no active_download_state",
        )
    state = (
        ActiveDownloadState.from_dict(raw_state)
        if isinstance(raw_state, dict)
        else ActiveDownloadState.from_json(str(raw_state))
    )
    entry = reconstruct_grab_list_entry(row, state)
    created_ctx = ctx is None
    runtime_ctx = ctx or _build_runtime_context(db)
    try:
        result = _run_completed_processing(
            entry,
            request_id,
            state,
            db,
            runtime_ctx,
            import_job_id=job.id,
        )
    finally:
        if created_ctx:
            runtime_ctx.pipeline_db_source.close()
    if result is None:
        return DispatchOutcome(
            success=False,
            message=(
                "Automation import was deferred or requires manual recovery"
            ),
            deferred=True,
        )
    if isinstance(result, DispatchOutcome):
        return result
    if result:
        return DispatchOutcome(
            success=True,
            message="Automation import processing completed",
        )
    return DispatchOutcome(
        success=False,
        message="Automation import processing failed",
    )


def process_claimed_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
) -> ImportJob | None:
    """Execute a claimed job and persist its terminal queue status."""
    try:
        outcome = execute_import_job(db, job, ctx=ctx)
    except Exception as exc:
        logger.exception("Import job %s crashed", job.id)
        return db.mark_import_job_failed(
            job.id,
            error=type(exc).__name__,
            message=str(exc),
            result={"success": False},
        )

    result = _job_result(outcome)
    if outcome.success:
        dismissal = _dismiss_successful_force_import(db, job)
        if dismissal is not None:
            result["wrong_match_dismissal"] = dismissal
        return db.mark_import_job_completed(
            job.id,
            result=result,
            message=outcome.message,
        )
    # U2: dispatch flipped this row back to the preview lane (or tried to).
    # We do NOT write a terminal failed status, do NOT bump retry counters,
    # and do NOT run the wrong-match cleanup decision. The dispatch-side
    # state change is already persisted; we just log and yield.
    if outcome.code == DISPATCH_CODE_REQUEUED_FOR_PREVIEW:
        logger.info(
            "Import job %s (request %s) requeued for preview: %s",
            job.id,
            job.request_id,
            outcome.message,
        )
        return None
    if outcome.code == DISPATCH_CODE_REQUEUE_FAILED:
        # The requeue UPDATE itself failed (DB transient). Mark the job
        # terminally failed so it surfaces to ops rather than leaving it in
        # 'running' for startup recovery, which would just re-claim and hit
        # the same condition (REL-001). The operator can re-trigger the
        # import once the underlying DB issue is resolved.
        logger.error(
            "Import job %s (request %s) requeue to preview failed; "
            "marking job failed (operator must investigate): %s",
            job.id,
            job.request_id,
            outcome.message,
        )
        return db.mark_import_job_failed(
            job.id,
            error=outcome.message,
            message=f"requeue-to-preview failed: {outcome.message}",
            result=result,
        )
    cleanup = _cleanup_failed_force_import(db, job, outcome)
    if cleanup is not None:
        result["cleanup"] = cleanup
    return db.mark_import_job_failed(
        job.id,
        error=outcome.message,
        message=outcome.message,
        result=result,
    )


def run_once(
    db: PipelineDB,
    *,
    worker_id: str,
    ctx: Any = None,
) -> ImportJob | None:
    job = db.claim_next_import_job(worker_id=worker_id)
    if job is None:
        return None
    logger.info("Claimed import job %s (%s)", job.id, job.job_type)
    return process_claimed_job(db, job, ctx=ctx)


def recover_abandoned_running_jobs(db: PipelineDB) -> list[ImportJob]:
    """Requeue jobs left running by a previous importer process."""
    return db.requeue_running_import_jobs(message=RESTART_REQUEUE_MESSAGE)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Drain the Cratedigger import queue",
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--worker-id", default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    worker_id = args.worker_id or f"{socket.gethostname()}:{os.getpid()}"
    db = PipelineDB(args.dsn)
    try:
        # Keep the beets-mutating queue to one worker process. See
        # docs/advisory-locks.md for namespace rules.
        with db.advisory_lock(ADVISORY_LOCK_NAMESPACE_IMPORTER, 1) as acquired:
            if not acquired:
                logger.error("Another cratedigger importer is already running")
                return 1

            recovered = recover_abandoned_running_jobs(db)
            if recovered:
                logger.warning(
                    "Requeued %s abandoned running import job(s)",
                    len(recovered),
                )

            while True:
                job = run_once(db, worker_id=worker_id)
                if args.once:
                    return 0
                if job is None:
                    time.sleep(args.poll_interval)
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
