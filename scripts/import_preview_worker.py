#!/usr/bin/env python3
"""Run async no-mutation previews for queued import jobs."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import threading
import time
from datetime import timedelta
from typing import Any

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.import_preview import ImportPreviewResult, preview_import_from_path
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    ImportJob,
)
from lib.pipeline_db import DEFAULT_DSN, PipelineDB
from lib.quality import ActiveDownloadState

logger = logging.getLogger("cratedigger-import-preview-worker")
STALE_PREVIEW_MESSAGE = "Preview worker restarted while job was running; retry queued"


def _preview_result_dict(result: ImportPreviewResult) -> dict[str, Any]:
    return result.to_dict()


def _preview_reason(result: ImportPreviewResult) -> str:
    return result.reason or result.decision or result.verdict


def _failure_preview_status(result: ImportPreviewResult) -> str:
    if result.confident_reject:
        return "confident_reject"
    if result.uncertain:
        return "uncertain"
    return "error"


def _state_from_raw(raw: Any) -> ActiveDownloadState:
    if isinstance(raw, dict):
        return ActiveDownloadState.from_dict(raw)
    if isinstance(raw, str):
        return ActiveDownloadState.from_json(raw)
    raise ValueError("Automation import job has no active_download_state")


def _first_state_username(state: ActiveDownloadState) -> str | None:
    for file_state in state.files:
        if file_state.username:
            return file_state.username
    return None


def _preview_input(db: Any, job: ImportJob) -> dict[str, Any]:
    if job.request_id is None:
        raise ValueError("Import job has no request_id")

    payload = job.payload or {}
    if job.job_type == IMPORT_JOB_FORCE:
        failed_path = payload.get("failed_path")
        if not isinstance(failed_path, str) or not failed_path:
            raise ValueError("Force import preview job is missing failed_path")
        source_username = payload.get("source_username")
        download_log_id = payload.get("download_log_id")
        return {
            "request_id": job.request_id,
            "path": failed_path,
            "force": True,
            "source_username": (
                str(source_username) if source_username is not None else None
            ),
            "download_log_id": (
                int(download_log_id)
                if isinstance(download_log_id, int)
                else None
            ),
        }

    if job.job_type == IMPORT_JOB_MANUAL:
        failed_path = payload.get("failed_path")
        if not isinstance(failed_path, str) or not failed_path:
            raise ValueError("Manual import preview job is missing failed_path")
        return {
            "request_id": job.request_id,
            "path": failed_path,
            "force": False,
            "source_username": None,
            "download_log_id": None,
        }

    if job.job_type == IMPORT_JOB_AUTOMATION:
        row = db.get_request(job.request_id)
        if not row:
            raise ValueError(f"Album request {job.request_id} not found")
        state = _state_from_raw(row.get("active_download_state"))
        if not state.current_path:
            raise ValueError(
                f"Album request {job.request_id} has no active download path"
            )
        return {
            "request_id": job.request_id,
            "path": state.current_path,
            "force": False,
            "source_username": _first_state_username(state),
            "download_log_id": None,
        }

    raise ValueError(f"Unsupported import job type: {job.job_type}")


def execute_preview_job(db: Any, job: ImportJob) -> ImportPreviewResult:
    preview_input = _preview_input(db, job)
    return preview_import_from_path(db, **preview_input)


def _denylist_confident_reject(
    db: Any,
    job: ImportJob,
    result: ImportPreviewResult,
) -> dict[str, Any] | None:
    if not result.confident_reject or job.request_id is None:
        return None
    if result.reason == "path_missing":
        return None

    try:
        preview_input = _preview_input(db, job)
    except Exception:
        return None
    source_username = preview_input.get("source_username")
    if not source_username:
        return None

    add_denylist = getattr(db, "add_denylist", None)
    if not callable(add_denylist):
        return None
    reason = f"import preview rejected: {_preview_reason(result)}"
    add_denylist(job.request_id, str(source_username), reason)
    return {
        "request_id": job.request_id,
        "username": str(source_username),
        "reason": reason,
    }


def process_claimed_preview_job(db: Any, job: ImportJob) -> ImportJob | None:
    try:
        result = execute_preview_job(db, job)
    except Exception as exc:
        logger.exception("Import job %s preview crashed", job.id)
        return db.mark_import_job_preview_failed(
            job.id,
            preview_status="error",
            error=type(exc).__name__,
            preview_result={
                "verdict": "error",
                "reason": type(exc).__name__,
                "detail": str(exc),
            },
            message=f"Preview failed: {exc}",
        )

    preview_payload = _preview_result_dict(result)
    if result.would_import:
        return db.mark_import_job_preview_importable(
            job.id,
            preview_result=preview_payload,
            message=f"Preview would import: {_preview_reason(result)}",
        )

    denylist = _denylist_confident_reject(db, job, result)
    if denylist is not None:
        preview_payload["denylist"] = denylist
    reason = _preview_reason(result)
    return db.mark_import_job_preview_failed(
        job.id,
        preview_status=_failure_preview_status(result),
        error=reason,
        preview_result=preview_payload,
        message=f"Preview failed: {reason}",
    )


def run_once(db: PipelineDB, *, worker_id: str) -> ImportJob | None:
    job = db.claim_next_import_preview_job(worker_id=worker_id)
    if job is None:
        return None
    logger.info("Claimed import preview job %s (%s)", job.id, job.job_type)
    return process_claimed_preview_job(db, job)


def recover_abandoned_preview_jobs(
    db: PipelineDB,
    *,
    older_than: timedelta = timedelta(hours=1),
) -> list[ImportJob]:
    return db.requeue_stale_import_preview_jobs(
        older_than=older_than,
        message=STALE_PREVIEW_MESSAGE,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run async previews for Cratedigger import jobs",
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be >= 1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    worker_id = args.worker_id or f"{socket.gethostname()}:{os.getpid()}"
    db = PipelineDB(args.dsn)
    try:
        recovered = recover_abandoned_preview_jobs(db)
        if recovered:
            logger.warning(
                "Requeued %s abandoned import preview job(s)",
                len(recovered),
            )

        if args.workers > 1 and not args.once:
            stop = threading.Event()

            def worker_loop(index: int) -> None:
                thread_db = PipelineDB(args.dsn)
                thread_worker_id = f"{worker_id}:preview-{index}"
                try:
                    while not stop.is_set():
                        job = run_once(thread_db, worker_id=thread_worker_id)
                        if job is None:
                            time.sleep(args.poll_interval)
                finally:
                    thread_db.close()

            threads = [
                threading.Thread(target=worker_loop, args=(i,), daemon=False)
                for i in range(args.workers)
            ]
            for thread in threads:
                thread.start()
            try:
                for thread in threads:
                    thread.join()
            except KeyboardInterrupt:
                stop.set()
                for thread in threads:
                    thread.join()
            return 0

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
