#!/usr/bin/env python3
"""Run async no-mutation previews for queued import jobs."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import threading
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import msgspec

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.import_preview import ImportPreviewResult, preview_import_from_path
from lib.import_evidence import (
    CANDIDATE_STATUS_REUSED,
    ensure_candidate_evidence_for_action,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    ImportJob,
)
from lib.pipeline_db import DEFAULT_DSN, PipelineDB
from lib.quality import ActiveDownloadState, AlbumQualityEvidence
from lib.quality_evidence import (
    EvidenceBuildResult,
    load_candidate_evidence_for_source,
)

logger = logging.getLogger("cratedigger-import-preview-worker")
STALE_PREVIEW_MESSAGE = "Preview worker restarted while job was running; retry queued"
PREVIEW_HEARTBEAT_INTERVAL_SECONDS = 30.0
PREVIEW_STALE_RECOVERY_INTERVAL_SECONDS = 60.0
PREVIEW_STALE_AGE = timedelta(hours=1)
PREVIEW_FAILURE_STATUS = "uncertain"


def _preview_result_dict(result: ImportPreviewResult) -> dict[str, Any]:
    return result.to_dict()


def _preview_reason(result: ImportPreviewResult) -> str:
    return result.reason or result.decision or result.verdict


def _download_log_id_from_job(job: ImportJob) -> int | None:
    payload = job.payload or {}
    download_log_id = payload.get("download_log_id")
    return download_log_id if isinstance(download_log_id, int) else None


def _candidate_evidence_ready_for_job(
    db: Any,
    job: ImportJob,
    result: ImportPreviewResult,
) -> tuple[bool, str]:
    source_path = result.source_path
    if not source_path:
        return False, "preview_source_path_missing"
    candidate = ensure_candidate_evidence_for_action(
        db,
        source_path=source_path,
        import_job_id=job.id,
        download_log_id=_download_log_id_from_job(job),
    )
    if candidate.available and candidate.evidence is not None:
        return True, "ready"
    return (
        False,
        candidate.provenance.fallback_reason
        or candidate.provenance.candidate_status
        or "candidate_evidence_unavailable",
    )


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


def derive_canonical_import_folder(
    row: dict[str, Any],
    state: ActiveDownloadState,
) -> str:
    """Cheaply derive the canonical automation import folder.

    Computes the same path ``_materialize_automation_preview_path`` would
    settle on, without performing any filesystem materialization. Used by
    the preview-worker front-gate to test stored candidate evidence's
    snapshot against the current source location before deciding whether
    to skip measurement.
    """
    from lib.config import read_runtime_config
    from lib.download import (
        _canonical_import_folder_path,
        reconstruct_grab_list_entry,
    )

    cfg = read_runtime_config()
    entry = reconstruct_grab_list_entry(row, state)
    if entry.import_folder:
        return entry.import_folder
    return _canonical_import_folder_path(entry, cfg.slskd_download_dir)


def _materialize_automation_preview_path(
    db: Any,
    request_id: int,
    row: dict[str, Any],
    state: ActiveDownloadState,
) -> str:
    """Ensure automation preview has the same stable folder importer uses."""
    from lib.config import read_runtime_config
    from lib.download import (
        _materialize_processing_dir,
        reconstruct_grab_list_entry,
    )
    from lib.staged_album import StagedAlbum

    cfg = read_runtime_config()
    entry = reconstruct_grab_list_entry(row, state)
    canonical_path = derive_canonical_import_folder(row, state)
    if entry.import_folder is None:
        entry.import_folder = canonical_path
    ctx = cast(Any, SimpleNamespace(
        cfg=cfg,
        pipeline_db_source=SimpleNamespace(_get_db=lambda: db),
    ))
    staged_album = StagedAlbum.from_entry(
        entry,
        default_path=canonical_path,
    )
    materialized = _materialize_processing_dir(entry, staged_album, ctx)
    if materialized is not True:
        raise ValueError(
            f"Album request {request_id} could not be materialized for preview"
        )
    return staged_album.current_path


def _front_gate_source_path(db: Any, job: ImportJob) -> str | None:
    """Cheap source-path derivation for the candidate-evidence front-gate.

    Returns the path the evidence snapshot would have captured, or ``None``
    when the path cannot be derived without invoking measurement-time
    materialization. ``None`` is a graceful skip: the worker falls through
    to the existing measurement codepath.
    """
    payload = job.payload or {}
    if job.job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_MANUAL):
        failed_path = payload.get("failed_path")
        if isinstance(failed_path, str) and failed_path:
            return failed_path
        return None
    if job.job_type == IMPORT_JOB_AUTOMATION:
        if job.request_id is None:
            return None
        row = db.get_request(job.request_id)
        if not row:
            return None
        state_raw = row.get("active_download_state")
        if state_raw is None:
            return None
        try:
            state = _state_from_raw(state_raw)
        except ValueError:
            return None
        try:
            return derive_canonical_import_folder(row, state)
        except Exception:
            logger.debug(
                "front-gate path derivation failed for job %s; "
                "falling through to measurement",
                job.id,
                exc_info=True,
            )
            return None
    return None


def _reused_evidence_preview_payload(
    job: ImportJob,
    evidence: AlbumQualityEvidence,
    source_path: str,
) -> dict[str, Any]:
    """Synthesize a preview_result payload for the reused-evidence branch.

    Mirrors the shape ``ImportPreviewResult.to_dict()`` produces so
    downstream consumers (web UI recents tab, decision-tree viewers) see
    the keys they already render. Adds top-level ``candidate_status``
    provenance so the reused path is distinguishable from the measured
    path.
    """
    del evidence  # measurement is recorded in the evidence row itself
    payload = msgspec.to_builtins(ImportPreviewResult(
        mode="reused",
        verdict="would_import",
        would_import=True,
        decision="candidate_evidence_reused",
        reason="candidate_evidence_reused",
        stage_chain=["preview:candidate_evidence_reused"],
        request_id=job.request_id,
        download_log_id=_download_log_id_from_job(job),
        source_path=source_path,
    ))
    assert isinstance(payload, dict)
    payload["candidate_status"] = CANDIDATE_STATUS_REUSED
    return payload


def _front_gate_check(
    db: Any,
    job: ImportJob,
) -> tuple[EvidenceBuildResult | None, str | None]:
    """Run the cheap candidate-evidence front-gate for ``job``.

    Returns ``(result, source_path)``. ``result is None`` means the
    front-gate could not run at all (path-derivation deferred to the
    measurement path) and the caller should fall through. A non-None
    result with ``status == 'ready'`` means measurement can be skipped.
    """
    source_path = _front_gate_source_path(db, job)
    if not source_path:
        return None, None
    try:
        result = load_candidate_evidence_for_source(
            db,
            source_path=source_path,
            download_log_id=_download_log_id_from_job(job),
            import_job_id=job.id,
        )
    except Exception:
        logger.debug(
            "front-gate evidence load failed for job %s; "
            "falling through to measurement",
            job.id,
            exc_info=True,
        )
        return None, source_path
    return result, source_path


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
        if not state.current_path or not os.path.isdir(state.current_path):
            state.current_path = _materialize_automation_preview_path(
                db,
                job.request_id,
                row,
                state,
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
    return preview_import_from_path(
        db,
        import_job_id=job.id,
        persist_candidate_evidence=True,
        **preview_input,
    )


def _mark_automation_preview_blocked(
    db: Any,
    job: ImportJob,
    *,
    preview_status: str,
    reason: str,
    preview_payload: dict[str, Any],
) -> ImportJob | None:
    blocker = getattr(db, "mark_import_job_preview_blocked", None)
    if callable(blocker):
        return cast(ImportJob | None, blocker(
            job.id,
            preview_status=preview_status,
            error=reason,
            preview_result=preview_payload,
            message=f"Preview blocked automation import: {reason}",
        ))
    return db.mark_import_job_preview_failed(
        job.id,
        preview_status=preview_status,
        error=reason,
        preview_result=preview_payload,
        message=f"Preview blocked automation import: {reason}",
    )


def process_claimed_preview_job(db: Any, job: ImportJob) -> ImportJob | None:
    # Front-gate: if stored candidate evidence already passes the cheap
    # snapshot guard, mark the job importable without invoking measurement.
    # The post-measurement gate below remains as belt-and-braces for the
    # fall-through path.
    front_gate_result, front_gate_source = _front_gate_check(db, job)
    if (
        front_gate_result is not None
        and front_gate_result.status == "ready"
        and front_gate_result.evidence is not None
        and front_gate_source is not None
    ):
        reused_payload = _reused_evidence_preview_payload(
            job,
            front_gate_result.evidence,
            front_gate_source,
        )
        logger.info(
            "Reused candidate evidence for import job %s; skipping preview measurement",
            job.id,
        )
        return db.mark_import_job_preview_importable(
            job.id,
            preview_result=reused_payload,
            message="Reused stored candidate evidence (snapshot matched)",
        )

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
    evidence_ready, evidence_reason = _candidate_evidence_ready_for_job(
        db,
        job,
        result,
    )
    if evidence_ready:
        return db.mark_import_job_preview_importable(
            job.id,
            preview_result=preview_payload,
            message=f"Evidence ready for final check: {_preview_reason(result)}",
        )

    reason = _preview_reason(result) or evidence_reason
    if job.job_type == IMPORT_JOB_AUTOMATION:
        return _mark_automation_preview_blocked(
            db,
            job,
            preview_status=PREVIEW_FAILURE_STATUS,
            reason=reason,
            preview_payload=preview_payload,
        )
    return db.mark_import_job_preview_failed(
        job.id,
        preview_status=PREVIEW_FAILURE_STATUS,
        error=reason,
        preview_result=preview_payload,
        message=f"Preview failed: {reason}",
    )


def preview_heartbeat_loop(
    *,
    dsn: str,
    job_id: int,
    stop: threading.Event,
    interval: float = PREVIEW_HEARTBEAT_INTERVAL_SECONDS,
    db_factory: Any | None = None,
) -> None:
    """Heartbeat a running preview from its own DB session."""
    factory = db_factory or PipelineDB
    db = factory(dsn)
    try:
        while not stop.wait(interval):
            if not db.heartbeat_import_job_preview(job_id):
                return
    except Exception:
        logger.warning("Preview heartbeat failed for job %s", job_id, exc_info=True)
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()


def process_claimed_preview_job_with_heartbeat(
    db: Any,
    job: ImportJob,
    *,
    heartbeat_interval: float = PREVIEW_HEARTBEAT_INTERVAL_SECONDS,
) -> ImportJob | None:
    dsn = getattr(db, "dsn", None)
    if not dsn:
        return process_claimed_preview_job(db, job)

    stop = threading.Event()
    heartbeat_thread = threading.Thread(
        target=preview_heartbeat_loop,
        kwargs={
            "dsn": str(dsn),
            "job_id": job.id,
            "stop": stop,
            "interval": heartbeat_interval,
            "db_factory": PipelineDB,
        },
        daemon=True,
        name=f"preview-heartbeat-{job.id}",
    )
    heartbeat_thread.start()
    try:
        return process_claimed_preview_job(db, job)
    finally:
        stop.set()
        heartbeat_thread.join(timeout=5.0)


def run_once(
    db: PipelineDB,
    *,
    worker_id: str,
    heartbeat_interval: float = PREVIEW_HEARTBEAT_INTERVAL_SECONDS,
) -> ImportJob | None:
    job = db.claim_next_import_preview_job(worker_id=worker_id)
    if job is None:
        return None
    logger.info("Claimed import preview job %s (%s)", job.id, job.job_type)
    return process_claimed_preview_job_with_heartbeat(
        db,
        job,
        heartbeat_interval=heartbeat_interval,
    )


def recover_abandoned_preview_jobs(
    db: PipelineDB,
    *,
    older_than: timedelta = PREVIEW_STALE_AGE,
) -> list[ImportJob]:
    return db.requeue_stale_import_preview_jobs(
        older_than=older_than,
        message=STALE_PREVIEW_MESSAGE,
    )


def preview_recovery_loop(
    *,
    dsn: str,
    stop: threading.Event,
    interval: float = PREVIEW_STALE_RECOVERY_INTERVAL_SECONDS,
    db_factory: Any | None = None,
) -> None:
    factory = db_factory or PipelineDB
    db = factory(dsn)
    try:
        while not stop.wait(interval):
            recovered = recover_abandoned_preview_jobs(db)
            if recovered:
                logger.warning(
                    "Requeued %s abandoned import preview job(s)",
                    len(recovered),
                )
    except Exception:
        logger.exception("Import preview recovery loop crashed")
        raise
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()


def run_threaded_workers(
    *,
    dsn: str,
    worker_id: str,
    worker_count: int,
    poll_interval: float,
) -> int:
    stop = threading.Event()
    errors: list[BaseException] = []
    error_lock = threading.Lock()

    def record_error(exc: BaseException) -> None:
        with error_lock:
            errors.append(exc)
        stop.set()

    def worker_loop(index: int) -> None:
        thread_db = PipelineDB(dsn)
        thread_worker_id = f"{worker_id}:preview-{index}"
        try:
            while not stop.is_set():
                job = run_once(thread_db, worker_id=thread_worker_id)
                if job is None:
                    stop.wait(poll_interval)
        except BaseException as exc:
            record_error(exc)
            logger.exception("Import preview worker thread %s crashed", index)
        finally:
            thread_db.close()

    def recovery_loop() -> None:
        try:
            preview_recovery_loop(dsn=dsn, stop=stop, db_factory=PipelineDB)
        except BaseException as exc:
            record_error(exc)

    threads = [
        threading.Thread(target=worker_loop, args=(i,), daemon=False)
        for i in range(worker_count)
    ]
    recovery_thread = threading.Thread(
        target=recovery_loop,
        daemon=False,
        name="preview-recovery",
    )
    recovery_thread.start()
    for thread in threads:
        thread.start()
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        stop.set()
        for thread in threads:
            thread.join()
        recovery_thread.join()
        return 0

    stop.set()
    recovery_thread.join()

    if errors:
        logger.error(
            "Import preview worker exiting after %s worker thread crash(es)",
            len(errors),
        )
        return 1
    return 0


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

        if not args.once:
            return run_threaded_workers(
                dsn=args.dsn,
                worker_id=worker_id,
                worker_count=args.workers,
                poll_interval=args.poll_interval,
            )

        run_once(db, worker_id=worker_id)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
