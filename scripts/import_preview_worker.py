#!/usr/bin/env python3
"""Run async no-mutation previews for queued import jobs."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import threading
from collections.abc import Callable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import msgspec
import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.config import read_runtime_config
from lib.dispatch import _record_preview_measurement_failed
from lib.import_preview import (
    PREVIEW_VERDICT_EVIDENCE_READY,
    PREVIEW_VERDICT_MEASUREMENT_FAILED,
    ImportPreviewResult,
    load_current_evidence_for_preview,
    load_persisted_existing_spectral,
    measure_and_persist_candidate_evidence,
    persist_exact_current_spectral_from_attempt,
    preserve_existing_source_spectral,
)
from lib.import_evidence import (
    CANDIDATE_STATUS_REUSED,
    ensure_candidate_evidence_for_action,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
)
from lib.pipeline_db import DEFAULT_DSN, PipelineDB
from lib.measurement import (
    ExistingSpectralAuditLookup,
    ExistingSpectralResolver,
    SpectralDetailAnalyzer,
    analyze_spectral_audit_path,
    collect_release_attempt_spectral_audit,
    existing_spectral_resolver_for_config,
    spectral_detail_from_persisted_source,
)
from lib.quality import (
    ActiveDownloadState,
    AlbumQualityEvidence,
    MeasurementFailure,
    ImportResult,
    SpectralAnalysisDetail,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    load_candidate_evidence_for_source,
)
from lib.youtube_ingest_service import YoutubeImportPayload

logger = logging.getLogger("cratedigger-import-preview-worker")
STALE_PREVIEW_MESSAGE = "Preview worker restarted while job was running; retry queued"
RESTART_PREVIEW_MESSAGE = "Preview worker restarted while job was running; retry queued"
PREVIEW_HEARTBEAT_INTERVAL_SECONDS = 30.0
PREVIEW_STALE_RECOVERY_INTERVAL_SECONDS = 60.0
PREVIEW_STALE_AGE = timedelta(minutes=15)


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
    from lib.download_reconstruction import reconstruct_grab_list_entry
    from lib.processing_paths import canonical_folder_for_row

    cfg = read_runtime_config()
    entry = reconstruct_grab_list_entry(row, state)
    if entry.import_folder:
        return entry.import_folder
    return canonical_folder_for_row(entry, cfg.slskd_download_dir)


def _materialize_automation_preview_path(
    db: Any,
    request_id: int,
    row: dict[str, Any],
    state: ActiveDownloadState,
) -> str:
    """Ensure automation preview has the same stable folder importer uses."""
    from lib.config import read_runtime_config
    from lib.download_reconstruction import reconstruct_grab_list_entry
    from lib.download_materialization import (
        Materialized,
        _materialize_processing_dir,
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
    if not isinstance(materialized, Materialized):
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
    if job.job_type == IMPORT_JOB_FORCE:
        failed_path = payload.get("failed_path")
        if isinstance(failed_path, str) and failed_path:
            return failed_path
        return None
    if job.job_type == IMPORT_JOB_YOUTUBE:
        # KTD1: YT path NEVER reads ``active_download_state``. The
        # staged path comes from ``import_jobs.payload['staged_path']``,
        # decoded via ``msgspec.convert`` for wire-boundary safety.
        try:
            youtube_payload = msgspec.convert(payload, type=YoutubeImportPayload)
        except msgspec.ValidationError:
            logger.debug(
                "front-gate YT payload validation failed for job %s; "
                "falling through to measurement",
                job.id,
                exc_info=True,
            )
            return None
        return youtube_payload.staged_path
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
            state = ActiveDownloadState.from_raw(state_raw)
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
    import_result: ImportResult,
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
        import_result=import_result,
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
        download_log_id = payload.get("download_log_id")
        return {
            "request_id": job.request_id,
            "path": failed_path,
            "force": True,
            "download_log_id": (
                int(download_log_id)
                if isinstance(download_log_id, int)
                else None
            ),
        }

    if job.job_type == IMPORT_JOB_AUTOMATION:
        row = db.get_request(job.request_id)
        if not row:
            raise ValueError(f"Album request {job.request_id} not found")
        state = ActiveDownloadState.from_raw(row.get("active_download_state"))
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
            "download_log_id": None,
        }

    if job.job_type == IMPORT_JOB_YOUTUBE:
        # KTD1: never read ``active_download_state``. The staged path
        # is the authoritative source — yt-dlp already wrote files
        # there, and we measure them in place.
        try:
            youtube_payload = msgspec.convert(payload, type=YoutubeImportPayload)
        except msgspec.ValidationError as exc:
            raise ValueError(
                f"YouTube import preview job has malformed payload: {exc}"
            ) from exc
        return {
            "request_id": job.request_id,
            "path": youtube_payload.staged_path,
            "force": False,
            "download_log_id": None,
        }

    raise ValueError(f"Unsupported import job type: {job.job_type}")


def execute_preview_job(db: Any, job: ImportJob) -> ImportPreviewResult:
    preview_input = _preview_input(db, job)
    return measure_and_persist_candidate_evidence(
        db,
        import_job_id=job.id,
        **preview_input,
    )


def _handle_measurement_failed(
    db: Any,
    job: ImportJob,
    result: ImportPreviewResult,
) -> ImportJob | None:
    """Persist a measurement failure through one DB-owned terminal bundle.

    Request-backed jobs atomically commit the preview fields, failed job,
    request self-heal, mandatory audit, and any denylist writes. A malformed
    orphan job with no request row has no legal ``download_log`` owner, so it
    remains a job-only precondition failure.

    ``denylist_username`` is currently always None — the per-user 5-strikes
    rule lives in the importer-side reject path (U6). Preview measurement
    failures are infrastructure-class failures (source vanished, snapshot
    stale, crashed); the user isn't responsible for the source going away
    mid-measure, so we do not denylist here.
    """
    payload = result.failure
    if payload is None:
        # Defensive: every measurement_failed result must carry a payload.
        # Synthesize one from the result fields so we never fall through
        # without firing the self-heal.
        payload = MeasurementFailure(
            reason="measurement_crashed",
            detail=result.detail or result.reason or "measurement_failed",
            source_path=result.source_path or "",
        )
    preview_payload = _preview_result_dict(result)
    if job.request_id is None or db.get_request(job.request_id) is None:
        return db.mark_import_job_preview_failed(
            job.id,
            preview_status=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            error=payload.reason,
            preview_result=preview_payload,
            message=f"Preview measurement failed: {payload.reason}",
        )
    _record_preview_measurement_failed(
        db,
        request_id=job.request_id,
        import_job_id=job.id,
        payload=payload,
        import_result=result.import_result,
        preview_result=preview_payload,
    )
    refreshed = getattr(db, "get_import_job", None)
    if callable(refreshed):
        return cast(ImportJob | None, refreshed(job.id))
    return None


PreviewFn = Callable[[Any, ImportJob], ImportPreviewResult]


def process_claimed_preview_job(
    db: Any,
    job: ImportJob,
    *,
    spectral_detail_analyzer: SpectralDetailAnalyzer | None = None,
    existing_spectral_resolver: ExistingSpectralResolver | None = None,
    preview_fn: PreviewFn | None = None,
) -> ImportJob | None:
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
        persisted_existing = SpectralAnalysisDetail(attempted=False)
        preserve_have_source = False
        mb_release_id = ""
        current_evidence = None
        if job.request_id is not None:
            try:
                req = db.get_request(job.request_id) or {}
                mb_release_id = str(req.get("mb_release_id") or "")
                current_evidence, persisted_existing, authoritative = (
                    load_persisted_existing_spectral(
                        db,
                        job.request_id,
                    )
                )
                preview_cfg = read_runtime_config()
                current_evidence = load_current_evidence_for_preview(
                    db,
                    request_id=job.request_id,
                    mb_release_id=mb_release_id,
                    quality_ranks=preview_cfg.quality_ranks,
                    beets_library_root=getattr(
                        preview_cfg, "beets_directory", ""
                    ),
                    preloaded_evidence=current_evidence,
                    preloaded_authoritative=authoritative,
                )
                if current_evidence is not None:
                    persisted_existing = spectral_detail_from_persisted_source(
                        current_evidence.measurement.spectral_grade,
                        current_evidence.measurement.spectral_bitrate_kbps,
                    )
                preserve_have_source = preserve_existing_source_spectral(
                    current_evidence,
                )
            except Exception:
                logger.exception(
                    "Unable to load reused HAVE evidence for request %s",
                    job.request_id,
                )
        audit_resolver = existing_spectral_resolver
        if audit_resolver is None:
            try:
                audit_cfg = read_runtime_config()
            except Exception as exc:
                logger.exception("Unable to load config for reused HAVE audit")
                failed_lookup = ExistingSpectralAuditLookup(
                    failure=SpectralAnalysisDetail(
                        attempted=True,
                        error=f"{type(exc).__name__}: {exc}",
                    ),
                )
                audit_resolver = lambda _release_id: failed_lookup
            else:
                audit_resolver = existing_spectral_resolver_for_config(audit_cfg)
        audit, have_lookup = collect_release_attempt_spectral_audit(
            front_gate_source,
            mb_release_id,
            existing_spectral_evidence=persisted_existing,
            preserve_existing_source_spectral=preserve_have_source,
            analyzer=(
                spectral_detail_analyzer or analyze_spectral_audit_path
            ),
            existing_resolver=audit_resolver,
        )
        # The reuse fast path skips measurement but must still make its
        # HAVE scan durable BEFORE the importer decides — an audit-only
        # scan left the decision spectrally blind (download_log 37206).
        # The persist helper's own guards keep this once-only, exact-path,
        # exact-snapshot; failures are fail-soft like the audit itself.
        if (
            job.request_id is not None
            and current_evidence is not None
            and not preserve_have_source
            and have_lookup.path is not None
        ):
            try:
                persist_exact_current_spectral_from_attempt(
                    db,
                    request_id=job.request_id,
                    current_evidence=current_evidence,
                    measured_existing=audit.existing,
                    measured_existing_path=have_lookup.path,
                )
            except Exception:
                logger.exception(
                    "Unable to persist reused-path HAVE spectral for "
                    "request %s",
                    job.request_id,
                )
        reused_payload = _reused_evidence_preview_payload(
            job,
            front_gate_result.evidence,
            front_gate_source,
            ImportResult(spectral=audit),
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
        result = (preview_fn or execute_preview_job)(db, job)
    except Exception as exc:
        logger.exception("Import job %s preview crashed", job.id)
        # Worker-mode preview should not raise — but if it does, route the
        # crash through the same self-healing helper so the parent request
        # gets finalized to ``wanted`` and the job is marked failed.
        crash_payload = MeasurementFailure(
            reason="measurement_crashed",
            detail=f"{type(exc).__name__}: {exc}",
            source_path="",
        )
        crash_result = ImportPreviewResult(
            mode="path",
            verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            uncertain=False,
            decision="measurement_crashed",
            reason="measurement_crashed",
            detail=f"{type(exc).__name__}: {exc}",
            request_id=job.request_id,
            failure=crash_payload,
        )
        return _handle_measurement_failed(db, job, crash_result)

    if result.verdict == PREVIEW_VERDICT_MEASUREMENT_FAILED:
        return _handle_measurement_failed(db, job, result)

    if result.verdict == PREVIEW_VERDICT_EVIDENCE_READY:
        preview_payload = _preview_result_dict(result)
        # Belt-and-braces: confirm candidate evidence is actually
        # persisted on disk before marking importable. If the
        # persistence stage was skipped or partial, fall back to
        # measurement_failed so the parent request still self-heals.
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
        fallback_payload = MeasurementFailure(
            reason="evidence_persist_failed",
            detail=evidence_reason or "candidate evidence unavailable",
            source_path=result.source_path or "",
        )
        fallback_result = ImportPreviewResult(
            mode=result.mode,
            verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            decision="evidence_persist_failed",
            reason="evidence_persist_failed",
            detail=evidence_reason,
            source_path=result.source_path,
            request_id=result.request_id,
            download_log_id=result.download_log_id,
            import_result=result.import_result,
            failure=fallback_payload,
        )
        return _handle_measurement_failed(db, job, fallback_result)

    # Defensive: anything else (including legacy verdicts in case of bugs)
    # routes through measurement_failed so the parent request self-heals
    # rather than getting stuck.
    logger.warning(
        "Import job %s preview returned unexpected verdict %r; treating as measurement_failed",
        job.id,
        result.verdict,
    )
    fallback_payload = MeasurementFailure(
        reason="measurement_crashed",
        detail=f"unexpected verdict: {result.verdict}",
        source_path=result.source_path or "",
    )
    fallback_result = ImportPreviewResult(
        mode=result.mode,
        verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
        decision="unexpected_verdict",
        reason=result.verdict,
        detail=f"unexpected verdict: {result.verdict}",
        source_path=result.source_path,
        request_id=result.request_id,
        download_log_id=result.download_log_id,
        import_result=result.import_result,
        failure=fallback_payload,
    )
    return _handle_measurement_failed(db, job, fallback_result)


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


def recover_running_preview_jobs(db: PipelineDB) -> list[ImportJob]:
    """Requeue every preview job left running by a previous worker process.

    Called once at startup. Systemd guarantees a single preview-worker
    process; if any ``preview_status='running'`` rows exist when this
    process starts, the previous owner is dead and the rows are
    orphaned regardless of how recently their heartbeats fired. This
    is the importer's ``recover_abandoned_running_jobs`` pattern,
    mirrored for the preview lane. The periodic
    ``preview_recovery_loop`` keeps using the 15-minute heartbeat
    threshold for the running-system safety net.
    """
    return db.requeue_running_import_preview_jobs(
        message=RESTART_PREVIEW_MESSAGE,
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
                try:
                    job = run_once(thread_db, worker_id=thread_worker_id)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    # Transient DB connection loss — the live failure mode
                    # is PostgreSQL dropping the worker's idle connection
                    # between jobs. ``PipelineDB._execute`` reconnects on
                    # subsequent calls, so we just need to back off and
                    # keep polling rather than tearing the whole process
                    # down. A persistent failure will surface as repeated
                    # warnings and either Postgres recovery or systemd
                    # restart resolves it.
                    logger.warning(
                        "Import preview worker thread %s lost DB connection; "
                        "backing off and retrying: %s",
                        index, exc,
                    )
                    stop.wait(poll_interval)
                    continue
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
        recovered = recover_running_preview_jobs(db)
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
