#!/usr/bin/env python3
"""Drain the shared import queue through one beets-mutating lane."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import time
from collections.abc import Callable
from typing import Any, assert_never

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import msgspec

from lib.dispatch import (
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DISPATCH_CODE_REQUEUE_FAILED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    DispatchOutcome,
)
from lib.dispatch.types import PostCommitCleanup
from lib.download_processing import (
    Completed,
    CompletionDeferred,
    CompletionDispatched,
    CompletionFailed,
    CompletionResult,
    ProcessAlbumFn,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
)
from lib.terminal_outcomes import ImportJobTerminal
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORTER,
    DEFAULT_DSN,
    PipelineDB,
)
from lib.import_manifest import audio_relative_paths
from lib.quality import ActiveDownloadFileState, ActiveDownloadState
from lib.youtube_ingest_service import (
    YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES,
    YoutubeImportPayload,
)

logger = logging.getLogger("cratedigger-importer")
RESTART_REQUEUE_MESSAGE = "Importer restarted while job was running; retry queued"
RESTART_RECOVERY_MESSAGE = (
    "Recovery required: importer restarted after Beets launch authorization"
)


def _job_result(outcome: DispatchOutcome) -> dict[str, Any]:
    return {
        "success": outcome.success,
        "message": outcome.message,
        "deferred": outcome.deferred,
        "code": outcome.code,
    }


def _run_post_commit_cleanup(outcome: DispatchOutcome) -> dict[str, object] | None:
    """Run narrow destructive convergence only after terminal acknowledgement."""
    plan = outcome.post_commit_cleanup
    if plan is None:
        return None

    details: dict[str, object] = {}
    if plan.duplicate_guard_source_path is not None:
        try:
            from lib.duplicate_remove_guard import (
                quarantine_duplicate_remove_guard_source,
            )

            quarantine = quarantine_duplicate_remove_guard_source(
                source_path=plan.duplicate_guard_source_path,
                staging_dir=plan.duplicate_guard_staging_dir or "",
                request_id=plan.duplicate_guard_request_id,
            )
            details["duplicate_guard_quarantine"] = {
                "source_path": quarantine.source_path,
                "quarantine_path": quarantine.quarantine_path,
                "moved": quarantine.moved,
                "already_quarantined": quarantine.already_quarantined,
                "path_missing": quarantine.path_missing,
                "error": quarantine.error,
            }
        except Exception as exc:  # noqa: BLE001 - terminal commit must stand
            logger.exception("Post-commit duplicate-guard quarantine failed")
            details["duplicate_guard_quarantine"] = {
                "source_path": plan.duplicate_guard_source_path,
                "error": f"{type(exc).__name__}: {exc}",
            }

    if plan.staged_path is not None:
        try:
            from lib.dispatch.helpers import _cleanup_staged_dir

            _cleanup_staged_dir(plan.staged_path)
            details["staged_path"] = {
                "path": plan.staged_path,
                "success": True,
            }
        except Exception as exc:  # noqa: BLE001 - terminal commit must stand
            logger.exception("Post-commit staged-path cleanup failed")
            details["staged_path"] = {
                "path": plan.staged_path,
                "success": False,
                "error": f"{type(exc).__name__}: {exc}",
            }

    if plan.disambiguation_imported_path is not None:
        try:
            from lib.util import cleanup_disambiguation_orphans

            removed = cleanup_disambiguation_orphans(
                plan.disambiguation_imported_path,
                beets_directory=plan.beets_directory,
            )
            details["disambiguation_orphans"] = {
                "imported_path": plan.disambiguation_imported_path,
                "removed": removed,
            }
        except Exception as exc:  # noqa: BLE001 - terminal commit must stand
            logger.exception("Post-commit disambiguation cleanup failed")
            details["disambiguation_orphans"] = {
                "imported_path": plan.disambiguation_imported_path,
                "error": f"{type(exc).__name__}: {exc}",
            }

    return details or None


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

    if job.job_type == IMPORT_JOB_FORCE:
        # FORCE delegates straight to dispatch_import_from_db, which
        # already returns a terminal DispatchOutcome from its own decision
        # tree — no CompletionResult in the middle, so nothing here is
        # parallel to _dispatch_outcome_from_completion below. See that
        # function's docstring (issue #510) for why this isn't unified
        # further.
        from lib.dispatch import dispatch_import_from_db

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

    if job.job_type == IMPORT_JOB_YOUTUBE:
        return execute_youtube_import_job(db, job, ctx=ctx)

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


def _dispatch_outcome_from_completion(
    result: CompletionResult,
    *,
    deferred_message: str,
    completed_message: str,
    failed_message: str,
) -> DispatchOutcome:
    """Map the completion-processing tag to the queue's DispatchOutcome.

    Both ``execute_automation_import_job`` and ``execute_youtube_import_job``
    drive the same completion-processing protocol (issue #474) and need to
    report the same four outcomes back to the importer queue; this is the
    single conversion so the two callers don't duplicate the match.

    FORCE import jobs deliberately do NOT route through this mapper (issue
    #510 considered and rejected folding all three job types in here): they
    never produce a ``CompletionResult`` at all. ``execute_import_job`` sends
    them straight to
    ``dispatch_import_from_db`` -> ``dispatch_import_core`` — a
    structurally different decision tree (manifest guard, evidence gate,
    quality gate) that already returns ``DispatchOutcome`` directly from
    many branches. Routing them through here would mean wrapping that
    already-terminal ``DispatchOutcome`` in a synthetic completion tag
    just to unwrap it again a line later — ceremony, not dedup. The
    mapper that DOES unify all three job types is one layer up:
    ``process_claimed_job`` (+ ``_job_result``) converts any
    ``DispatchOutcome`` — regardless of which job-type executor produced
    it — into the ``ImportJob``'s terminal queue status.
    """
    if isinstance(result, CompletionDeferred):
        return DispatchOutcome(
            success=False,
            message=deferred_message,
            deferred=True,
        )
    if isinstance(result, CompletionDispatched):
        return result.outcome
    if isinstance(result, Completed):
        return DispatchOutcome(
            success=True,
            message=completed_message,
            terminal_outcome=result.terminal_outcome,
        )
    if isinstance(result, CompletionFailed):
        return DispatchOutcome(
            success=False,
            message=failed_message,
            terminal_outcome=result.terminal_outcome,
        )
    assert_never(result)


def execute_automation_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    process_album_fn: ProcessAlbumFn | None = None,
) -> DispatchOutcome:
    """Run completed-download processing from an automation queue job."""
    from lib.download import _run_completed_processing
    from lib.download_reconstruction import reconstruct_grab_list_entry

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
    state = ActiveDownloadState.from_raw(raw_state)
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
            process_album_fn=process_album_fn,
            bundle_terminal_outcome=True,
        )
    finally:
        if created_ctx:
            runtime_ctx.pipeline_db_source.close()
    return _dispatch_outcome_from_completion(
        result,
        deferred_message=(
            "Automation import was deferred or requires manual recovery"
        ),
        completed_message="Automation import processing completed",
        failed_message="Automation import processing failed",
    )


def execute_youtube_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
) -> DispatchOutcome:
    """Run completed-staging processing for a YouTube-rescue import job.

    Mirrors ``execute_automation_import_job`` structurally but sources the
    staged path from ``job.payload['staged_path']`` (decoded via
    ``msgspec.convert(...)`` into ``YoutubeImportPayload`` at the wire
    boundary) rather than from ``album_requests.active_download_state``.

    KTD1: this path never reads from nor writes to ``active_download_state``.
    The YT staged dir lives under ``/Incoming/auto-import`` already (the
    U6 worker stages it there directly), so the downstream pipeline
    observes a ready local staging path with no slskd-resume state
    attached.

    R17: terminal status flips run through
    ``transitions.finalize_request → mark_imported_with_rescue`` (the
    single source-agnostic write site), so YT rescues populate
    ``rescued_at`` + ``prior_unfindable_category`` atomically when the
    request had a prior ``unfindable_category``.

    No cooldown side effects: the slskd cooldown machinery is keyed on
    peer usernames; YT has no peers. We never call ``denylist_user`` /
    ``update_user_failure_count`` / ``check_and_apply_cooldown``. The
    synthetic ``ActiveDownloadState`` we build uses blank usernames for
    the staged audio manifest, so the rejection paths inside
    ``_handle_rejected_result`` find no peers to denylist.
    """
    from lib.download_reconstruction import reconstruct_grab_list_entry
    from lib.download_processing import process_completed_album

    request_id = job.request_id
    if request_id is None:
        return DispatchOutcome(False, "YouTube import job has no request_id")

    try:
        payload = msgspec.convert(job.payload, type=YoutubeImportPayload)
    except msgspec.ValidationError as exc:
        # Malformed payload — surface as a failed DispatchOutcome rather
        # than crashing the worker. The orphan/retry machinery is the
        # importer's existing concern; we just refuse to act on garbage.
        logger.error(
            "YouTube import job %s payload validation failed: %s",
            job.id,
            exc,
        )
        return DispatchOutcome(
            success=False,
            message=f"YouTube import payload is malformed: {exc}",
        )

    row = db.get_request(request_id)
    if not row:
        return DispatchOutcome(False, f"Album request {request_id} not found")
    status = str(row.get("status") or "")
    if status not in YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES:
        return DispatchOutcome(
            False,
            (
                f"Album request {request_id} is status {status!r}; "
                "YouTube import requires wanted/unsearchable"
            ),
            post_commit_cleanup=PostCommitCleanup(
                staged_path=payload.staged_path,
            ),
        )

    staged_files = _youtube_active_download_files(payload.staged_path)

    # Synthetic ActiveDownloadState — used ONLY to feed
    # reconstruct_grab_list_entry. Files are a manifest bridge for the
    # already-staged yt-dlp audio; current_path = the payload's staged
    # path. This struct is never persisted: KTD1 keeps
    # active_download_state untouched on the row, and the downstream
    # update_download_state_current_path call inside
    # _materialize_processing_dir is gated by status='downloading' so
    # it no-ops for wanted/unsearchable rows.
    state = ActiveDownloadState(
        filetype=row.get("target_format") or "opus",
        enqueued_at="",
        last_progress_at="",
        files=staged_files,
        current_path=payload.staged_path,
    )
    entry = reconstruct_grab_list_entry(row, state)
    entry.import_folder = payload.staged_path

    created_ctx = ctx is None
    runtime_ctx = ctx or _build_runtime_context(db)
    try:
        result = process_completed_album(
            entry,
            runtime_ctx,
            import_job_id=job.id,
        )
    finally:
        if created_ctx:
            runtime_ctx.pipeline_db_source.close()

    return _dispatch_outcome_from_completion(
        result,
        deferred_message=(
            "YouTube import was deferred or requires manual recovery"
        ),
        completed_message="YouTube import processing completed",
        failed_message="YouTube import processing failed",
    )


def _youtube_active_download_files(staged_path: str) -> list[ActiveDownloadFileState]:
    """Build the manifest bridge for a YT-staged album directory."""
    out: list[ActiveDownloadFileState] = []
    for rel_path in audio_relative_paths(staged_path):
        full_path = os.path.join(staged_path, rel_path)
        try:
            size = os.path.getsize(full_path)
        except OSError:
            size = 0
        out.append(ActiveDownloadFileState(
            username="",
            filename=rel_path,
            file_dir=os.path.dirname(rel_path),
            size=size,
        ))
    return out


def _cleanup_committed_wrong_match_rejection(
    db: PipelineDB,
    job: ImportJob,
    download_log_id: int,
    scenario: str | None,
) -> None:
    """Run Wrong Matches convergence only after the terminal bundle commits."""
    from lib.wrong_match_policy import rejection_scenario_is_wrong_match_candidate

    if not rejection_scenario_is_wrong_match_candidate(scenario):
        return
    try:
        from lib.wrong_match_cleanup_service import cleanup_wrong_match

        evidence_id = db.get_import_job_candidate_evidence_id(job.id)
        if evidence_id is not None:
            db.set_download_log_candidate_evidence(download_log_id, evidence_id)
        cleanup_wrong_match(
            db,
            download_log_id,
            ignore_import_job_id=job.id,
        )
    except Exception:
        logger.exception(
            "WRONG-MATCH CLEANUP FAILED after terminal commit: download_log_id=%s",
            download_log_id,
        )


def process_claimed_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
) -> ImportJob | None:
    """Execute a claimed job and persist its terminal queue status.

    This is the single queue-outcome mapper all three job types (automation,
    force, youtube) route through: whichever job-type executor
    produced ``outcome``, the success/requeue/failure -> terminal
    ``ImportJob`` status conversion below is one shared path (see
    ``_dispatch_outcome_from_completion``'s docstring for why the
    completion-result -> DispatchOutcome conversion is instead scoped to
    just automation + youtube, issue #510).
    """
    try:
        outcome = execute_fn(db, job, ctx=ctx)
    except Exception as exc:
        logger.exception("Import job %s crashed", job.id)
        recovery = db.mark_import_job_recovery_required(
            job.id,
            reason=f"{type(exc).__name__}: {exc}",
        )
        if recovery is not None:
            return recovery
        return db.mark_import_job_failed(
            job.id,
            error=type(exc).__name__,
            message=str(exc),
            result={"success": False},
        )

    result = _job_result(outcome)
    if outcome.success:
        if outcome.terminal_outcome is not None:
            terminal = db.persist_import_terminal_outcome(
                outcome.terminal_outcome.with_job(ImportJobTerminal(
                    status="completed",
                    result=result,
                    message=outcome.message,
                ))
            )
            terminal_job = terminal.job
            post_commit_cleanup = _run_post_commit_cleanup(outcome)
            if post_commit_cleanup is not None:
                merged = db.merge_import_job_result(
                    job.id,
                    {"post_commit_cleanup": post_commit_cleanup},
                )
                if merged is not None:
                    terminal_job = merged
            dismissal = _dismiss_successful_force_import(db, job)
            if dismissal is not None:
                merged = db.merge_import_job_result(
                    job.id,
                    {"wrong_match_dismissal": dismissal},
                )
                if merged is not None:
                    terminal_job = merged
            _cleanup_committed_wrong_match_rejection(
                db,
                job,
                terminal.download_log_id,
                outcome.post_commit_wrong_match_scenario,
            )
            return terminal_job
        recovery = db.mark_import_job_recovery_required(
            job.id,
            reason="Beets returned without a terminal acknowledgement bundle",
        )
        if recovery is not None:
            return recovery
        completed = db.mark_import_job_completed(
            job.id,
            result=result,
            message=outcome.message,
        )
        if completed is None:
            return None
        dismissal = _dismiss_successful_force_import(db, job)
        if dismissal is not None:
            return db.merge_import_job_result(
                job.id,
                {"wrong_match_dismissal": dismissal},
            ) or completed
        return completed
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
        recovery = db.mark_import_job_recovery_required(
            job.id,
            reason=f"requeue-to-preview failed after launch: {outcome.message}",
        )
        if recovery is not None:
            return recovery
        return db.mark_import_job_failed(
            job.id,
            error=outcome.message,
            message=f"requeue-to-preview failed: {outcome.message}",
            result=result,
        )
    if outcome.terminal_outcome is not None:
        terminal = db.persist_import_terminal_outcome(
            outcome.terminal_outcome.with_job(ImportJobTerminal(
                status="failed",
                error=outcome.message,
                result=result,
                message=outcome.message,
            ))
        )
        terminal_job = terminal.job
        post_commit_cleanup = _run_post_commit_cleanup(outcome)
        if post_commit_cleanup is not None:
            merged = db.merge_import_job_result(
                job.id,
                {"post_commit_cleanup": post_commit_cleanup},
            )
            if merged is not None:
                terminal_job = merged
        cleanup = _cleanup_failed_force_import(db, job, outcome)
        if cleanup is not None:
            merged = db.merge_import_job_result(job.id, {"cleanup": cleanup})
            if merged is not None:
                terminal_job = merged
        _cleanup_committed_wrong_match_rejection(
            db,
            job,
            terminal.download_log_id,
            outcome.post_commit_wrong_match_scenario,
        )
        return terminal_job
    recovery = db.mark_import_job_recovery_required(
        job.id,
        reason="Beets returned without a terminal acknowledgement bundle",
    )
    if recovery is not None:
        return recovery
    failed = db.mark_import_job_failed(
        job.id,
        error=outcome.message,
        message=outcome.message,
        result=result,
    )
    if failed is None:
        return None
    terminal_job = failed
    post_commit_cleanup = _run_post_commit_cleanup(outcome)
    if post_commit_cleanup is not None:
        merged = db.merge_import_job_result(
            job.id,
            {"post_commit_cleanup": post_commit_cleanup},
        )
        if merged is not None:
            terminal_job = merged
    cleanup = _cleanup_failed_force_import(db, job, outcome)
    if cleanup is not None:
        return db.merge_import_job_result(
            job.id,
            {"cleanup": cleanup},
        ) or terminal_job
    return terminal_job


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
    """Retry only unlaunched jobs; stop ambiguous Beets work for recovery."""
    recovered: list[ImportJob] = []
    batch_size = 50
    while True:
        batch = db.recover_running_import_jobs(
            requeue_message=RESTART_REQUEUE_MESSAGE,
            recovery_message=RESTART_RECOVERY_MESSAGE,
            limit=batch_size,
        )
        recovered.extend(batch)
        if len(batch) < batch_size:
            return recovered


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
                    "Recovered %s abandoned running import job(s)",
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
