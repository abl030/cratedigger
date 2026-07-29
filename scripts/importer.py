"""Drain the shared import queue through one beets-mutating lane."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import time
from collections.abc import Callable
from dataclasses import replace
from typing import Any, Protocol, assert_never

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import msgspec

from lib import transitions
from lib.dispatch import (
    DISPATCH_CODE_REQUEUE_FAILED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    DispatchOutcome,
    _requeue_import_job_to_preview,
)
from lib.dispatch.types import (
    PostCommitCleanup,
    PostCommitQuarantineAudit,
)
from lib.download_processing import (
    Completed,
    CompletionDeferred,
    CompletionDispatched,
    CompletionFailed,
    CompletionResult,
    ProcessAlbumFn,
)
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    ExecutionLivenessProbe,
    OwnerSessionIdentity,
    ProcessIdentity,
    capture_execution_lease,
    checkpoint_automation_owner,
    probe_execution_liveness,
)
from lib.import_manifest import audio_relative_paths
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    ForceImportPayload,
    ImportJob,
    YoutubeImportPayload,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_IMPORTER,
    DEFAULT_DSN,
    CleanupJournalIntent,
    CleanupJournalReceipt,
    PipelineDB,
)
from lib.processing_cleanup import (
    PROCESSING_CLEANUP_NO_OP,
    PROCESSING_CLEANUP_QUARANTINE_SOURCE,
    PROCESSING_CLEANUP_REMOVE_SOURCE,
    cleanup_manifest_builtins,
    cleanup_manifest_hash,
    execute_processing_cleanup,
    inspect_processing_cleanup_source,
)
from lib.quality import ActiveDownloadFileState, ActiveDownloadState
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    ImportJobTerminal,
    PendingImportTerminalOutcome,
)
from lib.youtube_ingest_service import (
    YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES,
)

logger = logging.getLogger("cratedigger-importer")
RESTART_REQUEUE_MESSAGE = "Importer restarted while job was running; retry queued"
RESTART_RECOVERY_MESSAGE = (
    "Recovery required: importer restarted after Beets launch authorization"
)
IMPORTER_SYSTEMD_UNIT = "cratedigger-importer.service"


def _job_result(outcome: DispatchOutcome) -> dict[str, Any]:
    return {
        "success": outcome.success,
        "message": outcome.message,
        "deferred": outcome.deferred,
        "code": outcome.code,
    }


class _PostCommitCleanupDB(Protocol):
    """Narrow persistence seam used after the terminal transaction."""

    def record_post_commit_quarantine(
        self,
        log_id: int,
        audit: PostCommitQuarantineAudit,
    ) -> bool: ...


def _run_post_commit_cleanup(
    db: _PostCommitCleanupDB,
    outcome: DispatchOutcome,
    *,
    download_log_id: int | None = None,
) -> dict[str, object] | None:
    """Run narrow convergence only after terminal acknowledgement."""
    plan = outcome.post_commit_cleanup
    if plan is None:
        return None

    details: dict[str, object] = {}
    if plan.audio_quarantine_source_path is not None:
        if download_log_id is None:
            details["audio_quarantine"] = {
                "source_path": plan.audio_quarantine_source_path,
                "moved": False,
                "error": (
                    "terminal download_log id unavailable; source retained "
                    "at staging"
                ),
            }
        else:
            from lib.dispatch.quarantine import (
                quarantine_corrupt_audio_source,
            )

            audit = quarantine_corrupt_audio_source(
                source_path=plan.audio_quarantine_source_path,
                quarantine_root=plan.audio_quarantine_root or "",
            )
            audit_payload = msgspec.to_builtins(audit)
            assert isinstance(audit_payload, dict)
            try:
                audit_persisted = db.record_post_commit_quarantine(
                    download_log_id,
                    audit,
                )
            except Exception as exc:
                logger.exception(
                    "Failed to persist post-commit audio quarantine audit"
                )
                audit_payload["audit_persisted"] = False
                audit_payload["audit_error"] = (
                    f"{type(exc).__name__}: {exc}"
                )[:1024]
            else:
                audit_payload["audit_persisted"] = audit_persisted
            details["audio_quarantine"] = audit_payload

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
        except Exception as exc:
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
        except Exception as exc:
            logger.exception("Post-commit staged-path cleanup failed")
            details["staged_path"] = {
                "path": plan.staged_path,
                "success": False,
                "error": f"{type(exc).__name__}: {exc}",
            }

    return details or None


def _select_missing_destination(path: str, *, separator: str) -> str:
    candidate = os.path.abspath(path)
    index = 1 if separator == "_" else 2
    while os.path.lexists(candidate):
        candidate = f"{os.path.abspath(path)}{separator}{index}"
        index += 1
    return candidate


def _automation_completion_from_job(job: ImportJob):
    from lib.import_job_recovery_service import (
        AUTOMATION_COMPLETION_RESULT_KEY,
        AutomationCompletionReceipt,
        automation_completion_result_patch,
    )

    raw = (
        None
        if job.result is None
        else job.result.get(AUTOMATION_COMPLETION_RESULT_KEY)
    )
    if raw is None:
        return None
    receipt = msgspec.convert(
        raw,
        type=AutomationCompletionReceipt,
        strict=True,
    )
    automation_completion_result_patch(receipt)
    return receipt


def _automation_cleanup_intent(
    *,
    source_path: str,
    plan: PostCommitCleanup | None,
) -> CleanupJournalIntent:
    inspection = inspect_processing_cleanup_source(source_path)
    if inspection.status == "uninspectable":
        raise RuntimeError(
            "processor cleanup source cannot be inspected: "
            f"{inspection.reason}"
        )
    if inspection.status == "missing":
        empty_manifest = ()
        return CleanupJournalIntent(
            action=PROCESSING_CLEANUP_NO_OP,
            source_path=source_path,
            source_manifest=cleanup_manifest_builtins(empty_manifest),
            source_manifest_hash=cleanup_manifest_hash(empty_manifest),
        )
    assert inspection.manifest_hash is not None
    manifest = cleanup_manifest_builtins(inspection.manifest)

    if (
        plan is not None
        and plan.audio_quarantine_source_path is not None
    ):
        if os.path.abspath(plan.audio_quarantine_source_path) != source_path:
            raise RuntimeError(
                "audio quarantine plan does not name the canonical owner path"
            )
        quarantine_root = os.path.abspath(plan.audio_quarantine_root or "")
        base = os.path.join(
            quarantine_root,
            "failed_imports",
            "bad_files",
            os.path.basename(source_path),
        )
        destination = _select_missing_destination(base, separator="_")
        return CleanupJournalIntent(
            action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
            source_path=source_path,
            source_manifest=manifest,
            source_manifest_hash=inspection.manifest_hash,
            destination_path=destination,
            destination_manifest=manifest,
            destination_manifest_hash=inspection.manifest_hash,
            selected_destination_path=destination,
        )

    if (
        plan is not None
        and plan.duplicate_guard_source_path is not None
    ):
        if os.path.abspath(plan.duplicate_guard_source_path) != source_path:
            raise RuntimeError(
                "duplicate guard plan does not name the canonical owner path"
            )
        from lib.processing_paths import duplicate_remove_guard_path

        base = duplicate_remove_guard_path(
            staging_dir=plan.duplicate_guard_staging_dir or "",
            source_path=source_path,
            request_id=plan.duplicate_guard_request_id,
        )
        destination = _select_missing_destination(base, separator="-")
        return CleanupJournalIntent(
            action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
            source_path=source_path,
            source_manifest=manifest,
            source_manifest_hash=inspection.manifest_hash,
            destination_path=destination,
            destination_manifest=manifest,
            destination_manifest_hash=inspection.manifest_hash,
            selected_destination_path=destination,
        )

    if (
        plan is not None
        and plan.staged_path is not None
        and os.path.abspath(plan.staged_path) != source_path
    ):
        raise RuntimeError(
            "staged cleanup plan does not name the canonical owner path"
        )
    return CleanupJournalIntent(
        action=PROCESSING_CLEANUP_REMOVE_SOURCE,
        source_path=source_path,
        source_manifest=manifest,
        source_manifest_hash=inspection.manifest_hash,
    )


def _complete_automation_processing_cleanup(
    db: Any,
    job: ImportJob,
    outcome: DispatchOutcome,
    *,
    execution_lease: ExecutionLeaseSnapshot,
    cancellation_token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
) -> CleanupJournalReceipt:
    if job.request_id is None:
        raise RuntimeError("automation cleanup job has no request")
    request = db.get_request(job.request_id)
    if request is None or request.get("status") != "processing":
        raise RuntimeError("automation cleanup request is no longer processing")
    raw_state = request.get("active_download_state")
    if raw_state is None:
        raise RuntimeError("automation cleanup request lost active state")
    state = ActiveDownloadState.from_raw(raw_state)
    if state.current_path is None:
        raise RuntimeError("automation cleanup request has no canonical path")
    source_path = os.path.abspath(state.current_path)

    def checkpoint() -> None:
        checkpoint_automation_owner(
            db,
            import_job_id=job.id,
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )

    checkpoint()
    journal = db.get_processing_cleanup_journal(
        request_id=job.request_id,
        job_id=job.id,
    )
    if journal is None:
        intent = _automation_cleanup_intent(
            source_path=source_path,
            plan=outcome.post_commit_cleanup,
        )
        checkpoint()
        journal = db.create_processing_cleanup_journal(
            request_id=job.request_id,
            job_id=job.id,
            intent=intent,
        )
    elif os.path.abspath(journal["source_path"]) != source_path:
        raise RuntimeError(
            "automation cleanup journal changed canonical source path"
        )
    completed = execute_processing_cleanup(
        db,
        journal,
        owner_checkpoint=checkpoint,
    )
    receipt = completed["completed_receipt"]
    if receipt is None:
        raise RuntimeError("automation cleanup completed without a receipt")
    return receipt


def _force_job_wrong_match_payload(job: ImportJob) -> tuple[int, str | None] | None:
    if job.job_type != IMPORT_JOB_FORCE:
        return None
    if not isinstance(job.payload, ForceImportPayload):
        raise TypeError("force_import payload type mismatch")
    return job.payload.download_log_id, job.payload.failed_path


def _force_action_path(job: ImportJob) -> str | None:
    """Return the retained private copy selected by this preview, if any."""
    preview = job.preview_result
    value = preview.get("action_path") if preview is not None else None
    return value if isinstance(value, str) and value else None


def _cleanup_terminal_force_action(job: ImportJob) -> dict[str, object] | None:
    """Best-effort cleanup after a terminal force outcome.

    The action copy must outlive Beets, but it is disposable once no launch is
    pending.  Cleanup is deliberately reported after the real outcome rather
    than allowed to replace it.
    """
    action_path = _force_action_path(job)
    if action_path is None:
        return None
    try:
        from lib.config import read_runtime_config
        from lib.import_preview import cleanup_force_action_copy_for_job

        cfg = read_runtime_config()
        cleanup_force_action_copy_for_job(action_path, cfg, import_job_id=job.id)
        return {"action_path": action_path, "removed": True}
    except Exception as exc:
        logger.exception("Failed to remove retained force action copy %s", action_path)
        return {
            "action_path": action_path,
            "removed": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _record_terminal_force_action_cleanup(
    db: PipelineDB,
    job: ImportJob,
    terminal_job: ImportJob | None,
) -> ImportJob | None:
    cleanup = _cleanup_terminal_force_action(job)
    if cleanup is None:
        return terminal_job
    try:
        merged = db.merge_import_job_result(
            job.id, {"force_action_cleanup": cleanup},
        )
    except Exception:  # terminal acknowledgement must remain authoritative
        logger.exception(
            "Failed to record retained force action cleanup for job %s", job.id,
        )
        return terminal_job
    return merged or terminal_job


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
    cleanup_plan = outcome.post_commit_cleanup
    if (
        cleanup_plan is not None
        and cleanup_plan.audio_quarantine_source_path is not None
    ):
        # Corrupt candidates are archival evidence. The post-commit
        # quarantine either moved the source or left it in place and recorded
        # why; both states must bypass Wrong Matches deletion.
        return {
            "success": True,
            "download_log_id": download_log_id,
            "failed_path_hint": failed_path_hint,
            "outcome": "skipped_archival_audio_quarantine",
            "skipped": True,
            "dispatch_code": outcome.code,
            "dispatch_message": outcome.message,
        }
    # The original force/quarantine directory is operator authority and audit
    # evidence. Dispatch consumes only the private action copy; cleanup of the
    # raw source requires a distinct operator action, never a quality result.
    return {
        "success": True,
        "download_log_id": download_log_id,
        "failed_path_hint": failed_path_hint,
        "outcome": "preserved_operator_force_source",
        "skipped": True,
        "dispatch_code": outcome.code,
        "dispatch_message": outcome.message,
    }


def _dismiss_successful_force_import(
    db: PipelineDB,
    job: ImportJob,
) -> dict[str, object] | None:
    """Remove a successfully imported source from Wrong Matches, never disk.

    This runs only after the terminal acknowledgement.  The raw quarantine
    directory remains operator evidence; the dismissed pointers only stop it
    appearing as an actionable Wrong Matches entry.
    """
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
            "Failed to dismiss successful force import source for job %s",
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
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
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

        if not isinstance(job.payload, ForceImportPayload):
            raise AssertionError("force_import payload type mismatch")
        payload = job.payload
        from lib.config import read_runtime_config
        from lib.import_preview import force_action_copy_path

        action_path = _force_action_path(job)
        expected_action_path = force_action_copy_path(read_runtime_config(), job.id)
        if (
            action_path is None
            or action_path != expected_action_path
            or not os.path.isdir(action_path)
        ):
            return _requeue_import_job_to_preview(
                db,
                import_job_id=job.id,
                reason="force action copy unavailable; preview must rebuild it",
            )
        return dispatch_import_from_db(
            db,
            request_id=job.request_id,
            failed_path=action_path,
            source_reference_path=payload.failed_path,
            source_username=payload.source_username,
            source_dirs=(
                [source_dir for source_dir in payload.source_dirs if source_dir]
                or None
            ),
            import_job_id=job.id,
            download_log_id=payload.download_log_id,
        )

    if job.job_type == IMPORT_JOB_AUTOMATION:
        return execute_automation_import_job(
            db,
            job,
            ctx=ctx,
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )

    if job.job_type == IMPORT_JOB_YOUTUBE:
        return execute_youtube_import_job(db, job, ctx=ctx)

    return DispatchOutcome(
        success=False,
        message=f"Unsupported import job type: {job.job_type}",
    )


def _build_runtime_context(
    db: PipelineDB,
    *,
    borrow_session: bool = False,
):
    """Build the minimal CratediggerContext needed by download processing."""
    from album_source import DatabaseSource
    from lib.config import read_runtime_config
    from lib.context import CratediggerContext
    from web.api_bases import mb_ws2_base

    cfg = read_runtime_config()
    source = DatabaseSource(
        db.dsn,
        musicbrainz_ws2_base=mb_ws2_base(cfg.musicbrainz_api_base),
        discogs_api_base=cfg.discogs_api_base,
        borrowed_db=db if borrow_session else None,
    )
    return CratediggerContext(cfg=cfg, slskd=None, pipeline_db_source=source)


def _dispatch_outcome_from_completion(
    result: CompletionResult,
    *,
    deferred_message: str,
    completed_message: str,
    failed_message: str,
    fallback_terminal_outcome: PendingImportTerminalOutcome | None = None,
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

    Policy (issue #859): a deferred attempt REMAINS a terminal failed job
    carrying the honest ``result.detail`` (e.g. "incomplete_or_unsafe_canonical")
    appended to ``deferred_message`` — never a generic message that hides
    the diagnostic. Retry ownership stays with the poll cycle, which
    re-enqueues on the next cycle; requeueing inside the serial importer
    drain here would risk a hot loop.
    """
    if isinstance(result, CompletionDeferred):
        message = deferred_message
        if result.detail:
            message = f"{deferred_message}: {result.detail}"
        return DispatchOutcome(
            success=False,
            message=message,
            deferred=True,
            terminal_outcome=fallback_terminal_outcome,
        )
    if isinstance(result, CompletionDispatched):
        return result.outcome
    if isinstance(result, Completed):
        return DispatchOutcome(
            success=True,
            message=completed_message,
            terminal_outcome=(
                result.terminal_outcome or fallback_terminal_outcome
            ),
        )
    match result:
        case CompletionFailed():
            return DispatchOutcome(
                success=False,
                message=(
                    f"{failed_message}: {result.reason}"
                    if result.reason else failed_message
                ),
                terminal_outcome=(
                    result.terminal_outcome or fallback_terminal_outcome
                ),
            )
    assert_never(result)


def execute_automation_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    process_album_fn: ProcessAlbumFn | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
) -> DispatchOutcome:
    """Run completed-download processing from an automation queue job."""
    from lib.download import (
        _local_completion_terminal_outcome,
        _run_completed_processing,
    )
    from lib.download_reconstruction import reconstruct_grab_list_entry

    request_id = job.request_id
    if request_id is None:
        return DispatchOutcome(False, "Automation import job has no request_id")

    if (
        execution_lease is None
        or cancellation_token is None
        or owner_session_identity is None
    ):
        raise ValueError(
            "automation execution requires lease, cancellation token, "
            "and pinned owner session"
        )
    cancellation_token.raise_if_cancelled()
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
    runtime_ctx = ctx or _build_runtime_context(db, borrow_session=True)
    try:
        result = _run_completed_processing(
            entry,
            state,
            runtime_ctx,
            import_job_id=job.id,
            process_album_fn=process_album_fn,
            cancellation_token=cancellation_token,
            execution_lease=execution_lease,
            owner_session_identity=owner_session_identity,
        )
    finally:
        if created_ctx:
            runtime_ctx.pipeline_db_source.close()
    fallback_terminal: PendingImportTerminalOutcome | None = None
    if isinstance(result, Completed) and result.terminal_outcome is None:
        fallback_terminal = _local_completion_terminal_outcome(
            entry,
            state,
            request_id=request_id,
            import_job_id=job.id,
            transition=transitions.RequestTransition.to_imported(
                from_status="processing",
            ),
            outcome="success",
            detail="Local automation import processing completed",
        )
    elif isinstance(result, (CompletionDeferred, CompletionFailed)):
        detail = (
            result.detail
            if isinstance(result, CompletionDeferred)
            else result.reason
        )
        fallback_terminal = _local_completion_terminal_outcome(
            entry,
            state,
            request_id=request_id,
            import_job_id=job.id,
            transition=transitions.RequestTransition.to_wanted(
                from_status="processing",
                attempt_type="download",
            ),
            outcome="failed",
            detail=detail or "local automation processing failed",
            error_message=detail or "local automation processing failed",
        )
    elif (
        isinstance(result, CompletionDispatched)
        and result.outcome.terminal_outcome is None
        and result.outcome.code != DISPATCH_CODE_REQUEUED_FOR_PREVIEW
    ):
        fallback_terminal = _local_completion_terminal_outcome(
            entry,
            state,
            request_id=request_id,
            import_job_id=job.id,
            transition=(
                transitions.RequestTransition.to_imported(
                    from_status="processing",
                )
                if result.outcome.success
                else transitions.RequestTransition.to_wanted(
                    from_status="processing",
                    attempt_type="validation",
                )
            ),
            outcome="success" if result.outcome.success else "failed",
            detail=result.outcome.message,
            error_message=(
                None if result.outcome.success else result.outcome.message
            ),
        )
        result = CompletionDispatched(outcome=replace(
            result.outcome,
            terminal_outcome=fallback_terminal,
        ))
    return _dispatch_outcome_from_completion(
        result,
        deferred_message=(
            "Automation import was deferred or requires manual recovery"
        ),
        completed_message="Automation import processing completed",
        failed_message="Automation import processing failed",
        fallback_terminal_outcome=fallback_terminal,
    )


def execute_youtube_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
) -> DispatchOutcome:
    """Run completed-staging processing for a YouTube-rescue import job.

    Mirrors ``execute_automation_import_job`` structurally but sources the
    staged path from the typed payload decoded at ``ImportJob.from_row`` rather
    than from ``album_requests.active_download_state``.

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
    from lib.download_processing import process_completed_album
    from lib.download_reconstruction import reconstruct_grab_list_entry

    request_id = job.request_id
    if request_id is None:
        return DispatchOutcome(False, "YouTube import job has no request_id")

    if not isinstance(job.payload, YoutubeImportPayload):
        raise TypeError("youtube_import payload type mismatch")
    payload = job.payload

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
    # active_download_state untouched on the row.
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
    outcome: DispatchOutcome,
    *,
    cleanup_wrong_match_fn: Callable[..., object] | None = None,
) -> None:
    """Run Wrong Matches convergence only after the terminal bundle commits."""
    from lib.wrong_match_policy import rejection_scenario_is_wrong_match_candidate

    cleanup_plan = outcome.post_commit_cleanup
    archival_quarantine = (
        cleanup_plan is not None
        and cleanup_plan.audio_quarantine_source_path is not None
    )
    wrong_match_candidate = rejection_scenario_is_wrong_match_candidate(
        outcome.post_commit_wrong_match_scenario
    )
    if not archival_quarantine and not wrong_match_candidate:
        return
    try:
        evidence_id = db.get_import_job_candidate_evidence_id(job.id)
        if evidence_id is not None:
            db.set_download_log_candidate_evidence(download_log_id, evidence_id)
        if archival_quarantine:
            # The source is now protected archival evidence, whether
            # quarantine moved it or failed closed at the original path.
            # Never hand either location to the independent Wrong Matches
            # deletion reducer.
            return
        if cleanup_wrong_match_fn is None:
            from lib.wrong_match_cleanup_service import cleanup_wrong_match

            cleanup_wrong_match_fn = cleanup_wrong_match

        cleanup_wrong_match_fn(
            db,
            download_log_id,
            ignore_import_job_id=job.id,
        )
    except Exception:
        logger.exception(
            "WRONG-MATCH CLEANUP FAILED after terminal commit: download_log_id=%s",
            download_log_id,
        )


def _execution_lease_from_job(
    job: ImportJob | None,
) -> ExecutionLeaseSnapshot | None:
    if job is None:
        return None
    values = (
        job.execution_invocation_id,
        job.execution_host_boot_id,
        job.execution_systemd_unit,
        job.execution_worker_pid,
        job.execution_worker_start_ticks,
    )
    if any(value is None for value in values):
        return None
    assert job.execution_invocation_id is not None
    assert job.execution_host_boot_id is not None
    assert job.execution_systemd_unit is not None
    assert job.execution_worker_pid is not None
    assert job.execution_worker_start_ticks is not None
    child = (
        ProcessIdentity(
            job.execution_beets_pid,
            job.execution_beets_start_ticks,
        )
        if (
            job.execution_beets_pid is not None
            and job.execution_beets_start_ticks is not None
        )
        else None
    )
    return ExecutionLeaseSnapshot(
        host_boot_id=job.execution_host_boot_id,
        invocation_id=job.execution_invocation_id,
        systemd_unit=job.execution_systemd_unit,
        worker=ProcessIdentity(
            job.execution_worker_pid,
            job.execution_worker_start_ticks,
        ),
        beets=child,
    )


def _automation_claim_is_current(
    db: Any,
    job: ImportJob,
    execution_lease: ExecutionLeaseSnapshot,
) -> bool:
    if job.request_id is None:
        return False
    current = db.get_import_job(job.id)
    try:
        request = db.get_request(job.request_id)
    except RuntimeError:
        # The joined processing-owner projection deliberately rejects a
        # dangling/mismatched owner pointer. At this boundary that is stale
        # authority, not permission to continue or an importer crash.
        return False
    return bool(
        current is not None
        and request is not None
        and current.id == job.id
        and current.request_id == job.request_id
        and current.job_type == IMPORT_JOB_AUTOMATION
        and current.status == "running"
        and current.preview_status == "evidence_ready"
        and current.beets_launch_authorized_at is None
        and _execution_lease_from_job(current) == execution_lease
        and request.get("status") == "processing"
        and request.get("active_automation_import_job_id") == job.id
        and request.get("active_download_state") is not None
    )


def _process_automation_claim(
    job: ImportJob,
    *,
    dsn: str,
    execution_lease: ExecutionLeaseSnapshot,
    ctx: Any,
    stage_db_factory: Callable[[str], Any],
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
) -> ImportJob | None:
    """Run one automation importer under its pinned IMPORT owner session."""
    if job.request_id is None:
        return None
    stage_db = stage_db_factory(dsn)
    token = CancellationToken()
    try:
        # Pin first. Acquiring IMPORT before pinning could reconnect between
        # scopes and leave the pinned backend without the authority lock.
        with stage_db._pin_owner_session(token) as owner_session_identity:
            token.raise_if_cancelled()
            with stage_db.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                job.request_id,
            ) as acquired:
                token.raise_if_cancelled()
                if not acquired:
                    # A failed lock attempt grants no lifecycle authority.
                    # Preserve the exact running owner for its live holder or
                    # the startup death-proof recovery path; never mutate it
                    # from outside IMPORT.
                    return None
                if not _automation_claim_is_current(
                    stage_db,
                    job,
                    execution_lease,
                ):
                    return None
                return process_claimed_job(
                    stage_db,
                    job,
                    ctx=ctx,
                    execute_fn=execute_fn,
                    execution_lease=execution_lease,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                )
    finally:
        stage_db.close()


def process_claimed_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
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
    is_automation = job.job_type == IMPORT_JOB_AUTOMATION
    if is_automation and (
        execution_lease is None
        or cancellation_token is None
        or owner_session_identity is None
    ):
        raise ValueError(
            "automation job processing requires exact execution authority"
        )
    try:
        if is_automation:
            outcome = execute_fn(
                db,
                job,
                ctx=ctx,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
        else:
            outcome = execute_fn(db, job, ctx=ctx)
    except ExecutionCancelled:
        raise
    except Exception as exc:
        logger.exception("Import job %s crashed", job.id)
        if is_automation:
            current = db.get_import_job(job.id)
            current_lease = (
                _execution_lease_from_job(current)
                if current is not None else None
            )
            if current_lease is None:
                return None
            assert current is not None
            if current.beets_launch_authorized_at is not None:
                return db.mark_import_job_recovery_required(
                    job.id,
                    reason=f"{type(exc).__name__}: {exc}",
                    expected_execution_lease=current_lease,
                )
            return db.requeue_import_job_for_preview(
                job.id,
                reason=f"{type(exc).__name__}: {exc}",
                expected_execution_lease=current_lease,
            )
        recovery = db.mark_import_job_recovery_required(
            job.id,
            reason=f"{type(exc).__name__}: {exc}",
        )
        if recovery is not None:
            return recovery
        failed = db.mark_import_job_failed(
            job.id,
            error=type(exc).__name__,
            message=str(exc),
            result={"success": False},
        )
        return _record_terminal_force_action_cleanup(db, job, failed)

    result = _job_result(outcome)
    if is_automation:
        assert execution_lease is not None
        assert cancellation_token is not None
        assert owner_session_identity is not None
        cancellation_token.raise_if_cancelled()
        if outcome.code == DISPATCH_CODE_REQUEUED_FOR_PREVIEW:
            return None
        current = db.get_import_job(job.id)
        current_lease = (
            _execution_lease_from_job(current)
            if current is not None else None
        )
        if current is None or current_lease is None:
            return None
        if (
            current.beets_launch_authorized_at is None
            and outcome.terminal_outcome is None
            and (
                outcome.deferred
                or outcome.code == DISPATCH_CODE_REQUEUE_FAILED
            )
        ):
            if outcome.code == DISPATCH_CODE_REQUEUE_FAILED:
                # The failed owner-aware CAS remains running for startup
                # recovery. A job-only terminal writer must never hide it.
                return None
            db.requeue_import_job_for_preview(
                job.id,
                reason=outcome.message,
                expected_execution_lease=current_lease,
            )
            return None
        if (
            current.beets_launch_authorized_at is not None
            and (
                outcome.code == "beets_acknowledgement_ambiguous"
                or outcome.terminal_outcome is None
            )
        ):
            return db.mark_import_job_recovery_required(
                job.id,
                reason=outcome.message,
                expected_execution_lease=current_lease,
            )
        if outcome.terminal_outcome is None:
            return db.mark_import_job_recovery_required(
                job.id,
                reason=(
                    "Automation processor returned no owner-atomic terminal "
                    "outcome"
                ),
                expected_execution_lease=current_lease,
            )
        cleanup_receipt = _complete_automation_processing_cleanup(
            db,
            current,
            outcome,
            execution_lease=current_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )
        completion_receipt = _automation_completion_from_job(current)
        if (
            current.beets_launch_authorized_at is not None
            and completion_receipt is None
        ):
            return db.mark_import_job_recovery_required(
                job.id,
                reason="Automation completion receipt is missing or invalid",
                expected_execution_lease=current_lease,
            )
        pending = replace(
            outcome.terminal_outcome,
            automation=AutomationTerminalAuthority(
                expected_job_status="running",
                expected_preview_status=current.preview_status,
                expected_execution_lease=current_lease,
                cleanup_receipt=cleanup_receipt,
                completion_receipt=completion_receipt,
            ),
        )
        terminal = db.persist_import_terminal_outcome(
            pending.with_job(ImportJobTerminal(
                status="completed" if outcome.success else "failed",
                error=None if outcome.success else outcome.message,
                result=result,
                message=outcome.message,
            ))
        )
        return terminal.job
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
            post_commit_cleanup = _run_post_commit_cleanup(
                db,
                outcome,
                download_log_id=terminal.download_log_id,
            )
            if post_commit_cleanup is not None:
                merged = db.merge_import_job_result(
                    job.id,
                    {"post_commit_cleanup": post_commit_cleanup},
                )
                if merged is not None:
                    terminal_job = merged
            if job.job_type != IMPORT_JOB_FORCE:
                _cleanup_committed_wrong_match_rejection(
                    db,
                    job,
                    terminal.download_log_id,
                    outcome,
                )
            dismissal = _dismiss_successful_force_import(db, job)
            if dismissal is not None:
                merged = db.merge_import_job_result(
                    job.id,
                    {"wrong_match_dismissal": dismissal},
                )
                if merged is not None:
                    terminal_job = merged
            return _record_terminal_force_action_cleanup(db, job, terminal_job)
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
            completed = db.merge_import_job_result(
                job.id,
                {"wrong_match_dismissal": dismissal},
            ) or completed
        return _record_terminal_force_action_cleanup(db, job, completed)
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
        # The preview claim now owns this deterministic action path.  It may
        # publish its fresh copy before this importer frame returns, so an old
        # importer must never reclaim it after the durable requeue.
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
        failed = db.mark_import_job_failed(
            job.id,
            error=outcome.message,
            message=f"requeue-to-preview failed: {outcome.message}",
            result=result,
        )
        return _record_terminal_force_action_cleanup(db, job, failed)
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
        post_commit_cleanup = _run_post_commit_cleanup(
            db,
            outcome,
            download_log_id=terminal.download_log_id,
        )
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
        if job.job_type != IMPORT_JOB_FORCE:
            _cleanup_committed_wrong_match_rejection(
                db,
                job,
                terminal.download_log_id,
                outcome,
            )
        return _record_terminal_force_action_cleanup(db, job, terminal_job)
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
    post_commit_cleanup = _run_post_commit_cleanup(db, outcome)
    if post_commit_cleanup is not None:
        merged = db.merge_import_job_result(
            job.id,
            {"post_commit_cleanup": post_commit_cleanup},
        )
        if merged is not None:
            terminal_job = merged
    cleanup = _cleanup_failed_force_import(db, job, outcome)
    if cleanup is not None:
        terminal_job = db.merge_import_job_result(
            job.id,
            {"cleanup": cleanup},
        ) or terminal_job
    return _record_terminal_force_action_cleanup(db, job, terminal_job)


def run_once(
    db: PipelineDB,
    *,
    worker_id: str,
    ctx: Any = None,
    stage_db_factory: Callable[[str], Any] | None = None,
    execution_lease_factory: Callable[..., ExecutionLeaseSnapshot] | None = None,
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
) -> ImportJob | None:
    capture = execution_lease_factory or capture_execution_lease
    try:
        execution_lease = capture(systemd_unit=IMPORTER_SYSTEMD_UNIT)
    except ValueError:
        # Non-systemd development runs may still process Force/YouTube jobs.
        # Automation stays invisible without a complete invocation lease.
        execution_lease = None
    job = db.claim_next_import_job(
        worker_id=worker_id,
        execution_lease=execution_lease,
    )
    if job is None:
        return None
    logger.info("Claimed import job %s (%s)", job.id, job.job_type)
    if job.job_type == IMPORT_JOB_AUTOMATION:
        if execution_lease is None:
            return None
        dsn = getattr(db, "dsn", None)
        if not dsn:
            return None
        return _process_automation_claim(
            job,
            dsn=str(dsn),
            execution_lease=execution_lease,
            ctx=ctx,
            stage_db_factory=stage_db_factory or PipelineDB,
            execute_fn=execute_fn,
        )
    return process_claimed_job(db, job, ctx=ctx, execute_fn=execute_fn)


def recover_abandoned_running_jobs(
    db: PipelineDB,
    *,
    liveness_probe: ExecutionLivenessProbe | None = None,
) -> list[ImportJob]:
    """Recover only executions whose exact persisted lease is proven dead."""
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
            break
    for job in db.list_automation_import_jobs_for_startup_recovery():
        if (
            job.status != "running"
        ):
            continue
        lease = _execution_lease_from_job(job)
        if lease is None:
            continue
        decision = probe_execution_liveness(
            lease,
            probe=liveness_probe,
        )
        recovered_job = db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=decision,
            requeue_message=RESTART_REQUEUE_MESSAGE,
            recovery_message=RESTART_RECOVERY_MESSAGE,
        )
        if recovered_job is not None:
            recovered.append(recovered_job)
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
