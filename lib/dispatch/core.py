"""Core import dispatch — the import_one.py orchestration state machine.

``dispatch_import_core`` is the funnel every import path (automatic and
force) runs through: acquire the RELEASE advisory lock, load evidence,
run the subprocess, and dispatch on the decision.
Destructive convergence is returned to the queue owner as an in-memory
post-commit plan. The post-import search-policy appliers live in
``lib.dispatch.post_import``.
"""

from __future__ import annotations

import logging
import os
import subprocess as sp
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from lib import transitions
from lib.dispatch.evidence_gate import (
    _current_evidence_analysis_failed,
    _import_allowed_by_evidence_pipeline,
    _load_evidence_import_gate,
    _refresh_current_evidence_after_import,
    _remove_quality_evidence_action_file,
    _requeue_import_job_to_preview,
    _write_album_sidecar_after_import,
    _write_quality_evidence_action_file,
)
from lib.dispatch.helpers import (
    _guard_failure_detail,
    _log_postflight_bad_extensions,
    _populate_dl_info_from_import_result,
    _should_cleanup_path,
)
from lib.dispatch.outcome_actions import (
    _do_mark_done,
    _record_have_analysis_error,
    _record_rejection_and_maybe_requeue,
    _reject_import_from_evidence_decision,
)
from lib.dispatch.post_import import (
    _apply_or_stage_denylists,
    _apply_or_stage_transition,
    _apply_post_import_search_action,
    _resolve_post_import_search_policy,
    _run_or_stage_quality_gate,
)
from lib.dispatch.quality_gate import _check_quality_gate_core
from lib.dispatch.subprocess_runner import run_import_one
from lib.dispatch.types import (
    FORCE_IMPORT_SCENARIOS,
    DispatchOutcome,
    EvidenceImportGate,
    ImportAttemptResult,
    ImportOneRunner,
    PostCommitCleanup,
    QualityGateFn,
)
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
    ProcessIdentity,
    checkpoint_automation_owner,
    read_process_start_ticks,
)
from lib.processing_paths import (
    normalize_source_dirs,
    path_is_within_root,
    processing_albums_dir,
)
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
    DownloadInfo,
    ImportResult,
    TargetQualityContract,
    V0ProbeEvidence,
    ValidationResult,
    comparison_basis_from_decision,
    dispatch_action,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
    narrow_override_on_lossless_source_lock,
    override_bitrate_from_current_evidence,
    resolve_rejection_search_override,
    stage2_counterfactual_from_decision,
)
from lib.quality_evidence import EvidenceBuildResult, audit_v0_probe_from_metric
from lib.terminal_outcomes import PendingImportTerminalOutcome

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.import_evidence import (
        CandidateEvidenceActionResult,
        CurrentEvidenceActionResult,
    )
    from lib.pipeline_db import DownloadLogOutcome, PipelineDB
    from lib.quality import SpectralDetail

logger = logging.getLogger("cratedigger")


def _processing_quarantine_root(
    source_path: str,
    cfg: CratediggerConfig,
) -> str:
    """Keep exact-owner quarantine on the canonical source filesystem.

    Sources outside private processing retain their configured Incoming
    quarantine. Any Cratedigger-owned source beneath ``processing/albums``
    instead quarantines beneath that same private root, so cleanup remains one
    journaled atomic rename regardless of where the operator mounts Incoming.
    """
    albums_root = processing_albums_dir(cfg.processing_dir)
    if path_is_within_root(source_path, albums_root):
        return albums_root
    return cfg.beets_staging_dir


@dataclass(frozen=True)
class _RejectionDetail:
    scenario: str
    detail: str | None
    error: str | None
    duplicate_guard_path: str | None = None
    duplicate_guard_staging_dir: str | None = None


def _describe_rejection(
    *,
    decision: str,
    import_result: ImportResult,
    label: str,
    mode: str,
    path: str,
    request_id: int,
    cfg: CratediggerConfig | None,
    new_bitrate: int | None,
    previous_bitrate: int | None,
) -> _RejectionDetail:
    """Resolve the persisted rejection description and duplicate-guard plan."""
    duplicate_guard_path: str | None = None
    duplicate_guard_staging_dir: str | None = None
    if decision == "downgrade":
        scenario = "quality_downgrade"
        detail = f"new {new_bitrate}kbps <= existing {previous_bitrate}kbps"
        logger.warning("QUALITY DOWNGRADE PREVENTED: %s", label)
    elif decision == "transcode_downgrade":
        scenario = "transcode_downgrade"
        detail = (
            f"transcode {new_bitrate}kbps <= existing {previous_bitrate}kbps"
        )
        logger.warning(
            "TRANSCODE REJECTED: %s at %skbps — not an upgrade",
            label,
            new_bitrate,
        )
    elif decision == "suspect_lossless_downgrade":
        scenario = "suspect_lossless_downgrade"
        candidate_avg = (
            import_result.v0_probe.avg_bitrate_kbps
            if import_result.v0_probe
            else None
        )
        existing_avg = (
            import_result.existing_v0_probe.avg_bitrate_kbps
            if import_result.existing_v0_probe
            else None
        )
        detail = (
            f"lossless-source V0 avg {candidate_avg}kbps "
            f"<= existing source V0 avg {existing_avg}kbps within tolerance"
        )
        logger.warning(
            "SUSPECT LOSSLESS REJECTED: %s candidate_v0_avg=%s "
            "existing_v0_avg=%s",
            label,
            candidate_avg,
            existing_avg,
        )
    elif decision == "suspect_lossless_probe_missing":
        scenario = "suspect_lossless_probe_missing"
        detail = import_result.error or (
            "suspect lossless source lacks comparable V0 probe"
        )
        logger.warning(
            "SUSPECT LOSSLESS REJECTED: %s missing comparable V0 probe",
            label,
        )
    elif decision == "lossless_source_locked":
        scenario = "lossless_source_locked"
        existing_avg = (
            import_result.existing_v0_probe.avg_bitrate_kbps
            if import_result.existing_v0_probe
            else None
        )
        detail = import_result.error or (
            "lossy candidate cannot override existing lossless-source "
            f"V0 probe {existing_avg}kbps"
        )
        logger.warning(
            "LOSSLESS SOURCE LOCKED: %s existing_v0_avg=%skbps",
            label,
            existing_avg,
        )
    elif decision == "duplicate_remove_guard_failed":
        scenario = "duplicate_remove_guard_failed"
        detail = _guard_failure_detail(import_result)
        duplicate_guard_path = path
        duplicate_guard_staging_dir = (
            _processing_quarantine_root(path, cfg)
            if cfg is not None
            else os.path.dirname(os.path.abspath(path))
        )
        guard = import_result.postflight.duplicate_remove_guard
        if guard is not None:
            logger.error(
                "DUPLICATE REMOVE GUARD: request_id=%s target=%s:%s "
                "duplicates=%s candidates=%s",
                request_id,
                guard.target_source or "unknown",
                guard.target_release_id,
                guard.duplicate_count,
                [
                    {
                        "beets_album_id": candidate.beets_album_id,
                        "mb_albumid": candidate.mb_albumid,
                        "discogs_albumid": candidate.discogs_albumid,
                        "album_path": candidate.album_path,
                        "item_count": candidate.item_count,
                    }
                    for candidate in guard.candidates
                ],
            )
    else:
        scenario = decision or "import_error"
        detail = import_result.error
        logger.error(
            "%s FAILED: %s (decision=%s, error=%s)",
            mode,
            label,
            decision,
            import_result.error,
        )
    error = (
        import_result.error
        if decision
        not in {
            "downgrade",
            "transcode_downgrade",
            "suspect_lossless_downgrade",
            "suspect_lossless_probe_missing",
            "lossless_source_locked",
        }
        else None
    )
    return _RejectionDetail(
        scenario=scenario,
        detail=detail,
        error=error,
        duplicate_guard_path=duplicate_guard_path,
        duplicate_guard_staging_dir=duplicate_guard_staging_dir,
    )


def _resolve_rejection_override(
    db: PipelineDB,
    *,
    request_id: int,
    decision: str,
    dl_info: DownloadInfo,
    import_result: ImportResult,
    cfg: CratediggerConfig | None,
) -> tuple[str | None, str | None]:
    """Resolve a post-rejection search override without failing dispatch."""
    current_override: str | None = None
    narrowed_override: str | None = None
    if decision in {"downgrade", "transcode_downgrade"}:
        try:
            request = db.get_request(request_id)
            current_override = (
                request.get("search_filetype_override") if request else None
            )
        except Exception:  # noqa: BLE001 - optional policy lookup
            logger.debug(
                "Failed to inspect search_filetype_override before "
                "downgrade reset"
            )
        narrowed_override = resolve_rejection_search_override(
            decision=decision,
            current_override=current_override,
            dl_info=dl_info,
            current_measurement=import_result.current_measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=import_result.spectral.existing,
            cfg=cfg.quality_ranks if cfg is not None else None,
        ).override
    elif decision == "lossless_source_locked":
        try:
            request = db.get_request(request_id)
            current_override = (
                request.get("search_filetype_override") if request else None
            )
            narrowed_override = narrow_override_on_lossless_source_lock(
                current_override
            )
        except Exception:  # noqa: BLE001 - optional policy lookup
            logger.debug(
                "Failed to inspect search_filetype_override before "
                "lossless_source_locked narrow"
            )
    return current_override, narrowed_override


def _denylist_reason(decision: str, new_bitrate: int | None) -> str:
    """Canonical audit reason for a post-import source denylist."""
    if decision == "downgrade":
        return "quality downgrade prevented"
    if decision == "provisional_lossless_upgrade":
        return "provisional lossless source imported"
    if decision.startswith("suspect_lossless"):
        return "suspect lossless source not an upgrade"
    if decision.startswith("transcode"):
        return f"transcode: {new_bitrate}kbps" if new_bitrate else "transcode detected"
    if decision == "duplicate_remove_guard_failed":
        return "duplicate remove guard failed"
    return f"rejected: {decision}"


def _checkpoint_import_owner(
    db: PipelineDB,
    *,
    import_job_id: int | None,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
) -> None:
    """Fail-stop pinned force sessions and exact automation owners."""
    if (cancellation_token is None) != (owner_session_identity is None):
        raise ValueError(
            "cancellation token and owner session identity must be paired"
        )
    if cancellation_token is not None and owner_session_identity is not None:
        cancellation_token.raise_if_cancelled()
        probe = db._probe_owner_session(owner_session_identity)
        if not probe.live:
            cancellation_token.cancel(
                f"owner_session_reverification_failed:{probe.reason}"
            )
            cancellation_token.raise_if_cancelled()
    if (
        import_job_id is None
        or execution_lease is None
        or cancellation_token is None
        or owner_session_identity is None
    ):
        return
    checkpoint_automation_owner(
        db,
        import_job_id=import_job_id,
        execution_lease=execution_lease,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
    )


def _validate_automation_dispatch_authority(
    db: PipelineDB,
    *,
    force: bool,
    import_job_id: int | None,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
) -> None:
    """Require the complete exact-owner bundle before automation dispatch."""
    if (cancellation_token is None) != (owner_session_identity is None):
        raise ValueError(
            "cancellation token and owner session identity must be paired"
        )
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()
    if execution_lease is None:
        return
    if (
        force
        or cancellation_token is None
        or owner_session_identity is None
        or import_job_id is None
    ):
        raise ValueError(
            "automation dispatch requires lease, token, pinned session, "
            "and import job"
        )
    _checkpoint_import_owner(
        db,
        import_job_id=import_job_id,
        execution_lease=execution_lease,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
    )


def _import_runner_hooks(
    db: PipelineDB,
    *,
    import_job_id: int | None,
    execution_lease_holder: list[ExecutionLeaseSnapshot | None],
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
) -> tuple[
    CancellationToken | None,
    Callable[[int], None] | None,
    Callable[[], bool] | None,
]:
    """Build process-group cancellation hooks for pinned import sessions."""
    execution_lease = execution_lease_holder[0]
    if cancellation_token is None and owner_session_identity is None:
        return None, None, None
    if cancellation_token is None or owner_session_identity is None:
        raise ValueError(
            "cancellation token and owner session identity must be paired"
        )
    if execution_lease is None:
        return (
            cancellation_token,
            None,
            lambda: db._probe_owner_session(owner_session_identity).live,
        )
    assert import_job_id is not None

    def record_beets_child(pid: int) -> None:
        current_lease = execution_lease_holder[0]
        assert current_lease is not None
        _checkpoint_import_owner(
            db,
            import_job_id=import_job_id,
            execution_lease=current_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )
        start_ticks = read_process_start_ticks(pid)
        persisted = db.record_import_job_beets_child(
            import_job_id,
            expected_execution_lease=current_lease,
            beets_pid=pid,
            beets_start_ticks=start_ticks,
        )
        if persisted is None:
            cancellation_token.cancel("beets_child_identity_persist_rejected")
            cancellation_token.raise_if_cancelled()
        execution_lease_holder[0] = replace(
            current_lease,
            beets=ProcessIdentity(pid, start_ticks),
        )

    return (
        cancellation_token,
        record_beets_child,
        lambda: db._probe_owner_session(owner_session_identity).live,
    )


def _capture_automation_completion(
    db: PipelineDB,
    *,
    import_job_id: int | None,
    request_id: int,
    release_id: str,
    canonical_path: str,
    returncode: int,
    execution_lease: ExecutionLeaseSnapshot | None,
) -> DispatchOutcome | None:
    """Persist one exact child completion or return the recovery outcome."""
    if execution_lease is None:
        return None
    assert import_job_id is not None
    from lib.import_job_recovery_service import AutomationCompletionReceipt

    captured = db.capture_automation_import_completion(
        import_job_id,
        expected_execution_lease=execution_lease,
        receipt=AutomationCompletionReceipt(
            job_id=import_job_id,
            request_id=request_id,
            release_id=release_id,
            canonical_path=canonical_path,
            returncode=returncode,
            captured_at=datetime.now(UTC).isoformat(),
        ),
    )
    if captured is not None:
        return None
    return DispatchOutcome(
        success=False,
        message=(
            "Beets returned but exact completion capture conflicted"
        ),
        code="beets_acknowledgement_ambiguous",
    )


def _should_cleanup_action_file(
    *,
    quality_evidence_action_file: str | None,
    beets_launch_authorized: bool,
    execution_lease: ExecutionLeaseSnapshot | None,
    automation_completion_captured: bool,
) -> bool:
    """Whether this execution still owns cleanup of its action sidecar."""
    return (
        quality_evidence_action_file is not None
        and (
            not beets_launch_authorized
            or execution_lease is None
            or automation_completion_captured
        )
    )


def _trigger_post_import_notifiers(
    cfg: CratediggerConfig,
    db: PipelineDB,
    *,
    import_result: ImportResult,
    request_id: int,
    import_job_id: int | None,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
) -> None:
    """Capture historical pins, then refresh both configured media servers."""
    from lib.util import trigger_jellyfin_scan as trigger_jellyfin
    from lib.util import trigger_plex_scan as trigger_plex

    _checkpoint_import_owner(
        db,
        import_job_id=import_job_id,
        execution_lease=execution_lease,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
    )
    imported_path = import_result.postflight.imported_path
    plex_original_added_at: int | None = None
    try:
        from lib.plex_pin_service import capture_plex_added_at_pin

        plex_pin = capture_plex_added_at_pin(
            cfg,
            db,
            imported_path,
            request_id,
        )
        plex_original_added_at = plex_pin.original_added_at
    except Exception:
        logger.exception("PLEX PIN: capture wiring failed (non-fatal)")
    trigger_plex(cfg, imported_path)

    try:
        from lib.jellyfin_pin_service import capture_jellyfin_date_created_pin

        capture_jellyfin_date_created_pin(
            cfg,
            db,
            imported_path,
            request_id,
            historical_added_at=plex_original_added_at,
            replaced_album_paths=[
                candidate.album_path
                for candidate in import_result.postflight.replaced_albums
                if candidate.album_path
            ],
        )
    except Exception:
        logger.exception("JELLYFIN PIN: capture wiring failed (non-fatal)")
    trigger_jellyfin(cfg, imported_path)


def _resolve_dispatch_beets_paths(
    cfg: CratediggerConfig | None,
    *,
    db_path: str | None,
    library_root: str | None,
) -> tuple[str, str]:
    """Resolve one Beets DB/root authority for a complete dispatch.

    An isolated caller owns both values.  Production derives both from the
    complete runtime config, rather than combining a test DB with a deployed
    root (or vice versa).
    """
    from lib.beets_db import validate_beets_storage_pair

    validate_beets_storage_pair(db_path=db_path, library_root=library_root)
    if db_path is not None and library_root is not None:
        return db_path, library_root
    assert cfg is not None
    return cfg.beets_library_db, cfg.beets_directory


def _evidence_reject_import_result(
    *,
    decision: str,
    candidate: AlbumQualityEvidence,
    current: AlbumQualityEvidence | None,
    evidence_decision: dict[str, object],
    existing_v0_probe: V0ProbeEvidence | None,
) -> ImportResult:
    """The audit ``ImportResult`` a persisted-evidence rejection persists.

    Extracted from ``dispatch_import_core`` — it is a pure projection of
    the gate's own evidence plus the decision dict, and inlining it pushed
    that already-large function past Pyright's analysis-complexity limit.

    The two ``*_if_stage1_deferred`` fields are AUDIT ONLY (issue #829
    Phase 5 PR4): a Stage-1 spectral reject short-circuits before Stage 2
    runs, so without them the persisted trail stops at
    ``stage1_spectral:reject`` and says nothing about whether the candidate
    was actually an upgrade — which IS issue #813's disagreement question.
    Nothing branches on either field.
    """
    if candidate.target_format is None:
        target_contract = None
    elif candidate.target_is_cbr is not None:
        target_contract = TargetQualityContract.from_projection(
            candidate.target_format,
            projected_is_cbr=candidate.target_is_cbr,
        )
    else:
        target_contract = TargetQualityContract.from_explicit_label(
            candidate.target_format
        )
    return ImportResult(
        decision=decision,
        source_measurement=candidate.measurement,
        current_measurement=(
            current.measurement if current is not None else None
        ),
        target_quality_contract=target_contract,
        v0_probe=audit_v0_probe_from_metric(candidate.v0_metric),
        existing_v0_probe=existing_v0_probe,
        comparison_basis=comparison_basis_from_decision(evidence_decision),
        stage2_import_if_stage1_deferred=(
            stage2_counterfactual_from_decision(evidence_decision)
        ),
        comparison_basis_if_stage1_deferred=comparison_basis_from_decision(
            evidence_decision, key="comparison_basis_if_stage1_deferred",
        ),
    )


def dispatch_import_core(
    *,
    path: str,
    mb_release_id: str,
    request_id: int,
    label: str,
    force: bool = False,
    override_min_bitrate: int | None = None,
    target_format: str | None = None,
    verified_lossless_target: str = "",
    beets_harness_path: str,
    db: PipelineDB,
    dl_info: DownloadInfo,
    distance: float | None = None,
    scenario: str = "auto_import",
    files: Sequence[object] | None = None,
    cfg: CratediggerConfig | None = None,
    outcome_label: DownloadLogOutcome = "success",
    requeue_on_failure: bool = True,
    cooled_down_users: set[str] | None = None,
    source_dirs: list[str] | None = None,
    candidate_import_job_id: int | None = None,
    attempt_spectral_audit: SpectralDetail | None = None,
    attempt_result: ImportAttemptResult | None = None,
    candidate_download_log_id: int | None = None,
    launch_authority_path: str | None = None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    quality_gate_fn: QualityGateFn = _check_quality_gate_core,
    run_import_fn: ImportOneRunner | None = None,
    evidence_gate_fn: Callable[..., EvidenceImportGate] = _load_evidence_import_gate,
    current_evidence_loader: Callable[
        ..., CurrentEvidenceActionResult | None
    ] | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
) -> DispatchOutcome:
    """Core import dispatch — takes plain params + PipelineDB directly.

    Runs import_one.py, parses result, dispatches on decision (mark_done/failed,
    denylist, quality gate, media server notifiers, cleanup). Returns DispatchOutcome.

    ``beets_library_db_path`` / ``beets_library_root`` are an inseparable
    explicit storage authority for isolated real-Beets worlds. Production
    leaves both unset and derives the complete pair from runtime config.

    Used by the auto-import flow in ``lib.download`` and by
    ``dispatch_import_from_db()`` (force-import).
    """
    source_dirs = normalize_source_dirs(source_dirs or [])
    from lib.config import read_runtime_config

    beets_cfg = cfg or read_runtime_config()
    effective_beets_library_db_path, effective_beets_library_root = (
        _resolve_dispatch_beets_paths(
            beets_cfg,
            db_path=beets_library_db_path,
            library_root=beets_library_root,
        )
    )

    # Operation identity is distinct from the eventual download-log outcome:
    # an automatic attempt can still reject or fail after this start message.
    mode = "FORCE-IMPORT" if force else "AUTO-IMPORT"
    dist_label = f"{distance:.4f}" if distance is not None else "unmeasured"
    logger.info(f"{mode}: {label} "
                f"(source=request, dist={dist_label})")

    if attempt_result is None:
        attempt_result = ImportAttemptResult.from_import_job(
            db,
            candidate_import_job_id,
            attempt_spectral_audit,
        )

    outcome_success = False
    outcome_message = ""
    terminal_outcome: PendingImportTerminalOutcome | None = None
    post_commit_staged_path: str | None = None
    post_commit_duplicate_guard_path: str | None = None
    post_commit_duplicate_guard_staging_dir: str | None = None
    beets_launch_authorized = False
    automation_completion_captured = False
    active_execution_lease = execution_lease
    execution_lease_holder = [execution_lease]

    _validate_automation_dispatch_authority(
        db,
        force=force,
        import_job_id=candidate_import_job_id,
        execution_lease=active_execution_lease,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
    )

    # Acquire the RELEASE (per-MBID) advisory lock for the duration of
    # the ``import_one.py`` subprocess. This is the funnel every path
    # goes through (automatic and force), so the lock here closes the
    # cross-process race that could produce Palo Santo-*class* data loss
    # (issues #132 P1 / #133) for every entry point. The actual 04-20
    # Palo Santo incident had a different proximate cause (YAML misconfig —
    # see CLAUDE.md § Resolved canonical RCs); this lock defends against
    # an independent race vector the original fix left open.
    # Auto path: ``_handle_valid_result`` has already acquired RELEASE
    # outer — this acquisition is a session-reentrant no-op. Force path:
    # this is the first RELEASE acquisition, nested inside
    # the IMPORT lock held by ``dispatch_import_from_db``.
    # See ``docs/advisory-locks.md`` for the full rationale, the
    # ordering rules, and the call-site index.
    from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key
    release_lock_key: int | None
    if mb_release_id:
        release_lock_key = release_id_to_lock_key(mb_release_id)
    else:
        # Defensive: ``dispatch_import_from_db`` already rejects empty
        # mbids before reaching here; the auto-import flow passes
        # ``album_data.mb_release_id or ""``. An empty mbid means
        # there's nothing to serialise across, so skip the lock.
        release_lock_key = None
        logger.warning(
            f"{mode}: mb_release_id is empty; skipping release lock "
            "(no cross-release race to serialise)")

    if release_lock_key is not None:
        lock_ctx = db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE, release_lock_key)
    else:
        # No-op context manager that yields True (treat as "got lock"
        # so the critical section runs). ``contextlib.nullcontext``
        # forwards the enter value unchanged.
        from contextlib import nullcontext
        lock_ctx = nullcontext(True)

    with lock_ctx as got_release_lock:
        if not got_release_lock:
            logger.warning(
                f"{mode} SKIPPED: {label} — release lock held by "
                f"another process (mbid={mb_release_id})")
            # Contention == deferred retry. The entire function now
            # returns ``DispatchOutcome(deferred=True)`` without
            # mutating ANY state:
            #
            # - No status transition (was: reset to 'wanted'). The
            #   auto path's outer ``_run_completed_processing`` now
            #   branches on ``outcome.deferred`` — no flip to
            #   ``imported`` and no reset to ``wanted``; the request
            #   stays ``downloading`` with its ``active_download_state``
            #   intact, so ``poll_active_downloads`` re-enters
            #   ``process_completed_album`` on the next cycle and
            #   retries exactly where we stopped.
            # - No staged-dir cleanup (was: ``_cleanup_staged_dir``).
            #   Codex PR #136 R3 P3: if the competing import later
            #   fails, wiping the staged copy forces a redownload
            #   from Soulseek. Staging is preserved so the retry
            #   resumes with the local files already in place.
            # - No spectral-stamp clear. The request stamps remain historical
            #   audit data; linked evidence and fresh attempt analysis own
            #   subsequent decisions.
            #
            # Force-import paths (scenario in FORCE_IMPORT_SCENARIOS)
            # surface the message to the user via
            # ``dispatch_import_from_db``; no state change needed
            # because the request wasn't ``downloading`` to begin
            # with.
            if execution_lease is not None:
                return _requeue_import_job_to_preview(
                    db,
                    import_job_id=candidate_import_job_id,
                    reason="release lock contention",
                    expected_execution_lease=active_execution_lease,
                )
            return DispatchOutcome(
                success=False,
                message=("Another import is already in progress for "
                         f"this release ({mb_release_id})"),
                deferred=True,
            )

        quality_evidence_action_file: str | None = None
        try:
            # Bounded consistency boundary, not cross-system atomicity:
            # PostgreSQL evidence, Beets SQLite authority, and the candidate /
            # library filesystems cannot share one transaction. Reauthorize
            # both sides against the world observed immediately before the
            # unified decision and fail closed before Beets launch. Any later
            # world failure is audited and returned to the runnable lifecycle.
            evidence_gate_kwargs: dict[str, object] = {}
            if current_evidence_loader is not None:
                evidence_gate_kwargs["current_evidence_loader"] = (
                    current_evidence_loader
                )
            evidence_gate = evidence_gate_fn(
                db,
                request_id=request_id,
                mb_release_id=mb_release_id,
                path=path,
                quality_ranks=cfg.quality_ranks if cfg is not None else None,
                candidate_import_job_id=candidate_import_job_id,
                candidate_download_log_id=candidate_download_log_id,
                prevalidated_candidate_result=prevalidated_candidate_result,
                attempt_existing_spectral=(
                    attempt_result.audit.existing
                    if attempt_result.audit is not None
                    else None
                ),
                attempt_have_audit_available=attempt_result.audit is not None,
                beets_library_db_path=effective_beets_library_db_path,
                beets_library_root=effective_beets_library_root,
                **evidence_gate_kwargs,
            )
            if prevalidated_candidate_result is not None:
                # Re-check the queue-owned action copy before *any* evidence
                # decision can become terminal. A late content change is a
                # typed preview retry, never a false quality rejection.
                from lib.import_evidence import ensure_candidate_evidence_for_action

                fresh_candidate = ensure_candidate_evidence_for_action(
                    db,
                    source_path=path,
                    import_job_id=candidate_import_job_id,
                )
                if (
                    not fresh_candidate.available
                    or fresh_candidate.evidence is None
                    or evidence_gate.candidate is None
                    or fresh_candidate.evidence.id != evidence_gate.candidate.id
                    or fresh_candidate.evidence.snapshot_fingerprint
                        != evidence_gate.candidate.snapshot_fingerprint
                ):
                    reason = (
                        fresh_candidate.provenance.fallback_reason
                        or fresh_candidate.provenance.candidate_status
                        or "candidate evidence changed before dispatch"
                    )
                    return _requeue_import_job_to_preview(
                        db,
                        import_job_id=candidate_import_job_id,
                        reason=reason,
                        expected_execution_lease=active_execution_lease,
                    )
            if (
                evidence_gate.candidate is not None
                and _current_evidence_analysis_failed(evidence_gate)
            ):
                reason = (
                    evidence_gate.current_reason
                    or "installed HAVE analysis failed without diagnostics"
                )
                pending = _record_have_analysis_error(
                    db,
                    request_id=request_id,
                    dl_info=dl_info,
                    raw_error=reason,
                    installed_path=evidence_gate.current_path,
                    candidate_reference=path,
                    snapshot_guard=evidence_gate.current_snapshot_guard,
                    import_job_id=candidate_import_job_id,
                    source_download_log_id=candidate_download_log_id,
                    cooled_down_users=cooled_down_users,
                    requeue_to_wanted=requeue_on_failure,
                )
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Installed HAVE analysis failed; "
                        + (
                            "request returned to wanted for a future retry"
                            if requeue_on_failure
                            else "request lifecycle was preserved"
                        )
                    ),
                    code="have_analysis_error",
                    terminal_outcome=(
                        pending
                        if isinstance(pending, PendingImportTerminalOutcome)
                        else None
                    ),
                )
            existing_v0_probe = audit_v0_probe_from_metric(
                evidence_gate.current.v0_metric
                if evidence_gate.current is not None
                else None
            )
            evidence_override = override_bitrate_from_current_evidence(
                evidence_gate.current
            )
            if evidence_override is not None:
                override_min_bitrate = evidence_override
            if (
                (candidate_import_job_id is not None
                 or candidate_download_log_id is not None)
                and evidence_gate.candidate is None
            ):
                # U4: outer callers (``_dispatch_import_from_db_locked`` and
                # ``lib/download_validation.py::_process_beets_validation``) already
                # call ``ensure_candidate_evidence_for_action`` and requeue
                # via ``_requeue_import_job_to_preview`` when evidence is
                # missing. Reaching this inner site means a caller bypassed
                # the outer gate (test seam or future misuse). Behave
                # consistently with the outer invariant — requeue rather
                # than hard-fail — so the importer never measures and
                # never writes a terminal failure on missing evidence.
                reason = evidence_gate.candidate_reason or evidence_gate.candidate_status
                return _requeue_import_job_to_preview(
                    db,
                    import_job_id=candidate_import_job_id,
                    reason=reason or "missing",
                    expected_execution_lease=active_execution_lease,
                )
            if evidence_gate.candidate is not None:
                # U11: ``full_pipeline_decision_from_evidence`` is the single
                # decision function. Folder/audio-integrity facts
                # (audio_corrupt / bad_audio_hash / nested_layout /
                # empty_fileset / mixed_source) are early-exit rejects at the
                # top of that function. They still use the unified reject
                # helper below, which honours caller lifecycle authority:
                # automation self-heals; force imports preserve operator
                # status.
                facts = AlbumQualityEvidenceDecisionFacts(
                    verified_lossless_target=verified_lossless_target or None,
                    target_format=target_format,
                )
                evidence_decision = full_pipeline_decision_from_evidence(
                    evidence_gate.candidate,
                    evidence_gate.current,
                    facts=facts,
                    cfg=cfg.quality_ranks if cfg is not None else None,
                )
                if not _import_allowed_by_evidence_pipeline(evidence_decision):
                    decision = evidence_decision_name(evidence_decision)
                    detail = (
                        evidence_gate.candidate.audio_error
                        if (
                            decision == "audio_corrupt"
                            and evidence_gate.candidate.audio_error
                        )
                        else (
                            "import-time persisted evidence rejected candidate "
                            f"(decision={decision})"
                        )
                    )
                    attempt_result.merge(_evidence_reject_import_result(
                        decision=decision,
                        candidate=evidence_gate.candidate,
                        current=evidence_gate.current,
                        evidence_decision=evidence_decision,
                        existing_v0_probe=existing_v0_probe,
                    ))
                    return _reject_import_from_evidence_decision(
                        db=db,
                        request_id=request_id,
                        dl_info=dl_info,
                        attempt_result=attempt_result,
                        distance=distance,
                        decision=decision,
                        detail=detail,
                        requeue_on_failure=requeue_on_failure,
                        validation_result=dl_info.validation_result,
                        staged_path=path,
                        scenario=scenario,
                        files=files,
                        source_path_cleanup_scenario=scenario,
                        cooled_down_users=cooled_down_users,
                        import_job_id=candidate_import_job_id,
                        source_download_log_id=candidate_download_log_id,
                        quality_ranks=(
                            cfg.quality_ranks if cfg is not None else None
                        ),
                        audio_quarantine_root=_processing_quarantine_root(
                            path,
                            beets_cfg,
                        ),
                        preserve_corrupt_source=force,
                    )
                quality_evidence_action_file = _write_quality_evidence_action_file(
                    candidate=evidence_gate.candidate,
                    current=evidence_gate.current,
                    decision=evidence_decision,
                    target_format=target_format,
                    verified_lossless_target=verified_lossless_target,
                    gate=evidence_gate,
                )
            if candidate_import_job_id is None:
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Beets launch refused: no active import job owns "
                        "this mutation"
                    ),
                    code="launch_authority_missing",
                )
            # A force action can run from a retained private copy, but the
            # job's original source remains the durable launch/recovery
            # authority.  The action copy is separately bound by the
            # candidate-evidence snapshot above.
            authorized_job = db.authorize_import_job_launch(
                candidate_import_job_id,
                request_id=request_id,
                release_id=mb_release_id,
                source_path=launch_authority_path or path,
                expected_execution_lease=active_execution_lease,
            )
            if authorized_job is None:
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Beets launch refused: import job authority is stale "
                        "or no longer active"
                    ),
                    code="launch_authority_conflict",
                )
            beets_launch_authorized = True

            # Force-import operates on the user's only copy of the source
            # material (typically failed_imports/…). Tell the harness to keep
            # lossless originals intact until the quality decision — on
            # downgrade/transcode_downgrade verdicts we exit before deletion so
            # the user's FLACs survive (#111). An automatic request import runs
            # from its disposable, exact-owner processing path and does not
            # need the flag.
            quality_rank_config_json = (
                cfg.quality_ranks.to_json() if cfg is not None else None
            )
            (
                runner_cancellation_token,
                runner_on_spawn,
                runner_owner_session_probe,
            ) = _import_runner_hooks(
                db,
                import_job_id=candidate_import_job_id,
                execution_lease_holder=execution_lease_holder,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
            if run_import_fn is None:
                run = run_import_one(
                    path=path, mb_release_id=mb_release_id,
                    request_id=request_id, force=force,
                    preserve_source=scenario in FORCE_IMPORT_SCENARIOS,
                    override_min_bitrate=override_min_bitrate,
                    target_format=target_format,
                    verified_lossless_target=verified_lossless_target,
                    beets_harness_path=beets_harness_path,
                    quality_rank_config_json=quality_rank_config_json,
                    existing_v0_probe=existing_v0_probe,
                    quality_evidence_action_file=quality_evidence_action_file,
                    beets_config_dir=beets_cfg.beets_config_dir,
                    beets_python=beets_cfg.beets_python,
                    beets_library_db_path=effective_beets_library_db_path,
                    beets_library_root=effective_beets_library_root,
                    cancellation_token=runner_cancellation_token,
                    on_spawn=runner_on_spawn,
                    owner_session_probe=runner_owner_session_probe,
                )
            else:
                run = run_import_fn(
                    path=path, mb_release_id=mb_release_id,
                    request_id=request_id, force=force,
                    preserve_source=scenario in FORCE_IMPORT_SCENARIOS,
                    override_min_bitrate=override_min_bitrate,
                    target_format=target_format,
                    verified_lossless_target=verified_lossless_target,
                    beets_harness_path=beets_harness_path,
                    quality_rank_config_json=quality_rank_config_json,
                    existing_v0_probe=existing_v0_probe,
                    quality_evidence_action_file=quality_evidence_action_file,
                    beets_config_dir=beets_cfg.beets_config_dir,
                    beets_python=beets_cfg.beets_python,
                    beets_library_db_path=effective_beets_library_db_path,
                    beets_library_root=effective_beets_library_root,
                    cancellation_token=runner_cancellation_token,
                    on_spawn=runner_on_spawn,
                    owner_session_probe=runner_owner_session_probe,
                )
            active_execution_lease = execution_lease_holder[0]
            _checkpoint_import_owner(
                db,
                import_job_id=candidate_import_job_id,
                execution_lease=active_execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
            completion_conflict = _capture_automation_completion(
                db,
                import_job_id=candidate_import_job_id,
                request_id=request_id,
                release_id=mb_release_id,
                canonical_path=launch_authority_path or path,
                returncode=run.returncode,
                execution_lease=active_execution_lease,
            )
            if completion_conflict is not None:
                return completion_conflict
            automation_completion_captured = active_execution_lease is not None
            _remove_quality_evidence_action_file(quality_evidence_action_file)
            quality_evidence_action_file = None
            for line in run.stderr.strip().split("\n"):
                if line.strip():
                    logger.info(f"  [import] {line}")

            ir = run.import_result
            if ir is not None:
                ir = attempt_result.merge(ir)
            if ir is None:
                logger.error(
                    f"{mode} ACKNOWLEDGEMENT AMBIGUOUS "
                    f"(no JSON, rc={run.returncode}): {label}")
                for line in run.stdout.strip().split("\n"):
                    logger.error(f"  {line}")
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Beets returned without a terminal result"
                    ),
                    code="beets_acknowledgement_ambiguous",
                )
            else:
                _populate_dl_info_from_import_result(dl_info, ir)
                _log_postflight_bad_extensions(
                    ir=ir,
                    mode=mode,
                    request_id=request_id,
                    label=label,
                )
                decision = ir.decision or "unknown"
                action = dispatch_action(decision)
                (
                    search_action,
                    should_denylist,
                    usernames,
                    file_list,
                ) = _resolve_post_import_search_policy(
                    decision=decision,
                    files=files,
                    fallback_username=dl_info.username,
                )
                narrowed_override = None
                current_override = None
                post_import_evidence = EvidenceBuildResult(
                    None,
                    "failed",
                    "post-import evidence refresh did not run",
                )

                new_br = ir.source_measurement.min_bitrate_kbps if ir.source_measurement else None
                prev_br = ir.current_measurement.min_bitrate_kbps if ir.current_measurement else None

                # --- Mark done or failed with decision-specific details ---
                if action.mark_done:
                    logger.info(f"{mode} OK: {label} (decision={decision})")
                    mark_scenario = (
                        decision
                        if decision == "provisional_lossless_upgrade"
                        else scenario
                    )
                    pending = _do_mark_done(
                        db, request_id, dl_info,
                        distance=distance, scenario=mark_scenario,
                        dest_path=path, outcome_label=outcome_label,
                        clear_stale_v0_probe=(
                            decision != "preflight_existing"
                        ),
                        attempt_result=attempt_result,
                        import_job_id=candidate_import_job_id,
                        source_download_log_id=candidate_download_log_id)
                    if isinstance(pending, PendingImportTerminalOutcome):
                        terminal_outcome = pending
                    try:
                        _checkpoint_import_owner(
                            db,
                            import_job_id=candidate_import_job_id,
                            execution_lease=active_execution_lease,
                            cancellation_token=cancellation_token,
                            owner_session_identity=owner_session_identity,
                        )
                        post_import_evidence = _refresh_current_evidence_after_import(
                            db,
                            request_id=request_id,
                            mb_release_id=mb_release_id,
                            quality_ranks=(
                                cfg.quality_ranks if cfg is not None else None
                            ),
                            source_candidate=evidence_gate.candidate,
                            import_result=ir,
                            beets_library_db_path=effective_beets_library_db_path,
                            beets_library_root=effective_beets_library_root,
                        )
                    except ExecutionCancelled:
                        raise
                    except Exception as exc:
                        logger.exception(
                            "Failed to refresh current quality evidence "
                            "after import for request %s",
                            request_id,
                        )
                        post_import_evidence = EvidenceBuildResult(
                            None,
                            "failed",
                            f"{type(exc).__name__}: {exc}",
                        )
                    try:
                        _checkpoint_import_owner(
                            db,
                            import_job_id=candidate_import_job_id,
                            execution_lease=active_execution_lease,
                            cancellation_token=cancellation_token,
                            owner_session_identity=owner_session_identity,
                        )
                        if (
                            post_import_evidence.status == "ready"
                            and post_import_evidence.evidence is not None
                        ):
                            _write_album_sidecar_after_import(
                                db,
                                request_id=request_id,
                                mb_release_id=mb_release_id,
                                cfg=cfg,
                                beets_library_db_path=effective_beets_library_db_path,
                                beets_library_root=effective_beets_library_root,
                            )
                    except ExecutionCancelled:
                        raise
                    except Exception:
                        logger.exception(
                            "Failed to write verified-lossless sidecar "
                            "after import for request %s",
                            request_id,
                        )
                    if (
                        decision in ("import", "preflight_existing")
                        and (prev_br is not None or new_br is not None)
                    ):
                        try:
                            delta_transition = transitions.RequestTransition.to_imported(
                                from_status="imported",
                                prev_min_bitrate=prev_br,
                                min_bitrate=new_br,
                            )
                            terminal_outcome = _apply_or_stage_transition(
                                db,
                                request_id,
                                terminal_outcome,
                                delta_transition,
                            )
                        except Exception:
                            logger.exception("Failed to update upgrade delta")
                    outcome_success = True
                    outcome_message = "Import successful"
                elif action.record_rejection:
                    rejection = _describe_rejection(
                        decision=decision,
                        import_result=ir,
                        label=label,
                        mode=mode,
                        path=path,
                        request_id=request_id,
                        cfg=cfg,
                        new_bitrate=new_br,
                        previous_bitrate=prev_br,
                    )
                    fail_scenario = rejection.scenario
                    fail_detail = rejection.detail
                    fail_error = rejection.error
                    post_commit_duplicate_guard_path = (
                        rejection.duplicate_guard_path
                    )
                    post_commit_duplicate_guard_staging_dir = (
                        rejection.duplicate_guard_staging_dir
                    )

                    current_override, narrowed_override = (
                        _resolve_rejection_override(
                            db,
                            request_id=request_id,
                            decision=decision,
                            dl_info=dl_info,
                            import_result=ir,
                            cfg=cfg,
                        )
                    )

                    pending = _record_rejection_and_maybe_requeue(
                        db, request_id, dl_info,
                        detail=fail_detail,
                        error=fail_error,
                        requeue=requeue_on_failure,
                        outcome_label="rejected",
                        search_filetype_override=narrowed_override,
                        validation_result=(dl_info.validation_result
                                           or ValidationResult(
                                               distance=distance,
                                               scenario=fail_scenario,
                                               detail=fail_detail,
                                               error=fail_error,
                                               source_dirs=source_dirs,
                                           ).to_json()),
                        staged_path=path,
                        attempt_result=attempt_result,
                        import_job_id=candidate_import_job_id,
                        source_download_log_id=candidate_download_log_id)
                    if isinstance(pending, PendingImportTerminalOutcome):
                        terminal_outcome = pending
                    if narrowed_override is not None:
                        logger.info(
                            f"  Narrowed search_filetype_override '{current_override}'"
                            f" -> '{narrowed_override}' after downgrade")
                    outcome_message = f"Rejected: {fail_scenario} — {fail_detail}"

                # Rejections use dispatch_action; retained imports use the
                # canonical post-import reducer for the same denylist write.
                if should_denylist:
                    reason = _denylist_reason(decision, new_br)
                    if (decision == "duplicate_remove_guard_failed"
                            and not usernames):
                        logger.error(
                            "DUPLICATE REMOVE GUARD: no source username "
                            "available to denylist for request %s",
                            request_id,
                        )
                    terminal_outcome = _apply_or_stage_denylists(
                        db,
                        request_id,
                        terminal_outcome,
                        usernames,
                        reason,
                        cooled_down_users,
                    )
                    logger.info(f"  Denylisted {usernames} for request {request_id}")

                # Rejected auto-imports are already requeued by
                # _record_rejection_and_maybe_requeue(), which preserves retry
                # counters and records the validation attempt. This second
                # requeue is only for successful imports that intentionally go
                # back to wanted to keep searching for a better source.
                terminal_outcome = _apply_post_import_search_action(
                    db,
                    request_id=request_id,
                    pending=terminal_outcome,
                    decision=decision,
                    search_action=search_action,
                    mark_done=action.mark_done,
                    new_bitrate=new_br,
                )

                # Authority: "D19 — Force-import overrides the beets distance
                # and nothing else."
                # https://github.com/abl030/cratedigger/issues/711#issuecomment-4999204451
                # Authority: "The verified-lossless proof lock is absolute
                # for every import mode."
                # https://github.com/abl030/cratedigger/issues/711#issuecomment-5000425284
                # Operator imports run the identical quality/search policy.
                # The quality-gate plan explicitly distinguishes successful
                # terminal acceptance from every non-accepting outcome; the
                # terminal DB transaction owns search-stop arbitration.
                if action.run_quality_gate:
                    terminal_outcome = _run_or_stage_quality_gate(
                        quality_gate_fn,
                        terminal_outcome,
                        mb_id=mb_release_id,
                        label=label,
                        request_id=request_id,
                        files=list(file_list),
                        db=db,
                        quality_ranks=cfg.quality_ranks if cfg is not None else None,
                        expected_current_evidence_id=(
                            post_import_evidence.evidence.id
                            if post_import_evidence.status == "ready"
                            and post_import_evidence.evidence is not None
                            and post_import_evidence.evidence.id is not None
                            else 0
                        ),
                    )
                if action.trigger_notifiers and cfg is not None:
                    _trigger_post_import_notifiers(
                        cfg,
                        db,
                        import_result=ir,
                        request_id=request_id,
                        import_job_id=candidate_import_job_id,
                        execution_lease=active_execution_lease,
                        cancellation_token=cancellation_token,
                        owner_session_identity=owner_session_identity,
                    )
                if action.cleanup and _should_cleanup_path(scenario, action):
                    # Issue #89: force-import passes the user's
                    # ``failed_imports/…`` folder as ``path`` — cleanup is
                    # data loss on a ``downgrade`` / ``transcode_downgrade``
                    # decision where beets never moved the files.
                    # ``_should_cleanup_path`` only allows cleanup on force
                    # when the decision actually imported (mark_done=
                    # True, i.e. beets has moved the files and the source
                    # directory is now empty), which keeps the wrong-matches
                    # tab honest and prevents duplicate re-imports of an
                    # already-imported album. Auto-import scenarios always
                    # clean — their exact-owner processing source is
                    # disposable by design.
                    post_commit_staged_path = path
        except ExecutionCancelled:
            raise
        except sp.TimeoutExpired:
            logger.error(f"{mode} TIMEOUT: {label}")
            if beets_launch_authorized:
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Import timed out after Beets launch; operator "
                        "recovery is required"
                    ),
                    code="beets_acknowledgement_ambiguous",
                )
            pending = _record_rejection_and_maybe_requeue(
                db, request_id, dl_info,
                detail="import_one.py timed out", error="timeout",
                requeue=requeue_on_failure, outcome_label="failed",
                validation_result=ValidationResult(
                    distance=distance,
                    scenario="timeout",
                    detail="import_one.py timed out",
                    error="timeout",
                    source_dirs=source_dirs,
                ).to_json(),
                staged_path=path,
                attempt_result=attempt_result,
                import_job_id=candidate_import_job_id,
                source_download_log_id=candidate_download_log_id)
            if isinstance(pending, PendingImportTerminalOutcome):
                terminal_outcome = pending
            outcome_message = "Import timed out"
        except Exception:
            logger.exception(f"{mode} ERROR: {label}")
            if beets_launch_authorized:
                return DispatchOutcome(
                    success=False,
                    message=(
                        "Import failed after Beets launch without a terminal "
                        "acknowledgement"
                    ),
                    code="beets_acknowledgement_ambiguous",
                )
            pending = _record_rejection_and_maybe_requeue(
                db, request_id, dl_info,
                detail="unhandled exception in auto-import", error="exception",
                requeue=requeue_on_failure, outcome_label="failed",
                validation_result=ValidationResult(
                    distance=distance,
                    scenario="exception",
                    detail="unhandled exception in auto-import",
                    error="exception",
                    source_dirs=source_dirs,
                ).to_json(),
                staged_path=path,
                attempt_result=attempt_result,
                import_job_id=candidate_import_job_id,
                source_download_log_id=candidate_download_log_id)
            if isinstance(pending, PendingImportTerminalOutcome):
                terminal_outcome = pending
            outcome_message = "Unhandled exception"
        finally:
            cleanup_action_file = _should_cleanup_action_file(
                quality_evidence_action_file=quality_evidence_action_file,
                beets_launch_authorized=beets_launch_authorized,
                execution_lease=active_execution_lease,
                automation_completion_captured=automation_completion_captured,
            )
            if cleanup_action_file:
                _checkpoint_import_owner(
                    db,
                    import_job_id=candidate_import_job_id,
                    execution_lease=active_execution_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                )
                _remove_quality_evidence_action_file(
                    quality_evidence_action_file
                )

    _checkpoint_import_owner(
        db,
        import_job_id=candidate_import_job_id,
        execution_lease=active_execution_lease,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
    )
    return DispatchOutcome(
        success=outcome_success,
        message=outcome_message,
        terminal_outcome=terminal_outcome,
        post_commit_cleanup=(
            PostCommitCleanup(
                staged_path=post_commit_staged_path,
                duplicate_guard_source_path=post_commit_duplicate_guard_path,
                duplicate_guard_staging_dir=(
                    post_commit_duplicate_guard_staging_dir
                ),
                duplicate_guard_request_id=(
                    request_id
                    if post_commit_duplicate_guard_path is not None
                    else None
                ),
            )
            if any((
                post_commit_staged_path,
                post_commit_duplicate_guard_path,
            ))
            else None
        ),
    )
