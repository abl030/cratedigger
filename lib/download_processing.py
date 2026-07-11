"""Completed-download validation and dispatch orchestration.

Materialization and interrupted-import recovery live in
``lib.download_materialization``. Focused reject writers live in
``lib.download_rejection``; the poll state machine lives in ``lib.download``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, TYPE_CHECKING

from lib import download_materialization
from lib.grab_list import GrabListEntry
from lib.dispatch import (DispatchCoreFn, DispatchOutcome, QualityGateFn,
                          _build_download_info,
                          _check_quality_gate_core,
                          _requeue_import_job_to_preview,
                          dispatch_import_core)
from lib.download_rejection import (
    _handle_rejected_result,
    _reject_request_auto_import,
)
from lib.import_evidence import (
    CandidateEvidenceActionResult,
    ensure_candidate_evidence_for_action,
)
from lib.import_manifest import (
    audio_relative_paths,
    check_audio_manifest,
    manifest_trace_summary,
    tracked_audio_paths_for_downloads,
)
from lib.processing_paths import (
    canonical_folder_for_row,
    source_dirs_for_album,
    stage_to_ai_path,
)
from lib.quality import ValidationResult, compute_effective_override_bitrate
from lib.staged_album import StagedAlbum
from lib.util import log_validation_result

if TYPE_CHECKING:
    from lib.context import CratediggerContext
    from lib.download import DownloadDB

logger = logging.getLogger("cratedigger")


@dataclass(frozen=True)
class Completed:
    """``process_completed_album`` succeeded without producing a dispatch
    summary — no validation configured, or the redownload path already
    called ``mark_done`` directly. Caller finalizes to ``imported`` only
    if the request row is still ``downloading``. Historical bare ``True``.
    """


@dataclass(frozen=True)
class CompletionFailed:
    """A non-dispatch local failure (materialization failed). Caller
    resets to ``wanted`` only if the request row is still ``downloading``.
    Historical bare ``False``.
    """

    reason: str


@dataclass(frozen=True)
class CompletionDispatched:
    """The validation/dispatch path already owned the request transition.

    ``outcome`` is an import summary for the queue owner ONLY — it must
    NEVER drive a fallback status transition. Historical raw
    ``DispatchOutcome`` return value.
    """

    outcome: DispatchOutcome


@dataclass(frozen=True)
class CompletionDeferred:
    """The path intentionally left request state untouched: release-lock
    contention, a guarded post-move staged path, or an ownership-less
    reject needing manual recovery. Caller must NOT touch status.
    Historical bare ``None``.
    """

    detail: str


CompletionResult = Completed | CompletionFailed | CompletionDispatched | CompletionDeferred
"""Return type of ``process_completed_album`` / ``_run_completed_processing``."""


def _check_staged_audio_manifest(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
) -> tuple[bool, str]:
    check = check_audio_manifest(
        staged_album.current_path,
        tracked_audio_paths_for_downloads(album_data.files),
    )
    if check.ok:
        return True, ""
    detail = (
        "Staged import folder does not match the selected audio manifest: "
        f"{check.detail()}"
    )
    logger.error(
        "IMPORT MANIFEST REJECTED: request_id=%s path=%s %s",
        album_data.db_request_id,
        staged_album.current_path,
        detail,
    )
    return False, detail


def process_completed_album(
    album_data: GrabListEntry,
    failed_grab: list[Any],
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    validate_fn: "Callable[..., DispatchOutcome | None] | None" = None,
    handle_valid_fn: "Callable[..., DispatchOutcome | None] | None" = None,
    dispatch_fn: DispatchCoreFn | None = None,
) -> CompletionResult:
    """Process a fully-downloaded album: move files, tag, validate, stage/import.

    Returns the local processing result (see ``CompletionResult`` variants):
    - ``Completed`` — local non-dispatch processing succeeded. Outer caller
      may finalize to ``imported`` only if the request row is still
      ``downloading``. Historical bare ``True``.
    - ``CompletionFailed`` — local non-dispatch processing failed. Outer
      caller resets to ``wanted`` only if the request row is still
      ``downloading``. Historical bare ``False``.
    - ``CompletionDispatched`` — the validation / dispatch path already owned
      the request transition; ``.outcome`` is an import summary for the
      queue owner only. Historical raw ``DispatchOutcome``.
    - ``CompletionDeferred`` — the validation / dispatch path intentionally
      left state untouched for retry / manual recovery. Outer caller must
      NOT touch status. Historical bare ``None``.
    """
    staged_album = StagedAlbum.from_entry(
        album_data,
        default_path=canonical_folder_for_row(
            album_data, ctx.cfg.slskd_download_dir),
    )
    materialized = download_materialization._materialize_processing_dir(
        album_data, staged_album, ctx,
    )
    if isinstance(materialized, download_materialization.MaterializeFailed):
        return CompletionFailed(reason=materialized.reason)
    if isinstance(materialized, download_materialization.MaterializeGuarded):
        return CompletionDeferred(detail=materialized.detail)
    assert isinstance(materialized, download_materialization.Materialized)

    logger.info(f"Processing completed download: {album_data.artist} - {album_data.title}")
    if ctx.cfg.beets_validation_enabled and album_data.mb_release_id:
        _validate = validate_fn if validate_fn is not None else _process_beets_validation
        outcome = _validate(
            album_data,
            staged_album,
            ctx,
            import_job_id=import_job_id,
            handle_valid_fn=handle_valid_fn,
            dispatch_fn=dispatch_fn,
        )
        if outcome is not None:
            if outcome.deferred:
                # Release-lock contention. Propagate ``CompletionDeferred``
                # so ``_run_completed_processing`` leaves the request's
                # status, active_download_state, and staged files
                # untouched for the next cycle to retry.
                return CompletionDeferred(detail=outcome.message)
            # DispatchOutcome is an import summary only. Wrap it so the
            # importer queue can record the real terminal job outcome, but do
            # not let it drive fallback request-status transitions below.
            return CompletionDispatched(outcome=outcome)
    return Completed()


def _process_beets_validation(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    handle_valid_fn: "Callable[..., DispatchOutcome | None] | None" = None,
    dispatch_fn: DispatchCoreFn | None = None,
) -> "DispatchOutcome | None":
    """Beets validation sub-path of process_completed_album.

    After beets validation passes, ``ensure_candidate_evidence_for_action``
    confirms the preview worker has persisted candidate evidence keyed to
    this import_job. Missing evidence requeues the job to preview rather
    than measuring inline — the importer never measures, the preview
    worker owns evidence production, and the full pipeline decider
    (``full_pipeline_decision_from_evidence``) runs downstream of evidence.

    Returns the dispatch outcome when the auto-import path fires,
    ``None`` when beets validation rejects (``_handle_rejected_result``
    already handles the state transition) or when the non-auto
    redownload path takes over in ``_handle_valid_result``. Guarded
    ownership-less rejects also return a deferred outcome so callers
    keep the row untouched for manual recovery.
    """
    from lib.beets import beets_validate as _bv
    current_path = staged_album.current_path
    manifest_ok, manifest_detail = _check_staged_audio_manifest(
        album_data,
        staged_album,
    )
    logger.info(
        "MANIFEST-TRACE check request=%s ok=%s %s actual_audio=%s path=%s",
        album_data.db_request_id,
        manifest_ok,
        manifest_trace_summary(album_data.files),
        len(audio_relative_paths(current_path)),
        current_path,
    )
    if not manifest_ok:
        return _reject_request_auto_import(
            album_data,
            ValidationResult(
                valid=False,
                scenario="untracked_audio",
                detail=manifest_detail,
                error=manifest_detail,
                path=current_path,
            ),
            staged_album,
            ctx,
            detail=manifest_detail,
            scenario="untracked_audio",
            error=manifest_detail,
        )
    bv_result = _bv(ctx.cfg.beets_harness_path, current_path,
                    album_data.mb_release_id, ctx.cfg.beets_distance_threshold)
    usernames_pre = set(f.username for f in album_data.files if f.username)
    bv_result.soulseek_username = ", ".join(sorted(usernames_pre)) if usernames_pre else None
    bv_result.download_folder = current_path
    bv_result.source_dirs = source_dirs_for_album(album_data)
    if bv_result.valid:
        db = ctx.pipeline_db_source._get_db()
        candidate_result = ensure_candidate_evidence_for_action(
            db,
            source_path=current_path,
            import_job_id=import_job_id,
        )
        if not candidate_result.available:
            reason = (
                candidate_result.provenance.fallback_reason
                or candidate_result.provenance.candidate_status
                or "missing"
            )
            # Preview owns candidate-evidence production; the importer
            # never measures. Requeue rather than fail; the dispatch-side
            # requeue keeps the advisory-lock atomicity intact.
            return _requeue_import_job_to_preview(
                db,
                import_job_id=import_job_id,
                reason=reason,
            )
        _handle_valid = (
            handle_valid_fn if handle_valid_fn is not None else _handle_valid_result
        )
        return _handle_valid(
            album_data,
            bv_result,
            staged_album,
            ctx,
            import_job_id=import_job_id,
            prevalidated_candidate_result=candidate_result,
            dispatch_fn=dispatch_fn,
        )
    return _handle_rejected_result(
        album_data,
        bv_result,
        staged_album,
        ctx,
        import_job_id=import_job_id,
    )


def _handle_valid_result(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int | None = None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    quality_gate_fn: QualityGateFn | None = None,
    dispatch_fn: DispatchCoreFn | None = None,
) -> "DispatchOutcome | None":
    """Handle a valid beets validation result: stage and optionally auto-import.

    Returns the ``DispatchOutcome`` summary from ``dispatch_import_core``
    when the auto-import path fires (source='request', distance within
    threshold), or ``None`` for the redownload path that just stages
    and marks done. ``process_completed_album()`` propagates the summary
    upward for the importer queue, but request-state changes remain owned
    by the dispatch/finalization seam itself.

    This function acquires the RELEASE advisory lock outer for the
    auto-import path *before* ``StagedAlbum.move_to`` runs, so
    contention is a true no-op: files stay at their current local
    processing path, ``active_download_state.current_path`` stays
    unchanged, and the next cycle can idempotently re-enter without
    any extra filesystem churn. Redownload paths don't take the lock
    — they just move into staging and mark done, so no cross-process
    race applies.

    See ``docs/advisory-locks.md`` for namespaces, keys, ordering,
    and contention behaviour (including the staged-move rationale for
    acquiring at this level rather than inside
    ``dispatch_import_core``).
    """
    from contextlib import nullcontext
    from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                 release_id_to_lock_key)

    source_type = album_data.db_source or "redownload"
    request_id = album_data.db_request_id
    dist = bv_result.distance if bv_result.distance is not None else 1.0
    wants_auto_import = (
        source_type == "request"
        and dist <= ctx.cfg.beets_distance_threshold)

    if wants_auto_import and request_id is None:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail=(
                "Request auto-import is missing db_request_id; automatic "
                "resume/import is disabled."
            ),
            scenario="request_missing_request_id",
            error="missing_request_id",
        )

    current_path_location = download_materialization.classify_staged_album_location(
        album_data, staged_album, ctx,
    )

    if wants_auto_import and not album_data.mb_release_id:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail="Request auto-import requires a MusicBrainz release ID",
            scenario="request_missing_mbid",
            error="missing_mbid",
        )

    will_auto_import = wants_auto_import
    pdb = None

    if (
        will_auto_import
        and current_path_location.blocks_auto_import_dispatch
        and download_materialization._import_subprocess_already_started(
            ctx.pipeline_db_source._get_db()
            if ctx.pipeline_db_source is not None
            else None,
            request_id,
        )
    ):
        download_materialization._log_post_move_resume_blocked(
            album_data,
            current_path=staged_album.current_path,
            detail=(
                f"already lives at the {current_path_location.display_name}. "
                "Automatic retry is disabled to avoid duplicate import; "
                "manual recovery is required."
            ),
        )
        return DispatchOutcome(
            success=False,
            message=(
                "Auto-import may already have started for this staged "
                f"album ({album_data.mb_release_id})"
            ),
            deferred=True,
        )

    if will_auto_import and album_data.mb_release_id:
        pdb = ctx.pipeline_db_source._get_db()
        lock_ctx = pdb.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(album_data.mb_release_id))
    else:
        lock_ctx = nullcontext(True)

    with lock_ctx as got_release_lock:
        if not got_release_lock:
            logger.warning(
                f"AUTO-IMPORT DEFERRED: {album_data.artist} - "
                f"{album_data.title} — release lock held by another "
                f"process (mbid={album_data.mb_release_id}); skipping "
                "staged move and dispatch. Files stay at "
                f"{staged_album.current_path} so the next cycle can "
                "idempotently resume from process_completed_album.")
            return DispatchOutcome(
                success=False,
                message=("Another import is already in progress for "
                         f"this release ({album_data.mb_release_id})"),
                deferred=True,
            )

        db = (ctx.pipeline_db_source._get_db()
              if ctx.pipeline_db_source is not None else None)
        dest = staged_album.move_to(
            stage_to_ai_path(
                artist=album_data.artist,
                title=album_data.title,
                staging_dir=ctx.cfg.beets_staging_dir,
                request_id=request_id,
                auto_import=will_auto_import,
            ),
            db=db,
        )
        album_data.import_folder = dest
        log_validation_result(album_data, bv_result, ctx.cfg, dest_path=dest)
        logger.info(f"STAGED: {album_data.artist} - {album_data.title} "
                    f"(scenario={bv_result.scenario}, "
                    f"distance={bv_result.distance:.4f}) → {dest}")

        dl_info = _build_download_info(album_data)
        dl_info.validation_result = bv_result.to_json()
        if album_data.download_spectral is not None:
            dl_info.download_spectral = album_data.download_spectral
            dl_info.current_spectral = album_data.current_spectral
            dl_info.existing_min_bitrate = album_data.current_min_bitrate
            dl_info.slskd_filetype = dl_info.filetype
            dl_info.actual_filetype = dl_info.filetype
        if will_auto_import:
            assert request_id is not None, "pipeline request must have db_request_id"
            assert pdb is not None, "auto-import path must hold a pipeline DB handle"
            override_min_bitrate: int | None = None
            try:
                req = pdb.get_request(request_id)
                if req:
                    override_min_bitrate = compute_effective_override_bitrate(
                        req.get("min_bitrate"),
                        req.get("current_spectral_bitrate"),
                        req.get("current_spectral_grade"),
                    )
            except Exception:
                logger.debug("DB lookup failed for override-min-bitrate")

            resolved_quality_gate_fn = (
                quality_gate_fn
                if quality_gate_fn is not None
                else _check_quality_gate_core
            )
            if dispatch_fn is not None:
                # Test-only dependency injection seam. Its exact protocol is
                # ``DispatchCoreFn``; production always takes the direct,
                # pyright-checked call below.
                return dispatch_fn(
                    path=dest,
                    mb_release_id=album_data.mb_release_id or "",
                    request_id=request_id,
                    label=f"{album_data.artist} - {album_data.title}",
                    force=False,
                    override_min_bitrate=override_min_bitrate,
                    target_format=album_data.db_target_format,
                    verified_lossless_target=ctx.cfg.verified_lossless_target,
                    beets_harness_path=ctx.cfg.beets_harness_path,
                    db=pdb,
                    dl_info=dl_info,
                    distance=bv_result.distance,
                    scenario=bv_result.scenario or "auto_import",
                    files=album_data.files,
                    cfg=ctx.cfg,
                    outcome_label="success",
                    requeue_on_failure=True,
                    cooled_down_users=ctx.cooled_down_users,
                    source_dirs=source_dirs_for_album(album_data),
                    candidate_import_job_id=import_job_id,
                    candidate_download_log_id=None,
                    prevalidated_candidate_result=prevalidated_candidate_result,
                    quality_gate_fn=resolved_quality_gate_fn,
                )
            return dispatch_import_core(
                path=dest,
                mb_release_id=album_data.mb_release_id or "",
                request_id=request_id,
                label=f"{album_data.artist} - {album_data.title}",
                force=False,
                override_min_bitrate=override_min_bitrate,
                target_format=album_data.db_target_format,
                verified_lossless_target=ctx.cfg.verified_lossless_target,
                beets_harness_path=ctx.cfg.beets_harness_path,
                db=pdb,
                dl_info=dl_info,
                distance=bv_result.distance,
                scenario=bv_result.scenario or "auto_import",
                files=album_data.files,
                cfg=ctx.cfg,
                outcome_label="success",
                requeue_on_failure=True,
                cooled_down_users=ctx.cooled_down_users,
                source_dirs=source_dirs_for_album(album_data),
                candidate_import_job_id=import_job_id,
                candidate_download_log_id=None,
                prevalidated_candidate_result=prevalidated_candidate_result,
                quality_gate_fn=resolved_quality_gate_fn,
            )
        ctx.pipeline_db_source.mark_done(
            album_data, bv_result, dest_path=dest, download_info=dl_info)
        return None
