"""Completed-download manifest validation and validated-result dispatch.

This module owns the boundary from a materialized album through beets exact-
release validation and candidate-evidence gating to the staged dispatch handoff.
Completion result tagging remains in :mod:`lib.download_processing`, filesystem
materialization in :mod:`lib.download_materialization`, and reject persistence
in :mod:`lib.download_rejection`.
"""

from __future__ import annotations

import logging
from typing import Protocol, TYPE_CHECKING

from lib import download_materialization
from lib.dispatch import (
    DispatchCoreFn,
    DispatchOutcome,
    QualityGateFn,
    _build_download_info,
    _check_quality_gate_core,
    _requeue_import_job_to_preview,
    dispatch_import_core,
)
from lib.download_rejection import (
    _handle_rejected_result,
    _reject_request_auto_import,
)
from lib.grab_list import GrabListEntry
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
from lib.processing_paths import source_dirs_for_album, stage_to_ai_path
from lib.quality import ValidationResult, compute_effective_override_bitrate
from lib.staged_album import StagedAlbum
from lib.util import log_validation_result

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


class HandleValidFn(Protocol):
    """Exact injection contract for the validated-result handoff."""

    def __call__(
        self,
        album_data: GrabListEntry,
        bv_result: ValidationResult,
        staged_album: StagedAlbum,
        ctx: CratediggerContext,
        *,
        import_job_id: int | None = None,
        prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
        quality_gate_fn: QualityGateFn | None = None,
        dispatch_fn: DispatchCoreFn | None = None,
    ) -> DispatchOutcome | None: ...


class ValidateFn(Protocol):
    """Exact injection contract for materialized-album validation."""

    def __call__(
        self,
        album_data: GrabListEntry,
        staged_album: StagedAlbum,
        ctx: CratediggerContext,
        *,
        import_job_id: int,
        handle_valid_fn: HandleValidFn | None = None,
        dispatch_fn: DispatchCoreFn | None = None,
    ) -> DispatchOutcome | None: ...


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


def _process_beets_validation(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    handle_valid_fn: HandleValidFn | None = None,
    dispatch_fn: DispatchCoreFn | None = None,
) -> DispatchOutcome | None:
    """Validate one exact release and route its canonical result.

    Candidate evidence must already have been produced by preview. Missing
    evidence requeues the job to preview; the importer never measures inline.
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
            import_job_id=import_job_id,
        )
    bv_result = _bv(
        ctx.cfg.beets_harness_path,
        current_path,
        album_data.mb_release_id,
        ctx.cfg.beets_distance_threshold,
    )
    usernames_pre = {f.username for f in album_data.files if f.username}
    bv_result.soulseek_username = (
        ", ".join(sorted(usernames_pre)) if usernames_pre else None
    )
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
            return _requeue_import_job_to_preview(
                db,
                import_job_id=import_job_id,
                reason=reason,
            )
        resolved_handle_valid = (
            handle_valid_fn if handle_valid_fn is not None else _handle_valid_result
        )
        return resolved_handle_valid(
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
) -> DispatchOutcome | None:
    """Stage a valid exact-release result and dispatch request imports.

    The release advisory lock is acquired before the staged move. Redownloads
    only stage for manual review and mark the request done.
    """
    from contextlib import nullcontext
    from lib.pipeline_db import (
        ADVISORY_LOCK_NAMESPACE_RELEASE,
        release_id_to_lock_key,
    )

    source_type = album_data.db_source or "redownload"
    request_id = album_data.db_request_id
    dist = bv_result.distance if bv_result.distance is not None else 1.0
    wants_auto_import = (
        source_type == "request"
        and dist <= ctx.cfg.beets_distance_threshold
    )

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
            import_job_id=import_job_id,
        )

    current_path_location = download_materialization.classify_staged_album_location(
        album_data,
        staged_album,
        ctx,
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
            import_job_id=import_job_id,
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
            release_id_to_lock_key(album_data.mb_release_id),
        )
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
                "idempotently resume from process_completed_album."
            )
            return DispatchOutcome(
                success=False,
                message=(
                    "Another import is already in progress for "
                    f"this release ({album_data.mb_release_id})"
                ),
                deferred=True,
            )

        db = (
            ctx.pipeline_db_source._get_db()
            if ctx.pipeline_db_source is not None
            else None
        )
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
        logger.info(
            f"STAGED: {album_data.artist} - {album_data.title} "
            f"(scenario={bv_result.scenario}, "
            f"distance={bv_result.distance:.4f}) → {dest}"
        )

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
        pending = ctx.pipeline_db_source.mark_done(
            album_data,
            bv_result,
            dest_path=dest,
            download_info=dl_info,
            import_job_id=import_job_id,
        )
        if import_job_id is not None:
            from lib.terminal_outcomes import PendingImportTerminalOutcome
            if isinstance(pending, PendingImportTerminalOutcome):
                return DispatchOutcome(
                    success=True,
                    message="Staged for manual review",
                    terminal_outcome=pending,
                )
        return None


# Executable, pyright-visible proof that production functions implement the
# exact injection contracts used by the completion orchestrator and tests.
_validate_conformance: ValidateFn = _process_beets_validation
_handle_valid_conformance: HandleValidFn = _handle_valid_result
