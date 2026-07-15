"""Completed-download rejection writers and post-rejection convergence."""

from __future__ import annotations

import logging
from typing import Any, TYPE_CHECKING

from lib.dispatch import (
    DispatchOutcome,
    _build_download_info,
    _record_rejection_and_maybe_requeue,
)
from lib.terminal_outcomes import PendingImportTerminalOutcome
from lib.grab_list import GrabListEntry
from lib.import_manifest import (
    move_failed_import_curated,
    tracked_audio_paths_for_downloads,
)
from lib.processing_paths import source_dirs_for_album
from lib.quality import ValidationResult, rejection_backfill_override
from lib.release_identity import normalize_release_id
from lib.staged_album import StagedAlbum
from lib.util import log_validation_result
from lib.wrong_match_policy import rejection_scenario_is_wrong_match_candidate

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


def _run_post_rejection_wrong_match_cleanup(
    ctx: "CratediggerContext",
    download_log_id: object,
    *,
    scenario: str | None,
    import_job_id: int | None = None,
) -> Any:
    """Evaluate newly-created Wrong Matches rows through importer cleanup."""
    if not isinstance(download_log_id, int) or isinstance(download_log_id, bool):
        return None
    if not rejection_scenario_is_wrong_match_candidate(scenario):
        return None
    if ctx.pipeline_db_source is None:
        return None
    get_db = getattr(ctx.pipeline_db_source, "_get_db", None)
    if get_db is None:
        return None
    try:
        from lib.wrong_match_cleanup_service import cleanup_wrong_match

        db = get_db()
        if import_job_id is not None:
            evidence_id = db.get_import_job_candidate_evidence_id(import_job_id)
            if evidence_id is not None:
                db.set_download_log_candidate_evidence(download_log_id, evidence_id)
        result = cleanup_wrong_match(
            db,
            download_log_id,
            ignore_import_job_id=import_job_id,
        )
        logger.info(
            "WRONG-MATCH CLEANUP: download_log_id=%s outcome=%s verdict=%s reason=%s",
            download_log_id,
            getattr(result, "outcome", None),
            getattr(result, "verdict", None),
            getattr(result, "reason", None),
        )
        return result
    except Exception:
        logger.exception(
            "WRONG-MATCH CLEANUP FAILED: download_log_id=%s",
            download_log_id,
        )
        return None


def _resolved_request_rejection_id(
    album_data: GrabListEntry,
    ctx: CratediggerContext,
) -> tuple[Any | None, int | None]:
    """Resolve the backing request row for defensive auto-import rejects."""
    if ctx.pipeline_db_source is None:
        return None, None
    db = ctx.pipeline_db_source._get_db()
    if album_data.db_request_id is not None:
        return db, album_data.db_request_id

    candidate_request_id = album_data.album_id
    if not isinstance(candidate_request_id, int) or isinstance(candidate_request_id, bool):
        return db, None
    if candidate_request_id <= 0:
        return db, None

    request_row = db.get_request(candidate_request_id)
    if not isinstance(request_row, dict):
        return db, None
    if str(request_row.get("artist_name") or "") != album_data.artist:
        return db, None
    if str(request_row.get("album_title") or "") != album_data.title:
        return db, None
    request_year = request_row.get("year")
    if (
        album_data.year
        and request_year not in (None, "")
        and str(request_year) != album_data.year
    ):
        return db, None
    album_release_id = str(album_data.mb_release_id or "")
    request_release_id = str(request_row.get("mb_release_id") or "")
    if bool(album_release_id) != bool(request_release_id):
        return db, None
    if album_release_id and request_release_id != album_release_id:
        return db, None
    return db, candidate_request_id


def _reject_request_auto_import(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    detail: str,
    scenario: str | None,
    error: str,
    import_job_id: int | None = None,
) -> DispatchOutcome:
    """Reject a request auto-import when ownership can be proven safely."""
    db, request_id = _resolved_request_rejection_id(album_data, ctx)
    if db is None or request_id is None:
        logger.error(
            "AUTO-IMPORT REJECT BLOCKED WITHOUT REQUEST AUDIT: album_id=%s %s - %s "
            "(scenario=%s) could not resolve a safe pipeline request row; "
            "files remain at %s and automatic retry/import is disabled until "
            "manual recovery.",
            album_data.album_id,
            album_data.artist,
            album_data.title,
            scenario,
            staged_album.current_path,
        )
        return DispatchOutcome(success=False, message=detail, deferred=True)

    failed_result = ValidationResult(
        distance=bv_result.distance,
        scenario=scenario,
        detail=detail,
        error=error,
    )
    failed_result.source_dirs = source_dirs_for_album(album_data)
    failed_result.failed_path = move_failed_import_curated(
        staged_album.current_path,
        allowed_audio=tracked_audio_paths_for_downloads(album_data.files),
        scenario=failed_result.scenario,
    )
    logger.error(
        "AUTO-IMPORT REJECTED: %s - %s — %s",
        album_data.artist,
        album_data.title,
        detail,
    )
    log_validation_result(album_data, failed_result, ctx.cfg)

    dl_info = _build_download_info(album_data)
    if album_data.download_spectral is not None:
        dl_info.download_spectral = album_data.download_spectral
        dl_info.current_spectral = album_data.current_spectral
        dl_info.existing_min_bitrate = album_data.current_min_bitrate
        dl_info.slskd_filetype = dl_info.filetype
        dl_info.actual_filetype = dl_info.filetype
    owned_import_job_id = (
        import_job_id
        if import_job_id is not None and db.get_import_job(import_job_id) is not None
        else None
    )
    persisted = _record_rejection_and_maybe_requeue(
        db,
        request_id,
        dl_info,
        detail=detail,
        error=failed_result.error,
        validation_result=failed_result.to_json(),
        requeue=True,
        import_job_id=owned_import_job_id,
    )
    if isinstance(persisted, PendingImportTerminalOutcome):
        return DispatchOutcome(
            success=False,
            message=detail,
            terminal_outcome=persisted,
            post_commit_wrong_match_scenario=failed_result.scenario,
        )
    _run_post_rejection_wrong_match_cleanup(
        ctx,
        persisted,
        scenario=failed_result.scenario,
    )
    return DispatchOutcome(success=False, message=detail)


def _handle_rejected_result(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int | None = None,
) -> DispatchOutcome:
    """Handle a rejected beets validation result."""
    bv_result.source_dirs = source_dirs_for_album(album_data)
    bv_result.failed_path = move_failed_import_curated(
        staged_album.current_path,
        allowed_audio=tracked_audio_paths_for_downloads(album_data.files),
        scenario=bv_result.scenario,
    )
    log_validation_result(album_data, bv_result, ctx.cfg)
    usernames = {file.username for file in album_data.files}
    bv_result.denylisted_users = sorted(usernames)
    dl_info = _build_download_info(album_data)
    dl_info.validation_result = bv_result.to_json()
    if album_data.download_spectral is not None:
        dl_info.download_spectral = album_data.download_spectral
        dl_info.current_spectral = album_data.current_spectral
        dl_info.existing_min_bitrate = album_data.current_min_bitrate
        dl_info.slskd_filetype = dl_info.filetype
        dl_info.actual_filetype = dl_info.filetype

    db = ctx.pipeline_db_source._get_db()
    owned_import_job_id = (
        import_job_id
        if import_job_id is not None and db.get_import_job(import_job_id) is not None
        else None
    )
    persisted = ctx.pipeline_db_source.reject_and_requeue(
        album_data,
        bv_result,
        usernames=usernames,
        download_info=dl_info,
        search_filetype_override=_compute_rejection_backfill(album_data, ctx),
        cooled_down_users=ctx.cooled_down_users,
        import_job_id=owned_import_job_id,
    )
    pending = (
        persisted if isinstance(persisted, PendingImportTerminalOutcome) else None
    )
    if pending is None:
        _run_post_rejection_wrong_match_cleanup(
            ctx,
            persisted,
            scenario=bv_result.scenario,
            import_job_id=import_job_id,
        )
    logger.warning(
        "REJECTED: %s - %s (scenario=%s, distance=%s, detail=%s) "
        "| denylisted users: %s",
        album_data.artist,
        album_data.title,
        bv_result.scenario,
        bv_result.distance,
        bv_result.detail,
        ", ".join(usernames),
    )
    scenario = bv_result.scenario or "validation_rejected"
    detail = bv_result.detail or bv_result.error
    message = f"Rejected: {scenario}"
    if detail:
        message = f"{message} - {detail}"
    return DispatchOutcome(
        success=False,
        message=message,
        terminal_outcome=pending if import_job_id is not None else None,
        post_commit_wrong_match_scenario=(
            bv_result.scenario if import_job_id is not None else None
        ),
    )


def _compute_rejection_backfill(
    album_data: GrabListEntry,
    ctx: CratediggerContext,
) -> str | None:
    """Narrow from linked current evidence after a validation rejection."""
    request_id = album_data.db_request_id
    if not request_id or not ctx.pipeline_db_source:
        return None
    if album_data.db_search_filetype_override:
        return None
    try:
        db = ctx.pipeline_db_source._get_db()
        request = db.get_request(request_id)
        if not request or request.get("search_filetype_override"):
            return None
        evidence_id = db.get_request_current_evidence_id(request_id)
        if evidence_id is None:
            return None
        evidence = db.load_album_quality_evidence_by_id(evidence_id)
        if evidence is None or evidence.policy_incomplete_reasons():
            return None
        if (
            normalize_release_id(evidence.mb_release_id)
            != normalize_release_id(album_data.mb_release_id)
        ):
            return None
        override = rejection_backfill_override(
            current_measurement=evidence.measurement,
            spectral_evidence_source="linked_current_evidence",
            cfg=ctx.cfg.quality_ranks,
        )
        if override:
            logger.info(
                "BACKFILL: %s - %s search_filetype_override=NULL → %r "
                "(linked current evidence: format=%s, spectral=%s)",
                album_data.artist,
                album_data.title,
                override,
                evidence.measurement.format,
                evidence.measurement.spectral_grade,
            )
        return override
    except Exception:
        logger.debug("BACKFILL: failed to load linked current evidence", exc_info=True)
        return None
