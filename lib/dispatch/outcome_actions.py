"""Outcome-time rejection / mark-done writers.

The download_log-writing side of dispatch: mark an import done, record a
rejection (with optional self-heal requeue), and the unified persisted-
evidence reject helper. Each terminal helper submits one typed command to the
DB-owned transaction boundary.
"""

from __future__ import annotations

import logging
from typing import Sequence, TYPE_CHECKING

import msgspec

from lib.quality import (DownloadInfo, ValidationResult, dispatch_action, extract_usernames,
                         is_comparable_lossless_source_probe)
from lib.terminal_outcomes import (
    DenylistWrite,
    DownloadAuditWrite,
    ImportedRequestWrite,
    ImportJobOutcomeResult,
    ImportSuccessOutcome,
    ImporterRejectionOutcome,
    PreviewMeasurementFailureOutcome,
)
from lib.validation_envelope import derive_validation_log_columns

from lib.dispatch.types import (DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                                DispatchOutcome, ImportAttemptResult,
                                _PREIMPORT_FACT_REJECT_DECISIONS)
from lib.dispatch.helpers import (_cleanup_staged_dir,
                                  _populate_dl_info_from_import_result,
                                  _should_cleanup_path)

if TYPE_CHECKING:
    from lib.pipeline_db import DownloadLogOutcome, PipelineDB
    from lib.quality import ImportResult, MeasurementFailure

logger = logging.getLogger("cratedigger")


def _download_audit_write(
    dl_info: DownloadInfo,
    *,
    outcome: str,
    detail: str | None,
    staged_path: str | None,
    validation_result: str | None,
    error: str | None = None,
    fallback_distance: float | None = None,
    fallback_scenario: str | None = None,
) -> DownloadAuditWrite:
    """Project ``DownloadInfo`` into the typed terminal-audit contract."""
    if validation_result:
        beets_distance, beets_scenario = derive_validation_log_columns(
            validation_result,
        )
    else:
        beets_distance, beets_scenario = derive_validation_log_columns(
            validation_result,
            beets_distance=fallback_distance,
            beets_scenario=fallback_scenario,
        )
    probe = dl_info.v0_probe
    existing_probe = dl_info.existing_v0_probe
    return DownloadAuditWrite(
        outcome=outcome,
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        beets_distance=beets_distance,
        beets_scenario=beets_scenario,
        beets_detail=detail,
        staged_path=staged_path,
        error_message=error,
        bitrate=dl_info.bitrate,
        sample_rate=dl_info.sample_rate,
        bit_depth=dl_info.bit_depth,
        is_vbr=dl_info.is_vbr,
        was_converted=dl_info.was_converted,
        original_filetype=dl_info.original_filetype,
        slskd_filetype=dl_info.slskd_filetype,
        actual_filetype=dl_info.actual_filetype,
        actual_min_bitrate=dl_info.actual_min_bitrate,
        spectral_grade=(
            dl_info.download_spectral.grade
            if dl_info.download_spectral is not None
            else None
        ),
        spectral_bitrate=(
            dl_info.download_spectral.bitrate_kbps
            if dl_info.download_spectral is not None
            else None
        ),
        existing_min_bitrate=dl_info.existing_min_bitrate,
        existing_spectral_bitrate=(
            dl_info.current_spectral.bitrate_kbps
            if dl_info.current_spectral is not None
            else None
        ),
        import_result_json=dl_info.import_result,
        validation_result_json=validation_result,
        final_format=dl_info.final_format,
        v0_probe_kind=probe.kind if probe is not None else None,
        v0_probe_min_bitrate=(
            probe.min_bitrate_kbps if probe is not None else None
        ),
        v0_probe_avg_bitrate=(
            probe.avg_bitrate_kbps if probe is not None else None
        ),
        v0_probe_median_bitrate=(
            probe.median_bitrate_kbps if probe is not None else None
        ),
        existing_v0_probe_kind=(
            existing_probe.kind if existing_probe is not None else None
        ),
        existing_v0_probe_min_bitrate=(
            existing_probe.min_bitrate_kbps
            if existing_probe is not None
            else None
        ),
        existing_v0_probe_avg_bitrate=(
            existing_probe.avg_bitrate_kbps
            if existing_probe is not None
            else None
        ),
        existing_v0_probe_median_bitrate=(
            existing_probe.median_bitrate_kbps
            if existing_probe is not None
            else None
        ),
    )


def _reject_import_from_evidence_decision(
    *,
    db: "PipelineDB",
    request_id: int,
    dl_info: DownloadInfo,
    attempt_result: ImportAttemptResult,
    distance: float | None,
    decision: str,
    detail: str,
    requeue_on_failure: bool,
    validation_result: str | None,
    staged_path: str,
    scenario: str,
    files: Sequence[object] | None,
    source_path_cleanup_scenario: str,
    cooled_down_users: set[str] | None,
    import_job_id: int | None = None,
) -> DispatchOutcome:
    """Record a persisted-evidence rejection before beets can mutate files.

    Unified rejection helper for every ``full_pipeline_decision_from_evidence``
    reject outcome — quality-side (downgrade / suspect_lossless / etc.) AND
    folder/audio-integrity (audio_corrupt / bad_audio_hash / nested_layout /
    empty_fileset, formerly routed through the deleted
    ``_route_preimport_decision_reject``). One decision function, one
    rejection helper, one denylist policy.

    Reads the owner's richest result through
    ``_populate_dl_info_from_import_result``
    so the same top-level ``download_log`` columns the post-import reject
    path populates (``bitrate``, ``actual_filetype``, ``spectral_grade``,
    ``existing_min_bitrate``, ``v0_probe_*``, etc.) get filled here too.
    Without this, the Recents UI rendered evidence-decision rejections
    as just ``"downgrade · username"`` because every quality column
    came back NULL — see ``TestRejectImportFromEvidenceDecision``.

    **U11 forced-requeue invariant.** When ``decision`` names a
    folder/audio-integrity fact (``_PREIMPORT_FACT_REJECT_DECISIONS``), the
    helper forces ``requeue=True`` regardless of the caller's
    ``requeue_on_failure`` flag. These rejects fire upstream of any beets
    mutation and upstream of any operator intent — the album is still
    desired, only this specific source is bad — so the parent request must
    always self-heal back to ``wanted``. Quality-side rejects continue to
    honour ``requeue_on_failure`` (force/manual paths pass ``False``
    because the operator already chose to act on this source).
    """

    import_result = attempt_result.result
    if import_result is None:
        raise RuntimeError("persisted-evidence rejection requires an import result")
    _populate_dl_info_from_import_result(dl_info, import_result)
    action = dispatch_action(decision)
    # U11: force requeue on folder/audio-integrity rejects (formerly the
    # invariant enforced by the deleted ``_route_preimport_decision_reject``).
    effective_requeue = requeue_on_failure or decision in _PREIMPORT_FACT_REJECT_DECISIONS
    rejection_validation = validation_result or ValidationResult(
        distance=distance,
        scenario=decision or scenario,
        detail=detail,
    ).to_json()
    denylist: tuple[DenylistWrite, ...] = ()
    usernames: set[str] = set()
    if action.denylist:
        usernames = extract_usernames(files or [])
        if dl_info.username:
            usernames.add(dl_info.username)
        # Unified denylist policy. Quality-side and four-fact reject reasons
        # both live here — formerly split across ``_route_preimport_decision_reject``
        # (folder/audio-integrity) and the quality-side branch below.
        reason = (
            "quality downgrade prevented"
            if decision == "downgrade"
            else "suspect lossless source not an upgrade"
            if decision.startswith("suspect_lossless")
            else "lossless source locked"
            if decision == "lossless_source_locked"
            else "audio decode failures"
            if decision == "audio_corrupt"
            else "matched curated bad audio hash"
            if decision == "bad_audio_hash"
            else "spectral analysis rejected the source"
            if decision == "spectral_reject"
            else "mixed lossless+lossy source"
            if decision == "mixed_source"
            else f"rejected: {decision}"
        )
        denylist = tuple(
            DenylistWrite(username=username, reason=reason)
            for username in sorted(usernames)
        )
    dispatch_outcome = DispatchOutcome(
        success=False,
        message=f"Rejected by persisted quality evidence: {decision}",
        code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    )
    _record_rejection_and_maybe_requeue(
        db,
        request_id,
        dl_info,
        detail=detail,
        error=None,
        requeue=effective_requeue,
        outcome_label="rejected",
        validation_result=rejection_validation,
        staged_path=staged_path,
        attempt_result=attempt_result,
        import_job_id=import_job_id,
        denylist=denylist,
        job_result=ImportJobOutcomeResult(
            success=dispatch_outcome.success,
            message=dispatch_outcome.message,
            deferred=dispatch_outcome.deferred,
            code=dispatch_outcome.code,
        ),
        job_error=dispatch_outcome.message,
        job_message=dispatch_outcome.message,
    )
    for username in usernames:
        if cooled_down_users is not None and db.check_and_apply_cooldown(username):
            cooled_down_users.add(username)
    if action.cleanup and _should_cleanup_path(source_path_cleanup_scenario, action):
        try:
            _cleanup_staged_dir(staged_path)
        except Exception:
            logger.exception(
                "Post-rejection staged cleanup failed for request %s",
                request_id,
            )
    return dispatch_outcome


def _do_mark_done(
    db: "PipelineDB",
    request_id: int,
    dl_info: DownloadInfo,
    distance: float | None,
    scenario: str | None,
    dest_path: str | None,
    outcome_label: DownloadLogOutcome = "success",
    detail: str | None = None,
    imported_path: str | None = None,
    clear_stale_v0_probe: bool = True,
    attempt_result: ImportAttemptResult | None = None,
    import_job_id: int | None = None,
    denylist: tuple[DenylistWrite, ...] = (),
    requeue_after_import: bool = False,
    requeue_search_filetype_override: str | None = None,
    requeue_min_bitrate: int | None = None,
    write_quality_delta: bool = False,
    prev_min_bitrate: int | None = None,
    min_bitrate: int | None = None,
    job_result: ImportJobOutcomeResult | None = None,
    job_message: str = "Import successful",
) -> int | None:
    """Mark album as imported — standalone version of DatabaseSource.mark_done.

    Takes PipelineDB directly instead of going through DatabaseSource.
    Uses outcome_label for download_log (e.g. "force_import" instead of "success").

    ``imported_path`` is the beets destination (from
    ``ImportResult.postflight.imported_path``) — what shows up in the UI's
    "Imported to" label. ``dest_path`` is the source/staging path passed to
    the importer. When callers have both (auto/force/manual paths that ran
    beets), they pass ``imported_path`` so ``album_requests.imported_path``
    reflects the actual on-disk location. Callers that only stage for manual
    review (``album_source.mark_done``) leave ``imported_path=None``; it
    falls back to ``dest_path`` so legacy behavior is preserved (issue #93).
    """
    from lib.quality import SpectralMeasurement, is_verified_lossless
    verified_lossless = (
        bool(dl_info.verified_lossless_override)
        if dl_info.verified_lossless_override is not None
        else is_verified_lossless(
            dl_info.was_converted,
            dl_info.original_filetype,
            dl_info.download_spectral.grade if dl_info.download_spectral else None,
        )
    )
    # Persist the full current quality state, not only truthy upgrades.
    # Otherwise old verified/final-format labels leak into later imports.
    write_spectral = dl_info.download_spectral is not None
    last_download_spectral_grade: str | None = None
    last_download_spectral_bitrate: int | None = None
    current_spectral_grade: str | None = None
    current_spectral_bitrate: int | None = None
    if dl_info.download_spectral is not None:
        current_spectral = dl_info.download_spectral
        if verified_lossless and dl_info.bitrate:
            current_spectral = SpectralMeasurement(
                grade=dl_info.download_spectral.grade,
                bitrate_kbps=dl_info.bitrate // 1000,
            )
        last_download_spectral_grade = dl_info.download_spectral.grade
        last_download_spectral_bitrate = dl_info.download_spectral.bitrate_kbps
        current_spectral_grade = current_spectral.grade
        current_spectral_bitrate = current_spectral.bitrate_kbps
    write_v0_probe = False
    current_probe_min: int | None = None
    current_probe_avg: int | None = None
    current_probe_median: int | None = None
    if is_comparable_lossless_source_probe(dl_info.v0_probe):
        write_v0_probe = True
        assert dl_info.v0_probe is not None
        current_probe_min = dl_info.v0_probe.min_bitrate_kbps
        current_probe_avg = dl_info.v0_probe.avg_bitrate_kbps
        current_probe_median = dl_info.v0_probe.median_bitrate_kbps
    elif clear_stale_v0_probe:
        write_v0_probe = True

    validation_result = dl_info.validation_result or ValidationResult(
        valid=True,
        distance=distance,
        scenario=scenario,
        detail=detail,
    ).to_json()
    if attempt_result is not None:
        attempt_result.finalize_into(dl_info)
    applied = db.persist_import_success(
        ImportSuccessOutcome(
            request_id=request_id,
            request=ImportedRequestWrite(
                beets_distance=distance,
                beets_scenario=scenario,
                imported_path=imported_path if imported_path else dest_path,
                verified_lossless=verified_lossless,
                final_format=dl_info.final_format,
                write_spectral=write_spectral,
                last_download_spectral_grade=last_download_spectral_grade,
                last_download_spectral_bitrate=last_download_spectral_bitrate,
                current_spectral_grade=current_spectral_grade,
                current_spectral_bitrate=current_spectral_bitrate,
                write_v0_probe=write_v0_probe,
                current_lossless_source_v0_probe_min_bitrate=current_probe_min,
                current_lossless_source_v0_probe_avg_bitrate=current_probe_avg,
                current_lossless_source_v0_probe_median_bitrate=current_probe_median,
                write_quality_delta=write_quality_delta,
                prev_min_bitrate=prev_min_bitrate,
                min_bitrate=min_bitrate,
            ),
            audit=_download_audit_write(
                dl_info,
                outcome=outcome_label,
                detail=detail,
                staged_path=dest_path,
                validation_result=validation_result,
            ),
            import_job_id=import_job_id,
            denylist=denylist,
            requeue_after_import=requeue_after_import,
            requeue_search_filetype_override=requeue_search_filetype_override,
            requeue_min_bitrate=requeue_min_bitrate,
            job_result=job_result or ImportJobOutcomeResult(
                success=True,
                message=job_message,
                deferred=False,
                code=None,
            ),
            job_message=job_message,
        )
    )
    return applied.download_log_id


def _record_rejection_and_maybe_requeue(
    db: "PipelineDB",
    request_id: int,
    dl_info: DownloadInfo,
    detail: str | None,
    error: str | None,
    *,
    validation_result: str,
    requeue: bool = True,
    outcome_label: DownloadLogOutcome = "rejected",
    search_filetype_override: str | None = None,
    staged_path: str | None = None,
    attempt_result: ImportAttemptResult | None = None,
    import_job_id: int | None = None,
    denylist: tuple[DenylistWrite, ...] = (),
    job_result: ImportJobOutcomeResult | None = None,
    job_error: str | None = None,
    job_message: str | None = None,
) -> int:
    """Importer-side rejection entry point.

    Builds the typed terminal bundle from ``DownloadInfo``. ``PipelineDB``
    owns the request/audit/denylist/job transaction; callers must not compose
    those public committing helpers themselves.

    When ``requeue=True`` (auto-import): transitions to "wanted", records
    attempt. When ``requeue=False`` (force/manual import): only logs to
    download_log.

    Returns the new ``download_log`` row id — captured by the
    auto-import path for downstream Wrong Matches triage.

    ``validation_result`` is required and is the sole distance/scenario
    input for the audit row. ``PipelineDB.log_download`` derives its
    denormalized query columns from that envelope.
    """
    if attempt_result is not None:
        attempt_result.finalize_into(dl_info)
    message = job_message or detail or error or "Import rejected"
    applied = db.persist_importer_rejection(
        ImporterRejectionOutcome(
            request_id=request_id,
            requeue_to_wanted=requeue,
            record_validation_attempt=True,
            write_search_filetype_override=search_filetype_override is not None,
            search_filetype_override=search_filetype_override,
            audit=_download_audit_write(
                dl_info,
                outcome=outcome_label,
                detail=detail,
                staged_path=staged_path,
                validation_result=validation_result,
                error=error,
            ),
            import_job_id=import_job_id,
            denylist=denylist,
            job_result=job_result or ImportJobOutcomeResult(
                success=False,
                message=message,
                deferred=False,
                code=None,
            ),
            job_error=job_error or error or message,
            job_message=message,
        )
    )
    return applied.download_log_id


def _record_preview_measurement_failed(
    db: "PipelineDB",
    *,
    request_id: int | None,
    import_job_id: int,
    payload: MeasurementFailure,
    denylist_username: str | None = None,
    denylist_reason: str | None = None,
    import_result: ImportResult | None = None,
    preview_result_json: str | None = None,
) -> int:
    """Preview-side measurement_failed entry point (U4).

    Called when preview cannot produce evidence — measurement crashed, the
    source folder vanished, the snapshot went stale after retry, or one of
    the pre-claim sanity checks failed (request_not_found, missing MBID,
    etc.). Has no slskd context because no transfer is in flight; the
    ``download_log`` row carries NULL for username/bitrate/filetype/spectral
    columns and the typed ``MeasurementFailure`` payload as its
    ``validation_result`` JSONB.

    Delegates one typed command to PipelineDB for the four self-healing
    side effects:

      * ``download_log`` row written with ``outcome='measurement_failed'``,
        ``beets_scenario='measurement_failed'``, and the
        ``MeasurementFailure`` JSON as ``validation_result``.
      * Parent request → ``wanted`` via an exact-source DB compare-and-set.
      * Optional denylist write when ``denylist_username`` is supplied.
      * the preview/job failure fields, so the poll loop's active-import-job
        guard releases on the next tick.

    Returns the new ``download_log`` row id. A missing request id fails closed
    before any write because a mandatory audit row cannot be owned without its
    parent request.
    """
    if request_id is None:
        raise ValueError(
            "cannot persist preview terminal outcome without request_id"
        )
    validation_json = msgspec.json.encode(payload).decode("utf-8")
    preview_json = preview_result_json or validation_json
    denylist = (
        (DenylistWrite(username=denylist_username, reason=denylist_reason),)
        if denylist_username is not None
        else ()
    )
    applied = db.persist_preview_measurement_failure(
        PreviewMeasurementFailureOutcome(
            request_id=request_id,
            import_job_id=import_job_id,
            preview_status="measurement_failed",
            preview_result_json=preview_json,
            preview_error=payload.reason,
            preview_message=f"Preview measurement failed: {payload.reason}",
            validation_result_json=validation_json,
            import_result_json=(
                import_result.to_json() if import_result is not None else None
            ),
            staged_path=payload.source_path or None,
            detail=payload.detail,
            denylist=denylist,
        )
    )
    return applied.download_log_id
