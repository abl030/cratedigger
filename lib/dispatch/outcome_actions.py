"""Outcome-time rejection / mark-done writers.

The download_log-writing side of dispatch: mark an import done, record a
rejection (with optional self-heal requeue), and the unified persisted-
evidence reject helper. ``finalize_request`` is the module-local DI seam
(tests patch it here).
"""

from __future__ import annotations

import logging
from typing import Any, Sequence, TYPE_CHECKING, cast

import msgspec

from lib import transitions
from lib.import_evidence import (
    HaveAnalysisFailure,
    classify_have_analysis_failure,
)

# Module-level DI seam for ``transitions.finalize_request`` (see the leaf-seam
# allowlist in ``tests/_mock_audit_scanner.py``). Tests patch
# ``lib.dispatch.outcome_actions.finalize_request``.
finalize_request = transitions.finalize_request

from lib.quality import (DownloadInfo, QualityRankConfig, ValidationResult,
                         dispatch_action, extract_usernames,
                         is_comparable_lossless_source_probe,
                         resolve_rejection_search_override)

from lib.dispatch.types import (DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                                DispatchOutcome, ImportAttemptResult)
from lib.dispatch.helpers import (_cleanup_staged_dir,
                                  _populate_dl_info_from_import_result,
                                  _should_cleanup_path, _v0_probe_log_fields)
from lib.terminal_outcomes import (
    PendingImportTerminalOutcome,
    PreviewTerminalOutcome,
    TerminalCooldown,
    TerminalDenylist,
    TerminalDownloadAudit,
)

if TYPE_CHECKING:
    from lib.pipeline_db import DownloadLogOutcome, PipelineDB
    from lib.quality import ImportResult, MeasurementFailure

logger = logging.getLogger("cratedigger")


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
    source_download_log_id: int | None = None,
    quality_ranks: QualityRankConfig | None = None,
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

    Every reject honours ``requeue_on_failure``. Automatic imports pass True
    because a rejected candidate should self-heal to ``wanted``. Force imports
    pass False because their ``unsearchable`` request
    status is operator-owned and a candidate fact must not clear it.
    """

    import_result = attempt_result.result
    if import_result is None:
        raise RuntimeError("persisted-evidence rejection requires an import result")
    _populate_dl_info_from_import_result(dl_info, import_result)
    action = dispatch_action(decision)
    rejection_validation = validation_result or ValidationResult(
        distance=distance,
        scenario=decision or scenario,
        detail=detail,
    ).to_json()
    search_filetype_override = None
    if decision in ("downgrade", "transcode_downgrade"):
        current_override = None
        try:
            request = db.get_request(request_id)
            current_override = (
                request.get("search_filetype_override") if request else None
            )
        except Exception:
            logger.debug(
                "Failed to inspect search_filetype_override before rejection"
            )
        search_filetype_override = resolve_rejection_search_override(
            decision=decision,
            current_override=current_override,
            dl_info=dl_info,
            current_measurement=import_result.current_measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=import_result.spectral.existing,
            cfg=quality_ranks,
        ).override
    terminal_outcome = _record_rejection_and_maybe_requeue(
        db,
        request_id,
        dl_info,
        detail=detail,
        error=None,
        requeue=requeue_on_failure and not action.preserve_imported,
        outcome_label="rejected",
        search_filetype_override=search_filetype_override,
        validation_result=rejection_validation,
        staged_path=staged_path,
        attempt_result=attempt_result,
        import_job_id=import_job_id,
        source_download_log_id=source_download_log_id,
        preserve_imported=action.preserve_imported,
    )
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
        if isinstance(terminal_outcome, PendingImportTerminalOutcome):
            terminal_outcome = terminal_outcome.append_denylists(*(
                TerminalDenylist(username, reason, apply_cooldown=True)
                for username in sorted(usernames)
            ))
        else:
            for username in usernames:
                db.add_denylist(request_id, username, reason)
                if cooled_down_users is not None:
                    if db.check_and_apply_cooldown(username):
                        cooled_down_users.add(username)
    if action.cleanup and _should_cleanup_path(source_path_cleanup_scenario, action):
        _cleanup_staged_dir(staged_path)
    return DispatchOutcome(
        success=False,
        message=f"Rejected by persisted quality evidence: {decision}",
        code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
        terminal_outcome=(
            terminal_outcome
            if isinstance(terminal_outcome, PendingImportTerminalOutcome)
            else None
        ),
    )


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
    source_download_log_id: int | None = None,
) -> int | None | PendingImportTerminalOutcome:
    """Mark album as imported — standalone version of DatabaseSource.mark_done.

    Takes PipelineDB directly instead of going through DatabaseSource.
    Uses outcome_label for download_log (e.g. "force_import" instead of "success").

    ``imported_path`` is the beets destination (from
    ``ImportResult.postflight.imported_path``) — what shows up in the UI's
    "Imported to" label. ``dest_path`` is the source/staging path passed to
    the importer. When callers have both (automation/force paths that ran
    beets), they pass ``imported_path`` so ``album_requests.imported_path``
    reflects the actual on-disk location. Callers that only stage for manual
    review (``album_source.mark_done``) leave ``imported_path=None``; it
    falls back to ``dest_path`` so legacy behavior is preserved (issue #93).
    """
    from lib.quality import SpectralMeasurement, is_verified_lossless
    from lib.pipeline_db import RequestSpectralStateUpdate, RequestV0ProbeStateUpdate

    update_fields: dict[str, object] = dict(
        beets_distance=distance,
        beets_scenario=scenario,
        imported_path=imported_path if imported_path else dest_path,
    )
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
    update_fields["verified_lossless"] = verified_lossless
    if dl_info.download_spectral is not None:
        current_spectral = dl_info.download_spectral
        if update_fields.get("verified_lossless") and dl_info.bitrate:
            current_spectral = SpectralMeasurement(
                grade=dl_info.download_spectral.grade,
                bitrate_kbps=dl_info.bitrate // 1000,
            )
        update_fields.update(
            RequestSpectralStateUpdate(
                last_download=dl_info.download_spectral,
                current=current_spectral,
                ).as_update_fields()
        )
    if is_comparable_lossless_source_probe(dl_info.v0_probe):
        update_fields.update(
            RequestV0ProbeStateUpdate(
                current_lossless_source=dl_info.v0_probe,
            ).as_update_fields()
        )
    elif clear_stale_v0_probe:
        update_fields.update(
            RequestV0ProbeStateUpdate(
                clear_current_lossless_source=True,
            ).as_update_fields()
        )
    update_fields["final_format"] = dl_info.final_format
    transition = transitions.RequestTransition.to_imported_fields(
        fields=update_fields
    )

    validation_result = dl_info.validation_result or ValidationResult(
        valid=True,
        distance=distance,
        scenario=scenario,
        detail=detail,
    ).to_json()
    if attempt_result is not None:
        attempt_result.finalize_into(dl_info)
    audit = TerminalDownloadAudit(
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        beets_detail=detail,
        outcome=outcome_label,
        staged_path=dest_path,
        bitrate=dl_info.bitrate,
        sample_rate=dl_info.sample_rate,
        bit_depth=dl_info.bit_depth,
        is_vbr=dl_info.is_vbr,
        was_converted=dl_info.was_converted,
        original_filetype=dl_info.original_filetype,
        slskd_filetype=dl_info.slskd_filetype,
        actual_filetype=dl_info.actual_filetype,
        actual_min_bitrate=dl_info.actual_min_bitrate,
        spectral_grade=dl_info.download_spectral.grade if dl_info.download_spectral else None,
        spectral_bitrate=(
            dl_info.download_spectral.bitrate_kbps if dl_info.download_spectral else None
        ),
        existing_min_bitrate=dl_info.existing_min_bitrate,
        existing_spectral_bitrate=(
            dl_info.current_spectral.bitrate_kbps if dl_info.current_spectral else None
        ),
        import_result=dl_info.import_result,
        validation_result=validation_result,
        final_format=dl_info.final_format,
        **_v0_probe_log_fields(dl_info),
        source_download_log_id=source_download_log_id,
    )
    if import_job_id is not None:
        return PendingImportTerminalOutcome(
            request_id=request_id,
            import_job_id=import_job_id,
            initial_transition=transition,
            audit=audit,
        )
    transitions.require_transition_applied(finalize_request(
        db,
        request_id,
        transition,
    ))
    return cast(Any, db.log_download)(
        request_id=request_id,
        **audit.as_log_kwargs(),
    )


def _finalize_request_and_log_rejection(
    db: "PipelineDB",
    request_id: int | None,
    log_download_kwargs: dict[str, Any],
    *,
    requeue_to_wanted: bool,
    search_filetype_override: str | None = None,
    record_validation_attempt: bool = True,
    import_job_id: int | None = None,
    import_job_error: str = "",
    import_job_message: str | None = None,
    import_job_result: dict[str, Any] | None = None,
    denylist_username: str | None = None,
    denylist_reason: str | None = None,
    preserve_imported: bool = False,
) -> int:
    """Write an imperative rejection audit and optional lifecycle transition.

    The single source of truth for "a candidate was rejected; clean up
    state so the parent request can advance." Both the importer-side
    ``_record_rejection_and_maybe_requeue`` (with full ``DownloadInfo``
    context) and the direct installed-HAVE abort use this boundary. Queued
    preview/import outcomes use their atomic terminal command objects instead.

    Side effects, in order:

      1. Optional request transition via ``transitions.finalize_request``:
         proof-locked candidates restore terminal ``imported``; ordinary
         automatic rejects go to ``wanted``. When the wanted transition
         fires and ``record_validation_attempt=True``, it also bumps the
         validation attempt counter — matches pre-U4 importer behavior.
      2. ``download_log`` row write via ``db.log_download(**log_download_kwargs)``.
         Fires whenever ``request_id`` is present (raises otherwise — see
         below). Returns the new row id.
      3. ``source_denylist`` write when ``denylist_username`` is supplied
         AND ``request_id is not None`` (denylist FK-references a
         request). The importer-side entry point currently passes None
         here and handles denylist externally; the preview-side path
         passes a username when the 5-strikes rule applies.
      4. ``import_jobs.status='failed'`` via ``mark_import_job_failed``
         when ``import_job_id`` is supplied. The importer-side caller
         leaves this to the worker (``scripts/importer.py``) so it
         continues to pass None here; the preview-side caller fires it
         so the poll loop's active-import-job guard releases.

    Returns the new ``download_log`` row id. The ``request_not_found``
    subcase (``request_id is None``) raises instead — the audit row
    cannot be written because ``download_log.request_id`` is NOT NULL
    (this was always true; the INSERT used to raise NotNullViolation).
    The preview worker's lifecycle try/except absorbs it and the job is
    already ``failed`` from its step 1, so the queue still converges.
    """
    if preserve_imported and request_id is not None:
        transitions.require_transition_applied(finalize_request(
            db,
            request_id,
            transitions.RequestTransition.to_imported(),
        ))
    elif requeue_to_wanted and request_id is not None:
        transition_kwargs: dict[str, object] = {}
        if search_filetype_override is not None:
            transition_kwargs["search_filetype_override"] = search_filetype_override
        transitions.require_transition_applied(finalize_request(
            db,
            request_id,
            transitions.RequestTransition.to_wanted_fields(
                attempt_type=(
                    "validation" if record_validation_attempt else None
                ),
                fields=transition_kwargs),
        ))

    if request_id is None:
        # Same control flow as the NotNullViolation this used to raise at
        # the INSERT — download_log.request_id is NOT NULL — but with an
        # honest message (#409 typing exposed the documented-but-impossible
        # "request_not_found writes the log" subcase). The preview worker's
        # lifecycle try/except catches it; the job is already failed.
        raise ValueError(
            "cannot write download_log rejection audit: request_id is None "
            "and download_log.request_id is NOT NULL"
        )
    download_log_id = db.log_download(
        request_id=request_id,
        **log_download_kwargs,
    )

    if denylist_username:
        db.add_denylist(request_id, denylist_username, reason=denylist_reason)

    if import_job_id is not None:
        db.mark_import_job_failed(
            import_job_id,
            error=import_job_error,
            message=import_job_message,
            result=import_job_result,
        )

    return download_log_id


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
    source_download_log_id: int | None = None,
    preserve_imported: bool = False,
) -> int | PendingImportTerminalOutcome:
    """Importer-side rejection entry point.

    Builds the ``log_download`` kwargs from ``DownloadInfo`` (slskd context:
    username, bitrate, spectral, V0 probe, etc.) and delegates to
    ``_finalize_request_and_log_rejection``. Behavior is preserved from
    pre-U4: optional requeue-to-wanted with attempt bump, mandatory
    download_log row, no denylist (caller handles via ``action.denylist``
    in ``dispatch_import_core``), no job-failed mark (caller in
    ``scripts/importer.py`` handles it on the outer return).

    When ``requeue=True`` (auto-import): transitions to "wanted", records
    attempt. When ``requeue=False`` (force-import): only logs to
    download_log. ``preserve_imported=True`` is the proof-lock exception:
    it transitions back to terminal "imported" without an attempt bump.

    Returns the new ``download_log`` row id — captured by the
    auto-import path for downstream Wrong Matches triage.

    ``validation_result`` is required and is the sole distance/scenario
    input for the audit row. ``PipelineDB.log_download`` derives its
    denormalized query columns from that envelope.
    """
    if attempt_result is not None:
        attempt_result.finalize_into(dl_info)
    log_download_kwargs: dict[str, Any] = {
        "soulseek_username": dl_info.username,
        "filetype": dl_info.filetype,
        "beets_detail": detail,
        "outcome": outcome_label,
        "staged_path": staged_path,
        "error_message": error,
        "bitrate": dl_info.bitrate,
        "sample_rate": dl_info.sample_rate,
        "bit_depth": dl_info.bit_depth,
        "is_vbr": dl_info.is_vbr,
        "was_converted": dl_info.was_converted,
        "original_filetype": dl_info.original_filetype,
        "slskd_filetype": dl_info.slskd_filetype,
        "actual_filetype": dl_info.actual_filetype,
        "actual_min_bitrate": dl_info.actual_min_bitrate,
        "spectral_grade": (dl_info.download_spectral.grade
                           if dl_info.download_spectral else None),
        "spectral_bitrate": (dl_info.download_spectral.bitrate_kbps
                             if dl_info.download_spectral else None),
        "existing_min_bitrate": dl_info.existing_min_bitrate,
        "existing_spectral_bitrate": (dl_info.current_spectral.bitrate_kbps
                                      if dl_info.current_spectral else None),
        "import_result": dl_info.import_result,
        "validation_result": validation_result,
        "source_download_log_id": source_download_log_id,
    }
    log_download_kwargs.update(_v0_probe_log_fields(dl_info))
    if import_job_id is not None:
        return _pending_rejection_outcome(
            request_id=request_id,
            import_job_id=import_job_id,
            audit=TerminalDownloadAudit(**log_download_kwargs),
            requeue=requeue,
            search_filetype_override=search_filetype_override,
            preserve_imported=preserve_imported,
        )
    return _finalize_request_and_log_rejection(
        db,
        request_id,
        log_download_kwargs,
        requeue_to_wanted=requeue,
        search_filetype_override=search_filetype_override,
        record_validation_attempt=True,
        # Importer-side leaves job-failed + denylist to its caller.
        import_job_id=None,
        denylist_username=None,
        preserve_imported=preserve_imported,
    )


def _record_preview_measurement_failed(
    db: "PipelineDB",
    *,
    request_id: int | None,
    import_job_id: int,
    payload: MeasurementFailure,
    denylist_username: str | None = None,
    denylist_reason: str | None = None,
    import_result: ImportResult | None = None,
    preview_result: dict[str, object] | None = None,
    requeue_to_wanted: bool = True,
) -> int:
    """Preview-side measurement_failed entry point (U4).

    Called when preview cannot produce evidence — measurement crashed, the
    source folder vanished, the snapshot went stale after retry, or one of
    the pre-claim sanity checks failed (request_not_found, missing MBID,
    etc.). Has no slskd context because no transfer is in flight; the
    ``download_log`` row carries NULL for username/bitrate/filetype/spectral
    columns and the typed ``MeasurementFailure`` payload as its
    ``validation_result`` JSONB.

    Delegates to ``persist_preview_terminal_outcome`` for the terminal effects
    in one explicit transaction. Automation reopens ``wanted``; operator jobs
    omit the transition and preserve the request's current lifecycle state.

      * ``download_log`` row written with ``outcome='measurement_failed'``,
        ``beets_scenario='measurement_failed'``, and the
        ``MeasurementFailure`` JSON as ``validation_result``.
      * Parent request → ``wanted`` for automation, otherwise unchanged.
      * Optional denylist write when ``denylist_username`` is supplied.
      * ``import_jobs.status='failed'`` via ``mark_import_job_failed`` so
        the poll loop's active-import-job guard releases on the next tick.

    Returns the committed ``download_log`` row id. A missing request owner
    raises before any write because ``download_log.request_id`` is mandatory.
    """
    if request_id is None:
        raise ValueError(
            "cannot persist terminal preview outcome without request_id"
        )
    validation_json = msgspec.json.encode(payload).decode("utf-8")
    job_result = msgspec.to_builtins(payload)
    assert isinstance(job_result, dict), \
        "msgspec.to_builtins on a Struct returns a dict"
    denylists = (
        (TerminalDenylist(denylist_username, denylist_reason),)
        if denylist_username
        else ()
    )
    result = db.persist_preview_terminal_outcome(PreviewTerminalOutcome(
        request_id=request_id,
        import_job_id=import_job_id,
        request_transition=(
            transitions.RequestTransition.to_wanted()
            if requeue_to_wanted
            else None
        ),
        audit=TerminalDownloadAudit(
            soulseek_username=None,
            filetype=None,
            beets_distance=None,
            beets_scenario="measurement_failed",
            beets_detail=payload.detail,
            outcome="measurement_failed",
            staged_path=payload.source_path or None,
            error_message=None,
            validation_result=validation_json,
            import_result=(
                import_result.to_json() if import_result is not None else None
            ),
        ),
        preview_status="measurement_failed",
        preview_result=preview_result or job_result,
        message=payload.detail,
        error=payload.reason,
        denylists=denylists,
    ))
    return result.download_log_id


def _record_have_analysis_error(
    db: "PipelineDB",
    *,
    request_id: int,
    dl_info: DownloadInfo,
    raw_error: str,
    installed_path: str | None,
    candidate_reference: str | None,
    snapshot_guard: str | None,
    import_job_id: int | None,
    source_download_log_id: int | None = None,
    cooled_down_users: set[str] | None = None,
    requeue_to_wanted: bool = True,
) -> int | PendingImportTerminalOutcome:
    """Persist a non-quality abort while honoring caller lifecycle authority."""

    failure = HaveAnalysisFailure(
        failure_category=classify_have_analysis_failure(
            raw_error,
            snapshot_guard=snapshot_guard,
        ),
        error=raw_error,
        installed_path=installed_path,
        candidate_reference=candidate_reference,
    )
    validation_json = msgspec.json.encode(failure).decode("utf-8")
    detail = (
        "Installed HAVE analysis failed "
        f"({failure.failure_category}): {raw_error}"
    )
    audit = TerminalDownloadAudit(
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        download_path=installed_path,
        beets_scenario="have_analysis_error",
        beets_detail=detail,
        outcome="have_analysis_error",
        staged_path=candidate_reference,
        error_message=raw_error,
        validation_result=validation_json,
        source_download_log_id=source_download_log_id,
    )
    transition = (
        transitions.RequestTransition.to_wanted_fields(
            attempt_type="validation",
            fields={},
        )
        if requeue_to_wanted
        else None
    )
    cooldowns = (
        (TerminalCooldown(dl_info.username),)
        if dl_info.username
        else ()
    )
    if import_job_id is not None:
        return PendingImportTerminalOutcome(
            request_id=request_id,
            import_job_id=import_job_id,
            initial_transition=transition,
            audit=audit,
            cooldowns=cooldowns,
        )

    download_log_id = _finalize_request_and_log_rejection(
        db,
        request_id,
        audit.as_log_kwargs(),
        requeue_to_wanted=requeue_to_wanted,
        record_validation_attempt=requeue_to_wanted,
    )
    if dl_info.username and db.check_and_apply_cooldown(dl_info.username):
        if cooled_down_users is not None:
            cooled_down_users.add(dl_info.username)
    return download_log_id


def _pending_rejection_outcome(
    *,
    request_id: int,
    import_job_id: int,
    audit: TerminalDownloadAudit,
    requeue: bool,
    search_filetype_override: str | None = None,
    preserve_imported: bool = False,
) -> PendingImportTerminalOutcome:
    """Build the DB-owned terminal rejection command without writing."""
    fields: dict[str, object] = {}
    if search_filetype_override is not None:
        fields["search_filetype_override"] = search_filetype_override
    transition = (
        transitions.RequestTransition.to_imported()
        if preserve_imported
        else transitions.RequestTransition.to_wanted_fields(
            attempt_type="validation",
            fields=fields,
        )
        if requeue
        else None
    )
    return PendingImportTerminalOutcome(
        request_id=request_id,
        import_job_id=import_job_id,
        initial_transition=transition,
        audit=audit,
    )
