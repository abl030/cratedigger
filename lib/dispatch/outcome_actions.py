"""Outcome-time rejection / mark-done writers.

The download_log-writing side of dispatch: mark an import done, record a
rejection (with optional self-heal requeue), and the unified persisted-
evidence reject helper.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import msgspec

from lib import transitions
from lib.dispatch.helpers import (
    _cleanup_staged_dir,
    _populate_dl_info_from_import_result,
    _should_cleanup_path,
    _v0_probe_log_fields,
)
from lib.dispatch.types import (
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DispatchOutcome,
    ImportAttemptResult,
    PostCommitCleanup,
)
from lib.import_evidence import (
    HaveAnalysisFailure,
    classify_have_analysis_failure,
)
from lib.quality import (
    DownloadInfo,
    QualityRankConfig,
    ValidationResult,
    dispatch_action,
    extract_usernames,
    is_comparable_lossless_source_probe,
    resolve_rejection_search_override,
)
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    PendingImportTerminalOutcome,
    PreviewTerminalOutcome,
    RequestRejectionOutcome,
    RequestSuccessOutcome,
    TerminalCooldown,
    TerminalDenylist,
    TerminalDownloadAudit,
)

if TYPE_CHECKING:
    from lib.dispatch.types import DispatchDB, DispatchRequest
    from lib.pipeline_db import DownloadLogOutcome
    from lib.quality import ImportResult, MeasurementFailure

logger = logging.getLogger("cratedigger")


def _reject_import_from_evidence_decision(
    request: DispatchRequest,
    db: DispatchDB,
    *,
    attempt_result: ImportAttemptResult,
    decision: str,
    detail: str,
    quality_ranks: QualityRankConfig | None = None,
    protected_roots: frozenset[str] | None = None,
) -> DispatchOutcome:
    """Record a persisted-evidence rejection before beets can mutate files.

    Takes the whole ``DispatchRequest`` (issue #1277). Its one caller is
    ``dispatch_import_core``, and everything this helper used to take as a
    loose parameter — the request id, the download info, the distance, the
    lifecycle authority, the staged path, the scenario, the peer files, the
    cooldown set, the job/log ids — was already a verbatim field of that
    caller's own request. What is NOT on the request stays explicit: the
    attempt accumulator, the decision and its detail (both derived inside
    the caller from the evidence pipeline), and the two config-derived
    policy values.

    The scenario is used twice — as the fallback rejection envelope's own
    label and as the source-path cleanup gate — and both readings are
    ``request.scenario``. They were separate parameters before #1277, but
    the single production call site always passed the same value to both,
    so a divergence between them was never reachable.

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

    dl_info = request.dl_info
    import_result = attempt_result.result
    if import_result is None:
        raise RuntimeError("persisted-evidence rejection requires an import result")
    _populate_dl_info_from_import_result(dl_info, import_result)
    action = dispatch_action(decision)
    rejection_validation = dl_info.validation_result or ValidationResult(
        distance=request.distance,
        scenario=decision or request.scenario,
        detail=detail,
    ).to_json()
    search_filetype_override = None
    if decision in ("downgrade", "transcode_downgrade"):
        current_override = None
        try:
            request_row = db.get_request(request.request_id)
            current_override = (
                request_row.get("search_filetype_override")
                if request_row else None
            )
        except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
    denylists: tuple[TerminalDenylist, ...] = ()
    if action.denylist:
        usernames = extract_usernames(request.files or [])
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
        denylists = tuple(
            TerminalDenylist(username, reason, apply_cooldown=True)
            for username in sorted(usernames)
        )
    terminal_outcome = _record_rejection_and_maybe_requeue(
        db,
        request.request_id,
        dl_info,
        detail=detail,
        error=detail if decision == "audio_corrupt" else None,
        requeue=request.requeue_on_failure and not action.preserve_imported,
        outcome_label="rejected",
        search_filetype_override=search_filetype_override,
        validation_result=rejection_validation,
        staged_path=request.path,
        attempt_result=attempt_result,
        import_job_id=request.candidate_import_job_id,
        source_download_log_id=request.candidate_download_log_id,
        preserve_imported=action.preserve_imported,
        denylists=denylists,
        cooled_down_users=request.cooled_down_users,
    )
    cleanup_plan: PostCommitCleanup | None = None
    # Bad rips are ban + delete, never quarantined (issue #1077, D3): a
    # corrupt candidate has no salvage value for operator review, so
    # ``audio_corrupt`` no longer branches specially here — it disposes of
    # its disposable staged source exactly like every other auto-import
    # reject (the automation lane's journaled processor cleanup then
    # removes the owned canonical folder outright, since no post-commit
    # plan overrides its plan-free default). Force imports leave their
    # disposable action copy for ``_cleanup_terminal_force_action``
    # (``scripts/importer.py``); the force lane's ORIGINAL Wrong Matches
    # source is a distinct path this helper never sees, deleted by
    # ``_cleanup_failed_force_import`` after the terminal commit.
    if action.cleanup and _should_cleanup_path(
        request.scenario,
        action,
    ):
        # Issue #1077, R3-3 (widened issue #1122, review round 2): this
        # reject path's ``staged_path`` can be the canonical processing
        # album directly under ``<processing_dir>/albums/`` OR a YouTube
        # rescue's auto-import staging child (it imports in place, never
        # materialized under the canonical root) — either is a shared,
        # deploy-provisioned root ``_cleanup_staged_dir``'s empty-parent
        # prune must never remove. ``protected_roots`` is the caller's
        # ONE derivation (``lib.processing_paths.protected_staging_roots``)
        # covering both; this function stays decoupled from
        # ``processing_dir``/``beets_staging_dir`` naming. Guard both the
        # synchronous cleanup here AND the deferred post-commit plan the
        # same way; the deferred branch has no other way to carry the guard
        # across the boundary to
        # ``scripts/importer.py::_run_post_commit_cleanup``.
        if request.candidate_import_job_id is not None:
            cleanup_plan = PostCommitCleanup(
                staged_path=request.path,
                staged_path_protected_parents=protected_roots,
            )
        else:
            _cleanup_staged_dir(
                request.path, protected_parents=protected_roots,
            )
    return DispatchOutcome(
        success=False,
        message=f"Rejected by persisted quality evidence: {decision}",
        code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
        terminal_outcome=(
            terminal_outcome
            if isinstance(terminal_outcome, PendingImportTerminalOutcome)
            else None
        ),
        post_commit_wrong_match_scenario=decision,
        post_commit_cleanup=cleanup_plan,
    )


def _do_mark_done(
    db: DispatchDB,
    request_id: int,
    dl_info: DownloadInfo,
    distance: float | None,
    scenario: str | None,
    dest_path: str | None,
    outcome_label: DownloadLogOutcome = "success",
    detail: str | None = None,
    clear_stale_v0_probe: bool = True,
    attempt_result: ImportAttemptResult | None = None,
    import_job_id: int | None = None,
    source_download_log_id: int | None = None,
    clear_marked_incomplete: bool = False,
) -> int | None | PendingImportTerminalOutcome:
    """Mark album as imported — standalone version of DatabaseSource.mark_done.

    Takes PipelineDB directly instead of going through DatabaseSource.
    Uses outcome_label for download_log (e.g. "force_import" instead of "success").

    ``dest_path`` is the source/staging path recorded on the download audit.
    Current library location is resolved from Beets and is never copied onto
    the request row.
    """
    from lib.pipeline_db import RequestSpectralStateUpdate, RequestV0ProbeStateUpdate
    from lib.quality import SpectralMeasurement, is_verified_lossless

    update_fields: dict[str, object] = {
        "beets_distance": distance,
        "beets_scenario": scenario,
    }
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
    if clear_marked_incomplete:
        # Issue #1241: this acceptance's candidate was proven whole by
        # beets (the caller derived the bit from the attempt's own
        # scenario), so the operator's incomplete mark is satisfied. The
        # explicit None rides the transition's metadata CAS — cleared in
        # the SAME terminal transaction as the imported status, and a
        # harmless NULL-over-NULL on rows that were never marked.
        update_fields["marked_incomplete_at"] = None
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
        contributor_usernames=dl_info.contributor_usernames,
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
    # The transition and the mandatory ``download_log`` audit row commit
    # together in one PostgreSQL transaction (issue #1355 item A1) —
    # this used to be two separate autocommitted statements
    # (``finalize_request`` then ``db.log_download``), so a crash between
    # them left a request already transitioned to ``imported`` with no
    # audit row explaining why.
    result = db.persist_request_success_outcome(RequestSuccessOutcome(
        request_id=request_id,
        transition=transition,
        audit=audit,
    ))
    return result.download_log_id


def _finalize_request_and_log_rejection(
    db: DispatchDB,
    request_id: int,
    audit: TerminalDownloadAudit,
    *,
    requeue_to_wanted: bool,
    search_filetype_override: str | None = None,
    record_validation_attempt: bool = True,
    denylists: tuple[TerminalDenylist, ...] = (),
    cooldowns: tuple[TerminalCooldown, ...] = (),
    cooled_down_users: set[str] | None = None,
    preserve_imported: bool = False,
) -> int:
    """Atomically commit a job-less rejection: the single source of truth
    for "a candidate was rejected with no owning import job; clean up state
    so the parent request can advance."

    Both the importer-side ``_record_rejection_and_maybe_requeue`` (with
    full ``DownloadInfo`` context) and the direct installed-HAVE abort use
    this boundary. Queued preview/import outcomes use their atomic terminal
    command objects instead.

    The request transition, the ``download_log`` audit row, and any
    ``denylists``/``cooldowns`` entries commit together in one PostgreSQL
    transaction via ``db.persist_request_rejection_outcome`` (issue #1355
    item 3) — a request returned to ``wanted`` with no audit row explaining
    why, or a peer left un-denylisted after a "denylisted" rejection, are
    exactly the partial-write worlds that bundle exists to make
    unreachable.

    Returns the new ``download_log`` row id.
    """
    transition: transitions.RequestTransition | None = None
    if preserve_imported:
        transition = transitions.RequestTransition.to_imported()
    elif requeue_to_wanted:
        transition_kwargs: dict[str, object] = {}
        if search_filetype_override is not None:
            transition_kwargs["search_filetype_override"] = search_filetype_override
        transition = transitions.RequestTransition.to_wanted_fields(
            attempt_type=(
                "validation" if record_validation_attempt else None
            ),
            fields=transition_kwargs,
        )

    result = db.persist_request_rejection_outcome(RequestRejectionOutcome(
        request_id=request_id,
        audit=audit,
        transition=transition,
        denylists=denylists,
        cooldowns=cooldowns,
    ))
    if cooled_down_users is not None:
        cooled_down_users.update(result.cooled_down_users)

    return result.download_log_id


def _record_rejection_and_maybe_requeue(
    db: DispatchDB,
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
    denylists: tuple[TerminalDenylist, ...] = (),
    cooled_down_users: set[str] | None = None,
) -> int | PendingImportTerminalOutcome:
    """Importer-side rejection entry point.

    Builds the shared ``TerminalDownloadAudit`` from ``DownloadInfo`` (slskd
    context: username, bitrate, spectral, V0 probe, etc.) once, then
    delegates to whichever terminal-outcome authority matches the caller:
    a job-backed rejection returns a ``PendingImportTerminalOutcome`` for
    its owning import job to finish later; a job-less rejection commits
    immediately and atomically through ``_finalize_request_and_log_rejection``
    (issue #1355 item 3). Both branches build the SAME ``denylists`` tuple
    from the caller's usernames — the two lanes differ only in whether an
    ``import_job_id`` is attached, never in how source peers are recorded.

    When ``requeue=True`` (auto-import): transitions to "wanted", records
    attempt. When ``requeue=False`` (force-import): only logs to
    download_log. ``preserve_imported=True`` is the proof-lock exception:
    it transitions back to terminal "imported" without an attempt bump.
    ``cooled_down_users``, when supplied, is updated in place with every
    username the job-less commit actually cooled down — job-backed commits
    apply cooldowns later, at the owning job's own terminal write, so this
    set is untouched on that branch.

    Returns the new ``download_log`` row id — captured by the
    auto-import path for downstream Wrong Matches triage.

    ``validation_result`` is required and is the sole distance/scenario
    input for the audit row. ``PipelineDB.log_download`` derives its
    denormalized query columns from that envelope.
    """
    if attempt_result is not None:
        attempt_result.finalize_into(dl_info)
    audit = TerminalDownloadAudit(
        soulseek_username=dl_info.username,
        contributor_usernames=dl_info.contributor_usernames,
        filetype=dl_info.filetype,
        beets_detail=detail,
        outcome=outcome_label,
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
        spectral_grade=(dl_info.download_spectral.grade
                        if dl_info.download_spectral else None),
        spectral_bitrate=(dl_info.download_spectral.bitrate_kbps
                          if dl_info.download_spectral else None),
        existing_min_bitrate=dl_info.existing_min_bitrate,
        existing_spectral_bitrate=(dl_info.current_spectral.bitrate_kbps
                                   if dl_info.current_spectral else None),
        import_result=dl_info.import_result,
        validation_result=validation_result,
        source_download_log_id=source_download_log_id,
        **_v0_probe_log_fields(dl_info),
    )
    if import_job_id is not None:
        return _pending_rejection_outcome(
            request_id=request_id,
            import_job_id=import_job_id,
            audit=audit,
            requeue=requeue,
            search_filetype_override=search_filetype_override,
            preserve_imported=preserve_imported,
            denylists=denylists,
        )
    return _finalize_request_and_log_rejection(
        db,
        request_id,
        audit,
        requeue_to_wanted=requeue,
        search_filetype_override=search_filetype_override,
        record_validation_attempt=True,
        denylists=denylists,
        cooled_down_users=cooled_down_users,
        preserve_imported=preserve_imported,
    )


def _record_preview_measurement_failed(
    db: DispatchDB,
    *,
    request_id: int | None,
    import_job_id: int,
    payload: MeasurementFailure,
    import_result: ImportResult | None = None,
    preview_result: dict[str, object] | None = None,
    requeue_to_wanted: bool = True,
    automation_terminal_authority: AutomationTerminalAuthority | None = None,
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
        ``MeasurementFailure`` JSON as ``validation_result``; its detail is
        also persisted as the operator-facing ``error_message``.
      * Parent request → ``wanted`` for automation, otherwise unchanged.
      * No denylist write: this is a measurement-world failure, not evidence
        that the source audio itself is bad.
      * ``import_jobs.status='failed'`` via ``persist_preview_terminal_
        outcome``'s own inline ``UPDATE`` (``lib/pipeline_db/terminal_
        outcomes.py``) so the poll loop's active-import-job guard releases
        on the next tick.

    Returns the committed ``download_log`` row id. A missing request owner
    raises before any write because ``download_log.request_id`` is mandatory.
    """
    if request_id is None:
        raise ValueError(
            "cannot persist terminal preview outcome without request_id"
        )
    validation_json = msgspec.json.encode(payload).decode("utf-8")
    # ``to_builtins`` is declared ``-> Any``; annotate the target so pyright
    # sees ``dict[str, object]`` without reshaping the value (a re-``convert``
    # here is NOT identity in the terminal-outcome path — issue #784).
    job_result: dict[str, object] = msgspec.to_builtins(payload)
    assert isinstance(job_result, dict), \
        "msgspec.to_builtins on a Struct returns a dict"
    result = db.persist_preview_terminal_outcome(PreviewTerminalOutcome(
        request_id=request_id,
        import_job_id=import_job_id,
        request_transition=(
            transitions.RequestTransition.to_wanted(
                from_status=(
                    "processing"
                    if automation_terminal_authority is not None
                    else None
                ),
            )
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
            error_message=payload.detail,
            validation_result=validation_json,
            import_result=(
                import_result.to_json() if import_result is not None else None
            ),
        ),
        preview_status="measurement_failed",
        preview_result=preview_result or job_result,
        message=payload.detail,
        error=payload.reason,
        denylists=(),
        automation=automation_terminal_authority,
    ))
    return result.download_log_id


def _record_have_analysis_error(
    request: DispatchRequest,
    db: DispatchDB,
    *,
    raw_error: str,
    installed_path: str | None,
    snapshot_guard: str | None,
) -> int | PendingImportTerminalOutcome:
    """Persist a non-quality abort while honoring caller lifecycle authority.

    Takes the whole ``DispatchRequest`` (issue #1277): its one caller is
    ``dispatch_import_core``, and the request id, download info, candidate
    reference (the candidate path), job/log ids, cooldown set and lifecycle
    authority were all verbatim fields of that caller's request. What stays
    explicit is what the evidence gate produced for THIS abort — the raw
    error, the installed path it failed on, and the snapshot guard that
    classifies it.
    """

    dl_info = request.dl_info
    import_job_id = request.candidate_import_job_id
    requeue_to_wanted = request.requeue_on_failure
    candidate_reference = request.path
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
        contributor_usernames=dl_info.contributor_usernames,
        filetype=dl_info.filetype,
        download_path=installed_path,
        beets_scenario="have_analysis_error",
        beets_detail=detail,
        outcome="have_analysis_error",
        staged_path=candidate_reference,
        error_message=raw_error,
        validation_result=validation_json,
        source_download_log_id=request.candidate_download_log_id,
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
            request_id=request.request_id,
            import_job_id=import_job_id,
            initial_transition=transition,
            audit=audit,
            cooldowns=cooldowns,
        )

    return _finalize_request_and_log_rejection(
        db,
        request.request_id,
        audit,
        requeue_to_wanted=requeue_to_wanted,
        record_validation_attempt=requeue_to_wanted,
        cooldowns=cooldowns,
        cooled_down_users=request.cooled_down_users,
    )


def _pending_rejection_outcome(
    *,
    request_id: int,
    import_job_id: int,
    audit: TerminalDownloadAudit,
    requeue: bool,
    search_filetype_override: str | None = None,
    preserve_imported: bool = False,
    denylists: tuple[TerminalDenylist, ...] = (),
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
        denylists=denylists,
    )
