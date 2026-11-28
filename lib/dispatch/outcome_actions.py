"""Outcome-time rejection / mark-done writers.

The download_log-writing side of dispatch: mark an import done, record a
rejection (with optional self-heal requeue), and the unified persisted-
evidence reject helper.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
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
    DISPATCH_CODE_FOLDER_COLLISION_HELD,
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DISPATCH_CODE_UPGRADE_HELD,
    DispatchOutcome,
    ImportAttemptResult,
    PostCommitCleanup,
)
from lib.held_upgrade_view import folder_collision_sentence
from lib.import_evidence import (
    HaveAnalysisFailure,
    classify_have_analysis_failure,
)
from lib.quality import (
    ActiveDownloadState,
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
from lib.wrong_match_policy import FOLDER_COLLISION_DENYLIST_REASON

if TYPE_CHECKING:
    from lib.dispatch.types import DispatchDB, DispatchRequest
    from lib.import_execution import CancellationToken
    from lib.pipeline_db import DownloadLogOutcome
    from lib.quality import ImportResult, MeasurementFailure
    from lib.quality.installed_copy_guard import InstalledCopyComparison

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


def _hold_upgrade_from_evidence_decision(
    request: DispatchRequest,
    db: DispatchDB,
    *,
    attempt_result: ImportAttemptResult,
    comparison: InstalledCopyComparison,
    cancellation_token: CancellationToken | None = None,
) -> DispatchOutcome:
    """Keep the installed copy and hand the candidate to the operator.

    Issue #1445 item 3. Reached only from the automation import lane, under
    the RELEASE lock, before any action file is written or Beets launches —
    so nothing has been replaced. The candidate folder moves WHOLE into
    ``wrong_matches/`` (one atomic rename, the Lane B precedent in
    ``lib.download_rejection._reject_request_auto_import``) and the audit
    row names it as ``failed_path`` under the ``upgrade_held`` scenario,
    carrying the per-track comparison every surface renders from.

    Search state is left exactly as any ordinary rejected attempt leaves
    it: the request requeues per the caller's lifecycle authority, and no
    ``search_filetype_override`` narrowing is computed. The contributing
    peers are denylisted for THIS request only, without a cooldown strike
    (``apply_cooldown=False``) — a hold is not the peer's fault, but the
    identical folder must not be fetched again next cycle.

    This helper checks cancellation only before the move.
    The importer's owner checkpoint (``checkpoint_automation_owner``,
    ``lib/import_execution.py``) still runs before the terminal commit, so a
    cancel, a lost lease or a crash after the move and before the commit
    leaves the folder in ``wrong_matches/`` with no row and no denylist
    entry, while startup recovery requeues the request — the same window
    Lanes A and B have. Nothing is imported or deleted in that window; the
    worst case is an orphan folder nobody sees and the same peer's copy
    being fetched again.
    """
    from lib.quality.installed_copy_guard import held_upgrade_sentence

    sentence = held_upgrade_sentence(comparison)

    def decorate(held: ValidationResult) -> None:
        held.upgrade_hold = comparison

    return _hold_candidate_folder(
        request,
        db,
        attempt_result=attempt_result,
        scenario="upgrade_held",
        sentence=sentence,
        denylist_reason=_hold_denylist_reason(comparison),
        decorate=decorate,
        code=DISPATCH_CODE_UPGRADE_HELD,
        message=f"Upgrade held: {sentence}",
        log_label="UPGRADE HELD",
        cancellation_token=cancellation_token,
    )


def _hold_folder_collision(
    request: DispatchRequest,
    db: DispatchDB,
    *,
    attempt_result: ImportAttemptResult,
    cancellation_token: CancellationToken | None = None,
    in_place_path: str | None = None,
) -> DispatchOutcome:
    """Hold a candidate whose library folder is occupied (issue #1465).

    ``in_place_path``: a force import (the operator's retry of a held
    folder, or of any Wrong Matches folder) that still collides is held IN
    PLACE on the operator's own folder (not the private action copy Beets
    ran on): no move, no new denylist entry, the request's status
    untouched. Its row is the newest hold naming the folder, so the card
    shows the current occupants.

    Reached from the automation import lane after the harness refused the
    task before ``manipulate_files``: Beets removed nothing and moved
    nothing. A lossless source is normally refused by the pre-conversion
    folder probe (``harness/import_one.py``), so the held folder is the
    download as fetched; only a refusal by the real guard after conversion
    (the probe failed, or the library changed in between) holds a lossless
    download as its converted copy. It is held in Wrong Matches
    like a held upgrade, so the operator can fix the folder on disk and
    retry the import ("Use new copy" is the same force import) or delete
    the copy. The peers are denylisted for THIS request only and take no
    cooldown strike: the collision is not their fault, and denylisting
    already stops the same copy being fetched again. Searching continues.
    """
    import_result = attempt_result.result
    guard = (
        import_result.postflight.folder_exclusivity_guard
        if import_result is not None else None
    )
    sentence = folder_collision_sentence(guard)

    def decorate(held: ValidationResult) -> None:
        held.folder_collision = guard

    return _hold_candidate_folder(
        request,
        db,
        attempt_result=attempt_result,
        # Spelled literally for the Wrong Matches producer audit
        # (tests/test_wrong_match_scenario_producer_audit.py); equal to
        # ``lib.wrong_match_policy.FOLDER_COLLISION_HELD_SCENARIO``.
        scenario="folder_exclusivity_guard_failed",
        sentence=sentence,
        denylist_reason=FOLDER_COLLISION_DENYLIST_REASON,
        decorate=decorate,
        code=DISPATCH_CODE_FOLDER_COLLISION_HELD,
        message=f"Library folder occupied: {sentence}",
        log_label="LIBRARY FOLDER OCCUPIED",
        cancellation_token=cancellation_token,
        in_place_path=in_place_path,
    )


def _hold_candidate_folder(
    request: DispatchRequest,
    db: DispatchDB,
    *,
    attempt_result: ImportAttemptResult,
    scenario: str,
    sentence: str,
    denylist_reason: str,
    decorate: Callable[[ValidationResult], None],
    code: str,
    message: str,
    log_label: str,
    cancellation_token: CancellationToken | None,
    in_place_path: str | None = None,
) -> DispatchOutcome:
    """Move a candidate WHOLE into ``wrong_matches/`` and hold it there.

    ``in_place_path`` (a force import's own source folder, already where
    the operator keeps it) records the hold on that folder as it stands:
    no move and no denylist entry.

    Shared by every held scenario (``lib.wrong_match_policy.HELD_SCENARIOS``):
    one atomic rename, an audit row naming the folder as ``failed_path``
    under ``scenario`` (``decorate`` adds the scenario's own evidence), a
    per-request denylist with no cooldown strike, and the caller's requeue.
    See ``_hold_upgrade_from_evidence_decision`` for the cancellation window.
    """
    from lib.import_execution import cancellation_hook, checkpoint
    from lib.import_manifest import move_failed_import_whole

    dl_info = request.dl_info
    import_result = attempt_result.result
    if import_result is None:
        raise RuntimeError("a held folder requires an import result")
    _populate_dl_info_from_import_result(dl_info, import_result)
    usernames = extract_usernames(request.files or [])
    if dl_info.username:
        usernames.add(dl_info.username)

    # Cancellation is checked here before the move and not again in this
    # helper. The importer's owner checkpoint still runs before the terminal
    # commit, so a cancel or lost lease after the move can still leave the
    # folder in wrong_matches/ with no row and no denylist entry (the Lane
    # A/B window: nothing imported or deleted; worst case an orphan folder
    # and the same peer fetched again). See the docstring.
    checkpoint(cancellation_token)
    failed_path = (
        in_place_path if in_place_path is not None else move_failed_import_whole(
            request.path,
            scenario=scenario,
            before_mutation=cancellation_hook(cancellation_token),
        )
    )
    if in_place_path is not None:
        # The peer was denylisted when the copy was first held.
        usernames.clear()

    held = (
        ValidationResult.from_json(dl_info.validation_result)
        if dl_info.validation_result
        else ValidationResult(distance=request.distance)
    )
    held.valid = False
    held.scenario = scenario
    held.detail = sentence
    held.failed_path = failed_path
    held.denylisted_users = sorted(usernames)
    decorate(held)
    validation_json = held.to_json()
    dl_info.validation_result = validation_json

    terminal_outcome = _record_rejection_and_maybe_requeue(
        db,
        request.request_id,
        dl_info,
        detail=sentence,
        error=None,
        requeue=request.requeue_on_failure,
        outcome_label="rejected",
        validation_result=validation_json,
        staged_path=request.path,
        attempt_result=attempt_result,
        import_job_id=request.candidate_import_job_id,
        source_download_log_id=request.candidate_download_log_id,
        denylists=tuple(
            TerminalDenylist(username, denylist_reason, apply_cooldown=False)
            for username in sorted(usernames)
        ),
        cooled_down_users=request.cooled_down_users,
    )
    logger.warning(
        "%s: %s — %s | kept at %s | denylisted for this request: %s",
        log_label,
        request.label,
        sentence,
        failed_path,
        ", ".join(sorted(usernames)),
    )
    return DispatchOutcome(
        success=False,
        message=message,
        code=code,
        terminal_outcome=(
            terminal_outcome
            if isinstance(terminal_outcome, PendingImportTerminalOutcome)
            else None
        ),
        post_commit_wrong_match_scenario=scenario,
    )


def _hold_denylist_reason(comparison: InstalledCopyComparison) -> str:
    """The per-request denylist reason for a held upgrade, by its yardstick."""
    from lib.quality.installed_copy_guard import HOLD_BASIS_DECLARED_PROGRAM

    if comparison.basis == HOLD_BASIS_DECLARED_PROGRAM:
        return (
            "upgrade held: track lengths short of the release "
            "(installed copy marked incomplete)"
        )
    return "upgrade held: track lengths differ from the installed copy"


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
        search_log_id=dl_info.search_log_id,
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
        search_log_id=dl_info.search_log_id,
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


def _automation_grab_search_link(
    db: DispatchDB, request_id: int,
) -> int | None:
    """The search link of the grab an automation preview measured (#811).

    Under invariant 10 the owner's ``active_download_state`` is still
    attached for the whole ``processing`` lifetime — the terminal bundle
    clears it LAST — so the ``search_log_id`` the search stamped is
    readable here, even though this function's own arguments carry no
    ``DownloadInfo``.

    Scoped to the automation lane by its one caller. The force and local
    lanes have no live grab state (their source is a folder already on
    disk), and ``scripts/import_preview_worker.py``'s KTD1 forbids the
    YouTube path from reading this column at all — calling this only
    under ``automation_terminal_authority`` keeps every one of them on
    exactly the behaviour they had.

    Fail-soft by construction: enriching an audit row must never turn a
    measurement failure into a crash, so any read problem yields None and
    the row keeps the NULL it would have had.
    """
    try:
        row = db.get_request(request_id)
        raw = row.get("active_download_state") if row else None
        if raw is None:
            return None
        return ActiveDownloadState.from_raw(raw).search_log_id
    except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        logger.debug(
            "Failed to read the search link for request %s while recording "
            "a preview measurement failure", request_id,
        )
        return None


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
        The automation requeue counts a download attempt so search backoff
        grows across repeated failures (issue #1423: one request failed
        16 times in seven hours at plain search cadence).
      * No denylist, cooldown or peer attribution: this is a
        measurement-world failure, not evidence against the source. A
        failure that IS the source's fault belongs in a source-fault
        outcome, not here.
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
                attempt_type=(
                    "download"
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
            # Issue #811: this function holds no ``DownloadInfo``, but an
            # automation preview failure IS a grab outcome, and the
            # grab's own state is still attached to the owned request.
            search_log_id=(
                _automation_grab_search_link(db, request_id)
                if automation_terminal_authority is not None else None
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
        search_log_id=dl_info.search_log_id,
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
