"""Completed-download rejection writers and post-rejection convergence."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from lib.dispatch import (
    DispatchOutcome,
    _build_download_info,
    _record_rejection_and_maybe_requeue,
)
from lib.dispatch.helpers import _cleanup_staged_dir
from lib.grab_list import GrabListEntry
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    cancellation_hook,
    checkpoint,
)
from lib.import_manifest import (
    move_failed_import_curated,
    move_failed_import_whole,
    tracked_audio_paths_for_downloads,
)
from lib.processing_paths import protected_staging_roots, source_dirs_for_album
from lib.quality import ValidationResult, rejection_backfill_override
from lib.release_identity import normalize_release_id
from lib.staged_album import StagedAlbum
from lib.terminal_outcomes import PendingImportTerminalOutcome, TerminalDenylist
from lib.util import log_validation_result
from lib.wrong_match_policy import (
    rejection_scenario_is_delete_eligible,
    rejection_scenario_is_wrong_match_candidate,
)

if TYPE_CHECKING:
    from lib.context import CratediggerContext
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")


def _checkpoint_then_delete_rejected_source(
    path: str,
    *,
    processing_dir: str,
    beets_staging_dir: str,
    cancellation_token: CancellationToken | None,
) -> None:
    """Destroy a rejected candidate's source outright — no quarantine.

    Reuses the existing staged-dir teardown helper (issue #1077, D3): bad
    rips have no salvage value for operator review, so nothing is moved into
    ``wrong_matches/`` or ``failed_imports/`` and no worklist row is created.

    ``path`` is USUALLY a canonical processing album — a direct, flat
    child of ``<processing_dir>/albums/`` (CLAUDE.md invariant 9) — but a
    YouTube rescue's staged audio is never materialized there (it imports
    in place from the auto-import staging child, issue #1122 F1/F2), so
    ``audio_corrupt`` can reach this function with either shared root as
    ``path``'s parent. ``protected_parents`` (issue #1077, "smalls"
    round-2 review; widened to a set in issue #1122, review round 2) stops
    ``_cleanup_staged_dir``'s empty-parent prune from ever removing
    either one: a real guard, not the incidental protection of lock-shard
    files that happen to never be unlinked.
    """
    checkpoint(cancellation_token)
    _cleanup_staged_dir(
        path,
        protected_parents=protected_staging_roots(
            processing_dir=processing_dir,
            beets_staging_dir=beets_staging_dir,
        ),
    )


def _run_post_rejection_wrong_match_cleanup(
    ctx: CratediggerContext,
    download_log_id: object,
    *,
    scenario: str | None,
    import_job_id: int | None = None,
    contributor_usernames: Iterable[str] = (),
    cancellation_token: CancellationToken | None = None,
) -> Any:
    """Link candidate evidence for every visible row, then run the reducer
    only for delete-eligible scenarios.

    Issue #1077, F3/F8: evidence-linking must happen for every worklist-
    visible row, not just the delete-eligible subset — a kept, banned,
    visible row (e.g. ``untracked_audio``) still needs its candidate
    evidence FK set so the worklist card renders the candidate's own
    measurement, even though the reducer never evaluates it for deletion.
    Mirrors ``scripts/importer.py::_cleanup_committed_wrong_match_rejection``'s
    two-stage gate: outer gate is worklist visibility (a row to link evidence
    to even exists), inner gate is delete-eligibility (the reducer may act).
    """
    if not isinstance(download_log_id, int) or isinstance(download_log_id, bool):
        return None
    if not rejection_scenario_is_wrong_match_candidate(scenario):
        # Bad rips and every other folder/audio-integrity or quality-only
        # reject were never quarantined (issue #1077, D3): there is no
        # worklist row to link evidence to or hand to the reducer.
        return None
    try:
        checkpoint(cancellation_token)
        db = ctx.pipeline_db_source._get_db()
        if import_job_id is not None:
            evidence_id = db.get_import_job_candidate_evidence_id(import_job_id)
            if evidence_id is not None:
                db.set_download_log_candidate_evidence(
                    download_log_id,
                    evidence_id,
                    direct_attribution=True,
                    contributor_usernames=tuple(contributor_usernames),
                )
        if not rejection_scenario_is_delete_eligible(scenario):
            # World failures with a reviewable folder, and every unknown or
            # novel scenario string, are kept + banned + visible (issue
            # #1077, D1/D4/D6): the evaluate-and-possibly-delete reducer
            # never even looks at them.
            return None

        from lib.wrong_match_cleanup_service import cleanup_wrong_match

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
    except ExecutionCancelled:
        raise
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
    db: PipelineDB = ctx.pipeline_db_source._get_db()
    if album_data.db_request_id is not None:
        return db, album_data.db_request_id

    candidate_request_id = album_data.album_id
    if isinstance(candidate_request_id, bool):
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
    cancellation_token: CancellationToken | None = None,
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
    # World failures (issue #1077, D4): the operator reviews the WHOLE folder
    # exactly as rejected, including anything outside the download manifest —
    # the curated move used elsewhere (``move_failed_import_curated``) drops
    # anything outside ``allowed_audio``, which is exactly the files that
    # caused an ``untracked_audio`` rejection in the first place.
    usernames = {file.username for file in album_data.files if file.username}
    failed_result.denylisted_users = sorted(usernames)
    checkpoint(cancellation_token)
    failed_result.failed_path = move_failed_import_whole(
        staged_album.current_path,
        scenario=failed_result.scenario,
        before_mutation=cancellation_hook(cancellation_token),
    )
    checkpoint(cancellation_token)
    logger.error(
        "AUTO-IMPORT REJECTED: %s - %s — %s | denylisted users: %s",
        album_data.artist,
        album_data.title,
        detail,
        ", ".join(sorted(usernames)),
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
    # World failures with a reviewable folder are kept + banned + shown
    # (issue #1077, D1/D4): denylist the contributing peers exactly like the
    # candidate-match reject lane does, so the pipeline never re-fetches the
    # identical copy while it sits in the worklist. Built once and passed
    # through ``_record_rejection_and_maybe_requeue`` so the job-backed and
    # job-less branches commit the SAME denylist entries — the job-backed
    # branch atomically alongside its owning job's terminal write, the
    # job-less branch atomically alongside this call's own request
    # transition and audit row (issue #1355 item 3).
    denylist_reason = f"auto-import world failure: {failed_result.scenario}"
    denylists = tuple(
        TerminalDenylist(username, denylist_reason, apply_cooldown=True)
        for username in sorted(usernames)
    )
    checkpoint(cancellation_token)
    persisted = _record_rejection_and_maybe_requeue(
        db,
        request_id,
        dl_info,
        detail=detail,
        error=failed_result.error,
        validation_result=failed_result.to_json(),
        requeue=True,
        import_job_id=owned_import_job_id,
        denylists=denylists,
        cooled_down_users=ctx.cooled_down_users,
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
        import_job_id=import_job_id,
        contributor_usernames=usernames,
        cancellation_token=cancellation_token,
    )
    return DispatchOutcome(success=False, message=detail)


def _handle_rejected_result(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int | None = None,
    cancellation_token: CancellationToken | None = None,
) -> DispatchOutcome:
    """Handle a rejected beets validation result."""
    bv_result.source_dirs = source_dirs_for_album(album_data)
    checkpoint(cancellation_token)
    if bv_result.scenario == "audio_corrupt":
        # Bad rips are ban + delete, never quarantined (issue #1077, D3): a
        # corrupt candidate has no salvage value for operator review. This is
        # the only scenario reaching this function that names it — the
        # pre-beets media-readiness check is its sole producer
        # (``lib/download_processing.py``); ordinary beets-validation
        # rejects never name it, and keep the curated quarantine move below.
        #
        # Issue #1077, F4: the delete is attempted, but its outcome never
        # gates the record below. A failed/partial delete (permission error,
        # a file vanishing mid-rmtree, ...) must still produce the audit
        # row, ban the contributing peer, and requeue the request — invariant
        # 11 ("broken worlds surface and restart; nothing is parked"). Before
        # this fix an uncaught delete exception propagated out of this
        # function entirely: no download_log row, no denylist entry, no
        # requeue, and the request was left wherever it was before the
        # delete — the opposite of restart-on-failure. Cancellation is a
        # distinct interruption, not a delete failure, and still propagates.
        try:
            _checkpoint_then_delete_rejected_source(
                staged_album.current_path,
                processing_dir=ctx.cfg.processing_dir,
                beets_staging_dir=ctx.cfg.beets_staging_dir,
                cancellation_token=cancellation_token,
            )
        except ExecutionCancelled:
            raise
        except Exception:
            logger.exception(
                "AUDIO-CORRUPT DELETE FAILED: %s - %s path=%s — recording "
                "the rejection and requeuing regardless",
                album_data.artist,
                album_data.title,
                staged_album.current_path,
            )
        bv_result.failed_path = None
    else:
        move_result = move_failed_import_curated(
            staged_album.current_path,
            allowed_audio=tracked_audio_paths_for_downloads(album_data.files),
            scenario=bv_result.scenario,
            before_mutation=cancellation_hook(cancellation_token),
        )
        bv_result.failed_path = (
            move_result.target_path if move_result is not None else None
        )
        # Issue #1077, B1: a curated move that had to sweep unexpected
        # residue into the destination folds that anomaly into the
        # persisted detail — it surfaces to the operator in Recents, never
        # as a stack trace, and the rejection record below is written
        # exactly as normal regardless.
        if move_result is not None and move_result.anomaly:
            bv_result.detail = (
                f"{bv_result.detail} | {move_result.anomaly}"
                if bv_result.detail else move_result.anomaly
            )
    checkpoint(cancellation_token)
    log_validation_result(album_data, bv_result, ctx.cfg)
    # The YouTube staging lane deliberately reconstructs a manifest with blank
    # usernames: those files have no slskd peer identity.  Keep its terminal
    # outcome peer-free while retaining every real uploader for slskd rejects.
    usernames = {file.username for file in album_data.files if file.username}
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
    checkpoint(cancellation_token)
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
            contributor_usernames=usernames,
            cancellation_token=cancellation_token,
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
    # ``beets_validate`` names a scenario on every result it returns — a
    # decided ``choose_match``, or ``no_choose_match`` with its
    # harness-session evidence (issue #888). The placeholder this used to
    # fall back to, ``validation_rejected``, was a token no producer ever
    # emitted and zero live rows ever carried; it is deleted rather than
    # kept "just in case".
    detail = bv_result.detail or bv_result.error
    message = f"Rejected: {bv_result.scenario}"
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
    try:
        db = ctx.pipeline_db_source._get_db()
        request = db.get_request(request_id)
        if not request:
            return None
        current_override = request.get("search_filetype_override")
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
        if override and override != current_override:
            logger.info(
                "BACKFILL: %s - %s search_filetype_override=%r → %r "
                "(linked current evidence: format=%s, spectral=%s)",
                album_data.artist,
                album_data.title,
                current_override,
                override,
                evidence.measurement.format,
                evidence.measurement.spectral_grade,
            )
            return override
        return None
    except Exception:
        logger.debug("BACKFILL: failed to load linked current evidence", exc_info=True)
        return None
