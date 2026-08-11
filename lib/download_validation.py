"""Completed-download manifest validation and validated-result dispatch.

This module owns the boundary from a materialized album through beets exact-
release validation and candidate-evidence gating to the staged dispatch handoff.
Completion result tagging remains in :mod:`lib.download_processing`, filesystem
materialization in :mod:`lib.download_materialization`, and reject persistence
in :mod:`lib.download_rejection`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal, Protocol

from lib.beets_retag import (
    RETAG_FAILED,
    RETAG_READY_OUTCOMES,
    BeetsRetagResult,
    retag_merged_album,
)
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
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
)
from lib.import_manifest import (
    audio_relative_paths,
    check_audio_manifest,
    manifest_trace_summary,
    tracked_audio_paths_for_downloads,
)
from lib.mb_canonical import CanonicalReleaseFn, production_canonical_release_fn
from lib.processing_paths import source_dirs_for_album, stage_to_ai_path
from lib.quality import (
    SpectralEvidenceFacts,
    ValidationResult,
    compute_effective_override_bitrate,
    interpret_spectral_evidence,
)
from lib.release_identity import ReleaseIdentity, normalize_release_id
from lib.staged_album import StagedAlbum
from lib.util import log_validation_result

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")

#: The production merge-survivor resolver, bound once at import. It reads the
#: process's configured WS/2 base LATE (per call), so startup order cannot
#: matter, and holding it in a module singleton keeps it a definition-time
#: default that tests inject rather than patch.
_PRODUCTION_CANONICAL_RELEASE_FN: Final[CanonicalReleaseFn] = (
    production_canonical_release_fn()
)


def _checkpoint(cancellation_token: CancellationToken | None) -> None:
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()


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
        cancellation_token: CancellationToken | None = None,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        owner_session_identity: OwnerSessionIdentity | None = None,
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
        cancellation_token: CancellationToken | None = None,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        owner_session_identity: OwnerSessionIdentity | None = None,
    ) -> DispatchOutcome | None: ...


#: The request was never a candidate for a merge rekey: no request row, or a
#: release identity MusicBrainz has no redirect concept for (Discogs).
MERGE_NOT_APPLICABLE: Final = "not_applicable"
#: This validation does not hold the request's exact processing owner, so it
#: has no authority to retag the library or move the row (YouTube rescue, or a
#: stale owner). Checked BEFORE the mirror so the lookup is never spent on a
#: world that could not act on the answer.
MERGE_NOT_OWNED: Final = "not_owned"
#: MusicBrainz still considers the stored id current, or would not answer.
#: The overwhelmingly common result, and the reason the mirror is only asked
#: on ``mbid_not_found``.
MERGE_NO_REDIRECT: Final = "no_redirect"
#: A merge is real, but this download is not the survivor either.
MERGE_SURVIVOR_NOT_OFFERED: Final = "survivor_not_offered"
#: The library could not be moved onto the survivor. Nothing else happens:
#: rekeying now would make the next import land a SECOND album.
MERGE_RETAG_NOT_READY: Final = "retag_not_ready"
#: The library moved but the request row did not (another request already
#: holds the survivor, or the owner fence lost). Fails closed.
MERGE_REKEY_REFUSED: Final = "rekey_refused"
#: The library is at the survivor and the request row now names it.
MERGE_REKEYED: Final = "rekeyed"

type MergeRekeyStatus = Literal[
    "not_applicable",
    "not_owned",
    "no_redirect",
    "survivor_not_offered",
    "retag_not_ready",
    "rekey_refused",
    "rekeyed",
]


@dataclass(frozen=True)
class MergeRekeyOutcome:
    """What the merge-redirect branch did, and why."""

    status: MergeRekeyStatus
    detail: str = ""
    survivor: str | None = None

    @property
    def rekeyed(self) -> bool:
        return self.status == MERGE_REKEYED


class MergeRetagFn(Protocol):
    """Exact injection contract for the one-album library retag."""

    def __call__(
        self,
        cfg: CratediggerConfig,
        *,
        old_identity: ReleaseIdentity,
        new_identity: ReleaseIdentity,
    ) -> BeetsRetagResult: ...


def _retag_merged_album_with_beets(
    cfg: CratediggerConfig,
    *,
    old_identity: ReleaseIdentity,
    new_identity: ReleaseIdentity,
) -> BeetsRetagResult:
    """Open the deployment-owned Beets library and run the one-album retag.

    The Beets handle is the only thing this adds over
    :func:`lib.beets_retag.retag_merged_album`; a handle that cannot be opened
    is a typed failure, never an exception into the import path.
    """
    from lib.beets_db import open_beets_db

    try:
        with open_beets_db(cfg) as beets:
            return retag_merged_album(
                beets,
                old_identity=old_identity,
                new_identity=new_identity,
            )
    except Exception as exc:  # noqa: BLE001 - external edge, typed outcome
        return BeetsRetagResult(
            outcome=RETAG_FAILED,
            detail=(
                "Beets library could not be opened for the merge retag: "
                f"{type(exc).__name__}: {exc}"
            ),
        )


def _follow_merged_release(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    canonical_release_fn: CanonicalReleaseFn,
    retag_fn: MergeRetagFn,
) -> MergeRekeyOutcome:
    """Follow a MusicBrainz merge when the exact release stops matching.

    ``mbid_not_found`` is where a merged-away request announces itself: Beets
    offers the survivor, our matcher demands the stored id, and the download is
    rejected forever. This is the only place that asks MusicBrainz what the
    release is called now, and it asks ONLY here — the ~8,500 healthy rows a
    cycle never touch the mirror.

    **Ordering is the whole design.** Beets keys album duplicate detection on
    ``mb_albumid`` (``duplicate_keys: album: [mb_albumid, discogs_albumid]``),
    so a request rekeyed to the survivor while the installed album is still
    filed under the merged-away id flags NO duplicate: the import lands a
    SECOND album beside the first, and the existing-album lookup misses so the
    quality decision routes through ``import_no_exist`` and silently skips the
    downgrade guard. Retag first, verify the library observably moved, and only
    then move the row.

    Every failure keeps today's rejection exactly as it was and leaves the
    request runnable for the next cycle — nothing is flagged for a human
    (invariant 11). ``bv_result`` and ``album_data`` are mutated only on the
    final success, after every fallible step has already succeeded.
    """
    request_id = album_data.db_request_id
    if request_id is None:
        return MergeRekeyOutcome(MERGE_NOT_APPLICABLE, "no request row")
    stored = normalize_release_id(album_data.mb_release_id)
    old_identity = ReleaseIdentity.from_id(stored)
    if old_identity is None or old_identity.source != "musicbrainz":
        # Discogs release ids have no redirect concept; this is not an
        # adapter between the two sources.
        return MergeRekeyOutcome(
            MERGE_NOT_APPLICABLE,
            f"{album_data.mb_release_id!r} is not a MusicBrainz release id",
        )

    # The write below is fenced on all three of these anyway; checking them
    # here means an unowned world (a YouTube rescue, a stale owner, a row
    # somebody else already moved) never spends a mirror lookup or retags the
    # shared Beets library for a rekey that could not land.
    db = ctx.pipeline_db_source._get_db()
    row = db.get_request(request_id)
    if row is None:
        return MergeRekeyOutcome(
            MERGE_NOT_OWNED, f"request {request_id} no longer exists",
        )
    if (
        row.get("status") != "processing"
        or row.get("active_automation_import_job_id") != import_job_id
    ):
        return MergeRekeyOutcome(
            MERGE_NOT_OWNED,
            (
                f"request {request_id} is {row.get('status')!r} owned by "
                f"{row.get('active_automation_import_job_id')!r}, not by this "
                f"import job {import_job_id}"
            ),
        )
    if normalize_release_id(row.get("mb_release_id")) != old_identity.release_id:
        return MergeRekeyOutcome(
            MERGE_NOT_OWNED,
            (
                f"request {request_id} now names "
                f"{row.get('mb_release_id')!r}, not the validated "
                f"{old_identity.release_id}"
            ),
        )

    survivor = canonical_release_fn(old_identity.release_id)
    new_identity = (
        ReleaseIdentity.from_id(survivor) if survivor is not None else None
    )
    if (
        new_identity is None
        or new_identity.source != "musicbrainz"
        or new_identity == old_identity
    ):
        # Fails closed on every non-answer alike, INCLUDING a resolver that
        # hands back the stored id or a non-MusicBrainz one. Its contract
        # already forbids both; a seam that authorizes a retag of installed
        # files and a rekey of the request does not delegate that check
        # (found by the generated property, which supplies both).
        return MergeRekeyOutcome(
            MERGE_NO_REDIRECT,
            f"MusicBrainz declares no successor for {old_identity.release_id}",
        )

    match = next(
        (
            candidate
            for candidate in bv_result.candidates
            if normalize_release_id(candidate.mbid) == new_identity.release_id
        ),
        None,
    )
    if match is None:
        return MergeRekeyOutcome(
            MERGE_SURVIVOR_NOT_OFFERED,
            (
                f"{old_identity.release_id} was merged into "
                f"{new_identity.release_id}, but this download does not match "
                "the survivor either"
            ),
            survivor=new_identity.release_id,
        )

    retag = retag_fn(
        ctx.cfg,
        old_identity=old_identity,
        new_identity=new_identity,
    )
    if retag.outcome not in RETAG_READY_OUTCOMES:
        # Gate on membership, never on ``!= failed``: ``ambiguous`` is not a
        # failure and still must not authorize a rekey.
        return MergeRekeyOutcome(
            MERGE_RETAG_NOT_READY,
            f"library retag returned {retag.outcome}: {retag.detail}",
            survivor=new_identity.release_id,
        )

    if not db.update_request_release_for_merge(
        request_id,
        old_release_id=old_identity.release_id,
        new_release_id=new_identity.release_id,
        expected_import_job_id=import_job_id,
    ):
        return MergeRekeyOutcome(
            MERGE_REKEY_REFUSED,
            (
                f"request {request_id} could not be rekeyed onto "
                f"{new_identity.release_id}; another request may already hold "
                "it (merging or deleting a request is an operator decision)"
            ),
            survivor=new_identity.release_id,
        )

    album_data.mb_release_id = new_identity.release_id
    # ONE place turns a candidate into a scenario — the same function
    # ``beets_validate`` uses for the requested release (issue #1059).
    from lib.beets import apply_candidate_scenario

    apply_candidate_scenario(bv_result, match, ctx.cfg.beets_distance_threshold)
    return MergeRekeyOutcome(
        MERGE_REKEYED,
        (
            f"{old_identity.release_id} was merged into "
            f"{new_identity.release_id}; {retag.detail}"
        ),
        survivor=new_identity.release_id,
    )


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
    cancellation_token: CancellationToken | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
    canonical_release_fn: CanonicalReleaseFn = _PRODUCTION_CANONICAL_RELEASE_FN,
    retag_fn: MergeRetagFn = _retag_merged_album_with_beets,
) -> DispatchOutcome | None:
    """Validate one exact release and route its canonical result.

    Candidate evidence must already have been produced by preview. Missing
    evidence requeues the job to preview; the importer never measures inline.

    ``canonical_release_fn`` and ``retag_fn`` are definition-time defaults for
    the MusicBrainz merge seam below: tests INJECT replacements, they never
    patch the module binding, because patching does not replace a captured
    default (``.claude/rules/code-quality.md`` § mocks, strategy 2).
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
            cancellation_token=cancellation_token,
        )
    _checkpoint(cancellation_token)
    bv_result = _bv(
        ctx.cfg.beets_harness_path,
        current_path,
        album_data.mb_release_id,
        ctx.cfg.beets_distance_threshold,
    )
    _checkpoint(cancellation_token)
    usernames_pre = {f.username for f in album_data.files if f.username}
    bv_result.soulseek_username = (
        ", ".join(sorted(usernames_pre)) if usernames_pre else None
    )
    bv_result.download_folder = current_path
    bv_result.source_dirs = source_dirs_for_album(album_data)
    if bv_result.scenario == "mbid_not_found":
        # The one place a MusicBrainz merge is followed. Gated on the exact
        # scenario so the mirror is never touched by a healthy validation.
        merge = _follow_merged_release(
            album_data,
            bv_result,
            ctx,
            import_job_id=import_job_id,
            canonical_release_fn=canonical_release_fn,
            retag_fn=retag_fn,
        )
        if merge.rekeyed:
            logger.info(
                "MERGE REKEY: request=%s %s (now scenario=%s valid=%s)",
                album_data.db_request_id,
                merge.detail,
                bv_result.scenario,
                bv_result.valid,
            )
        elif merge.status not in (MERGE_NOT_APPLICABLE, MERGE_NO_REDIRECT):
            # Surfaced, never parked: the existing rejection stands and the
            # request goes back to the search pool for the next cycle.
            logger.warning(
                "MERGE REKEY DECLINED (%s): request=%s %s",
                merge.status,
                album_data.db_request_id,
                merge.detail,
            )
    if bv_result.valid:
        _checkpoint(cancellation_token)
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
                expected_execution_lease=execution_lease,
            )
        resolved_handle_valid = (
            handle_valid_fn if handle_valid_fn is not None else _handle_valid_result
        )
        if cancellation_token is None:
            return resolved_handle_valid(
                album_data,
                bv_result,
                staged_album,
                ctx,
                import_job_id=import_job_id,
                prevalidated_candidate_result=candidate_result,
                dispatch_fn=dispatch_fn,
                execution_lease=execution_lease,
                owner_session_identity=owner_session_identity,
            )
        return resolved_handle_valid(
            album_data,
            bv_result,
            staged_album,
            ctx,
            import_job_id=import_job_id,
            prevalidated_candidate_result=candidate_result,
            dispatch_fn=dispatch_fn,
            cancellation_token=cancellation_token,
            execution_lease=execution_lease,
            owner_session_identity=owner_session_identity,
        )
    return _handle_rejected_result(
        album_data,
        bv_result,
        staged_album,
        ctx,
        import_job_id=import_job_id,
        cancellation_token=cancellation_token,
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
    cancellation_token: CancellationToken | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
) -> DispatchOutcome | None:
    """Dispatch a valid exact-release result from its authoritative path.

    Request imports remain at their durable processing-owner path. Redownloads
    move to manual-review staging and mark the request done.
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
            cancellation_token=cancellation_token,
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
            cancellation_token=cancellation_token,
        )

    will_auto_import = wants_auto_import
    pdb = None

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
                "dispatch. Files stay at "
                f"{staged_album.current_path} so the next cycle can "
                "idempotently resume from process_completed_album."
            )
            if execution_lease is not None:
                return _requeue_import_job_to_preview(
                    ctx.pipeline_db_source._get_db(),
                    import_job_id=import_job_id,
                    reason="release lock contention",
                    expected_execution_lease=execution_lease,
                )
            return DispatchOutcome(
                success=False,
                message=(
                    "Another import is already in progress for "
                    f"this release ({album_data.mb_release_id})"
                ),
                deferred=True,
            )

        if will_auto_import:
            # The processing handoff persisted this exact path as immutable
            # owner provenance. Beets launch, journaled cleanup, and terminal
            # acknowledgement all fence on the same value, so relocating the
            # folder here would split live filesystem state from its durable
            # authority.
            dest = staged_album.current_path
        else:
            _checkpoint(cancellation_token)
            dest = staged_album.move_to(
                stage_to_ai_path(
                    artist=album_data.artist,
                    title=album_data.title,
                    staging_dir=ctx.cfg.beets_staging_dir,
                    request_id=request_id,
                    auto_import=False,
                ),
                cancellation_token=cancellation_token,
            )
        _checkpoint(cancellation_token)
        album_data.import_folder = dest
        log_validation_result(album_data, bv_result, ctx.cfg, dest_path=dest)
        logger.info(
            f"{'PROCESSING SOURCE' if will_auto_import else 'STAGED'}: "
            f"{album_data.artist} - {album_data.title} "
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
            # This branch is reached only for ``bv_result.valid``, and
            # ``beets_validate`` sets ``valid`` in exactly one place — the
            # ``strong_match`` arm, which names the scenario in the same
            # statement. The ``or "auto_import"`` placeholder that used to
            # sit on both dispatch calls below was therefore unreachable
            # (zero live ``download_log`` rows ever carried it) and is gone
            # (issue #888).
            dispatch_scenario = bv_result.scenario
            assert dispatch_scenario is not None, (
                "beets_validate names a scenario on every valid result"
            )
            current_spectral = album_data.current_spectral
            # Codec-aware (issue #829 Phase 5 PR2b). This fallback seam holds
            # only a fresh spectral audit of the installed files, so the
            # measured ``codec_family``/``cliff_hz`` captured alongside the
            # grade are the whole codec context; a legacy audit that captured
            # neither withholds, which leaves the container bitrate untouched.
            # ``lib/dispatch/core.py`` overrides this value from linked current
            # evidence (a strictly richer resolution) whenever one exists.
            override_min_bitrate = compute_effective_override_bitrate(
                album_data.current_min_bitrate,
                interpret_spectral_evidence(SpectralEvidenceFacts(
                    spectral_grade=(
                        current_spectral.grade
                        if current_spectral is not None
                        else None
                    ),
                    codec_family=(
                        current_spectral.codec_family
                        if current_spectral is not None
                        else None
                    ),
                    cliff_hz=(
                        current_spectral.cliff_hz
                        if current_spectral is not None
                        else None
                    ),
                    spectral_bitrate_kbps=(
                        current_spectral.bitrate_kbps
                        if current_spectral is not None
                        else None
                    ),
                )),
            )

            resolved_quality_gate_fn = (
                quality_gate_fn
                if quality_gate_fn is not None
                else _check_quality_gate_core
            )
            if dispatch_fn is not None:
                _checkpoint(cancellation_token)
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
                    scenario=dispatch_scenario,
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
                    execution_lease=execution_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                )
            _checkpoint(cancellation_token)
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
                scenario=dispatch_scenario,
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
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
        _checkpoint(cancellation_token)
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
