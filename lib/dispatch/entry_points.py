"""Force-import entry-point adapter.

``dispatch_import_from_db`` takes the per-request IMPORT advisory lock,
validates preconditions + the audio manifest, loads candidate evidence, and
delegates to ``dispatch_import_core``. ``ensure_candidate_evidence_for_action``
is looked up here (tests patch it on this module).
"""

from __future__ import annotations

import logging
import os
from dataclasses import replace
from typing import TYPE_CHECKING

from lib import transitions
from lib.dispatch.core import dispatch_import_core
from lib.dispatch.evidence_gate import (
    _download_info_from_candidate_evidence,
    _requeue_import_job_to_preview,
)
from lib.dispatch.manifest_guard import _guard_force_import_audio_manifest
from lib.dispatch.quality_gate import _check_quality_gate_core
from lib.dispatch.types import (
    DISPATCH_CODE_BAD_REQUEST,
    DISPATCH_CODE_PROCESSING_LOCKED,
    DispatchOutcome,
    ImportAttemptResult,
)
from lib.import_evidence import ensure_candidate_evidence_for_action
from lib.processing_paths import normalize_source_dirs
from lib.terminal_outcomes import ImportJobTerminal

if TYPE_CHECKING:
    from lib.beets_retag import MergeRetagFn
    from lib.config import CratediggerConfig
    from lib.dispatch.types import ImportOneRunner, QualityGateFn
    from lib.import_execution import CancellationToken, OwnerSessionIdentity
    from lib.mb_canonical import CanonicalReleaseFn
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")


def dispatch_import_from_db(
    db: PipelineDB,
    request_id: int,
    failed_path: str,
    *,
    source_reference_path: str | None = None,
    source_username: str | None = None,
    source_dirs: list[str] | None = None,
    import_job_id: int | None = None,
    download_log_id: int | None = None,
    quality_gate_fn: QualityGateFn | None = None,
    cfg: CratediggerConfig | None = None,
    run_import_fn: ImportOneRunner | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
    canonical_release_fn: CanonicalReleaseFn | None = None,
    retag_fn: MergeRetagFn | None = None,
) -> DispatchOutcome:
    """Run a force-import through the full dispatch pipeline.

    Requires pre-recorded candidate evidence: the caller supplies either
    ``import_job_id`` or ``download_log_id`` (or both), and dispatch loads
    the candidate ``AlbumQualityEvidence`` via
    ``ensure_candidate_evidence_for_action``. The preview worker is the
    only producer of candidate measurements; dispatch never invokes
    ``measure_preimport_state`` itself. When evidence is missing or stale, the
    job is requeued back to the preview lane via
    ``_requeue_import_job_to_preview`` (returning
    ``DISPATCH_CODE_REQUEUED_FOR_PREVIEW``); the actual measurement happens
    on the preview worker's next claim. Quality decisions (downgrade
    prevention, quality gate, media-server scans, denylist) still run identically
    to auto-import — only the beets *distance* check is overridden.

    Since #1080 that override is literal rather than structural: this lane
    runs the SAME exact-release validation the automation lane runs
    (``lib.download_validation.validate_release_with_merge_redirect``),
    differing in one argument — the distance threshold, raised to
    ``FORCE_IMPORT_DISTANCE_THRESHOLD``. See ``_dispatch_import_from_db_locked``
    for why the result is identity resolution and never a verdict.

    Concurrency (issue #92): a per-``request_id`` advisory lock (IMPORT
    namespace) is taken up front. Two concurrent force imports
    on the same request (double-click in the UI, racing CLI
    invocations) would otherwise each run the full pipeline and write
    duplicate ``download_log`` rows. The second caller fast-fails
    without side effects. ``dispatch_import_core`` below will acquire
    the RELEASE lock as the inner nested acquisition. See
    ``docs/advisory-locks.md`` for namespaces, ordering, and the
    call-site index.

    Args:
        db: PipelineDB instance
        request_id: Album request ID
        failed_path: Path to the files on disk
        source_username: Soulseek peer who supplied the source files
        source_dirs: Remote directories the source was downloaded from
        import_job_id: Import-job row this dispatch belongs to. Required
            in production (the importer always supplies it); ``None`` is
            a developer-error precondition error.
        download_log_id: Originating download_log row for Wrong Matches
            force-imports; scopes candidate-evidence lookup to that
            owner. Optional but typically supplied for force-imports.
        cfg/run_import_fn/beets_library_*: explicit isolation seams used by
            the real-storage world model. Production callers omit them and
            retain runtime config, subprocess import, and deployed Beets.
    """
    from lib.beets_db import validate_beets_storage_pair
    from lib.config import read_runtime_config
    from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT

    if (cancellation_token is None) != (owner_session_identity is None):
        raise ValueError(
            "cancellation token and owner session identity must be paired"
        )
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()

    validate_beets_storage_pair(
        db_path=beets_library_db_path,
        library_root=beets_library_root,
    )
    # Snapshot the complete runtime config before locks or lifecycle effects;
    # core uses this same snapshot for the subprocess and all Beets readers.
    resolved_cfg = cfg or read_runtime_config()

    with db.advisory_lock(ADVISORY_LOCK_NAMESPACE_IMPORT, request_id) as acquired:
        if not acquired:
            logger.warning(
                f"FORCE-IMPORT SKIPPED: request {request_id} — "
                f"another import is already in progress")
            return DispatchOutcome(
                success=False,
                message=f"Another import is already in progress for request {request_id}",
            )
        return _dispatch_import_from_db_locked(
            db, request_id, failed_path,
            source_reference_path=source_reference_path,
            source_username=source_username,
            source_dirs=source_dirs,
            import_job_id=import_job_id,
            download_log_id=download_log_id,
            quality_gate_fn=quality_gate_fn,
            cfg=resolved_cfg,
            run_import_fn=run_import_fn,
            beets_library_db_path=beets_library_db_path,
            beets_library_root=beets_library_root,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
            canonical_release_fn=canonical_release_fn,
            retag_fn=retag_fn,
        )


def _dispatch_import_from_db_locked(
    db: PipelineDB,
    request_id: int,
    failed_path: str,
    *,
    source_reference_path: str | None = None,
    source_username: str | None,
    source_dirs: list[str] | None,
    import_job_id: int | None,
    download_log_id: int | None,
    quality_gate_fn: QualityGateFn | None = None,
    cfg: CratediggerConfig | None = None,
    run_import_fn: ImportOneRunner | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
    canonical_release_fn: CanonicalReleaseFn | None = None,
    retag_fn: MergeRetagFn | None = None,
) -> DispatchOutcome:
    """Body of dispatch_import_from_db, called once the advisory lock is held.

    Precondition: at least one of ``import_job_id`` or ``download_log_id``
    MUST be supplied. After U4 (importer-never-measures refactor) the only
    production caller is ``scripts/importer.py``, which always supplies
    ``import_job_id``. The previous legacy direct-measurement branch that
    ran ``inspect_local_files`` / ``measure_preimport_state`` for callers
    that omitted both IDs has been deleted; the importer never measures.
    """
    from lib.grab_list import DownloadFile

    if import_job_id is None and download_log_id is None:
        # Programmer-error: every production caller supplies at least
        # ``import_job_id``. Reject up front rather than silently measuring.
        return DispatchOutcome(
            success=False,
            message=(
                "dispatch_import_from_db requires import_job_id or "
                "download_log_id (importer never measures; preview owns "
                "candidate evidence production)"
            ),
            code=DISPATCH_CODE_BAD_REQUEST,
        )

    source_dirs = normalize_source_dirs(source_dirs or [])
    launch_authority_path = source_reference_path or failed_path

    req = db.get_request(request_id)
    if not req:
        return DispatchOutcome(success=False, message=f"Request {request_id} not found")
    processing_locked = transitions.processing_locked_conflict(
        req,
        request_id,
        "force_import",
        expected_status=str(req["status"]),
    )
    if processing_locked is not None:
        owner = processing_locked.processing_owner
        if owner is None:
            raise RuntimeError(
                "processing conflict is missing its exact owner"
            )
        return DispatchOutcome(
            success=False,
            message=(
                f"Request {request_id} is owned by automation import "
                f"job {owner.job_id}"
            ),
            code=DISPATCH_CODE_PROCESSING_LOCKED,
        )

    mbid = req.get("mb_release_id", "")
    if not mbid:
        return DispatchOutcome(success=False, message="No MusicBrainz release ID")

    if not os.path.isdir(failed_path):
        return DispatchOutcome(success=False, message=f"Path not found: {failed_path}")

    from lib.config import read_runtime_config

    resolved_cfg = cfg or read_runtime_config()

    # The exact-release comparison, run through the SAME seam the automation
    # lane runs, with the one documented difference: the distance threshold is
    # the operator's override (#1080). Its purpose here is identity
    # resolution, not a verdict — a force import exists to import DESPITE the
    # validation verdict, so nothing below branches on ``result.valid``. What
    # it does buy is the merge-redirect follow: when MusicBrainz has merged
    # this release away, the library is retagged and the request rekeyed here,
    # and the survivor is what dispatch imports. Before this, force met the
    # merged-away release at ``import_one.py::_find_target_candidate`` instead
    # and rejected ``mbid_missing`` forever.
    #
    # Ordering: this runs before candidate evidence is loaded, because a rekey
    # moves the request's ``album_quality_evidence`` rows onto the survivor in
    # the same transaction. Loading evidence first would pin the pre-rekey
    # identity. It also runs under the IMPORT lock taken above, preserving the
    # documented ``IMPORT → RELEASE`` order for the retag's two release locks.
    from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD
    from lib.download_validation import validate_release_with_merge_redirect

    validation = validate_release_with_merge_redirect(
        db=db,
        cfg=resolved_cfg,
        album_path=failed_path,
        request_id=request_id,
        release_id=mbid,
        import_job_id=import_job_id,
        distance_threshold=FORCE_IMPORT_DISTANCE_THRESHOLD,
        cancellation_token=cancellation_token,
        canonical_release_fn=canonical_release_fn,
        retag_fn=retag_fn,
    )
    if validation.merge.rekeyed and validation.merge.survivor is not None:
        logger.info(
            "FORCE-IMPORT MERGE REKEY: request %s %s -> %s",
            request_id, mbid, validation.merge.survivor,
        )
        mbid = validation.merge.survivor
    elif validation.merge.split_identity:
        # The seam moved the installed album onto the survivor and the request
        # could not follow (the survivor was claimed inside the race window
        # the pre-check cannot cover). Launching Beets at the id the row still
        # names would import against a library album that is no longer filed
        # there — no duplicate would be flagged and the operator would read
        # the pre-#1080 ``mbid_missing`` while their library had silently
        # moved. Refuse instead; the seam has already recorded the durable
        # audit row. Nothing is parked: the job terminalizes failed with this
        # message and the request keeps whatever runnable status it had.
        return DispatchOutcome(
            success=False,
            message=validation.merge.detail,
        )

    files: list[DownloadFile] = []
    if source_username:
        files = [DownloadFile(
            filename="", id="", file_dir="",
            username=source_username, size=0,
        )]

    label = f"{req.get('artist_name', '')} - {req.get('album_title', '')}"

    candidate_result = ensure_candidate_evidence_for_action(
        db,
        source_path=failed_path,
        import_job_id=import_job_id,
        download_log_id=download_log_id,
    )
    if not candidate_result.available or candidate_result.evidence is None:
        reason = (
            candidate_result.provenance.fallback_reason
            or candidate_result.provenance.candidate_status
            or "missing"
        )
        # U2: requeue to preview rather than failing. Preview owns
        # candidate-evidence production; the importer never measures.
        return _requeue_import_job_to_preview(
            db,
            import_job_id=import_job_id,
            reason=reason,
        )

    # Candidate freshness is the first force-import gate.  Manifest drift is
    # evidence drift, not a terminal audit outcome: preview will snapshot the
    # current action tree and re-establish the candidate before this guard
    # evaluates the operator's expected audio manifest.
    attempt_result = ImportAttemptResult.from_import_job(db, import_job_id)
    manifest_reject = _guard_force_import_audio_manifest(
        db,
        request_id=request_id,
        failed_path=failed_path,
        audit_source_path=source_reference_path,
        download_log_id=download_log_id,
        source_username=source_username,
        attempt_result=attempt_result,
        import_job_id=import_job_id,
    )
    if manifest_reject is not None:
        return _persist_terminal_dispatch_outcome(
            db,
            manifest_reject,
            defer=_job_is_running(db, import_job_id),
        )

    contributor_usernames: tuple[str, ...] = ()
    if download_log_id is not None:
        source_log = db.get_download_log_entry(download_log_id)
        if source_log is not None:
            contributor_usernames = tuple(
                source_log["candidate_contributor_usernames"] or ()
            )
    dl_info = _download_info_from_candidate_evidence(
        candidate_result.evidence,
        username=source_username,
        contributor_usernames=contributor_usernames,
    )
    resolved_quality_gate_fn = (
        quality_gate_fn if quality_gate_fn is not None else _check_quality_gate_core
    )
    outcome = dispatch_import_core(
        path=failed_path,
        mb_release_id=mbid,
        request_id=request_id,
        label=label,
        force=True,
        override_min_bitrate=None,
        target_format=req.get("target_format"),
        verified_lossless_target=resolved_cfg.verified_lossless_target,
        beets_harness_path=resolved_cfg.beets_harness_path,
        db=db,
        dl_info=dl_info,
        # Force-import explicitly bypasses the beets distance
        # check — no measurement exists to report (#550 defect #4).
        distance=None,
        scenario="force_import",
        files=files,
        cfg=resolved_cfg,
        outcome_label="force_import",
        requeue_on_failure=False,
        source_dirs=source_dirs,
        candidate_import_job_id=import_job_id,
        attempt_result=attempt_result,
        candidate_download_log_id=download_log_id,
        launch_authority_path=launch_authority_path,
        prevalidated_candidate_result=candidate_result,
        quality_gate_fn=resolved_quality_gate_fn,
        run_import_fn=run_import_fn,
        beets_library_db_path=beets_library_db_path,
        beets_library_root=beets_library_root,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
    )
    return _persist_terminal_dispatch_outcome(
        db,
        outcome,
        defer=_job_is_running(db, import_job_id),
    )


def _job_is_running(db: PipelineDB, import_job_id: int | None) -> bool:
    if import_job_id is None:
        return False
    job = db.get_import_job(import_job_id)
    return job is not None and job.status == "running"


def _persist_terminal_dispatch_outcome(
    db: PipelineDB,
    outcome: DispatchOutcome,
    *,
    defer: bool,
) -> DispatchOutcome:
    """Finalize direct calls while letting the queue owner add job metadata."""
    pending = outcome.terminal_outcome
    if pending is None or defer:
        return outcome
    result: dict[str, object] = {
        "success": outcome.success,
        "message": outcome.message,
        "deferred": outcome.deferred,
        "code": outcome.code,
    }
    db.persist_import_terminal_outcome(pending.with_job(ImportJobTerminal(
        status="completed" if outcome.success else "failed",
        error=None if outcome.success else outcome.message,
        result=result,
        message=outcome.message,
    )))
    return replace(outcome, terminal_outcome=None)
