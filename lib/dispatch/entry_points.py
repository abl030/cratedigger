"""Force / manual import entry-point adapters.

``dispatch_import_from_db`` takes the per-request IMPORT advisory lock,
validates preconditions + the audio manifest, loads candidate evidence, and
delegates to ``dispatch_import_core``. ``ensure_candidate_evidence_for_action``
is looked up here (tests patch it on this module).
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from lib.processing_paths import normalize_source_dirs
from lib.import_evidence import ensure_candidate_evidence_for_action

from lib.dispatch.types import DISPATCH_CODE_BAD_REQUEST, DispatchOutcome
from lib.dispatch.manifest_guard import _guard_force_manual_audio_manifest
from lib.dispatch.evidence_gate import (_download_info_from_candidate_evidence,
                                        _requeue_import_job_to_preview)
from lib.dispatch.core import dispatch_import_core
from lib.dispatch.quality_gate import _check_quality_gate_core

if TYPE_CHECKING:
    from lib.pipeline_db import DownloadLogOutcome, PipelineDB
    from lib.dispatch.types import QualityGateFn

logger = logging.getLogger("cratedigger")


def dispatch_import_from_db(
    db: "PipelineDB",
    request_id: int,
    failed_path: str,
    *,
    force: bool = False,
    outcome_label: DownloadLogOutcome = "force_import",
    source_username: str | None = None,
    source_dirs: list[str] | None = None,
    import_job_id: int | None = None,
    download_log_id: int | None = None,
    quality_gate_fn: "QualityGateFn | None" = None,
) -> "DispatchOutcome":
    """Run a force-import or manual-import through the full dispatch pipeline.

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
    prevention, quality gate, meelo scan, denylist) still run identically
    to auto-import — only the beets *distance* check is skipped when
    ``force=True``.

    Concurrency (issue #92): a per-``request_id`` advisory lock (IMPORT
    namespace) is taken up front. Two concurrent force/manual imports
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
        force: Pass --force to import_one.py (bypass distance check)
        outcome_label: download_log outcome label for successful imports
        source_username: Soulseek peer who supplied the source files
        source_dirs: Remote directories the source was downloaded from
        import_job_id: Import-job row this dispatch belongs to. Required
            in production (the importer always supplies it); ``None`` is
            a developer-error precondition error.
        download_log_id: Originating download_log row for Wrong Matches
            force-imports; scopes candidate-evidence lookup to that
            owner. Optional but typically supplied for force-imports.
    """
    from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT

    with db.advisory_lock(ADVISORY_LOCK_NAMESPACE_IMPORT, request_id) as acquired:
        if not acquired:
            mode = "FORCE-IMPORT" if force else "MANUAL-IMPORT"
            logger.warning(
                f"{mode} SKIPPED: request {request_id} — "
                f"another import is already in progress")
            return DispatchOutcome(
                success=False,
                message=f"Another import is already in progress for request {request_id}",
            )
        return _dispatch_import_from_db_locked(
            db, request_id, failed_path,
            force=force,
            outcome_label=outcome_label,
            source_username=source_username,
            source_dirs=source_dirs,
            import_job_id=import_job_id,
            download_log_id=download_log_id,
            quality_gate_fn=quality_gate_fn,
        )


def _dispatch_import_from_db_locked(
    db: "PipelineDB",
    request_id: int,
    failed_path: str,
    *,
    force: bool,
    outcome_label: DownloadLogOutcome,
    source_username: str | None,
    source_dirs: list[str] | None,
    import_job_id: int | None,
    download_log_id: int | None,
    quality_gate_fn: "QualityGateFn | None" = None,
) -> "DispatchOutcome":
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

    req = db.get_request(request_id)
    if not req:
        return DispatchOutcome(success=False, message=f"Request {request_id} not found")

    mbid = req.get("mb_release_id", "")
    if not mbid:
        return DispatchOutcome(success=False, message="No MusicBrainz release ID")

    if not os.path.isdir(failed_path):
        return DispatchOutcome(success=False, message=f"Path not found: {failed_path}")

    manifest_reject = _guard_force_manual_audio_manifest(
        db,
        request_id=request_id,
        failed_path=failed_path,
        download_log_id=download_log_id,
        source_username=source_username,
    )
    if manifest_reject is not None:
        return manifest_reject

    from lib.config import read_runtime_config

    cfg = read_runtime_config()

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
    dl_info = _download_info_from_candidate_evidence(
        candidate_result.evidence,
        username=source_username,
    )
    resolved_quality_gate_fn = (
        quality_gate_fn if quality_gate_fn is not None else _check_quality_gate_core
    )
    return dispatch_import_core(
        path=failed_path,
        mb_release_id=mbid,
        request_id=request_id,
        label=label,
        force=force,
        override_min_bitrate=None,
        target_format=req.get("target_format"),
        verified_lossless_target=cfg.verified_lossless_target,
        beets_harness_path=cfg.beets_harness_path,
        db=db,
        dl_info=dl_info,
        # Force/manual import explicitly bypasses the beets distance
        # check — no measurement exists to report (#550 defect #4).
        distance=None,
        scenario="force_import" if force else "manual_import",
        files=files,
        cfg=cfg,
        outcome_label=outcome_label,
        requeue_on_failure=False,
        source_dirs=source_dirs,
        candidate_import_job_id=import_job_id,
        candidate_download_log_id=download_log_id,
        prevalidated_candidate_result=candidate_result,
        quality_gate_fn=resolved_quality_gate_fn,
    )
