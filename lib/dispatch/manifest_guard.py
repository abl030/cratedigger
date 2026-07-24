"""Force-import audio-manifest guard.

Reconciles a staged source folder against its validated audio reference
before beets can run, preserving both the operator-owned request status and
source folder on any mismatch. See ``_guard_reject``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from lib.quality import DownloadInfo
from lib.import_manifest import (
    audio_relative_paths,
    check_audio_manifest,
    tracked_audio_paths_from_validation_items,
)
from lib.validation_envelope import decode_validation_envelope
from lib.quality import ValidationResult

from lib.dispatch.types import (DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
                                DispatchOutcome, ImportAttemptResult)
from lib.dispatch.outcome_actions import _record_rejection_and_maybe_requeue
from lib.terminal_outcomes import PendingImportTerminalOutcome

if TYPE_CHECKING:
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")


def _origin_manifest_for_download_log(
    db: "PipelineDB",
    *,
    download_log_id: int | None,
    failed_path: str,
) -> list[str]:
    if download_log_id is None:
        return []
    entry = db.get_download_log_entry(download_log_id)
    if entry is None:
        return []
    vr = decode_validation_envelope(entry.get("validation_result"))
    if not vr.items:
        return []
    return tracked_audio_paths_from_validation_items(vr.items, root=failed_path)


def _expected_request_track_count(db: "PipelineDB", request_id: int) -> int | None:
    try:
        tracks = db.get_tracks(request_id)
    except Exception:
        logger.debug("Failed to read expected tracks for manifest guard", exc_info=True)
        return None
    return len(tracks) if tracks else None


def _guard_reject(
    db: "PipelineDB",
    *,
    request_id: int,
    failed_path: str,
    audit_source_path: str | None = None,
    source_username: str | None,
    attempt_result: ImportAttemptResult,
    detail: str,
    scenario: str,
    import_job_id: int | None = None,
    source_download_log_id: int | None = None,
) -> DispatchOutcome:
    """Reject a force-import at the manifest guard.

    Writes the mandatory ``download_log`` audit row without changing the
    request lifecycle or retry counters. A force attempt may inspect a
    ``wanted`` request, or one whose search was explicitly stopped by the
    operator; neither state belongs to this guard to rewrite.

    When a private action copy is used, the audit retains its original
    operator-owned source path rather than exposing the disposable copy.

    Always returns ``DISPATCH_CODE_IMPORT_MANIFEST_REJECTED`` so the importer
    worker preserves the original operator source. The guard only ever sees a
    *non-empty* folder (an empty source returns ``None`` from the caller and
    reaches the evidence pipeline's ``empty_fileset`` early-exit instead), so
    deleting here would always destroy real audio the operator
    chose to import — the irreversible auto-decision the archivist frame
    forbids. Wrong-match-folder deletion is reserved for the genuinely-empty
    (0-file) case, which routes through the evidence pipeline, not this guard.

    No denylist write either — a manifest mismatch reflects the operator's
    folder choice, not the peer's quality (mirrors ``nested_layout``).
    """
    logger.error("IMPORT GUARD REJECT (%s): path=%s %s", scenario, failed_path, detail)
    # No beets distance was measured — this guard fires before beets can
    # even run (#550 defect #4). Record NULL, not a fabricated 0.0.
    pending = _record_rejection_and_maybe_requeue(
        db,
        request_id,
        DownloadInfo(username=source_username),
        detail=detail,
        error=None,
        validation_result=ValidationResult(
            distance=None,
            scenario=scenario,
            detail=detail,
            failed_path=audit_source_path or failed_path,
        ).to_json(),
        requeue=False,
        outcome_label="rejected",
        staged_path=failed_path,
        attempt_result=attempt_result,
        import_job_id=import_job_id,
        source_download_log_id=source_download_log_id,
    )
    return DispatchOutcome(
        success=False,
        message=detail,
        code=DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
        terminal_outcome=(
            pending
            if isinstance(pending, PendingImportTerminalOutcome)
            else None
        ),
    )


def _guard_force_import_audio_manifest(
    db: "PipelineDB",
    *,
    request_id: int,
    failed_path: str,
    audit_source_path: str | None = None,
    download_log_id: int | None,
    source_username: str | None,
    attempt_result: ImportAttemptResult,
    import_job_id: int | None = None,
) -> DispatchOutcome | None:
    """Reconcile the staged source against its validated audio reference.

    Outcomes, keyed on whether the source has *extra*, *matching*, or
    *missing* audio relative to the reference (origin manifest, else request
    track count). Every reject preserves the request status and the operator's
    source folder (see ``_guard_reject``); they
    differ only in the audit ``scenario`` label.

    * **PROCEED** (``None``) — on-disk audio matches the reference, OR the
      source is empty (0 audio files). An empty source flows through to the
      canonical ``empty_fileset`` early-exit in
      ``full_pipeline_decision_from_evidence`` (or a requeue-to-preview when
      evidence isn't ready); caller lifecycle authority determines status, and the
      (empty) folder cleanup is owned there. The guard does NOT own the
      ``empty_fileset`` decision; that lives in ONE place (see CLAUDE.md
      § "Quality decisions live in ONE place").
    * **INCOMPLETE** (``incomplete_fileset``) — the source is *missing* audio
      (under-count or manifest subset, no extras) but still has real files on
      disk. Preserve status + keep the folder for review.
    * **EXTRA / UNVERIFIABLE** (``untracked_audio`` / ``unverifiable_source``)
      — the source carries *extra* untracked audio, or there is no reference
      at all for a non-empty source. Preserve status + keep the folder; passing it
      to beets would import unowned files, so the operator must review it.
    """
    expected_count = _expected_request_track_count(db, request_id)
    manifest = _origin_manifest_for_download_log(
        db,
        download_log_id=download_log_id,
        failed_path=audit_source_path or failed_path,
    )
    actual_audio = audio_relative_paths(failed_path)

    def incomplete(detail: str) -> DispatchOutcome:
        return _guard_reject(
            db, request_id=request_id, failed_path=failed_path,
            audit_source_path=audit_source_path,
            source_username=source_username, detail=detail,
            scenario="incomplete_fileset", attempt_result=attempt_result,
            import_job_id=import_job_id,
            source_download_log_id=download_log_id)

    def extra(detail: str, *, scenario: str = "untracked_audio") -> DispatchOutcome:
        return _guard_reject(
            db, request_id=request_id, failed_path=failed_path,
            audit_source_path=audit_source_path,
            source_username=source_username, detail=detail,
            scenario=scenario, attempt_result=attempt_result,
            import_job_id=import_job_id,
            source_download_log_id=download_log_id)

    # Empty source: the canonical empty_fileset early-exit in the evidence
    # pipeline owns the verdict. Returning None lets the import flow reach it.
    if not actual_audio:
        return None

    if manifest:
        if expected_count is not None and len(manifest) != expected_count:
            if len(manifest) > expected_count:
                return extra(
                    "Origin validation manifest has "
                    f"{len(manifest)} audio files but the request expects "
                    f"{expected_count}; refusing force import")
            return incomplete(
                "Origin validation manifest has "
                f"{len(manifest)} audio files but the request expects "
                f"{expected_count}; source is missing audio")
        check = check_audio_manifest(failed_path, manifest)
        if check.ok:
            return None
        if check.extra_audio:
            return extra(
                "Force import source does not match the original "
                f"selected audio manifest: {check.detail()}")
        # Only missing audio — the source is a strict subset of the manifest.
        return incomplete(
            "Force import source is missing validated audio: "
            f"{check.detail()}")

    if expected_count is None:
        # Non-empty source with no manifest and no track rows: we cannot
        # verify the folder is owned, so fail closed against beets and keep
        # it for review.
        return extra(
            "Force import requires either an origin audio manifest or "
            "request track rows; refusing to pass an unowned folder to beets",
            scenario="unverifiable_source")

    if len(actual_audio) == expected_count:
        return None
    detail = (
        "Force import source has "
        f"{len(actual_audio)} audio files but the request expects "
        f"{expected_count}; source audio: {', '.join(actual_audio)}"
    )
    if len(actual_audio) > expected_count:
        return extra(detail)
    # Under-count — fewer audio files than the request expects.
    return incomplete(detail)
