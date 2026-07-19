"""Action-time quality-evidence load/gate + post-import evidence refresh.

The preview->importer contract's importer-side seam: load persisted
candidate/current ``AlbumQualityEvidence`` for a mutating import, write the
action-file consumed by ``import_one.py``, requeue back to preview when
evidence is unavailable, and refresh current evidence (+ sidecar) after a
successful import. ``load_current_evidence_for_action`` is looked up here.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any, Callable, TYPE_CHECKING

import msgspec

from lib.quality import (DownloadInfo, QualityEvidenceActionPayload,
                         QualityEvidenceActionProvenance, SpectralMeasurement,
                         evidence_decision_name)
from lib.quality_evidence import (
    EvidenceBuildResult,
    audit_v0_probe_from_metric,
    backfill_current_evidence_from_album_info,
    propagate_candidate_evidence_to_current,
)
from lib.import_evidence import (
    CandidateEvidenceActionResult,
    CurrentEvidenceActionResult,
    CURRENT_STATUS_FAILED,
    CURRENT_STATUS_MISSING,
    ensure_candidate_evidence_for_action,
    load_current_evidence_for_action,
)

from lib.dispatch.types import (DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
                                DISPATCH_CODE_REQUEUE_FAILED, DispatchOutcome,
                                EvidenceImportGate)

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db import PipelineDB
    from lib.quality import (
        AlbumQualityEvidence,
        ImportResult,
        QualityRankConfig,
        SpectralAnalysisDetail,
    )
    from lib.sidecar_service import SidecarDB, SidecarWriteResult

logger = logging.getLogger("cratedigger")


def _requeue_import_job_to_preview(
    db: "PipelineDB",
    *,
    import_job_id: int | None,
    reason: str,
) -> "DispatchOutcome":
    """Shared requeue helper for the two outer evidence-required branches.

    Called from ``_dispatch_import_from_db_locked`` (force-import) and from
    ``lib.download_validation._process_beets_validation`` (automation) when
    ``ensure_candidate_evidence_for_action`` reports the candidate row is
    missing, stale, or incomplete.

    Lock context differs by caller. The force-import call site holds the
    per-request IMPORT advisory lock; the automation call site holds the
    RELEASE lock. Either way the evidence-check + state-flip pair sits
    inside whatever lock the caller already has, which is sufficient for
    importer-vs-importer atomicity (only one importer worker drains the
    queue serially) — concurrent preview-worker claims of a still-running
    job are prevented by the importer's own ``status='running'``
    invariant, not by this lock.

    If the requeue UPDATE itself raises (DB transient), we swallow and
    return ``DISPATCH_CODE_REQUEUE_FAILED`` — the job stays in
    ``running`` for ``requeue_running_import_jobs`` on next importer
    boot to recover.

    ``import_job_id=None`` covers the automation pre-import branch in
    ``lib/download.py`` for paths that did not enqueue an import_job
    (legacy or test seam). Returns a hard requeue-failed outcome in
    that case rather than crashing — there's no row to flip.
    """
    detail = f"Candidate quality evidence unavailable at import time: {reason}"
    if import_job_id is None:
        # No row to flip. Report as a requeue failure so the importer
        # leaves the job (if any) in running for startup recovery.
        return DispatchOutcome(
            success=False,
            message=detail + " (no import_job_id; cannot requeue)",
            code=DISPATCH_CODE_REQUEUE_FAILED,
        )
    try:
        updated = db.requeue_import_job_for_preview(import_job_id, reason=reason)
    except Exception as exc:  # noqa: BLE001 — swallow + log for retry
        logger.exception(
            "Failed to requeue import_job %s for preview", import_job_id
        )
        return DispatchOutcome(
            success=False,
            message=f"Requeue to preview failed: {type(exc).__name__}: {exc}",
            code=DISPATCH_CODE_REQUEUE_FAILED,
        )
    if updated is None:
        # Row was not in ``status='running'`` when the UPDATE fired — either
        # already requeued by a concurrent worker, terminal, or never existed.
        # Conflating this with a successful requeue would hide drift; report
        # as a requeue failure so startup recovery handles whatever state the
        # job is actually in.
        logger.warning(
            "Requeue for import_job %s matched zero rows; job may already be "
            "requeued or terminal", import_job_id
        )
        return DispatchOutcome(
            success=False,
            message=detail + " (requeue UPDATE matched zero rows)",
            code=DISPATCH_CODE_REQUEUE_FAILED,
        )
    return DispatchOutcome(
        success=False,
        message=detail + "; requeued for preview",
        code=DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    )


def _import_allowed_by_evidence_pipeline(result: dict[str, object]) -> bool:
    return bool(result.get("imported"))


def _current_evidence_analysis_failed(gate: EvidenceImportGate) -> bool:
    """Only a failed HAVE analysis aborts; a genuinely absent HAVE proceeds."""

    return gate.current_status == CURRENT_STATUS_FAILED


def _download_info_from_candidate_evidence(
    candidate: AlbumQualityEvidence,
    *,
    username: str | None,
) -> DownloadInfo:
    """Build force-import audit info without remeasuring the candidate."""

    measurement = candidate.measurement
    bitrate = (
        measurement.min_bitrate_kbps * 1000
        if measurement.min_bitrate_kbps is not None
        else None
    )
    return DownloadInfo(
        username=username,
        filetype=(
            candidate.storage_format
            or measurement.format
            or candidate.container
            or candidate.codec
        ),
        bitrate=bitrate,
        is_vbr=not measurement.is_cbr,
        download_spectral=SpectralMeasurement.from_parts(
            measurement.spectral_grade,
            measurement.spectral_bitrate_kbps,
        ),
        v0_probe=audit_v0_probe_from_metric(candidate.v0_metric),
    )


def _write_quality_evidence_action_file(
    *,
    candidate: AlbumQualityEvidence,
    current: AlbumQualityEvidence | None,
    decision: dict[str, object],
    target_format: str | None,
    verified_lossless_target: str,
    gate: EvidenceImportGate,
) -> str:
    """Write the action-time evidence payload consumed by import_one.py."""

    payload = QualityEvidenceActionPayload(
        candidate=candidate,
        current=current,
        decision=decision,
        decision_name=evidence_decision_name(decision),
        target_format=target_format,
        verified_lossless_target=verified_lossless_target or None,
        provenance=QualityEvidenceActionProvenance(
            candidate_status=gate.candidate_status,
            current_status=gate.current_status,
            snapshot_status=gate.snapshot_guard,
            fallback_reason=gate.candidate_reason,
        ),
    )
    handle = tempfile.NamedTemporaryFile(
        prefix="cratedigger-quality-evidence-action-",
        suffix=".json",
        delete=False,
    )
    try:
        handle.write(msgspec.json.encode(payload))
        return handle.name
    finally:
        handle.close()


def _remove_quality_evidence_action_file(path: str | None) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        logger.debug(
            "Failed to remove quality evidence action file %s",
            path,
            exc_info=True,
        )


def _load_evidence_import_gate(
    db: "PipelineDB",
    *,
    request_id: int,
    mb_release_id: str,
    path: str,
    quality_ranks: "QualityRankConfig | None",
    candidate_import_job_id: int | None,
    candidate_download_log_id: int | None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    attempt_existing_spectral: SpectralAnalysisDetail | None = None,
    attempt_have_audit_available: bool = False,
    beets_library_db_path: str | None = None,
    beets_library_root: str = "",
    current_evidence_loader: Callable[
        ..., CurrentEvidenceActionResult | None
    ] = load_current_evidence_for_action,
) -> EvidenceImportGate:
    """Load persisted evidence for import-time quality authority."""

    if candidate_import_job_id is None and candidate_download_log_id is None:
        return EvidenceImportGate()

    candidate_result = prevalidated_candidate_result
    if candidate_result is None:
        candidate_result = ensure_candidate_evidence_for_action(
            db,
            source_path=path,
            import_job_id=candidate_import_job_id,
            download_log_id=candidate_download_log_id,
        )
    if not candidate_result.available:
        return EvidenceImportGate(
            candidate=None,
            candidate_status=candidate_result.provenance.candidate_status,
            candidate_reason=candidate_result.provenance.fallback_reason,
            snapshot_guard=candidate_result.provenance.snapshot_guard,
        )

    current_result = current_evidence_loader(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        quality_ranks=quality_ranks,
        beets_library_db_path=beets_library_db_path,
        beets_library_root=beets_library_root,
    )
    if current_result is None:
        return EvidenceImportGate(
            current=None,
            candidate=candidate_result.evidence,
            candidate_status=candidate_result.provenance.candidate_status,
            candidate_reason=candidate_result.provenance.fallback_reason,
            current_status=CURRENT_STATUS_MISSING,
            current_reason="album not in beets",
            snapshot_guard=candidate_result.provenance.snapshot_guard,
        )

    fresh_have_failure: str | None = None
    if attempt_have_audit_available:
        if attempt_existing_spectral is None:
            fresh_have_failure = "attempt returned no installed HAVE spectral result"
        elif not attempt_existing_spectral.attempted:
            fresh_have_failure = "attempt did not run installed HAVE spectral analysis"
        elif attempt_existing_spectral.error is not None:
            fresh_have_failure = attempt_existing_spectral.error
        elif attempt_existing_spectral.grade in (None, "error"):
            fresh_have_failure = (
                "attempt did not produce a usable installed HAVE spectral grade"
            )
    if fresh_have_failure is not None:
        current = current_result.evidence
        return EvidenceImportGate(
            current=None,
            candidate=candidate_result.evidence,
            candidate_status=candidate_result.provenance.candidate_status,
            candidate_reason=candidate_result.provenance.fallback_reason,
            current_status=CURRENT_STATUS_FAILED,
            current_reason=fresh_have_failure,
            current_path=(
                current_result.provenance.installed_path
                or (current.source_path if current is not None else None)
            ),
            current_snapshot_guard=current_result.provenance.snapshot_guard,
            snapshot_guard=candidate_result.provenance.snapshot_guard,
        )

    return EvidenceImportGate(
        current=current_result.evidence if current_result.available else None,
        candidate=candidate_result.evidence,
        candidate_status=candidate_result.provenance.candidate_status,
        candidate_reason=candidate_result.provenance.fallback_reason,
        current_status=current_result.provenance.current_status,
        current_reason=current_result.provenance.fallback_reason,
        current_path=current_result.provenance.installed_path,
        current_snapshot_guard=current_result.provenance.snapshot_guard,
        snapshot_guard=candidate_result.provenance.snapshot_guard,
    )


def _refresh_current_evidence_after_import(
    db: "PipelineDB",
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: "QualityRankConfig | None",
    source_candidate: AlbumQualityEvidence | None = None,
    import_result: ImportResult | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str = "",
) -> EvidenceBuildResult:
    """Persist current evidence for the just-imported Beets album.

    When ``source_candidate`` is supplied (the normal post-U10 path), the new
    library-side evidence row is built by propagating the candidate's
    measurement payload — see
    :func:`lib.quality_evidence.propagate_candidate_evidence_to_current`
    for the lossless-source gate that governs which fields propagate.
    Bitrate/format always re-derive from ``album_info`` (dual-check
    against the candidate measurement).

    When ``source_candidate`` is ``None`` (rare — legacy callers, an evidence
    record that vanished, or non-post-import callers reusing this helper),
    fall back to the pre-U10 ``backfill_current_evidence_from_album_info``
    path. That path rebuilds evidence from beets fields plus a carried-
    forward ``verified_lossless_proof`` and is preserved for non-post-import
    callers (e.g., wrong-match triage backfilling library evidence for
    pre-refactor albums).

    ``beets_library_db_path`` selects an isolated real Beets database for
    world-model runs. Normal production callers omit it and retain the
    module-configured read-only library path.
    """

    from lib.beets_db import BeetsDB
    from lib.quality import QualityRankConfig

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    # ``beets_library_root`` must be set: ``BeetsDB.get_album_info`` returns a
    # path *relative* to the library root when constructed without one, which
    # breaks ``snapshot_audio_files`` (host-side filesystem ops) — see the
    # BeetsDB docstring. Both the U10 propagation path and the legacy
    # ``backfill_current_evidence_from_album_info`` path depend on an
    # absolute ``album_info.album_path`` to read the just-imported files.
    if beets_library_db_path is None:
        beets_handle = BeetsDB(library_root=beets_library_root)
    else:
        beets_handle = BeetsDB(
            beets_library_db_path,
            library_root=beets_library_root,
        )
    with beets_handle as beets:
        album_info = beets.get_album_info(mb_release_id, cfg)
    if album_info is None:
        return EvidenceBuildResult(None, "empty_current", "album not in beets")

    if source_candidate is not None:
        result = propagate_candidate_evidence_to_current(
            db,
            request_id=request_id,
            candidate_evidence=source_candidate,
            album_info=album_info,
        )
        return _exact_linked_refresh_result(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            result=result,
        )

    # Legacy fallback: no candidate evidence on hand. Rebuild from beets +
    # carry-forward verified_lossless_proof from the import_result, matching
    # pre-U10 behaviour exactly.
    decision = import_result.decision if import_result is not None else None
    verified_lossless_proof = None
    if decision != "preflight_existing":
        verified_lossless_proof = (
            import_result.verified_lossless_proof
            if import_result is not None
            else None
        )
    result = backfill_current_evidence_from_album_info(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        album_info=album_info,
        verified_lossless_proof=verified_lossless_proof,
        preserve_existing_verified_lossless_proof=(
            import_result is None or decision == "preflight_existing"
        ),
    )
    return _exact_linked_refresh_result(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        result=result,
    )


def _exact_linked_refresh_result(
    db: "PipelineDB",
    *,
    request_id: int,
    mb_release_id: str,
    result: EvidenceBuildResult,
) -> EvidenceBuildResult:
    """Resolve a ready refresh to the exact row linked by this write."""

    if result.status != "ready" or result.evidence is None:
        return result
    try:
        linked_id = db.get_request_current_evidence_id(request_id)
        linked = (
            db.load_album_quality_evidence_by_id(linked_id)
            if linked_id is not None
            else None
        )
    except Exception as exc:
        return EvidenceBuildResult(
            None,
            "failed",
            f"{type(exc).__name__}: {exc}",
        )
    if (
        linked is None
        or linked.id is None
        or linked.mb_release_id != mb_release_id
        or linked.snapshot_fingerprint != result.evidence.snapshot_fingerprint
    ):
        return EvidenceBuildResult(
            None,
            "stale_request",
            "post-import evidence is not the exact linked current snapshot",
        )
    return EvidenceBuildResult(linked, "ready")


def _write_album_sidecar_after_import(
    db: "SidecarDB",
    *,
    request_id: int,
    mb_release_id: str,
    cfg: "CratediggerConfig | None",
    beets_factory: "Callable[..., Any] | None" = None,
) -> "SidecarWriteResult":
    """Write the verified-lossless ``cratedigger.json`` sidecar after import.

    Reads the request's freshly-persisted current evidence (set by
    ``_refresh_current_evidence_after_import``) and delegates to the shared
    ``write_sidecar_for_request`` service — the same entry point the one-shot
    backfill uses, so there is no parallel sidecar-writing code path. The
    sidecar is derived state; re-running rebuilds it idempotently.

    ``beets_factory`` is a kwarg-DI seam for tests; production constructs a
    short-lived ``BeetsDB`` (mirroring ``_refresh_current_evidence_after_import``).
    """
    from lib.beets_db import BeetsDB
    from lib.sidecar_service import write_sidecar_for_request

    factory = beets_factory if beets_factory is not None else BeetsDB
    root = cfg.beets_directory if cfg is not None else ""
    quality_ranks = cfg.quality_ranks if cfg is not None else None
    with factory(library_root=root) as beets:
        return write_sidecar_for_request(
            db,
            beets,
            request_id,
            mb_release_id=mb_release_id,
            quality_ranks=quality_ranks,
        )
