"""Action-time quality-evidence fail-closed gate + post-import refresh.

The preview->importer contract's importer-side seam: load persisted
candidate/current ``AlbumQualityEvidence`` for a mutating import, write the
action-file consumed by ``import_one.py``, requeue back to preview when
evidence is unavailable, and refresh current evidence (+ sidecar) after a
successful import. ``load_current_evidence_for_action`` is looked up here.

This gate is deliberately not described as atomic or as a TOCTOU proof.
Pipeline PostgreSQL, Beets SQLite, and the candidate/library filesystem
subjects cannot share one transaction. The gate independently reauthorizes
both evidence sides against the world observed immediately before dispatch
and fails closed before Beets launch. A later world failure belongs to the
owner-audit-retry lifecycle, not to a synthetic cross-system transaction.
"""
# ruff: noqa: UP037 - quoted Any annotation is part of the typing ratchet

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lib.dispatch.types import (
    DISPATCH_CODE_REQUEUE_EXHAUSTED,
    DISPATCH_CODE_REQUEUE_FAILED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    DispatchOutcome,
    EvidenceImportGate,
)
from lib.evidence_action_file import (
    remove_quality_evidence_action_file,
    write_quality_evidence_action_file,
)
from lib.import_evidence import (
    CURRENT_STATUS_FAILED,
    CURRENT_STATUS_MISSING,
    CandidateEvidenceActionResult,
    CurrentEvidenceActionResult,
    ensure_candidate_evidence_for_action,
    load_current_evidence_for_action,
)
from lib.import_queue import (
    import_preview_requeue_delay,
    import_preview_requeue_exhausted,
)
from lib.quality import (
    DownloadInfo,
    QualityEvidenceActionPayload,
    QualityEvidenceActionProvenance,
    SpectralMeasurement,
    evidence_decision_name,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    audit_v0_probe_from_metric,
    backfill_current_evidence_from_album_info,
    propagate_candidate_evidence_to_current,
)

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.import_execution import ExecutionLeaseSnapshot
    from lib.pipeline_db import PipelineDB
    from lib.quality import (
        AlbumQualityEvidence,
        ImportResult,
        QualityRankConfig,
        SpectralAnalysisDetail,
    )
    from lib.quality_evidence import QualityEvidenceDB
    from lib.sidecar_service import SidecarDB, SidecarWriteResult

logger = logging.getLogger("cratedigger")


def _requeue_import_job_to_preview(
    db: PipelineDB,
    *,
    import_job_id: int | None,
    reason: str,
    expected_execution_lease: ExecutionLeaseSnapshot | None = None,
) -> DispatchOutcome:
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
    ``running`` for conservative startup recovery on next importer
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
    current = db.get_import_job(import_job_id)
    if current is not None and current.created_at is not None:
        now = datetime.now(UTC)
        age = now - current.created_at
        if import_preview_requeue_exhausted(current.created_at, now):
            return DispatchOutcome(
                success=False,
                message=(
                    f"{detail}; preview/import requeue budget exhausted "
                    f"(attempts={current.attempts}, preview_attempts="
                    f"{current.preview_attempts}, age_seconds="
                    f"{int(age.total_seconds())})"
                ),
                code=DISPATCH_CODE_REQUEUE_EXHAUSTED,
            )
    try:
        updated = db.requeue_import_job_for_preview(
            import_job_id,
            reason=reason,
            expected_execution_lease=expected_execution_lease,
        )
    except Exception as exc:
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
        message=(
            f"{detail}; requeued for preview after "
            f"{int(import_preview_requeue_delay(updated.attempts).total_seconds())}s"
        ),
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
    contributor_usernames: tuple[str, ...] = (),
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
        contributor_usernames=contributor_usernames,
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
    """Write the action-time evidence payload consumed by import_one.py.

    Builds the importer's own payload shape, then delegates the actual
    write to ``lib.evidence_action_file`` — the ONE tempfile-write
    implementation shared with the preview worker (issue #859; see that
    module's docstring for why a second writer is forbidden).
    """

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
    return write_quality_evidence_action_file(payload)


def _remove_quality_evidence_action_file(path: str | None) -> None:
    """Thin re-export so ``lib.dispatch.core`` has one import site for the
    load/gate/write/remove action-file surface; the real implementation
    lives in ``lib.evidence_action_file`` (issue #859)."""
    remove_quality_evidence_action_file(path)


def _load_evidence_import_gate(
    db: PipelineDB,
    *,
    request_id: int,
    mb_release_id: str,
    path: str,
    quality_ranks: QualityRankConfig | None,
    candidate_import_job_id: int | None,
    candidate_download_log_id: int | None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    attempt_existing_spectral: SpectralAnalysisDetail | None = None,
    attempt_have_audit_available: bool = False,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    current_evidence_loader: Callable[
        ..., CurrentEvidenceActionResult | None
    ] = load_current_evidence_for_action,
) -> EvidenceImportGate:
    """Reauthorize both evidence sides at the bounded import-time check.

    A successful return authorizes the decision from the world observed here;
    it does not lock either database or filesystem against subsequent change.
    """

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
        return EvidenceImportGate(
            current=None,
            candidate=candidate_result.evidence,
            candidate_status=candidate_result.provenance.candidate_status,
            candidate_reason=candidate_result.provenance.fallback_reason,
            current_status=CURRENT_STATUS_FAILED,
            current_reason=fresh_have_failure,
            current_path=current_result.provenance.installed_path,
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
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig | None,
    source_candidate: AlbumQualityEvidence | None = None,
    import_result: ImportResult | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    candidate_program_complete: bool | None = None,
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

    ``candidate_program_complete`` is the caller's proof that this import's
    candidate covered every declared release track (issue #1241). ``False``
    — the force-import lane, which imports despite an invalid beets verdict
    and can therefore install an ``extra_tracks`` folder — withholds the
    verified-lossless proof from the new current row so an album that is
    missing a declared component never enters the locked state. ``None``
    (unknown) and ``True`` carry the proof exactly as before.
    """

    from lib.beets_db import (
        CurrentBeetsAmbiguous,
        CurrentBeetsMissing,
        album_info_from_current,
        exact_release_identity_matches,
        open_beets_db,
        release_identity_for_lookup,
    )
    from lib.quality import QualityRankConfig

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    # ``beets_library_root`` must be set: ``BeetsDB.get_album_info`` returns a
    # path *relative* to the library root when constructed without one, which
    # breaks ``snapshot_audio_files`` (host-side filesystem ops) — see the
    # BeetsDB docstring. Both the U10 propagation path and the legacy
    # ``backfill_current_evidence_from_album_info`` path depend on an
    # absolute ``album_info.album_path`` to read the just-imported files.
    beets_handle = open_beets_db(
        db_path=beets_library_db_path,
        library_root=beets_library_root,
    )
    identity = release_identity_for_lookup(mb_release_id)
    if identity is None:
        return EvidenceBuildResult(
            None,
            "failed",
            f"invalid exact release identity {mb_release_id!r}",
        )
    if source_candidate is not None and not exact_release_identity_matches(
        mb_release_id,
        source_candidate.mb_release_id,
    ):
        return EvidenceBuildResult(
            None,
            "identity_mismatch",
            "candidate evidence exact release identity does not match import",
        )
    with beets_handle as beets:
        current = beets.resolve_current_release(identity)
    if isinstance(current, CurrentBeetsMissing):
        return EvidenceBuildResult(None, "empty_current", "album not in beets")
    if isinstance(current, CurrentBeetsAmbiguous):
        return EvidenceBuildResult(
            None,
            "ambiguous_current",
            "ambiguous current Beets authority: "
            f"{current.reason}; album_ids={current.album_ids}",
        )
    album_info = album_info_from_current(current, cfg)
    if album_info is None:
        return EvidenceBuildResult(
            None,
            "failed",
            "unique current Beets album has no usable bitrate metadata",
            current_album_path=current.album_path,
        )

    if source_candidate is not None:
        result = propagate_candidate_evidence_to_current(
            db,
            request_id=request_id,
            candidate_evidence=source_candidate,
            album_info=album_info,
            candidate_program_complete=candidate_program_complete,
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
    db: QualityEvidenceDB,
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
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
    db: SidecarDB,
    *,
    request_id: int,
    mb_release_id: str,
    cfg: CratediggerConfig | None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    beets_factory: "Callable[..., Any] | None" = None,
) -> SidecarWriteResult:
    """Write the verified-lossless ``cratedigger.json`` sidecar after import.

    Reads the request's freshly-persisted current evidence (set by
    ``_refresh_current_evidence_after_import``) and delegates to the shared
    ``write_sidecar_for_request`` service — the same entry point the one-shot
    backfill uses, so there is no parallel sidecar-writing code path. The
    sidecar is derived state; re-running rebuilds it idempotently.

    ``beets_factory`` is a kwarg-DI seam for tests. Production opens the exact
    dispatch DB/root pair through ``open_beets_db``.
    """
    from lib.beets_db import open_beets_db
    from lib.sidecar_service import write_sidecar_for_request

    quality_ranks = cfg.quality_ranks if cfg is not None else None
    from lib.beets_db import validate_beets_storage_pair

    validate_beets_storage_pair(
        db_path=beets_library_db_path,
        library_root=beets_library_root,
    )
    if beets_factory is not None:
        if beets_library_db_path is None:
            beets_handle = beets_factory(library_root="")
        else:
            beets_handle = beets_factory(
                beets_library_db_path,
                library_root=beets_library_root,
            )
    elif beets_library_db_path is None:
        beets_handle = open_beets_db(cfg)
    else:
        beets_handle = open_beets_db(
            db_path=beets_library_db_path,
            library_root=beets_library_root,
        )
    with beets_handle as beets:
        return write_sidecar_for_request(
            db,
            beets,
            request_id,
            mb_release_id=mb_release_id,
            quality_ranks=quality_ranks,
        )
