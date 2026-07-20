"""Evidence-only cleanup service for Wrong Matches."""

from __future__ import annotations

import logging
import os
from contextlib import AbstractContextManager
from typing import Any, Iterable, Protocol, runtime_checkable

import msgspec

from lib.import_evidence import CURRENT_STATUS_LOADED, load_current_evidence_for_action
from lib.import_queue import ImportJob
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
    wrong_match_cleanup_lock_key,
)
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
    classify_full_pipeline_decision,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
    narrow_override_on_lossless_source_lock,
    AudioQualityMeasurement,
    QualityComparisonBasis,
    V0ProbeEvidence,
    comparison_basis_from_decision,
)
from lib.quality_evidence import (
    QualityEvidenceDB,
    load_candidate_evidence_for_source,
    audit_v0_probe_from_metric,
)
from lib.util import resolve_failed_path
from lib.validation_envelope import (
    WrongMatchTriageAudit,
    decode_validation_envelope,
)
from lib.wrong_matches import (
    WrongMatchSourceDB,
    cleanup_wrong_match_source,
    validation_failed_path,
)

logger = logging.getLogger("cratedigger")


@runtime_checkable
class WrongMatchCleanupDB(WrongMatchSourceDB, QualityEvidenceDB, Protocol):
    """The PipelineDB surface this service uses (#409).

    Extends ``WrongMatchSourceDB`` (the handle is forwarded into
    ``cleanup_wrong_match_source``) and ``QualityEvidenceDB`` (forwarded
    into the evidence loaders and ``preview_fn``). ``PipelineDB`` and
    ``FakePipelineDB`` satisfy it structurally — pyright enforces signature
    parity at every call site, and the issubclass parity tests in
    ``tests/test_wrong_match_cleanup_service.py`` guard method presence at
    runtime.
    """

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def update_request_fields(self, request_id: int, **extra: Any) -> bool: ...

    def list_active_import_jobs_for_wrong_match(
        self,
        *,
        download_log_id: int,
        request_id: int | None,
        failed_paths: Iterable[str],
        source_dirs: Iterable[str],
        ignore_import_job_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]: ...

    def record_wrong_match_triage(
        self, log_id: int, triage_result: WrongMatchTriageAudit,
    ) -> bool: ...


OUTCOME_DELETED = "deleted"
OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT = "deleted_verified_lossless_parent"
OUTCOME_KEPT_WOULD_IMPORT = "kept_would_import"
OUTCOME_KEPT_UNCERTAIN = "kept_uncertain"
OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING = "skipped_candidate_evidence_missing"
OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE = "skipped_candidate_evidence_stale"
OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING = "skipped_current_evidence_missing"
OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE = "skipped_current_evidence_stale"
OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED = "skipped_current_evidence_failed"
OUTCOME_SKIPPED_ACTIVE_JOB = "skipped_active_job"
OUTCOME_SKIPPED_INVALID_ROW = "skipped_invalid_row"
OUTCOME_SKIPPED_MISSING_PATH = "skipped_missing_path"
OUTCOME_SKIPPED_OPERATIONAL = "skipped_operational"
OUTCOME_DELETE_FAILED = "delete_failed"

OUTCOME_KEYS: tuple[str, ...] = (
    OUTCOME_DELETED,
    OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT,
    OUTCOME_KEPT_WOULD_IMPORT,
    OUTCOME_KEPT_UNCERTAIN,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED,
    OUTCOME_SKIPPED_ACTIVE_JOB,
    OUTCOME_SKIPPED_INVALID_ROW,
    OUTCOME_SKIPPED_MISSING_PATH,
    OUTCOME_SKIPPED_OPERATIONAL,
    OUTCOME_DELETE_FAILED,
)

FINAL_AUDIT_OUTCOMES: frozenset[str] = frozenset({
    OUTCOME_DELETED,
    OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT,
    OUTCOME_KEPT_WOULD_IMPORT,
    OUTCOME_KEPT_UNCERTAIN,
    OUTCOME_DELETE_FAILED,
})

# Issue #271: stuck-skip outcomes are persisted to the triage audit too, so
# a row that cleanup cannot classify shows WHY in the UI instead of sitting
# in the queue with no indication. Transient skips (active job, lock
# contention, invalid row, operational crash) are deliberately excluded —
# they resolve on their own and would overwrite a meaningful audit with
# noise.
STUCK_SKIP_AUDIT_OUTCOMES: frozenset[str] = frozenset({
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED,
    OUTCOME_SKIPPED_MISSING_PATH,
})

AUDITED_OUTCOMES: frozenset[str] = FINAL_AUDIT_OUTCOMES | STUCK_SKIP_AUDIT_OUTCOMES


class WrongMatchCleanupOutcome(msgspec.Struct, frozen=True):
    download_log_id: int
    outcome: str
    success: bool = False
    request_id: int | None = None
    source_path: str | None = None
    reason: str | None = None
    verdict: str | None = None
    preview_decision: str | None = None
    cleanup_eligible: bool = False
    cleared_rows: int = 0
    deleted_path: str | None = None
    path_missing: bool = False
    error: str | None = None
    decision: dict[str, Any] = msgspec.field(default_factory=dict)
    candidate_measurement: AudioQualityMeasurement | None = None
    current_measurement: AudioQualityMeasurement | None = None
    candidate_v0_probe: V0ProbeEvidence | None = None
    current_v0_probe: V0ProbeEvidence | None = None
    comparison_basis: QualityComparisonBasis | None = None

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


class WrongMatchCleanupSummary(msgspec.Struct, frozen=True):
    processed: int = 0
    deleted: int = 0
    deleted_verified_lossless_parent: int = 0
    kept_would_import: int = 0
    kept_uncertain: int = 0
    skipped_candidate_evidence_missing: int = 0
    skipped_candidate_evidence_stale: int = 0
    skipped_current_evidence_missing: int = 0
    skipped_current_evidence_stale: int = 0
    skipped_current_evidence_failed: int = 0
    skipped_active_job: int = 0
    skipped_invalid_row: int = 0
    skipped_missing_path: int = 0
    skipped_operational: int = 0
    delete_failed: int = 0
    results: tuple[WrongMatchCleanupOutcome, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


class _LoadedEvidence(msgspec.Struct, frozen=True):
    evidence: AlbumQualityEvidence | None
    outcome: str | None = None
    reason: str | None = None


def cleanup_all_wrong_matches(
    db: WrongMatchCleanupDB,
    *,
    confirm_all_wrong_matches: bool = False,
    ignore_import_job_id: int | None = None,
    cfg: Any = None,
    preview_fn: Any = None,
) -> WrongMatchCleanupSummary:
    """Run cleanup over the full current Wrong Matches queue."""
    if confirm_all_wrong_matches is not True:
        raise ValueError("confirm_all_wrong_matches must be true")

    if cfg is None:
        cfg = _runtime_config()

    results: list[WrongMatchCleanupOutcome] = []
    for row in db.get_wrong_matches():
        # ``download_log_id`` is a required, non-nullable ``download_log.id``
        # column (WrongMatchCandidateRow), so the row type already proves
        # this is an ``int`` — only the bool-subtype guard still needs a
        # runtime check.
        download_log_id = row["download_log_id"]
        if isinstance(download_log_id, bool):
            results.append(WrongMatchCleanupOutcome(
                download_log_id=0,
                outcome=OUTCOME_SKIPPED_INVALID_ROW,
                reason="invalid download_log_id",
            ))
            continue
        results.append(cleanup_wrong_match(
            db,
            download_log_id,
            ignore_import_job_id=ignore_import_job_id,
            cfg=cfg,
            preview_fn=preview_fn,
        ))
    return _summary(results)


def cleanup_wrong_match(
    db: WrongMatchCleanupDB,
    download_log_id: int,
    *,
    failed_path_hint: str | None = None,
    ignore_import_job_id: int | None = None,
    cfg: Any = None,
    preview_fn: Any = None,
) -> WrongMatchCleanupOutcome:
    """Evaluate and possibly delete one Wrong Matches source row.

    ``preview_fn`` is the DI seam for the stale-evidence refresh (issue
    #271); production resolves it to
    ``measure_and_persist_candidate_evidence``.
    """
    try:
        result = _cleanup_wrong_match(
            db,
            download_log_id,
            failed_path_hint=failed_path_hint,
            ignore_import_job_id=ignore_import_job_id,
            cfg=cfg,
            preview_fn=preview_fn,
        )
        _persist_cleanup_audit(db, result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "wrong_match_cleanup.operational_failure download_log_id=%s",
            download_log_id,
        )
        return WrongMatchCleanupOutcome(
            download_log_id=download_log_id,
            outcome=OUTCOME_SKIPPED_OPERATIONAL,
            reason=f"{type(exc).__name__}: {exc}",
            error=f"{type(exc).__name__}: {exc}",
        )


def _cleanup_wrong_match(
    db: WrongMatchCleanupDB,
    download_log_id: int,
    *,
    failed_path_hint: str | None,
    ignore_import_job_id: int | None,
    cfg: Any,
    preview_fn: Any = None,
) -> WrongMatchCleanupOutcome:
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_INVALID_ROW,
            reason="download_log_missing",
        )

    request_id_raw = entry.get("request_id")
    request_id = request_id_raw if type(request_id_raw) is int else None
    if request_id is None:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_INVALID_ROW,
            reason="request_id_missing",
        )

    request = db.get_request(request_id)
    if not isinstance(request, dict):
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_INVALID_ROW,
            request_id=request_id,
            reason="request_missing",
        )

    raw_path = validation_failed_path(entry.get("validation_result"))
    if not raw_path:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_INVALID_ROW,
            request_id=request_id,
            reason="failed_path_missing",
        )

    candidates = _path_candidates(failed_path_hint, raw_path)
    resolved_path = _resolve_first_existing(candidates)
    if resolved_path is None:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_MISSING_PATH,
            request_id=request_id,
            source_path=failed_path_hint or raw_path,
            reason="failed_path_missing_on_disk",
        )
    candidates = _path_candidates(*candidates, resolved_path)
    source_dirs = tuple(
        decode_validation_envelope(entry.get("validation_result")).source_dirs
    )

    active_jobs = _matching_active_jobs(
        db,
        download_log_id=download_log_id,
        request_id=request_id,
        failed_paths=candidates,
        source_dirs=source_dirs,
        ignore_import_job_id=ignore_import_job_id,
    )
    if active_jobs:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_ACTIVE_JOB,
            request_id=request_id,
            source_path=resolved_path,
            reason="active_import_job",
        )

    candidate = _load_candidate_evidence(db, download_log_id, resolved_path)
    if (
        candidate.evidence is None
        and candidate.outcome == OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE
    ):
        candidate = _refresh_stale_candidate_evidence(
            db,
            request_id=request_id,
            download_log_id=download_log_id,
            source_path=resolved_path,
            preview_fn=preview_fn,
        )
    if candidate.evidence is None:
        return _result(
            download_log_id,
            candidate.outcome or OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
            request_id=request_id,
            source_path=resolved_path,
            reason=candidate.reason,
        )

    runtime_cfg = cfg if cfg is not None else _runtime_config()
    mb_release_id_raw = request.get("mb_release_id")
    mb_release_id = str(mb_release_id_raw) if mb_release_id_raw else None
    beets_library_root = getattr(runtime_cfg, "beets_directory", "") or ""
    current_evidence: AlbumQualityEvidence | None = None
    current_evidence_status: str | None = None
    if mb_release_id is not None:
        current_result = load_current_evidence_for_action(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=getattr(runtime_cfg, "quality_ranks", None),
            beets_library_root=beets_library_root,
        )
        if current_result is not None:
            if current_result.provenance.fail_closed:
                return _result(
                    download_log_id,
                    OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED,
                    request_id=request_id,
                    source_path=resolved_path,
                    reason=current_result.provenance.fallback_reason
                    or "current_evidence_unavailable",
                )
            if not current_result.available:
                return _result(
                    download_log_id,
                    OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
                    request_id=request_id,
                    source_path=resolved_path,
                    reason=current_result.provenance.fallback_reason
                    or "current_evidence_unavailable",
                )
            current_evidence = current_result.evidence
            current_evidence_status = current_result.provenance.current_status

    # Cleanup-only policy (NOT a quality decision): when the in-Beets parent is
    # verified-lossless AND the evidence was loaded directly from disk (not
    # backfilled, which can preserve a stale verified_lossless_proof against
    # changed audio), any candidate in Wrong Matches against this MBID is
    # guaranteed to lose the upgrade gate. Short-circuit deletion. The reducer
    # is deliberately not called -- see TestVerifiedLosslessShortCircuit.
    if (
        current_evidence is not None
        and current_evidence.verified_lossless_proof is not None
        and current_evidence_status == CURRENT_STATUS_LOADED
    ):
        return _perform_cleanup_deletion(
            db,
            download_log_id=download_log_id,
            request_id=request_id,
            resolved_path=resolved_path,
            candidates=tuple(candidates),
            source_dirs=tuple(source_dirs),
            ignore_import_job_id=ignore_import_job_id,
            success_outcome=OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT,
            reason="parent_album_verified_lossless",
            verdict="confident_reject",
            preview_decision="verified_lossless_parent",
            cleanup_eligible=True,
            decision=None,
            candidate_evidence=candidate.evidence,
            current_evidence=current_evidence,
        )

    decision = full_pipeline_decision_from_evidence(
        candidate.evidence,
        current_evidence,
        facts=AlbumQualityEvidenceDecisionFacts(
            verified_lossless_target=getattr(
                runtime_cfg,
                "verified_lossless_target",
                None,
            ),
            target_format=request.get("target_format"),
        ),
        cfg=getattr(runtime_cfg, "quality_ranks", None),
    )
    verdict, cleanup_eligible, reason = classify_full_pipeline_decision(decision)
    preview_decision = evidence_decision_name(decision)

    # Decision 21: the proof lock fires in force mode too, so a backfilled
    # (non-disk-loaded) parent proof now reaches this reducer as
    # verified_lossless_locked — a cleanup-eligible confident reject.
    # Deleting on it is deliberate and consistent with the disk-loaded
    # short-circuit above: carried proof remains decisive through the
    # audited reducer path (out-of-band audio mutation is outside the
    # state model per decision 6).

    if verdict == "would_import":
        return _result(
            download_log_id,
            OUTCOME_KEPT_WOULD_IMPORT,
            request_id=request_id,
            source_path=resolved_path,
            reason=reason,
            verdict=verdict,
            preview_decision=preview_decision,
            cleanup_eligible=cleanup_eligible,
            decision=decision,
            candidate_evidence=candidate.evidence,
            current_evidence=current_evidence,
        )
    if verdict != "confident_reject" or not cleanup_eligible:
        return _result(
            download_log_id,
            OUTCOME_KEPT_UNCERTAIN,
            request_id=request_id,
            source_path=resolved_path,
            reason=reason,
            verdict=verdict,
            preview_decision=preview_decision,
            cleanup_eligible=cleanup_eligible,
            decision=decision,
            candidate_evidence=candidate.evidence,
            current_evidence=current_evidence,
        )

    return _perform_cleanup_deletion(
        db,
        download_log_id=download_log_id,
        request_id=request_id,
        resolved_path=resolved_path,
        candidates=tuple(candidates),
        source_dirs=tuple(source_dirs),
        ignore_import_job_id=ignore_import_job_id,
        success_outcome=OUTCOME_DELETED,
        reason=reason,
        verdict=verdict,
        preview_decision=preview_decision,
        cleanup_eligible=cleanup_eligible,
        decision=decision,
        candidate_evidence=candidate.evidence,
        current_evidence=current_evidence,
    )


def _perform_cleanup_deletion(
    db: WrongMatchCleanupDB,
    *,
    download_log_id: int,
    request_id: int,
    resolved_path: str,
    candidates: tuple[str, ...],
    source_dirs: tuple[str, ...],
    ignore_import_job_id: int | None,
    success_outcome: str,
    reason: str | None,
    verdict: str | None,
    preview_decision: str | None,
    cleanup_eligible: bool,
    decision: dict[str, Any] | None,
    candidate_evidence: AlbumQualityEvidence,
    current_evidence: AlbumQualityEvidence | None,
) -> WrongMatchCleanupOutcome:
    """Acquire WMCL lock, re-check active jobs, delete the source, translate the result."""
    lock_key = wrong_match_cleanup_lock_key(
        request_id,
        download_log_id,
        resolved_path,
    )
    # See docs/advisory-locks.md: WMCL serialises final source deletion.
    with db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
        lock_key,
    ) as acquired:
        if not acquired:
            return _result(
                download_log_id,
                OUTCOME_SKIPPED_ACTIVE_JOB,
                request_id=request_id,
                source_path=resolved_path,
                reason="cleanup_lock_unavailable",
                verdict=verdict,
                preview_decision=preview_decision,
                cleanup_eligible=cleanup_eligible,
                decision=decision,
                candidate_evidence=candidate_evidence,
                current_evidence=current_evidence,
            )

        active_jobs = _matching_active_jobs(
            db,
            download_log_id=download_log_id,
            request_id=request_id,
            failed_paths=candidates,
            source_dirs=source_dirs,
            ignore_import_job_id=ignore_import_job_id,
        )
        if active_jobs:
            return _result(
                download_log_id,
                OUTCOME_SKIPPED_ACTIVE_JOB,
                request_id=request_id,
                source_path=resolved_path,
                reason="active_import_job",
                verdict=verdict,
                preview_decision=preview_decision,
                cleanup_eligible=cleanup_eligible,
                decision=decision,
                candidate_evidence=candidate_evidence,
                current_evidence=current_evidence,
            )

        cleanup = cleanup_wrong_match_source(
            db,
            download_log_id,
            failed_path_hint=resolved_path,
            clear_missing=False,
        )

    if not cleanup.success or cleanup.error:
        return _result(
            download_log_id,
            OUTCOME_DELETE_FAILED,
            request_id=request_id,
            source_path=resolved_path,
            reason=cleanup.error or "delete_failed",
            verdict=verdict,
            preview_decision=preview_decision,
            cleanup_eligible=cleanup_eligible,
            decision=decision,
            error=cleanup.error,
            candidate_evidence=candidate_evidence,
            current_evidence=current_evidence,
        )
    if cleanup.path_missing:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_MISSING_PATH,
            request_id=request_id,
            source_path=resolved_path,
            reason="failed_path_missing_on_delete",
            verdict=verdict,
            preview_decision=preview_decision,
            cleanup_eligible=cleanup_eligible,
            decision=decision,
            cleared_rows=cleanup.cleared_rows,
            path_missing=True,
            candidate_evidence=candidate_evidence,
            current_evidence=current_evidence,
        )

    # R8 / AE2: when the cleanup fired because of `lossless_source_locked`,
    # narrow the request's search_filetype_override to "lossless" so the
    # next search cycle stops asking Soulseek for lossy candidates that
    # will only hit the lock again. The narrowing is no-op when the
    # override is already "lossless" (helper returns None).
    if preview_decision == "lossless_source_locked":
        try:
            request_row = db.get_request(request_id)
            current_override = (
                request_row.get("search_filetype_override")
                if request_row else None
            )
            narrowed = narrow_override_on_lossless_source_lock(current_override)
            if narrowed is not None:
                applied = db.update_request_fields(
                    request_id,
                    search_filetype_override=narrowed,
                    expected_status=(
                        str(request_row["status"])
                        if request_row is not None else None
                    ),
                )
                if not applied:
                    return _result(
                        download_log_id,
                        OUTCOME_SKIPPED_OPERATIONAL,
                        request_id=request_id,
                        source_path=resolved_path,
                        reason="request_changed_during_cleanup",
                        verdict=verdict,
                        preview_decision=preview_decision,
                        cleanup_eligible=cleanup_eligible,
                        decision=decision,
                        cleared_rows=cleanup.cleared_rows,
                        deleted_path=cleanup.deleted_path,
                        candidate_evidence=candidate_evidence,
                        current_evidence=current_evidence,
                    )
                logger.info(
                    "wrong_match_cleanup: narrowed search_filetype_override"
                    " from %r to %r after lossless_source_locked"
                    " (request_id=%s)",
                    current_override, narrowed, request_id,
                )
        except Exception:
            logger.exception(
                "wrong_match_cleanup: failed to narrow search_filetype_override"
                " after lossless_source_locked (request_id=%s)",
                request_id,
            )

    return _result(
        download_log_id,
        success_outcome,
        success=True,
        request_id=request_id,
        source_path=resolved_path,
        reason=reason,
        verdict=verdict,
        preview_decision=preview_decision,
        cleanup_eligible=cleanup_eligible,
        decision=decision,
        cleared_rows=cleanup.cleared_rows,
        deleted_path=cleanup.deleted_path,
        candidate_evidence=candidate_evidence,
        current_evidence=current_evidence,
    )


def _load_candidate_evidence(
    db: WrongMatchCleanupDB,
    download_log_id: int,
    source_path: str,
) -> _LoadedEvidence:
    result = load_candidate_evidence_for_source(
        db,
        source_path=source_path,
        download_log_id=download_log_id,
    )
    if result.evidence is not None:
        return _LoadedEvidence(result.evidence)
    if result.status == "stale":
        return _LoadedEvidence(
            None,
            OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
            result.reason,
        )
    return _LoadedEvidence(
        None,
        OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
        result.reason,
    )


def _refresh_stale_candidate_evidence(
    db: WrongMatchCleanupDB,
    *,
    request_id: int,
    download_log_id: int,
    source_path: str,
    preview_fn: Any,
) -> _LoadedEvidence:
    """Re-measure a stale candidate and reload its evidence (issue #271).

    Candidate evidence goes stale when slskd races the original capture —
    a file that was still 0 bytes at measurement time completes later, or
    the failed_imports move truncates a file after measurement. Either way
    the snapshot no longer describes the disk, so cleanup could neither
    classify nor surface the row. Delegating to
    ``measure_and_persist_candidate_evidence`` (the one existing
    measure-and-persist surface) rebuilds evidence from current disk
    truth; the reload then goes through the same freshness check as any
    other candidate. One attempt only — a source that is still churning
    stays a stale skip until the next sweep.
    """
    if preview_fn is None:
        from lib.import_preview import measure_and_persist_candidate_evidence

        preview_fn = measure_and_persist_candidate_evidence
    try:
        preview = preview_fn(
            db,
            request_id=request_id,
            path=source_path,
            download_log_id=download_log_id,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "wrong_match_cleanup.evidence_refresh_crashed download_log_id=%s",
            download_log_id,
        )
        return _LoadedEvidence(
            None,
            OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
            f"evidence_refresh_crashed: {type(exc).__name__}: {exc}",
        )

    from lib.import_preview import PREVIEW_VERDICT_EVIDENCE_READY

    verdict = getattr(preview, "verdict", None)
    if verdict != PREVIEW_VERDICT_EVIDENCE_READY:
        detail = (
            getattr(preview, "reason", None)
            or getattr(preview, "detail", None)
            or verdict
            or "no_preview_result"
        )
        return _LoadedEvidence(
            None,
            OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
            f"evidence_refresh_failed: {detail}",
        )

    reloaded = _load_candidate_evidence(db, download_log_id, source_path)
    if reloaded.evidence is None:
        return _LoadedEvidence(
            None,
            reloaded.outcome or OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
            f"after_refresh: {reloaded.reason}",
        )
    logger.info(
        "wrong_match_cleanup.evidence_refreshed download_log_id=%s path=%s",
        download_log_id,
        source_path,
    )
    return reloaded


def _matching_active_jobs(
    db: WrongMatchCleanupDB,
    *,
    download_log_id: int,
    request_id: int | None,
    failed_paths: Iterable[str],
    source_dirs: Iterable[str],
    ignore_import_job_id: int | None,
) -> list[ImportJob]:
    return db.list_active_import_jobs_for_wrong_match(
        download_log_id=download_log_id,
        request_id=request_id,
        failed_paths=failed_paths,
        source_dirs=source_dirs,
        ignore_import_job_id=ignore_import_job_id,
    )


def _path_candidates(*paths: str | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for path in paths:
        if not path:
            continue
        normalized = str(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _resolve_first_existing(paths: Iterable[str]) -> str | None:
    for path in paths:
        resolved = resolve_failed_path(path)
        if resolved is not None:
            return os.path.abspath(resolved)
    return None


def _runtime_config() -> Any:
    from lib.config import read_runtime_config

    return read_runtime_config()


def _summary(
    results: list[WrongMatchCleanupOutcome],
) -> WrongMatchCleanupSummary:
    # Outcome strings ARE the Summary's count-field names (guarded by
    # TestSummaryOutcomeContract), so the counts dict splats straight in.
    counts = {key: 0 for key in OUTCOME_KEYS}
    for result in results:
        if result.outcome in counts:
            counts[result.outcome] += 1
    return msgspec.structs.replace(
        WrongMatchCleanupSummary(
            processed=len(results),
            results=tuple(results),
        ),
        **counts,
    )


def _result(
    download_log_id: int,
    outcome: str,
    *,
    success: bool = False,
    request_id: int | None = None,
    source_path: str | None = None,
    reason: str | None = None,
    verdict: str | None = None,
    preview_decision: str | None = None,
    cleanup_eligible: bool = False,
    decision: dict[str, Any] | None = None,
    cleared_rows: int = 0,
    deleted_path: str | None = None,
    path_missing: bool = False,
    error: str | None = None,
    candidate_evidence: AlbumQualityEvidence | None = None,
    current_evidence: AlbumQualityEvidence | None = None,
) -> WrongMatchCleanupOutcome:
    ambiguous_source_lineage = any(
        evidence is not None and evidence.lineage_version not in (3, 4)
        for evidence in (candidate_evidence, current_evidence)
    )
    # Migration 050 lineage-v1 measurements may be target projections. A
    # comparison basis derived from either one is source-shaped too, so do not
    # even decode it at the cleanup audit boundary.
    basis = (
        None if ambiguous_source_lineage
        else comparison_basis_from_decision(decision)
    )

    def audit_measurement(
        evidence: AlbumQualityEvidence | None,
    ) -> AudioQualityMeasurement | None:
        if evidence is None:
            return None
        if evidence.lineage_version in (3, 4):
            return evidence.measurement
        # Spectral facts were never target projections. Preserve only that
        # valid subset, while an explicit empty measurement makes Recents
        # fail closed instead of falling back to ambiguous source values.
        measurement = evidence.measurement
        return AudioQualityMeasurement(
            spectral_grade=measurement.spectral_grade,
            spectral_bitrate_kbps=measurement.spectral_bitrate_kbps,
            spectral_subject=measurement.spectral_subject,
            spectral_provenance=measurement.spectral_provenance,
        )

    return WrongMatchCleanupOutcome(
        download_log_id=download_log_id,
        outcome=outcome,
        success=success,
        request_id=request_id,
        source_path=source_path,
        reason=reason,
        verdict=verdict,
        preview_decision=preview_decision,
        cleanup_eligible=cleanup_eligible,
        decision=decision or {},
        cleared_rows=cleared_rows,
        deleted_path=deleted_path,
        path_missing=path_missing,
        error=error,
        candidate_measurement=audit_measurement(candidate_evidence),
        current_measurement=audit_measurement(current_evidence),
        candidate_v0_probe=audit_v0_probe_from_metric(
            candidate_evidence.v0_metric
            if candidate_evidence is not None else None
        ),
        current_v0_probe=audit_v0_probe_from_metric(
            current_evidence.v0_metric
            if current_evidence is not None else None
        ),
        comparison_basis=basis,
    )

def _audit_action(outcome: str) -> str:
    if outcome == OUTCOME_DELETED:
        return "deleted_reject"
    if outcome == OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT:
        return "deleted_verified_lossless_parent"
    return outcome


def _stage_chain_from_decision(decision: dict[str, Any]) -> list[str]:
    chain: list[str] = []
    for key in (
        "preimport_audio",
        "preimport_bad_hash",
        "preimport_nested",
        "preimport_empty_fileset",
        "stage0_spectral_gate",
        "stage1_spectral",
        "stage2_import",
        "stage3_quality_gate",
    ):
        value = decision.get(key)
        if isinstance(value, str) and value:
            chain.append(f"{key}:{value}")
    return chain


def _cleanup_audit_payload(
    result: WrongMatchCleanupOutcome,
) -> WrongMatchTriageAudit:
    # omit_defaults on the Struct keeps unset fields out of the JSONB,
    # matching the old conditional dict building.
    return WrongMatchTriageAudit(
        action=_audit_action(result.outcome),
        outcome=result.outcome,
        success=result.success,
        reason=result.reason,
        preview_verdict=result.verdict,
        preview_decision=result.preview_decision,
        cleanup_eligible=result.cleanup_eligible,
        source_path=result.source_path,
        stage_chain=_stage_chain_from_decision(result.decision),
        cleared_rows=result.cleared_rows,
        deleted_path=result.deleted_path,
        path_missing=result.path_missing,
        error=result.error,
        candidate_measurement=result.candidate_measurement,
        current_measurement=result.current_measurement,
        candidate_v0_probe=result.candidate_v0_probe,
        current_v0_probe=result.current_v0_probe,
        comparison_basis=result.comparison_basis,
    )


def _persist_cleanup_audit(
    db: WrongMatchCleanupDB,
    result: WrongMatchCleanupOutcome,
) -> None:
    if result.outcome not in AUDITED_OUTCOMES:
        return
    try:
        db.record_wrong_match_triage(
            result.download_log_id, _cleanup_audit_payload(result))
    except Exception:  # noqa: BLE001
        logger.exception(
            "wrong_match_cleanup.audit_persist_failed download_log_id=%s",
            result.download_log_id,
        )
