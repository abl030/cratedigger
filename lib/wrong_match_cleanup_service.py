"""Evidence-only cleanup service for Wrong Matches."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

import msgspec

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
)
from lib.quality_evidence import audio_snapshot_matches
from lib.quality_evidence import load_candidate_evidence_for_source
from lib.util import resolve_failed_path
from lib.wrong_matches import cleanup_wrong_match_source, validation_failed_path

logger = logging.getLogger("cratedigger")

OUTCOME_DELETED = "deleted"
OUTCOME_KEPT_WOULD_IMPORT = "kept_would_import"
OUTCOME_KEPT_UNCERTAIN = "kept_uncertain"
OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING = "skipped_candidate_evidence_missing"
OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE = "skipped_candidate_evidence_stale"
OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING = "skipped_current_evidence_missing"
OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE = "skipped_current_evidence_stale"
OUTCOME_SKIPPED_ACTIVE_JOB = "skipped_active_job"
OUTCOME_SKIPPED_INVALID_ROW = "skipped_invalid_row"
OUTCOME_SKIPPED_MISSING_PATH = "skipped_missing_path"
OUTCOME_SKIPPED_OPERATIONAL = "skipped_operational"
OUTCOME_DELETE_FAILED = "delete_failed"

OUTCOME_KEYS: tuple[str, ...] = (
    OUTCOME_DELETED,
    OUTCOME_KEPT_WOULD_IMPORT,
    OUTCOME_KEPT_UNCERTAIN,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE,
    OUTCOME_SKIPPED_ACTIVE_JOB,
    OUTCOME_SKIPPED_INVALID_ROW,
    OUTCOME_SKIPPED_MISSING_PATH,
    OUTCOME_SKIPPED_OPERATIONAL,
    OUTCOME_DELETE_FAILED,
)

FINAL_AUDIT_OUTCOMES: frozenset[str] = frozenset({
    OUTCOME_DELETED,
    OUTCOME_KEPT_WOULD_IMPORT,
    OUTCOME_KEPT_UNCERTAIN,
    OUTCOME_DELETE_FAILED,
})


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

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


class WrongMatchCleanupSummary(msgspec.Struct, frozen=True):
    processed: int = 0
    deleted: int = 0
    kept_would_import: int = 0
    kept_uncertain: int = 0
    skipped_candidate_evidence_missing: int = 0
    skipped_candidate_evidence_stale: int = 0
    skipped_current_evidence_missing: int = 0
    skipped_current_evidence_stale: int = 0
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
    db: Any,
    *,
    confirm_all_wrong_matches: bool = False,
    ignore_import_job_id: int | None = None,
    cfg: Any = None,
) -> WrongMatchCleanupSummary:
    """Run cleanup over the full current Wrong Matches queue."""
    if confirm_all_wrong_matches is not True:
        raise ValueError("confirm_all_wrong_matches must be true")

    results: list[WrongMatchCleanupOutcome] = []
    for row in db.get_wrong_matches():
        download_log_id = row.get("download_log_id")
        if not isinstance(download_log_id, int) or isinstance(download_log_id, bool):
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
        ))
    return _summary(results)


def cleanup_wrong_match(
    db: Any,
    download_log_id: int,
    *,
    failed_path_hint: str | None = None,
    ignore_import_job_id: int | None = None,
    cfg: Any = None,
) -> WrongMatchCleanupOutcome:
    """Evaluate and possibly delete one Wrong Matches source row."""
    try:
        result = _cleanup_wrong_match(
            db,
            download_log_id,
            failed_path_hint=failed_path_hint,
            ignore_import_job_id=ignore_import_job_id,
            cfg=cfg,
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
    db: Any,
    download_log_id: int,
    *,
    failed_path_hint: str | None,
    ignore_import_job_id: int | None,
    cfg: Any,
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
    source_dirs = _validation_source_dirs(entry.get("validation_result"))

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
    if candidate.evidence is None:
        return _result(
            download_log_id,
            candidate.outcome or OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
            request_id=request_id,
            source_path=resolved_path,
            reason=candidate.reason,
        )

    current = _load_current_evidence(db, request, request_id)
    if current.outcome is not None:
        return _result(
            download_log_id,
            current.outcome,
            request_id=request_id,
            source_path=resolved_path,
            reason=current.reason,
        )

    runtime_cfg = cfg if cfg is not None else _runtime_config()
    decision = full_pipeline_decision_from_evidence(
        candidate.evidence,
        current.evidence,
        facts=AlbumQualityEvidenceDecisionFacts(
            import_mode="force",
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
        )

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
        )
    return _result(
        download_log_id,
        OUTCOME_DELETED,
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
    )


def _load_candidate_evidence(
    db: Any,
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


def _load_current_evidence(
    db: Any,
    request: dict[str, Any],
    request_id: int,
) -> _LoadedEvidence:
    current_path = request.get("imported_path")
    current_required = bool(current_path) or request.get("status") == "imported"
    if not current_required:
        return _LoadedEvidence(None)

    evidence_id = db.get_request_current_evidence_id(request_id)
    if evidence_id is None:
        return _LoadedEvidence(
            None,
            OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
            "current_evidence_fk_missing",
        )
    evidence = db.load_album_quality_evidence_by_id(evidence_id)
    if evidence is None:
        return _LoadedEvidence(
            None,
            OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
            f"current_evidence_id_{evidence_id}_missing",
        )
    if not current_path or not audio_snapshot_matches(str(current_path), evidence.files):
        return _LoadedEvidence(
            None,
            OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE,
            "current_album_changed_since_evidence_capture",
        )
    reasons = evidence.policy_incomplete_reasons()
    if reasons:
        return _LoadedEvidence(
            None,
            OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
            "; ".join(reasons),
        )
    return _LoadedEvidence(evidence)


def _matching_active_jobs(
    db: Any,
    *,
    download_log_id: int,
    request_id: int | None,
    failed_paths: Iterable[str],
    source_dirs: Iterable[str],
    ignore_import_job_id: int | None,
) -> list[Any]:
    finder = getattr(db, "list_active_import_jobs_for_wrong_match", None)
    if callable(finder):
        return list(finder(
            download_log_id=download_log_id,
            request_id=request_id,
            failed_paths=failed_paths,
            source_dirs=source_dirs,
            ignore_import_job_id=ignore_import_job_id,
        ))

    jobs = getattr(db, "list_active_import_jobs", lambda **_: [])(
        request_id=request_id
    )
    if ignore_import_job_id is None:
        return list(jobs)
    return [job for job in jobs if getattr(job, "id", None) != ignore_import_job_id]


def _validation_result_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _validation_source_dirs(raw: Any) -> tuple[str, ...]:
    data = _validation_result_dict(raw)
    value = data.get("source_dirs")
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(path) for path in value if path)


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
    counts = {key: 0 for key in OUTCOME_KEYS}
    for result in results:
        if result.outcome in counts:
            counts[result.outcome] += 1
    return WrongMatchCleanupSummary(
        processed=len(results),
        deleted=counts[OUTCOME_DELETED],
        kept_would_import=counts[OUTCOME_KEPT_WOULD_IMPORT],
        kept_uncertain=counts[OUTCOME_KEPT_UNCERTAIN],
        skipped_candidate_evidence_missing=counts[
            OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING
        ],
        skipped_candidate_evidence_stale=counts[
            OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE
        ],
        skipped_current_evidence_missing=counts[
            OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING
        ],
        skipped_current_evidence_stale=counts[
            OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE
        ],
        skipped_active_job=counts[OUTCOME_SKIPPED_ACTIVE_JOB],
        skipped_invalid_row=counts[OUTCOME_SKIPPED_INVALID_ROW],
        skipped_missing_path=counts[OUTCOME_SKIPPED_MISSING_PATH],
        skipped_operational=counts[OUTCOME_SKIPPED_OPERATIONAL],
        delete_failed=counts[OUTCOME_DELETE_FAILED],
        results=tuple(results),
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
) -> WrongMatchCleanupOutcome:
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
    )

def _audit_action(outcome: str) -> str:
    if outcome == OUTCOME_DELETED:
        return "deleted_reject"
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
) -> dict[str, object]:
    payload: dict[str, object] = {
        "action": _audit_action(result.outcome),
        "success": result.success,
        "outcome": result.outcome,
    }
    if result.reason is not None:
        payload["reason"] = result.reason
    if result.verdict is not None:
        payload["preview_verdict"] = result.verdict
    if result.preview_decision is not None:
        payload["preview_decision"] = result.preview_decision
    if result.cleanup_eligible:
        payload["cleanup_eligible"] = True
    if result.source_path is not None:
        payload["source_path"] = result.source_path
    stage_chain = _stage_chain_from_decision(result.decision)
    if stage_chain:
        payload["stage_chain"] = stage_chain
    if result.cleared_rows:
        payload["cleared_rows"] = result.cleared_rows
    if result.deleted_path is not None:
        payload["deleted_path"] = result.deleted_path
    if result.path_missing:
        payload["path_missing"] = True
    if result.error is not None:
        payload["error"] = result.error
    return payload


def _persist_cleanup_audit(
    db: Any,
    result: WrongMatchCleanupOutcome,
) -> None:
    if result.outcome not in FINAL_AUDIT_OUTCOMES:
        return
    recorder = getattr(db, "record_wrong_match_triage", None)
    if not callable(recorder):
        return
    try:
        recorder(result.download_log_id, _cleanup_audit_payload(result))
    except Exception:  # noqa: BLE001
        logger.exception(
            "wrong_match_cleanup.audit_persist_failed download_log_id=%s",
            result.download_log_id,
        )
