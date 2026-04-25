"""Conservative Wrong Matches triage using unified import preview."""

from __future__ import annotations

from typing import Any

import msgspec

from lib.import_preview import ImportPreviewResult, preview_import_from_download_log
from lib.import_queue import force_import_dedupe_key
from lib.util import resolve_failed_path
from lib.wrong_matches import cleanup_wrong_match_source, dismiss_wrong_match_source


class WrongMatchTriageResult(msgspec.Struct):
    download_log_id: int
    action: str
    success: bool
    reason: str | None = None
    preview: ImportPreviewResult | None = None
    cleanup: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)  # type: ignore[no-any-return]


def _triage_audit_payload(result: WrongMatchTriageResult) -> dict[str, object]:
    preview = result.preview
    payload: dict[str, object] = {
        "action": result.action,
        "success": result.success,
    }
    if result.reason is not None:
        payload["reason"] = result.reason
    if preview is not None:
        payload.update({
            "preview_verdict": preview.verdict,
            "preview_decision": preview.decision,
            "preview_reason": preview.reason,
            "would_import": preview.would_import,
            "confident_reject": preview.confident_reject,
            "uncertain": preview.uncertain,
            "cleanup_eligible": preview.cleanup_eligible,
            "source_path": preview.source_path,
            "stage_chain": preview.stage_chain,
        })
    if result.cleanup is not None:
        payload["cleanup"] = result.cleanup
    return payload


def _persist_triage_audit(
    db: Any,
    result: WrongMatchTriageResult,
) -> WrongMatchTriageResult:
    recorder = getattr(db, "record_wrong_match_triage", None)
    if callable(recorder):
        recorder(result.download_log_id, _triage_audit_payload(result))
    return result


def triage_wrong_match(db: Any, download_log_id: int) -> WrongMatchTriageResult:
    """Preview one wrong-match row and clean only explicit safe rejects."""
    preview = preview_import_from_download_log(db, download_log_id)

    if preview.confident_reject and preview.cleanup_eligible:
        cleanup = cleanup_wrong_match_source(
            db,
            download_log_id,
            failed_path_hint=preview.source_path,
        )
        return _persist_triage_audit(db, WrongMatchTriageResult(
            download_log_id=download_log_id,
            action="deleted_reject" if cleanup.success else "delete_failed",
            success=cleanup.success,
            reason=preview.reason,
            preview=preview,
            cleanup=cleanup.to_dict(),
        ))

    if preview.decision == "path_missing":
        dismissal = dismiss_wrong_match_source(
            db,
            download_log_id,
            failed_path_hint=preview.source_path,
        )
        return _persist_triage_audit(db, WrongMatchTriageResult(
            download_log_id=download_log_id,
            action="stale_path_cleared" if dismissal.success else "stale_path_clear_failed",
            success=dismissal.success,
            reason=preview.reason,
            preview=preview,
            cleanup=dismissal.to_dict(),
        ))

    if preview.would_import:
        return _persist_triage_audit(db, WrongMatchTriageResult(
            download_log_id=download_log_id,
            action="kept_would_import",
            success=True,
            reason=preview.reason,
            preview=preview,
        ))

    return _persist_triage_audit(db, WrongMatchTriageResult(
        download_log_id=download_log_id,
        action="kept_uncertain",
        success=True,
        reason=preview.reason,
        preview=preview,
    ))


def triage_wrong_matches(
    db: Any,
    *,
    request_id: int | None = None,
    limit: int | None = None,
) -> list[WrongMatchTriageResult]:
    if limit is not None and limit <= 0:
        return []

    rows = db.get_wrong_matches()
    results: list[WrongMatchTriageResult] = []
    for row in rows:
        if request_id is not None and row.get("request_id") != request_id:
            continue
        raw_id = row.get("download_log_id") or row.get("id")
        if not isinstance(raw_id, int):
            continue
        results.append(triage_wrong_match(db, raw_id))
        if limit is not None and len(results) >= limit:
            break
    return results


def _summary_template() -> dict[str, int]:
    return {
        "previewed": 0,
        "would_import": 0,
        "confident_reject": 0,
        "uncertain_or_error": 0,
        "cleanup_candidates": 0,
        "cleaned": 0,
        "cleanup_failed": 0,
        "skipped_missing_files": 0,
        "skipped_active_jobs": 0,
        "skipped_invalid_rows": 0,
    }


def _failed_path_from_row(row: dict[str, object]) -> str | None:
    vr = row.get("validation_result")
    if isinstance(vr, str):
        try:
            decoded = msgspec.json.decode(vr)
        except msgspec.DecodeError:
            return None
        vr = decoded if isinstance(decoded, dict) else None
    if not isinstance(vr, dict):
        return None
    failed_path = vr.get("failed_path")
    return str(failed_path) if failed_path else None


def _count_preview(summary: dict[str, int], preview: ImportPreviewResult) -> None:
    summary["previewed"] += 1
    if preview.would_import:
        summary["would_import"] += 1
    elif preview.confident_reject:
        summary["confident_reject"] += 1
    else:
        summary["uncertain_or_error"] += 1


def _count_cleanup_candidate(summary: dict[str, int], preview: ImportPreviewResult) -> None:
    if preview.confident_reject and preview.cleanup_eligible:
        summary["cleanup_candidates"] += 1


def backfill_wrong_match_previews(
    db: Any,
    *,
    request_id: int | None = None,
    limit: int | None = None,
    cleanup: bool = False,
    dry_run: bool = False,
) -> dict[str, int]:
    """Explicit one-shot preview backfill for current Wrong Matches rows.

    ``dry_run`` previews and counts candidate outcomes without persisting audit
    details or deleting cleanup-eligible rows.
    """
    summary = _summary_template()
    if limit is not None and limit <= 0:
        return summary

    for row in db.get_wrong_matches():
        if request_id is not None and row.get("request_id") != request_id:
            continue
        raw_id = row.get("download_log_id") or row.get("id")
        if not isinstance(raw_id, int):
            summary["skipped_invalid_rows"] += 1
            continue
        failed_path = _failed_path_from_row(row)
        if not failed_path or resolve_failed_path(failed_path) is None:
            summary["skipped_missing_files"] += 1
            continue
        active_job = db.get_import_job_by_dedupe_key(
            force_import_dedupe_key(raw_id),
        )
        if active_job is not None:
            summary["skipped_active_jobs"] += 1
            continue

        if dry_run:
            preview = preview_import_from_download_log(db, raw_id)
            _count_preview(summary, preview)
            _count_cleanup_candidate(summary, preview)
        elif cleanup:
            result = triage_wrong_match(db, raw_id)
            if result.preview is not None:
                _count_preview(summary, result.preview)
                _count_cleanup_candidate(summary, result.preview)
            if result.action == "deleted_reject" and result.success:
                summary["cleaned"] += 1
            elif result.action == "delete_failed":
                summary["cleanup_failed"] += 1
        else:
            preview = preview_import_from_download_log(db, raw_id)
            _count_preview(summary, preview)
            _count_cleanup_candidate(summary, preview)
            _persist_triage_audit(db, WrongMatchTriageResult(
                download_log_id=raw_id,
                action="preview_backfilled",
                success=True,
                reason=preview.reason,
                preview=preview,
            ))

        if limit is not None and summary["previewed"] >= limit:
            break
    return summary
