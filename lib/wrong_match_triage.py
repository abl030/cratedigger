"""Conservative Wrong Matches triage using unified import preview."""

from __future__ import annotations

from typing import Any

import msgspec

from lib.import_preview import ImportPreviewResult, preview_import_from_download_log
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
