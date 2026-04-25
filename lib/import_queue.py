"""Typed helpers for the shared importer queue."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

IMPORT_JOB_FORCE = "force_import"
IMPORT_JOB_MANUAL = "manual_import"
IMPORT_JOB_AUTOMATION = "automation_import"

IMPORT_JOB_TYPES = frozenset({
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    IMPORT_JOB_AUTOMATION,
})
IMPORT_JOB_STATUSES = frozenset({"queued", "running", "completed", "failed"})
IMPORT_JOB_ACTIVE_STATUSES = frozenset({"queued", "running"})
IMPORT_JOB_PREVIEW_WAITING = "waiting"
IMPORT_JOB_PREVIEW_RUNNING = "running"
IMPORT_JOB_PREVIEW_WOULD_IMPORT = "would_import"
IMPORT_JOB_PREVIEW_CONFIDENT_REJECT = "confident_reject"
IMPORT_JOB_PREVIEW_UNCERTAIN = "uncertain"
IMPORT_JOB_PREVIEW_ERROR = "error"
IMPORT_JOB_PREVIEW_STATUSES = frozenset({
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_PREVIEW_RUNNING,
    IMPORT_JOB_PREVIEW_WOULD_IMPORT,
    IMPORT_JOB_PREVIEW_CONFIDENT_REJECT,
    IMPORT_JOB_PREVIEW_UNCERTAIN,
    IMPORT_JOB_PREVIEW_ERROR,
})
IMPORT_JOB_PREVIEW_FAILURE_STATUSES = frozenset({
    IMPORT_JOB_PREVIEW_CONFIDENT_REJECT,
    IMPORT_JOB_PREVIEW_UNCERTAIN,
    IMPORT_JOB_PREVIEW_ERROR,
})
IMPORT_JOB_PREVIEW_ENABLED_ENV = "CRATEDIGGER_IMPORT_PREVIEW_ENABLE"
IMPORT_JOB_PREVIEW_DISABLED_MESSAGE = "Preview gate disabled"


def import_preview_enabled_from_env() -> bool:
    value = os.environ.get(IMPORT_JOB_PREVIEW_ENABLED_ENV)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class ImportJob:
    """One row from ``import_jobs`` with JSONB fields normalized to dicts."""

    id: int
    job_type: str
    status: str
    request_id: int | None
    dedupe_key: str | None
    payload: dict[str, Any]
    result: dict[str, Any] | None
    message: str | None
    error: str | None
    attempts: int
    worker_id: str | None
    created_at: datetime | None
    updated_at: datetime | None
    started_at: datetime | None
    heartbeat_at: datetime | None
    completed_at: datetime | None
    preview_status: str | None = None
    preview_result: dict[str, Any] | None = None
    preview_message: str | None = None
    preview_error: str | None = None
    preview_attempts: int = 0
    preview_worker_id: str | None = None
    preview_started_at: datetime | None = None
    preview_heartbeat_at: datetime | None = None
    preview_completed_at: datetime | None = None
    importable_at: datetime | None = None
    deduped: bool = False

    @classmethod
    def from_row(cls, row: dict[str, Any], *, deduped: bool = False) -> "ImportJob":
        payload = _json_dict(row.get("payload"))
        result_raw = row.get("result")
        result = _json_dict(result_raw) if result_raw is not None else None
        preview_result_raw = row.get("preview_result")
        preview_result = (
            _json_dict(preview_result_raw)
            if preview_result_raw is not None
            else None
        )
        return cls(
            id=int(row["id"]),
            job_type=str(row["job_type"]),
            status=str(row["status"]),
            request_id=(
                int(row["request_id"])
                if row.get("request_id") is not None
                else None
            ),
            dedupe_key=(
                str(row["dedupe_key"])
                if row.get("dedupe_key") is not None
                else None
            ),
            payload=payload,
            result=result,
            message=(
                str(row["message"])
                if row.get("message") is not None
                else None
            ),
            error=(
                str(row["error"])
                if row.get("error") is not None
                else None
            ),
            attempts=int(row.get("attempts") or 0),
            worker_id=(
                str(row["worker_id"])
                if row.get("worker_id") is not None
                else None
            ),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            started_at=row.get("started_at"),
            heartbeat_at=row.get("heartbeat_at"),
            completed_at=row.get("completed_at"),
            preview_status=(
                str(row["preview_status"])
                if row.get("preview_status") is not None
                else None
            ),
            preview_result=preview_result,
            preview_message=(
                str(row["preview_message"])
                if row.get("preview_message") is not None
                else None
            ),
            preview_error=(
                str(row["preview_error"])
                if row.get("preview_error") is not None
                else None
            ),
            preview_attempts=int(row.get("preview_attempts") or 0),
            preview_worker_id=(
                str(row["preview_worker_id"])
                if row.get("preview_worker_id") is not None
                else None
            ),
            preview_started_at=row.get("preview_started_at"),
            preview_heartbeat_at=row.get("preview_heartbeat_at"),
            preview_completed_at=row.get("preview_completed_at"),
            importable_at=row.get("importable_at"),
            deduped=deduped,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_type": self.job_type,
            "status": self.status,
            "request_id": self.request_id,
            "dedupe_key": self.dedupe_key,
            "payload": self.payload,
            "result": self.result,
            "message": self.message,
            "error": self.error,
            "attempts": self.attempts,
            "worker_id": self.worker_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "heartbeat_at": self.heartbeat_at,
            "completed_at": self.completed_at,
            "preview_status": self.preview_status,
            "preview_result": self.preview_result,
            "preview_message": self.preview_message,
            "preview_error": self.preview_error,
            "preview_attempts": self.preview_attempts,
            "preview_worker_id": self.preview_worker_id,
            "preview_started_at": self.preview_started_at,
            "preview_heartbeat_at": self.preview_heartbeat_at,
            "preview_completed_at": self.preview_completed_at,
            "importable_at": self.importable_at,
            "deduped": self.deduped,
        }

    def to_json_dict(self) -> dict[str, Any]:
        result = self.to_dict()
        for key in (
            "created_at",
            "updated_at",
            "started_at",
            "heartbeat_at",
            "completed_at",
            "preview_started_at",
            "preview_heartbeat_at",
            "preview_completed_at",
            "importable_at",
        ):
            value = result[key]
            if hasattr(value, "isoformat"):
                result[key] = value.isoformat()
        return result


def _json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("import job JSON payload must be an object")


def validate_job_type(job_type: str) -> str:
    if job_type not in IMPORT_JOB_TYPES:
        raise ValueError(f"Invalid import job type: {job_type}")
    return job_type


def validate_status(status: str) -> str:
    if status not in IMPORT_JOB_STATUSES:
        raise ValueError(f"Invalid import job status: {status}")
    return status


def validate_preview_status(status: str) -> str:
    if status not in IMPORT_JOB_PREVIEW_STATUSES:
        raise ValueError(f"Invalid import job preview status: {status}")
    return status


def validate_preview_failure_status(status: str) -> str:
    validate_preview_status(status)
    if status not in IMPORT_JOB_PREVIEW_FAILURE_STATUSES:
        raise ValueError(f"Invalid import job preview failure status: {status}")
    return status


def validate_payload(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    validate_job_type(job_type)
    payload = _json_dict(payload)
    if job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_MANUAL):
        failed_path = payload.get("failed_path")
        if not isinstance(failed_path, str) or not failed_path:
            raise ValueError(f"{job_type} payload requires failed_path")
    return payload


def force_import_dedupe_key(download_log_id: int) -> str:
    return f"{IMPORT_JOB_FORCE}:download_log:{int(download_log_id)}"


def manual_import_dedupe_key(request_id: int, path: str) -> str:
    return f"{IMPORT_JOB_MANUAL}:request:{int(request_id)}:path:{path}"


def automation_import_dedupe_key(request_id: int) -> str:
    return f"{IMPORT_JOB_AUTOMATION}:request:{int(request_id)}"


def force_import_payload(
    *,
    download_log_id: int,
    failed_path: str,
    source_username: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "download_log_id": int(download_log_id),
        "failed_path": failed_path,
    }
    if source_username:
        payload["source_username"] = source_username
    return payload


def manual_import_payload(*, failed_path: str) -> dict[str, Any]:
    return {"failed_path": failed_path}


def automation_import_payload() -> dict[str, Any]:
    return {}
