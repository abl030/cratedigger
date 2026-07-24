"""Typed helpers for the shared importer queue."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any

import msgspec

IMPORT_JOB_FORCE = "force_import"
IMPORT_JOB_AUTOMATION = "automation_import"
# YouTube rescue ingest (U2 of the YT rescue plan). The YT worker stages
# audio to the configured ``auto-import/<artist>-<album>/`` directory and
# enqueues a
# ``youtube_import`` job with the staged path carried in ``payload``
# rather than via ``album_requests.active_download_state``. The importer
# dispatcher (U9) reads the path from the payload and reuses the rest of
# the existing per-job pipeline (preview measurement, quality gate,
# beets distance, wrong-matches OR auto-import, ``mark_imported_with_rescue``).
IMPORT_JOB_YOUTUBE = "youtube_import"
IMPORT_JOB_RECOVERY_REQUIRED = "recovery_required"

IMPORT_JOB_TYPES = frozenset({
    IMPORT_JOB_FORCE,
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_YOUTUBE,
})
IMPORT_JOB_STATUSES = frozenset({
    "queued",
    "running",
    IMPORT_JOB_RECOVERY_REQUIRED,
    "completed",
    "failed",
})
IMPORT_JOB_ACTIVE_STATUSES = frozenset({
    "queued",
    "running",
    IMPORT_JOB_RECOVERY_REQUIRED,
})
IMPORT_JOB_PREVIEW_WAITING = "waiting"
IMPORT_JOB_PREVIEW_RUNNING = "running"
IMPORT_JOB_PREVIEW_EVIDENCE_READY = "evidence_ready"
# Historical/raw display/audit vocabulary. New typed jobs never write this
# status; raw/default queued rows can exist but are not runnable.
IMPORT_JOB_PREVIEW_WOULD_IMPORT = "would_import"
IMPORT_JOB_PREVIEW_CONFIDENT_REJECT = "confident_reject"
# Historical: ``IMPORT_JOB_PREVIEW_UNCERTAIN`` was retired by U5 as a value
# production code can write. The string ``'uncertain'`` remains in
# ``IMPORT_JOB_PREVIEW_STATUSES`` so legacy rows decode cleanly, but no
# production code path writes it after U5 — preview emits
# ``IMPORT_JOB_PREVIEW_MEASUREMENT_FAILED`` instead.
IMPORT_JOB_PREVIEW_MEASUREMENT_FAILED = "measurement_failed"
IMPORT_JOB_PREVIEW_ERROR = "error"
IMPORT_JOB_PREVIEW_STATUSES = frozenset({
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_PREVIEW_RUNNING,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_WOULD_IMPORT,
    IMPORT_JOB_PREVIEW_CONFIDENT_REJECT,
    "uncertain",  # historical-row support only; no production writer
    IMPORT_JOB_PREVIEW_MEASUREMENT_FAILED,
    IMPORT_JOB_PREVIEW_ERROR,
})
IMPORT_JOB_PREVIEW_FAILURE_STATUSES = frozenset({
    IMPORT_JOB_PREVIEW_CONFIDENT_REJECT,
    IMPORT_JOB_PREVIEW_MEASUREMENT_FAILED,
    IMPORT_JOB_PREVIEW_ERROR,
})
IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES = frozenset({
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
})


_PositiveInt = Annotated[int, msgspec.Meta(gt=0)]
_NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]


class ForceImportPayload(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """The strict JSONB contract for a ``force_import`` queue row."""

    download_log_id: _PositiveInt
    failed_path: _NonEmptyStr
    source_username: str | None = None
    source_dirs: list[str] = msgspec.field(default_factory=list[str])


class AutomationImportPayload(
    msgspec.Struct,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """The intentionally empty JSONB contract for automation queue rows."""


class YoutubeImportPayload(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    """The strict JSONB contract for a ``youtube_import`` queue row."""

    staged_path: _NonEmptyStr
    request_id: _PositiveInt
    browse_id: _NonEmptyStr
    download_log_id: _PositiveInt


ImportJobPayload = (
    ForceImportPayload | AutomationImportPayload | YoutubeImportPayload
)


def decode_import_job_payload(job_type: str, value: Any) -> ImportJobPayload:
    """Decode the one strict payload selected by a database row's job type."""
    validate_job_type(job_type)
    if job_type == IMPORT_JOB_FORCE:
        return msgspec.convert(value, type=ForceImportPayload)
    if job_type == IMPORT_JOB_AUTOMATION:
        return msgspec.convert(value, type=AutomationImportPayload)
    if job_type == IMPORT_JOB_YOUTUBE:
        return msgspec.convert(value, type=YoutubeImportPayload)
    raise AssertionError(f"validated unknown import job type: {job_type}")


def _payload_to_builtins(payload: ImportJobPayload) -> dict[str, Any]:
    """Serialize one canonical payload Struct to its JSONB object shape."""
    return msgspec.convert(msgspec.to_builtins(payload), type=dict[str, Any])


@dataclass(frozen=True)
class ImportJob:
    """One row from ``import_jobs`` with a strict job-specific payload."""

    id: int
    job_type: str
    status: str
    request_id: int | None
    dedupe_key: str | None
    payload: ImportJobPayload
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
    candidate_evidence_id: int | None = None
    expected_request_status: str | None = None
    beets_launch_authorized_at: datetime | None = None
    beets_launch_release_id: str | None = None
    beets_launch_source_path: str | None = None
    beets_launch_request_status: str | None = None
    beets_launch_snapshot_fingerprint: str | None = None
    deduped: bool = False

    @classmethod
    def from_row(cls, row: dict[str, Any], *, deduped: bool = False) -> "ImportJob":
        job_type = validate_job_type(str(row["job_type"]))
        payload = decode_import_job_payload(job_type, row.get("payload"))
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
            job_type=job_type,
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
            candidate_evidence_id=(
                int(row["candidate_evidence_id"])
                if row.get("candidate_evidence_id") is not None
                else None
            ),
            expected_request_status=(
                str(row["expected_request_status"])
                if row.get("expected_request_status") is not None
                else None
            ),
            beets_launch_authorized_at=row.get(
                "beets_launch_authorized_at"
            ),
            beets_launch_release_id=(
                str(row["beets_launch_release_id"])
                if row.get("beets_launch_release_id") is not None
                else None
            ),
            beets_launch_source_path=(
                str(row["beets_launch_source_path"])
                if row.get("beets_launch_source_path") is not None
                else None
            ),
            beets_launch_request_status=(
                str(row["beets_launch_request_status"])
                if row.get("beets_launch_request_status") is not None
                else None
            ),
            beets_launch_snapshot_fingerprint=(
                str(row["beets_launch_snapshot_fingerprint"])
                if row.get("beets_launch_snapshot_fingerprint") is not None
                else None
            ),
            deduped=deduped,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_type": self.job_type,
            "status": self.status,
            "request_id": self.request_id,
            "dedupe_key": self.dedupe_key,
            "payload": _payload_to_builtins(self.payload),
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
            "candidate_evidence_id": self.candidate_evidence_id,
            "expected_request_status": self.expected_request_status,
            "beets_launch_authorized_at": self.beets_launch_authorized_at,
            "beets_launch_release_id": self.beets_launch_release_id,
            "beets_launch_source_path": self.beets_launch_source_path,
            "beets_launch_request_status": self.beets_launch_request_status,
            "beets_launch_snapshot_fingerprint": (
                self.beets_launch_snapshot_fingerprint
            ),
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
            "beets_launch_authorized_at",
        ):
            value = result[key]
            if hasattr(value, "isoformat"):
                result[key] = value.isoformat()
        return result


def _json_dict(value: Any) -> dict[str, Any]:
    """Narrow a JSONB/JSON-decoded value to a plain string-keyed dict.

    A bare ``isinstance(value, dict)`` leaves pyright with a partially
    unknown ``dict[Unknown, Unknown]`` even when ``value`` was already
    fully known — strict mode never lets an ``isinstance`` narrowing
    inherit a generic's type argument (same quirk documented on
    ``lib.youtube_album_service._json_dict``). ``msgspec.convert`` gives
    every caller a fully known ``dict[str, Any]`` back — a real
    reconstruction rather than a shallow ``dict(value)`` copy, but with
    the identical result for JSON-shaped (string-keyed) input, which is
    the only shape this JSONB/JSON-decoded payload ever carries.
    """
    if value is None:
        return {}
    if isinstance(value, dict):
        return msgspec.convert(value, type=dict[str, Any])
    if isinstance(value, str):
        parsed: object = json.loads(value)
        if isinstance(parsed, dict):
            return msgspec.convert(parsed, type=dict[str, Any])
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
    """Validate and serialize the canonical job-specific JSONB contract."""
    return _payload_to_builtins(decode_import_job_payload(job_type, payload))


def force_import_dedupe_key(download_log_id: int) -> str:
    return f"{IMPORT_JOB_FORCE}:download_log:{int(download_log_id)}"


def automation_import_dedupe_key(request_id: int) -> str:
    return f"{IMPORT_JOB_AUTOMATION}:request:{int(request_id)}"


def youtube_import_dedupe_key(download_log_id: int) -> str:
    """Dedupe-key for a YT rescue's `youtube_import` job_type row.

    The YT worker creates exactly one ``youtube_import`` per
    ``download_log`` row (the row is the queue entry's audit ancestor).
    Keying on the download_log id is the right grain: a re-enqueue
    attempt for the same submission would otherwise create a parallel
    import_jobs row.
    """
    return f"{IMPORT_JOB_YOUTUBE}:download_log:{int(download_log_id)}"


def force_import_payload(
    *,
    download_log_id: int,
    failed_path: str,
    source_username: str | None = None,
    source_dirs: list[str] | None = None,
) -> dict[str, Any]:
    return _payload_to_builtins(ForceImportPayload(
        download_log_id=download_log_id,
        failed_path=failed_path,
        source_username=source_username,
        source_dirs=source_dirs or [],
    ))


def automation_import_payload() -> dict[str, Any]:
    return _payload_to_builtins(AutomationImportPayload())


def youtube_import_payload(
    *,
    staged_path: str,
    request_id: int,
    browse_id: str,
    download_log_id: int,
) -> dict[str, Any]:
    """Build the payload for a ``youtube_import`` job.

    The YT ingest worker stages audio to
    ``/Incoming/auto-import/<artist>-<album>/`` and enqueues this
    payload — the importer dispatcher (U9) reads ``staged_path``
    instead of the slskd-shaped ``active_download_state``.

    The positive request/download-log IDs and nonempty path/browse ID are
    validated from the same Struct immediately before either queue write.
    """
    return _payload_to_builtins(YoutubeImportPayload(
        staged_path=staged_path,
        request_id=request_id,
        browse_id=browse_id,
        download_log_id=download_log_id,
    ))
