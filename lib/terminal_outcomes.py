"""Typed commands for atomic terminal import and preview outcomes.

These values cross the dispatch/worker -> PipelineDB ownership boundary and
contain JSON strings destined for JSONB columns.  They are ``msgspec.Struct``
contracts so callers cannot silently omit or mis-type one member of a terminal
bundle while the database owns the transaction.
"""

from __future__ import annotations

from enum import Enum

import msgspec

from lib.validation_envelope import derive_validation_log_columns


class TerminalOutcomeBoundary(str, Enum):
    """Observable write boundaries used by real-PG rollback qualification."""

    request = "request"
    audit = "audit"
    denylist = "denylist"
    final_request = "final_request"
    job = "job"


class ImportJobSupplementKey(str, Enum):
    cleanup = "cleanup"
    wrong_match_dismissal = "wrong_match_dismissal"


class TerminalOutcomeConflict(RuntimeError):
    """The terminal command no longer owns its request or import job."""


class DenylistWrite(msgspec.Struct, frozen=True, kw_only=True):
    username: str
    reason: str | None = None


class DownloadAuditWrite(msgspec.Struct, frozen=True, kw_only=True):
    """Complete download_log projection used by terminal transactions."""

    outcome: str
    soulseek_username: str | None = None
    filetype: str | None = None
    download_path: str | None = None
    beets_distance: float | None = None
    beets_scenario: str | None = None
    beets_detail: str | None = None
    valid: bool | None = None
    staged_path: str | None = None
    error_message: str | None = None
    bitrate: int | None = None
    sample_rate: int | None = None
    bit_depth: int | None = None
    is_vbr: bool | None = None
    was_converted: bool | None = None
    original_filetype: str | None = None
    slskd_filetype: str | None = None
    actual_filetype: str | None = None
    actual_min_bitrate: int | None = None
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_min_bitrate: int | None = None
    existing_spectral_bitrate: int | None = None
    import_result_json: str | None = None
    validation_result_json: str | None = None
    final_format: str | None = None
    v0_probe_kind: str | None = None
    v0_probe_min_bitrate: int | None = None
    v0_probe_avg_bitrate: int | None = None
    v0_probe_median_bitrate: int | None = None
    existing_v0_probe_kind: str | None = None
    existing_v0_probe_min_bitrate: int | None = None
    existing_v0_probe_avg_bitrate: int | None = None
    existing_v0_probe_median_bitrate: int | None = None


def canonicalize_download_audit(
    audit: DownloadAuditWrite,
) -> DownloadAuditWrite:
    """Make validation JSON the authority for denormalized query columns."""
    raw = audit.validation_result_json
    projected_distance, projected_scenario = derive_validation_log_columns(raw)
    if raw:
        decoded = msgspec.json.decode(raw)
        if not isinstance(decoded, dict):
            raise ValueError("terminal validation_result JSON must encode an object")
        has_distance = "distance" in decoded
        has_scenario = "scenario" in decoded
    else:
        has_distance = False
        has_scenario = False
    return msgspec.structs.replace(
        audit,
        beets_distance=(
            projected_distance if has_distance else audit.beets_distance
        ),
        beets_scenario=(
            projected_scenario if has_scenario else audit.beets_scenario
        ),
    )


class ImportedRequestWrite(msgspec.Struct, frozen=True, kw_only=True):
    """Imported-state metadata, with explicit write flags for nullable pairs."""

    beets_distance: float | None
    beets_scenario: str | None
    imported_path: str | None
    verified_lossless: bool
    final_format: str | None
    write_spectral: bool = False
    last_download_spectral_grade: str | None = None
    last_download_spectral_bitrate: int | None = None
    current_spectral_grade: str | None = None
    current_spectral_bitrate: int | None = None
    write_v0_probe: bool = False
    current_lossless_source_v0_probe_min_bitrate: int | None = None
    current_lossless_source_v0_probe_avg_bitrate: int | None = None
    current_lossless_source_v0_probe_median_bitrate: int | None = None
    write_quality_delta: bool = False
    prev_min_bitrate: int | None = None
    min_bitrate: int | None = None


class ImportJobOutcomeResult(msgspec.Struct, frozen=True, kw_only=True):
    success: bool
    message: str
    deferred: bool
    code: str | None


class ImportSuccessOutcome(msgspec.Struct, frozen=True, kw_only=True):
    request_id: int
    request: ImportedRequestWrite
    audit: DownloadAuditWrite
    job_result: ImportJobOutcomeResult
    job_message: str
    import_job_id: int | None = None
    denylist: tuple[DenylistWrite, ...] = ()
    requeue_after_import: bool = False
    requeue_search_filetype_override: str | None = None
    requeue_min_bitrate: int | None = None


class ImporterRejectionOutcome(msgspec.Struct, frozen=True, kw_only=True):
    request_id: int
    requeue_to_wanted: bool
    record_validation_attempt: bool
    write_search_filetype_override: bool
    search_filetype_override: str | None
    audit: DownloadAuditWrite
    job_result: ImportJobOutcomeResult
    job_error: str
    job_message: str
    import_job_id: int | None = None
    denylist: tuple[DenylistWrite, ...] = ()


class PreviewMeasurementFailureOutcome(msgspec.Struct, frozen=True, kw_only=True):
    request_id: int
    import_job_id: int
    preview_status: str
    preview_result_json: str
    preview_error: str
    preview_message: str
    validation_result_json: str
    import_result_json: str | None
    staged_path: str | None
    detail: str | None
    denylist: tuple[DenylistWrite, ...] = ()


class TerminalOutcomeApplied(msgspec.Struct, frozen=True, kw_only=True):
    request_id: int
    download_log_id: int
    import_job_id: int | None


class ImportJobOutcomeSupplement(msgspec.Struct, frozen=True, kw_only=True):
    """Non-terminal operator audit appended after filesystem convergence."""

    import_job_id: int
    key: ImportJobSupplementKey
    payload_json: str
