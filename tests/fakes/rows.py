"""Row dataclasses captured by FakePipelineDB's recorders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from tests.fakes._shared import _utcnow

@dataclass
class DownloadLogRow:
    """One row in download_log, captured by FakePipelineDB.log_download."""
    request_id: int
    outcome: str | None = None
    soulseek_username: str | None = None
    filetype: str | None = None
    beets_distance: float | None = None
    beets_scenario: str | None = None
    beets_detail: str | None = None
    staged_path: str | None = None
    error_message: str | None = None
    validation_result: Any = None
    import_result: Any = None
    # Migration 043 — per-file failure detail audit blob (issue #564 C7).
    transfer_detail: Any = None
    # Auto-assigned monotonic id matching PostgreSQL serial behaviour.
    id: int = 0
    # Migration 021: addressing FK to album_quality_evidence(id).
    candidate_evidence_id: int | None = None
    # Migration 037 — source discriminator + YT-specific JSONB blob.
    # ``source`` defaults to ``'slskd'`` matching the production NOT NULL
    # DEFAULT. ``youtube_metadata`` is NULL unless this row was written
    # via ``insert_youtube_running``.
    source: str = "slskd"
    youtube_metadata: dict[str, Any] | None = None
    # Auto-populated timestamp matching download_log.created_at.
    created_at: datetime = field(default_factory=_utcnow)
    # Catch-all for less commonly asserted fields
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class DenylistEntry:
    """One row in source_denylist."""
    request_id: int
    username: str
    reason: str | None = None


@dataclass
class SearchLogRow:
    """One row in search_log, captured by FakePipelineDB.log_search."""
    request_id: int
    query: str | None = None
    result_count: int | None = None
    elapsed_s: float | None = None
    outcome: str = "error"
    id: int = 0
    created_at: datetime = field(default_factory=_utcnow)
    # Forensic capture (U5 of search-escalation-and-forensics).
    # ``candidates`` is the JSONB blob persisted by ``log_search`` — the
    # in-memory representation is the JSON string the production code
    # would have written via ``msgspec.json.encode``. NULL on error rows.
    candidates: str | None = None
    variant: str | None = None
    final_state: str | None = None
    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0
    # U1 persisted-search-plans plan-context fields. All nullable; rows
    # written via ``log_search`` keep them None to mirror historical /
    # legacy production rows.
    plan_id: int | None = None
    plan_item_id: int | None = None
    plan_ordinal: int | None = None
    plan_strategy: str | None = None
    plan_canonical_query_key: str | None = None
    plan_repeat_group: str | None = None
    plan_generator_id: str | None = None
    execution_stage: str | None = None
    attempt_consumed: bool | None = None
    cursor_update_status: str | None = None
    stale_reason: str | None = None
    plan_cycle_snapshot: int | None = None
    # U2 of search-plan-entropy: pre-filter skip count. NOT NULL on
    # the real column with default 0; mirror that here so test asserts
    # never see None on the field.
    pre_filter_skip_count: int = 0
    # U11 forensics columns (R22-R27). All nullable in production;
    # default ``None`` here so legacy / pre-attempt rows mirror the
    # real DB ``NULL`` shape.
    rejection_reason: str | None = None
    result_count_uncapped: int | None = None
    query_token_count: int | None = None
    query_distinct_token_count: int | None = None
    expected_track_count: int | None = None
    matcher_score_top1: float | None = None
    query_template: str | None = None


@dataclass
class UserCooldownRow:
    """One row in user_cooldowns, captured by FakePipelineDB.add_cooldown."""
    username: str
    cooldown_until: datetime
    reason: str | None = None
    created_at: datetime = field(default_factory=_utcnow)


@dataclass
class SearchLedgerRow:
    """One row in slskd_search_ledger (migration 044, issue #576).

    Captured both by ``FakePipelineDB.record_search_id_calls`` (one entry
    per call, call-recording semantics) and as the underlying ledger table
    state (``ON CONFLICT DO NOTHING`` semantics — the first insert for a
    given ``search_id`` sticks; ``mark_search_ids_deleted`` mutates
    ``deleted_at`` in place on that same instance).
    """
    search_id: str
    purpose: str
    request_id: int | None
    created_at: datetime = field(default_factory=_utcnow)
    deleted_at: datetime | None = None


@dataclass
class FakeTransferLedgerRow:
    """One row in slskd_transfer_ledger (migration 045, issue #571).

    Named ``FakeTransferLedgerRow`` (not ``TransferLedgerRow``) to avoid
    colliding with the real write-payload ``msgspec.Struct`` of the same
    short name in ``lib.pipeline_db`` — that Struct carries only the
    write-ahead fields (``request_id``/``username``/``filename``/
    ``attempt_fingerprint``); this fake row carries the FULL table shape
    (``id``/``transfer_id``/``enqueued_at``/``local_path``/
    ``completed_at``) so the purpose-shaped ownership reads and fake
    write-method self-tests observe production-shaped state.
    """
    id: int
    request_id: int
    username: str
    filename: str
    attempt_fingerprint: str | None = None
    transfer_id: str | None = None
    enqueued_at: datetime = field(default_factory=_utcnow)
    local_path: str | None = None
    completed_at: datetime | None = None


@dataclass
class FieldResolutionRow:
    """One row in album_request_field_resolutions (migration 030).

    Captured by ``FakePipelineDB.record_field_resolution``. Mirrors the
    production schema field-for-field so tests can assert against the
    same row shape PipelineDB returns from ``get_field_resolution``.
    """
    request_id: int
    field_name: str
    status: str
    reason_code: str | None
    attempts: int = 1
    resolved_at: datetime = field(default_factory=_utcnow)
    id: int = 0
