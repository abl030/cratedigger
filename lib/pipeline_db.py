#!/usr/bin/env python3
"""Pipeline DB — PostgreSQL-based source of truth for the download pipeline.

Connects to PostgreSQL via a DSN (connection string). Both doc1 and doc2
connect over the network — no more SQLite file locking issues on virtiofs.

Usage:
    from lib.pipeline_db import PipelineDB
    db = PipelineDB("postgresql://cratedigger@192.168.1.35/cratedigger")
    db.add_request(mb_release_id="...", artist_name="...", album_title="...", source="redownload")
"""

import hashlib
import json
import logging
import os
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Iterable, Iterator

if TYPE_CHECKING:
    from lib.quality import CandidateScore

import psycopg2
import psycopg2.extras
import msgspec

from lib.import_queue import (
    ImportJob,
    IMPORT_JOB_PREVIEW_DISABLED_MESSAGE,
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_PREVIEW_WOULD_IMPORT,
    import_preview_enabled_from_env,
    validate_preview_failure_status,
    validate_job_type,
    validate_payload,
    validate_status,
)
from lib.quality import (CooldownConfig, SpectralMeasurement, V0ProbeEvidence,
                         should_cooldown)
from lib.release_identity import ReleaseIdentity, normalize_release_id

logger = logging.getLogger(__name__)

DEFAULT_DSN = os.environ.get("PIPELINE_DB_DSN", "postgresql://cratedigger@localhost/cratedigger")

# Exponential backoff: base_minutes * 2^(attempts-1), capped at max
BACKOFF_BASE_MINUTES = 30
BACKOFF_MAX_MINUTES = 60 * 6  # 6 hours
DASHBOARD_WINDOWS: tuple[tuple[str, int], ...] = (("24h", 24), ("6h", 6))


def _escape_like_pattern(value: str) -> str:
    """Escape SQL LIKE wildcards for ``... LIKE %s ESCAPE '\'``."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _isoformat_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _stable_hash(label: str, *parts: str) -> str:
    """Return a namespaced SHA-256 digest for privacy-preserving counters."""
    digest = hashlib.sha256()
    digest.update(f"cratedigger:{label}\0".encode("utf-8"))
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(str(len(encoded)).encode("ascii"))
        digest.update(b"\0")
        digest.update(encoded)
        digest.update(b"\0")
    return digest.hexdigest()


def _peer_dir_hashes(username: str, file_dir: str) -> tuple[str, str, str]:
    return (
        _stable_hash("peer-dir-combo", username, file_dir),
        _stable_hash("peer-dir-user", username),
        _stable_hash("peer-dir-dir", file_dir),
    )

# Advisory-lock namespaces. Every lock in this codebase is
# session-scoped, non-blocking (``pg_try_advisory_lock``), and
# session-reentrant. See ``docs/advisory-locks.md`` for the canonical
# rules covering namespace values, key derivation, ordering, and call
# sites. Every acquire site links back there.

# Per-request lock — force/manual-import double-click protection
# (issue #92). Key = ``request_id``. ``0x46494D50`` = ASCII "FIMP",
# recognisable in ``pg_locks`` during debugging.
ADVISORY_LOCK_NAMESPACE_IMPORT = 0x46494D50

# Per-release lock — cross-process Palo Santo-class protection
# (issue #132 P1, issue #133). Not the 04-20 incident's root cause
# (that was YAML misconfig; see CLAUDE.md § Resolved canonical RCs)
# but an independent vector that could produce similar data loss if
# the lock were missing. Key = ``release_id_to_lock_key(mb_release_id)``.
# ``0x52454C45`` = ASCII "RELE", recognisable alongside FIMP in ``pg_locks``.
ADVISORY_LOCK_NAMESPACE_RELEASE = 0x52454C45

# Singleton importer-worker lock. The DB queue serializes claims, but beets
# mutation is intentionally a single lane, so the worker process itself also
# takes a process-wide lock before recovering or claiming jobs.
# ``0x51554555`` = ASCII "QUEU", recognisable in ``pg_locks``.
ADVISORY_LOCK_NAMESPACE_IMPORTER = 0x51554555

# Per-request lock — search-plan generation / supersession / cursor
# serialisation. Held by ``SearchPlanService`` so plan creation, supersession,
# and the cursor-coupled writes around them never race for the same
# ``request_id`` across CLI / web / startup. Key = ``request_id``.
# ``0x504C414E`` = ASCII "PLAN", recognisable in ``pg_locks``.
ADVISORY_LOCK_NAMESPACE_PLAN = 0x504C414E


def release_id_to_lock_key(mb_release_id: str) -> int:
    """Map an ``mb_release_id`` string to a stable int32 advisory-lock key.

    PostgreSQL's two-arg ``pg_advisory_lock(int4, int4)`` takes signed
    int32 keys. ``mb_release_id`` is a str — either a MusicBrainz UUID
    (36 chars) or a Discogs numeric release id. ``zlib.crc32`` is
    stable across processes (Python's builtin ``hash`` is salted per
    interpreter — unusable for cross-process locking), fast, and its
    32-bit output fits once we mask to 31 bits to keep the value
    non-negative (simpler to display in ``pg_locks`` rows).

    Collision behaviour: 2^31 distinct keys. With N concurrent
    same-release contenders, collision probability is ~N²/2^31 — a
    false-collision would serialise two unrelated releases, delaying
    the second by at most one import cycle (~minutes). Acceptable:
    losing a cycle of parallelism is cheap, whereas a missed lock on
    the real cross-process race could produce Palo Santo-*class* data
    loss (an independent vector from the 04-20 incident's YAML-misconfig
    root cause — see ``CLAUDE.md`` § Resolved canonical RCs).

    Input is ``.strip()``ed before hashing so a legacy DB row with
    stray leading/trailing whitespace (``"12856590 "`` vs
    ``"12856590"``) still keys the lock at the same value across
    processes — otherwise a normalization mismatch would defeat the
    lock's purpose silently.
    """
    return zlib.crc32(mb_release_id.strip().encode("utf-8")) & 0x7FFFFFFF

# Schema is managed by lib/migrator.py via numbered files in migrations/.
# PipelineDB itself never runs DDL — see scripts/migrate_db.py and the
# cratedigger-db-migrate.service systemd unit (Nix module).


@dataclass(frozen=True)
class RequestSpectralStateUpdate:
    """Typed update for latest-download and on-disk spectral state."""
    last_download: SpectralMeasurement | None = None
    current: SpectralMeasurement | None = None

    def as_update_fields(self) -> dict[str, object]:
        """Expand the typed state into album_requests column updates."""
        fields: dict[str, object] = {}
        if self.last_download is not None:
            fields["last_download_spectral_grade"] = self.last_download.grade
            fields["last_download_spectral_bitrate"] = self.last_download.bitrate_kbps
        if self.current is not None:
            fields["current_spectral_grade"] = self.current.grade
            fields["current_spectral_bitrate"] = self.current.bitrate_kbps
        return fields


# BadAudioHashRow / BadAudioHashInput are @dataclass — not msgspec.Struct —
# because they never cross JSON. They round-trip between Python and PostgreSQL
# only (`bad_audio_hashes` table). Per `.claude/rules/code-quality.md`
# "Wire-boundary types", `@dataclass` is correct here.
@dataclass(frozen=True)
class BadAudioHashInput:
    """One row to insert into `bad_audio_hashes`."""
    hash_value: bytes  # raw 32-byte SHA-256
    audio_format: str  # 'flac' | 'mp3' | 'm4a' | 'ogg' | ...


@dataclass(frozen=True)
class BadAudioHashRow:
    """One row read back from `bad_audio_hashes`."""
    id: int
    hash_value: bytes
    audio_format: str
    request_id: int | None
    reported_username: str | None
    reason: str | None
    reported_at: datetime  # tz-aware


# ---------------------------------------------------------------------------
# Search-plan types
# ---------------------------------------------------------------------------
#
# These are @dataclass (not msgspec.Struct) because they round-trip between
# Python and PostgreSQL only -- their inputs are constructed entirely in our
# typed Python code (the plan generator, persistence service, executor) and
# they never cross JSON. The JSONB blobs they carry (metadata_snapshot,
# provenance) are dict[str, object]; sanitization/bounding is the
# application's responsibility before insert.

# Plan status domain matches the CHECK constraint on search_plans.status in
# migrations/014_persisted_search_plans.sql.
PLAN_STATUS_ACTIVE = "active"
PLAN_STATUS_SUPERSEDED = "superseded"
PLAN_STATUS_FAILED_DETERMINISTIC = "failed_deterministic"
PLAN_STATUS_FAILED_TRANSIENT = "failed_transient"

# search_log.execution_stage values.
SEARCH_LOG_STAGE_PRE_ATTEMPT = "pre_attempt"
SEARCH_LOG_STAGE_ACCEPTED = "accepted"
SEARCH_LOG_STAGE_STALE_COMPLETION = "stale_completion"
SEARCH_LOG_STAGE_RECONCILIATION = "reconciliation"

# search_log.cursor_update_status values.
CURSOR_UPDATE_ADVANCED = "advanced"
CURSOR_UPDATE_WRAPPED = "wrapped"
CURSOR_UPDATE_UNCHANGED = "unchanged"
CURSOR_UPDATE_STALE = "stale"


@dataclass(frozen=True)
class SearchPlanItemInput:
    """One runnable plan item before insert.

    ``ordinal`` is assigned by the caller (typically the generator);
    ``record_consumed_search_attempt`` reads ``next_plan_ordinal`` off the
    request row to know which item to advance from.
    """
    ordinal: int
    strategy: str
    query: str
    canonical_query_key: str | None = None
    repeat_group: str | None = None
    provenance: dict[str, object] | None = None


class SearchPlanMetadataSnapshot(
    msgspec.Struct, frozen=True, omit_defaults=True,
):
    """Typed JSONB boundary for ``search_plans.metadata_snapshot``."""

    artist_name: str | None = None
    album_title: str | None = None
    year: Any = None
    track_count: int | None = None
    redownload: bool = False
    prepend_artist: bool = False


class SearchPlanProvenance(
    msgspec.Struct, frozen=True,
):
    """Typed JSONB boundary for free-form plan generator provenance."""

    values: dict[str, Any] = msgspec.field(default_factory=dict)


class SearchPlanItemProvenance(
    msgspec.Struct, frozen=True,
):
    """Typed JSONB boundary for free-form plan-item provenance."""

    values: dict[str, Any] = msgspec.field(default_factory=dict)


def _metadata_snapshot_from_jsonb(
    value: Any,
) -> SearchPlanMetadataSnapshot | None:
    if value is None:
        return None
    return msgspec.convert(value, type=SearchPlanMetadataSnapshot)


def _plan_provenance_from_jsonb(value: Any) -> SearchPlanProvenance | None:
    if value is None:
        return None
    if isinstance(value, SearchPlanProvenance):
        return value
    return SearchPlanProvenance(
        values=msgspec.convert(value, type=dict[str, Any]))


def _item_provenance_from_jsonb(value: Any) -> SearchPlanItemProvenance | None:
    if value is None:
        return None
    if isinstance(value, SearchPlanItemProvenance):
        return value
    return SearchPlanItemProvenance(
        values=msgspec.convert(value, type=dict[str, Any]))


def jsonb_to_builtins(value: Any) -> Any:
    """Return a JSON-serialisable dict/list/primitive for JSONB structs."""
    if value is None:
        return None
    if isinstance(value, (SearchPlanProvenance, SearchPlanItemProvenance)):
        return dict(value.values)
    if isinstance(value, msgspec.Struct):
        return msgspec.to_builtins(value)
    return value


def _json_param(value: Any, typ: Any) -> psycopg2.extras.Json | None:
    if value is None:
        return None
    if typ in (SearchPlanProvenance, SearchPlanItemProvenance):
        if isinstance(value, typ):
            return psycopg2.extras.Json(dict(value.values))
        return psycopg2.extras.Json(
            msgspec.convert(value, type=dict[str, Any]))
    return psycopg2.extras.Json(
        msgspec.to_builtins(msgspec.convert(value, type=typ)))


@dataclass(frozen=True)
class SearchPlanItemRow:
    """One ``search_plan_items`` row read back from the DB."""
    id: int
    plan_id: int
    ordinal: int
    strategy: str
    query: str
    canonical_query_key: str | None
    repeat_group: str | None
    provenance: SearchPlanItemProvenance | None


@dataclass(frozen=True)
class SearchPlanRow:
    """One ``search_plans`` row read back from the DB."""
    id: int
    request_id: int
    generator_id: str
    status: str
    failure_class: str | None
    metadata_snapshot: SearchPlanMetadataSnapshot | None
    provenance: SearchPlanProvenance | None
    error_message: str | None
    superseded_at: datetime | None
    superseded_by_plan_id: int | None
    created_at: datetime


@dataclass(frozen=True)
class ActiveSearchPlan:
    """Active plan + items + cursor state for one request.

    Returned by ``get_active_search_plan``; consumers use it to know which
    plan/ordinal to execute next without joining tables themselves.
    """
    plan: SearchPlanRow
    items: list[SearchPlanItemRow]
    next_ordinal: int
    cycle_count: int


@dataclass(frozen=True)
class WantedReconciliationCandidate:
    """One row from ``list_wanted_for_plan_reconciliation``.

    All-wanted scan output: every wanted request paired with its current
    active plan id (NULL when missing) and current generator id (NULL
    when there is no active plan). Startup decides whether to generate /
    regenerate / skip per-row.
    """
    request_id: int
    active_plan_id: int | None
    active_plan_generator_id: str | None
    next_plan_ordinal: int
    plan_cycle_count: int


@dataclass(frozen=True)
class DryRunPlanClassification:
    """Minimum per-request data the dry-run reconciliation classifier needs.

    ``_classify_dry_run`` only reads the active plan's generator id
    (already on ``WantedReconciliationCandidate``) and the *generator id*
    of the most recent failed deterministic / failed transient plan for
    the request. Returning anything more than that turns a single batch
    fetch into per-row work; returning anything less makes the bucket
    decision impossible.

    Either ``latest_failed_deterministic_generator_id`` or
    ``latest_failed_transient_generator_id`` may be ``None`` independently
    -- a request can have a deterministic failure from one generator and
    no transient failure recorded, or vice versa.
    """

    request_id: int
    latest_failed_deterministic_generator_id: str | None
    latest_failed_transient_generator_id: str | None
    latest_failed_transient_created_at: datetime | None = None


@dataclass(frozen=True)
class SearchPlanInspection:
    """Aggregate plan view for one request (CLI/API inspection).

    Includes the current active plan (if any), the most recent failed
    deterministic / transient attempt for the same generator, the count
    of superseded plans, and a count of legacy search_log rows that
    pre-date persisted plans (NULL plan_id).
    """
    request_id: int
    active: ActiveSearchPlan | None
    latest_failed_deterministic: SearchPlanRow | None
    latest_failed_transient: SearchPlanRow | None
    superseded_count: int
    legacy_search_log_count: int


# ---------------------------------------------------------------------
# Search-plan usefulness stats (U8)
# ---------------------------------------------------------------------
#
# Cache attribution honesty: the on-disk schema today only attributes
# cache hits/misses at the *cycle* level (`cycle_metrics.cache_pos_hits`
# and friends — see `migrations/011_cycle_metrics.sql`). Search_log has
# no `cache_*` columns, so we cannot honestly say "this slot caused N
# hits". Stats expose ``cache_attribution_level='cycle_only'`` and
# ``cache_per_search_available=False`` rather than implying per-slot
# cache data exists when it does not.

CACHE_ATTRIBUTION_PER_SEARCH = "per_search"
CACHE_ATTRIBUTION_CYCLE_ONLY = "cycle_only"


@dataclass(frozen=True)
class SearchPlanStatsGroup:
    """One per-slot or per-query-group usefulness aggregate.

    The shape is the same for both grouping levels — slot grouping uses
    ``(plan_id, ordinal, strategy)`` as identity, query-group grouping
    uses ``(plan_id, repeat_group, canonical_query_key)``. The
    ``identity`` dict carries whichever of those keys apply for that row
    so dashboards can render either grouping.
    """
    identity: dict[str, object]
    attempts: int
    consumed_attempts: int
    non_consuming_attempts: int
    stale_completion_attempts: int
    outcome_counts: dict[str, int]
    elapsed_s_mean: float | None
    elapsed_s_p95: float | None
    result_count_mean: float | None
    browse_time_s_mean: float | None
    match_time_s_mean: float | None
    peers_browsed_mean: float | None
    fanout_waves_mean: float | None
    last_seen_at: datetime | None


@dataclass(frozen=True)
class SearchPlanStatsBucket:
    """Stats for one cohort of plan-aware search_log rows.

    Two buckets are returned per request: ``current`` (the active plan)
    and ``superseded_and_legacy`` (every plan the request used to have
    plus a separate ``legacy_bucket`` for pre-plan rows).
    """
    slots: list[SearchPlanStatsGroup]
    query_groups: list[SearchPlanStatsGroup]
    legacy_bucket: SearchPlanStatsGroup | None
    cache_attribution_level: str
    cache_per_search_available: bool


@dataclass(frozen=True)
class SearchPlanStats:
    """Per-request usefulness stats payload.

    ``current`` covers only the active plan (when one exists). Its
    ``legacy_bucket`` is always None — legacy rows belong to history.
    ``superseded_and_legacy`` covers every plan that ever existed for
    this request, plus a populated ``legacy_bucket`` if there are any
    rows without plan context.
    """
    request_id: int
    current: SearchPlanStatsBucket
    superseded_and_legacy: SearchPlanStatsBucket


@dataclass(frozen=True)
class ConsumedAttemptInput:
    """Input contract for ``record_consumed_search_attempt``.

    The executor builds this from the plan-item it ran plus the slskd /
    browse / match telemetry that was historically passed to log_search.
    The DB method then does the guarded log-insert + cursor-advance in one
    transaction.
    """
    request_id: int
    plan_id: int
    plan_item_id: int
    plan_ordinal: int
    plan_strategy: str
    plan_canonical_query_key: str | None
    plan_repeat_group: str | None
    plan_generator_id: str
    query: str
    outcome: str
    result_count: int | None = None
    elapsed_s: float | None = None
    candidates_json: str | None = None
    variant: str | None = None
    final_state: str | None = None
    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0
    # Plan-item count required by wrap detection. The executor reads it
    # from the active plan it executed; passing it explicitly avoids a
    # second SELECT inside the transaction.
    plan_item_count: int = 0
    # The request-level cycle count captured when this plan item was
    # selected. A same-plan/same-ordinal completion after wrap or
    # regeneration is stale even when the cursor ordinal matches again.
    cycle_count_snapshot: int = 0
    # Whether to record a scheduler/backoff write for this consumed
    # outcome. found/enqueued is True with `success=True`; no_match /
    # no_results / error is True with `success=False`. Caller decides.
    apply_scheduler_attempt: bool = False
    scheduler_success: bool = False


@dataclass(frozen=True)
class NonConsumingAttemptInput:
    """Input contract for ``record_non_consuming_search_attempt``.

    Pre-attempt / setup-failure rows. Plan context fields are optional
    because the failure may have happened before the executor resolved
    a plan/item -- but when known, capture them so dashboards can
    attribute the failure.
    """
    request_id: int
    outcome: str
    plan_id: int | None = None
    plan_item_id: int | None = None
    plan_ordinal: int | None = None
    plan_strategy: str | None = None
    plan_canonical_query_key: str | None = None
    plan_repeat_group: str | None = None
    plan_generator_id: str | None = None
    query: str | None = None
    result_count: int | None = None
    elapsed_s: float | None = None
    final_state: str | None = None
    error_message: str | None = None
    apply_scheduler_attempt: bool = True


@dataclass(frozen=True)
class ConsumedAttemptResult:
    """Outcome of ``record_consumed_search_attempt``.

    Tells the caller whether the cursor advanced, wrapped, or was treated
    as stale because the request had been regenerated mid-flight. The log
    row is always written (so usefulness stats stay complete).
    """
    search_log_id: int
    cursor_update_status: str
    new_next_ordinal: int
    new_cycle_count: int
    is_stale: bool


@dataclass(frozen=True)
class RequestV0ProbeStateUpdate:
    """Typed update for current comparable lossless-source V0 probe state."""

    current_lossless_source: V0ProbeEvidence | None = None
    clear_current_lossless_source: bool = False

    def as_update_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {}
        if self.clear_current_lossless_source:
            fields["current_lossless_source_v0_probe_min_bitrate"] = None
            fields["current_lossless_source_v0_probe_avg_bitrate"] = None
            fields["current_lossless_source_v0_probe_median_bitrate"] = None
        elif self.current_lossless_source is not None:
            fields["current_lossless_source_v0_probe_min_bitrate"] = (
                self.current_lossless_source.min_bitrate_kbps
            )
            fields["current_lossless_source_v0_probe_avg_bitrate"] = (
                self.current_lossless_source.avg_bitrate_kbps
            )
            fields["current_lossless_source_v0_probe_median_bitrate"] = (
                self.current_lossless_source.median_bitrate_kbps
            )
        return fields


# ---------------------------------------------------------------------
# Search-plan stats aggregation helpers (U8)
# ---------------------------------------------------------------------


def _percentile(values: list[float], pct: float) -> float | None:
    """Linear-interpolation percentile. Tiny + dependency-free.

    Returns None on empty input. The stats payload renders None as
    "n/a" so callers don't need a special-case.
    """
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])
    rank = (pct / 100.0) * (len(s) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(s) - 1)
    frac = rank - lo
    return float(s[lo] + (s[hi] - s[lo]) * frac)


def _mean(values: list[float]) -> float | None:
    return float(sum(values) / len(values)) if values else None


def _row_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _aggregate_group(
    rows: list[dict[str, Any]], identity: dict[str, object],
) -> "SearchPlanStatsGroup":
    """Aggregate one cohort of search_log rows into a stats group.

    Counts attempts/outcomes, computes mean/p95 elapsed, and means for
    result_count + browse/match/peers/fanout. Stale and pre-attempt
    rows are tracked separately so dashboards can distinguish "noisy
    setup failures" from "useful slot work".
    """
    elapsed_vals = [
        float(r["elapsed_s"]) for r in rows
        if r.get("elapsed_s") is not None
    ]
    result_counts = [
        float(r["result_count"]) for r in rows
        if r.get("result_count") is not None
    ]
    browse_vals = [
        float(r["browse_time_s"]) for r in rows
        if r.get("browse_time_s") is not None
    ]
    match_vals = [
        float(r["match_time_s"]) for r in rows
        if r.get("match_time_s") is not None
    ]
    peers_vals = [
        float(int(r.get("peers_browsed") or 0)
              + int(r.get("peers_browsed_lazy") or 0))
        for r in rows
    ]
    fanout_vals = [
        float(r["fanout_waves"]) for r in rows
        if r.get("fanout_waves") is not None
    ]
    outcome_counts: dict[str, int] = {}
    consumed = 0
    non_consuming = 0
    stale = 0
    last_seen: datetime | None = None
    for r in rows:
        outcome = r.get("outcome") or "unknown"
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        if r.get("attempt_consumed") is True:
            consumed += 1
        if r.get("attempt_consumed") is False:
            non_consuming += 1
        if r.get("cursor_update_status") == CURSOR_UPDATE_STALE:
            stale += 1
        ts = _row_dt(r.get("created_at"))
        if ts is not None and (last_seen is None or ts > last_seen):
            last_seen = ts
    return SearchPlanStatsGroup(
        identity=identity,
        attempts=len(rows),
        consumed_attempts=consumed,
        non_consuming_attempts=non_consuming,
        stale_completion_attempts=stale,
        outcome_counts=outcome_counts,
        elapsed_s_mean=_mean(elapsed_vals),
        elapsed_s_p95=_percentile(elapsed_vals, 95),
        result_count_mean=_mean(result_counts),
        browse_time_s_mean=_mean(browse_vals),
        match_time_s_mean=_mean(match_vals),
        peers_browsed_mean=_mean(peers_vals) if rows else None,
        fanout_waves_mean=_mean(fanout_vals),
        last_seen_at=last_seen,
    )


def _build_stats_bucket(
    *,
    plan_aware_rows: list[dict[str, Any]],
    legacy_rows: list[dict[str, Any]],
    include_legacy_bucket: bool,
) -> "SearchPlanStatsBucket":
    """Group plan-aware rows by slot + query-group, render legacy bucket.

    Slot grouping is sorted by (plan_id, ordinal); query-group
    grouping is sorted by (plan_id, repeat_group, canonical_query_key).
    Sort order is stable so dashboard rendering is deterministic.
    """
    slot_groups: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    qg_groups: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    for r in plan_aware_rows:
        slot_key = (
            r.get("plan_id"), r.get("plan_ordinal"), r.get("plan_strategy"),
        )
        slot_groups.setdefault(slot_key, []).append(r)
        qg_key = (
            r.get("plan_id"), r.get("plan_repeat_group"),
            r.get("plan_canonical_query_key"),
        )
        qg_groups.setdefault(qg_key, []).append(r)
    slots: list[SearchPlanStatsGroup] = []
    for key in sorted(
        slot_groups,
        key=lambda k: (
            k[0] if k[0] is not None else -1,
            k[1] if k[1] is not None else -1,
            k[2] or "",
        ),
    ):
        plan_id, ordinal, strategy = key
        slots.append(_aggregate_group(slot_groups[key], identity={
            "plan_id": plan_id, "ordinal": ordinal, "strategy": strategy,
        }))
    query_groups: list[SearchPlanStatsGroup] = []
    for key in sorted(
        qg_groups,
        key=lambda k: (
            k[0] if k[0] is not None else -1,
            k[1] or "",
            k[2] or "",
        ),
    ):
        plan_id, repeat_group, canonical_query_key = key
        query_groups.append(_aggregate_group(qg_groups[key], identity={
            "plan_id": plan_id, "repeat_group": repeat_group,
            "canonical_query_key": canonical_query_key,
        }))
    legacy_bucket: SearchPlanStatsGroup | None = None
    if include_legacy_bucket and legacy_rows:
        legacy_bucket = _aggregate_group(legacy_rows, identity={
            "kind": "legacy",
        })
    return SearchPlanStatsBucket(
        slots=slots,
        query_groups=query_groups,
        legacy_bucket=legacy_bucket,
        cache_attribution_level=CACHE_ATTRIBUTION_CYCLE_ONLY,
        cache_per_search_available=False,
    )


class PipelineDB:
    """PostgreSQL-backed pipeline database.

    Schema migrations are NOT this class's responsibility. They live in
    ``migrations/*.sql`` and are applied by ``lib.migrator.apply_migrations``,
    which the deploy systemd unit ``cratedigger-db-migrate.service`` runs on every
    ``nixos-rebuild switch``. Construct this class against an already-migrated
    database.
    """

    def __init__(self, dsn=None):
        self.dsn = dsn or DEFAULT_DSN
        self.conn = self._connect()

    def _connect(self):
        conn = psycopg2.connect(
            self.dsn,
            connect_timeout=10,
            options="-c statement_timeout=30000"
                    " -c tcp_keepalives_idle=60"
                    " -c tcp_keepalives_interval=10"
                    " -c tcp_keepalives_count=5",
        )
        conn.autocommit = True
        return conn

    def _ensure_conn(self):
        """Reconnect if the connection is dead."""
        if self.conn.closed:
            self.conn = self._connect()

    def close(self):
        self.conn.close()

    def _execute(self, sql, params=()):
        self._ensure_conn()
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur

    @contextmanager
    def advisory_lock(self, namespace: int, key: int) -> Iterator[bool]:
        """Try to acquire a session-level PostgreSQL advisory lock. Non-blocking.

        Yields ``True`` if acquired, ``False`` if another session already
        holds it. Always releases on ``__exit__`` when acquired.

        Used to serialise operations that must not run concurrently on the
        same ``(namespace, key)`` pair across different DB sessions — e.g.
        two ``pipeline-cli force-import`` invocations racing on the same
        ``request_id`` (issue #92). Advisory locks are reentrant within a
        single session, so this only protects against inter-session races;
        the web server (single-threaded ``HTTPServer``) already serialises
        within its own session.

        See ``docs/advisory-locks.md`` for namespaces, keys, ordering,
        and call-site index.
        """
        self._ensure_conn()
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (namespace, key))
            row = cur.fetchone()
        acquired = bool(row and row[0])
        try:
            yield acquired
        finally:
            if acquired:
                # Swallow unlock errors so they cannot mask the original
                # exception from the ``with`` body. PostgreSQL releases
                # session-level advisory locks on connection death anyway,
                # so a transient cursor/connection failure here cannot
                # leak the lock beyond the session.
                try:
                    with self.conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_unlock(%s, %s)",
                            (namespace, key),
                        )
                        cur.fetchone()
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "advisory_unlock(%s, %s) failed; lock will be "
                        "released at session end",
                        namespace, key,
                    )

    # --- import_jobs queue ---

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
        preview_enabled: bool | None = None,
    ) -> ImportJob:
        """Create an import job or return the active job with the same key."""
        validate_job_type(job_type)
        payload = validate_payload(job_type, payload or {})
        preview_enabled = (
            import_preview_enabled_from_env()
            if preview_enabled is None
            else preview_enabled
        )
        preview_status = (
            IMPORT_JOB_PREVIEW_WAITING
            if preview_enabled
            else IMPORT_JOB_PREVIEW_WOULD_IMPORT
        )
        preview_message = None if preview_enabled else IMPORT_JOB_PREVIEW_DISABLED_MESSAGE
        preview_completed_at = None if preview_enabled else datetime.now(timezone.utc)
        importable_at = None if preview_enabled else preview_completed_at
        cur = self._execute("""
            WITH inserted AS (
                INSERT INTO import_jobs (
                    job_type, request_id, dedupe_key, payload, message,
                    preview_status, preview_message, preview_completed_at,
                    importable_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dedupe_key)
                    WHERE dedupe_key IS NOT NULL
                      AND status IN ('queued', 'running')
                DO NOTHING
                RETURNING *
            )
            SELECT inserted.*, false AS deduped
            FROM inserted
            UNION ALL
            SELECT import_jobs.*, true AS deduped
            FROM import_jobs
            WHERE %s IS NOT NULL
              AND dedupe_key = %s
              AND status IN ('queued', 'running')
              AND NOT EXISTS (SELECT 1 FROM inserted)
            ORDER BY deduped
            LIMIT 1
        """, (
            job_type,
            request_id,
            dedupe_key,
            psycopg2.extras.Json(payload),
            message,
            preview_status,
            preview_message,
            preview_completed_at,
            importable_at,
            dedupe_key,
            dedupe_key,
        ))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("import job enqueue returned no row")
        return ImportJob.from_row(dict(row), deduped=bool(row["deduped"]))

    def get_import_job(self, job_id: int) -> ImportJob | None:
        cur = self._execute(
            "SELECT * FROM import_jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def get_import_job_by_dedupe_key(
        self,
        dedupe_key: str,
        *,
        active_only: bool = True,
    ) -> ImportJob | None:
        status_filter = (
            "AND status IN ('queued', 'running')"
            if active_only
            else ""
        )
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            WHERE dedupe_key = %s
            {status_filter}
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
        """, (dedupe_key,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def list_import_jobs(
        self,
        *,
        status: str | None = None,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        params: list[Any] = []
        clauses: list[str] = []
        if status is not None:
            validate_status(status)
            clauses.append("status = %s")
            params.append(status)
        if request_id is not None:
            clauses.append("request_id = %s")
            params.append(request_id)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(limit)
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            {where}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
        """, tuple(params))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def list_active_import_jobs(
        self,
        *,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        params: list[Any] = []
        request_filter = ""
        if request_id is not None:
            request_filter = "AND request_id = %s"
            params.append(request_id)
        params.append(limit)
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            WHERE status IN ('queued', 'running')
            {request_filter}
            ORDER BY created_at ASC, id ASC
            LIMIT %s
        """, tuple(params))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def count_import_jobs_by_status(self) -> dict[str, int]:
        cur = self._execute("""
            SELECT status, COUNT(*) AS count
            FROM import_jobs
            GROUP BY status
        """)
        return {str(row["status"]): int(row["count"]) for row in cur.fetchall()}

    def list_import_job_timeline(self, *, limit: int = 50) -> list[ImportJob]:
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE status IN ('queued', 'running')
            ORDER BY
              CASE
                WHEN status = 'queued' AND preview_status = 'would_import' THEN 0
                WHEN status = 'running' THEN 1
                WHEN status = 'queued' AND preview_status = 'running' THEN 2
                WHEN status = 'queued' AND preview_status = 'waiting' THEN 3
                ELSE 4
              END,
              CASE
                WHEN status = 'queued' THEN importable_at
              END ASC NULLS LAST,
              created_at ASC,
              id ASC
            LIMIT %s
        """, (limit,))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def claim_next_import_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            WITH next_job AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'would_import'
                ORDER BY importable_at ASC NULLS LAST, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE import_jobs
            SET status = 'running',
                attempts = attempts + 1,
                worker_id = %s,
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                updated_at = NOW()
            FROM next_job
            WHERE import_jobs.id = next_job.id
            RETURNING import_jobs.*
        """, (worker_id,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def heartbeat_import_job(self, job_id: int) -> bool:
        cur = self._execute("""
            UPDATE import_jobs
            SET heartbeat_at = NOW(), updated_at = NOW()
            WHERE id = %s AND status = 'running'
            RETURNING id
        """, (job_id,))
        return cur.fetchone() is not None

    def mark_import_job_completed(
        self,
        job_id: int,
        *,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'completed',
                result = %s,
                message = %s,
                error = NULL,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('queued', 'running')
            RETURNING *
        """, (psycopg2.extras.Json(result or {}), message, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def mark_import_job_failed(
        self,
        job_id: int,
        *,
        error: str,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'failed',
                result = %s,
                message = %s,
                error = %s,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('queued', 'running')
            RETURNING *
        """, (psycopg2.extras.Json(result or {}), message, error, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def list_stale_running_import_jobs(
        self,
        *,
        older_than: timedelta,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE status = 'running'
              AND COALESCE(heartbeat_at, started_at, updated_at) < %s
            ORDER BY updated_at ASC, id ASC
            LIMIT %s
        """, (cutoff, limit))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def fail_stale_running_import_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            WITH stale AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'running'
                  AND COALESCE(heartbeat_at, started_at, updated_at) < %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET status = 'failed',
                error = %s,
                message = %s,
                completed_at = NOW(),
                updated_at = NOW()
            FROM stale
            WHERE import_jobs.id = stale.id
            RETURNING import_jobs.*
        """, (cutoff, limit, message, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def requeue_running_import_jobs(
        self,
        *,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        """Reset abandoned running jobs to queued for immediate retry."""
        cur = self._execute("""
            WITH running AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'running'
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET status = 'queued',
                message = %s,
                error = NULL,
                worker_id = NULL,
                started_at = NULL,
                heartbeat_at = NULL,
                updated_at = NOW()
            FROM running
            WHERE import_jobs.id = running.id
            RETURNING import_jobs.*
        """, (limit, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def claim_next_import_preview_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            WITH next_job AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'waiting'
                ORDER BY created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE import_jobs
            SET preview_status = 'running',
                preview_attempts = preview_attempts + 1,
                preview_worker_id = %s,
                preview_started_at = COALESCE(preview_started_at, NOW()),
                preview_heartbeat_at = NOW(),
                preview_message = NULL,
                preview_error = NULL,
                updated_at = NOW()
            FROM next_job
            WHERE import_jobs.id = next_job.id
            RETURNING import_jobs.*
        """, (worker_id,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def heartbeat_import_job_preview(self, job_id: int) -> bool:
        cur = self._execute("""
            UPDATE import_jobs
            SET preview_heartbeat_at = NOW(), updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status = 'running'
            RETURNING id
        """, (job_id,))
        return cur.fetchone() is not None

    def mark_import_job_preview_importable(
        self,
        job_id: int,
        *,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET preview_status = 'would_import',
                preview_result = %s,
                preview_message = %s,
                preview_error = NULL,
                preview_completed_at = NOW(),
                importable_at = COALESCE(importable_at, NOW()),
                preview_worker_id = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status IN ('waiting', 'running')
            RETURNING *
        """, (
            psycopg2.extras.Json(preview_result or {}),
            message,
            job_id,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def mark_import_job_preview_failed(
        self,
        job_id: int,
        *,
        preview_status: str,
        error: str,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        validate_preview_failure_status(preview_status)
        result = dict(preview_result or {})
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'failed',
                preview_status = %s,
                preview_result = %s,
                preview_message = %s,
                preview_error = %s,
                result = %s,
                message = %s,
                error = %s,
                preview_completed_at = NOW(),
                completed_at = NOW(),
                preview_worker_id = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status IN ('waiting', 'running')
            RETURNING *
        """, (
            preview_status,
            psycopg2.extras.Json(result),
            message,
            error,
            psycopg2.extras.Json({"preview": result}),
            message,
            error,
            job_id,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def list_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE status = 'queued'
              AND preview_status = 'running'
              AND COALESCE(preview_heartbeat_at, preview_started_at, updated_at) < %s
            ORDER BY updated_at ASC, id ASC
            LIMIT %s
        """, (cutoff, limit))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def requeue_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            WITH stale AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'running'
                  AND COALESCE(preview_heartbeat_at, preview_started_at, updated_at) < %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET preview_status = 'waiting',
                preview_message = %s,
                preview_error = NULL,
                preview_worker_id = NULL,
                preview_started_at = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            FROM stale
            WHERE import_jobs.id = stale.id
            RETURNING import_jobs.*
        """, (cutoff, limit, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    # --- album_requests CRUD ---

    def add_request(self, artist_name, album_title, source,
                    mb_release_id=None, mb_release_group_id=None,
                    mb_artist_id=None, discogs_release_id=None,
                    year=None, country=None, format=None,
                    source_path=None, reasoning=None,
                    status="wanted"):
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            INSERT INTO album_requests (
                mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
                artist_name, album_title, year, country, format,
                source, source_path, reasoning, status,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
            artist_name, album_title, year, country, format,
            source, source_path, reasoning, status,
            now, now,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return row["id"]

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE id = %s", (request_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_request_by_mb_release_id(self, mb_release_id) -> dict[str, Any] | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE mb_release_id = %s", (mb_release_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_request_by_discogs_release_id(self, discogs_release_id: str) -> dict[str, Any] | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE discogs_release_id = %s", (discogs_release_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_request_by_release_id(self, release_id: object | None) -> dict[str, Any] | None:
        """Resolve a pipeline row through the shared exact-release seam.

        - MB UUIDs query ``mb_release_id``.
        - Discogs numerics prefer ``discogs_release_id`` and then fall back to
          ``mb_release_id`` for legacy rows that stored the numeric there.
        - Unknown non-empty strings fall back to ``mb_release_id`` so tests and
          synthetic/manual fixture IDs still round-trip without special casing.
        """
        normalized = normalize_release_id(release_id)
        if not normalized:
            return None

        identity = ReleaseIdentity.from_fields(normalized)
        if identity is None:
            return self.get_request_by_mb_release_id(normalized)

        if identity.source == "musicbrainz":
            return self.get_request_by_mb_release_id(identity.release_id)

        req = self.get_request_by_discogs_release_id(identity.release_id)
        if req:
            return req
        return self.get_request_by_mb_release_id(identity.release_id)

    def delete_request(self, request_id: int) -> None:
        self._execute("DELETE FROM album_requests WHERE id = %s", (request_id,))
        self.conn.commit()

    def update_request_fields(self, request_id: int, **extra: Any) -> None:
        """Update album_requests metadata without changing status."""
        if not extra:
            return
        now = datetime.now(timezone.utc)
        sets = ["updated_at = %s"]
        params: list[object] = [now]
        for key, val in extra.items():
            sets.append(f"{key} = %s")
            params.append(val)
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def update_status(self, request_id, status, **extra):
        now = datetime.now(timezone.utc)
        sets = ["status = %s", "active_download_state = NULL", "updated_at = %s"]
        params = [status, now]
        for key, val in extra.items():
            sets.append(f"{key} = %s")
            params.append(val)
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def set_manual(
        self,
        request_id: int,
        *,
        manual_reason: str | None = None,
    ) -> None:
        """Flip a request to ``status='manual'``, optionally writing a reason.

        - ``manual_reason`` is a system-driven cause string (e.g.
          ``'search_exhausted'``). When non-None, it is written to the
          new ``album_requests.manual_reason`` column.
        - When ``manual_reason`` is None (the default), the column is left
          untouched — never overwritten with NULL. This protects an
          existing reason when a generic flip path runs against a row
          that already has a populated reason.

        Re-queue is the only path that clears ``manual_reason``; that
        clearing happens in ``reset_to_wanted``.
        """
        now = datetime.now(timezone.utc)
        sets = [
            "status = 'manual'",
            "active_download_state = NULL",
            "updated_at = %s",
        ]
        params: list[object] = [now]
        if manual_reason is not None:
            sets.append("manual_reason = %s")
            params.append(manual_reason)
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def update_spectral_state(
        self,
        request_id: int,
        update: RequestSpectralStateUpdate,
    ) -> None:
        """Write spectral state pairs together, including explicit NULLs."""
        self.update_request_fields(request_id, **update.as_update_fields())

    def update_v0_probe_state(
        self,
        request_id: int,
        update: RequestV0ProbeStateUpdate,
    ) -> None:
        """Write current comparable source-probe state together."""
        self.update_request_fields(request_id, **update.as_update_fields())

    def clear_on_disk_quality_fields(self, request_id: int) -> None:
        """Zero fields that describe files currently on disk in beets.

        Call this whenever an album leaves the beets library — ban-source
        followed by ``beet remove -d``, a manual ``beet rm``, etc. The
        fields cleared describe on-disk state:

        - ``verified_lossless`` (set only after a genuine FLAC→V0 chain)
        - ``current_spectral_*`` (spectral grade of files currently in
          beets)
        - ``imported_path`` (beets filesystem path for the release, shown
          directly by the web UI — leaving it populated after a remove
          means the pipeline tab still claims the album is imported at a
          path that has just been deleted)

        ``min_bitrate`` and ``prev_min_bitrate`` are preserved deliberately
        — they still act as a conservative baseline for the next quality-
        gate comparison. ``last_download_spectral_*`` is also preserved:
        that's an audit field describing the most recent download attempt,
        independent of whether the result made it onto disk.
        """
        now = datetime.now(timezone.utc)
        self._execute(
            """UPDATE album_requests SET
                   verified_lossless = FALSE,
                   current_spectral_grade = NULL,
                   current_spectral_bitrate = NULL,
                   current_lossless_source_v0_probe_min_bitrate = NULL,
                   current_lossless_source_v0_probe_avg_bitrate = NULL,
                   current_lossless_source_v0_probe_median_bitrate = NULL,
                   imported_path = NULL,
                   updated_at = %s
               WHERE id = %s""",
            (now, request_id),
        )
        self.conn.commit()

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> None:
        """Reset to wanted.

        Only fields explicitly passed are updated — omitted fields are
        preserved.  Pass ``search_filetype_override=None`` to clear the column;
        omitting it leaves the existing value untouched.

        ``clear_retry_counters`` is for operator/manual requeues that should get
        a clean slate. Automatic downloading → wanted failure paths preserve the
        counters so backoff can keep growing and the picker does not treat the
        row as brand new.

        Always clears ``manual_reason`` — re-queueing past a manual flip
        means the operator wants a clean slate. Per U6: every re-queue path
        funnels through this method, so a single ``manual_reason = NULL``
        write here covers web UI, CLI, and importer requeue paths.
        """
        now = datetime.now(timezone.utc)
        sets = [
            "status = 'wanted'",
            "active_download_state = NULL",
            "manual_reason = NULL",
            "updated_at = %s",
        ]
        params: list[object] = [now]
        if clear_retry_counters:
            sets.extend([
                "search_attempts = 0",
                "download_attempts = 0",
                "validation_attempts = 0",
                "next_retry_after = NULL",
                "last_attempt_at = NULL",
            ])
        if "search_filetype_override" in fields:
            sets.append("search_filetype_override = %s")
            params.append(fields["search_filetype_override"])
        if "min_bitrate" in fields:
            sets.append("prev_min_bitrate = COALESCE(min_bitrate, prev_min_bitrate)")
            sets.append("min_bitrate = %s")
            params.append(fields["min_bitrate"])
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        **fields: Any,
    ) -> bool:
        """Reset a still-downloading request to wanted.

        This is the guarded automatic failure path: stale workers must not
        requeue rows that an operator or another phase already moved elsewhere.
        Retry counters are preserved so automatic backoff keeps growing.
        """
        now = datetime.now(timezone.utc)
        sets = [
            "status = 'wanted'",
            "active_download_state = NULL",
            "manual_reason = NULL",
            "updated_at = %s",
        ]
        params: list[object] = [now]
        if "search_filetype_override" in fields:
            sets.append("search_filetype_override = %s")
            params.append(fields["search_filetype_override"])
        if "min_bitrate" in fields:
            sets.append("prev_min_bitrate = COALESCE(min_bitrate, prev_min_bitrate)")
            sets.append("min_bitrate = %s")
            params.append(fields["min_bitrate"])
        params.append(request_id)
        cur = self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} "
            "WHERE id = %s AND status = 'downloading'",
            params,
        )
        self.conn.commit()
        return cur.rowcount > 0

    def reset_search_attempts(self, request_id: int) -> None:
        """Reset ``search_attempts`` to 0; leave status/backoff/other counters alone.

        Used by the variant-ladder exhaustion path: when the V4 token pool
        runs out, the request stays ``wanted`` and the ladder wraps back to
        the default query. The standard ``next_retry_after`` cooldown still
        governs when the next cycle picks it up.
        """
        now = datetime.now(timezone.utc)
        self._execute(
            "UPDATE album_requests SET search_attempts = 0, updated_at = %s WHERE id = %s",
            (now, request_id),
        )
        self.conn.commit()

    def update_imported_path_by_release_id(
        self,
        *,
        mb_albumid: str,
        discogs_albumid: str,
        new_path: str,
    ) -> int:
        """Update ``imported_path`` for any request whose release id matches.

        Issue #132 P2 / issue #133: when sibling canonicalization in
        the harness moves a sibling's files on disk (e.g. from
        ``/Beets/Shearwater/2006 - Palo Santo/`` to ``…/2006 - Palo
        Santo [2006]/`` after ``%aunique`` re-evaluates because a new
        same-name edition was just imported), the sibling might itself
        be a tracked pipeline request — in which case its
        ``album_requests.imported_path`` column is now stale. The UI
        ("Imported to" label, ban-source button) would point at a
        directory that no longer exists.

        This method finds the tracked request across both layout combos
        (MB-sourced: ``mb_release_id=<mbid>``; Discogs-sourced:
        ``discogs_release_id=<numeric>`` and/or ``mb_release_id=<numeric>``
        for legacy pre-plugin-patch imports) and updates its
        ``imported_path``. Callers pass the two beets-side columns as
        two arguments; either may be the empty string. No-op if neither
        is populated.

        Returns the number of rows updated (usually 0 or 1). A duplicate
        request for the same release in the pipeline DB would return
        more — that's the caller's signal that data is inconsistent
        (the ``UNIQUE`` constraint on ``mb_release_id`` makes duplicate
        MBIDs impossible in practice).
        """
        if not mb_albumid and not discogs_albumid:
            return 0
        now = datetime.now(timezone.utc)
        clauses: list[str] = []
        params: list[object] = [new_path, now]
        if mb_albumid:
            # Beets-side ``mb_albumid`` is either a MB UUID (stored
            # in pipeline's ``mb_release_id``) or a legacy numeric
            # (also stored in ``mb_release_id`` — the pre-plugin-patch
            # layout). Either way the single-column match covers it.
            clauses.append("mb_release_id = %s")
            params.append(mb_albumid)
        if discogs_albumid:
            # Beets-side ``discogs_albumid`` is always numeric. The
            # pipeline side could store the same numeric in EITHER
            # ``discogs_release_id`` (rows added through the web UI
            # after the discogs-plugin integration) OR
            # ``mb_release_id`` (legacy "pipeline compat" convention
            # documented in CLAUDE.md § "Discogs-sourced albums":
            # *Numeric IDs stored in ``mb_release_id`` for pipeline
            # compat*). Match both columns so a sibling whose beets
            # row carries only ``discogs_albumid`` still finds its
            # tracked request regardless of which pipeline layout
            # that request was created under. Codex R2 P2.
            clauses.append(
                "(mb_release_id = %s OR discogs_release_id = %s)")
            params.append(discogs_albumid)
            params.append(discogs_albumid)
        where = " OR ".join(clauses)
        cur = self._execute(
            f"UPDATE album_requests SET imported_path = %s, "
            f"updated_at = %s WHERE {where}",
            tuple(params),
        )
        self.conn.commit()
        return cur.rowcount

    # --- Downloading state ---

    def set_downloading(self, request_id: int, state_json: str) -> bool:
        """Set album to downloading and store the active download state.

        Only transitions from 'wanted' status. Returns True if the update
        matched (album was wanted), False if the status guard prevented it.
        """
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET status = 'downloading',
                active_download_state = %s::jsonb,
                last_attempt_at = %s,
                updated_at = %s
            WHERE id = %s AND status = 'wanted'
        """, (state_json, now, now, request_id))
        self.conn.commit()
        return cur.rowcount > 0

    def set_downloading_if_plan_current(
        self,
        request_id: int,
        state_json: str,
        *,
        plan_id: int,
        plan_ordinal: int,
        cycle_count_snapshot: int,
    ) -> bool:
        """Atomic plan-aware ``set_downloading`` for stale-completion guard.

        Equivalent to ``set_downloading`` but additionally requires the
        request's ``active_plan_id`` / ``next_plan_ordinal`` /
        ``plan_cycle_count`` to still match the snapshot the executor
        captured at search-submit time. The single UPDATE eliminates the
        TOCTOU window between a separate currentness check and the
        wanted->downloading flip.

        Returns True iff the UPDATE matched and downloading was claimed.
        Returns False on any of: status no longer 'wanted', plan
        regenerated (active_plan_id mismatch), cursor advanced (ordinal
        mismatch), cycle bumped (cycle_count mismatch).
        """
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET status = 'downloading',
                active_download_state = %s::jsonb,
                last_attempt_at = %s,
                updated_at = %s
            WHERE id = %s
              AND status = 'wanted'
              AND active_plan_id = %s
              AND next_plan_ordinal = %s
              AND plan_cycle_count = %s
        """, (
            state_json, now, now, request_id,
            plan_id, plan_ordinal, cycle_count_snapshot,
        ))
        self.conn.commit()
        return cur.rowcount > 0

    def update_download_state(self, request_id: int, state_json: str) -> None:
        """Rewrite active_download_state without changing status or attempt counters."""
        now = datetime.now(timezone.utc)
        self._execute("""
            UPDATE album_requests
            SET active_download_state = %s::jsonb,
                updated_at = %s
            WHERE id = %s
        """, (state_json, now, request_id))
        self.conn.commit()

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
    ) -> bool:
        """Rewrite active_download_state only while the request is downloading."""
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET active_download_state = %s::jsonb,
                updated_at = %s
            WHERE id = %s
              AND status = 'downloading'
        """, (state_json, now, request_id))
        self.conn.commit()
        return cur.rowcount > 0

    def update_download_state_current_path(
        self,
        request_id: int,
        current_path: str | None,
    ) -> None:
        """Rewrite only ``active_download_state.current_path`` on downloading rows."""
        now = datetime.now(timezone.utc)
        self._execute("""
            UPDATE album_requests
            SET active_download_state = jsonb_set(
                    COALESCE(active_download_state, '{}'::jsonb),
                    '{current_path}',
                    to_jsonb(%s::text),
                    true
                ),
                updated_at = %s
            WHERE id = %s
              AND status = 'downloading'
              AND active_download_state IS NOT NULL
        """, (current_path, now, request_id))
        self.conn.commit()

    def mark_import_subprocess_started(
        self,
        request_id: int,
        timestamp: str,
    ) -> None:
        """Stamp ``active_download_state.import_subprocess_started_at``.

        Called immediately before launching ``import_one.py`` on the
        auto-import path so the resume guard can later distinguish
        "subprocess never launched" (safe to retry) from "subprocess
        may have written to beets" (manual recovery required). No-op
        when ``active_download_state`` is NULL — force/manual paths
        operate on a different ownership boundary
        (``failed_imports/...``) and don't carry this state.
        See ``docs/advisory-locks.md``.
        """
        now = datetime.now(timezone.utc)
        self._execute("""
            UPDATE album_requests
            SET active_download_state = jsonb_set(
                    active_download_state,
                    '{import_subprocess_started_at}',
                    to_jsonb(%s::text),
                    true
                ),
                updated_at = %s
            WHERE id = %s
              AND active_download_state IS NOT NULL
        """, (timestamp, now, request_id))
        self.conn.commit()

    def get_downloading(self) -> list[dict[str, Any]]:
        """Get all albums currently being downloaded."""
        cur = self._execute(
            "SELECT * FROM album_requests WHERE status = 'downloading' "
            "ORDER BY updated_at ASC"
        )
        return [dict(r) for r in cur.fetchall()]

    def clear_download_state(self, request_id: int) -> None:
        """Clear active_download_state when download completes/fails."""
        now = datetime.now(timezone.utc)
        self._execute("""
            UPDATE album_requests
            SET active_download_state = NULL,
                updated_at = %s
            WHERE id = %s
        """, (now, request_id))
        self.conn.commit()

    # --- Query methods ---

    def get_wanted(self, limit=None):
        now = datetime.now(timezone.utc)
        # New/manual-requeued albums go first, then random.
        # This ensures freshly added or upgrade-requeued albums get picked
        # up on the next cycle instead of waiting for random selection, while
        # automatic failed-download requeues stay in the normal random pool.
        sql = """
            SELECT * FROM album_requests
            WHERE status = 'wanted'
              AND (next_retry_after IS NULL OR next_retry_after <= %s)
            ORDER BY
              CASE
                WHEN COALESCE(search_attempts, 0) = 0
                 AND COALESCE(download_attempts, 0) = 0
                 AND COALESCE(validation_attempts, 0) = 0
                THEN 0
                ELSE 1
              END,
              RANDOM()
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur = self._execute(sql, (now,))
        return [dict(r) for r in cur.fetchall()]

    def get_log(self, limit: int = 50,
                outcome_filter: str | None = None) -> list[dict[str, object]]:
        """Get recent download_log entries joined with album_requests.

        Args:
            limit: max entries to return
            outcome_filter: "imported" (success + force_import),
                           "rejected" (rejected + failed + timeout),
                           or None for all
        """
        base = """
            SELECT dl.*,
                   ar.album_title, ar.artist_name, ar.mb_release_id,
                   ar.year, ar.country, ar.status AS request_status,
                   ar.min_bitrate AS request_min_bitrate,
                   ar.prev_min_bitrate, ar.search_filetype_override, ar.source
            FROM download_log dl
            JOIN album_requests ar ON dl.request_id = ar.id
        """
        if outcome_filter == "imported":
            base += " WHERE dl.outcome IN ('success', 'force_import')"
        elif outcome_filter == "rejected":
            base += " WHERE dl.outcome IN ('rejected', 'failed', 'timeout')"
        base += " ORDER BY dl.created_at DESC LIMIT %s"
        cur = self._execute(base, (limit,))
        return [dict(r) for r in cur.fetchall()]

    def get_by_status(self, status):
        cur = self._execute(
            "SELECT * FROM album_requests WHERE status = %s ORDER BY created_at ASC",
            (status,),
        )
        return [dict(r) for r in cur.fetchall()]

    def get_recent(self, limit=20):
        """Get recently downloaded/imported albums (must have download history)."""
        cur = self._execute(
            "SELECT ar.* FROM album_requests ar "
            "WHERE EXISTS (SELECT 1 FROM download_log dl WHERE dl.request_id = ar.id) "
            "ORDER BY ar.updated_at DESC LIMIT %s",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]

    def count_by_status(self):
        cur = self._execute(
            "SELECT status, COUNT(*) as cnt FROM album_requests GROUP BY status"
        )
        return {r["status"]: r["cnt"] for r in cur.fetchall()}

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, Any]]:
        """List request rows for one artist, including legacy name fallbacks.

        ``/api/library/artist`` is the SSOT view for albums already in
        beets and albums still wanted in beets. Prefer exact
        ``mb_artist_id`` matches when available, but keep the legacy
        name fallback for older pipeline rows that predate artist-id
        population or store a non-MB value there.
        """
        # Pair with `ESCAPE '\'` below so literal `%` / `_` in artist names
        # do not expand into wildcard matches on PostgreSQL.
        name_pattern = f"%{_escape_like_pattern(artist_name.strip())}%"
        if mb_artist_id:
            cur = self._execute(
                """
                SELECT *
                FROM album_requests
                WHERE mb_artist_id = %s
                   OR (artist_name ILIKE %s ESCAPE '\\'
                       -- Hyphen-free ids (e.g. legacy numerics / Discogs ids)
                       -- deliberately fall back to the artist-name match.
                       AND (mb_artist_id IS NULL OR mb_artist_id = ''
                            OR mb_artist_id NOT LIKE '%%-%%'))
                ORDER BY year, album_title
                """,
                (mb_artist_id, name_pattern),
            )
        else:
            cur = self._execute(
                """
                SELECT *
                FROM album_requests
                WHERE artist_name ILIKE %s ESCAPE '\\'
                ORDER BY year, album_title
                """,
                (name_pattern,),
            )
        return [dict(r) for r in cur.fetchall()]

    # --- Track management ---

    def set_tracks(self, request_id, tracks):
        self._execute("DELETE FROM album_tracks WHERE request_id = %s", (request_id,))
        for t in tracks:
            self._execute("""
                INSERT INTO album_tracks (request_id, disc_number, track_number, title, length_seconds)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                request_id,
                t.get("disc_number", 1),
                t["track_number"],
                t["title"],
                t.get("length_seconds"),
            ))
        self.conn.commit()

    def get_tracks(self, request_id):
        cur = self._execute("""
            SELECT disc_number, track_number, title, length_seconds
            FROM album_tracks
            WHERE request_id = %s
            ORDER BY disc_number, track_number
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    # --- Download logging ---

    def log_download(self, request_id, soulseek_username=None, filetype=None,
                     download_path=None, beets_distance=None, beets_scenario=None,
                     beets_detail=None, valid=None, outcome=None,
                     staged_path=None, error_message=None,
                     bitrate=None, sample_rate=None, bit_depth=None,
                     is_vbr=None, was_converted=None, original_filetype=None,
                     # Spectral quality verification fields
                     slskd_filetype=None, slskd_bitrate=None,
                     actual_filetype=None, actual_min_bitrate=None,
                     spectral_grade=None, spectral_bitrate=None,
                     existing_min_bitrate=None, existing_spectral_bitrate=None,
                     # Full import_one.py result (JSON string)
                     import_result=None,
                     # Full validation result (JSON string)
                     validation_result=None,
                     # Final format on disk
                     final_format=None,
                     v0_probe_kind=None, v0_probe_min_bitrate=None,
                     v0_probe_avg_bitrate=None,
                     v0_probe_median_bitrate=None,
                     existing_v0_probe_kind=None,
                     existing_v0_probe_min_bitrate=None,
                     existing_v0_probe_avg_bitrate=None,
                     existing_v0_probe_median_bitrate=None):
        cur = self._execute("""
            INSERT INTO download_log (
                request_id, soulseek_username, filetype, download_path,
                beets_distance, beets_scenario, beets_detail, valid,
                outcome, staged_path, error_message,
                bitrate, sample_rate, bit_depth, is_vbr,
                was_converted, original_filetype,
                slskd_filetype, slskd_bitrate,
                actual_filetype, actual_min_bitrate,
                spectral_grade, spectral_bitrate,
                existing_min_bitrate, existing_spectral_bitrate,
                import_result, validation_result, final_format,
                v0_probe_kind, v0_probe_min_bitrate,
                v0_probe_avg_bitrate, v0_probe_median_bitrate,
                existing_v0_probe_kind, existing_v0_probe_min_bitrate,
                existing_v0_probe_avg_bitrate, existing_v0_probe_median_bitrate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            request_id, soulseek_username, filetype, download_path,
            beets_distance, beets_scenario, beets_detail, valid,
            outcome, staged_path, error_message,
            bitrate, sample_rate, bit_depth, is_vbr,
            was_converted, original_filetype,
            slskd_filetype, slskd_bitrate,
            actual_filetype, actual_min_bitrate,
            spectral_grade, spectral_bitrate,
            existing_min_bitrate, existing_spectral_bitrate,
            import_result, validation_result, final_format,
            v0_probe_kind, v0_probe_min_bitrate,
            v0_probe_avg_bitrate, v0_probe_median_bitrate,
            existing_v0_probe_kind, existing_v0_probe_min_bitrate,
            existing_v0_probe_avg_bitrate, existing_v0_probe_median_bitrate,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])

    def abandon_auto_import_request(
        self,
        *,
        request_id: int,
        current_path: str,
        soulseek_username: str | None,
        filetype: str | None,
        beets_scenario: str,
        beets_detail: str,
        outcome: str,
        staged_path: str,
        error_message: str,
        validation_result: str | None,
    ) -> int | None:
        """Atomically audit and reset an owned interrupted auto-import row."""
        self._ensure_conn()
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    """
                    UPDATE album_requests
                    SET status = 'wanted',
                        active_download_state = NULL,
                        manual_reason = NULL,
                        updated_at = %s
                    WHERE id = %s
                      AND status = 'downloading'
                      AND active_download_state IS NOT NULL
                      AND active_download_state->>'current_path' = %s
                      AND active_download_state->>'import_subprocess_started_at'
                          IS NOT NULL
                    RETURNING id
                    """,
                    (now, request_id, current_path),
                )
                if cur.fetchone() is None:
                    self.conn.rollback()
                    return None

                cur.execute(
                    """
                    UPDATE album_requests
                    SET download_attempts = COALESCE(download_attempts, 0) + 1,
                        last_attempt_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    RETURNING download_attempts
                    """,
                    (now, now, request_id),
                )
                attempt_row = cur.fetchone()
                assert attempt_row is not None, f"Request {request_id} not found"
                new_count = int(attempt_row["download_attempts"])
                backoff_minutes = min(
                    BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                    BACKOFF_MAX_MINUTES,
                )
                cur.execute(
                    """
                    UPDATE album_requests
                    SET next_retry_after = %s
                    WHERE id = %s
                    """,
                    (now + timedelta(minutes=backoff_minutes), request_id),
                )

                cur.execute(
                    """
                    INSERT INTO download_log (
                        request_id, soulseek_username, filetype,
                        beets_scenario, beets_detail, outcome,
                        staged_path, error_message, validation_result
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        request_id,
                        soulseek_username,
                        filetype,
                        beets_scenario,
                        beets_detail,
                        outcome,
                        staged_path,
                        error_message,
                        validation_result,
                    ),
                )
                log_row = cur.fetchone()
                assert log_row is not None, "INSERT RETURNING should return a row"
                log_id = int(log_row["id"])
            self.conn.commit()
            return log_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    def get_download_log_entry(self, log_id):
        """Get a single download_log entry by its ID."""
        cur = self._execute(
            "SELECT * FROM download_log WHERE id = %s", (log_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_download_history(self, request_id):
        cur = self._execute("""
            SELECT * FROM download_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_download_history_batch(self, request_ids: list[int]) -> dict[int, list[dict]]:
        """Batch fetch download history for multiple request IDs.

        Returns dict of request_id → list of history rows (most recent first).
        """
        if not request_ids:
            return {}
        ph = ",".join(["%s"] * len(request_ids))
        cur = self._execute(
            f"SELECT * FROM download_log WHERE request_id IN ({ph}) ORDER BY id DESC",
            tuple(request_ids),
        )
        result: dict[int, list[dict]] = {}
        for row in cur.fetchall():
            r = dict(row)
            rid = r["request_id"]
            if rid not in result:
                result[rid] = []
            result[rid].append(r)
        return result

    # -- Wrong matches ---------------------------------------------------------

    def get_wrong_matches(self) -> list[dict[str, object]]:
        """Return every rejected wrong-match candidate still on disk.

        Issue #113: one row per actionable folder, not one per request.
        ``download_log`` accumulates multiple rejected rows for the same
        ``failed_path`` whenever a folder is retried (force/manual paths log
        the same ``failed_path`` on every retry), so we collapse to the newest
        row per ``(request_id, failed_path)`` pair — each surviving row
        represents a distinct on-disk directory the user can act on.

        Only wrong-match rejections survive — ``audio_corrupt`` /
        ``spectral_reject`` scenarios have their own handling and stay out of
        the manual-review queue.
        """
        cur = self._execute("""
            SELECT DISTINCT ON (dl.request_id, dl.validation_result->>'failed_path')
                dl.id AS download_log_id,
                dl.request_id,
                ar.artist_name,
                ar.album_title,
                ar.mb_release_id,
                dl.soulseek_username,
                dl.validation_result,
                dl.spectral_grade,
                dl.spectral_bitrate,
                dl.v0_probe_kind,
                dl.v0_probe_avg_bitrate,
                ar.status AS request_status,
                ar.min_bitrate AS request_min_bitrate,
                ar.verified_lossless AS request_verified_lossless,
                ar.current_spectral_grade AS request_current_spectral_grade,
                ar.current_spectral_bitrate AS request_current_spectral_bitrate,
                ar.imported_path AS request_imported_path
            FROM download_log dl
            JOIN album_requests ar ON dl.request_id = ar.id
            WHERE dl.outcome = 'rejected'
              AND dl.validation_result->>'failed_path' IS NOT NULL
              AND (dl.validation_result->>'scenario' IS NULL
                   OR dl.validation_result->>'scenario' NOT IN ('audio_corrupt', 'spectral_reject'))
            ORDER BY dl.request_id, dl.validation_result->>'failed_path', dl.id DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        # DISTINCT ON sorts by path within a request; re-sort so the route
        # layer sees newest-first within each request, matching the frontend
        # expectation that the most-recent candidate appears first.
        rows.sort(key=lambda r: (r["request_id"], -int(r["download_log_id"])))
        return rows

    def clear_wrong_match_path(self, log_id: int) -> bool:
        """Null out failed_path in validation_result for a download_log entry.

        Returns True if the entry was found and updated.
        """
        cur = self._execute("""
            UPDATE download_log
            SET validation_result = validation_result - 'failed_path'
            WHERE id = %s AND validation_result->>'failed_path' IS NOT NULL
        """, (log_id,))
        return cur.rowcount > 0

    def clear_wrong_match_paths(
        self,
        request_id: int,
        failed_paths: list[str] | tuple[str, ...] | set[str],
    ) -> int:
        """Null out failed_path for rejected rows matching request/path pairs."""
        paths = [str(path) for path in dict.fromkeys(failed_paths) if path]
        if not paths:
            return 0
        placeholders = ", ".join(["%s"] * len(paths))
        cur = self._execute(f"""
            UPDATE download_log
            SET validation_result = validation_result - 'failed_path'
            WHERE request_id = %s
              AND outcome = 'rejected'
              AND validation_result->>'failed_path' IN ({placeholders})
        """, tuple([request_id, *paths]))
        self.conn.commit()
        return cur.rowcount

    def update_download_log_measurement(
        self,
        download_log_id: int,
        *,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        v0_probe_kind: str | None = None,
        v0_probe_avg_bitrate: int | None = None,
    ) -> bool:
        """Persist measurement evidence onto one download_log row.

        Partial / non-destructive: only columns whose source value is
        non-None are touched. Used by wrong-match triage to plumb the
        measurement from ``ImportPreviewResult.import_result`` onto the
        same row that ``get_wrong_matches`` reads, so the candidate-
        evidence cells from PR #181 populate without changing the read
        path. Returns True when at least one column was updated, False
        when the call was a no-op (all None) or the row didn't exist.
        """
        sets: list[str] = []
        params: list[object] = []
        if spectral_grade is not None:
            sets.append("spectral_grade = %s")
            params.append(spectral_grade)
        if spectral_bitrate is not None:
            sets.append("spectral_bitrate = %s")
            params.append(spectral_bitrate)
        if v0_probe_kind is not None:
            sets.append("v0_probe_kind = %s")
            params.append(v0_probe_kind)
        if v0_probe_avg_bitrate is not None:
            sets.append("v0_probe_avg_bitrate = %s")
            params.append(v0_probe_avg_bitrate)
        if not sets:
            return False
        params.append(download_log_id)
        cur = self._execute(
            f"UPDATE download_log SET {', '.join(sets)} WHERE id = %s",
            tuple(params),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def record_wrong_match_triage(
        self,
        log_id: int,
        triage_result: dict[str, object],
    ) -> bool:
        """Persist preview-driven triage audit details on a download_log row."""
        cur = self._execute("""
            UPDATE download_log
            SET validation_result = jsonb_set(
                CASE
                    WHEN jsonb_typeof(validation_result) = 'object'
                    THEN validation_result
                    ELSE '{}'::jsonb
                END,
                '{wrong_match_triage}',
                %s::jsonb,
                true
            )
            WHERE id = %s
        """, (json.dumps(triage_result), log_id))
        self.conn.commit()
        return cur.rowcount > 0

    # -- Search log -----------------------------------------------------------

    def log_search(self, request_id: int, query: str | None = None,
                   result_count: int | None = None,
                   elapsed_s: float | None = None,
                   outcome: str = "error",
                   candidates: "list[CandidateScore] | None" = None,
                   variant: str | None = None,
                   final_state: str | None = None,
                   browse_time_s: float = 0.0,
                   match_time_s: float = 0.0,
                   peers_browsed: int = 0,
                   peers_browsed_lazy: int = 0,
                   fanout_waves: int = 0) -> None:
        """Record one search attempt for an album request.

        ``candidates`` is the top-N forensic ``CandidateScore`` list (already
        truncated by the caller). It is encoded via ``msgspec.json.encode``
        and written to ``search_log.candidates`` JSONB. ``None`` writes SQL
        NULL — error / submission-failure rows have no scoring data to
        report. See ``.claude/rules/code-quality.md`` § Wire-boundary types
        for the symmetric encode/decode contract.
        """
        candidates_json: str | None = None
        if candidates is not None:
            import msgspec  # local import keeps top-of-module deps narrow
            candidates_json = msgspec.json.encode(candidates).decode()
        self._execute("""
            INSERT INTO search_log (
                request_id, query, result_count, elapsed_s, outcome,
                candidates, variant, final_state, browse_time_s, match_time_s,
                peers_browsed, peers_browsed_lazy, fanout_waves
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (request_id, query, result_count, elapsed_s, outcome,
              candidates_json, variant, final_state, browse_time_s, match_time_s,
              peers_browsed, peers_browsed_lazy, fanout_waves))
        self.conn.commit()

    def get_search_history(self, request_id: int) -> list[dict[str, object]]:
        """Return all search_log rows for a single request_id, newest first."""
        cur = self._execute("""
            SELECT * FROM search_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_search_plan_stats_history(
        self, request_id: int,
    ) -> list[dict[str, object]]:
        """Projection-only search_log rows needed for search-plan stats.

        Intentionally excludes candidates JSONB so inspection endpoints do
        not deserialize every candidate blob just to compute aggregate stats.
        """
        cur = self._execute("""
            SELECT id, request_id, query, result_count, elapsed_s, outcome,
                   variant, final_state, browse_time_s, match_time_s,
                   peers_browsed, peers_browsed_lazy, fanout_waves,
                   plan_id, plan_item_id, plan_ordinal, plan_strategy,
                   plan_canonical_query_key, plan_repeat_group,
                   plan_generator_id, execution_stage, attempt_consumed,
                   cursor_update_status, stale_reason, plan_cycle_snapshot,
                   created_at
            FROM search_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_legacy_search_log_summary(
        self, request_id: int, *, limit: int,
    ) -> tuple[int, list[dict[str, object]]]:
        """Return count + bounded head sample of legacy search_log rows."""
        count_cur = self._execute("""
            SELECT COUNT(*) AS c
            FROM search_log
            WHERE request_id = %s AND plan_id IS NULL
        """, (request_id,))
        count_row = count_cur.fetchone()
        count = int(count_row["c"]) if count_row is not None else 0
        head_cur = self._execute("""
            SELECT id, request_id, query, result_count, elapsed_s, outcome,
                   variant, final_state, created_at
            FROM search_log
            WHERE request_id = %s AND plan_id IS NULL
            ORDER BY id DESC
            LIMIT %s
        """, (request_id, int(limit)))
        return count, [dict(r) for r in head_cur.fetchall()]

    def get_search_history_batch(self, request_ids: list[int]) -> dict[int, list[dict[str, object]]]:
        """Batch fetch search history for multiple request IDs.

        Returns dict of request_id → list of history rows (most recent first).
        """
        if not request_ids:
            return {}
        ph = ",".join(["%s"] * len(request_ids))
        cur = self._execute(
            f"SELECT * FROM search_log WHERE request_id IN ({ph}) ORDER BY id DESC",
            tuple(request_ids),
        )
        result: dict[int, list[dict[str, object]]] = {}
        for row in cur.fetchall():
            r = dict(row)
            rid = r["request_id"]
            assert isinstance(rid, int)
            if rid not in result:
                result[rid] = []
            result[rid].append(r)
        return result

    # -- Pipeline dashboard telemetry ----------------------------------------

    def record_cycle_metrics(
        self,
        *,
        cycle_total_s: float,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        browse_time_s: float = 0.0,
        match_time_s: float = 0.0,
        search_time_s: float = 0.0,
        cache_pos_hits: int = 0,
        cache_neg_hits: int = 0,
        cache_misses: int = 0,
        cache_errors: int = 0,
        cache_fuse_tripped: int = 0,
        cache_write_errors: int = 0,
        peers_browsed: int = 0,
        peers_browsed_lazy: int = 0,
        fanout_waves: int = 0,
        cycle_searches_watchdog_killed: int = 0,
        find_download_queued: int = 0,
        find_download_completed: int = 0,
        find_download_drain_time_s: float = 0.0,
    ) -> int:
        """Persist one completed cratedigger cycle's runtime counters."""
        completed = completed_at or datetime.now(timezone.utc)
        cur = self._execute("""
            INSERT INTO cycle_metrics (
                started_at, created_at, cycle_total_s, browse_time_s,
                match_time_s, search_time_s, cache_pos_hits, cache_neg_hits,
                cache_misses, cache_errors, cache_fuse_tripped,
                cache_write_errors, peers_browsed, peers_browsed_lazy,
                fanout_waves, cycle_searches_watchdog_killed,
                find_download_queued, find_download_completed,
                find_download_drain_time_s
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            RETURNING id
        """, (
            started_at, completed, cycle_total_s, browse_time_s,
            match_time_s, search_time_s, cache_pos_hits, cache_neg_hits,
            cache_misses, cache_errors, cache_fuse_tripped,
            cache_write_errors, peers_browsed, peers_browsed_lazy,
            fanout_waves, cycle_searches_watchdog_killed,
            find_download_queued, find_download_completed,
            find_download_drain_time_s,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])

    def record_peer_dir_observations(
        self,
        observations: Iterable[tuple[str, str]],
        *,
        observed_at: datetime | None = None,
    ) -> int:
        """Persist hashed peer/directory observations and return new combos.

        Each input pair represents one cold slskd browse submission that made
        it past the hot context cache, Redis positive cache, Redis negative
        cache, and the coordinator's duplicate in-flight join.
        """
        unique = {
            (str(username), str(file_dir))
            for username, file_dir in observations
            if username and file_dir
        }
        if not unique:
            return 0

        observed = observed_at or datetime.now(timezone.utc)
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)

        rows = [
            (*_peer_dir_hashes(username, file_dir), observed, observed)
            for username, file_dir in sorted(unique)
        ]
        combo_hashes = [row[0] for row in rows]
        existing_cur = self._execute(
            """
            SELECT combo_hash
            FROM peer_dir_observations
            WHERE combo_hash = ANY(%s)
            """,
            (combo_hashes,),
        )
        existing = {row["combo_hash"] for row in existing_cur.fetchall()}

        self._ensure_conn()
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO peer_dir_observations (
                    combo_hash, username_hash, dir_hash,
                    first_seen_at, last_seen_at
                )
                VALUES %s
                ON CONFLICT (combo_hash) DO UPDATE
                SET
                    last_seen_at = GREATEST(
                        peer_dir_observations.last_seen_at,
                        EXCLUDED.last_seen_at
                    ),
                    seen_count = peer_dir_observations.seen_count + 1
                """,
                rows,
            )
        self.conn.commit()
        return len(set(combo_hashes) - existing)

    def get_peer_dir_daily_metrics(self, days: int = 14) -> dict[str, Any]:
        """Return first-seen peer/directory trend metrics for the dashboard.

        Completed-day buckets are read from (and lazy-filled into) the
        ``peer_dir_daily_aggregates`` cache table so a populated cache
        collapses the per-day breakdown to cheap PK lookups. Today's
        Perth-local row is always recomputed live from a 1-day-bounded
        slice of ``peer_dir_observations`` -- it is mutable until Perth
        midnight and so is never cached.

        Cache PK is the Perth-local date, matching the existing
        ``(first_seen_at AT TIME ZONE 'Australia/Perth')::date``
        bucketing. Backfill runs under the connection's autocommit with
        ``INSERT ... ON CONFLICT (day) DO NOTHING``; each row is
        independently idempotent so concurrent dashboard requests
        cannot duplicate or corrupt cache entries.
        """
        clamped_days = max(1, min(int(days), 90))

        # Single source of truth for "today" in Perth-local terms. All
        # subsequent date math derives from this row so a Perth-midnight
        # rollover mid-call cannot split bucketing across boundaries.
        bounds_cur = self._execute("""
            SELECT
                (NOW() AT TIME ZONE 'Australia/Perth')::date AS today_perth,
                date_trunc(
                    'day', NOW() AT TIME ZONE 'Australia/Perth'
                ) AT TIME ZONE 'Australia/Perth' AS today_perth_start_utc
        """)
        bounds_row = bounds_cur.fetchone()
        assert bounds_row is not None, "NOW()-based query must return a row"
        today_perth = bounds_row["today_perth"]
        today_perth_start_utc = bounds_row["today_perth_start_utc"]
        window_start_perth = today_perth - timedelta(days=clamped_days - 1)
        completed_window_end = today_perth - timedelta(days=1)

        # Phase 1: read whatever the cache already has for the
        # completed-day portion of the window.
        cached_rows: dict[Any, dict[str, int]] = {}
        if completed_window_end >= window_start_perth:
            cur = self._execute(
                """
                SELECT day, new_combos, new_peers, new_dirs
                FROM peer_dir_daily_aggregates
                WHERE day BETWEEN %s AND %s
                """,
                (window_start_perth, completed_window_end),
            )
            for row in cur.fetchall():
                cached_rows[row["day"]] = {
                    "new_combos": int(row["new_combos"]),
                    "new_peers": int(row["new_peers"]),
                    "new_dirs": int(row["new_dirs"]),
                }

        # Phase 2: detect missing completed days (every Perth-local date
        # in [window_start, today - 1] not represented in the cache).
        missing_days: list[Any] = []
        if completed_window_end >= window_start_perth:
            day_cursor = window_start_perth
            while day_cursor <= completed_window_end:
                if day_cursor not in cached_rows:
                    missing_days.append(day_cursor)
                day_cursor = day_cursor + timedelta(days=1)

        # Phase 3: lazy-fill any missing completed days. One bounded
        # GROUP BY query covers all missing days; execute_values writes
        # them in a single round-trip with ON CONFLICT DO NOTHING.
        if missing_days:
            agg_cur = self._execute(
                """
                SELECT
                    (first_seen_at AT TIME ZONE 'Australia/Perth')::date AS day,
                    COUNT(*)::int AS new_combos,
                    COUNT(DISTINCT username_hash)::int AS new_peers,
                    COUNT(DISTINCT dir_hash)::int AS new_dirs
                FROM peer_dir_observations
                WHERE first_seen_at >= %s
                  AND first_seen_at < %s
                  AND (first_seen_at AT TIME ZONE 'Australia/Perth')::date
                      = ANY(%s)
                GROUP BY 1
                """,
                (
                    # UTC pre-filter: earliest possible UTC instant for
                    # any Perth date in the missing-day list is
                    # (min_day 00:00 Perth) -- 8h. Use a 1-hour buffer
                    # to absorb any IANA edge case.
                    datetime.combine(
                        min(missing_days),
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    ) - timedelta(hours=9),
                    today_perth_start_utc,
                    list(missing_days),
                ),
            )
            agg_by_day = {
                row["day"]: row for row in agg_cur.fetchall()
            }
            insert_rows = [
                (
                    day,
                    int((agg_by_day.get(day) or {}).get("new_combos") or 0),
                    int((agg_by_day.get(day) or {}).get("new_peers") or 0),
                    int((agg_by_day.get(day) or {}).get("new_dirs") or 0),
                )
                for day in missing_days
            ]

            self._ensure_conn()
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO peer_dir_daily_aggregates
                        (day, new_combos, new_peers, new_dirs)
                    VALUES %s
                    ON CONFLICT (day) DO NOTHING
                    """,
                    insert_rows,
                )

            # Re-read the cache for the full completed window. execute_values
            # + ON CONFLICT DO NOTHING does not reliably return conflicting
            # rows, so a re-read is the contractually clean way to merge
            # whatever a concurrent caller may have inserted concurrently.
            cur = self._execute(
                """
                SELECT day, new_combos, new_peers, new_dirs
                FROM peer_dir_daily_aggregates
                WHERE day BETWEEN %s AND %s
                """,
                (window_start_perth, completed_window_end),
            )
            cached_rows = {
                row["day"]: {
                    "new_combos": int(row["new_combos"]),
                    "new_peers": int(row["new_peers"]),
                    "new_dirs": int(row["new_dirs"]),
                }
                for row in cur.fetchall()
            }

        # Phase 4: today's row -- live, 1-day-bounded query. The UTC
        # pre-filter lets idx_peer_dir_observations_first_seen prune
        # most rows before the timezone cast; the Perth-date predicate
        # pins boundary correctness.
        today_cur = self._execute(
            """
            SELECT
                COUNT(*)::int AS new_combos,
                COUNT(DISTINCT username_hash)::int AS new_peers,
                COUNT(DISTINCT dir_hash)::int AS new_dirs
            FROM peer_dir_observations
            WHERE first_seen_at >= %s - INTERVAL '1 hour'
              AND (first_seen_at AT TIME ZONE 'Australia/Perth')::date = %s
            """,
            (today_perth_start_utc, today_perth),
        )
        today_row = today_cur.fetchone() or {}
        today_metrics = {
            "new_combos": int(today_row.get("new_combos") or 0),
            "new_peers": int(today_row.get("new_peers") or 0),
            "new_dirs": int(today_row.get("new_dirs") or 0),
        }

        # Phase 5: totals query (Q1) -- unchanged. Lifetime aggregates
        # are intrinsically full-scan and out of scope for this plan.
        totals_cur = self._execute("""
            SELECT
                COUNT(*)::int AS known_combos,
                COUNT(DISTINCT username_hash)::int AS known_peers,
                COUNT(DISTINCT dir_hash)::int AS known_dirs,
                COUNT(*) FILTER (
                    WHERE first_seen_at >= NOW() - INTERVAL '24 hours'
                )::int AS new_24h,
                COUNT(*) FILTER (
                    WHERE last_seen_at >= NOW() - INTERVAL '24 hours'
                )::int AS cold_seen_24h,
                MIN(first_seen_at) AS tracked_since,
                COUNT(DISTINCT (first_seen_at AT TIME ZONE 'Australia/Perth')::date)::int
                    AS days_with_new
            FROM peer_dir_observations
        """)
        totals_row = totals_cur.fetchone() or {}

        # Phase 6: merge cached completed days + today's live row into
        # the existing response shape. Days array is ordered DESC by
        # date (today first), matching the legacy query's
        # ``ORDER BY day_series.day DESC``.
        day_dicts: list[dict[str, Any]] = []
        day_cursor = today_perth
        while day_cursor >= window_start_perth:
            if day_cursor == today_perth:
                metrics = today_metrics
            else:
                metrics = cached_rows.get(day_cursor) or {
                    "new_combos": 0, "new_peers": 0, "new_dirs": 0,
                }
            day_dicts.append({
                "date": day_cursor.isoformat(),
                "new_combos": int(metrics["new_combos"]),
                "new_peers": int(metrics["new_peers"]),
                "new_dirs": int(metrics["new_dirs"]),
            })
            day_cursor = day_cursor - timedelta(days=1)

        return {
            "days": day_dicts,
            "totals": {
                "known_combos": int(totals_row.get("known_combos") or 0),
                "known_peers": int(totals_row.get("known_peers") or 0),
                "known_dirs": int(totals_row.get("known_dirs") or 0),
                "new_24h": int(totals_row.get("new_24h") or 0),
                "cold_seen_24h": int(totals_row.get("cold_seen_24h") or 0),
                "days_with_new": int(totals_row.get("days_with_new") or 0),
                "tracked_since": _isoformat_or_none(
                    totals_row.get("tracked_since")
                ),
            },
        }

    def get_pipeline_dashboard_metrics(
        self,
        *,
        plan_generator_id: str | None = None,
    ) -> dict[str, Any]:
        """Return DB-derived metrics for the Pipeline dashboard.

        Redis status is owned by the web cache layer; this method intentionally
        covers only persisted Postgres state: searches, cycles, and active
        request coverage.

        ``plan_generator_id`` selects the search-plan generator id used to
        bucket wanted rows in the plan-readiness panel. Defaults to
        ``lib.search.SEARCH_PLAN_GENERATOR_ID`` so the dashboard tracks
        whatever the running pipeline considers current. Tests can pin a
        different id without monkey-patching the constant.
        """
        if plan_generator_id is None:
            from lib.search import SEARCH_PLAN_GENERATOR_ID
            plan_generator_id = SEARCH_PLAN_GENERATOR_ID
        peer_dirs = self.get_peer_dir_daily_metrics()
        peer_dirs["heavy_queries"] = self._dashboard_peer_dir_heavy_queries()
        peer_dirs["heavy_query_hours"] = 24
        plan_readiness = self.get_search_plan_readiness(plan_generator_id)
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "searches": {
                "windows": [self._dashboard_search_window(label, hours)
                            for label, hours in DASHBOARD_WINDOWS],
            },
            "cycles": {
                "windows": [self._dashboard_cycle_window(label, hours)
                            for label, hours in DASHBOARD_WINDOWS],
                "recent": self._dashboard_cycle_rows(
                    order_by="created_at DESC",
                    limit=12,
                ),
                "outliers": self._dashboard_cycle_rows(
                    where="created_at >= NOW() - %s::interval",
                    params=("24 hours",),
                    order_by="cycle_total_s DESC",
                    limit=8,
                ),
            },
            "coverage": self._dashboard_coverage(),
            "peer_dirs": peer_dirs,
            "plan_readiness": plan_readiness,
        }

    def _dashboard_peer_dir_heavy_queries(
        self,
        *,
        hours: int = 24,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        """Return recent search rows that generated the most peer/dir work."""
        clamped_hours = max(1, min(int(hours), 168))
        clamped_limit = max(1, min(int(limit), 50))
        cur = self._execute("""
            SELECT
                sl.id AS search_log_id,
                sl.request_id,
                ar.mb_release_id,
                ar.artist_name,
                ar.album_title,
                ar.status,
                sl.created_at,
                sl.query,
                sl.variant,
                sl.outcome,
                sl.result_count,
                sl.elapsed_s,
                sl.browse_time_s,
                sl.match_time_s,
                sl.peers_browsed,
                sl.peers_browsed_lazy,
                sl.fanout_waves,
                (sl.peers_browsed + sl.peers_browsed_lazy)::int AS peer_dirs
            FROM search_log sl
            JOIN album_requests ar ON ar.id = sl.request_id
            WHERE sl.created_at >= NOW() - %s::interval
              AND (sl.peers_browsed + sl.peers_browsed_lazy) > 0
            ORDER BY
                (sl.peers_browsed + sl.peers_browsed_lazy) DESC,
                sl.fanout_waves DESC,
                sl.created_at DESC,
                sl.id DESC
            LIMIT %s
        """, (f"{clamped_hours} hours", clamped_limit))
        return [
            self._serialize_dashboard_heavy_query_row(dict(row))
            for row in cur.fetchall()
        ]

    def _serialize_dashboard_heavy_query_row(
        self,
        row: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "search_log_id": int(row["search_log_id"]),
            "request_id": int(row["request_id"]),
            "mb_release_id": row.get("mb_release_id"),
            "artist_name": row.get("artist_name"),
            "album_title": row.get("album_title"),
            "status": row.get("status"),
            "created_at": _isoformat_or_none(row.get("created_at")),
            "query": row.get("query"),
            "variant": row.get("variant"),
            "outcome": row.get("outcome"),
            "result_count": int(row.get("result_count") or 0),
            "elapsed_s": _float_or_none(row.get("elapsed_s")),
            "browse_time_s": float(row.get("browse_time_s") or 0.0),
            "match_time_s": float(row.get("match_time_s") or 0.0),
            "peers_browsed": int(row.get("peers_browsed") or 0),
            "peers_browsed_lazy": int(row.get("peers_browsed_lazy") or 0),
            "peer_dirs": int(row.get("peer_dirs") or 0),
            "fanout_waves": int(row.get("fanout_waves") or 0),
        }

    def _dashboard_search_window(self, label: str, hours: int) -> dict[str, Any]:
        # ``exhausted`` is HISTORICAL ONLY after the persisted-search-plans
        # cutover -- new code never writes ``outcome='exhausted'`` rows.
        # ``cursor_wraps`` is the plan-driven equivalent: a search-log row
        # with ``cursor_update_status='wrapped'`` is what increments
        # ``plan_cycle_count``. Together the two fields let dashboards
        # diff "old reset signal" vs "new wrap signal" during the rollout.
        cur = self._execute("""
            SELECT
                COUNT(*)::int AS searches,
                COUNT(DISTINCT request_id)::int AS distinct_requests,
                AVG(elapsed_s)::double precision AS avg_elapsed_s,
                (percentile_cont(0.5) WITHIN GROUP (ORDER BY elapsed_s)
                    FILTER (WHERE elapsed_s IS NOT NULL))::double precision AS median_elapsed_s,
                (percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_s)
                    FILTER (WHERE elapsed_s IS NOT NULL))::double precision AS p95_elapsed_s,
                MAX(elapsed_s)::double precision AS max_elapsed_s,
                COUNT(*) FILTER (WHERE outcome = 'found')::int AS found,
                COUNT(*) FILTER (WHERE outcome = 'no_match')::int AS no_match,
                COUNT(*) FILTER (WHERE outcome = 'no_results')::int AS no_results,
                COUNT(*) FILTER (WHERE outcome = 'exhausted')::int AS exhausted,
                COUNT(*) FILTER (WHERE outcome IN ('timeout', 'error', 'empty_query'))::int AS errors,
                COUNT(*) FILTER (WHERE cursor_update_status = 'wrapped')::int AS cursor_wraps,
                COUNT(*) FILTER (WHERE cursor_update_status = 'stale')::int AS stale_completions,
                COUNT(*) FILTER (WHERE attempt_consumed = false)::int AS non_consuming
            FROM search_log
            WHERE created_at >= NOW() - %s::interval
        """, (f"{hours} hours",))
        row = cur.fetchone() or {}
        searches = int(row.get("searches") or 0)
        return {
            "label": label,
            "hours": hours,
            "searches": searches,
            "distinct_requests": int(row.get("distinct_requests") or 0),
            "searches_per_hour": searches / hours if hours else 0,
            "searches_per_24h": (searches / hours * 24) if hours else 0,
            "avg_elapsed_s": _float_or_none(row.get("avg_elapsed_s")),
            "median_elapsed_s": _float_or_none(row.get("median_elapsed_s")),
            "p95_elapsed_s": _float_or_none(row.get("p95_elapsed_s")),
            "max_elapsed_s": _float_or_none(row.get("max_elapsed_s")),
            "outcomes": {
                "found": int(row.get("found") or 0),
                "no_match": int(row.get("no_match") or 0),
                "no_results": int(row.get("no_results") or 0),
                # Historical only -- preserved so legacy rows still render
                # in their existing position. Any non-zero count for rows
                # newer than the persisted-search-plans deploy timestamp is
                # a regression; see docs/persisted-search-plans-rollout.md.
                "exhausted": int(row.get("exhausted") or 0),
                "errors": int(row.get("errors") or 0),
            },
            # Plan-driven cycle metrics. ``cursor_wraps`` replaces the
            # ``exhausted`` reset signal: it is one-per-cycle per request
            # and increments ``plan_cycle_count``. ``stale_completions``
            # are post-regeneration log-only rows. ``non_consuming`` are
            # pre-attempt setup failures that did not advance the cursor.
            "cursor_wraps": int(row.get("cursor_wraps") or 0),
            "stale_completions": int(row.get("stale_completions") or 0),
            "non_consuming": int(row.get("non_consuming") or 0),
            # Cache attribution honesty: surface that ``search_log`` has
            # no per-search cache columns today; only cycle-level counters
            # exist. See ``CACHE_ATTRIBUTION_CYCLE_ONLY``.
            "cache_attribution_level": CACHE_ATTRIBUTION_CYCLE_ONLY,
        }

    def _dashboard_cycle_window(self, label: str, hours: int) -> dict[str, Any]:
        cur = self._execute("""
            SELECT
                COUNT(*)::int AS cycles,
                AVG(cycle_total_s)::double precision AS avg_cycle_s,
                (percentile_cont(0.5) WITHIN GROUP (ORDER BY cycle_total_s)
                    FILTER (WHERE cycle_total_s IS NOT NULL))::double precision AS median_cycle_s,
                (percentile_cont(0.95) WITHIN GROUP (ORDER BY cycle_total_s)
                    FILTER (WHERE cycle_total_s IS NOT NULL))::double precision AS p95_cycle_s,
                MAX(cycle_total_s)::double precision AS max_cycle_s,
                (percentile_cont(0.5) WITHIN GROUP (ORDER BY search_time_s)
                    FILTER (WHERE search_time_s IS NOT NULL))::double precision AS median_search_s,
                SUM(cycle_searches_watchdog_killed)::int AS watchdog_kills,
                SUM(find_download_queued)::int AS find_download_queued,
                SUM(find_download_completed)::int AS find_download_completed,
                SUM(cache_errors)::int AS cache_errors,
                SUM(cache_write_errors)::int AS cache_write_errors,
                SUM(cache_fuse_tripped)::int AS cache_fuse_tripped,
                SUM(peers_browsed)::int AS peers_browsed,
                SUM(peers_browsed_lazy)::int AS peers_browsed_lazy,
                SUM(fanout_waves)::int AS fanout_waves
            FROM cycle_metrics
            WHERE created_at >= NOW() - %s::interval
        """, (f"{hours} hours",))
        row = cur.fetchone() or {}
        return {
            "label": label,
            "hours": hours,
            "cycles": int(row.get("cycles") or 0),
            "avg_cycle_s": _float_or_none(row.get("avg_cycle_s")),
            "median_cycle_s": _float_or_none(row.get("median_cycle_s")),
            "p95_cycle_s": _float_or_none(row.get("p95_cycle_s")),
            "max_cycle_s": _float_or_none(row.get("max_cycle_s")),
            "median_search_s": _float_or_none(row.get("median_search_s")),
            "watchdog_kills": int(row.get("watchdog_kills") or 0),
            "find_download_queued": int(row.get("find_download_queued") or 0),
            "find_download_completed": int(row.get("find_download_completed") or 0),
            "cache_errors": int(row.get("cache_errors") or 0),
            "cache_write_errors": int(row.get("cache_write_errors") or 0),
            "cache_fuse_tripped": int(row.get("cache_fuse_tripped") or 0),
            "peers_browsed": int(row.get("peers_browsed") or 0),
            "peers_browsed_lazy": int(row.get("peers_browsed_lazy") or 0),
            "fanout_waves": int(row.get("fanout_waves") or 0),
        }

    def _dashboard_cycle_rows(
        self,
        *,
        order_by: str,
        limit: int,
        where: str | None = None,
        params: tuple[object, ...] = (),
    ) -> list[dict[str, Any]]:
        filter_sql = f"WHERE {where}" if where else ""
        cur = self._execute(f"""
            SELECT
                id, started_at, created_at, cycle_total_s, browse_time_s,
                match_time_s, search_time_s, cycle_searches_watchdog_killed,
                find_download_queued, find_download_completed,
                find_download_drain_time_s, cache_errors, cache_write_errors,
                cache_fuse_tripped, peers_browsed, peers_browsed_lazy,
                fanout_waves
            FROM cycle_metrics
            {filter_sql}
            ORDER BY {order_by}
            LIMIT %s
        """, (*params, limit))
        return [self._serialize_dashboard_cycle_row(dict(row))
                for row in cur.fetchall()]

    def _serialize_dashboard_cycle_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "started_at": _isoformat_or_none(row.get("started_at")),
            "created_at": _isoformat_or_none(row.get("created_at")),
            "cycle_total_s": float(row["cycle_total_s"]),
            "browse_time_s": float(row["browse_time_s"]),
            "match_time_s": float(row["match_time_s"]),
            "search_time_s": float(row["search_time_s"]),
            "watchdog_kills": int(row["cycle_searches_watchdog_killed"]),
            "find_download_queued": int(row["find_download_queued"]),
            "find_download_completed": int(row["find_download_completed"]),
            "find_download_drain_time_s": float(row["find_download_drain_time_s"]),
            "cache_errors": int(row["cache_errors"]),
            "cache_write_errors": int(row["cache_write_errors"]),
            "cache_fuse_tripped": int(row["cache_fuse_tripped"]),
            "peers_browsed": int(row["peers_browsed"]),
            "peers_browsed_lazy": int(row["peers_browsed_lazy"]),
            "fanout_waves": int(row["fanout_waves"]),
        }

    def _dashboard_coverage(self) -> dict[str, Any]:
        summary = self._dashboard_coverage_summary()
        top_suspects = self._dashboard_loop_suspects()
        active_searches_24h = int(summary.get("active_wanted_searches_24h") or 0)
        top_10_searches = sum(int(r["searches_24h"]) for r in top_suspects[:10])
        top_10_share = (
            top_10_searches / active_searches_24h if active_searches_24h else 0
        )
        return {
            **summary,
            "match_rate_series_24h": self._dashboard_match_rate_series(24),
            "match_rate_series_28d": self._dashboard_daily_match_rate_series(28),
            "top_10_share_24h": top_10_share,
            "top_loop_suspects": top_suspects,
            "stale_wanted": self._dashboard_stale_wanted(),
        }

    def _dashboard_match_rate_series(self, hours: int) -> list[dict[str, Any]]:
        clamped_hours = max(1, min(int(hours), 168))
        cur = self._execute("""
            WITH buckets AS (
                SELECT generate_series(
                    date_trunc('hour', NOW())
                        - ((%s::int - 1) * INTERVAL '1 hour'),
                    date_trunc('hour', NOW()),
                    INTERVAL '1 hour'
                ) AS bucket_start
            ),
            found AS (
                SELECT
                    date_trunc('hour', created_at) AS bucket_start,
                    COUNT(*)::int AS matches
                FROM search_log
                WHERE outcome = 'found'
                  AND created_at >= date_trunc('hour', NOW())
                    - ((%s::int - 1) * INTERVAL '1 hour')
                GROUP BY 1
            )
            SELECT
                buckets.bucket_start,
                COALESCE(found.matches, 0)::int AS matches
            FROM buckets
            LEFT JOIN found ON found.bucket_start = buckets.bucket_start
            ORDER BY buckets.bucket_start
        """, (clamped_hours, clamped_hours))
        return [
            {
                "bucket_start": _isoformat_or_none(row["bucket_start"]),
                "matches": int(row["matches"] or 0),
                "matches_per_hour": int(row["matches"] or 0),
            }
            for row in cur.fetchall()
        ]

    def _dashboard_daily_match_rate_series(self, days: int) -> list[dict[str, Any]]:
        clamped_days = max(1, min(int(days), 90))
        cur = self._execute("""
            WITH buckets AS (
                SELECT generate_series(
                    date_trunc('day', NOW())
                        - ((%s::int - 1) * INTERVAL '1 day'),
                    date_trunc('day', NOW()),
                    INTERVAL '1 day'
                ) AS bucket_start
            ),
            found AS (
                SELECT
                    date_trunc('day', created_at) AS bucket_start,
                    COUNT(*)::int AS matches
                FROM search_log
                WHERE outcome = 'found'
                  AND created_at >= date_trunc('day', NOW())
                    - ((%s::int - 1) * INTERVAL '1 day')
                GROUP BY 1
            )
            SELECT
                buckets.bucket_start,
                COALESCE(found.matches, 0)::int AS matches
            FROM buckets
            LEFT JOIN found ON found.bucket_start = buckets.bucket_start
            ORDER BY buckets.bucket_start
        """, (clamped_days, clamped_days))
        return [
            {
                "bucket_start": _isoformat_or_none(row["bucket_start"]),
                "matches": int(row["matches"] or 0),
                "matches_per_day": int(row["matches"] or 0),
            }
            for row in cur.fetchall()
        ]

    def _dashboard_coverage_summary(self) -> dict[str, Any]:
        cur = self._execute("""
            WITH wanted AS (
                SELECT id
                FROM album_requests
                WHERE status = 'wanted'
            ),
            per_request AS (
                SELECT
                    request_id,
                    MAX(created_at) AS last_search_at,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS searches_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS searches_6h
                FROM search_log
                GROUP BY request_id
            ),
            match_rates AS (
                SELECT
                    COUNT(*) FILTER (
                        WHERE outcome = 'found'
                          AND created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS matches_24h,
                    COUNT(*) FILTER (
                        WHERE outcome = 'found'
                          AND created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS matches_6h
                FROM search_log
            )
            SELECT
                COUNT(*)::int AS wanted_total,
                COUNT(*) FILTER (
                    WHERE pr.last_search_at >= NOW() - INTERVAL '24 hours'
                )::int AS wanted_searched_24h,
                COUNT(*) FILTER (
                    WHERE pr.last_search_at >= NOW() - INTERVAL '6 hours'
                )::int AS wanted_searched_6h,
                COUNT(*) FILTER (WHERE pr.last_search_at IS NULL)::int
                    AS wanted_never_searched,
                COALESCE(SUM(pr.searches_24h), 0)::int
                    AS active_wanted_searches_24h,
                COALESCE(SUM(pr.searches_6h), 0)::int
                    AS active_wanted_searches_6h,
                MIN(pr.last_search_at) FILTER (WHERE pr.last_search_at IS NOT NULL)
                    AS oldest_last_search_at,
                COALESCE(MAX(match_rates.matches_24h), 0)::int AS matches_24h,
                COALESCE(MAX(match_rates.matches_6h), 0)::int AS matches_6h
            FROM wanted w
            LEFT JOIN per_request pr ON pr.request_id = w.id
            CROSS JOIN match_rates
        """)
        row = cur.fetchone() or {}
        wanted_total = int(row.get("wanted_total") or 0)
        searched_24h = int(row.get("wanted_searched_24h") or 0)
        searched_6h = int(row.get("wanted_searched_6h") or 0)
        matches_24h = int(row.get("matches_24h") or 0)
        matches_6h = int(row.get("matches_6h") or 0)
        return {
            "wanted_total": wanted_total,
            "wanted_searched_24h": searched_24h,
            "wanted_searched_6h": searched_6h,
            "wanted_unsearched_24h": max(wanted_total - searched_24h, 0),
            "wanted_unsearched_6h": max(wanted_total - searched_6h, 0),
            "wanted_never_searched": int(row.get("wanted_never_searched") or 0),
            "active_wanted_searches_24h": int(
                row.get("active_wanted_searches_24h") or 0
            ),
            "active_wanted_searches_6h": int(
                row.get("active_wanted_searches_6h") or 0
            ),
            "oldest_last_search_at": _isoformat_or_none(
                row.get("oldest_last_search_at")
            ),
            "matches_24h": matches_24h,
            "matches_6h": matches_6h,
            "matches_per_hour_24h": matches_24h / 24,
            "matches_per_hour_6h": matches_6h / 6,
        }

    def get_search_plan_readiness(
        self,
        generator_id: str,
    ) -> dict[str, Any]:
        """Aggregate plan-readiness counts for the wanted bucket.

        Bucket precedence (each wanted row falls into exactly one bucket):

          1. ``wanted_searchable`` -- ``status='wanted'`` AND active plan
             whose ``generator_id`` matches the current generator id.
          2. ``wanted_legacy`` -- has an active plan but its ``generator_id``
             differs from the current id (old-generator carryover that
             startup reconciliation will supersede next pass).
          3. ``wanted_failed_deterministic`` -- no active plan AND a
             ``failed_deterministic`` plan exists for the current generator
             id. Sticky; cannot be re-tried by reconciliation.
          4. ``wanted_failed_transient`` -- no active plan AND a
             ``failed_transient`` plan exists for the current generator id.
             Reconciliation will retry next cycle.
          5. ``wanted_no_plan`` -- no active plan AND no current-generator
             plan rows at all. This is the stop-the-deploy signal.

        ``wanted_total`` equals the sum of buckets. The total is read off
        ``album_requests`` directly so any drift between sum and total is
        a bug (drop-the-buckets-on-the-floor classifier mistake) and not
        a missing row.

        Read-only and dashboard-grade: one SQL query. Callers should not
        treat any zero count as proof of post-cutover correctness; pair
        this with ``docs/persisted-search-plans-rollout.md`` SQL spot
        checks (active-plan FK integrity, contiguous ordinals, post-deploy
        ``outcome='exhausted'`` rate).
        """
        cur = self._execute(
            """
            WITH wanted AS (
                SELECT id, active_plan_id
                FROM album_requests
                WHERE status = 'wanted'
            ),
            classified AS (
                SELECT
                    w.id,
                    CASE
                        WHEN w.active_plan_id IS NOT NULL
                             AND active_plan.generator_id = %s
                            THEN 'wanted_searchable'
                        WHEN w.active_plan_id IS NOT NULL
                             AND active_plan.generator_id IS NOT NULL
                             AND active_plan.generator_id <> %s
                            THEN 'wanted_legacy'
                        WHEN EXISTS (
                            SELECT 1 FROM search_plans sp
                            WHERE sp.request_id = w.id
                              AND sp.generator_id = %s
                              AND sp.status = 'failed_deterministic'
                        )
                            THEN 'wanted_failed_deterministic'
                        WHEN EXISTS (
                            SELECT 1 FROM search_plans sp
                            WHERE sp.request_id = w.id
                              AND sp.generator_id = %s
                              AND sp.status = 'failed_transient'
                        )
                            THEN 'wanted_failed_transient'
                        ELSE 'wanted_no_plan'
                    END AS bucket
                FROM wanted w
                LEFT JOIN search_plans active_plan
                  ON active_plan.id = w.active_plan_id
            )
            SELECT
                COUNT(*)::int AS wanted_total,
                COUNT(*) FILTER (WHERE bucket = 'wanted_searchable')::int
                    AS wanted_searchable,
                COUNT(*) FILTER (WHERE bucket = 'wanted_legacy')::int
                    AS wanted_legacy,
                COUNT(*) FILTER (WHERE bucket = 'wanted_failed_deterministic')::int
                    AS wanted_failed_deterministic,
                COUNT(*) FILTER (WHERE bucket = 'wanted_failed_transient')::int
                    AS wanted_failed_transient,
                COUNT(*) FILTER (WHERE bucket = 'wanted_no_plan')::int
                    AS wanted_no_plan
            FROM classified
            """,
            (generator_id, generator_id, generator_id, generator_id),
        )
        row = cur.fetchone() or {}
        return {
            "generator_id": generator_id,
            "wanted_total": int(row.get("wanted_total") or 0),
            "wanted_searchable": int(row.get("wanted_searchable") or 0),
            "wanted_legacy": int(row.get("wanted_legacy") or 0),
            "wanted_failed_deterministic": int(
                row.get("wanted_failed_deterministic") or 0),
            "wanted_failed_transient": int(
                row.get("wanted_failed_transient") or 0),
            "wanted_no_plan": int(row.get("wanted_no_plan") or 0),
        }

    def _dashboard_loop_suspects(self) -> list[dict[str, Any]]:
        cur = self._execute("""
            WITH wanted AS (
                SELECT id, artist_name, album_title, status
                FROM album_requests
                WHERE status = 'wanted'
            ),
            per_request AS (
                SELECT
                    request_id,
                    MAX(created_at) AS last_search_at,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS searches_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS searches_6h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'found'
                    )::int AS found_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'no_match'
                    )::int AS no_match_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'no_results'
                    )::int AS no_results_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'exhausted'
                    )::int AS reset_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome IN ('timeout', 'error', 'empty_query')
                    )::int AS problem_24h
                FROM search_log
                GROUP BY request_id
            )
            SELECT
                w.id AS request_id, w.artist_name, w.album_title, w.status,
                pr.last_search_at,
                COALESCE(pr.searches_24h, 0)::int AS searches_24h,
                COALESCE(pr.searches_6h, 0)::int AS searches_6h,
                COALESCE(pr.found_24h, 0)::int AS found_24h,
                COALESCE(pr.no_match_24h, 0)::int AS no_match_24h,
                COALESCE(pr.no_results_24h, 0)::int AS no_results_24h,
                COALESCE(pr.reset_24h, 0)::int AS reset_24h,
                COALESCE(pr.problem_24h, 0)::int AS problem_24h
            FROM wanted w
            JOIN per_request pr ON pr.request_id = w.id
            WHERE COALESCE(pr.searches_24h, 0) > 0
            ORDER BY pr.searches_24h DESC, pr.searches_6h DESC, w.id ASC
            LIMIT 12
        """)
        return [self._serialize_dashboard_request_row(dict(row))
                for row in cur.fetchall()]

    def _dashboard_stale_wanted(self) -> list[dict[str, Any]]:
        cur = self._execute("""
            WITH wanted AS (
                SELECT id, artist_name, album_title, status, created_at
                FROM album_requests
                WHERE status = 'wanted'
            ),
            per_request AS (
                SELECT
                    request_id,
                    MAX(created_at) AS last_search_at,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS searches_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS searches_6h
                FROM search_log
                GROUP BY request_id
            )
            SELECT
                w.id AS request_id, w.artist_name, w.album_title, w.status,
                pr.last_search_at,
                CASE
                    WHEN pr.last_search_at IS NULL THEN NULL
                    ELSE EXTRACT(EPOCH FROM (NOW() - pr.last_search_at)) / 3600.0
                END AS hours_since_search,
                COALESCE(pr.searches_24h, 0)::int AS searches_24h,
                COALESCE(pr.searches_6h, 0)::int AS searches_6h
            FROM wanted w
            LEFT JOIN per_request pr ON pr.request_id = w.id
            ORDER BY pr.last_search_at ASC NULLS FIRST, w.created_at ASC, w.id ASC
            LIMIT 12
        """)
        rows = []
        for row in cur.fetchall():
            item = self._serialize_dashboard_request_row(dict(row))
            item["hours_since_search"] = _float_or_none(row["hours_since_search"])
            rows.append(item)
        return rows

    def _serialize_dashboard_request_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "request_id": int(row["request_id"]),
            "artist_name": row["artist_name"],
            "album_title": row["album_title"],
            "status": row["status"],
            "last_search_at": _isoformat_or_none(row.get("last_search_at")),
            "searches_24h": int(row.get("searches_24h") or 0),
            "searches_6h": int(row.get("searches_6h") or 0),
            "found_24h": int(row.get("found_24h") or 0),
            "no_match_24h": int(row.get("no_match_24h") or 0),
            "no_results_24h": int(row.get("no_results_24h") or 0),
            "reset_24h": int(row.get("reset_24h") or 0),
            "problem_24h": int(row.get("problem_24h") or 0),
        }

    # -- Track counts --------------------------------------------------------

    def get_track_counts(self, request_ids: list[int]) -> dict[int, int]:
        """Batch fetch track counts for multiple request IDs.

        Returns dict of request_id → track count (only for IDs with tracks).
        """
        if not request_ids:
            return {}
        ph = ",".join(["%s"] * len(request_ids))
        cur = self._execute(
            f"SELECT request_id, COUNT(*) FROM album_tracks "
            f"WHERE request_id IN ({ph}) GROUP BY request_id",
            tuple(request_ids),
        )
        return {row["request_id"]: row["count"] for row in cur.fetchall()}

    # --- Denylist ---

    def add_denylist(self, request_id, username, reason=None):
        self._execute("""
            INSERT INTO source_denylist (request_id, username, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (request_id, username) DO NOTHING
        """, (request_id, username, reason))
        self.conn.commit()

    def get_denylisted_users(self, request_id):
        cur = self._execute("""
            SELECT username, reason, created_at
            FROM source_denylist
            WHERE request_id = %s
            ORDER BY created_at ASC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    # --- User cooldowns (issue #39) ---

    def add_cooldown(self, username: str, cooldown_until: datetime,
                     reason: str | None = None) -> None:
        """Insert or update a user cooldown (upsert by username)."""
        self._execute("""
            INSERT INTO user_cooldowns (username, cooldown_until, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (username) DO UPDATE
                SET cooldown_until = EXCLUDED.cooldown_until,
                    reason = EXCLUDED.reason
        """, (username, cooldown_until, reason))
        self.conn.commit()

    def get_cooled_down_users(self) -> list[str]:
        """Return usernames with active (non-expired) cooldowns."""
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            SELECT username FROM user_cooldowns
            WHERE cooldown_until > %s
        """, (now,))
        return [r["username"] for r in cur.fetchall()]

    def get_user_cooldowns(self) -> list[dict[str, Any]]:
        """Return all cooldown rows (including expired) for CLI/web display."""
        cur = self._execute("""
            SELECT username, cooldown_until, reason, created_at
            FROM user_cooldowns
            ORDER BY cooldown_until DESC
        """)
        return [dict(r) for r in cur.fetchall()]

    def check_and_apply_cooldown(
        self,
        username: str,
        config: CooldownConfig | None = None,
    ) -> bool:
        """Check a user's recent outcomes and apply cooldown if warranted.

        Queries the last N download_log outcomes for this user globally
        (across all requests), then delegates to should_cooldown().
        Returns True if a cooldown was applied.
        """
        cfg = config or CooldownConfig()
        cur = self._execute("""
            SELECT outcome FROM download_log
            WHERE outcome IS NOT NULL
              AND COALESCE(beets_scenario, '') <> 'abandoned_auto_import'
              AND %s = ANY(
                  regexp_split_to_array(
                      regexp_replace(COALESCE(soulseek_username, ''), '\\s*,\\s*', ',', 'g'),
                      ','
                  )
              )
            ORDER BY id DESC
            LIMIT %s
        """, (username, cfg.lookback_window))
        outcomes = [r["outcome"] for r in cur.fetchall()]
        if not should_cooldown(outcomes, cfg):
            return False
        cooldown_until = datetime.now(timezone.utc) + timedelta(days=cfg.cooldown_days)
        self.add_cooldown(
            username, cooldown_until,
            f"{cfg.failure_threshold} consecutive failures",
        )
        return True

    # ----------------------------------------------------------------
    # Persisted search plans
    # ----------------------------------------------------------------
    #
    # All plan DDL lives in migrations/014_persisted_search_plans.sql.
    # These methods read/write only -- never CREATE/ALTER. The
    # consumed-attempt method (`record_consumed_search_attempt`) is the
    # one intentional exception to PipelineDB's autocommit rule: it must
    # log + advance cursor in one transaction. See the method docstring.

    def create_successful_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
        set_active: bool = True,
    ) -> int:
        """Create a successful plan + items; optionally make it the active
        plan and reset the request's cursor/cycle.

        Items must be non-empty (successful plans by contract carry at least
        one runnable slot); the CHECK + UNIQUE constraints in migration 014
        enforce non-empty queries and unique ``(plan, ordinal)``.

        Runs in a single transaction so the plan, its items, and the cursor
        update either all land or none do. Used by add-time generation,
        startup reconciliation, and explicit regeneration. Callers that need
        to supersede an existing active plan should call
        ``supersede_search_plan_with_replacement`` instead -- it takes the
        same shape and additionally flips the old active plan and updates
        the request cursor/cycle to point at the new one.
        """
        if not items:
            raise ValueError(
                "create_successful_search_plan requires at least one item; "
                "use create_failed_search_plan for empty results.")
        self._ensure_conn()
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    """
                    INSERT INTO search_plans
                        (request_id, generator_id, status,
                         metadata_snapshot, provenance, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        request_id,
                        generator_id,
                        PLAN_STATUS_ACTIVE,
                        _json_param(
                            metadata_snapshot, SearchPlanMetadataSnapshot),
                        _json_param(provenance, SearchPlanProvenance),
                        now,
                    ),
                )
                row = cur.fetchone()
                assert row is not None, "INSERT RETURNING must produce a row"
                plan_id = int(row["id"])

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO search_plan_items
                        (plan_id, ordinal, strategy, query,
                         canonical_query_key, repeat_group, provenance)
                    VALUES %s
                    """,
                    [
                        (
                            plan_id,
                            item.ordinal,
                            item.strategy,
                            item.query,
                            item.canonical_query_key,
                            item.repeat_group,
                            _json_param(
                                item.provenance, SearchPlanItemProvenance),
                        )
                        for item in items
                    ],
                )

                if set_active:
                    cur.execute(
                        """
                        UPDATE album_requests
                        SET active_plan_id = %s,
                            next_plan_ordinal = 0,
                            plan_cycle_count = 0,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (plan_id, now, request_id),
                    )
            self.conn.commit()
            return plan_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    def create_failed_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        failure_class: str,
        error_message: str | None = None,
        transient: bool,
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
    ) -> int:
        """Persist one generation failure attempt.

        ``transient=False`` -> deterministic sticky failure (no runnable
        query, missing required metadata): request stays wanted but is not
        searchable until a successful plan replaces it.

        ``transient=True`` -> retryable (resolver outage, etc.): startup
        reconciliation will retry on a later cycle.

        Either way, the request's existing active plan (if any) is left
        untouched -- failed regeneration must not disable a previously
        good plan.
        """
        status = (
            PLAN_STATUS_FAILED_TRANSIENT if transient
            else PLAN_STATUS_FAILED_DETERMINISTIC
        )
        cur = self._execute(
            """
            INSERT INTO search_plans
                (request_id, generator_id, status, failure_class,
                 metadata_snapshot, provenance, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                request_id,
                generator_id,
                status,
                failure_class,
                _json_param(metadata_snapshot, SearchPlanMetadataSnapshot),
                _json_param(provenance, SearchPlanProvenance),
                error_message,
            ),
        )
        row = cur.fetchone()
        assert row is not None, "INSERT RETURNING must produce a row"
        return int(row["id"])

    def supersede_search_plan_with_replacement(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
    ) -> int:
        """Create a new successful plan AND replace the existing active plan
        for this request, atomically.

        The previous active plan (if any) is flipped to status='superseded'
        with ``superseded_at`` and ``superseded_by_plan_id`` populated. The
        request's cursor/cycle is reset to ``(0, 0)`` and ``active_plan_id``
        repointed at the new plan.

        Used by explicit regeneration and by startup reconciliation when an
        old-generator plan is being replaced. Falls back to
        ``create_successful_search_plan(set_active=True)`` semantics when
        the request has no active plan yet.
        """
        if not items:
            raise ValueError(
                "supersede_search_plan_with_replacement requires items.")
        self._ensure_conn()
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                # Read current active plan id under the lock implied by the
                # transaction; NULL means "no replacement, just create+activate".
                cur.execute(
                    "SELECT active_plan_id FROM album_requests WHERE id = %s "
                    "FOR UPDATE",
                    (request_id,),
                )
                req_row = cur.fetchone()
                if req_row is None:
                    raise ValueError(
                        f"request {request_id} not found")
                old_active_id = req_row["active_plan_id"]

                # Detach the old active plan first so the partial unique
                # index "one active per request" lets us insert the new
                # active row.
                if old_active_id is not None:
                    cur.execute(
                        """
                        UPDATE search_plans
                        SET status = %s,
                            superseded_at = %s
                        WHERE id = %s
                        """,
                        (PLAN_STATUS_SUPERSEDED, now, old_active_id),
                    )

                cur.execute(
                    """
                    INSERT INTO search_plans
                        (request_id, generator_id, status,
                         metadata_snapshot, provenance, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        request_id,
                        generator_id,
                        PLAN_STATUS_ACTIVE,
                        _json_param(
                            metadata_snapshot, SearchPlanMetadataSnapshot),
                        _json_param(provenance, SearchPlanProvenance),
                        now,
                    ),
                )
                new_row = cur.fetchone()
                assert new_row is not None
                new_plan_id = int(new_row["id"])

                if old_active_id is not None:
                    cur.execute(
                        """
                        UPDATE search_plans
                        SET superseded_by_plan_id = %s
                        WHERE id = %s
                        """,
                        (new_plan_id, old_active_id),
                    )

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO search_plan_items
                        (plan_id, ordinal, strategy, query,
                         canonical_query_key, repeat_group, provenance)
                    VALUES %s
                    """,
                    [
                        (
                            new_plan_id,
                            item.ordinal,
                            item.strategy,
                            item.query,
                            item.canonical_query_key,
                            item.repeat_group,
                            _json_param(
                                item.provenance, SearchPlanItemProvenance),
                        )
                        for item in items
                    ],
                )

                cur.execute(
                    """
                    UPDATE album_requests
                    SET active_plan_id = %s,
                        next_plan_ordinal = 0,
                        plan_cycle_count = 0,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (new_plan_id, now, request_id),
                )
            self.conn.commit()
            return new_plan_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    def get_active_search_plan(
        self,
        request_id: int,
    ) -> ActiveSearchPlan | None:
        """Return the active plan + items + cursor state for one request.

        Returns ``None`` when the request has no active plan (either it was
        never generated, or the latest attempt failed deterministically).
        Use ``get_search_plan_inspection`` to also surface failed/superseded
        plans for human inspection.

        Single-query implementation: joins ``album_requests`` →
        ``search_plans`` → ``search_plan_items`` and aggregates items into
        a JSONB array via ``jsonb_agg(... ORDER BY spi.ordinal)
        FILTER (WHERE spi.id IS NOT NULL)``. The FILTER clause keeps the
        outer LEFT JOIN safe when a plan has zero items
        (``coalesce(..., '[]'::jsonb)`` returns an empty list rather than
        ``[null]``). Phase 2 calls this once per wanted album per cycle,
        so collapsing 2 RTTs to 1 saves ~1168 round-trips/cycle in prod.
        """
        cur = self._execute(
            """
            SELECT ar.next_plan_ordinal, ar.plan_cycle_count,
                   sp.id AS plan_id, sp.request_id, sp.generator_id,
                   sp.status, sp.failure_class, sp.metadata_snapshot,
                   sp.provenance, sp.error_message, sp.superseded_at,
                   sp.superseded_by_plan_id, sp.created_at,
                   COALESCE(
                     jsonb_agg(
                       jsonb_build_object(
                         'id', spi.id,
                         'plan_id', spi.plan_id,
                         'ordinal', spi.ordinal,
                         'strategy', spi.strategy,
                         'query', spi.query,
                         'canonical_query_key', spi.canonical_query_key,
                         'repeat_group', spi.repeat_group,
                         'provenance', spi.provenance
                       )
                       ORDER BY spi.ordinal ASC
                     ) FILTER (WHERE spi.id IS NOT NULL),
                     '[]'::jsonb
                   ) AS items_json
            FROM album_requests ar
            JOIN search_plans sp ON ar.active_plan_id = sp.id
            LEFT JOIN search_plan_items spi ON spi.plan_id = sp.id
            WHERE ar.id = %s
            GROUP BY ar.next_plan_ordinal, ar.plan_cycle_count,
                     sp.id, sp.request_id, sp.generator_id, sp.status,
                     sp.failure_class, sp.metadata_snapshot, sp.provenance,
                     sp.error_message, sp.superseded_at,
                     sp.superseded_by_plan_id, sp.created_at
            """,
            (request_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        plan = SearchPlanRow(
            id=int(row["plan_id"]),
            request_id=int(row["request_id"]),
            generator_id=row["generator_id"],
            status=row["status"],
            failure_class=row["failure_class"],
            metadata_snapshot=_metadata_snapshot_from_jsonb(
                row["metadata_snapshot"]),
            provenance=_plan_provenance_from_jsonb(row["provenance"]),
            error_message=row["error_message"],
            superseded_at=row["superseded_at"],
            superseded_by_plan_id=(
                int(row["superseded_by_plan_id"])
                if row["superseded_by_plan_id"] is not None else None),
            created_at=row["created_at"],
        )
        items = [
            SearchPlanItemRow(
                id=int(it["id"]),
                plan_id=int(it["plan_id"]),
                ordinal=int(it["ordinal"]),
                strategy=it["strategy"],
                query=it["query"],
                canonical_query_key=it["canonical_query_key"],
                repeat_group=it["repeat_group"],
                provenance=_item_provenance_from_jsonb(it["provenance"]),
            )
            for it in row["items_json"]
        ]
        return ActiveSearchPlan(
            plan=plan,
            items=items,
            next_ordinal=int(row["next_plan_ordinal"]),
            cycle_count=int(row["plan_cycle_count"]),
        )

    def is_request_plan_current(
        self,
        request_id: int,
        plan_id: int,
        plan_ordinal: int,
        cycle_count_snapshot: int,
    ) -> bool:
        """Stale-completion guard for active-state mutations.

        Returns True iff the request still points at the given plan, the
        cursor is still at the given ordinal, AND the plan_cycle_count
        still equals the snapshot the executor took at selection time.

        Used by ``lib.enqueue``, ``lib.download_ownership``, and
        ``lib.transitions`` to avoid claiming download ownership / mutating
        request status from a search completion that finished after the
        request was regenerated mid-flight (stale-completion
        contract). The atomic log+cursor write inside
        ``record_consumed_search_attempt`` is independent of this helper —
        callers do BOTH the search-log write and the active-state mutation
        guard.
        """
        cur = self._execute(
            """
            SELECT active_plan_id, next_plan_ordinal, plan_cycle_count
            FROM album_requests WHERE id = %s
            """,
            (request_id,),
        )
        row = cur.fetchone()
        if row is None:
            return False
        if row["active_plan_id"] != plan_id:
            return False
        if int(row["next_plan_ordinal"]) != plan_ordinal:
            return False
        if int(row["plan_cycle_count"]) != cycle_count_snapshot:
            return False
        return True

    def _fetch_plan_items(self, plan_id: int) -> list[SearchPlanItemRow]:
        cur = self._execute(
            """
            SELECT id, plan_id, ordinal, strategy, query,
                   canonical_query_key, repeat_group, provenance
            FROM search_plan_items
            WHERE plan_id = %s
            ORDER BY ordinal ASC
            """,
            (plan_id,),
        )
        return [
            SearchPlanItemRow(
                id=int(r["id"]),
                plan_id=int(r["plan_id"]),
                ordinal=int(r["ordinal"]),
                strategy=r["strategy"],
                query=r["query"],
                canonical_query_key=r["canonical_query_key"],
                repeat_group=r["repeat_group"],
                provenance=_item_provenance_from_jsonb(r["provenance"]),
            )
            for r in cur.fetchall()
        ]

    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return wanted rows whose active plan matches ``generator_id``.

        This is the **execution-eligibility** filter used by the Phase 2
        search loop. A wanted request is searchable only if:

          * ``status = 'wanted'`` (same as ``get_wanted``), and
          * ``next_retry_after`` is null or already due (same backoff
            semantics as ``get_wanted``), and
          * ``active_plan_id`` points at a row in ``search_plans`` whose
            ``status = 'active'`` AND ``generator_id = %s``.

        Rows with no active plan, a deterministic-failed-only plan, a
        transient-failed-only plan, or an old-generator active plan are
        excluded -- startup reconciliation owns repairing those before
        the next cycle.

        Forensic / dashboard / inspection callers should keep using the
        older ``get_wanted`` (no plan filter) so they can show every
        wanted row regardless of plan readiness.
        """
        now = datetime.now(timezone.utc)
        sql = """
            SELECT ar.* FROM album_requests ar
            JOIN search_plans sp ON ar.active_plan_id = sp.id
            WHERE ar.status = 'wanted'
              AND (ar.next_retry_after IS NULL OR ar.next_retry_after <= %s)
              AND sp.status = 'active'
              AND sp.generator_id = %s
            ORDER BY
              CASE
                WHEN COALESCE(ar.search_attempts, 0) = 0
                 AND COALESCE(ar.download_attempts, 0) = 0
                 AND COALESCE(ar.validation_attempts, 0) = 0
                THEN 0
                ELSE 1
              END,
              RANDOM()
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur = self._execute(sql, (now, generator_id))
        return [dict(r) for r in cur.fetchall()]

    def list_wanted_for_plan_reconciliation(
        self,
    ) -> list[WantedReconciliationCandidate]:
        """All-wanted scan for startup reconciliation.

        Ignores ``next_retry_after`` and the page-size limit that
        ``get_wanted`` applies. Used once per startup to decide which
        wanted rows need a generated/regenerated plan -- callers must
        compare ``active_plan_generator_id`` to the current generator id
        themselves.
        """
        cur = self._execute(
            """
            SELECT ar.id AS request_id,
                   CASE
                     WHEN sp.status = 'active' THEN ar.active_plan_id
                     ELSE NULL
                   END AS active_plan_id,
                   ar.next_plan_ordinal, ar.plan_cycle_count,
                   CASE
                     WHEN sp.status = 'active' THEN sp.generator_id
                     ELSE NULL
                   END AS active_plan_generator_id
            FROM album_requests ar
            LEFT JOIN search_plans sp ON ar.active_plan_id = sp.id
            WHERE ar.status = 'wanted'
            ORDER BY ar.id
            """
        )
        return [
            WantedReconciliationCandidate(
                request_id=int(r["request_id"]),
                active_plan_id=(
                    int(r["active_plan_id"])
                    if r["active_plan_id"] is not None else None),
                active_plan_generator_id=r["active_plan_generator_id"],
                next_plan_ordinal=int(r["next_plan_ordinal"]),
                plan_cycle_count=int(r["plan_cycle_count"]),
            )
            for r in cur.fetchall()
        ]

    def list_search_plan_classification_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, DryRunPlanClassification]:
        """Batch-fetch the per-request data dry-run classification needs.

        Replaces the per-row ``get_search_plan_inspection`` call inside
        ``startup_reconciliation._classify_dry_run`` (5 sequential
        queries × ~600 candidates ≈ 2,920 round-trips) with a single
        query.

        Returns one entry per request id passed in. Requests without
        any failed plan rows still get an entry whose generator-id
        fields are both ``None``. Requests not in ``request_ids`` are
        absent from the result. An empty input list returns ``{}``
        without hitting the DB.

        We use ``DISTINCT ON (request_id, status)`` ordered by
        ``created_at DESC, id DESC`` so each request gets at most one
        row per failure status -- the same row ``_latest()`` selects
        inside ``get_search_plan_inspection``.
        """
        if not request_ids:
            return {}
        # Initialise every requested id with a None/None entry so
        # callers don't have to handle "missing" vs. "no failed plan".
        out: dict[int, DryRunPlanClassification] = {
            int(rid): DryRunPlanClassification(
                request_id=int(rid),
                latest_failed_deterministic_generator_id=None,
                latest_failed_transient_generator_id=None,
            )
            for rid in request_ids
        }
        cur = self._execute(
            """
            SELECT DISTINCT ON (request_id, status)
                   request_id, status, generator_id, created_at
            FROM search_plans
            WHERE request_id = ANY(%s)
              AND status IN (%s, %s)
            ORDER BY request_id, status, created_at DESC, id DESC
            """,
            (
                list(out.keys()),
                PLAN_STATUS_FAILED_DETERMINISTIC,
                PLAN_STATUS_FAILED_TRANSIENT,
            ),
        )
        for r in cur.fetchall():
            rid = int(r["request_id"])
            current = out[rid]
            if r["status"] == PLAN_STATUS_FAILED_DETERMINISTIC:
                out[rid] = DryRunPlanClassification(
                    request_id=rid,
                    latest_failed_deterministic_generator_id=r[
                        "generator_id"],
                    latest_failed_transient_generator_id=current
                        .latest_failed_transient_generator_id,
                    latest_failed_transient_created_at=current
                        .latest_failed_transient_created_at,
                )
            elif r["status"] == PLAN_STATUS_FAILED_TRANSIENT:
                out[rid] = DryRunPlanClassification(
                    request_id=rid,
                    latest_failed_deterministic_generator_id=current
                        .latest_failed_deterministic_generator_id,
                    latest_failed_transient_generator_id=r[
                        "generator_id"],
                    latest_failed_transient_created_at=r["created_at"],
                )
        return out

    def get_search_plan_inspection(
        self,
        request_id: int,
    ) -> SearchPlanInspection:
        """Aggregate read for CLI/API inspection.

        Returns the active plan (with items + cursor), the latest
        deterministic and transient failed attempts (most recent of each),
        the count of superseded plans, and the count of historical
        search_log rows for this request that pre-date persisted plans.
        """
        active = self.get_active_search_plan(request_id)

        def _latest(status: str) -> SearchPlanRow | None:
            cur = self._execute(
                """
                SELECT id, request_id, generator_id, status, failure_class,
                       metadata_snapshot, provenance, error_message,
                       superseded_at, superseded_by_plan_id, created_at
                FROM search_plans
                WHERE request_id = %s AND status = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (request_id, status),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return SearchPlanRow(
                id=int(row["id"]),
                request_id=int(row["request_id"]),
                generator_id=row["generator_id"],
                status=row["status"],
                failure_class=row["failure_class"],
                metadata_snapshot=_metadata_snapshot_from_jsonb(
                    row["metadata_snapshot"]),
                provenance=_plan_provenance_from_jsonb(row["provenance"]),
                error_message=row["error_message"],
                superseded_at=row["superseded_at"],
                superseded_by_plan_id=(
                    int(row["superseded_by_plan_id"])
                    if row["superseded_by_plan_id"] is not None else None),
                created_at=row["created_at"],
            )

        latest_det = _latest(PLAN_STATUS_FAILED_DETERMINISTIC)
        latest_trans = _latest(PLAN_STATUS_FAILED_TRANSIENT)

        sup_cur = self._execute(
            "SELECT COUNT(*) AS c FROM search_plans "
            "WHERE request_id = %s AND status = %s",
            (request_id, PLAN_STATUS_SUPERSEDED),
        )
        sup_row = sup_cur.fetchone()
        superseded_count = int(sup_row["c"]) if sup_row is not None else 0

        legacy_cur = self._execute(
            "SELECT COUNT(*) AS c FROM search_log "
            "WHERE request_id = %s AND plan_id IS NULL",
            (request_id,),
        )
        legacy_row = legacy_cur.fetchone()
        legacy_count = int(legacy_row["c"]) if legacy_row is not None else 0

        return SearchPlanInspection(
            request_id=request_id,
            active=active,
            latest_failed_deterministic=latest_det,
            latest_failed_transient=latest_trans,
            superseded_count=superseded_count,
            legacy_search_log_count=legacy_count,
        )

    def get_search_plan_stats(
        self,
        request_id: int,
        *,
        current_only: bool = True,
        prefetched_history: list[dict[str, object]] | None = None,
    ) -> SearchPlanStats:
        """Aggregate plan-aware ``search_log`` rows into usefulness stats.

        Two grouping levels per cohort:
          * **slots** keyed by ``(plan_id, ordinal, strategy)`` —
            ordinal-ordered.
          * **query_groups** keyed by ``(plan_id, repeat_group,
            canonical_query_key)`` — stable order by
            ``(repeat_group, canonical_query_key)``.

        ``current_only=True`` (default) returns the active-plan cohort
        in ``current`` and an empty ``superseded_and_legacy`` cohort.
        ``current_only=False`` populates both cohorts from every plan
        the request ever had plus a ``legacy_bucket`` for pre-plan rows.

        Cache attribution is reported as ``cycle_only`` because
        ``search_log`` has no per-search cache columns today (cache
        counters live on ``cycle_metrics`` — see
        ``migrations/011_cycle_metrics.sql``). If a future migration
        adds them, flip ``cache_per_search_available=True`` here.
        """
        active = self.get_active_search_plan(request_id)
        active_plan_id = active.plan.id if active is not None else None

        history = (prefetched_history if prefetched_history is not None
                   else self.get_search_history(request_id))

        plan_aware = [r for r in history if r.get("plan_id") is not None]
        legacy = [r for r in history if r.get("plan_id") is None]

        current_rows = (
            [r for r in plan_aware if r.get("plan_id") == active_plan_id]
            if active_plan_id is not None else []
        )
        if current_only:
            other_rows: list[dict[str, object]] = []
            other_legacy: list[dict[str, object]] = []
        else:
            other_rows = [r for r in plan_aware
                          if r.get("plan_id") != active_plan_id]
            other_legacy = legacy

        current_bucket = _build_stats_bucket(
            plan_aware_rows=current_rows, legacy_rows=[],
            include_legacy_bucket=False,
        )
        other_bucket = _build_stats_bucket(
            plan_aware_rows=other_rows, legacy_rows=other_legacy,
            include_legacy_bucket=True,
        )
        return SearchPlanStats(
            request_id=request_id,
            current=current_bucket,
            superseded_and_legacy=other_bucket,
        )

    def record_consumed_search_attempt(
        self,
        attempt: ConsumedAttemptInput,
    ) -> ConsumedAttemptResult:
        """Atomically log a consumed search attempt and advance/wrap cursor.

        This is the one intentional exception to PipelineDB's
        ``autocommit=True`` rule. The transaction does:

          1. Re-read the request's ``active_plan_id`` and
             ``next_plan_ordinal`` ``FOR UPDATE``.
          2. Insert one ``search_log`` row carrying full plan context and
             a ``plan_cycle_snapshot``.
          3. If the executing plan/ordinal still match the active state:
             advance ordinal (or wrap to 0 + cycle++) and stamp
             ``cursor_update_status`` accordingly; flagged ``advanced``
             or ``wrapped`` on the log row.
          4. Otherwise: leave the cursor alone, flag the log row as
             ``stale`` with ``stale_reason='regenerated'`` and
             ``execution_stage='stale_completion'``.

        Either every write commits or none do. Callers must NOT separately
        call ``log_search`` for the same accepted attempt -- this method
        is the consumed-attempt seam.

        ``apply_scheduler_attempt=True`` increments ``search_attempts`` and
        sets backoff inside the same transaction, so the legacy
        ``search_attempts`` field stays a scheduler-only counter.
        """
        self._ensure_conn()
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    "SELECT active_plan_id, next_plan_ordinal, "
                    "       plan_cycle_count "
                    "FROM album_requests WHERE id = %s FOR UPDATE",
                    (attempt.request_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(
                        f"request {attempt.request_id} not found")
                active_plan_id = row["active_plan_id"]
                next_ordinal = int(row["next_plan_ordinal"])
                cycle_count = int(row["plan_cycle_count"])

                cur.execute(
                    """
                    SELECT 1
                    FROM search_plan_items spi
                    JOIN search_plans sp ON sp.id = spi.plan_id
                    WHERE spi.id = %s
                      AND spi.plan_id = %s
                      AND sp.request_id = %s
                    """,
                    (
                        attempt.plan_item_id,
                        attempt.plan_id,
                        attempt.request_id,
                    ),
                )
                if cur.fetchone() is None:
                    raise ValueError(
                        f"plan_item_id={attempt.plan_item_id} does not "
                        f"belong to plan_id={attempt.plan_id} for "
                        f"request_id={attempt.request_id}")

                is_stale = (
                    active_plan_id != attempt.plan_id
                    or next_ordinal != attempt.plan_ordinal
                    or cycle_count != attempt.cycle_count_snapshot
                )

                if is_stale:
                    cursor_update_status = CURSOR_UPDATE_STALE
                    execution_stage = SEARCH_LOG_STAGE_STALE_COMPLETION
                    stale_reason = "regenerated"
                    new_next_ordinal = next_ordinal
                    new_cycle = cycle_count
                else:
                    execution_stage = SEARCH_LOG_STAGE_ACCEPTED
                    stale_reason = None
                    plan_item_count = max(int(attempt.plan_item_count), 0)
                    if plan_item_count == 0:
                        # Pathological: caller said no items. Treat as
                        # advanced-without-wrap to avoid /0 wrap math; the
                        # generator's CHECK + service contract should
                        # prevent this in practice.
                        cursor_update_status = CURSOR_UPDATE_ADVANCED
                        new_next_ordinal = next_ordinal + 1
                        new_cycle = cycle_count
                    elif attempt.plan_ordinal >= plan_item_count - 1:
                        cursor_update_status = CURSOR_UPDATE_WRAPPED
                        new_next_ordinal = 0
                        new_cycle = cycle_count + 1
                    else:
                        cursor_update_status = CURSOR_UPDATE_ADVANCED
                        new_next_ordinal = next_ordinal + 1
                        new_cycle = cycle_count

                cur.execute(
                    """
                    INSERT INTO search_log (
                        request_id, query, result_count, elapsed_s, outcome,
                        candidates, variant, final_state,
                        browse_time_s, match_time_s,
                        peers_browsed, peers_browsed_lazy, fanout_waves,
                        plan_id, plan_item_id, plan_ordinal,
                        plan_strategy, plan_canonical_query_key,
                        plan_repeat_group, plan_generator_id,
                        execution_stage, attempt_consumed,
                        cursor_update_status, stale_reason,
                        plan_cycle_snapshot
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s
                    )
                    RETURNING id
                    """,
                    (
                        attempt.request_id,
                        attempt.query,
                        attempt.result_count,
                        attempt.elapsed_s,
                        attempt.outcome,
                        attempt.candidates_json,
                        attempt.variant,
                        attempt.final_state,
                        attempt.browse_time_s,
                        attempt.match_time_s,
                        attempt.peers_browsed,
                        attempt.peers_browsed_lazy,
                        attempt.fanout_waves,
                        attempt.plan_id,
                        attempt.plan_item_id,
                        attempt.plan_ordinal,
                        attempt.plan_strategy,
                        attempt.plan_canonical_query_key,
                        attempt.plan_repeat_group,
                        attempt.plan_generator_id,
                        execution_stage,
                        not is_stale,
                        cursor_update_status,
                        stale_reason,
                        attempt.cycle_count_snapshot,
                    ),
                )
                log_row = cur.fetchone()
                assert log_row is not None
                search_log_id = int(log_row["id"])

                # Cursor + scheduler/backoff writes only when not stale.
                if not is_stale:
                    cur.execute(
                        """
                        UPDATE album_requests
                        SET next_plan_ordinal = %s,
                            plan_cycle_count = %s,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (new_next_ordinal, new_cycle, now,
                         attempt.request_id),
                    )

                    if (
                        attempt.apply_scheduler_attempt
                        and not attempt.scheduler_success
                    ):
                        cur.execute(
                            """
                            UPDATE album_requests
                            SET search_attempts = COALESCE(search_attempts, 0) + 1,
                                last_attempt_at = %s,
                                updated_at = %s
                            WHERE id = %s
                            RETURNING search_attempts
                            """,
                            (now, now, attempt.request_id),
                        )
                        s_row = cur.fetchone()
                        assert s_row is not None
                        new_count = int(s_row["search_attempts"])
                        backoff_minutes = min(
                            BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                            BACKOFF_MAX_MINUTES,
                        )
                        cur.execute(
                            "UPDATE album_requests "
                            "SET next_retry_after = %s WHERE id = %s",
                            (now + timedelta(minutes=backoff_minutes),
                             attempt.request_id),
                        )
                    elif (
                        attempt.apply_scheduler_attempt
                        and attempt.scheduler_success
                    ):
                        # Reset retry-pacing on a useful slot. We do not
                        # reset attempt counters -- those are forensic.
                        cur.execute(
                            "UPDATE album_requests "
                            "SET last_attempt_at = %s, updated_at = %s "
                            "WHERE id = %s",
                            (now, now, attempt.request_id),
                        )

            self.conn.commit()
            return ConsumedAttemptResult(
                search_log_id=search_log_id,
                cursor_update_status=cursor_update_status,
                new_next_ordinal=new_next_ordinal,
                new_cycle_count=new_cycle,
                is_stale=is_stale,
            )
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    def record_non_consuming_search_attempt(
        self,
        attempt: NonConsumingAttemptInput,
    ) -> int:
        """Record a pre-attempt / setup-failure search_log row.

        Always non-consuming -- cursor and cycle are never touched. Plan
        context fields are nullable because the failure may have happened
        before the executor resolved a plan/item. When
        ``apply_scheduler_attempt=True`` this also increments
        ``search_attempts`` and applies exponential backoff so a stuck
        request cannot spin.

        Returns the new ``search_log.id``.
        """
        self._ensure_conn()
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    "SELECT plan_cycle_count "
                    "FROM album_requests WHERE id = %s FOR UPDATE",
                    (attempt.request_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(
                        f"request {attempt.request_id} not found")
                cycle_snapshot = int(row["plan_cycle_count"])

                cur.execute(
                    """
                    INSERT INTO search_log (
                        request_id, query, result_count, elapsed_s, outcome,
                        final_state,
                        plan_id, plan_item_id, plan_ordinal,
                        plan_strategy, plan_canonical_query_key,
                        plan_repeat_group, plan_generator_id,
                        execution_stage, attempt_consumed,
                        cursor_update_status, plan_cycle_snapshot
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s
                    )
                    RETURNING id
                    """,
                    (
                        attempt.request_id,
                        attempt.query,
                        attempt.result_count,
                        attempt.elapsed_s,
                        attempt.outcome,
                        attempt.final_state,
                        attempt.plan_id,
                        attempt.plan_item_id,
                        attempt.plan_ordinal,
                        attempt.plan_strategy,
                        attempt.plan_canonical_query_key,
                        attempt.plan_repeat_group,
                        attempt.plan_generator_id,
                        SEARCH_LOG_STAGE_PRE_ATTEMPT,
                        False,  # attempt_consumed
                        CURSOR_UPDATE_UNCHANGED,
                        cycle_snapshot,
                    ),
                )
                log_row = cur.fetchone()
                assert log_row is not None
                search_log_id = int(log_row["id"])

                if attempt.apply_scheduler_attempt:
                    cur.execute(
                        """
                        UPDATE album_requests
                        SET search_attempts = COALESCE(search_attempts, 0) + 1,
                            last_attempt_at = %s,
                            updated_at = %s
                        WHERE id = %s
                        RETURNING search_attempts
                        """,
                        (now, now, attempt.request_id),
                    )
                    s_row = cur.fetchone()
                    assert s_row is not None
                    new_count = int(s_row["search_attempts"])
                    backoff_minutes = min(
                        BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                        BACKOFF_MAX_MINUTES,
                    )
                    cur.execute(
                        "UPDATE album_requests "
                        "SET next_retry_after = %s WHERE id = %s",
                        (now + timedelta(minutes=backoff_minutes),
                         attempt.request_id),
                    )
            self.conn.commit()
            return search_log_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

    # --- Retry logic ---

    def record_attempt(self, request_id, attempt_type):
        col = f"{attempt_type}_attempts"
        now = datetime.now(timezone.utc)

        # Atomic increment + fetch in single statement (avoids TOCTOU race)
        cur = self._execute(f"""
            UPDATE album_requests
            SET {col} = COALESCE({col}, 0) + 1,
                last_attempt_at = %s,
                updated_at = %s
            WHERE id = %s
            RETURNING {col}
        """, (now, now, request_id))
        row = cur.fetchone()
        assert row is not None, f"Request {request_id} not found"
        new_count: int = int(row[col])

        # Exponential backoff: base * 2^(attempts-1), capped
        backoff_minutes = min(
            BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
            BACKOFF_MAX_MINUTES,
        )
        next_retry = now + timedelta(minutes=backoff_minutes)

        self._execute("""
            UPDATE album_requests
            SET next_retry_after = %s
            WHERE id = %s
        """, (next_retry, request_id))

    # --- bad_audio_hashes (curator-reported bad-rip audio-content hashes) ---

    def add_bad_audio_hashes(
        self,
        request_id: int,
        reported_username: str | None,
        reason: str | None,
        hashes: list[BadAudioHashInput],
    ) -> int:
        """Insert curator-reported bad-rip hashes; return count of NEW rows.

        Single multi-row INSERT with ON CONFLICT (hash_value, audio_format)
        DO NOTHING — re-reporting the same content on a second click is a
        no-op (returns 0). Per Key Technical Decision in the plan,
        request_id is intentionally NOT part of the unique key.
        """
        if not hashes:
            return 0
        values_sql = ",".join(["(%s, %s, %s, %s, %s)"] * len(hashes))
        params: list[Any] = []
        for h in hashes:
            params.extend([
                psycopg2.Binary(h.hash_value),
                h.audio_format,
                request_id,
                reported_username,
                reason,
            ])
        cur = self._execute(f"""
            INSERT INTO bad_audio_hashes
                (hash_value, audio_format, request_id, reported_username, reason)
            VALUES {values_sql}
            ON CONFLICT (hash_value, audio_format) DO NOTHING
            RETURNING id
        """, tuple(params))
        inserted = cur.fetchall()
        return len(inserted)

    def lookup_bad_audio_hash(
        self,
        hash_value: bytes,
        audio_format: str,
    ) -> BadAudioHashRow | None:
        """Point-lookup by (hash_value, audio_format). Returns None on miss."""
        cur = self._execute("""
            SELECT id, hash_value, audio_format, request_id,
                   reported_username, reason, reported_at
            FROM bad_audio_hashes
            WHERE hash_value = %s AND audio_format = %s
            LIMIT 1
        """, (psycopg2.Binary(hash_value), audio_format))
        row = cur.fetchone()
        if row is None:
            return None
        # psycopg2 returns BYTEA as memoryview; coerce to bytes for the typed row.
        raw = row["hash_value"]
        if isinstance(raw, memoryview):
            raw = bytes(raw)
        return BadAudioHashRow(
            id=int(row["id"]),
            hash_value=raw,
            audio_format=str(row["audio_format"]),
            request_id=(int(row["request_id"])
                        if row["request_id"] is not None else None),
            reported_username=row["reported_username"],
            reason=row["reason"],
            reported_at=row["reported_at"],
        )

    def has_any_bad_audio_hashes(self) -> bool:
        """Empty-table fast-path probe; uncached at this layer."""
        cur = self._execute(
            "SELECT 1 FROM bad_audio_hashes LIMIT 1"
        )
        return cur.fetchone() is not None

    def get_recent_successful_uploader(
        self,
        request_id: int,
    ) -> str | None:
        """Return the most recent successful uploader for this request.

        Used by the ban-source route to resolve `reported_username`
        server-side. Considers both `success` and `force_import` outcomes.
        """
        cur = self._execute("""
            SELECT soulseek_username
            FROM download_log
            WHERE request_id = %s
              AND outcome IN ('success', 'force_import')
              AND soulseek_username IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
        """, (request_id,))
        row = cur.fetchone()
        return row["soulseek_username"] if row else None

    def get_active_import_job_for_request(
        self,
        request_id: int,
    ) -> dict[str, Any] | None:
        """Return the most recent queued/running import job for this request.

        Used by the ban-source route's importer-race check (E1.3 in the
        plan). Returns the raw row dict (not an `ImportJob`) because the
        caller only inspects `status` for the 409 decision.
        """
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE request_id = %s
              AND status IN ('queued', 'running')
            ORDER BY id DESC
            LIMIT 1
        """, (request_id,))
        row = cur.fetchone()
        return dict(row) if row else None
