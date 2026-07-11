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
# Explicit redundant aliases are the exact baseline for the pre-split flat
# PipelineDB namespace. New unused imports remain F401 failures.
import json as json
import logging
import os
import zlib
from contextlib import contextmanager as contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta as timedelta, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable as Iterable,
    Iterator as Iterator,
    Optional,
)

if TYPE_CHECKING:
    from lib.quality import CandidateScore as CandidateScore
    from lib.triage_service import ParsedTriageFilter as ParsedTriageFilter
    from lib.unfindable_detection_service import (
        UnfindableSearchLogSignal as UnfindableSearchLogSignal,
    )

import psycopg2
import psycopg2.extras
import msgspec

from lib.import_queue import (
    IMPORT_JOB_YOUTUBE as IMPORT_JOB_YOUTUBE,
    ImportJob as ImportJob,
    IMPORT_JOB_PREVIEW_WAITING as IMPORT_JOB_PREVIEW_WAITING,
    validate_preview_failure_status as validate_preview_failure_status,
    validate_job_type as validate_job_type,
    validate_payload as validate_payload,
    validate_status as validate_status,
)
from lib.quality import (
    AlbumQualityEvidence as AlbumQualityEvidence,
    AlbumQualityEvidenceFile as AlbumQualityEvidenceFile,
    AlbumQualityV0Metric as AlbumQualityV0Metric,
    AudioQualityMeasurement as AudioQualityMeasurement,
    CooldownConfig as CooldownConfig,
    SpectralMeasurement,
    V0ProbeEvidence,
    VerifiedLosslessProof as VerifiedLosslessProof,
    should_cooldown as should_cooldown,
)
from lib.release_identity import (
    ReleaseIdentity as ReleaseIdentity,
    normalize_release_id as normalize_release_id,
)
from lib.search_classification import (
    SearchSummary as _ImportedSearchSummary,
    classify_failure_class as _imported_classify_failure_class,
)

_SearchSummary = _ImportedSearchSummary
_classify_failure_class = _imported_classify_failure_class
del _ImportedSearchSummary, _imported_classify_failure_class

logger = logging.getLogger(__name__)

DEFAULT_DSN = os.environ.get("PIPELINE_DB_DSN", "postgresql://cratedigger@localhost/cratedigger")

# Exponential backoff: base_minutes * 2^(attempts-1), capped at max
BACKOFF_BASE_MINUTES = 30
BACKOFF_MAX_MINUTES = 60 * 4  # 4 hours — caps steady-state at ~6 searches/release/day
DASHBOARD_WINDOWS: tuple[tuple[str, int], ...] = (("24h", 24), ("6h", 6))
DASHBOARD_WANTED_TREND_WINDOWS: tuple[tuple[str, int], ...] = (
    ("6h", 6),
    ("24h", 24),
    ("7d", 24 * 7),
)
# Operator-facing dashboard semantics: active downloads are still wanted
# backlog, just in the acquisition sub-state.
DASHBOARD_WANTED_BACKLOG_STATUSES: tuple[str, ...] = ("wanted", "downloading")


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


def _peer_hash(username: str) -> str:
    # Label stays "peer-dir-user" — migration 039 backfilled
    # ``peer_observations`` from the combo table's ``username_hash``
    # column, so changing the label would orphan every backfilled row.
    return _stable_hash("peer-dir-user", username)

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

# Per-source lock for destructive Wrong Matches cleanup. Key =
# ``wrong_match_cleanup_lock_key(request_id, download_log_id, source_path)``.
# ``0x574D434C`` = ASCII "WMCL", recognisable in ``pg_locks``.
ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP = 0x574D434C

# Singleton YouTube-ingest worker lock. The DB-side partial unique index
# on ``download_log`` (``one_youtube_running_per_request``) serializes
# per-request submissions, but a second worker process running
# concurrently could race on draining the queue. The worker takes this
# process-wide lock at startup before sweeping orphans or claiming the
# next ``youtube_running`` row.
# ``0x59544942`` = ASCII "YTIB" (YouTube InBound), recognisable in
# ``pg_locks``.
ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST = 0x59544942


def release_id_to_lock_key(mb_release_id: str) -> int:
    """Map an ``mb_release_id`` string to a stable int32 advisory-lock key.

    PostgreSQL's two-arg ``pg_try_advisory_lock`` function takes signed
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


def wrong_match_cleanup_lock_key(
    request_id: int | None,
    download_log_id: int,
    source_path: str | None,
) -> int:
    """Map a Wrong Matches source row to a stable int32 advisory-lock key."""
    parts = (
        str(request_id) if request_id is not None else "",
        str(int(download_log_id)),
        str(source_path or "").strip(),
    )
    return zlib.crc32("\0".join(parts).encode("utf-8")) & 0x7FFFFFFF

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


@dataclass(frozen=True)
class AddRequestInput:
    """Typed payload for inserting one ``album_requests`` row.

    Every field name IS an ``album_requests`` column name; ``add_request``
    derives the INSERT column list directly from these fields, so a field can
    never silently drift from the SQL (the ``album_title`` class of bug that
    #382 Layer 1 targets — a column present in the payload but missing from
    the hand-written INSERT). ``created_at`` / ``updated_at`` are stamped by
    the write (``NOW()``), not carried here.

    The fields-are-a-subset-of-columns invariant is enforced at test time by
    ``tests/test_pipeline_db_column_contract.py``. ``@dataclass`` (not
    ``msgspec.Struct``) because the payload never crosses JSON — it round-trips
    Python -> PostgreSQL only, exactly like ``BadAudioHashInput`` above.
    """
    artist_name: str
    album_title: str
    source: str
    mb_release_id: str | None = None
    mb_release_group_id: str | None = None
    mb_artist_id: str | None = None
    discogs_release_id: str | None = None
    year: int | None = None
    release_group_year: int | None = None
    country: str | None = None
    format: str | None = None
    source_path: str | None = None
    reasoning: str | None = None
    status: str = "wanted"
    is_va_compilation: bool = False


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
    """Typed JSONB boundary for ``search_plans.metadata_snapshot``.

    Mirrors the subset of ``ReleaseSnapshot`` an operator triaging an
    active plan would want to see without joining back to
    ``album_requests``. ``release_group_year``, ``is_va_compilation``,
    and ``catalog_number`` were added in PR2 of search-plan iter2 to
    close the asymmetry where the dict-builder wrote ``release_group_year``
    but the Struct didn't declare it (silently dropped on decode).
    ``track_artists`` is intentionally NOT mirrored — per-track lists
    can run 50+ entries on box sets and bloat JSONB; operators read
    ``album_tracks.track_artist`` directly.
    """

    artist_name: str | None = None
    album_title: str | None = None
    year: Any = None
    track_count: int | None = None
    redownload: bool = False
    prepend_artist: bool = False
    release_group_year: int | None = None
    is_va_compilation: bool = False
    catalog_number: str | None = None


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
    # Count of dirs the asymmetric pre-filter rejected before browse
    # during this search's find_download walk; persisted on
    # ``search_log.pre_filter_skip_count``. Default 0 keeps existing
    # callers compatible.
    pre_filter_skip_count: int = 0
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
    # U11 forensics (R22-R27): the executor populates these from the
    # SearchResult immediately before recording. All nullable so the
    # executor can leave a column NULL when the upstream signal is
    # genuinely absent (e.g. ``matcher_score_top1`` is NULL on
    # ``outcome="no_results"`` because the matcher never ran).
    rejection_reason: str | None = None
    result_count_uncapped: int | None = None
    query_token_count: int | None = None
    query_distinct_token_count: int | None = None
    expected_track_count: int | None = None
    matcher_score_top1: float | None = None
    query_template: str | None = None


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
    # Pre-filter skip count for the failed attempt. Almost always 0
    # because the matcher rarely runs on pre-attempt failures, but
    # plumbed through so the column is consistently populated.
    pre_filter_skip_count: int = 0
    # U11 forensics (R22-R27). Same shape / nullability as
    # ``ConsumedAttemptInput``. Most pre-attempt failures only carry
    # ``query_token_count`` / ``query_distinct_token_count`` /
    # ``query_template`` / ``expected_track_count`` — the matcher
    # never ran so ``rejection_reason`` / ``matcher_score_top1`` /
    # ``result_count_uncapped`` are typically NULL.
    rejection_reason: str | None = None
    result_count_uncapped: int | None = None
    query_token_count: int | None = None
    query_distinct_token_count: int | None = None
    expected_track_count: int | None = None
    matcher_score_top1: float | None = None
    query_template: str | None = None


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
class SearchLogHistoryPage:
    """One paginated slice of ``search_log`` rows for a single request.

    Returned by ``PipelineDB.get_search_history_page``. ``rows`` is an
    ordered list (newest first) of full ``search_log`` row dicts —
    JSONB columns (``candidates``) are already deserialized by
    psycopg2's ``DictRow`` pipeline. ``next_before_id`` seeds the next
    page's ``before_id`` cursor; ``None`` means this page exhausted the
    history. Internal-only typed result; the route/CLI hand back a dict
    tree on the wire.
    """

    rows: list[dict[str, object]]
    next_before_id: int | None


@dataclass(frozen=True)
class SaturationSummary:
    """U7: aggregate saturation/forensic counts for one request over a window.

    Returned by ``PipelineDB.get_saturation_summary``. A search counts as
    "saturated" when its ``final_state`` contains ``LimitReached`` (e.g.
    ``Completed, ResponseLimitReached`` or ``Completed, FileLimitReached``)
    — slskd hit its response/file ceiling before the search naturally
    finished, so the result set is a truncated head sample rather than
    the full picture. High saturation rates indicate the queries are too
    generic for the album the operator is trying to find.

    ``total_pre_filter_skips`` rolls up the U2 ``pre_filter_skip_count``
    column — how many candidate peer dirs the asymmetric matcher
    pre-filter rejected before browse over the same window.

    ``saturation_rate`` is ``saturated_searches / total_searches`` with
    an explicit ``0.0`` fallback when ``total_searches == 0`` (NOT NaN
    — callers serialise this to JSON and NaN would break the
    contract). Computed in Python after the aggregate fetch.

    The summary is the data layer for the future search-plan dashboard
    (see ``docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md``);
    today it ships via ``pipeline-cli search-plan saturation`` and
    ``GET /api/pipeline/<id>/search-plan/saturation``.
    """

    total_searches: int
    saturated_searches: int
    saturation_rate: float
    total_pre_filter_skips: int
    window_days: int


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


class MbidCollisionError(Exception):
    """Raised by ``PipelineDB.supersede_request_mbid`` when the target MBID
    already exists in ``album_requests`` (UNIQUE violation on insert)."""


class SupersedeRaceError(Exception):
    """Raised by ``PipelineDB.supersede_request_mbid`` when the old row's
    ``UPDATE ... WHERE status != 'replaced'`` matched zero rows — another
    session already superseded the row between our row-lock acquisition
    and the UPDATE. Caller maps this to a transient retry outcome.
    """


class YoutubeInFlightError(Exception):
    """Raised by ``PipelineDB.insert_youtube_running`` when the partial
    unique index ``one_youtube_running_per_request`` (migration 037)
    rejects the insert because a prior ``youtube_running`` row already
    exists for the same ``request_id``.

    The caller (``YoutubeIngestService.submit``) maps this to the
    ``in_flight`` outcome and returns the existing ``download_log_id`` in
    ``SubmitResult.detail`` so the operator can observe the in-flight
    submission they collided with.
    """

    def __init__(self, request_id: int, existing_download_log_id: int | None) -> None:
        super().__init__(
            f"YouTube rescue already in flight for request_id={request_id}"
        )
        self.request_id = request_id
        self.existing_download_log_id = existing_download_log_id


# ---------------------------------------------------------------------------
# YouTube album-mapping persisted JSONB shapes (moved from
# lib/youtube_album_service.py, #546 W3) — wire-boundary structs for the
# durable ``youtube_album_mappings`` cache.
# ---------------------------------------------------------------------------
#
# ``youtube_album_mappings.yt_tracks`` and ``.distances`` are JSONB
# columns. Per the wire-boundary rule in ``.claude/rules/code-quality.md``,
# anything that crosses JSON gets a typed Struct and validates at the
# decode site — we cannot rely on Pyright seeing into ``dict.get()``.
# ``msgspec.convert`` is the read-side detector for malformed rows.
#
# These live in the DB layer (not the service layer that produces and
# consumes them) because ``upsert_youtube_album_mapping`` derives its
# INSERT column list from ``msgspec.structs.fields(PersistedYoutubeRow)``
# at runtime — the DB layer needs the type to do that, and the DB layer
# importing UP from the service layer would be a layering violation /
# import cycle. ``lib.youtube_album_service`` imports these back from
# ``lib.pipeline_db``.


class PersistedTrack(msgspec.Struct, kw_only=True):
    """One persisted track inside ``yt_tracks`` JSONB.

    ``video_id`` is used by the YT rescue ingest audit trail to persist
    the exact per-track videos selected from the resolver row.
    """

    title: Optional[str] = None
    artists: Optional[list[dict[str, Any]]] = None
    length_seconds: Optional[float] = None
    track_number: Optional[int] = None
    disc_number: Optional[int] = None
    video_id: Optional[str] = None


class PersistedDistance(msgspec.Struct, kw_only=True):
    """One persisted per-pair distance inside ``distances`` JSONB."""

    mbid: Optional[str] = None
    outcome: Optional[str] = None
    distance: Optional[float] = None
    components: Optional[dict[str, float]] = None
    matched_tracks: Optional[int] = None
    total_local_tracks: Optional[int] = None
    total_mb_tracks: Optional[int] = None
    extra_local_tracks: Optional[int] = None
    extra_mb_tracks: Optional[int] = None
    error_message: Optional[str] = None


class PersistedYoutubeRow(msgspec.Struct, kw_only=True):
    """One persisted row in ``youtube_album_mappings``.

    Outer columns (``id``, ``release_group_identifier``, ``source``,
    ``resolved_at``) aren't carried here — the read path
    (``get_youtube_album_mapping``) keys by
    ``(release_group_identifier, source)`` so those fields are
    redundant. JSONB columns are decoded via ``msgspec.convert``;
    everything else is row metadata.

    Every OTHER field name IS a ``youtube_album_mappings`` column name;
    ``upsert_youtube_album_mapping`` derives the INSERT column list
    directly from these fields (``msgspec.structs.fields``), so a field
    can never silently drift from the SQL (the ``album_title`` class of
    bug migration 036 fixed — a column present in the payload but
    missing from the hand-written INSERT). The fields-are-a-subset-of-
    columns invariant is enforced at test time by
    ``tests/test_pipeline_db_column_contract.py``.
    """

    # Required-on-the-wire fields: the writer always populates them
    # (round 2 maintainability-2). Declaring them ``str``/``int`` makes
    # ``msgspec.convert`` reject malformed JSONB rows at the wire seam
    # rather than silently producing default-valued objects downstream.
    yt_browse_id: str
    yt_url: str
    yt_track_count: int
    # Genuinely optional: ``yt_audio_playlist_id`` and ``yt_year`` are
    # documented NULLable in migration 034.
    yt_audio_playlist_id: Optional[str] = None
    yt_year: Optional[int] = None
    # Album-level facts persisted alongside the row so the cache
    # rehydration in ``_rows_to_youtube_releases`` produces SyntheticItem
    # values structurally identical to the fresh-resolve path. Both are
    # nullable to allow legacy rows written before migration 036 (none
    # in production yet, but the column is nullable per the migration).
    album_title: Optional[str] = None
    album_artist: Optional[str] = None
    yt_tracks: list[PersistedTrack] = msgspec.field(default_factory=list)
    distances: list[PersistedDistance] = msgspec.field(default_factory=list)


class TransferLedgerRow(msgspec.Struct, kw_only=True):
    """One write-ahead row for ``slskd_transfer_ledger`` (migration 045,
    issue #571 good-citizen doctrine, T1).

    Every field name IS a ``slskd_transfer_ledger`` column name --
    ``record_transfer_enqueue`` derives the INSERT column list directly
    from these fields (``msgspec.structs.fields``), the struct-typed
    write pattern #565 established for ``PersistedYoutubeRow``, so a
    field can never silently drift from the SQL (the ``album_title``
    class of bug migration 036 fixed). The fields-are-a-subset-of-columns
    invariant is enforced at test time by
    ``tests/test_pipeline_db_column_contract.py``.

    Deliberately NOT part of this Struct: ``enqueued_at`` defaults from
    the DB (``DEFAULT now()``); ``transfer_id`` is never known yet at
    write-ahead time (the row is inserted BEFORE the POST that would
    return it); ``local_path``/``completed_at`` are stamped later, only
    by event ingestion (``stamp_transfer_completion``, T2), never at
    enqueue time.
    """

    request_id: int
    username: str
    filename: str
    attempt_fingerprint: Optional[str] = None


@dataclass(frozen=True)
class TransferIdOwnership:
    """Ledger ``transfer_id`` membership, partitioned by completion stamp
    (#571 PR 5) -- what ``lib.slskd_transfers.purge_completed_transfers``
    needs to classify a live completed slskd transfer without a second
    query per row.

    ``stamped`` -- transfer_ids whose ledger row has ``completed_at`` set
    (the P2 stamp-before-remove ordering constraint is satisfied; safe to
    remove). ``unstamped`` -- transfer_ids cratedigger ledgered but whose
    completion stamp hasn't landed yet (event ingestion hasn't caught up);
    left for a later cycle, never removed. A transfer_id in neither set is
    foreign -- never cratedigger's, never touched.
    """
    stamped: set[str]
    unstamped: set[str]
