"""Shared search-plan generation service.

Single seam consumed by add-time CLI/web flows, startup reconciliation, and
explicit regeneration. The service:

1. Builds a `ReleaseSnapshot` (from persisted state or an add payload).
2. Invokes the pure `generate_search_plan` generator from `lib.search`.
3. Persists the outcome:
   * Successful plan → `create_successful_search_plan` (or
     `supersede_search_plan_with_replacement` when regenerating).
   * Deterministic generator failure (no runnable query / metadata
     incomplete) → `create_failed_search_plan(transient=False)`.
   * Resolver / dependency outage → `create_failed_search_plan(transient=True)`,
     which startup or explicit retry can clear later.

A `SEARCH_PLAN_GENERATOR_ID` constant is re-exported from `lib.search` so
every caller agrees on the currentness contract — there is no fork.

Per-request serialisation runs through a PostgreSQL advisory lock
(`ADVISORY_LOCK_NAMESPACE_PLAN`, key = `request_id`). See
`docs/advisory-locks.md` for the namespace contract.

This module never imports from CLI or web layers — adapters pass
already-resolved data in. That keeps add-time and startup paths from
diverging on metadata semantics.
"""

from __future__ import annotations

from collections.abc import Mapping

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

from lib.config import CratediggerConfig
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_PLAN,
    PLAN_STATUS_ACTIVE,
    ActiveSearchPlan,
    SaturationSummary,
    SearchLogHistoryPage,
    SearchPlanInspection,
    SearchPlanItemInput,
    ReplacedRequestMutationError,
)
from lib.release_snapshot import (
    ReleaseSnapshot,
    ResolverFailure,
    ResolverMetadataIncomplete,
    TrackResolver,
    snapshot_from_add_payload,
    snapshot_from_request_row,
)
from lib.search import (
    FAILURE_CLASS_NO_RUNNABLE_QUERY,
    PLAN_STATUS_GENERATION_FAILED,
    PLAN_STATUS_SUCCESS,
    SEARCH_PLAN_GENERATOR_ID,
    SearchPlan,
    SearchPlanConfig,
    generate_search_plan,
)

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow


logger = logging.getLogger(__name__)


@runtime_checkable
class SearchPlanDB(Protocol):
    """The PipelineDB surface SearchPlanService uses (#409).

    Parity tests live in ``tests/test_search_plan_service.py``.
    """

    def get_request(self, request_id: int) -> "AlbumRequestRow | None": ...

    def get_tracks(self, request_id: int) -> list[dict[str, Any]]: ...

    def set_tracks(
        self, request_id: int, tracks: list[dict[str, Any]],
    ) -> None: ...

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def get_active_search_plan(
        self, request_id: int,
    ) -> ActiveSearchPlan | None: ...

    def create_successful_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
        set_active: bool = True,
    ) -> int: ...

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
    ) -> int: ...

    def supersede_search_plan_with_replacement(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
    ) -> int: ...

    def advance_search_plan_cursor(
        self,
        request_id: int,
        *,
        target_ordinal: int,
        plan_item_count: int,
    ) -> tuple[int, int, int]: ...

    def get_saturation_summary(
        self, request_id: int, *, window_days: int = 14,
    ) -> SaturationSummary: ...

    def get_search_history_page(
        self,
        request_id: int,
        *,
        limit: int,
        before_id: int | None = None,
    ) -> SearchLogHistoryPage: ...

    def get_search_plan_inspection(
        self, request_id: int,
    ) -> SearchPlanInspection: ...


# Failure-class strings (mirror migration 014's CHECK constraint and the
# constants in `lib/pipeline_db.py`). Defined here so callers can branch
# on the service result without importing pipeline_db directly.
# `FAILURE_CLASS_NO_RUNNABLE_QUERY` is owned by `lib.search` (the generator
# is the producer); the others are service-layer classifications.
FAILURE_CLASS_METADATA_INCOMPLETE = "metadata_incomplete"
FAILURE_CLASS_RESOLVER_UNAVAILABLE = "resolver_unavailable"
FAILURE_CLASS_DEPENDENCY_FAILURE = "dependency_failure"
FAILURE_CLASS_UNKNOWN = "unknown"

# Service result outcome strings.
RESULT_SUCCESS = "success"
RESULT_NOOP_ACTIVE_PLAN_EXISTS = "noop_active_plan_exists"
RESULT_FAILED_DETERMINISTIC = "failed_deterministic"
RESULT_FAILED_TRANSIENT = "failed_transient"
RESULT_REQUEST_NOT_FOUND = "request_not_found"
RESULT_REQUEST_REPLACED = "request_replaced"
# advance_for_request outcomes (forward-only cursor mutation, no plan write).
RESULT_ADVANCED = "advanced"
RESULT_NO_ACTIVE_PLAN = "no_active_plan"
RESULT_INVALID_TARGET = "invalid_target"
# history_for_request outcomes (read-only paginated search_log slice).
RESULT_HISTORY_PAGE_SUCCESS = "success"
RESULT_HISTORY_PAGE_INPUT_INVALID = "input_invalid"
# dry_run_for_request outcomes (U6 — read-only generator simulation).
RESULT_DRY_RUN_SUCCESS = "success"
RESULT_DRY_RUN_GENERATION_FAILED = "generation_failed"
# saturation_for_request outcomes (U7 — read-only telemetry aggregate).
RESULT_SATURATION_SUCCESS = "success"
RESULT_SATURATION_INPUT_INVALID = "input_invalid"

# Saturation telemetry window bounds (U7). Operators can ask for any
# window in [1, 90] days; defaults to 14 days matching the brainstorm.
# The CLI / route both clamp before calling the service so the DB layer
# stays a thin adapter.
SATURATION_WINDOW_MIN_DAYS = 1
SATURATION_WINDOW_MAX_DAYS = 90
SATURATION_WINDOW_DEFAULT_DAYS = 14

# Bounds for the paginated history endpoint. The route handler and CLI
# both consult these so the cap stays consistent across surfaces.
HISTORY_PAGE_MIN_LIMIT = 1
HISTORY_PAGE_MAX_LIMIT = 200
# Default page size when the query string / CLI flag is omitted. 50 is
# the v1 sensible default per the plan; tunable as the inspector evolves.
HISTORY_PAGE_DEFAULT_LIMIT = 50

# Sanitisation cap for persisted error/provenance strings. 4 KB is well
# above any expected exception string but bounds JSONB blob growth so a
# misbehaving exception cannot bloat the table.
MAX_ERROR_MESSAGE_BYTES = 4 * 1024
TRUNCATION_MARKER = "…[truncated]"

# How long a transient generation failure is "sticky" before another
# attempt is permitted. Without this, every cycle that walks a
# transient-failed request would re-run the resolver and append yet
# another failed_transient row, causing unbounded JSONB blob growth.
# 1h is short enough that a recovered upstream is detected within one
# cycle of its window expiring, while long enough that a persistent
# outage produces at most ~24 rows/day per request.
_TRANSIENT_FAILURE_RETRY_INTERVAL = timedelta(hours=1)


# Path-prefix redaction list. We strip absolute paths beneath these roots
# so a stray `os.strerror` / `FileNotFoundError` cannot leak deployment
# layout / secret paths into JSONB. The list is conservative — anything
# rooted under these is replaced with a placeholder.
_REDACTED_PATH_PREFIXES = (
    "/run/secrets/",
    "/var/lib/cratedigger/",
    "/etc/",
    "/home/",
    "/root/",
)

# Secret-shape patterns. The slskd API key is a hex-or-base64 string of
# at least 32 chars; redact anything that could match. Tokens longer than
# 24 chars composed only of safe-secret characters are also redacted.
_SECRET_PATTERNS = (
    re.compile(r"\b[A-Fa-f0-9]{32,}\b"),
    re.compile(r"\b[A-Za-z0-9_\-]{40,}\b"),
)


def sanitize_error_message(text: str | None) -> str | None:
    """Bound + scrub a generation error string before persistence.

    Order:
      1. Replace absolute paths under sensitive prefixes with `[REDACTED-PATH]`.
      2. Mask long secret-shaped tokens with `[REDACTED-SECRET]`.
      3. Truncate to `MAX_ERROR_MESSAGE_BYTES` UTF-8 bytes.

    Returns ``None`` for ``None`` input so callers can pass it through
    unchanged.
    """
    if text is None:
        return None
    cleaned = text
    for prefix in _REDACTED_PATH_PREFIXES:
        # Replace any whitespace-delimited path beginning with prefix.
        pattern = re.compile(re.escape(prefix) + r"[^\s\"'<>]*")
        cleaned = pattern.sub("[REDACTED-PATH]", cleaned)
    for pattern in _SECRET_PATTERNS:
        cleaned = pattern.sub("[REDACTED-SECRET]", cleaned)
    encoded = cleaned.encode("utf-8")
    if len(encoded) > MAX_ERROR_MESSAGE_BYTES:
        marker = TRUNCATION_MARKER.encode("utf-8")
        head = encoded[: max(0, MAX_ERROR_MESSAGE_BYTES - len(marker))]
        # Strip a partial trailing UTF-8 char if the cut landed mid-rune.
        cleaned = head.decode("utf-8", errors="ignore") + TRUNCATION_MARKER
    return cleaned


def sanitize_provenance(provenance: dict[str, Any] | None) -> dict[str, Any] | None:
    """Walk a provenance dict and apply `sanitize_error_message` to strings.

    Generator provenance is generator-controlled and small. We still
    sanitize string leaves so a future generator that surfaces (say) the
    resolver URL it attempted cannot leak secrets via the same boundary.

    Hardened against pathological structures: cycles are replaced with
    ``"[CYCLE]"`` and depth beyond ``_SANITIZE_MAX_DEPTH`` becomes
    ``"[TRUNCATED]"`` instead of recursing without bound.
    """
    if provenance is None:
        return None
    return _sanitize_obj(provenance, _depth=0, _seen=set())


# Defence-in-depth caps for sanitiser recursion. Real generator
# provenance is shallow (~3 levels); 10 is well above realistic cases
# yet far below Python's ~1000-frame default recursion limit.
_SANITIZE_MAX_DEPTH = 10
_CYCLE_MARKER = "[CYCLE]"
_TRUNCATED_MARKER = "[TRUNCATED]"


def _is_container_like(value: object) -> bool:
    """``isinstance(value, (dict, list, tuple))`` behind a plain boundary.

    Same rationale as ``lib.youtube_album_service._is_dict_like``: an
    inline ``isinstance`` check narrows the caller's ``Any``-typed
    ``value`` to a partially-unknown parameterized type (``dict[Unknown,
    Unknown] | list[Unknown] | tuple[Unknown, ...]``), tainting every
    downstream ``id()``/``.items()``/iteration call in strict mode. A
    plain (non-``TypeGuard``) function performs the identical runtime
    check without narrowing the caller's variable.
    """
    return isinstance(value, (dict, list, tuple))


def _is_dict_like(value: object) -> bool:
    """``isinstance(value, dict)`` behind a plain boundary — see above."""
    return isinstance(value, dict)


def _is_list_like(value: object) -> bool:
    """``isinstance(value, list)`` behind a plain boundary — see above."""
    return isinstance(value, list)


def _sanitize_obj(value: Any, *, _depth: int, _seen: set[int]) -> Any:
    if _depth > _SANITIZE_MAX_DEPTH:
        return _TRUNCATED_MARKER
    if isinstance(value, str):
        cleaned = sanitize_error_message(value)
        return cleaned if cleaned is not None else value
    if _is_container_like(value):
        ident = id(value)
        if ident in _seen:
            return _CYCLE_MARKER
        _seen.add(ident)
        try:
            if _is_dict_like(value):
                return {
                    k: _sanitize_obj(v, _depth=_depth + 1, _seen=_seen)
                    for k, v in value.items()
                }
            if _is_list_like(value):
                return [
                    _sanitize_obj(v, _depth=_depth + 1, _seen=_seen)
                    for v in value
                ]
            return tuple(
                _sanitize_obj(v, _depth=_depth + 1, _seen=_seen)
                for v in value
            )
        finally:
            _seen.discard(ident)
    return value


def search_plan_config_from_cratedigger_config(
    cfg: CratediggerConfig,
) -> SearchPlanConfig:
    """Translate runtime config into the generator's pure config.

    Keeps generation-affecting options under one boundary so CLI / web /
    startup never drift. Add new generation knobs here and bump
    `SEARCH_PLAN_GENERATOR_ID` in `lib.search`.
    """
    return SearchPlanConfig(
        escalation_threshold=cfg.search_escalation_threshold,
    )


@dataclass(frozen=True)
class ServiceResult:
    """Outcome of a single `generate_for_*` call.

    `outcome` is one of the `RESULT_*` constants. `plan_id` is the
    persisted `search_plans.id` for success / failure / supersede paths
    (None for `noop_active_plan_exists` and `request_not_found`).
    `failure_class` is set on failure outcomes and copies one of the
    `FAILURE_CLASS_*` strings.
    """

    outcome: str
    plan_id: int | None = None
    failure_class: str | None = None
    error_message: str | None = None
    is_supersede: bool = False


@dataclass(frozen=True)
class SearchLogHistoryPageResult:
    """Outcome of one ``history_for_request`` call.

    ``outcome`` is one of:
      * ``RESULT_HISTORY_PAGE_SUCCESS`` — page produced; ``rows`` /
        ``next_before_id`` populated.
      * ``RESULT_REQUEST_NOT_FOUND`` — no such request id.
      * ``RESULT_HISTORY_PAGE_INPUT_INVALID`` — ``limit`` or
        ``before_id`` violated the bounds; ``error_message`` describes.

    Internal-only typed result (the route + CLI hand back a dict tree on
    the wire; no msgspec.Struct boundary needed since the rows are read
    straight from the DB and never re-encoded as a Struct).
    """

    outcome: str
    request_id: int
    rows: list[dict[str, Any]]
    next_before_id: int | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class DryRunResult:
    """Outcome of one ``dry_run_for_request`` call (U6).

    Read-only simulator: the service constructs the same
    ``ReleaseSnapshot`` startup/regeneration would build, runs
    ``generate_search_plan`` against the current code, and returns the
    in-memory ``SearchPlan`` plus a JSON-friendly view. No
    ``search_plans`` / ``plan_items`` rows are written and the
    request's ``active_plan_id`` is untouched.

    ``outcome`` is one of:
      * ``RESULT_DRY_RUN_SUCCESS`` — generator produced a non-empty plan.
      * ``RESULT_DRY_RUN_GENERATION_FAILED`` — generator returned
        ``PLAN_STATUS_GENERATION_FAILED`` (no runnable query / metadata
        incomplete); ``plan`` is populated but ``items`` is empty.
      * ``RESULT_REQUEST_NOT_FOUND`` — no row for ``request_id``.

    ``plan`` is the live ``SearchPlan`` object. ``snapshot`` is the
    ``ReleaseSnapshot`` fed to the generator (useful for operators
    debugging snapshot inputs vs generator outputs).
    """

    outcome: str
    request_id: int
    plan: SearchPlan | None = None
    snapshot: ReleaseSnapshot | None = None
    metadata_snapshot: dict[str, Any] | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class SaturationResult:
    """Outcome of one ``saturation_for_request`` call (U7).

    Read-only aggregator: wraps
    :class:`lib.pipeline_db.SaturationSummary` with a service-layer
    outcome string so the CLI / API surfaces can map onto the standard
    exit-code / status-code convention without coupling to the DB
    dataclass directly.

    ``outcome`` is one of:
      * ``RESULT_SATURATION_SUCCESS`` — request exists; ``summary`` is
        populated (it may contain all-zero fields if the window has no
        searches, which is a valid "found but quiet" state).
      * ``RESULT_REQUEST_NOT_FOUND`` — no row for ``request_id`` in
        ``album_requests``; ``summary`` is ``None``. We check the
        request row separately from the aggregate so zeros from a real
        request never collide with "request id doesn't exist at all".
      * ``RESULT_SATURATION_INPUT_INVALID`` — ``window_days`` is out of
        the supported ``[SATURATION_WINDOW_MIN_DAYS,
        SATURATION_WINDOW_MAX_DAYS]`` range; ``error_message`` carries
        the offending value.
    """

    outcome: str
    request_id: int
    window_days: int
    summary: "SaturationSummary | None" = None
    error_message: str | None = None


@dataclass(frozen=True)
class AdvanceResult:
    """Outcome of a single ``advance_for_request`` call.

    Distinct from ``ServiceResult`` because cursor advance does not
    persist a plan and reports its own set of outcomes
    (``RESULT_ADVANCED``, ``RESULT_NO_ACTIVE_PLAN``, ``RESULT_INVALID_TARGET``,
    ``RESULT_REQUEST_NOT_FOUND``). ``previous_ordinal`` and ``new_ordinal``
    are populated on the success path so CLI / API can render a clear
    "cursor: 1 → 7" line.
    """

    outcome: str
    request_id: int
    plan_id: int | None = None
    previous_ordinal: int | None = None
    new_ordinal: int | None = None
    new_strategy: str | None = None
    new_query: str | None = None
    error_message: str | None = None


class SearchPlanService:
    """Shared plan generation/persistence service.

    Construct one per process (or per logical caller). The service is
    stateless beyond its `db` / `resolver` / `config` references; it does
    not cache plans.
    """

    def __init__(
        self,
        db: SearchPlanDB,
        config: CratediggerConfig,
        resolver: Optional[TrackResolver] = None,
    ) -> None:
        self.db = db
        self.config = config
        self.resolver = resolver
        self.plan_config = search_plan_config_from_cratedigger_config(config)
        self.generator_id = SEARCH_PLAN_GENERATOR_ID

    # ---------- public surface ----------

    def generate_for_new_request(
        self,
        request_id: int,
        *,
        artist_name: str,
        album_title: str,
        year: object,
        tracks: list[dict[str, Any]],
        source: str = "request",
        prepend_artist: bool | None = None,
        release_group_year: object = None,
        is_va_compilation: bool = False,
        catalog_number: object = None,
    ) -> ServiceResult:
        """Generate a plan for a freshly-added request.

        Used by CLI `cmd_add` and web `/api/pipeline/add` immediately
        after `set_tracks`. The add path is repairable: a deterministic
        or transient failure here records a failed plan but does not
        roll back the request — startup reconciliation or explicit
        regeneration can repair it later.

        PR2 Apply #2: ``is_va_compilation`` and ``catalog_number`` are
        threaded so the add-path snapshot sees the resolver's verdict
        immediately. Without these, the first generation runs against
        ``is_va_compilation=False`` / ``catalog_number=None`` (the
        ``snapshot_from_add_payload`` defaults) and the VA branch is
        only entered when a later regeneration re-reads the persisted
        row — which today only happens on operator-driven regeneration.

        Per-track ``track_artist`` values flow through ``tracks`` (the
        list passed in must carry ``track_artist`` on each dict — see
        ``release_snapshot._tracks_titles_and_artists``). Callers that
        run after ``apply_resolve_all_result`` already see the resolved
        values via ``get_tracks``; callers that run before resolution
        legitimately see ``None`` and the VA branch degrades cleanly.
        """
        prepend = self._resolve_prepend_artist(prepend_artist)
        with self.db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_PLAN, request_id,
        ) as acquired:
            # See docs/advisory-locks.md (PLAN namespace).
            if not acquired:
                return self._lock_contention_result()
            row = self.db.get_request(request_id)
            if row is None:
                return ServiceResult(
                    outcome=RESULT_REQUEST_NOT_FOUND,
                    error_message=f"request {request_id} not found",
                )
            if row.get("status") == "replaced":
                return self._replaced_result(request_id)
            snapshot = snapshot_from_add_payload(
                artist_name=artist_name,
                album_title=album_title,
                year=year,
                tracks=tracks,
                source=source,
                prepend_artist=prepend,
                release_group_year=release_group_year,
                is_va_compilation=is_va_compilation,
                catalog_number=catalog_number,
            )
            return self._persist(request_id, snapshot, regenerate=False)

    def generate_for_request(
        self,
        request_id: int,
        *,
        regenerate: bool = False,
        prepend_artist: bool | None = None,
    ) -> ServiceResult:
        """Generate / regenerate a plan from persisted request state.

        ``regenerate=False`` (default): no-op when an active plan already
        exists. Used by startup reconciliation when only no-plan rows
        should be acted on.

        ``regenerate=True``: always attempts to produce a new plan.
        Successful regeneration calls
        ``supersede_search_plan_with_replacement``. Failed regeneration
        records the failed attempt without superseding the prior active
        plan.
        """
        prepend = self._resolve_prepend_artist(prepend_artist)
        with self.db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_PLAN, request_id,
        ) as acquired:
            # See docs/advisory-locks.md (PLAN namespace).
            if not acquired:
                return self._lock_contention_result()
            return self._generate_for_request_locked(
                request_id, regenerate=regenerate,
                prepend_artist=prepend,
            )

    def advance_for_request(
        self,
        request_id: int,
        *,
        to_ordinal: int | None = None,
        to_strategy: str | None = None,
    ) -> "AdvanceResult":
        """Forward-only cursor advance for an operator's active plan.

        Resolves the target ordinal from either ``to_ordinal`` (explicit)
        or ``to_strategy`` (prefix match against ``strategy`` of plan items
        at or after the current cursor). Exactly one must be set.

        Designed for cases like self-titled releases where the dedup
        collapses 5 default-strategy slots into the same query, leaving
        track searches stranded behind retries. ``advance --to-strategy
        track`` jumps the cursor to the first track-strategy slot so the
        next cycle exercises a useful query.

        Forward-only: backward intent should go through ``regenerate``.
        Held under the PLAN advisory lock so concurrent regeneration
        cannot replace the plan we're advancing within. Forward-stale
        completions from in-flight executors are still detected by
        ``record_consumed_search_attempt``'s row-level cursor check.

        Outcomes:
          * ``RESULT_ADVANCED`` — cursor moved; ``previous_ordinal`` /
            ``new_ordinal`` / ``new_strategy`` / ``new_query`` populated.
          * ``RESULT_REQUEST_NOT_FOUND`` — no such request id.
          * ``RESULT_NO_ACTIVE_PLAN`` — request exists but has no active
            plan (request never got past add-time, or last attempt failed
            deterministically).
          * ``RESULT_INVALID_TARGET`` — ordinal out of range, would go
            backward, or no plan item matches the strategy prefix.
        """
        if (to_ordinal is None) == (to_strategy is None):
            return AdvanceResult(
                outcome=RESULT_INVALID_TARGET,
                request_id=request_id,
                error_message=(
                    "exactly one of to_ordinal or to_strategy must be "
                    "provided"),
            )
        with self.db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_PLAN, request_id,
        ) as acquired:
            if not acquired:
                return AdvanceResult(
                    outcome=RESULT_FAILED_TRANSIENT,
                    request_id=request_id,
                    error_message="another writer holds the plan lock",
                )
            row = self.db.get_request(request_id)
            if row is None:
                return AdvanceResult(
                    outcome=RESULT_REQUEST_NOT_FOUND,
                    request_id=request_id,
                    error_message=f"request {request_id} not found",
                )
            if row.get("status") == "replaced":
                return AdvanceResult(
                    outcome=RESULT_REQUEST_REPLACED,
                    request_id=request_id,
                    error_message=f"request {request_id} is replaced",
                )
            active = self.db.get_active_search_plan(request_id)
            if active is None:
                return AdvanceResult(
                    outcome=RESULT_NO_ACTIVE_PLAN,
                    request_id=request_id,
                    error_message=(
                        f"request {request_id} has no active plan; "
                        "regenerate first"),
                )
            current = active.next_ordinal
            target = self._resolve_advance_target(
                items=active.items, current_ordinal=current,
                to_ordinal=to_ordinal, to_strategy=to_strategy,
            )
            if isinstance(target, str):
                # Resolution returned an error message.
                return AdvanceResult(
                    outcome=RESULT_INVALID_TARGET,
                    request_id=request_id,
                    plan_id=active.plan.id,
                    previous_ordinal=current,
                    error_message=target,
                )
            try:
                _, prev, new = self.db.advance_search_plan_cursor(
                    request_id, target_ordinal=target,
                    plan_item_count=len(active.items),
                )
            except ReplacedRequestMutationError:
                return AdvanceResult(
                    outcome=RESULT_REQUEST_REPLACED,
                    request_id=request_id,
                    plan_id=active.plan.id,
                    previous_ordinal=current,
                    error_message=f"request {request_id} is replaced",
                )
            except ValueError as e:
                return AdvanceResult(
                    outcome=RESULT_INVALID_TARGET,
                    request_id=request_id,
                    plan_id=active.plan.id,
                    previous_ordinal=current,
                    error_message=str(e),
                )
            new_item = active.items[new]
            return AdvanceResult(
                outcome=RESULT_ADVANCED,
                request_id=request_id,
                plan_id=active.plan.id,
                previous_ordinal=prev,
                new_ordinal=new,
                new_strategy=new_item.strategy,
                new_query=new_item.query,
            )

    def history_for_request(
        self,
        request_id: int,
        *,
        limit: int,
        before_id: int | None = None,
    ) -> "SearchLogHistoryPageResult":
        """Paginated read of one request's ``search_log`` rows.

        Counterpart of ``GET /api/pipeline/<id>/search-plan/history`` and
        ``pipeline-cli search-plan history``. Both surfaces wrap this
        single service method so they cannot drift on the input bounds
        or status mapping.

        ``limit`` must be in ``[HISTORY_PAGE_MIN_LIMIT, HISTORY_PAGE_MAX_LIMIT]``;
        violators return ``RESULT_HISTORY_PAGE_INPUT_INVALID``. ``before_id``
        when present must be ``>= 1``. The DB layer is a thin SQL adapter
        and does not re-validate.

        Outcomes:
          * ``RESULT_HISTORY_PAGE_SUCCESS`` — page produced; ``rows`` are
            newest-first, capped at ``limit``; ``next_before_id`` seeds
            the next page or is ``None`` when exhausted.
          * ``RESULT_REQUEST_NOT_FOUND`` — request_id does not exist.
          * ``RESULT_HISTORY_PAGE_INPUT_INVALID`` — bounds violation.
        """
        if (limit < HISTORY_PAGE_MIN_LIMIT
                or limit > HISTORY_PAGE_MAX_LIMIT):
            return SearchLogHistoryPageResult(
                outcome=RESULT_HISTORY_PAGE_INPUT_INVALID,
                request_id=request_id,
                rows=[],
                error_message=(
                    f"limit must be in [{HISTORY_PAGE_MIN_LIMIT}, "
                    f"{HISTORY_PAGE_MAX_LIMIT}]"
                ),
            )
        if before_id is not None and (before_id < 1 or before_id > 2147483647):
            return SearchLogHistoryPageResult(
                outcome=RESULT_HISTORY_PAGE_INPUT_INVALID,
                request_id=request_id,
                rows=[],
                error_message=(
                    "before_id must be in [1, 2147483647] when provided"
                ),
            )
        row = self.db.get_request(request_id)
        if row is None:
            return SearchLogHistoryPageResult(
                outcome=RESULT_REQUEST_NOT_FOUND,
                request_id=request_id,
                rows=[],
                error_message=f"request {request_id} not found",
            )
        page = self.db.get_search_history_page(
            request_id, limit=limit, before_id=before_id,
        )
        return SearchLogHistoryPageResult(
            outcome=RESULT_HISTORY_PAGE_SUCCESS,
            request_id=request_id,
            rows=page.rows,
            next_before_id=page.next_before_id,
        )

    def dry_run_for_request(
        self,
        request_id: int,
        *,
        prepend_artist: bool | None = None,
    ) -> "DryRunResult":
        """U6 read-only simulator: run the generator without persisting.

        Loads the request row + persisted tracks, constructs the same
        ``ReleaseSnapshot`` that startup / explicit regeneration would
        build, and invokes ``generate_search_plan`` against the current
        code. Nothing is written: no ``search_plans`` / ``plan_items``
        row is created, no ``active_plan_id`` mutation, no advisory
        lock taken (no concurrent mutation to protect against).

        Counterpart of ``pipeline-cli search-plan dry-run <id>`` and
        ``GET /api/pipeline/<id>/search-plan/dry-run``.

        Outcomes:
          * ``RESULT_DRY_RUN_SUCCESS`` — generator emitted a plan with
            non-empty items.
          * ``RESULT_DRY_RUN_GENERATION_FAILED`` — generator returned
            ``PLAN_STATUS_GENERATION_FAILED``; ``plan`` is populated so
            callers can inspect ``failure_reason`` / ``provenance``.
          * ``RESULT_REQUEST_NOT_FOUND`` — no row for ``request_id``.

        The simulator deliberately does NOT call the ``TrackResolver``:
        dry-run reads what's persisted today so operators can see what
        the next cycle's generator would produce against the current
        persisted state. If tracks are missing in the DB the snapshot
        is built without them — same way the generator would see them
        at startup before any resolver hydration.
        """
        prepend = self._resolve_prepend_artist(prepend_artist)
        row = self.db.get_request(request_id)
        if row is None:
            return DryRunResult(
                outcome=RESULT_REQUEST_NOT_FOUND,
                request_id=request_id,
                error_message=f"request {request_id} not found",
            )
        try:
            tracks = self.db.get_tracks(request_id) or []
        except Exception:  # noqa: BLE001
            tracks = []
        snapshot = snapshot_from_request_row(
            row, tracks, prepend_artist=prepend,
        )
        plan = generate_search_plan(snapshot, self.plan_config)
        metadata_snapshot = _metadata_snapshot_from_snapshot(snapshot)
        if plan.status == PLAN_STATUS_GENERATION_FAILED:
            return DryRunResult(
                outcome=RESULT_DRY_RUN_GENERATION_FAILED,
                request_id=request_id,
                plan=plan,
                snapshot=snapshot,
                metadata_snapshot=metadata_snapshot,
                error_message=plan.failure_reason,
            )
        return DryRunResult(
            outcome=RESULT_DRY_RUN_SUCCESS,
            request_id=request_id,
            plan=plan,
            snapshot=snapshot,
            metadata_snapshot=metadata_snapshot,
        )

    def saturation_for_request(
        self,
        request_id: int,
        *,
        window_days: int = SATURATION_WINDOW_DEFAULT_DAYS,
    ) -> "SaturationResult":
        """U7 read-only telemetry aggregate over the request's search_log.

        Wraps :meth:`PipelineDB.get_saturation_summary` with the
        not-found semantics the operator surfaces expect:

        * The request row is checked first. If it does not exist, the
          outcome is ``RESULT_REQUEST_NOT_FOUND`` and the route /
          CLI map that to 404 / exit 2.
        * If the request exists but has no logged searches in window,
          the outcome is ``RESULT_SATURATION_SUCCESS`` with all-zero
          counts — a valid "found but quiet" state. We deliberately do
          NOT collapse this onto 404; the operator asked "how saturated
          is request X?" and the truthful answer is "not at all".
        * ``window_days`` is validated against
          ``[SATURATION_WINDOW_MIN_DAYS, SATURATION_WINDOW_MAX_DAYS]``;
          out-of-range values return ``RESULT_SATURATION_INPUT_INVALID``
          which CLI argparse normally prevents but the API surface
          relies on for its 400 response.

        Counterpart of ``pipeline-cli search-plan saturation <id>`` and
        ``GET /api/pipeline/<id>/search-plan/saturation``. Both
        surfaces wrap this method (see ``CLAUDE.md`` § "CLI ⇄ API
        surface symmetry").
        """
        if (
            window_days < SATURATION_WINDOW_MIN_DAYS
            or window_days > SATURATION_WINDOW_MAX_DAYS
        ):
            return SaturationResult(
                outcome=RESULT_SATURATION_INPUT_INVALID,
                request_id=request_id,
                window_days=int(window_days),
                error_message=(
                    f"window_days must be in "
                    f"[{SATURATION_WINDOW_MIN_DAYS}, "
                    f"{SATURATION_WINDOW_MAX_DAYS}]; got {window_days}"
                ),
            )
        row = self.db.get_request(request_id)
        if row is None:
            return SaturationResult(
                outcome=RESULT_REQUEST_NOT_FOUND,
                request_id=request_id,
                window_days=int(window_days),
                error_message=f"request {request_id} not found",
            )
        summary = self.db.get_saturation_summary(
            request_id, window_days=int(window_days),
        )
        return SaturationResult(
            outcome=RESULT_SATURATION_SUCCESS,
            request_id=request_id,
            window_days=int(window_days),
            summary=summary,
        )

    @staticmethod
    def _resolve_advance_target(
        *,
        items: list[Any],
        current_ordinal: int,
        to_ordinal: int | None,
        to_strategy: str | None,
    ) -> int | str:
        """Return the absolute target ordinal, or an error string."""
        if to_ordinal is not None:
            if to_ordinal < 0 or to_ordinal >= len(items):
                return (
                    f"to_ordinal {to_ordinal} out of range "
                    f"[0, {len(items)})")
            if to_ordinal <= current_ordinal:
                return (
                    f"to_ordinal {to_ordinal} must be greater than "
                    f"current cursor {current_ordinal} "
                    "(advance is forward-only)")
            return to_ordinal
        # to_strategy: first item past current cursor whose strategy
        # starts with the given prefix.
        assert to_strategy is not None
        for item in items:
            if (item.ordinal > current_ordinal
                    and item.strategy.startswith(to_strategy)):
                return int(item.ordinal)
        return (
            f"no plan item past cursor {current_ordinal} has strategy "
            f"prefix {to_strategy!r}")

    def _generate_for_request_locked(
        self,
        request_id: int,
        *,
        regenerate: bool,
        prepend_artist: bool,
    ) -> ServiceResult:
        # Read request row + tracks under the per-request PLAN lock so
        # snapshot construction, generation, and persistence see one
        # serialized view of request state.
        row = self.db.get_request(request_id)
        if row is None:
            return ServiceResult(
                outcome=RESULT_REQUEST_NOT_FOUND,
                error_message=f"request {request_id} not found",
            )
        if row.get("status") == "replaced":
            return self._replaced_result(request_id)

        if not regenerate:
            existing = self.db.get_active_search_plan(request_id)
            if existing is not None and existing.plan.status == PLAN_STATUS_ACTIVE:
                if existing.plan.generator_id == self.generator_id:
                    # Active current-generator plan check covers the
                    # happy path; the partial-track replan check (#3)
                    # decides whether to force regeneration anyway.
                    if not self._should_regenerate_for_track_count(
                        request_id, existing,
                    ):
                        return ServiceResult(
                            outcome=RESULT_NOOP_ACTIVE_PLAN_EXISTS,
                            plan_id=existing.plan.id,
                        )
                    return self._build_and_persist(
                        request_id, row, regenerate=True,
                        prepend_artist=prepend_artist,
                    )
                # Old-generator plan: fall through and replace it under
                # supersede semantics so callers (startup) don't need a
                # second branch.
                #
                # First: check whether the current generator id has
                # ALREADY recorded a sticky failure for this request. If
                # so, short-circuit instead of appending another row each
                # cycle.
                sticky = self._sticky_failure_short_circuit(request_id)
                if sticky is not None:
                    return sticky
                return self._build_and_persist(
                    request_id, row, regenerate=True,
                    prepend_artist=prepend_artist,
                )

            # No active plan: still respect a sticky failure on the
            # current generator id so we don't pile up failed rows.
            sticky = self._sticky_failure_short_circuit(request_id)
            if sticky is not None:
                return sticky

        return self._build_and_persist(
            request_id, row, regenerate=regenerate,
            prepend_artist=prepend_artist,
        )

    def _sticky_failure_short_circuit(
        self, request_id: int,
    ) -> ServiceResult | None:
        """Return a sticky-failure ``ServiceResult`` or ``None``.

        Deterministic failures recorded under the current generator id
        are sticky forever; transient failures are sticky for
        ``_TRANSIENT_FAILURE_RETRY_INTERVAL`` after they were recorded.
        Failures recorded under an older generator id never short-circuit
        — the new id deserves a fresh attempt.
        """
        try:
            inspection = self.db.get_search_plan_inspection(request_id)
        except Exception:  # noqa: BLE001
            # If we can't read inspection state, fail open: let the
            # generator run rather than silently never trying again.
            return None

        det = inspection.latest_failed_deterministic
        if det is not None and det.generator_id == self.generator_id:
            return ServiceResult(
                outcome=RESULT_FAILED_DETERMINISTIC,
                plan_id=det.id,
                failure_class=det.failure_class,
                error_message=det.error_message,
            )

        trans = inspection.latest_failed_transient
        if (trans is not None
                and trans.generator_id == self.generator_id
                and _within_retry_window(trans.created_at)):
            return ServiceResult(
                outcome=RESULT_FAILED_TRANSIENT,
                plan_id=trans.id,
                failure_class=trans.failure_class,
                error_message=trans.error_message,
            )
        return None

    def _should_regenerate_for_track_count(
        self,
        request_id: int,
        existing: Any,
    ) -> bool:
        """#3: force regeneration when the request now has more tracks
        than the active plan was built against.

        ``existing`` is an ``ActiveSearchPlan``. We compare today's
        ``len(get_tracks)`` to the plan's ``metadata_snapshot.track_count``.
        Older plans without a recorded ``track_count`` are treated as
        equal (skip the check) — they will be replaced on the next
        generator-id bump.
        """
        snap = existing.plan.metadata_snapshot
        if snap is None:
            return False
        recorded = getattr(snap, "track_count", None)
        if recorded is None and _is_dict_like(snap):
            recorded = snap.get("track_count")
        if recorded is None:
            return False
        try:
            recorded_int = int(recorded)
        except (TypeError, ValueError):
            return False
        try:
            current_tracks = self.db.get_tracks(request_id) or []
        except Exception:  # noqa: BLE001
            return False
        return len(current_tracks) > recorded_int

    # ---------- internal ----------

    def _build_and_persist(
        self,
        request_id: int,
        row: Mapping[str, Any],
        *,
        regenerate: bool,
        prepend_artist: bool,
    ) -> ServiceResult:
        # Resolve tracks with the resolver only when we don't already
        # have them — this keeps startup happy paths cheap and confines
        # resolver-failure handling to a small surface.
        try:
            tracks = self.db.get_tracks(request_id) or []
            if not tracks and self.resolver is not None:
                release_id = (
                    row.get("mb_release_id")
                    or row.get("discogs_release_id")
                    or ""
                )
                if release_id:
                    resolved = self.resolver.resolve_tracks(
                        release_id=release_id, request_id=request_id,
                    )
                    if resolved:
                        self.db.set_tracks(request_id, resolved)
                        tracks = self.db.get_tracks(request_id) or list(resolved)
        except ReplacedRequestMutationError:
            return self._replaced_result(request_id)
        except ResolverFailure as exc:
            return self._record_failure(
                request_id,
                failure_class=FAILURE_CLASS_RESOLVER_UNAVAILABLE,
                transient=True,
                error_message=str(exc),
                snapshot=None,
                metadata_snapshot=_metadata_snapshot_from_row(row, []),
            )
        except ResolverMetadataIncomplete as exc:
            return self._record_failure(
                request_id,
                failure_class=FAILURE_CLASS_METADATA_INCOMPLETE,
                transient=False,
                error_message=str(exc),
                snapshot=None,
                metadata_snapshot=_metadata_snapshot_from_row(row, []),
            )
        except Exception as exc:  # noqa: BLE001
            # Generic dependency failure (unexpected DB / resolver
            # exception). Treat as transient — startup will retry next
            # cycle and we don't want a flaky upstream to permanently
            # mark a request unsearchable.
            return self._record_failure(
                request_id,
                failure_class=FAILURE_CLASS_DEPENDENCY_FAILURE,
                transient=True,
                error_message=str(exc),
                snapshot=None,
                metadata_snapshot=_metadata_snapshot_from_row(row, []),
            )

        snapshot = snapshot_from_request_row(
            row, tracks, prepend_artist=prepend_artist,
        )
        return self._persist(request_id, snapshot, regenerate=regenerate)

    def _persist(
        self,
        request_id: int,
        snapshot: ReleaseSnapshot,
        *,
        regenerate: bool,
    ) -> ServiceResult:
        plan = generate_search_plan(snapshot, self.plan_config)
        metadata_snapshot = _metadata_snapshot_from_snapshot(snapshot)

        return self._persist_locked(
            request_id, plan, snapshot, metadata_snapshot,
            regenerate=regenerate,
        )

    def _resolve_prepend_artist(self, explicit: bool | None) -> bool:
        if explicit is not None:
            return bool(explicit)
        return bool(getattr(self.config, "album_prepend_artist", False))

    def _lock_contention_result(self) -> ServiceResult:
        return ServiceResult(
            outcome=RESULT_FAILED_TRANSIENT,
            failure_class=FAILURE_CLASS_DEPENDENCY_FAILURE,
            error_message=(
                "advisory_lock held; another plan generation in progress"
            ),
        )

    @staticmethod
    def _replaced_result(request_id: int) -> ServiceResult:
        return ServiceResult(
            outcome=RESULT_REQUEST_REPLACED,
            error_message=f"request {request_id} is replaced",
        )

    def _persist_locked(
        self,
        request_id: int,
        plan: SearchPlan,
        snapshot: ReleaseSnapshot,
        metadata_snapshot: dict[str, Any],
        *,
        regenerate: bool,
    ) -> ServiceResult:
        provenance = sanitize_provenance(plan.provenance)

        if plan.status == PLAN_STATUS_GENERATION_FAILED:
            return self._record_failure(
                request_id,
                failure_class=FAILURE_CLASS_NO_RUNNABLE_QUERY,
                transient=False,
                error_message=plan.failure_reason,
                snapshot=snapshot,
                metadata_snapshot=metadata_snapshot,
                provenance=provenance,
            )

        if plan.status != PLAN_STATUS_SUCCESS or not plan.items:
            # Defensive: a non-success status with empty items should
            # have been caught above. Record as deterministic failure.
            return self._record_failure(
                request_id,
                failure_class=FAILURE_CLASS_NO_RUNNABLE_QUERY,
                transient=False,
                error_message="generator returned no items",
                snapshot=snapshot,
                metadata_snapshot=metadata_snapshot,
                provenance=provenance,
            )

        items = [
            SearchPlanItemInput(
                ordinal=it.ordinal,
                strategy=it.strategy,
                query=it.query,
                canonical_query_key=it.canonical_query_key,
                repeat_group=it.repeat_group,
                provenance=dict(it.provenance) if it.provenance else None,
            )
            for it in plan.items
        ]

        if regenerate:
            try:
                plan_id = self.db.supersede_search_plan_with_replacement(
                    request_id=request_id,
                    generator_id=self.generator_id,
                    items=items,
                    metadata_snapshot=metadata_snapshot,
                    provenance=provenance,
                )
            except ReplacedRequestMutationError:
                return self._replaced_result(request_id)
            except Exception as exc:  # noqa: BLE001
                return self._record_failure(
                    request_id,
                    failure_class=FAILURE_CLASS_DEPENDENCY_FAILURE,
                    transient=True,
                    error_message=f"supersede failed: {exc}",
                    snapshot=snapshot,
                    metadata_snapshot=metadata_snapshot,
                    provenance=provenance,
                )
            return ServiceResult(
                outcome=RESULT_SUCCESS, plan_id=plan_id, is_supersede=True,
            )

        try:
            plan_id = self.db.create_successful_search_plan(
                request_id=request_id,
                generator_id=self.generator_id,
                items=items,
                metadata_snapshot=metadata_snapshot,
                provenance=provenance,
                set_active=True,
            )
        except ReplacedRequestMutationError:
            return self._replaced_result(request_id)
        except Exception as exc:  # noqa: BLE001
            return self._record_failure(
                request_id,
                failure_class=FAILURE_CLASS_DEPENDENCY_FAILURE,
                transient=True,
                error_message=f"create plan failed: {exc}",
                snapshot=snapshot,
                metadata_snapshot=metadata_snapshot,
                provenance=provenance,
            )
        return ServiceResult(outcome=RESULT_SUCCESS, plan_id=plan_id)

    def _record_failure(
        self,
        request_id: int,
        *,
        failure_class: str,
        transient: bool,
        error_message: str | None,
        snapshot: ReleaseSnapshot | None,
        metadata_snapshot: dict[str, Any] | None,
        provenance: dict[str, Any] | None = None,
    ) -> ServiceResult:
        sanitized_msg = sanitize_error_message(error_message)
        sanitized_prov = sanitize_provenance(provenance)
        try:
            plan_id = self.db.create_failed_search_plan(
                request_id=request_id,
                generator_id=self.generator_id,
                failure_class=failure_class,
                error_message=sanitized_msg,
                transient=transient,
                metadata_snapshot=metadata_snapshot,
                provenance=sanitized_prov,
            )
        except ReplacedRequestMutationError:
            return self._replaced_result(request_id)
        except Exception as exc:  # noqa: BLE001
            # If we can't even persist the failure row, surface that as
            # a transient outcome but do not raise — callers (CLI/web)
            # rely on the service never aborting their request flow.
            logger.warning(
                "search_plan_service: failed to persist failure row "
                "for request %s: %s", request_id, exc,
            )
            return ServiceResult(
                outcome=RESULT_FAILED_TRANSIENT,
                failure_class=FAILURE_CLASS_DEPENDENCY_FAILURE,
                error_message=str(exc),
            )
        outcome = (
            RESULT_FAILED_TRANSIENT if transient else RESULT_FAILED_DETERMINISTIC
        )
        return ServiceResult(
            outcome=outcome,
            plan_id=plan_id,
            failure_class=failure_class,
            error_message=sanitized_msg,
        )


def _envelope(result: Any) -> dict[str, Any]:
    """Common ``{request_id, outcome, error_message}`` envelope.

    Shared by every payload helper (dry-run, saturation, ...) so the
    CLI ⇄ API surface emits the same three keys regardless of outcome.
    """
    return {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "error_message": result.error_message,
    }


def dry_run_payload(
    result: "DryRunResult",
    *,
    current_generator_id: str,
    request_row: Mapping[str, Any] | None,
    has_active_plan: bool,
) -> dict[str, Any]:
    """JSON-serialisable payload for U6 dry-run CLI + API responses.

    Single helper so the CLI ``--json`` mode and the API endpoint emit
    the same dict tree (CLI ⇄ API symmetry).
    """
    plan_payload: dict[str, Any] | None = None
    if result.plan is not None:
        items_payload: list[dict[str, Any]] = []
        for it in result.plan.items:
            items_payload.append({
                "ordinal": it.ordinal,
                "strategy": it.strategy,
                "query": it.query,
                "canonical_query_key": it.canonical_query_key,
                "repeat_group": it.repeat_group,
                "provenance": dict(it.provenance) if it.provenance else {},
            })
        plan_payload = {
            "generator_id": result.plan.generator_id,
            "status": result.plan.status,
            "items": items_payload,
            "provenance": (
                dict(result.plan.provenance)
                if result.plan.provenance else {}
            ),
            "failure_reason": result.plan.failure_reason,
            "metadata_snapshot": result.metadata_snapshot,
        }
    request_payload: dict[str, Any] | None = None
    if request_row is not None:
        request_payload = {
            "id": request_row.get("id"),
            "status": request_row.get("status"),
            "artist_name": request_row.get("artist_name"),
            "album_title": request_row.get("album_title"),
            "mb_release_id": request_row.get("mb_release_id"),
            "discogs_release_id": request_row.get("discogs_release_id"),
            "year": request_row.get("year"),
            "release_group_year": request_row.get("release_group_year"),
            "source": request_row.get("source"),
        }
    return {
        **_envelope(result),
        "current_generator_id": current_generator_id,
        "request": request_payload,
        "plan": plan_payload,
        "would_supersede_active": bool(has_active_plan),
    }


def saturation_payload(result: "SaturationResult") -> dict[str, Any]:
    """JSON-serialisable payload for U7 saturation CLI + API responses.

    The wire shape is always the five summary fields at the top level
    so a client can read ``data["saturation_rate"]`` directly without
    branching on outcome. When the summary is missing (request not
    found, invalid window) the counts are zero-filled and the
    ``outcome`` field signals the failure mode. ``error_message`` is
    populated on every non-success path. Single helper so the CLI
    ``--json`` mode and the API endpoint emit the same dict tree (CLI
    ⇄ API symmetry).
    """
    summary = result.summary
    if summary is not None:
        total = int(summary.total_searches)
        saturated = int(summary.saturated_searches)
        rate = float(summary.saturation_rate)
        skips = int(summary.total_pre_filter_skips)
        window = int(summary.window_days)
    else:
        total = 0
        saturated = 0
        rate = 0.0
        skips = 0
        window = int(result.window_days)
    return {
        **_envelope(result),
        "total_searches": total,
        "saturated_searches": saturated,
        "saturation_rate": rate,
        "total_pre_filter_skips": skips,
        "window_days": window,
    }


def _within_retry_window(created_at: datetime | None) -> bool:
    """True when ``created_at`` is within the transient-retry window of now."""
    if created_at is None:
        return False
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - created_at) < _TRANSIENT_FAILURE_RETRY_INTERVAL


def _metadata_snapshot_from_snapshot(
    snapshot: ReleaseSnapshot,
) -> dict[str, Any]:
    """JSON-friendly metadata snapshot for `search_plans.metadata_snapshot`."""
    out: dict[str, Any] = {
        "artist_name": snapshot.artist_name,
        "album_title": snapshot.title,
        "year": snapshot.year,
        "track_count": len(snapshot.track_titles),
        "redownload": snapshot.redownload,
        "prepend_artist": snapshot.prepend_artist,
    }
    if snapshot.release_group_year is not None:
        out["release_group_year"] = int(snapshot.release_group_year)
    if snapshot.is_va_compilation:
        out["is_va_compilation"] = True
    if snapshot.catalog_number is not None:
        out["catalog_number"] = snapshot.catalog_number
    return out


def _metadata_snapshot_from_row(
    row: Mapping[str, Any],
    tracks: list[dict[str, Any]],
) -> dict[str, Any]:
    """Best-effort metadata snapshot when we never built a `ReleaseSnapshot`.

    Used on resolver-failure paths so the failed-plan row still carries
    enough context to debug what was attempted.
    """
    out: dict[str, Any] = {
        "artist_name": row.get("artist_name"),
        "album_title": row.get("album_title"),
        "year": row.get("year"),
        "track_count": len(tracks),
        "redownload": row.get("source") == "redownload",
    }
    rg_year = row.get("release_group_year")
    if rg_year is not None:
        out["release_group_year"] = rg_year
    return out
