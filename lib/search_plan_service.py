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

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from lib.config import CratediggerConfig
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_PLAN,
    PLAN_STATUS_ACTIVE,
    SearchPlanItemInput,
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

logger = logging.getLogger(__name__)


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


def _sanitize_obj(value: Any, *, _depth: int, _seen: set[int]) -> Any:
    if _depth > _SANITIZE_MAX_DEPTH:
        return _TRUNCATED_MARKER
    if isinstance(value, str):
        cleaned = sanitize_error_message(value)
        return cleaned if cleaned is not None else value
    if isinstance(value, (dict, list, tuple)):
        ident = id(value)
        if ident in _seen:
            return _CYCLE_MARKER
        _seen.add(ident)
        try:
            if isinstance(value, dict):
                return {
                    k: _sanitize_obj(v, _depth=_depth + 1, _seen=_seen)
                    for k, v in value.items()
                }
            if isinstance(value, list):
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


class SearchPlanService:
    """Shared plan generation/persistence service.

    Construct one per process (or per logical caller). The service is
    stateless beyond its `db` / `resolver` / `config` references; it does
    not cache plans.
    """

    def __init__(
        self,
        db: Any,
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
    ) -> ServiceResult:
        """Generate a plan for a freshly-added request.

        Used by CLI `cmd_add` and web `/api/pipeline/add` immediately
        after `set_tracks`. The add path is repairable: a deterministic
        or transient failure here records a failed plan but does not
        roll back the request — startup reconciliation or explicit
        regeneration can repair it later.
        """
        prepend = self._resolve_prepend_artist(prepend_artist)
        with self.db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_PLAN, request_id,
        ) as acquired:
            # See docs/advisory-locks.md (PLAN namespace).
            if not acquired:
                return self._lock_contention_result()
            snapshot = snapshot_from_add_payload(
                artist_name=artist_name,
                album_title=album_title,
                year=year,
                tracks=tracks,
                source=source,
                prepend_artist=prepend,
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
        snap = existing.plan.metadata_snapshot or {}
        try:
            recorded = snap.get("track_count")
        except AttributeError:
            return False
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
        row: dict[str, Any],
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
    return {
        "artist_name": snapshot.artist_name,
        "album_title": snapshot.title,
        "year": snapshot.year,
        "track_count": len(snapshot.track_titles),
        "redownload": snapshot.redownload,
        "prepend_artist": snapshot.prepend_artist,
    }


def _metadata_snapshot_from_row(
    row: dict[str, Any],
    tracks: list[dict[str, Any]],
) -> dict[str, Any]:
    """Best-effort metadata snapshot when we never built a `ReleaseSnapshot`.

    Used on resolver-failure paths so the failed-plan row still carries
    enough context to debug what was attempted.
    """
    return {
        "artist_name": row.get("artist_name"),
        "album_title": row.get("album_title"),
        "year": row.get("year"),
        "track_count": len(tracks),
        "redownload": row.get("source") == "redownload",
    }
