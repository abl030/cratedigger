"""Triage service — composes the per-request "why is this request stuck?"
view from the four observability domains shipped in PR1-PR3:

1. ``album_requests`` metadata and failure-class column (U12).
2. Unfindable cohort state (U13 — ``unfindable_category``, probe history,
   long-tail-rescue audit).
3. ``album_request_field_resolutions`` — external metadata resolver status
   (U2).
4. ``search_log`` forensics — the ``request_search_summary`` view (U11)
   plus a window of recent rejection-reason rows.

The service produces a single typed ``TriageResult`` (``msgspec.Struct``)
that wraps every domain. ``compose_triage_for_request`` returns the
result for one request_id; ``list_triage`` returns a paged cohort under
an operator filter spec.

The cohort path is N+1-bounded: regardless of page size, ``list_triage``
emits **4 DB queries (+ 1 headroom for future growth)** — the page +
bulk field-resolutions + bulk search-summaries + bulk recent search_log
rows. The N+1 guard test in ``tests/test_triage_service.py`` asserts
the bound holds at page_size=50.

This module deliberately lives upstream of the CLI / HTTP wrappers.
``pipeline-cli triage`` and ``/api/triage`` are thin adapters mapping
the ``TriageResult`` (and the parametric filter parser's
``InvalidFilterError``) onto exit-code / status-code surfaces — same
shape as ``search_plan_service`` and ``beets_distance``. Per
``docs/solutions/architecture/service-first-then-glue.md`` the service
ships green before either wrapper exists; U16 / U17 fill those in.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional, Protocol

import msgspec

from lib.field_resolver_service import (
    FIELD_CATALOG_NUMBER,
    FIELD_RELEASE_GROUP_ID,
    FIELD_RELEASE_GROUP_YEAR,
    FIELD_TRACK_ARTIST,
)
from lib.unfindable_detection_service import (
    CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT,
    CATEGORY_ARTIST_ABSENT,
    CATEGORY_ONE_TRACK_STRUCTURAL,
    CATEGORY_WRONG_PRESSING_AVAILABLE,
)


# --- Filter parsing -------------------------------------------------------


# The cohort filter is a small DSL. The CLI accepts the same string the
# HTTP endpoint accepts so operators can paste between surfaces without
# adjusting syntax. Service-layer parser raises ``InvalidFilterError``
# on garbage; CLI maps to exit code 3, API maps to HTTP 400 (per the
# CLI ⇄ API symmetry convention).
_FILTER_ALL = "all"
_FILTER_UNFINDABLE = "unfindable"
_FILTER_DATA_QUALITY = "data_quality"
_FILTER_SEARCH_NOT_CONVERTING = "search_not_converting"

# Sourced from ``lib.unfindable_detection_service`` — the daily detection
# service owns the vocabulary; this parser is downstream. Adding a new
# category there auto-flows into the cohort filter.
VALID_UNFINDABLE_CATEGORIES: frozenset[str] = frozenset({
    CATEGORY_ARTIST_ABSENT,
    CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT,
    CATEGORY_ONE_TRACK_STRUCTURAL,
    CATEGORY_WRONG_PRESSING_AVAILABLE,
})

# Sourced from ``lib.field_resolver_service`` — the resolver service owns
# the side-table column names. Used by the ``data_quality:<field>`` filter
# arm to reject typos before they reach the DB.
VALID_DATA_QUALITY_FIELD_NAMES: frozenset[str] = frozenset({
    FIELD_RELEASE_GROUP_YEAR,
    FIELD_RELEASE_GROUP_ID,
    FIELD_TRACK_ARTIST,
    FIELD_CATALOG_NUMBER,
})


# Canonical list of valid filter forms — single source of truth for the
# CLI ``--help`` text and the API 400 envelope. CLI and HTTP route both
# import this tuple; the values are machine-parseable tokens that the
# operator can paste back into a follow-up request.
VALID_FILTER_FORMS: tuple[str, ...] = (
    _FILTER_ALL,
    _FILTER_UNFINDABLE,
    f"{_FILTER_UNFINDABLE}:<category>",
    _FILTER_DATA_QUALITY,
    f"{_FILTER_DATA_QUALITY}:<field>",
    f"{_FILTER_DATA_QUALITY}:status=<status>",
    f"{_FILTER_DATA_QUALITY}:reason=<code>",
    _FILTER_SEARCH_NOT_CONVERTING,
)


class InvalidFilterError(ValueError):
    """Raised by ``parse_filter`` when the spec is garbage.

    Carries the offending spec for the wrapper layers to echo back to
    the operator. The CLI emits the message on stderr; the HTTP route
    embeds it in the 400 body.
    """


class ParsedTriageFilter(msgspec.Struct, frozen=True):
    """Normalised filter spec.

    ``kind`` is one of ``"all"`` / ``"unfindable"`` / ``"data_quality"``
    / ``"search_not_converting"``. The parameter columns are populated
    only on the parameterised forms:

    * ``unfindable_category`` — set for ``unfindable:<cat>``.
    * ``field_name`` — set for ``data_quality:<field>``.
    * ``status_code`` — set for ``data_quality:status=<status>`` (#374
      canonical form — matches ``album_request_field_resolutions.status``
      which is what ``lib.field_resolver_service`` actually writes).
    * ``reason_code`` — set for ``data_quality:reason=<code>`` (filters
      on ``album_request_field_resolutions.reason_code``, e.g.
      ``http_400`` / ``http_410`` / ``http_422``).
    """

    kind: str
    unfindable_category: Optional[str] = None
    field_name: Optional[str] = None
    status_code: Optional[str] = None
    reason_code: Optional[str] = None
    raw: str = ""


def parse_filter(spec: str) -> ParsedTriageFilter:
    """Parse ``spec`` into a normalised filter.

    Trim + lowercase; accept the documented forms; raise
    ``InvalidFilterError`` on anything else. The DB layer trusts the
    parser — invalid specs never reach SQL.
    """
    raw = spec.strip().lower()
    if not raw:
        raise InvalidFilterError("filter spec is empty")

    if raw == _FILTER_ALL:
        return ParsedTriageFilter(kind=_FILTER_ALL, raw=raw)

    if raw == _FILTER_UNFINDABLE:
        return ParsedTriageFilter(kind=_FILTER_UNFINDABLE, raw=raw)

    if raw.startswith(_FILTER_UNFINDABLE + ":"):
        cat = raw[len(_FILTER_UNFINDABLE) + 1 :].strip()
        if not cat:
            raise InvalidFilterError(
                "unfindable:<category> requires a non-empty category"
            )
        if cat not in VALID_UNFINDABLE_CATEGORIES:
            raise InvalidFilterError(
                f"unknown unfindable category {cat!r}; "
                f"expected one of {sorted(VALID_UNFINDABLE_CATEGORIES)}"
            )
        return ParsedTriageFilter(
            kind=_FILTER_UNFINDABLE, unfindable_category=cat, raw=raw,
        )

    if raw == _FILTER_DATA_QUALITY:
        return ParsedTriageFilter(kind=_FILTER_DATA_QUALITY, raw=raw)

    if raw.startswith(_FILTER_DATA_QUALITY + ":"):
        rest = raw[len(_FILTER_DATA_QUALITY) + 1 :].strip()
        if not rest:
            raise InvalidFilterError(
                "data_quality:<field>|status=<status>|reason=<code> "
                "requires a value"
            )
        if rest.startswith("status="):
            status = rest[len("status=") :].strip()
            if not status:
                raise InvalidFilterError(
                    "data_quality:status= requires a status value"
                )
            return ParsedTriageFilter(
                kind=_FILTER_DATA_QUALITY, status_code=status, raw=raw,
            )
        if rest.startswith("reason="):
            code = rest[len("reason=") :].strip()
            if not code:
                raise InvalidFilterError(
                    "data_quality:reason= requires a reason_code value"
                )
            return ParsedTriageFilter(
                kind=_FILTER_DATA_QUALITY, reason_code=code, raw=raw,
            )
        # No "status=" / "reason=" prefix → field name selector.
        if rest not in VALID_DATA_QUALITY_FIELD_NAMES:
            raise InvalidFilterError(
                f"unknown data_quality field {rest!r}; expected one of "
                f"{sorted(VALID_DATA_QUALITY_FIELD_NAMES)}"
            )
        return ParsedTriageFilter(
            kind=_FILTER_DATA_QUALITY, field_name=rest, raw=raw,
        )

    if raw == _FILTER_SEARCH_NOT_CONVERTING:
        return ParsedTriageFilter(kind=_FILTER_SEARCH_NOT_CONVERTING, raw=raw)

    raise InvalidFilterError(f"unknown filter spec {spec!r}")


# --- Typed result Structs -------------------------------------------------


class RequestMeta(msgspec.Struct, frozen=True):
    """Static identity columns lifted off ``album_requests``.

    Mirrors the operator-relevant subset of the row — enough to render
    "Artist – Album (year) #N" plus the three identity probes the
    forensic surfaces use (failure_class, source, search_filetype_override).
    """

    id: int
    artist_name: str
    album_title: str
    year: Optional[int]
    status: str
    source: Optional[str]
    mb_release_id: Optional[str]
    discogs_release_id: Optional[str]
    release_group_year: Optional[int]
    is_va_compilation: bool
    catalog_number: Optional[str]
    failure_class: Optional[str]
    search_filetype_override: Optional[str]


class UnfindableState(msgspec.Struct, frozen=True):
    """Per-request unfindable cohort state (U13).

    Populated only when the request has at least one signal:
    ``unfindable_category``, ``unfindable_categorised_at``,
    ``last_artist_probe_at``, or ``rescued_at``. Healthy requests get
    ``unfindable=None`` on the parent ``TriageResult`` so the operator
    surface can render "no concerns" without a sentinel struct.
    """

    category: Optional[str]
    categorised_at: Optional[datetime]
    last_artist_probe_at: Optional[datetime]
    last_artist_probe_match_count: Optional[int]
    rescued_at: Optional[datetime]
    prior_unfindable_category: Optional[str]


class FieldResolutionState(msgspec.Struct, frozen=True):
    """One row of ``album_request_field_resolutions`` lifted to the wire."""

    field_name: str
    status: str
    reason_code: Optional[str]
    attempts: int
    resolved_at: datetime


class SearchLogEntry(msgspec.Struct, frozen=True):
    """One row from the ``recent_entries`` slice.

    Carries the columns the operator surface actually renders — id,
    timestamp, the strategy + query that produced the row, outcome,
    and the U11 forensics scalars (``rejection_reason``,
    ``matcher_score_top1``). Anything else stays on the raw
    ``search_log`` row.
    """

    id: int
    created_at: datetime
    plan_strategy: Optional[str]
    query: Optional[str]
    outcome: str
    result_count: Optional[int]
    rejection_reason: Optional[str]
    matcher_score_top1: Optional[float]


class SearchForensicsSummary(msgspec.Struct, frozen=True):
    """Rollup over ``request_search_summary`` view + a 10-row slice.

    The summary scalars are read straight from the view; ``recent_entries``
    is the last ten ``search_log`` rows for the request (newest-first).
    Both come from bulk queries scoped to the page's request_ids so the
    cohort path stays N+1-bounded.
    """

    total_searches: int
    with_cands_count: int
    found_count: int
    near_cap_count: int
    zero_results_count: int
    pre_filter_skips_total: int
    first_strategy_with_cands: Optional[str]
    dominant_rejection_reason: Optional[str]
    last_search_at: Optional[datetime]
    recent_entries: list[SearchLogEntry]


class TriageResult(msgspec.Struct, frozen=True):
    """The full per-request triage payload.

    ``failure_class`` is on ``request_meta`` (the canonical home —
    every other ``album_requests`` column lives there). Consumers
    that need it should read ``result.request_meta.failure_class``.
    """

    request_meta: RequestMeta
    unfindable: Optional[UnfindableState]
    field_quality: list[FieldResolutionState]
    search_forensics: SearchForensicsSummary


# --- Service entrypoint ---------------------------------------------------


# Limit for ``recent_entries`` — small, since the operator only wants a
# scroll of the last attempts; the historical timeline lives behind
# ``search-plan history``.
DEFAULT_RECENT_SEARCH_LOG_LIMIT = 10

# Page-size + cursor bounds for ``list_triage`` — single source of truth
# for the CLI and HTTP wrappers, so both surfaces reject the same set of
# out-of-range values. Mirrors the convention of ``search-plan history``.
TRIAGE_LIMIT_MIN = 1
TRIAGE_LIMIT_MAX = 200
TRIAGE_AFTER_MIN = 1

# Default page size for ``list_triage``.
DEFAULT_TRIAGE_PAGE_SIZE = 50


class _PipelineDB(Protocol):
    """Duck-typed pipeline DB — service body never imports the concrete
    class so tests can drop in a ``FakePipelineDB`` without monkey-patching.
    """

    def get_request(self, request_id: int) -> Optional[dict[str, Any]]: ...

    def list_triage_page(
        self,
        *,
        filter_spec: ParsedTriageFilter,
        page_size: int,
        after_request_id: Optional[int],
    ) -> list[dict[str, Any]]: ...

    def get_field_resolutions_for_requests(
        self, request_ids: list[int],
    ) -> dict[int, list[dict[str, Any]]]: ...

    def get_search_summaries_for_requests(
        self, request_ids: list[int],
    ) -> dict[int, dict[str, Any]]: ...

    def get_recent_search_log_for_requests(
        self,
        request_ids: list[int],
        *,
        per_request_limit: int,
    ) -> dict[int, list[dict[str, Any]]]: ...


def compose_triage_for_request(
    request_id: int,
    pdb: _PipelineDB,
) -> Optional[TriageResult]:
    """Compose a single-request triage payload.

    Returns ``None`` when the row doesn't exist. Composes by reading
    one request row + the same bulk-scoped per-domain getters
    ``list_triage`` uses (passing a single-element list). Pull paths
    stay symmetric: any caller asking "show me triage" goes through
    the same SQL, just at different page sizes.
    """
    row = pdb.get_request(int(request_id))
    if row is None:
        return None

    field_rows = pdb.get_field_resolutions_for_requests([int(request_id)])
    summary_rows = pdb.get_search_summaries_for_requests([int(request_id)])
    log_rows = pdb.get_recent_search_log_for_requests(
        [int(request_id)],
        per_request_limit=DEFAULT_RECENT_SEARCH_LOG_LIMIT,
    )
    return _compose_one(
        row,
        field_rows.get(int(request_id), []),
        summary_rows.get(int(request_id)),
        log_rows.get(int(request_id), []),
    )


def list_triage(
    filter_spec: str,
    pdb: _PipelineDB,
    *,
    page_size: int = DEFAULT_TRIAGE_PAGE_SIZE,
    after_request_id: Optional[int] = None,
) -> list[TriageResult]:
    """List one page of triage results matching ``filter_spec``.

    N+1-bounded: regardless of ``page_size``, the call emits 4 DB
    queries (+ 1 headroom for future growth):

    1. ``list_triage_page`` — the cohort page filtered + keyset-paged.
    2. ``get_field_resolutions_for_requests`` — one bulk
       ``WHERE request_id = ANY(%s)`` over the page's ids.
    3. ``get_search_summaries_for_requests`` — one bulk join against
       ``request_search_summary``.
    4. ``get_recent_search_log_for_requests`` — one bulk window-function
       slice of the last N rows per request.

    The result list preserves the page's ``id`` ASC ordering so
    keyset pagination is stable across calls.
    """
    parsed = parse_filter(filter_spec)
    rows = pdb.list_triage_page(
        filter_spec=parsed,
        page_size=int(page_size),
        after_request_id=(int(after_request_id)
                          if after_request_id is not None else None),
    )
    if not rows:
        return []

    request_ids = [int(r["id"]) for r in rows]
    field_rows = pdb.get_field_resolutions_for_requests(request_ids)
    summary_rows = pdb.get_search_summaries_for_requests(request_ids)
    log_rows = pdb.get_recent_search_log_for_requests(
        request_ids, per_request_limit=DEFAULT_RECENT_SEARCH_LOG_LIMIT,
    )

    out: list[TriageResult] = []
    for r in rows:
        rid = int(r["id"])
        out.append(_compose_one(
            r,
            field_rows.get(rid, []),
            summary_rows.get(rid),
            log_rows.get(rid, []),
        ))
    return out


# --- Composition helpers --------------------------------------------------


def _compose_one(
    request_row: dict[str, Any],
    field_rows: Iterable[dict[str, Any]],
    summary_row: Optional[dict[str, Any]],
    log_rows: Iterable[dict[str, Any]],
) -> TriageResult:
    request_meta = _request_meta(request_row)
    unfindable = _unfindable_state(request_row)
    field_quality = [_field_resolution(r) for r in field_rows]
    search_forensics = _search_forensics(summary_row, log_rows)
    return TriageResult(
        request_meta=request_meta,
        unfindable=unfindable,
        field_quality=field_quality,
        search_forensics=search_forensics,
    )


def _request_meta(row: dict[str, Any]) -> RequestMeta:
    return RequestMeta(
        id=int(row["id"]),
        artist_name=str(row.get("artist_name") or ""),
        album_title=str(row.get("album_title") or ""),
        year=_int_or_none(row.get("year")),
        status=str(row.get("status") or ""),
        source=row.get("source"),
        mb_release_id=row.get("mb_release_id"),
        discogs_release_id=row.get("discogs_release_id"),
        release_group_year=_int_or_none(row.get("release_group_year")),
        is_va_compilation=bool(row.get("is_va_compilation") or False),
        catalog_number=row.get("catalog_number"),
        failure_class=row.get("failure_class"),
        search_filetype_override=row.get("search_filetype_override"),
    )


def _unfindable_state(row: dict[str, Any]) -> Optional[UnfindableState]:
    category = row.get("unfindable_category")
    categorised_at = row.get("unfindable_categorised_at")
    last_probe = row.get("last_artist_probe_at")
    rescued_at = row.get("rescued_at")
    if (category is None and categorised_at is None and last_probe is None
            and rescued_at is None):
        return None
    return UnfindableState(
        category=category,
        categorised_at=categorised_at,
        last_artist_probe_at=last_probe,
        last_artist_probe_match_count=_int_or_none(
            row.get("last_artist_probe_match_count")
        ),
        rescued_at=rescued_at,
        prior_unfindable_category=row.get("prior_unfindable_category"),
    )


def _field_resolution(row: dict[str, Any]) -> FieldResolutionState:
    return FieldResolutionState(
        field_name=str(row["field_name"]),
        status=str(row["status"]),
        reason_code=row.get("reason_code"),
        attempts=int(row.get("attempts") or 0),
        resolved_at=row["resolved_at"],
    )


def _search_forensics(
    summary_row: Optional[dict[str, Any]],
    log_rows: Iterable[dict[str, Any]],
) -> SearchForensicsSummary:
    entries = [_search_log_entry(r) for r in log_rows]
    if summary_row is None:
        # No view row for this request — every counter is zero. ``last_search_at``
        # stays None; the operator surface renders the "no searches yet" state.
        return SearchForensicsSummary(
            total_searches=0,
            with_cands_count=0,
            found_count=0,
            near_cap_count=0,
            zero_results_count=0,
            pre_filter_skips_total=0,
            first_strategy_with_cands=None,
            dominant_rejection_reason=None,
            last_search_at=None,
            recent_entries=entries,
        )
    return SearchForensicsSummary(
        total_searches=int(summary_row.get("total_searches") or 0),
        with_cands_count=int(summary_row.get("with_cands_count") or 0),
        found_count=int(summary_row.get("found_count") or 0),
        near_cap_count=int(summary_row.get("near_cap_count") or 0),
        zero_results_count=int(summary_row.get("zero_results_count") or 0),
        pre_filter_skips_total=int(
            summary_row.get("pre_filter_skips_total") or 0
        ),
        first_strategy_with_cands=summary_row.get("first_strategy_with_cands"),
        dominant_rejection_reason=summary_row.get("dominant_rejection_reason"),
        last_search_at=summary_row.get("last_search_at"),
        recent_entries=entries,
    )


def _search_log_entry(row: dict[str, Any]) -> SearchLogEntry:
    matcher = row.get("matcher_score_top1")
    matcher_f: Optional[float] = None
    if matcher is not None:
        try:
            matcher_f = float(matcher)
        except (TypeError, ValueError):
            matcher_f = None
    return SearchLogEntry(
        id=int(row["id"]),
        created_at=row["created_at"],
        plan_strategy=row.get("plan_strategy"),
        query=row.get("query"),
        outcome=str(row.get("outcome") or ""),
        result_count=_int_or_none(row.get("result_count")),
        rejection_reason=row.get("rejection_reason"),
        matcher_score_top1=matcher_f,
    )


def _int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
