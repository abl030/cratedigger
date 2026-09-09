"""Read-only inspection renderer for persisted search plans.

Both the CLI (``pipeline-cli search-plan show``) and the web API
(``GET /api/pipeline/<id>/search-plan``) call into this module so the
operator-facing surfaces stay in lock-step. The payload shape is the
API contract: add a key here, update the contract test in
``tests/web/test_routes_search_plan.py``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol

import msgspec

from lib.pipeline_db import (
    ActiveSearchPlan,
    SearchAcquisitionSummary,
    SearchPlanInspection,
    SearchPlanItemRow,
    SearchPlanRow,
    SearchPlanStats,
    SearchPlanStatsBucket,
    SearchPlanStatsGroup,
    jsonb_to_builtins,
)
from lib.quality import effective_search_tiers
from lib.search import SEARCH_PLAN_GENERATOR_ID

# Number of legacy (plan_id IS NULL) ``search_log`` rows to surface as
# a head sample. Operators inspecting a stuck request need a quick read
# on what the pre-plan-context history looked like; the full list is
# always available via ``pipeline-cli show``.
if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow


LEGACY_LOG_HEAD_LIMIT = 5


class _DBLike(Protocol):
    """The slice of PipelineDB / FakePipelineDB we depend on."""

    def get_request(
        self, request_id: int,
    ) -> AlbumRequestRow | None: ...

    def get_search_plan_inspection(
        self, request_id: int,
    ) -> SearchPlanInspection: ...

    def get_legacy_search_log_summary(
        self, request_id: int, *, limit: int,
    ) -> tuple[int, list[dict[str, Any]]]: ...

    def get_search_plan_stats_history(
        self, request_id: int,
    ) -> list[dict[str, Any]]: ...

    def get_search_plan_stats(
        self, request_id: int, *, current_only: bool = ...,
        prefetched_history: list[dict[str, Any]] | None = ...,
    ) -> SearchPlanStats: ...

    def get_search_acquisition_summary(
        self, request_id: int,
    ) -> SearchAcquisitionSummary: ...


@dataclass(frozen=True)
class RequestNotFound:
    """Sentinel returned when a request id has no row.

    CLI maps this to a non-zero exit; web maps it to a 404 body.
    """

    request_id: int


def _iso(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _plan_to_dict(plan: SearchPlanRow) -> dict[str, Any]:
    return {
        "id": plan.id,
        "request_id": plan.request_id,
        "generator_id": plan.generator_id,
        "status": plan.status,
        "failure_class": plan.failure_class,
        "metadata_snapshot": jsonb_to_builtins(plan.metadata_snapshot),
        "provenance": jsonb_to_builtins(plan.provenance),
        "error_message": plan.error_message,
        "superseded_at": _iso(plan.superseded_at),
        "superseded_by_plan_id": plan.superseded_by_plan_id,
        "created_at": _iso(plan.created_at),
    }


def _item_to_dict(item: SearchPlanItemRow) -> dict[str, Any]:
    return {
        "id": item.id,
        "plan_id": item.plan_id,
        "ordinal": item.ordinal,
        "strategy": item.strategy,
        "query": item.query,
        "canonical_query_key": item.canonical_query_key,
        "repeat_group": item.repeat_group,
        "provenance": jsonb_to_builtins(item.provenance),
    }


def _active_to_dict(active: ActiveSearchPlan) -> dict[str, Any]:
    return {
        "plan": _plan_to_dict(active.plan),
        "items": [_item_to_dict(it) for it in active.items],
        "next_ordinal": active.next_ordinal,
        "cycle_count": active.cycle_count,
    }


def _legacy_log_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Project a ``search_log`` dict to the small public legacy shape.

    Legacy rows pre-date persisted plan context, so plan_* fields are
    always None. We surface only the fields a human/dashboard cares
    about for "what happened before plans existed".
    """
    return {
        "id": row.get("id"),
        "created_at": _iso(row.get("created_at")),
        "outcome": row.get("outcome"),
        "variant": row.get("variant"),
        "query": row.get("query"),
        "result_count": row.get("result_count"),
        "elapsed_s": row.get("elapsed_s"),
        "final_state": row.get("final_state"),
    }


def _stats_group_to_dict(
    group: SearchPlanStatsGroup,
) -> dict[str, Any]:
    return {
        "identity": dict(group.identity),
        "attempts": group.attempts,
        "consumed_attempts": group.consumed_attempts,
        "non_consuming_attempts": group.non_consuming_attempts,
        "stale_completion_attempts": group.stale_completion_attempts,
        "outcome_counts": dict(group.outcome_counts),
        "elapsed_s_mean": group.elapsed_s_mean,
        "elapsed_s_p95": group.elapsed_s_p95,
        "result_count_mean": group.result_count_mean,
        "browse_time_s_mean": group.browse_time_s_mean,
        "match_time_s_mean": group.match_time_s_mean,
        "peers_browsed_mean": group.peers_browsed_mean,
        "fanout_waves_mean": group.fanout_waves_mean,
        "last_seen_at": _iso(group.last_seen_at),
    }


def _stats_bucket_to_dict(
    bucket: SearchPlanStatsBucket,
) -> dict[str, Any]:
    return {
        "slots": [_stats_group_to_dict(g) for g in bucket.slots],
        "query_groups": [_stats_group_to_dict(g) for g in bucket.query_groups],
        "legacy_bucket": (
            _stats_group_to_dict(bucket.legacy_bucket)
            if bucket.legacy_bucket is not None else None),
        "cache_attribution_level": bucket.cache_attribution_level,
        "cache_per_search_available": bucket.cache_per_search_available,
    }


def _stats_to_dict(stats: SearchPlanStats) -> dict[str, Any]:
    return {
        "request_id": stats.request_id,
        "current": _stats_bucket_to_dict(stats.current),
        "superseded_and_legacy": _stats_bucket_to_dict(
            stats.superseded_and_legacy),
    }


def _acquisition_to_dict(
    summary: SearchAcquisitionSummary, scope_tiers: Sequence[str],
) -> dict[str, object]:
    """Project the acquisition summary, scoring tiers against the scope.

    ``candidates_outside_scope`` is computed HERE rather than in SQL:
    what counts as in-scope is ``effective_search_tiers``' answer for
    this request's overrides plus the runtime config, and the DB layer
    has neither.
    """
    in_scope = set(scope_tiers)
    last_found = summary.last_found
    return {
        "since": _iso(summary.since),
        "since_reason": summary.since_reason,
        "candidate_tiers": [
            {"tier": t.tier, "count": t.count} for t in summary.candidate_tiers
        ],
        "candidates_outside_scope": sum(
            t.count for t in summary.candidate_tiers if t.tier not in in_scope
        ),
        "grabs": [
            {
                "filetype": g.filetype,
                "count": g.count,
                "last_at": _iso(g.last_at),
                "last_outcome": g.last_outcome,
            }
            for g in summary.grabs
        ],
        "grabs_total": summary.grabs_total,
        "last_found": (
            None if last_found is None else {
                "search_log_id": last_found.search_log_id,
                "at": _iso(last_found.at),
                "strategy": last_found.strategy,
                "username": last_found.username,
                "tier": last_found.tier,
                "matched_tracks": last_found.matched_tracks,
                "total_tracks": last_found.total_tracks,
                "grab": (
                    None if last_found.grab is None else {
                        "download_log_id": last_found.grab.download_log_id,
                        "outcome": last_found.grab.outcome,
                        "error_message": last_found.grab.error_message,
                        "at": _iso(last_found.grab.at),
                    }
                ),
            }
        ),
        "peers": [
            {
                "username": p.username,
                "tier": p.tier,
                "best_matched_tracks": p.best_matched_tracks,
                "total_tracks": p.total_tracks,
                "attempts": p.attempts,
                "last_at": _iso(p.last_at),
            }
            for p in summary.peers
        ],
    }


def build_inspection_payload(
    db: _DBLike,
    request_id: int,
    *,
    allowed_filetypes: Sequence[str],
    current_generator_id: str = SEARCH_PLAN_GENERATOR_ID,
    legacy_log_head_limit: int = LEGACY_LOG_HEAD_LIMIT,
    include_stats: bool = True,
) -> dict[str, Any] | RequestNotFound:
    """Read-only render of one request's plan/cursor state.

    The returned dict is JSON-serialisable (datetimes → ISO strings,
    everything else is dict/list/primitive) so both the API and CLI
    ``--json`` mode emit it directly.

    ``allowed_filetypes`` is the runtime config's ``allowed_filetypes``
    (``CratediggerConfig.allowed_filetypes``) — required, not defaulted,
    because ``search_scope`` claims to report the tiers the executor
    would really walk, and a stand-in default would report a scope no
    cycle uses. It is the same third argument
    ``lib/enqueue.py::find_download`` hands ``effective_search_tiers``.

    Currentness is computed against ``current_generator_id`` per the
    plan's Currentness Model:

    * ``active`` → active successful plan (regardless of generator).
    * ``current_generator_searchable`` → True iff active plan exists,
      its generator id matches, **and** the request is wanted.
    * ``generator_id_mismatch`` → True iff there is an active plan and
      its generator id does NOT match the current id (drift / stale
      reconciliation).
    * ``latest_failed_deterministic`` → sticky deterministic failure
      from the same generator id; surfaced even when an active plan
      exists so operators see "plan worked, but a regen attempt
      failed".
    * ``latest_failed_transient`` → retryable transient failure from
      the same generator id.
    """
    req = db.get_request(request_id)
    if req is None:
        return RequestNotFound(request_id=request_id)

    inspection = db.get_search_plan_inspection(request_id)
    legacy_count, legacy_rows = db.get_legacy_search_log_summary(
        request_id, limit=legacy_log_head_limit,
    )
    legacy_head = [_legacy_log_row_to_dict(r) for r in legacy_rows]

    active_dict: dict[str, Any] | None = None
    active_plan_generator_id: str | None = None
    if inspection.active is not None:
        active_dict = _active_to_dict(inspection.active)
        active_plan_generator_id = inspection.active.plan.generator_id

    status = req.get("status")
    is_wanted = status == "wanted"
    has_active_current_plan = (
        active_plan_generator_id == current_generator_id
        and inspection.active is not None
    )
    searchable = bool(is_wanted and has_active_current_plan)
    generator_id_mismatch = (
        inspection.active is not None
        and active_plan_generator_id != current_generator_id
    )

    deterministic_failed = inspection.latest_failed_deterministic
    transient_failed = inspection.latest_failed_transient

    # Issue #811: the operator's headline question on this view is "is my
    # lossless override actually in force?", so the effective tier ladder
    # is computed through the SAME function the executor uses rather than
    # re-read from the override string.
    override = req.get("search_filetype_override")
    target_format = req.get("target_format")
    configured_tiers = list(allowed_filetypes)
    scope_tiers, catch_all = effective_search_tiers(
        override, target_format, configured_tiers,
    )
    acquisition = _acquisition_to_dict(
        db.get_search_acquisition_summary(request_id), scope_tiers,
    )

    payload: dict[str, Any] = {
        "request_id": request_id,
        "request": {
            "id": req.get("id"),
            "status": status,
            "artist_name": req.get("artist_name"),
            "album_title": req.get("album_title"),
            "mb_release_id": req.get("mb_release_id"),
            "discogs_release_id": req.get("discogs_release_id"),
            "year": req.get("year"),
            "source": req.get("source"),
            "search_filetype_override": override,
            "target_format": target_format,
            "min_bitrate": req.get("min_bitrate"),
            "search_attempts": req.get("search_attempts"),
            "created_at": _iso(req.get("created_at")),
            "last_attempt_at": _iso(req.get("last_attempt_at")),
            "next_retry_after": _iso(req.get("next_retry_after")),
        },
        "search_scope": {
            "override": override,
            "target_format": target_format,
            "min_bitrate": req.get("min_bitrate"),
            "tiers": scope_tiers,
            "catch_all": catch_all,
            "configured_tiers": configured_tiers,
            # Which of the three inputs decided the ladder — the same
            # precedence ``effective_search_tiers`` applies.
            "source": (
                "override" if override
                else "target_format" if target_format
                else "config"
            ),
        },
        "acquisition": acquisition,
        "current_generator_id": current_generator_id,
        "currentness": {
            "is_wanted": is_wanted,
            "has_active_plan": inspection.active is not None,
            "active_plan_generator_id": active_plan_generator_id,
            "current_generator_searchable": searchable,
            "generator_id_mismatch": generator_id_mismatch,
            "has_deterministic_failure": deterministic_failed is not None,
            "has_retryable_failure": transient_failed is not None,
        },
        "active_plan": active_dict,
        "latest_failed_deterministic": (
            _plan_to_dict(deterministic_failed)
            if deterministic_failed is not None else None),
        "latest_failed_transient": (
            _plan_to_dict(transient_failed)
            if transient_failed is not None else None),
        "superseded_count": inspection.superseded_count,
        "legacy_logs": {
            "count": legacy_count,
            "head": legacy_head,
        },
    }
    if include_stats:
        # Stats include both current-active-plan rows and historical
        # plan rows (superseded + legacy) so dashboards can answer "is
        # this slot still useful" and "was this slot useful before".
        # Use a projection-only fetch to avoid deserializing candidates JSONB.
        stats_history = db.get_search_plan_stats_history(request_id)
        stats = db.get_search_plan_stats(
            request_id, current_only=False, prefetched_history=stats_history,
        )
        payload["stats"] = _stats_to_dict(stats)
    return payload


# ── Human renderer ───────────────────────────────────────────────
#
# The renderer walks the ``dict[str, Any]`` inspection payload built above
# (deliberately loose — it's the JSON-serialisable API/CLI contract).
# ``_as_list`` / ``_as_dict`` narrow one nested value at a time; see their
# docstrings for why a plain ``isinstance`` check alone isn't enough under
# strict mode.


def _as_list(value: object) -> list[object]:
    """Narrow an untyped payload value to a plain list.

    ``isinstance(value, list)`` alone leaves pyright with a partially-
    unknown ``list[Unknown]`` even when ``value`` was already fully
    known — strict mode never lets an ``isinstance`` narrowing inherit
    a generic's type argument. Routing through ``msgspec.convert`` gives
    every caller a fully known ``list[object]`` back, with no change to
    the elements themselves (each stays the exact same object —
    verified: ``msgspec.convert`` does not copy or coerce elements at
    ``object`` value type). A non-list value returns ``[]``, matching
    the ``... or []`` fallback every call site already uses.

    Callers must pass a freshly-evaluated expression (e.g. a ``dict``
    subscript/``.get()``), not an already ``isinstance``-narrowed local
    — the narrowing taint survives even at this declared ``object``
    parameter, same as it survives at the call site itself.
    """
    if not isinstance(value, list):
        return []
    return msgspec.convert(value, type=list[object])


def _as_dict(value: object) -> dict[str, object]:
    """Narrow an untyped payload value to a plain string-keyed dict.

    Dict counterpart of ``_as_list`` — see its docstring for why the
    ``msgspec.convert`` indirection is needed and why callers must pass
    a fresh expression rather than an already-narrowed local. A non-dict
    value returns ``{}``, matching the ``... or {}`` fallback every call
    site already uses.
    """
    if not isinstance(value, dict):
        return {}
    return msgspec.convert(value, type=dict[str, object])


def _fmt_iso(value: object) -> str:
    if value is None:
        return "-"
    return str(value)


def _fmt_bool(value: object) -> str:
    return "yes" if bool(value) else "no"


def _heading(title: str) -> str:
    return f"\n  {title}"


def _failure_lines(label: str, plan: dict[str, Any] | None) -> list[str]:
    if plan is None:
        return [f"    {label}: (none)"]
    return [
        f"    {label}:",
        f"      plan_id:        {plan['id']}",
        f"      generator_id:   {plan['generator_id']}",
        f"      status:         {plan['status']}",
        f"      failure_class:  {plan.get('failure_class') or '-'}",
        f"      created_at:     {_fmt_iso(plan.get('created_at'))}",
        f"      error_message:  {plan.get('error_message') or '-'}",
    ]


def _plan_provenance_line(prov: dict[str, Any] | None) -> list[str]:
    if not prov:
        return ["      provenance:     (none)"]
    out = ["      provenance:"]
    for key, raw_value in prov.items():
        if isinstance(raw_value, list):
            value = _as_list(
                prov[key],  # noqa: PLR1733 - preserves object type for strict Pyright
            )
            out.append(f"        {key}: {len(value)} item(s)")
            for entry in value[:5]:
                out.append(f"          - {entry}")
            if len(value) > 5:
                out.append(f"          ... +{len(value) - 5} more")
        else:
            out.append(f"        {key}: {raw_value}")
    return out


def _item_lines(item: dict[str, Any]) -> list[str]:
    head = (
        f"      [{item['ordinal']:>2}] strategy={item['strategy']}"
        f"  query={item['query']!r}"
    )
    if item.get("canonical_query_key"):
        head += f"  key={item['canonical_query_key']}"
    if item.get("repeat_group"):
        head += f"  repeat={item['repeat_group']}"
    out = [head]
    prov = item.get("provenance")
    if prov:
        for key, value in prov.items():
            out.append(f"          provenance.{key}: {value}")
    return out


def _render_search_scope_lines(scope: dict[str, object]) -> list[str]:
    """The tier ladder this request's next search will actually walk."""
    tiers = [str(t) for t in _as_list(scope.get("tiers"))]
    configured = [str(t) for t in _as_list(scope.get("configured_tiers"))]
    return [
        _heading("Search scope:"),
        f"    quality override:  {scope.get('override') or '(none)'}",
        f"    target_format:     {scope.get('target_format') or '-'}",
        f"    min_bitrate:       {_fmt_num(scope.get('min_bitrate'))}",
        f"    decided by:        {scope.get('source')}",
        f"    tiers searched:    {', '.join(tiers) if tiers else '(none)'}",
        f"    catch-all:         {_fmt_bool(scope.get('catch_all'))}",
        (
            f"    configured tiers:  "
            f"{', '.join(configured) if configured else '(none)'}"
        ),
    ]


def _render_acquisition_lines(acq: dict[str, object]) -> list[str]:
    """What the search has found since the last import, and its fate."""
    lines = [_heading("Acquisition:")]
    since = acq.get("since")
    lines.append(
        f"    since:             {_fmt_iso(since)}"
        f"  ({acq.get('since_reason')})")

    tiers = _as_list(acq.get("candidate_tiers"))
    if not tiers:
        lines.append("    candidate tiers:   (none scored)")
    else:
        rendered = " ".join(
            f"{_as_dict(t).get('tier')}={_as_dict(t).get('count')}"
            for t in tiers)
        lines.append(f"    candidate tiers:   {rendered}")
    lines.append(
        f"    outside scope:     {acq.get('candidates_outside_scope')}")

    grabs = _as_list(acq.get("grabs"))
    lines.append(f"    grabs:             {acq.get('grabs_total')} total")
    for raw in grabs:
        grab = _as_dict(raw)
        lines.append(
            f"      {grab.get('filetype') or '-'}"
            f"  x{grab.get('count')}"
            f"  last={_fmt_iso(grab.get('last_at'))}"
            f"  ({grab.get('last_outcome')})")

    found_raw = acq.get("last_found")
    if found_raw is None:
        lines.append("    last found:        (none in window)")
    else:
        found = _as_dict(found_raw)
        lines.append(
            f"    last found:        "
            f"search_log_id={found.get('search_log_id')}"
            f"  at={_fmt_iso(found.get('at'))}"
            f"  strategy={found.get('strategy') or '-'}")
        lines.append(
            f"      peer={found.get('username') or '-'}"
            f"  tier={found.get('tier') or '-'}"
            f"  matched={found.get('matched_tracks')}"
            f"/{found.get('total_tracks')}")
        grab_raw = found.get("grab")
        if grab_raw is None:
            lines.append("      grab: (no linked download_log row)")
        else:
            grab = _as_dict(grab_raw)
            lines.append(
                f"      grab: download_log_id={grab.get('download_log_id')}"
                f"  outcome={grab.get('outcome')}"
                f"  at={_fmt_iso(grab.get('at'))}")
            if grab.get("error_message"):
                lines.append(f"        error: {grab.get('error_message')}")

    peers = _as_list(acq.get("peers"))
    if not peers:
        lines.append("    peers:             (none scored)")
    else:
        lines.append(f"    peers ({len(peers)}, by attempts):")
        for raw in peers:
            peer = _as_dict(raw)
            lines.append(
                f"      {peer.get('username')}"
                f"  tier={peer.get('tier') or '-'}"
                f"  best={peer.get('best_matched_tracks')}"
                f"/{peer.get('total_tracks')}"
                f"  attempts={peer.get('attempts')}"
                f"  last={_fmt_iso(peer.get('last_at'))}")
    return lines


def render_human_lines(payload: dict[str, Any]) -> list[str]:
    """Render the inspection payload as human-readable lines.

    The caller is responsible for printing — keeps the renderer pure
    and testable.
    """
    req = payload["request"]
    cu = payload["currentness"]
    lines: list[str] = []

    lines.append(f"  Request ID:               {payload['request_id']}")
    lines.append(f"  Status:                   {req.get('status')}")
    lines.append(f"  Artist:                   {req.get('artist_name')}")
    lines.append(f"  Album:                    {req.get('album_title')}")
    lines.append(f"  Source:                   {req.get('source')}")
    lines.append(f"  Year:                     {req.get('year') or '-'}")
    lines.append(
        f"  MB Release:               {req.get('mb_release_id') or '-'}")
    lines.append(
        f"  Discogs Release:          {req.get('discogs_release_id') or '-'}")
    lines.append(
        f"  Current generator id:     {payload['current_generator_id']}")

    lines.extend(_render_search_scope_lines(_as_dict(payload["search_scope"])))
    lines.extend(_render_acquisition_lines(_as_dict(payload["acquisition"])))

    lines.append(_heading("Currentness:"))
    lines.append(f"    wanted:                       {_fmt_bool(cu['is_wanted'])}")
    lines.append(f"    has_active_plan:              {_fmt_bool(cu['has_active_plan'])}")
    lines.append(
        f"    active_plan_generator_id:     "
        f"{cu['active_plan_generator_id'] or '-'}")
    lines.append(
        f"    current_generator_searchable: "
        f"{_fmt_bool(cu['current_generator_searchable'])}"
        f"  (executable={_fmt_bool(cu['current_generator_searchable'])})")
    lines.append(
        f"    generator_id_mismatch:        "
        f"{_fmt_bool(cu['generator_id_mismatch'])}")
    lines.append(
        f"    has_deterministic_failure:    "
        f"{_fmt_bool(cu['has_deterministic_failure'])}")
    lines.append(
        f"    has_retryable_failure:        "
        f"{_fmt_bool(cu['has_retryable_failure'])}")

    active = payload["active_plan"]
    lines.append(_heading("Active successful plan:"))
    if active is None:
        lines.append("    (no active successful plan)")
    else:
        plan = active["plan"]
        lines.append(f"    plan_id:        {plan['id']}")
        lines.append(f"    generator_id:   {plan['generator_id']}")
        lines.append(f"    status:         {plan['status']}")
        lines.append(f"    created_at:     {_fmt_iso(plan.get('created_at'))}")
        lines.append(f"    next_ordinal:   {active['next_ordinal']}")
        lines.append(f"    cycle_count:    {active['cycle_count']}")
        lines.extend(_plan_provenance_line(plan.get("provenance")))
        items = active["items"]
        lines.append(f"    items ({len(items)}):")
        for item in items:
            lines.extend(_item_lines(item))

    lines.append(_heading("Current-generator failures:"))
    lines.extend(
        _failure_lines("Deterministic (sticky)",
                       payload["latest_failed_deterministic"]))
    transient = payload["latest_failed_transient"]
    lines.extend(_failure_lines("Transient (retryable)", transient))
    if transient is not None:
        lines.append(
            "      retry_eligible: yes "
            "(startup reconciliation may retry)")

    lines.append(_heading("Superseded plans:"))
    lines.append(f"    count: {payload['superseded_count']}")

    legacy = payload["legacy_logs"]
    lines.append(_heading("Legacy search log (no plan context):"))
    lines.append(f"    count: {legacy['count']}")
    if legacy["head"]:
        lines.append("    head:")
        for row in legacy["head"]:
            lines.append(
                f"      [{_fmt_iso(row.get('created_at'))}] "
                f"{row.get('outcome'):<14}"
                f" variant={row.get('variant') or '-'}"
                f" query={row.get('query')!r}")

    stats = payload.get("stats")
    if stats is not None:
        lines.extend(_render_stats_lines(stats))
    return lines


def _fmt_num(value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _stats_group_sort_key(raw: object) -> tuple[int, int]:
    """Rank a rendered stats-group dict by attempts desc, ordinal asc.

    Ties broken by ordinal so ``_render_stats_section``'s slot ranking is
    deterministic. ``attempts``/``ordinal`` are always ``int`` on a
    genuine ``SearchPlanStatsGroup`` projection (see
    ``_stats_group_to_dict``); the ``isinstance`` fallback to ``0`` is
    rendering-layer defense, matching this module's existing ``... or
    {}``/``... or []`` tolerance for a value that drifted shape.
    """
    g = _as_dict(raw)
    attempts = g.get("attempts", 0)
    ordinal = _as_dict(g.get("identity")).get("ordinal") or 0
    return (
        -attempts if isinstance(attempts, int) else 0,
        ordinal if isinstance(ordinal, int) else 0,
    )


def _render_stats_section(
    title: str, bucket: dict[str, Any],
) -> list[str]:
    lines = [_heading(title)]
    cache_label = bucket.get("cache_attribution_level")
    cache_per_search = bucket.get("cache_per_search_available")
    lines.append(
        f"    cache_attribution_level: {cache_label}"
        f" (per_search_available={_fmt_bool(cache_per_search)})")
    slots = _as_list(bucket.get("slots"))
    # Rank slots by attempts (desc) — ties broken by ordinal so the
    # output is deterministic.
    ranked_slots = sorted(slots, key=_stats_group_sort_key)
    if not ranked_slots:
        lines.append("    slots: (none)")
    else:
        lines.append(f"    slots ({len(ranked_slots)}, ranked by attempts):")
        for g_raw in ranked_slots:
            g = _as_dict(g_raw)
            ident = _as_dict(g.get("identity"))
            lines.append(
                f"      ordinal={ident.get('ordinal')}"
                f"  strategy={ident.get('strategy') or '-'}"
                f"  attempts={g.get('attempts')}"
                f"  consumed={g.get('consumed_attempts')}"
                f"  stale={g.get('stale_completion_attempts')}"
                f"  non_consuming={g.get('non_consuming_attempts')}"
            )
            outcome_counts = _as_dict(g.get("outcome_counts"))
            if outcome_counts:
                outcomes = " ".join(
                    f"{k}={v}" for k, v in
                    sorted(outcome_counts.items()))
                lines.append(f"        outcomes: {outcomes}")
            lines.append(
                f"        elapsed_s mean={_fmt_num(g.get('elapsed_s_mean'))}"
                f" p95={_fmt_num(g.get('elapsed_s_p95'))}"
                f"  result_count_mean={_fmt_num(g.get('result_count_mean'))}"
            )
            lines.append(
                f"        browse_s={_fmt_num(g.get('browse_time_s_mean'))}"
                f"  match_s={_fmt_num(g.get('match_time_s_mean'))}"
                f"  peers_browsed_mean={_fmt_num(g.get('peers_browsed_mean'))}"
                f"  fanout_waves_mean={_fmt_num(g.get('fanout_waves_mean'))}"
            )
    qg = _as_list(bucket.get("query_groups"))
    if qg:
        lines.append(f"    query_groups ({len(qg)}):")
        for g_raw in qg:
            g = _as_dict(g_raw)
            ident = _as_dict(g.get("identity"))
            lines.append(
                f"      key={ident.get('canonical_query_key') or '-'}"
                f"  repeat={ident.get('repeat_group') or '-'}"
                f"  attempts={g.get('attempts')}"
                f"  consumed={g.get('consumed_attempts')}"
            )
    legacy_bucket_raw = bucket.get("legacy_bucket")
    if legacy_bucket_raw is not None:
        legacy_bucket = _as_dict(legacy_bucket_raw)
        lines.append(
            f"    legacy_bucket: attempts={legacy_bucket.get('attempts')}"
            f"  consumed={legacy_bucket.get('consumed_attempts')}"
            f"  non_consuming={legacy_bucket.get('non_consuming_attempts')}"
        )
    return lines


def _render_stats_lines(stats: dict[str, Any]) -> list[str]:
    """Top-level stats section. Two cohorts: current vs everything else."""
    lines = [_heading("Stats:")]
    lines.extend(_render_stats_section(
        "  Current active plan:", stats.get("current") or {}))
    lines.extend(_render_stats_section(
        "  Superseded plans + legacy logs:",
        stats.get("superseded_and_legacy") or {}))
    return lines
