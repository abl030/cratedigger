"""Read-only inspection renderer for persisted search plans.

Both the CLI (``pipeline-cli search-plan show``) and the web API
(``GET /api/pipeline/<id>/search-plan``) call into this module so the
operator-facing surfaces stay in lock-step. The payload shape is the
API contract: add a key here, update the contract test in
``tests/test_web_server.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from lib.pipeline_db import (
    ActiveSearchPlan,
    SearchPlanInspection,
    SearchPlanItemRow,
    SearchPlanRow,
    SearchPlanStats,
    SearchPlanStatsBucket,
    SearchPlanStatsGroup,
    jsonb_to_builtins,
)
from lib.search import SEARCH_PLAN_GENERATOR_ID

# Number of legacy (plan_id IS NULL) ``search_log`` rows to surface as
# a head sample. Operators inspecting a stuck request need a quick read
# on what the pre-plan-context history looked like; the full list is
# always available via ``pipeline-cli show``.
LEGACY_LOG_HEAD_LIMIT = 5


class _DBLike(Protocol):
    """The slice of PipelineDB / FakePipelineDB we depend on."""

    def get_request(
        self, request_id: int,
    ) -> dict[str, Any] | None: ...

    def get_search_plan_inspection(
        self, request_id: int,
    ) -> SearchPlanInspection: ...

    def get_search_history(
        self, request_id: int,
    ) -> list[dict[str, Any]]: ...

    def get_search_plan_stats(
        self, request_id: int, *, current_only: bool = ...,
        prefetched_history: list[dict[str, Any]] | None = ...,
    ) -> SearchPlanStats: ...


@dataclass(frozen=True)
class RequestNotFound:
    """Sentinel returned when a request id has no row.

    CLI maps this to a non-zero exit; web maps it to a 404 body.
    """

    request_id: int


def _iso(value: object) -> object:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()  # type: ignore[union-attr]
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


def _legacy_logs_for_request(
    history: list[dict[str, Any]],
    *,
    limit: int,
) -> tuple[int, list[dict[str, Any]]]:
    """Return (count, head sample) of plan-less search_log rows.

    ``history`` is the newest-first list returned by
    ``PipelineDB.get_search_history``. We filter to rows where
    ``plan_id`` is None so plan-aware rows don't drown out the legacy
    bucket.
    """
    legacy = [r for r in history if r.get("plan_id") is None]
    return len(legacy), [_legacy_log_row_to_dict(r) for r in legacy[:limit]]


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


def build_inspection_payload(
    db: _DBLike,
    request_id: int,
    *,
    current_generator_id: str = SEARCH_PLAN_GENERATOR_ID,
    legacy_log_head_limit: int = LEGACY_LOG_HEAD_LIMIT,
    include_stats: bool = True,
) -> dict[str, Any] | RequestNotFound:
    """Read-only render of one request's plan/cursor state.

    The returned dict is JSON-serialisable (datetimes → ISO strings,
    everything else is dict/list/primitive) so both the API and CLI
    ``--json`` mode emit it directly.

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
    history = db.get_search_history(request_id)

    legacy_count, legacy_head = _legacy_logs_for_request(
        history, limit=legacy_log_head_limit,
    )

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
        },
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
        # Pass `history` to skip a second fetch of search_log.
        stats = db.get_search_plan_stats(
            request_id, current_only=False, prefetched_history=history,
        )
        payload["stats"] = _stats_to_dict(stats)
    return payload


# ── Human renderer ───────────────────────────────────────────────


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
    for key, value in prov.items():
        if isinstance(value, list):
            out.append(f"        {key}: {len(value)} item(s)")
            for entry in value[:5]:
                out.append(f"          - {entry}")
            if len(value) > 5:
                out.append(f"          ... +{len(value) - 5} more")
        else:
            out.append(f"        {key}: {value}")
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


def _render_stats_section(
    title: str, bucket: dict[str, Any],
) -> list[str]:
    lines = [_heading(title)]
    cache_label = bucket.get("cache_attribution_level")
    cache_per_search = bucket.get("cache_per_search_available")
    lines.append(
        f"    cache_attribution_level: {cache_label}"
        f" (per_search_available={_fmt_bool(cache_per_search)})")
    slots = bucket.get("slots") or []
    # Rank slots by attempts (desc) — ties broken by ordinal so the
    # output is deterministic.
    ranked_slots = sorted(
        slots,
        key=lambda g: (-int(g.get("attempts", 0)),
                       g.get("identity", {}).get("ordinal") or 0),
    )
    if not ranked_slots:
        lines.append("    slots: (none)")
    else:
        lines.append(f"    slots ({len(ranked_slots)}, ranked by attempts):")
        for g in ranked_slots:
            ident = g.get("identity") or {}
            lines.append(
                f"      ordinal={ident.get('ordinal')}"
                f"  strategy={ident.get('strategy') or '-'}"
                f"  attempts={g.get('attempts')}"
                f"  consumed={g.get('consumed_attempts')}"
                f"  stale={g.get('stale_completion_attempts')}"
                f"  non_consuming={g.get('non_consuming_attempts')}"
            )
            outcome_counts = g.get("outcome_counts") or {}
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
    qg = bucket.get("query_groups") or []
    if qg:
        lines.append(f"    query_groups ({len(qg)}):")
        for g in qg:
            ident = g.get("identity") or {}
            lines.append(
                f"      key={ident.get('canonical_query_key') or '-'}"
                f"  repeat={ident.get('repeat_group') or '-'}"
                f"  attempts={g.get('attempts')}"
                f"  consumed={g.get('consumed_attempts')}"
            )
    legacy_bucket = bucket.get("legacy_bucket")
    if legacy_bucket is not None:
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
