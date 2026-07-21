"""pipeline-cli ``search-plan`` command family (#495 carve).

``search-plan show / regenerate / dry-run / saturation / advance / history``
— read-only inspection plus the operator mutations (regenerate, advance)
over persisted search plans. All wrap ``lib.search_plan_service`` (CLI ⇄
API surface symmetry, CLAUDE.md).
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Protocol, TYPE_CHECKING

import msgspec

from scripts.pipeline_cli._format import _json_default

if TYPE_CHECKING:
    from lib.pipeline_db import SearchPlanInspection, SearchPlanStats
    from lib.pipeline_db.rows import AlbumRequestRow
    from lib.search_plan_service import SearchPlanDB


class _SearchPlanShowDB(Protocol):
    """``db`` shape ``cmd_search_plan_show`` needs — mirrors
    ``lib.search_plan_inspection._DBLike`` structurally (issue #784,
    #409 pattern) so ``FakePipelineDB`` conforms without importing that
    private symbol across the module boundary."""

    def get_request(self, request_id: int) -> "AlbumRequestRow | None": ...

    def get_search_plan_inspection(
        self, request_id: int,
    ) -> "SearchPlanInspection": ...

    def get_legacy_search_log_summary(
        self, request_id: int, *, limit: int,
    ) -> tuple[int, list[dict[str, Any]]]: ...

    def get_search_plan_stats_history(
        self, request_id: int,
    ) -> list[dict[str, Any]]: ...

    def get_search_plan_stats(
        self,
        request_id: int,
        *,
        current_only: bool = ...,
        prefetched_history: list[dict[str, Any]] | None = ...,
    ) -> "SearchPlanStats": ...


def _search_plan_exit_code(outcome: str) -> int:
    """CLI ⇄ API exit-code mapping for search-plan read/advance subcommands.

    Per CLAUDE.md § "CLI ⇄ API surface symmetry":
    0=success, 2=not_found, 3=input_validation, 4=wrong_state, 5=transient.

    Covers the outcome strings emitted by ``dry_run_for_request``,
    ``saturation_for_request``, ``advance_for_request`` and
    ``history_for_request``. Regenerate has its own ladder
    (``failed_transient`` → 4 there, predating this convention).
    """
    from lib.search_plan_service import (
        RESULT_ADVANCED,
        RESULT_DRY_RUN_GENERATION_FAILED,
        RESULT_DRY_RUN_SUCCESS,
        RESULT_FAILED_TRANSIENT,
        RESULT_HISTORY_PAGE_INPUT_INVALID,
        RESULT_HISTORY_PAGE_SUCCESS,
        RESULT_INVALID_TARGET,
        RESULT_NO_ACTIVE_PLAN,
        RESULT_REQUEST_NOT_FOUND,
        RESULT_REQUEST_REPLACED,
        RESULT_SATURATION_INPUT_INVALID,
        RESULT_SATURATION_SUCCESS,
    )
    mapping: dict[str, int] = {
        RESULT_DRY_RUN_SUCCESS: 0,
        RESULT_DRY_RUN_GENERATION_FAILED: 0,
        RESULT_SATURATION_SUCCESS: 0,
        RESULT_ADVANCED: 0,
        RESULT_HISTORY_PAGE_SUCCESS: 0,
        RESULT_REQUEST_NOT_FOUND: 2,
        RESULT_REQUEST_REPLACED: 4,
        RESULT_SATURATION_INPUT_INVALID: 3,
        RESULT_INVALID_TARGET: 3,
        RESULT_HISTORY_PAGE_INPUT_INVALID: 3,
        RESULT_NO_ACTIVE_PLAN: 4,
        RESULT_FAILED_TRANSIENT: 5,
    }
    return mapping.get(outcome, 1)


def cmd_search_plan_show(
    db: "_SearchPlanShowDB", args: argparse.Namespace,
) -> int:
    """U6: read-only `pipeline-cli search-plan show <id>`.

    Default: human-readable text including the U8 stats section. Pass
    ``--no-stats`` to suppress stats (useful for legacy assertions /
    scripts that want only the static plan dump). ``--json``: same
    payload the web route emits, useful for scripting / future
    dashboard parity. Exit code 2 on missing request, 0 on found.
    """
    from lib.search_plan_inspection import (
        RequestNotFound,
        build_inspection_payload,
        render_human_lines,
    )

    include_stats = not getattr(args, "no_stats", False)
    payload = build_inspection_payload(
        db, int(args.id), include_stats=include_stats)
    if isinstance(payload, RequestNotFound):
        if getattr(args, "json", False):
            print(json.dumps({
                "error": "Not found",
                "request_id": payload.request_id,
            }, indent=2, sort_keys=True))
            return 2
        print(f"  Request {payload.request_id} not found.")
        return 2
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0
    for line in render_human_lines(payload):
        print(line)
    return 0


def cmd_search_plan_regenerate(
    db: "SearchPlanDB", args: argparse.Namespace,
) -> int:
    """U8: ``pipeline-cli search-plan regenerate <request_id>``.

    Wraps ``SearchPlanService.generate_for_request(regenerate=True)``
    so the CLI never hand-rolls plan persistence. Allowed for every
    non-terminal request status, but only ``wanted`` requests with a
    successful active plan are executable. Replaced audit ancestors
    reject regeneration.

    Exit codes:
      * 0 — ``RESULT_SUCCESS`` or ``RESULT_NOOP_ACTIVE_PLAN_EXISTS``
        (the latter only when called without ``--regenerate``-style
        force; the service treats explicit regeneration as always
        attempting).
      * 2 — ``RESULT_REQUEST_NOT_FOUND`` (matches search-plan show).
      * 3 — ``RESULT_FAILED_DETERMINISTIC`` (sticky failure; old
        active plan preserved).
      * 4 — ``RESULT_REQUEST_REPLACED`` or ``RESULT_FAILED_TRANSIENT``
        (the latter is retryable; old active plan preserved).
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_FAILED_DETERMINISTIC,
        RESULT_FAILED_TRANSIENT,
        RESULT_NOOP_ACTIVE_PLAN_EXISTS,
        RESULT_REQUEST_NOT_FOUND,
        RESULT_REQUEST_REPLACED,
        RESULT_SUCCESS,
        SearchPlanService,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.generate_for_request(
        int(args.id),
        regenerate=True,
        prepend_artist=getattr(args, "prepend_artist", None),
    )

    payload = {
        "request_id": int(args.id),
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "is_supersede": result.is_supersede,
        "failure_class": result.failure_class,
        "error_message": result.error_message,
    }
    # Add an executability hint so operators don't misread "200 / success"
    # on an imported/unsearchable request as "now downloading".
    req = db.get_request(int(args.id))
    if req is not None:
        payload["request_status"] = req.get("status")
        payload["executable"] = (
            req.get("status") == "wanted"
            and result.outcome == RESULT_SUCCESS
        )
    else:
        payload["request_status"] = None
        payload["executable"] = False

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:        {payload['request_id']}")
        print(f"  Outcome:           {result.outcome}")
        if result.plan_id is not None:
            print(f"  New plan id:       {result.plan_id}")
        if result.is_supersede:
            print("  Replaced previous active plan: yes")
        if result.failure_class:
            print(f"  Failure class:     {result.failure_class}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")
        print(f"  Request status:    {payload['request_status'] or '-'}")
        print(f"  Executable:        {'yes' if payload['executable'] else 'no'}")
        if not payload["executable"] and result.outcome == RESULT_SUCCESS:
            print("  Note: only `wanted` requests run searches; the new "
                  "plan is recorded but will not be executed for this status.")

    if result.outcome == RESULT_SUCCESS:
        return 0
    if result.outcome == RESULT_NOOP_ACTIVE_PLAN_EXISTS:
        return 0
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        return 2
    if result.outcome == RESULT_REQUEST_REPLACED:
        return 4
    if result.outcome == RESULT_FAILED_DETERMINISTIC:
        return 3
    if result.outcome == RESULT_FAILED_TRANSIENT:
        return 4
    # Defensive fallback for any future outcome string.
    return 1


def cmd_search_plan_dry_run(
    db: "SearchPlanDB", args: argparse.Namespace,
) -> int:
    """U6: ``pipeline-cli search-plan dry-run <request_id>``.

    Read-only simulator: runs the current generator against the
    request's persisted snapshot and prints the slot list without
    writing anything. Counterpart of ``GET /api/pipeline/<id>/search-plan/dry-run``.
    Both surfaces wrap ``SearchPlanService.dry_run_for_request`` — keep
    them in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Use this during generator development (see
    ``.claude/rules/code-quality.md`` § "Pipeline Decision Debugging
    — Simulator-First TDD") to validate that the next cycle's
    generator output matches expectations before bumping
    ``SEARCH_PLAN_GENERATOR_ID``.

    Exit codes:
      * 0 — ``RESULT_DRY_RUN_SUCCESS`` or
        ``RESULT_DRY_RUN_GENERATION_FAILED`` (generator returned a
        deterministic generation failure — informational, not a CLI
        error; the operator still wants to see ``failure_reason`` and
        provenance).
      * 2 — ``RESULT_REQUEST_NOT_FOUND``.
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        SearchPlanService,
        dry_run_payload,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.dry_run_for_request(
        int(args.id),
        prepend_artist=getattr(args, "prepend_artist", None),
    )
    row = db.get_request(int(args.id))
    has_active = False
    if row is not None:
        try:
            active = db.get_active_search_plan(int(args.id))
            has_active = active is not None
        except Exception:  # noqa: BLE001
            has_active = False
    payload = dry_run_payload(
        result,
        current_generator_id=svc.generator_id,
        request_row=row,
        has_active_plan=has_active,
    )

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:             {payload['request_id']}")
        print(f"  Outcome:                {payload['outcome']}")
        print(
            f"  Current generator id:   {payload['current_generator_id']}")
        if payload["request"] is not None:
            req = payload["request"]
            print(f"  Artist:                 {req.get('artist_name')}")
            print(f"  Album:                  {req.get('album_title')}")
            print(f"  Status:                 {req.get('status')}")
            print(f"  Year:                   {req.get('year') or '-'}")
            rg = req.get("release_group_year")
            print(f"  Release-group year:     {rg if rg is not None else '-'}")
            print(
                f"  Would supersede active: "
                f"{'yes' if payload['would_supersede_active'] else 'no'}")
        plan = payload["plan"]
        if plan is None:
            print(f"  Plan:                   (none)")
            if result.error_message:
                print(f"  Error message:          {result.error_message}")
        else:
            print(f"  Plan generator_id:      {plan['generator_id']}")
            print(f"  Plan status:            {plan['status']}")
            if plan["failure_reason"]:
                print(
                    f"  Plan failure_reason:    {plan['failure_reason']}")
            items = plan["items"]
            print(f"  Plan items ({len(items)}):")
            for it in items:
                head = (
                    f"    [{it['ordinal']:>2}] strategy={it['strategy']}"
                    f"  query={it['query']!r}")
                if it.get("canonical_query_key"):
                    head += f"  key={it['canonical_query_key']}"
                if it.get("repeat_group"):
                    head += f"  repeat={it['repeat_group']}"
                print(head)
                prov: dict[str, Any] = it.get("provenance") or {}
                for key, value in prov.items():
                    print(f"          provenance.{key}: {value}")
            prov_plan: dict[str, Any] = plan.get("provenance") or {}
            if prov_plan:
                print(f"  Plan provenance:")
                for pkey, pvalue in prov_plan.items():
                    if isinstance(pvalue, list):
                        value_list = msgspec.convert(pvalue, type=list[object])
                        print(f"    {pkey}: {len(value_list)} item(s)")
                        for entry in value_list[:5]:
                            print(f"      - {entry}")
                        if len(value_list) > 5:
                            print(f"      ... +{len(value_list) - 5} more")
                    else:
                        print(f"    {pkey}: {pvalue}")

    return _search_plan_exit_code(result.outcome)


def cmd_search_plan_saturation(
    db: "SearchPlanDB", args: argparse.Namespace,
) -> int:
    """U7: ``pipeline-cli search-plan saturation <request_id>``.

    Read-only telemetry aggregator: reports the saturation rate (rows
    whose ``final_state`` contains ``LimitReached``) and total
    ``pre_filter_skip_count`` over the last ``--window-days`` (default
    14) of ``search_log`` rows. Counterpart of
    ``GET /api/pipeline/<id>/search-plan/saturation``; both surfaces
    wrap ``SearchPlanService.saturation_for_request`` — keep them in
    sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Exit codes:
      * 0 — ``RESULT_SATURATION_SUCCESS`` (zeros are still success —
        the request exists, the window is just quiet)
      * 2 — ``RESULT_REQUEST_NOT_FOUND``
      * 3 — ``RESULT_SATURATION_INPUT_INVALID`` (argparse normally
        bounds this; the branch is defensive parity with the API's
        400)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        SATURATION_WINDOW_DEFAULT_DAYS,
        SearchPlanService,
        saturation_payload,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    # ``None`` means "argparse default" (operator omitted the flag);
    # treat 0 / negative as explicit and let the service flag them
    # invalid so the operator sees the failure rather than silently
    # widening to 14.
    raw_window = getattr(args, "window_days", None)
    window_days = int(
        raw_window if raw_window is not None
        else SATURATION_WINDOW_DEFAULT_DAYS)
    result = svc.saturation_for_request(
        int(args.id), window_days=window_days,
    )
    payload = saturation_payload(result)

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:             {payload['request_id']}")
        print(f"  Outcome:                {payload['outcome']}")
        print(f"  Window (days):          {payload['window_days']}")
        print(f"  Total searches:         {payload['total_searches']}")
        print(f"  Saturated searches:     {payload['saturated_searches']}")
        # Render the rate as a percentage with one decimal so the
        # number is human-readable at a glance.
        rate_pct = 100.0 * float(payload['saturation_rate'])
        print(f"  Saturation rate:        {rate_pct:.1f}%")
        print(
            f"  Pre-filter skips total: {payload['total_pre_filter_skips']}")
        if payload.get("error_message"):
            print(f"  Error message:          {payload['error_message']}")

    return _search_plan_exit_code(result.outcome)


def cmd_search_plan_advance(
    db: "SearchPlanDB", args: argparse.Namespace,
) -> int:
    """Forward-only operator advance of the search-plan cursor.

    Counterpart of ``POST /api/pipeline/<id>/search-plan/advance``. Both
    surfaces wrap ``SearchPlanService.advance_for_request`` — keep them
    in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Exit codes:
      * 0 — ``RESULT_ADVANCED``
      * 2 — ``RESULT_REQUEST_NOT_FOUND``
      * 3 — ``RESULT_INVALID_TARGET`` (out of range, would go backward,
        no slot matches strategy, or both/neither flag given)
      * 4 — ``RESULT_NO_ACTIVE_PLAN`` or ``RESULT_REQUEST_REPLACED``
      * 5 — ``RESULT_FAILED_TRANSIENT`` (lock contention)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        SearchPlanService,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.advance_for_request(
        int(args.id),
        to_ordinal=args.to_ordinal,
        to_strategy=args.to_strategy,
    )
    payload = {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "previous_ordinal": result.previous_ordinal,
        "new_ordinal": result.new_ordinal,
        "new_strategy": result.new_strategy,
        "new_query": result.new_query,
        "error_message": result.error_message,
    }
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:        {payload['request_id']}")
        print(f"  Outcome:           {result.outcome}")
        if result.plan_id is not None:
            print(f"  Plan id:           {result.plan_id}")
        if (result.previous_ordinal is not None
                and result.new_ordinal is not None):
            print(
                f"  Cursor:            {result.previous_ordinal} → "
                f"{result.new_ordinal}")
        if result.new_strategy is not None:
            print(f"  New slot strategy: {result.new_strategy}")
        if result.new_query is not None:
            print(f"  New slot query:    {result.new_query}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")

    return _search_plan_exit_code(result.outcome)


def cmd_search_plan_history(
    db: "SearchPlanDB", args: argparse.Namespace,
) -> int:
    """Cursor-paginated read of one request's ``search_log`` rows.

    Counterpart of ``GET /api/pipeline/<id>/search-plan/history``. Both
    surfaces wrap ``SearchPlanService.history_for_request`` — keep them
    in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Default limit is ``HISTORY_PAGE_DEFAULT_LIMIT`` (50). Pass
    ``--before-id <id>`` to read the next page. JSON mode returns the
    same payload as the API (``request_id`` / ``rows`` / ``next_before_id``).

    Exit codes:
      * 0 — ``RESULT_HISTORY_PAGE_SUCCESS``
      * 2 — ``RESULT_REQUEST_NOT_FOUND``
      * 3 — ``RESULT_HISTORY_PAGE_INPUT_INVALID`` (limit out of bounds,
        before_id < 1)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        HISTORY_PAGE_DEFAULT_LIMIT,
        RESULT_HISTORY_PAGE_SUCCESS,
        SearchPlanService,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    limit = args.limit if args.limit is not None else HISTORY_PAGE_DEFAULT_LIMIT
    result = svc.history_for_request(
        int(args.id),
        limit=int(limit),
        before_id=args.before_id,
    )
    payload = {
        "request_id": result.request_id,
        "rows": result.rows,
        "next_before_id": result.next_before_id,
        "outcome": result.outcome,
        "error_message": result.error_message,
    }
    if getattr(args, "json", False):
        if result.outcome == RESULT_HISTORY_PAGE_SUCCESS:
            # F7: strip internal routing keys so --json output matches the
            # API 200 shape (CLI ⇄ API surface symmetry, CLAUDE.md).
            api_payload = {
                "request_id": result.request_id,
                "rows": result.rows,
                "next_before_id": result.next_before_id,
            }
        else:
            api_payload = payload
        print(json.dumps(api_payload, indent=2, sort_keys=True,
                         default=_json_default))
    elif result.outcome == RESULT_HISTORY_PAGE_SUCCESS:
        print(f"  Request ID:        {result.request_id}")
        print(f"  Rows on page:      {len(result.rows)}")
        print(f"  Next before-id:    "
              f"{result.next_before_id if result.next_before_id is not None else '-'}")
        for row in result.rows:
            created = row.get("created_at") or "-"
            outcome = row.get("outcome") or "-"
            strategy = row.get("plan_strategy") or "(legacy)"
            ordinal = row.get("plan_ordinal")
            ord_str = f"ord={ordinal}" if ordinal is not None else "ord=-"
            query = row.get("query") or ""
            row_id = row.get("id")
            print(
                f"  [{created}] id={row_id} {outcome} {strategy} "
                f"{ord_str} query={query!r}"
            )
        if result.next_before_id is not None:
            print(
                "  Next page: "
                f"pipeline-cli search-plan history {result.request_id} "
                f"--before-id {result.next_before_id}"
            )
    else:
        print(f"  Request ID:        {result.request_id}")
        print(f"  Outcome:           {result.outcome}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")

    return _search_plan_exit_code(result.outcome)


def add_search_plan_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    """Add ``search-plan`` + its nested subcommands (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions).

    Returns the ``search-plan`` subparser itself so ``main()`` can print
    its help when invoked without a nested subcommand (mirrors the
    ``triage`` no-subcommand handling).
    """
    p_sp = sub.add_parser(
        "search-plan",
        help="Inspect persisted search plans (read-only, U6)")
    sp_sub = p_sp.add_subparsers(dest="search_plan_command")
    p_sp_show = sp_sub.add_parser(
        "show",
        help="Show active/failed plans, cursor, items, provenance, "
             "legacy logs for one request")
    p_sp_show.add_argument("id", type=int, help="Request ID")
    p_sp_show.add_argument("--json", action="store_true",
                            help="Print structured JSON instead of text")
    p_sp_show.add_argument("--no-stats", action="store_true",
                            dest="no_stats",
                            help="Suppress per-slot/query usefulness stats")
    p_sp_regen = sp_sub.add_parser(
        "regenerate",
        help="Regenerate the search plan for a request (U8)")
    p_sp_regen.add_argument("id", type=int, help="Request ID")
    p_sp_regen.add_argument("--prepend-artist", action="store_true",
                             dest="prepend_artist", default=None,
                             help="Prepend artist name to album title in "
                             "generated queries (overrides config; absent "
                             "means use config's album_prepend_artist)")
    p_sp_regen.add_argument("--json", action="store_true",
                             help="Print structured JSON instead of text")
    p_sp_advance = sp_sub.add_parser(
        "advance",
        help="Forward-only operator advance of the cursor (e.g. skip "
             "collapsed default-strategy slots on a self-titled release)")
    p_sp_advance.add_argument("id", type=int, help="Request ID")
    sp_target = p_sp_advance.add_mutually_exclusive_group(required=True)
    sp_target.add_argument(
        "--to-ordinal", type=int, dest="to_ordinal",
        help="Absolute target ordinal in [0, plan_item_count)")
    sp_target.add_argument(
        "--to-strategy", dest="to_strategy",
        help="Strategy prefix; advance to the first plan item past the "
             "current cursor whose strategy starts with this string "
             "(e.g. `track`, `unwild_year`)")
    p_sp_advance.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")
    p_sp_dry_run = sp_sub.add_parser(
        "dry-run",
        help="Run the generator against the request's snapshot without "
             "persisting (U6 simulator)")
    p_sp_dry_run.add_argument("id", type=int, help="Request ID")
    p_sp_dry_run.add_argument("--prepend-artist", action="store_true",
                              dest="prepend_artist", default=None,
                              help="Prepend artist name to album title in "
                              "generated queries (overrides config; absent "
                              "means use config's album_prepend_artist)")
    p_sp_dry_run.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")
    p_sp_saturation = sp_sub.add_parser(
        "saturation",
        help="Show per-request saturation rate + pre-filter skip total "
             "over the recent search_log window (U7 telemetry)")
    p_sp_saturation.add_argument("id", type=int, help="Request ID")
    p_sp_saturation.add_argument(
        "--window-days", type=int, default=None, dest="window_days",
        help="Window in days; defaults to 14; valid range [1, 90]")
    p_sp_saturation.add_argument(
        "--json", action="store_true",
        help="Print structured JSON instead of text")
    p_sp_history = sp_sub.add_parser(
        "history",
        help="Cursor-paginated read of one request's search_log rows "
             "(per-attempt forensics)")
    p_sp_history.add_argument("id", type=int, help="Request ID")
    p_sp_history.add_argument(
        "--limit", type=int, default=None,
        help="Rows per page; defaults to 50; valid range [1, 200]")
    p_sp_history.add_argument(
        "--before-id", type=int, default=None, dest="before_id",
        help="Resume cursor: pass the previous page's next_before_id")
    p_sp_history.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")
    return p_sp
