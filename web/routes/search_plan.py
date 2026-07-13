"""Search-plan routes for a pipeline request — persisted plan read, dry-run
simulator, saturation telemetry, cursor history, regenerate, and advance.

Split from web/routes/pipeline.py (#481 item 3). Every handler here wraps
``lib.search_plan_service.SearchPlanService`` / ``lib.search_plan_inspection``
— same services ``pipeline-cli search-plan *`` wraps (CLI ⇄ API symmetry,
see CLAUDE.md).
"""

import logging

import msgspec
from pydantic import BaseModel, Field, model_validator

from lib.quality import CandidateScore
from web.routes._pydantic import parse_body
from web.routes._registry import RouteRegistration, pattern_route
from web.routes._server_access import _server

logger = logging.getLogger(__name__)


def get_pipeline_search_plan(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """U6: read-only view of a request's persisted search plan.

    Mirrors ``pipeline-cli search-plan show --json`` so the future
    dashboard and operators see the same JSON. The U8 stats block is
    included by default; pass ``stats=0`` to suppress it for a leaner
    payload (the show endpoint stays a single contract).
    """
    from lib.search_plan_inspection import (
        RequestNotFound,
        build_inspection_payload,
    )
    include_stats = params.get("stats", ["1"])[0] != "0"
    db = _server()._db()
    payload = build_inspection_payload(
        db, int(req_id_str), include_stats=include_stats)
    if isinstance(payload, RequestNotFound):
        h._error("Not found", 404)
        return
    h._json(payload)


def get_pipeline_search_plan_dry_run(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """U6: ``GET /api/pipeline/<id>/search-plan/dry-run``.

    Read-only generator simulator. Runs ``generate_search_plan``
    against the current persisted snapshot for ``<id>`` and returns
    the resulting plan items without writing anything. Counterpart of
    ``pipeline-cli search-plan dry-run``; both surfaces wrap
    ``SearchPlanService.dry_run_for_request`` — keep them in sync
    (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Query string:
      * ``prepend_artist`` — ``1`` to enable (defaults to runtime
        config's ``album_prepend_artist``).

    Status-code mapping:
      * 200 — ``RESULT_DRY_RUN_SUCCESS`` or
        ``RESULT_DRY_RUN_GENERATION_FAILED`` (the latter is a
        deterministic generator outcome the operator wants to see in
        the body — the route is informational, not a hard error).
      * 404 — ``RESULT_REQUEST_NOT_FOUND``.
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_DRY_RUN_GENERATION_FAILED,
        RESULT_DRY_RUN_SUCCESS,
        RESULT_REQUEST_NOT_FOUND,
        SearchPlanService,
        dry_run_payload,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    prepend_artist_raw = params.get("prepend_artist", [None])[0]
    prepend_artist: bool | None
    if prepend_artist_raw is None or prepend_artist_raw == "":
        prepend_artist = None
    else:
        prepend_artist = prepend_artist_raw == "1"

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.dry_run_for_request(
        request_id, prepend_artist=prepend_artist,
    )
    row = db.get_request(request_id)
    has_active = False
    if row is not None:
        try:
            active = db.get_active_search_plan(request_id)
            has_active = active is not None
        except Exception:  # noqa: BLE001
            has_active = False
    payload = dry_run_payload(
        result,
        current_generator_id=svc.generator_id,
        request_row=row,
        has_active_plan=has_active,
    )
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome in (
            RESULT_DRY_RUN_SUCCESS, RESULT_DRY_RUN_GENERATION_FAILED):
        h._json(payload)
        return
    # Defensive fallback for any future outcome string.
    h._error(f"Unknown dry-run outcome: {result.outcome}", 500)


def get_pipeline_search_plan_saturation(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """U7: ``GET /api/pipeline/<id>/search-plan/saturation``.

    Read-only telemetry aggregator. Returns the saturation rate (rows
    whose ``final_state`` contains ``LimitReached``) and total
    ``pre_filter_skip_count`` over the last ``window_days`` (default
    14) of ``search_log`` rows for ``<id>``. Counterpart of
    ``pipeline-cli search-plan saturation``; both surfaces wrap
    ``SearchPlanService.saturation_for_request`` — keep them in sync
    (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Query string:
      * ``window_days`` — int in
        ``[SATURATION_WINDOW_MIN_DAYS, SATURATION_WINDOW_MAX_DAYS]``;
        defaults to ``SATURATION_WINDOW_DEFAULT_DAYS``.

    Status-code mapping:
      * 200 — ``RESULT_SATURATION_SUCCESS`` (counts may be zero — a
        valid "found but quiet" state).
      * 400 — ``window_days`` not parseable as int, or
        ``RESULT_SATURATION_INPUT_INVALID``.
      * 404 — ``RESULT_REQUEST_NOT_FOUND`` (request_id does not exist
        in ``album_requests``; distinct from a real request whose
        window happens to be empty).
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_REQUEST_NOT_FOUND,
        RESULT_SATURATION_INPUT_INVALID,
        RESULT_SATURATION_SUCCESS,
        SATURATION_WINDOW_DEFAULT_DAYS,
        SearchPlanService,
        saturation_payload,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    window_raw = params.get("window_days", [None])[0]
    if window_raw is None or window_raw == "":
        window_days = SATURATION_WINDOW_DEFAULT_DAYS
    else:
        try:
            window_days = int(window_raw)
        except (TypeError, ValueError):
            h._error(
                f"window_days must be an integer; got {window_raw!r}",
                status=400,
            )
            return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.saturation_for_request(
        request_id, window_days=window_days,
    )
    payload = saturation_payload(result)
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome == RESULT_SATURATION_INPUT_INVALID:
        payload["error"] = (
            result.error_message or "Invalid window_days")
        h._json(payload, status=400)
        return
    if result.outcome == RESULT_SATURATION_SUCCESS:
        h._json(payload)
        return
    # Defensive fallback for any future outcome string.
    h._error(f"Unknown saturation outcome: {result.outcome}", 500)


def get_pipeline_search_plan_history(
    h, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """``GET /api/pipeline/<id>/search-plan/history``.

    Cursor-paginated read of one request's ``search_log`` rows. Wraps
    ``SearchPlanService.history_for_request``; both this route and
    ``pipeline-cli search-plan history`` go through the same service
    method so the input bounds and outcome mapping cannot drift.

    Query string:
      * ``limit`` — int in ``[1, 200]``; defaults to
        ``HISTORY_PAGE_DEFAULT_LIMIT`` when omitted.
      * ``before_id`` — int >= 1; the ``next_before_id`` from the
        previous page. Omit for the first page.

    Status-code mapping:
      * 200 — ``RESULT_HISTORY_PAGE_SUCCESS``
      * 400 — query string non-int / ``RESULT_HISTORY_PAGE_INPUT_INVALID``
      * 404 — ``RESULT_REQUEST_NOT_FOUND``
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        HISTORY_PAGE_DEFAULT_LIMIT,
        RESULT_HISTORY_PAGE_INPUT_INVALID,
        RESULT_HISTORY_PAGE_SUCCESS,
        RESULT_REQUEST_NOT_FOUND,
        SearchPlanService,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    limit_raw = params.get("limit", [None])[0]
    if limit_raw is None or limit_raw == "":
        limit = HISTORY_PAGE_DEFAULT_LIMIT
    else:
        try:
            limit = int(limit_raw)
        except (TypeError, ValueError):
            h._error("limit must be an integer")
            return
    before_id_raw = params.get("before_id", [None])[0]
    before_id: int | None
    if before_id_raw is None or before_id_raw == "":
        before_id = None
    else:
        try:
            before_id = int(before_id_raw)
        except (TypeError, ValueError):
            h._error("before_id must be an integer")
            return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.history_for_request(
        request_id, limit=limit, before_id=before_id,
    )
    s = _server()
    if result.outcome == RESULT_HISTORY_PAGE_SUCCESS:
        # F1: map rows through _serialize_row so datetime fields
        # (created_at) become ISO strings before json.dumps is called.
        # F5: pass candidates through msgspec.convert + msgspec.to_builtins
        # for symmetric wire-boundary strictness (mirrors _build_last_search_payload).
        serialized_rows: list[dict[str, object]] = []
        for r in result.rows:
            sr = s._serialize_row(r)
            raw_candidates = sr.get("candidates")
            if raw_candidates is not None:
                try:
                    candidates = msgspec.convert(
                        raw_candidates, type=list[CandidateScore])
                    sr["candidates"] = [
                        msgspec.to_builtins(c) for c in candidates]
                except msgspec.ValidationError as exc:
                    logger.warning(
                        "search_log.candidates JSONB failed msgspec validation "
                        "(request_id=%s, search_log_id=%s): %s",
                        r.get("request_id"), r.get("id"), exc,
                    )
                    sr["candidates"] = None
            serialized_rows.append(sr)
        payload: dict[str, object] = {
            "request_id": result.request_id,
            "rows": serialized_rows,
            "next_before_id": result.next_before_id,
        }
        h._json(payload)
        return
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        # F3: match h._error() shape used by neighbor routes (get_pipeline_detail etc.)
        h._error(result.error_message or "Request not found", 404)
        return
    if result.outcome == RESULT_HISTORY_PAGE_INPUT_INVALID:
        h._error(result.error_message or "Invalid history page request", 400)
        return
    # Defensive fallback for any future outcome string.
    h._error(f"Unknown history outcome: {result.outcome}", 500)


class PipelineSearchPlanRegenerateRequest(BaseModel):
    # ``strict=True`` because Pydantic v2's default lax mode coerces
    # ``"true"``/``"false"`` strings to bool — the regenerate route's
    # contract is JSON-bool only and the test pins that.
    prepend_artist: bool | None = Field(default=None, strict=True)


def post_pipeline_search_plan_regenerate(
    h, body: dict, req_id_str: str,
) -> None:
    """U8: ``POST /api/pipeline/<id>/search-plan/regenerate``.

    Wraps ``SearchPlanService.generate_for_request(regenerate=True)``.
    Allowed for every non-terminal request status; only ``wanted``
    requests are executable, surfaced via ``executable`` in the response
    so operators can't misread "regenerated" as "now downloading".
    Replaced audit ancestors reject regeneration.

    Status-code mapping mirrors the CLI's exit codes:
      * 200 — ``RESULT_SUCCESS`` or ``RESULT_NOOP_ACTIVE_PLAN_EXISTS``
      * 404 — ``RESULT_REQUEST_NOT_FOUND``
      * 409 — ``RESULT_REQUEST_REPLACED``
      * 422 — ``RESULT_FAILED_DETERMINISTIC`` (sticky, body explains)
      * 503 — ``RESULT_FAILED_TRANSIENT`` (retryable)
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
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    req_body = parse_body(h, body or {}, PipelineSearchPlanRegenerateRequest)
    if req_body is None:
        return
    prepend_artist: bool | None = req_body.prepend_artist
    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.generate_for_request(
        request_id, regenerate=True, prepend_artist=prepend_artist,
    )

    payload: dict[str, object] = {
        "request_id": request_id,
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "is_supersede": result.is_supersede,
        "failure_class": result.failure_class,
        "error_message": result.error_message,
    }

    req = db.get_request(request_id)
    if req is not None:
        payload["request_status"] = req.get("status")
        payload["executable"] = (
            req.get("status") == "wanted"
            and result.outcome == RESULT_SUCCESS
        )
    else:
        payload["request_status"] = None
        payload["executable"] = False

    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        # Symmetric body shape with 422 / 503: clients expect to see
        # request_id / outcome / plan_id (None) / failure_class /
        # error_message even on the not-found path.
        payload["error"] = "Not found"
        h._json(payload, status=404)
        return
    if result.outcome == RESULT_REQUEST_REPLACED:
        payload["error"] = result.error_message or "Request is replaced"
        h._json(payload, status=409)
        return
    if result.outcome == RESULT_FAILED_DETERMINISTIC:
        payload["error"] = result.error_message or "Plan generation failed"
        h._json(payload, status=422)
        return
    if result.outcome == RESULT_FAILED_TRANSIENT:
        payload["error"] = result.error_message or "Plan generation retryable"
        h._json(payload, status=503)
        return
    # RESULT_SUCCESS or RESULT_NOOP_ACTIVE_PLAN_EXISTS.
    if result.outcome not in (RESULT_SUCCESS, RESULT_NOOP_ACTIVE_PLAN_EXISTS):
        # Defensive fallback; surface as 500 so we notice unknown shapes.
        h._error(f"Unknown plan generation outcome: {result.outcome}", 500)
        return
    h._json(payload)


class PipelineSearchPlanAdvanceRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/<id>/search-plan/advance``.

    Exactly one of ``to_ordinal`` / ``to_strategy`` is required. The
    ``@model_validator`` enforces the XOR — Pydantic checks types but
    not "exactly one of two".
    """

    to_ordinal: int | None = None
    to_strategy: str | None = None

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "PipelineSearchPlanAdvanceRequest":
        if (self.to_ordinal is None) == (self.to_strategy is None):
            raise ValueError(
                "exactly one of to_ordinal or to_strategy is required"
            )
        return self


def post_pipeline_search_plan_advance(
    h, body: dict, req_id_str: str,
) -> None:
    """``POST /api/pipeline/<id>/search-plan/advance``.

    Forward-only operator advance of the search-plan cursor. Counterpart
    of ``pipeline-cli search-plan advance``. Both surfaces wrap
    ``SearchPlanService.advance_for_request`` — keep them in sync (see
    ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Body: exactly one of
      * ``{"to_ordinal": <int>}`` — absolute target ordinal
      * ``{"to_strategy": <str>}`` — first slot past cursor whose strategy
        starts with this prefix

    Status-code mapping mirrors the CLI exit codes:
      * 200 — ``RESULT_ADVANCED``
      * 400 — body validation failure (missing/extra keys, wrong type)
      * 404 — ``RESULT_REQUEST_NOT_FOUND``
      * 409 — ``RESULT_NO_ACTIVE_PLAN`` (request needs ``regenerate`` first)
        or ``RESULT_REQUEST_REPLACED``
      * 422 — ``RESULT_INVALID_TARGET`` (out of range, backward, no slot
        matches strategy)
      * 503 — ``RESULT_FAILED_TRANSIENT`` (lock contention)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_ADVANCED,
        RESULT_FAILED_TRANSIENT,
        RESULT_INVALID_TARGET,
        RESULT_NO_ACTIVE_PLAN,
        RESULT_REQUEST_NOT_FOUND,
        RESULT_REQUEST_REPLACED,
        SearchPlanService,
    )
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return
    req_body = parse_body(h, body or {}, PipelineSearchPlanAdvanceRequest)
    if req_body is None:
        return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.advance_for_request(
        request_id,
        to_ordinal=req_body.to_ordinal,
        to_strategy=req_body.to_strategy,
    )
    payload: dict[str, object] = {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "previous_ordinal": result.previous_ordinal,
        "new_ordinal": result.new_ordinal,
        "new_strategy": result.new_strategy,
        "new_query": result.new_query,
        "error_message": result.error_message,
    }
    if result.outcome == RESULT_ADVANCED:
        h._json(payload)
        return
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome == RESULT_REQUEST_REPLACED:
        payload["error"] = result.error_message or "Request is replaced"
        h._json(payload, status=409)
        return
    if result.outcome == RESULT_NO_ACTIVE_PLAN:
        payload["error"] = (
            result.error_message or "No active plan; regenerate first")
        h._json(payload, status=409)
        return
    if result.outcome == RESULT_INVALID_TARGET:
        payload["error"] = (
            result.error_message or "Invalid advance target")
        h._json(payload, status=422)
        return
    if result.outcome == RESULT_FAILED_TRANSIENT:
        payload["error"] = (
            result.error_message or "Plan lock contention; retry")
        h._json(payload, status=503)
        return
    # Defensive: any unknown outcome string is a bug.
    h._error(f"Unknown advance outcome: {result.outcome}", 500)


ROUTES: list[RouteRegistration] = [
    pattern_route(
        "GET", r"^/api/pipeline/(\d+)/search-plan$", get_pipeline_search_plan,
        "Read-only view of a request's persisted search plan (cursor, "
        "items, provenance, per-slot stats).",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/pipeline/(\d+)/search-plan/dry-run$",
        get_pipeline_search_plan_dry_run,
        "Generator simulator — runs generate_search_plan against the "
        "current snapshot without writing.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/pipeline/(\d+)/search-plan/saturation$",
        get_pipeline_search_plan_saturation,
        "Saturation rate + pre-filter skip total over a recent search_log "
        "window for this request.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/pipeline/(\d+)/search-plan/history$",
        get_pipeline_search_plan_history,
        "Cursor-paginated read of one request's search_log rows.",
        classified=True,
    ),
    pattern_route(
        "POST", r"^/api/pipeline/(\d+)/search-plan/regenerate$",
        post_pipeline_search_plan_regenerate,
        "Regenerate the search plan for a request.",
        classified=True,
    ),
    pattern_route(
        "POST", r"^/api/pipeline/(\d+)/search-plan/advance$",
        post_pipeline_search_plan_advance,
        "Forward-only operator advance of the search-plan cursor "
        "(by ordinal or strategy prefix).",
        classified=True,
    ),
]
