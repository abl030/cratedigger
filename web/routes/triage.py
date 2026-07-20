"""Triage routes — request/cohort composition plus quarantine lifecycle.

Request and cohort routes wrap ``lib.triage_service`` (unfindable
categorisation, field-resolution telemetry, search-log forensics). The
read-only quarantine route wraps ``lib.quarantine_triage_service``.

Split from web/routes/pipeline.py (#481 item 3). Not to be confused with
the unrelated "wrong-match triage" console in web/routes/imports.py
(``/api/wrong-matches/triage*``) — this module is the U15/U17
``lib.triage_service`` surface (``/api/triage/*``).
"""

import msgspec

from web.routes._registry import RouteHandler, RouteRegistration, pattern_route, route
from web.routes._server_access import _server


# --- U17: /api/triage HTTP endpoints --------------------------------------
#
# Two HTTP routes wrap the U15 triage service (``lib.triage_service``):
#
#   * ``GET /api/triage/<id>`` — per-request composition. Mirrors
#     ``pipeline-cli triage show <id>`` (U16). Outcome → status:
#       - 200: ``TriageResult`` payload (msgspec.to_builtins).
#       - 400: non-int request id (h._error default).
#       - 404: request_id has no album_requests row.
#
#   * ``GET /api/triage/list`` — cohort listing. Mirrors
#     ``pipeline-cli triage list --filter=<spec>`` (U16). Outcome →
#     status:
#       - 200: ``{results, next_after, page_size, filter}`` payload.
#       - 400: ``InvalidFilterError`` or non-int ``limit``/``after``.
#
# Both surfaces route through the same service entrypoints
# (``compose_triage_for_request`` / ``list_triage``) so the CLI ⇄ API
# symmetry rule holds — see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry".
# ``GET /api/triage/quarantine`` separately mirrors
# ``pipeline-cli triage quarantine`` through one shared read-only lifecycle
# service; both map an unavailable complete scan to 503 / exit 5.

# Filter forms surfaced in the 400 body — single source of truth lives
# in ``lib.triage_service.VALID_FILTER_FORMS`` so the CLI and the HTTP
# 400 envelope advertise the same vocabulary.
from lib.triage_service import VALID_FILTER_FORMS as _TRIAGE_VALID_FILTER_FORMS_API  # noqa: E402

# Page-size bounds for ``GET /api/triage/list`` — re-exports of the
# single-source-of-truth constants on ``lib.triage_service`` so the CLI
# and API enforce the same ranges. Mirrors the convention established by
# ``get_pipeline_search_plan_history`` (1..200): a hard upper bound
# prevents an unbounded scan; the lower bound rules out the nonsense
# ``limit=0`` request shape.
from lib.triage_service import (  # noqa: E402
    DEFAULT_TRIAGE_PAGE_SIZE as _TRIAGE_LIST_DEFAULT_LIMIT,
    TRIAGE_AFTER_MIN as _TRIAGE_LIST_MIN_AFTER,
    TRIAGE_LIMIT_MAX as _TRIAGE_LIST_MAX_LIMIT,
    TRIAGE_LIMIT_MIN as _TRIAGE_LIST_MIN_LIMIT,
)


def get_triage_quarantine(
    h: RouteHandler, params: dict[str, list[str]],
) -> None:
    """Return unreferenced immediate ``failed_imports`` album folders.

    Mirrors ``pipeline-cli triage quarantine``. A complete scan returns 200;
    any configuration, DB, decode, or filesystem uncertainty returns 503
    rather than a misleading partial/empty list.
    """
    from lib.quarantine_triage_service import (
        QuarantineScanError,
        list_unreferenced_quarantine_folders,
    )

    try:
        db = _server()._db()
    except Exception:
        h._json(
            {"error": "Could not open pipeline database for quarantine scan"},
            status=503,
        )
        return
    try:
        result = list_unreferenced_quarantine_folders(db)
    except QuarantineScanError as exc:
        h._json({"error": str(exc)}, status=503)
        return
    h._json(msgspec.to_builtins(result))


def get_triage_for_request(
    h: RouteHandler, params: dict[str, list[str]], req_id_str: str,
) -> None:
    """U17: ``GET /api/triage/<id>``.

    Compose the per-request triage payload via
    ``lib.triage_service.compose_triage_for_request``. The response
    body is ``msgspec.to_builtins(TriageResult)`` — the JSON shape on
    the wire IS the Struct shape verbatim, which is what makes
    ``msgspec.convert(payload, type=TriageResult)`` round-trip on the
    consumer side (frontend or CLI parity tests).

    Status-code mapping (mirrors ``cmd_triage_show``'s exit codes):
      * 200 — composition success.
      * 404 — ``compose_triage_for_request`` returned ``None`` (no row).

    The route's regex (``r"^/api/triage/(\\d+)$"``) requires a digit-only
    path segment, so ``req_id_str`` is always coercible — non-digit
    paths never match this pattern in the first place and fall through
    to the catch-all 404 in ``web/server.py``.
    """
    from lib.triage_service import compose_triage_for_request

    request_id = int(req_id_str)
    db = _server()._db()
    result = compose_triage_for_request(request_id, db)
    if result is None:
        h._json(
            {"error": "Not found", "request_id": request_id},
            status=404,
        )
        return

    payload = msgspec.to_builtins(result)
    h._json(payload)


def get_triage_list(
    h: RouteHandler, params: dict[str, list[str]],
) -> None:
    """U17: ``GET /api/triage/list``.

    Cohort-filtered triage listing. Query string:
      * ``filter`` — filter spec (default ``"all"``). Forms documented
        in ``_TRIAGE_VALID_FILTER_FORMS_API`` and ``lib.triage_service
        .parse_filter``.
      * ``limit`` — int in ``[_TRIAGE_LIST_MIN_LIMIT,
        _TRIAGE_LIST_MAX_LIMIT]``; defaults to
        ``_TRIAGE_LIST_DEFAULT_LIMIT``.
      * ``after`` — int >= 1; the ``next_after`` cursor from the
        previous page. Omit for the first page.

    Response shape (success):
        ``{"results": [...], "next_after": <int|null>,
           "page_size": <int>, "filter": <spec str>}``

    ``next_after`` is ``None`` when ``len(results) < page_size`` (the
    page exhausts the cohort); otherwise the last request id so
    operators can keep paging.

    Status-code mapping (mirrors ``cmd_triage_list``'s exit codes):
      * 200 — success (empty results list is a valid cohort state).
      * 400 — ``InvalidFilterError`` (parser rejects the spec) OR
              non-int ``limit`` / ``after`` / out-of-range ``limit``.
    """
    from lib.triage_service import InvalidFilterError, list_triage

    filter_spec = params.get("filter", ["all"])[0]
    if filter_spec == "":
        filter_spec = "all"

    limit_raw = params.get("limit", [None])[0]
    if limit_raw is None or limit_raw == "":
        limit = _TRIAGE_LIST_DEFAULT_LIMIT
    else:
        try:
            limit = int(limit_raw)
        except (TypeError, ValueError):
            h._error("limit must be an integer")
            return
    if not (_TRIAGE_LIST_MIN_LIMIT <= limit <= _TRIAGE_LIST_MAX_LIMIT):
        h._error(
            f"limit must be in [{_TRIAGE_LIST_MIN_LIMIT}, "
            f"{_TRIAGE_LIST_MAX_LIMIT}]",
            status=400,
        )
        return

    after_raw = params.get("after", [None])[0]
    after: int | None
    if after_raw is None or after_raw == "":
        after = None
    else:
        try:
            after = int(after_raw)
        except (TypeError, ValueError):
            h._error("after must be an integer")
            return
        if after < _TRIAGE_LIST_MIN_AFTER:
            h._error(
                f"after must be >= {_TRIAGE_LIST_MIN_AFTER}", status=400,
            )
            return

    db = _server()._db()
    try:
        results = list_triage(
            filter_spec, db, page_size=limit, after_request_id=after,
        )
    except InvalidFilterError as exc:
        # Pull the parameter-vocab arrays so API-only operators can
        # self-correct from the response body alone (e.g. on
        # ``unfindable:<bad_cat>``, the operator sees the four valid
        # categories without needing to consult --help).
        from lib.triage_service import (
            VALID_DATA_QUALITY_FIELD_NAMES,
            VALID_UNFINDABLE_CATEGORIES,
        )
        h._json(
            {
                "error": str(exc),
                "valid_filters": list(_TRIAGE_VALID_FILTER_FORMS_API),
                "valid_unfindable_categories": sorted(
                    VALID_UNFINDABLE_CATEGORIES
                ),
                "valid_data_quality_fields": sorted(
                    VALID_DATA_QUALITY_FIELD_NAMES
                ),
            },
            status=400,
        )
        return

    next_after: int | None = None
    if len(results) >= limit and results:
        next_after = results[-1].request_meta.id

    payload: dict[str, object] = {
        "results": msgspec.to_builtins(results),
        "next_after": next_after,
        "page_size": limit,
        "filter": filter_spec,
    }
    h._json(payload)


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/triage/quarantine", get_triage_quarantine,
        "Read-only quarantine lifecycle view — unreferenced immediate "
        "failed_imports album folders with no visible Wrong Matches reference; excludes "
        "code-owned bad_files and untracked_audio buckets.",
        classified=True,
    ),
    route(
        "GET", "/api/triage/list", get_triage_list,
        # U17: /api/triage HTTP endpoints. Per-request composition and
        # cohort listing both wrap ``lib.triage_service`` (U15) — same
        # service as ``pipeline-cli triage`` (U16) per CLI ⇄ API symmetry.
        "Cohort triage listing — filter by unfindable category, "
        "field-quality field/status/reason, or search-not-converting "
        "state. ``data_quality:status=<status>`` filters on the "
        "resolver-status column (e.g. unresolved_4xx_client); "
        "``data_quality:reason=<code>`` filters on the reason_code "
        "column (e.g. http_400).",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/triage/(\d+)$", get_triage_for_request,
        "Per-request triage composition — unfindable categorisation, "
        "field-resolution telemetry, search-log forensics.",
        classified=True,
    ),
]
