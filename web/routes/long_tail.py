"""Long-tail worklist route — the banded ``wanted`` cohort read that backs
the Long-Tail Triage Console.

Split from web/routes/pipeline.py (#481 item 3). Wraps
``lib.long_tail_service`` — same service ``pipeline-cli long-tail`` wraps
(CLI ⇄ API symmetry, R16).
"""

import msgspec

from web.routes._registry import RouteRegistration, route


def _server():
    """Deferred import to avoid circular deps."""
    from web import server
    return server


def get_pipeline_long_tail(h, params: dict[str, list[str]]) -> None:
    """U1: ``GET /api/pipeline/long-tail`` — banded ``wanted`` worklist.

    Returns every ``wanted`` request pre-banded by on-disk quality
    (``missing`` / a lowercase ``QualityRank`` band / ``unknown``) and
    stamped with ``in_flight_rescue``. Wraps
    ``lib.long_tail_service.list_long_tail`` — the SAME service method
    ``pipeline-cli long-tail`` wraps (CLI ⇄ API symmetry, R16).

    Query string:
      * ``band`` — optional single-band filter (``missing`` / a rank /
        ``unknown``). The UI fetches unfiltered and filters client-side;
        this backs scripted / CLI-parity callers.
      * ``id`` — optional single request id. Returns just that request's
        authoritative band + flags (KTD8 — the post-action single-row
        refetch). 404 when the id doesn't exist OR is no longer
        ``wanted`` (an imported / replaced row is correctly absent from
        the worklist). Mutually exclusive with ``band`` (``band`` is
        ignored when ``id`` is present — a single row is already its own
        cohort).

    Response shape (cohort success):
        ``{"results": [...], "band": <str|null>, "count": <int>}``
    Response shape (single-id success):
        ``{"result": <row>, "id": <int>}``

    Each result is a ``LongTailRow`` serialized via
    ``msgspec.to_builtins`` so ``msgspec.convert(row, type=LongTailRow)``
    round-trips on the consumer side. Datetime / UUID columns are pinned
    out of the row by ``_LONG_TAIL_SELECT``'s projection, but the row is
    still routed through ``_serialize_row`` to guard the datetime-500
    class against any future column add.

    Status-code mapping:
      * 200 — success (empty cohort is a valid state).
      * 400 — non-int ``id``.
      * 404 — ``id`` not found / not ``wanted``.
    """
    from lib.long_tail_service import band_one_long_tail, list_long_tail
    from web.routes._overlay import band_release_ids

    s = _server()

    id_raw = params.get("id", [None])[0]
    if id_raw is not None and id_raw != "":
        try:
            request_id = int(id_raw)
        except (TypeError, ValueError):
            h._error("id must be an integer")
            return
        row = band_one_long_tail(s._db(), band_release_ids, request_id)
        if row is None:
            h._json(
                {"error": "Not found", "id": request_id},
                status=404,
            )
            return
        serialized = s._serialize_row(msgspec.to_builtins(row))
        h._json({"result": serialized, "id": request_id})
        return

    band = params.get("band", [None])[0]
    if band == "":
        band = None

    result = list_long_tail(s._db(), band_release_ids, band=band)

    # Route the serialized rows through _serialize_row to convert any
    # datetime / UUID columns to JSON-safe values (datetime-500 guard).
    rows = [s._serialize_row(r) for r in msgspec.to_builtins(result.rows)]
    h._json({
        "results": rows,
        "band": result.band_filter,
        "count": len(rows),
    })


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/pipeline/long-tail", get_pipeline_long_tail,
        # U1: Long-Tail Triage Console worklist read. Wraps
        # ``lib.long_tail_service.list_long_tail`` — same service as
        # ``pipeline-cli long-tail`` per CLI ⇄ API symmetry.
        "Long-tail worklist — the full wanted cohort pre-banded by "
        "on-disk quality (missing / QualityRank band / unknown) and "
        "stamped with in_flight_rescue. Optional ?band= filter.",
        classified=True,
    ),
]
