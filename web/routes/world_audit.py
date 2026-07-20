"""Read-only cross-engine world audit API."""

from __future__ import annotations

import msgspec

from lib.world_audit_service import audit_world
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server


def get_world_audit(h: RouteHandler, params: dict[str, list[str]]) -> None:
    del params
    server = _server()
    beets = server._beets_db()
    if beets is None:
        h._error("Beets DB not configured", 503)
        return
    h._json(msgspec.to_builtins(audit_world(server._db(), beets)))


ROUTES: list[RouteRegistration] = [
    route(
        "GET",
        "/api/audit/world",
        get_world_audit,
        "Read-only cross-engine invariant audit of PipelineDB, Beets, and disk.",
        classified=True,
    ),
]
