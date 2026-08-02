"""Read-only cross-engine world audit API."""

from __future__ import annotations

import logging

import msgspec

from lib.world_audit_service import audit_world_from_borrowed_factory
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server

log = logging.getLogger(__name__)


def get_world_audit(h: RouteHandler, params: dict[str, list[str]]) -> None:
    del params
    server = _server()
    try:
        def beets_factory():
            beets = server._beets_db()
            if beets is None:
                raise FileNotFoundError("Beets DB not configured")
            return beets

        report = audit_world_from_borrowed_factory(server._db(), beets_factory)
        payload = msgspec.to_builtins(report)
    except Exception:
        log.exception("world audit failed unexpectedly")
        h._json({"error": "World audit failed"}, status=503)
        return
    h._json(payload)


ROUTES: list[RouteRegistration] = [
    route(
        "GET",
        "/api/audit/world",
        get_world_audit,
        "Grouped read-only A/B/C ownership audit with completeness and "
        "Bucket-A integrity status.",
        classified=True,
    ),
]
