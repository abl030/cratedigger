"""Read-only retag ``-W`` divergence cohort audit API (#1093 item 1)."""

from __future__ import annotations

import logging

import msgspec

from lib.retag_divergence_audit import scan_retag_divergence_from_borrowed_factory
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server

log = logging.getLogger(__name__)


def get_retag_divergence_audit(h: RouteHandler, params: dict[str, list[str]]) -> None:
    del params
    server = _server()
    try:
        def beets_factory():
            beets = server._beets_db()
            if beets is None:
                raise FileNotFoundError("Beets DB not configured")
            return beets

        report = scan_retag_divergence_from_borrowed_factory(beets_factory)
        payload = msgspec.to_builtins(report)
    except Exception:
        log.exception("retag divergence audit failed unexpectedly")
        h._json({"error": "Retag divergence audit failed"}, status=503)
        return
    h._json(payload)


ROUTES: list[RouteRegistration] = [
    route(
        "GET",
        "/api/audit/retag-divergence",
        get_retag_divergence_audit,
        "Read-only census of albums whose Beets DB identity moved (the "
        "retag) but whose installed file tags did not.",
        classified=True,
    ),
]
