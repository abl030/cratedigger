"""Read-only retag ``-W`` divergence cohort audit API (#1093 item 1)."""

from __future__ import annotations

import logging

import msgspec

from lib.retag_divergence_audit import scan_retag_divergence_from_borrowed_factory
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server

log = logging.getLogger(__name__)

#: Wall-clock bound for one HTTP request's scan (#1093 review finding 2).
#: A measured unbounded full census over the live ~93k-item library took
#: ~196s; the deployed vhost's reverse proxy has no configured
#: ``proxy_read_timeout``, so it falls back to nginx's 60s default and the
#: backend would keep scanning past a 504 the client already gave up on.
#: This value leaves real margin under that default. A bounded scan reports
#: ``complete=False`` — the report SHAPE never changes; run
#: ``pipeline-cli audit retag-divergence`` (no deadline) for the full,
#: unbounded census.
API_SCAN_DEADLINE_SECONDS = 40.0


def get_retag_divergence_audit(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """``beets_unavailable`` returns HTTP 503 — the audit never actually
    ran, so 200 would let a caller read "no divergence" from a report that
    answered nothing (#1093 review round 3, finding 1; the transient/
    retryable class this maps from is exactly the 503/5 convention in
    `.claude/rules/code-quality.md` § CLI ⇄ API Surface Symmetry). NOTE:
    `GET /api/audit/world` still returns 200 for its own analogous
    beets-unavailable bucket — a pre-existing deviation from that same
    convention, deliberately left alone here (see the PR body / post-ship
    reflection).
    """
    del params
    server = _server()
    try:
        def beets_factory():
            beets = server._beets_db()
            if beets is None:
                raise FileNotFoundError("Beets DB not configured")
            return beets

        report = scan_retag_divergence_from_borrowed_factory(
            beets_factory, deadline_seconds=API_SCAN_DEADLINE_SECONDS,
        )
        payload = msgspec.to_builtins(report)
    except Exception:
        log.exception("retag divergence audit failed unexpectedly")
        h._json({"error": "Retag divergence audit failed"}, status=503)
        return
    status_code = 503 if report.status == "beets_unavailable" else 200
    h._json(payload, status=status_code)


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
