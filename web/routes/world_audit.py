"""Read-only cross-engine world audit API."""

from __future__ import annotations

import logging

import msgspec

from lib.world_audit_service import (
    WORLD_AUDIT_HTTP_STATUS,
    audit_world_from_borrowed_factory,
    world_audit_outcome,
)
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.runtime import runtime

log = logging.getLogger(__name__)


def get_world_audit(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Status-code mapping (issue #1355 item 4), derived via
    ``lib.world_audit_service.world_audit_outcome``: ``clean``/
    ``observations_only`` -> 200 (a COMPLETE report, whatever it observed);
    ``integrity_failed`` -> 200 too (a genuine Bucket A finding stays a
    successful answer — the payload's own ``status`` carries it, mirroring
    the sibling retag-divergence audit's ``divergence_found`` -> 200);
    ``beets_unavailable`` (``complete == False``) -> 503 (transient/
    retryable — the audit never actually ran). This used to return 200 for
    ``beets_unavailable`` too — a pre-existing deviation from
    ``GET /api/audit/retag-divergence``'s own convention, closed here.
    """
    del params
    rt = runtime()
    try:
        def beets_factory():
            beets = rt.beets_db()
            if beets is None:
                raise FileNotFoundError("Beets DB not configured")
            return beets

        report = audit_world_from_borrowed_factory(rt.db(), beets_factory)
        payload = msgspec.to_builtins(report)
        # Decided inside the try, alongside the render: a defect in
        # `world_audit_outcome` or the status-code lookup must land on
        # the documented 503, never escape uncaught past this handler's
        # own error response (independent reader finding, issue #1355
        # item 4).
        outcome = world_audit_outcome(report)
        status_code = (
            200 if outcome == "integrity_failed" else WORLD_AUDIT_HTTP_STATUS[outcome]
        )
    except Exception:
        log.exception("world audit failed unexpectedly")
        h._json({"error": "World audit failed"}, status=503)
        return
    h._json(payload, status=status_code)


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
