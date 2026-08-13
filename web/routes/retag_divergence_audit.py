"""Read-only retag ``-W`` divergence cohort audit API (#1093 item 1)."""

from __future__ import annotations

import logging

import msgspec

from lib.retag_divergence_audit import scan_retag_divergence_from_borrowed_factory
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server

log = logging.getLogger(__name__)

#: nginx's documented default when a vhost sets no explicit
#: ``proxy_read_timeout`` (http://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_read_timeout).
#: The deployed ``music.ablz.au`` vhost sets none, so this is the real
#: ceiling one HTTP request has (#1093 review round 4, finding 3).
NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS = 60.0

#: Wall-clock bound for this route's per-album read LOOP only — NOT the
#: whole request (#1093 review round 4, finding 3). ``beets.
#: list_album_mb_identities()`` (~3.2s measured live) and the JSON encode
#: below both run UNBOUNDED, outside this timer, so the real request time
#: is this deadline PLUS that unbounded overhead (measured live: a 40.0s
#: deadline produced a ~41.9-43.2s total route time). A measured fully
#: UNBOUNDED census over the live ~93k-item library took ~196s, which is
#: why this route bounds the loop at all. A bounded scan reports
#: ``complete=False`` — the report SHAPE never changes; run
#: ``pipeline-cli audit retag-divergence`` (no deadline) for the full
#: census, or chain calls with ``after_album_id``/``next_after_album_id``
#: to complete one across multiple bounded requests.
#: `tests/web/test_routes_retag_divergence_audit.py` pins that this value
#: leaves real margin under ``NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS`` —
#: not merely "less than", which a check asserting only `> 0` would have
#: let a value as large as 10 hours pass.
API_SCAN_DEADLINE_SECONDS = 40.0

# Enforced here, not only in the test pin: a value that merely satisfied
# `> 0` (e.g. accidentally repinned to hours) must fail at import time, not
# only in `TestApiScanDeadlineConstant` (#1093 review round 4, finding 3).
assert API_SCAN_DEADLINE_SECONDS < NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS, (
    "API_SCAN_DEADLINE_SECONDS must leave real margin under the reverse "
    "proxy's read timeout — see the constant's own docstring above"
)


def _parse_after_album_id(
    params: dict[str, list[str]],
) -> tuple[int | None, bool]:
    """Parse the optional resume-cursor query param.

    Returns ``(value, ok)``; ``ok=False`` means the caller supplied a
    non-integer value and the route should refuse the request rather than
    silently ignore it.
    """
    raw = params.get("after_album_id")
    if not raw or not raw[0]:
        return None, True
    try:
        return int(raw[0]), True
    except ValueError:
        return None, False


def get_retag_divergence_audit(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """``?after_album_id=N`` resumes a previous truncated scan — pass the
    prior response's ``next_after_album_id`` to continue the census where
    it stopped, chaining calls until ``next_after_album_id`` comes back
    ``null`` (#1093 review round 4, finding 4).

    Status-code mapping: ``clean``/``divergence_found`` → 200 (the audit
    ran and answered the question, whatever the answer); ``incomplete`` →
    409 (wrong state — the world blocked a complete answer, so a caller
    must not read this as "no divergence"); ``beets_unavailable`` → 503
    (transient/retryable — the audit never actually ran at all). Both non-
    200 mappings follow `.claude/rules/code-quality.md` § CLI ⇄ API
    Surface Symmetry's convention table (`409` wrong state, `503`
    transient/retryable) — #1093 review round 4, finding 5. NOTE:
    `GET /api/audit/world` still returns 200 for its own analogous
    beets-unavailable bucket — a pre-existing deviation from that same
    convention, deliberately left alone here (see the PR body / post-ship
    reflection).
    """
    after_album_id, ok = _parse_after_album_id(params)
    if not ok:
        h._error("after_album_id must be an integer")
        return
    server = _server()
    try:
        def beets_factory():
            beets = server._beets_db()
            if beets is None:
                raise FileNotFoundError("Beets DB not configured")
            return beets

        report = scan_retag_divergence_from_borrowed_factory(
            beets_factory,
            deadline_seconds=API_SCAN_DEADLINE_SECONDS,
            after_album_id=after_album_id,
        )
        payload = msgspec.to_builtins(report)
    except Exception:
        log.exception("retag divergence audit failed unexpectedly")
        h._json({"error": "Retag divergence audit failed"}, status=503)
        return
    status_code = {
        "clean": 200,
        "divergence_found": 200,
        "incomplete": 409,
        "beets_unavailable": 503,
    }[report.status]
    h._json(payload, status=status_code)


ROUTES: list[RouteRegistration] = [
    route(
        "GET",
        "/api/audit/retag-divergence",
        get_retag_divergence_audit,
        "Read-only census of albums whose Beets DB identity moved (the "
        "retag) but whose installed file tags did not; accepts "
        "?after_album_id=N to resume a truncated scan.",
        classified=True,
    ),
]
