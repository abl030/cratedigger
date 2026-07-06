"""Pipeline dashboard metrics route.

Split from web/routes/pipeline.py (#522) — mirrors
``web/js/pipeline_dashboard.js`` on the frontend side.
"""

import msgspec

from lib.disk_coverage_service import disk_coverage
from web import cache as cache_api
from web.routes._registry import RouteRegistration, route
from web.routes._server_access import _server


def get_pipeline_dashboard(h, params: dict[str, list[str]]) -> None:
    """Return operational metrics for the Pipeline dashboard subtab."""
    s = _server()
    data = s._db().get_pipeline_dashboard_metrics()
    data["redis"] = cache_api.redis_metrics()
    data["disk_coverage"] = _dashboard_disk_coverage(s)
    h._json(data)


def _dashboard_disk_coverage(s) -> dict[str, object] | None:
    """Pipeline-vs-beets coverage block for the dashboard, or None when
    no beets DB is configured.

    Only ``imported`` claims beets presence, so ``drift_rows`` carries
    off-disk ``imported`` rows only (a release that vanished from beets
    is the Lucksmiths-class out-of-band drift signal). Off-disk wanted
    (not yet acquired), downloading (in flight), and manual (staged for
    review) rows are lifecycle-normal, not drift."""
    beets = s._beets_db()
    if beets is None:
        return None
    result = disk_coverage(s._db(), beets, include_rows=True)
    return {
        "counts": msgspec.to_builtins(result.counts),
        "drift_rows": [
            msgspec.to_builtins(row)
            for row in (result.off_disk or [])
            if row.status == "imported"
        ],
    }


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/pipeline/dashboard", get_pipeline_dashboard,
        "Operational metrics for the dashboard subtab (searches, "
        "cycles, redis).",
        classified=True,
    ),
]
