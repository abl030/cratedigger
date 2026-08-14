"""Pipeline dashboard metrics route.

Split from web/routes/pipeline.py (#522) — mirrors
``web/js/pipeline_dashboard.js`` on the frontend side.
"""

import json
import logging

import msgspec

from lib.disk_coverage_service import disk_coverage
from lib.retag_divergence_census_snapshot import (
    read_retag_divergence_census_snapshot,
)
from web import cache as cache_api
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server

log = logging.getLogger(__name__)


def get_pipeline_dashboard(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Return operational metrics for the Pipeline dashboard subtab."""
    s = _server()
    data = s._db().get_pipeline_dashboard_metrics()
    data["redis"] = cache_api.redis_metrics()
    data["disk_coverage"] = _dashboard_disk_coverage()
    data["retag_divergence_census"] = _dashboard_retag_divergence_census()
    h._json(data)


def _dashboard_disk_coverage() -> dict[str, object] | None:
    """Pipeline-vs-beets coverage block for the dashboard, or None when
    no beets DB is configured.

    Only ``imported`` claims beets presence, so ``drift_rows`` carries
    off-disk ``imported`` rows only (a release that vanished from beets
    is the Lucksmiths-class out-of-band drift signal). Off-disk wanted
    (not yet acquired), downloading (in flight), and unsearchable
    (operator search stop) rows are lifecycle-normal, not drift."""
    s = _server()
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


def _dashboard_retag_divergence_census() -> dict[str, object]:
    """The persisted daily whole-library retag-divergence census snapshot
    (#1142) — a distinct Beets-DB-vs-file-tags drift card, NOT the Disk
    Coverage (pipeline-ledger-vs-Beets-DB) card above. Read-only: this
    reads the file the daily oneshot published; it never scans, and
    never touches Beets at all.

    ``state``:
      * ``"missing"`` — no snapshot path configured, or the daily census
        has never published one yet (a fresh deploy, or every run so far
        crashed before completing). Real, honest state — not an error.
      * ``"ok"`` — a snapshot was read; ``snapshot`` carries the full
        ``RetagDivergenceCensusSnapshot`` (``generated_at``,
        ``duration_seconds``, ``report``). The report's OWN ``status``
        (clean/divergence_found/incomplete/beets_unavailable) is nested
        inside — this route makes no claim about it.
      * ``"unreadable"`` — a snapshot file exists but could not be read
        or decoded: a filesystem-level failure (``OSError`` — denied
        permissions, the path resolving to a directory, …) or malformed
        content (bit-rotted or hand-edited outside the atomic publish
        path — ``UnicodeDecodeError``/``json.JSONDecodeError``/
        ``msgspec.ValidationError``, matching
        ``read_retag_divergence_census_snapshot``'s own documented
        exception contract). Logged; never crashes the whole dashboard
        route over one card (#1142 review N4) — ``FileNotFoundError`` is
        NOT in this set: that one is handled inside the read helper and
        surfaces as the ordinary ``"missing"`` state below, not an
        error.
    """
    s = _server()
    path = s.retag_census_snapshot_path
    if path is None:
        return {"state": "missing", "error": None, "snapshot": None}
    try:
        snapshot = read_retag_divergence_census_snapshot(path)
    except (
        OSError, UnicodeDecodeError, json.JSONDecodeError,
        msgspec.ValidationError,
    ) as exc:
        log.exception(
            "retag divergence census snapshot at %s is unreadable", path,
        )
        return {"state": "unreadable", "error": str(exc), "snapshot": None}
    if snapshot is None:
        return {"state": "missing", "error": None, "snapshot": None}
    return {
        "state": "ok", "error": None, "snapshot": msgspec.to_builtins(snapshot),
    }


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/pipeline/dashboard", get_pipeline_dashboard,
        "Operational metrics for the dashboard subtab (searches, "
        "cycles, redis).",
        classified=True,
    ),
]
