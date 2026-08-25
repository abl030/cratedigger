"""Pipeline dashboard metrics route.

Split from web/routes/pipeline.py (#522) — mirrors
``web/js/pipeline_dashboard.js`` on the frontend side.
"""

import json
import logging
import os

import msgspec

from lib.disk_coverage_service import disk_coverage
from lib.library_completeness_snapshot import (
    read_library_completeness_snapshot,
    request_library_completeness_census,
)
from lib.retag_divergence_census_snapshot import (
    read_retag_divergence_census_snapshot,
)
from web import cache as cache_api
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server

log = logging.getLogger(__name__)

#: Maximum non-agreeing albums the DASHBOARD route ever embeds in one
#: response (#1142 fresh review N1). The PERSISTED snapshot on disk is
#: never touched or truncated — a read-failure-heavy world could
#: legitimately list every one of the ~8,700 real albums — but this
#: route's own JSON projection is a dashboard card, not a bulk export,
#: and must not serialize an unbounded response. ``albums_shown``/
#: ``albums_listed_total`` in the payload make the cap visible to the
#: caller rather than a silent truncation.
DASHBOARD_RETAG_CENSUS_ALBUM_CAP = 50
DASHBOARD_LIBRARY_COMPLETENESS_ALBUM_CAP = 50


def get_pipeline_dashboard(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Return operational metrics for the Pipeline dashboard subtab."""
    s = _server()
    data = s._db().get_pipeline_dashboard_metrics()
    data["redis"] = cache_api.redis_metrics()
    data["disk_coverage"] = _dashboard_disk_coverage()
    data["retag_divergence_census"] = _dashboard_retag_divergence_census()
    data["library_completeness"] = _dashboard_library_completeness()
    h._json(data)


def _dashboard_disk_coverage() -> dict[str, object] | None:
    """Pipeline-vs-beets coverage block for the dashboard, or None when
    no beets DB is configured.

    Only ``imported`` claims unique Beets presence, so ``drift_rows`` carries
    imported rows that are missing or ambiguous only (a release that vanished
    from Beets is the Lucksmiths-class out-of-band drift signal). Wanted,
    downloading, and unsearchable rows that are not uniquely present are
    lifecycle-normal, not drift."""
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
        inside — this route makes no claim about it. ``report.counts``
        is always the true, uncapped whole-library numbers — ONLY
        ``report.albums`` (the per-album listing) is capped at
        :data:`DASHBOARD_RETAG_CENSUS_ALBUM_CAP`, never silently: the
        sibling ``albums_shown``/``albums_listed_total`` fields name
        exactly how many were embedded versus how many the persisted
        report actually lists (#1142 fresh review N1). The persisted
        file on disk is never rewritten by this cap.
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

    ``albums_shown``/``albums_listed_total`` are always present (``0``
    for ``missing``/``unreadable``) so the payload shape never branches
    on state.
    """
    s = _server()
    path = s.retag_census_snapshot_path
    if path is None:
        return _empty_retag_divergence_census("missing")
    try:
        snapshot = read_retag_divergence_census_snapshot(path)
    except (
        OSError, UnicodeDecodeError, json.JSONDecodeError,
        msgspec.ValidationError,
    ) as exc:
        log.exception(
            "retag divergence census snapshot at %s is unreadable", path,
        )
        result = _empty_retag_divergence_census("unreadable")
        result["error"] = str(exc)
        return result
    if snapshot is None:
        return _empty_retag_divergence_census("missing")
    snapshot_dict = msgspec.to_builtins(snapshot)
    report_dict = snapshot_dict["report"]
    all_albums = report_dict["albums"]
    albums_listed_total = len(all_albums)
    report_dict["albums"] = all_albums[:DASHBOARD_RETAG_CENSUS_ALBUM_CAP]
    return {
        "state": "ok",
        "error": None,
        "snapshot": snapshot_dict,
        "albums_shown": len(report_dict["albums"]),
        "albums_listed_total": albums_listed_total,
    }


def _empty_retag_divergence_census(state: str) -> dict[str, object]:
    return {
        "state": state, "error": None, "snapshot": None,
        "albums_shown": 0, "albums_listed_total": 0,
    }


def _dashboard_library_completeness() -> dict[str, object]:
    """Read the daily source/catalog/filesystem census without rescanning."""
    s = _server()
    path = s.library_completeness_snapshot_path
    if path is None:
        return _empty_library_completeness("missing")
    try:
        snapshot = read_library_completeness_snapshot(path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, msgspec.ValidationError) as exc:
        log.exception("library completeness snapshot at %s is unreadable", path)
        result = _empty_library_completeness("unreadable")
        result["error"] = str(exc)
        return result
    if snapshot is None:
        return _empty_library_completeness("missing")
    snapshot_dict = msgspec.to_builtins(snapshot)
    report = snapshot_dict["report"]
    albums = report["albums"]
    total = len(albums)
    report["albums"] = albums[:DASHBOARD_LIBRARY_COMPLETENESS_ALBUM_CAP]
    # Issue #1241: enrich each embedded album with its pipeline request and
    # the operator's incomplete-mark state so the card can offer the
    # mark/clear action inline. Joined only for the capped embed, never for
    # the persisted snapshot. A census album with no resolvable request
    # (release_id absent from the pipeline) carries request_id=None and the
    # card renders no action for it. Per-album isolation (#1257 review F9):
    # the presentation projection refuses inconsistent mid-transition rows
    # (e.g. a processing row missing its owner join), and one refused row
    # must degrade that ONE album to actionless, never 500 the dashboard.
    for album in report["albums"]:
        try:
            req = s._db().get_request_by_release_id(album.get("release_id"))
        except Exception:
            log.exception(
                "library completeness enrichment failed for release %r",
                album.get("release_id"),
            )
            req = None
        album["request_id"] = req["id"] if req else None
        album["marked_incomplete"] = bool(
            req and req.get("marked_incomplete_at")
        )
    return {
        "state": "ok", "error": None, "snapshot": snapshot_dict,
        "albums_shown": len(report["albums"]), "albums_listed_total": total,
    }


def _empty_library_completeness(state: str) -> dict[str, object]:
    return {"state": state, "error": None, "snapshot": None,
            "albums_shown": 0, "albums_listed_total": 0}


def post_library_census_refresh(
    h: RouteHandler, _body: dict[str, object],
) -> None:
    """Request an out-of-schedule library-completeness census run.

    Writes the trigger file the module's path unit watches; the census
    oneshot itself remains the single execution path. The state dir is
    derived from the configured snapshot path — both live in the
    module's ``stateDir``.
    """
    s = _server()
    snapshot_path = s.library_completeness_snapshot_path
    if snapshot_path is None:
        h._json({
            "outcome": "unconfigured",
            "error": "library completeness snapshot path is not configured",
        }, status=503)
        return
    try:
        result = request_library_completeness_census(
            os.path.dirname(snapshot_path),
        )
    except OSError as exc:
        log.exception("census trigger write failed")
        h._json({"outcome": "unavailable", "error": str(exc)}, status=503)
        return
    h._json({"outcome": result.outcome, "error": None})


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/pipeline/dashboard", get_pipeline_dashboard,
        "Operational metrics for the dashboard subtab (searches, cycles, "
        "redis, read-only library census snapshots).",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/dashboard/library-census/refresh",
        post_library_census_refresh,
        "Request an out-of-schedule library-completeness census run "
        "(writes the trigger file the census path unit watches; the "
        "daily oneshot stays the single execution path).",
        classified=True,
    ),
]
