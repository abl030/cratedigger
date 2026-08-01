"""Pipeline API route handlers, extracted from server.py.

Core pipeline read routes (log/status/recent/all/search/downloading/detail/
requests-by-rg/active-rgs) and import-job listing. Search-plan, triage,
long-tail, and the /api/_index self-documentation machinery were split out
(#481 item 3) into sibling modules: ``web/routes/search_plan.py``,
``web/routes/triage.py``, ``web/routes/long_tail.py``,
``web/routes/api_index.py``. The dashboard metrics endpoint, the Decisions
tab (constants/simulate), the beets-distance endpoint, and the
resolve-rg/replace release-identity endpoints were split out (#522) into
``web/routes/pipeline_dashboard.py``, ``web/routes/decisions.py``,
``web/routes/beets_distance.py``, and
``web/routes/release_identity_routes.py``. The CRUD mutation cluster
(add/update/upgrade/set-quality/set-intent/ban-source/force-import/delete)
was split out (#546 W4) into ``web/routes/pipeline_mutations.py``.
"""

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

import msgspec

from lib.beets_db import CurrentBeetsUnique
from lib.current_library_display import (
    CurrentLibraryUnavailable,
    current_library_display,
    resolve_request_current_library,
)
from lib.import_queue import ImportJob

if TYPE_CHECKING:
    from lib.pipeline_db import LatestDownloadSummary

from web.routes._registry import RouteHandler, RouteRegistration, pattern_route, route
from web.routes._server_access import _server

logger = logging.getLogger(__name__)

from lib.quality import CandidateScore, top_candidates
from web.classify import classify_import_job_display, evidence_accusation_flags
from web.download_history_view import (
    build_download_history_row,
    build_download_history_rows,
    build_recents_download_log_rows,
    last_download_accusation_flags,
)

DEFAULT_PIPELINE_LOG_LIMIT = 50
MAX_PIPELINE_LOG_LIMIT = 500


def _pipeline_log_limit(params: dict[str, list[str]]) -> int:
    raw = params.get("limit", [str(DEFAULT_PIPELINE_LOG_LIMIT)])[0]
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_PIPELINE_LOG_LIMIT
    return max(1, min(limit, MAX_PIPELINE_LOG_LIMIT))


# ── GET handlers ─────────────────────────────────────────────────


def get_pipeline_log(h: RouteHandler, params: dict[str, list[str]]) -> None:
    outcome_filter = params.get("outcome", [None])[0]
    if outcome_filter not in (None, "imported", "rejected"):
        outcome_filter = None
    entries = _server()._db().get_log(
        limit=_pipeline_log_limit(params),
        outcome_filter=outcome_filter,
    )
    mbids = list({
        str(e["mb_release_id"]) for e in entries if e.get("mb_release_id")
    })
    beets_info = _server().check_beets_library_detail(mbids) if mbids else {}
    source_ids = [entry["id"] for entry in entries]
    linked_rows = _server()._db().get_linked_import_logs(source_ids)
    result = build_recents_download_log_rows(
        entries,
        linked_successor_rows=linked_rows,
    )
    for item in result:
        mbid = item.get("mb_release_id")
        bi = beets_info.get(mbid) if isinstance(mbid, str) else None
        item["in_beets"] = bi is not None
        if bi:
            item["beets_format"] = bi.get("beets_format")
            item["beets_bitrate"] = bi.get("beets_bitrate")
            item["beets_avg_bitrate"] = bi.get("beets_avg_bitrate")
    # Count recents filters plus found-search enqueue rates (single query).
    counts = _server()._db().get_download_log_counts()
    h._json({
        "log": result,
        "counts": {
            "all": counts.total,
            "imported": counts.imported,
            "rejected": counts.total - counts.imported,
            "matches_24h": counts.matches_24h,
            "matches_6h": counts.matches_6h,
            "matches_per_hour_24h": counts.matches_24h / 24,
            "matches_per_hour_6h": counts.matches_6h / 6,
        },
    })


def get_pipeline_status(h: RouteHandler, params: dict[str, list[str]]) -> None:
    counts = _server()._db().count_by_status()
    wanted = _server()._db().get_wanted(limit=50)
    h._json({
        "counts": counts,
        "wanted": [
            {
                "id": w["id"],
                "artist": w["artist_name"],
                "album": w["album_title"],
                "mb_release_id": w["mb_release_id"],
                "source": w["source"],
                "created_at": str(w["created_at"]),
                "processing_owner": None,
            }
            for w in wanted
        ],
    })


def _attach_latest_download_summaries(
    items: list[dict[str, object]],
    summaries: "Mapping[int, LatestDownloadSummary]",
) -> list[dict[str, object]]:
    """Stamp each request row with its newest download's verdict fields.

    ``summaries`` comes from ``get_latest_download_summaries`` — one
    latest row + a count per request, never the full history (#426).
    """
    for item in items:
        summary = summaries.get(int(str(item["id"])))
        if summary:
            last = build_download_history_row(summary["latest"])
            item["last_verdict"] = last.verdict
            item["last_outcome"] = last.outcome
            item["last_username"] = last.soulseek_username
            item["download_count"] = summary["count"]
    return items


# The imported cohort is the whole library backfill (~7K rows and
# growing) — the queue serves a recency window plus server-side search
# instead of the full list (#426).
IMPORTED_RECENT_LIMIT = 100


def get_pipeline_all(h: RouteHandler, params: dict[str, list[str]]) -> None:
    s = _server()
    counts = s._db().count_by_status()
    all_data: dict[str, object] = {"counts": counts}
    status_items: dict[str, list[dict[str, object]]] = {}
    all_ids: list[int] = []
    statuses: tuple[str, ...] = (
        "wanted", "downloading", "processing", "imported", "unsearchable")
    # ``?include_replaced=true`` opt-in surfaces the frozen audit rows
    # for operators reviewing past Replace actions (R30). Default off so
    # the standard view stays focused on active work.
    include_replaced = (
        params.get("include_replaced", ["false"])[0].lower() == "true"
    )
    if include_replaced:
        statuses = statuses + ("replaced",)
    for status in statuses:
        if status == "imported":
            db_rows = s._db().get_by_status(
                "imported", limit=IMPORTED_RECENT_LIMIT, newest_first=True)
        else:
            db_rows = s._db().get_by_status(status)
        rows = [s._serialize_row(r) for r in db_rows]
        status_items[status] = rows
        all_ids.extend([int(str(r["id"])) for r in rows])
    summaries = s._db().get_latest_download_summaries(all_ids)
    for status in statuses:
        all_data[status] = _attach_latest_download_summaries(
            status_items[status],
            summaries,
        )
    all_data["imported_total"] = int(counts.get("imported", 0))
    all_data["imported_truncated"] = (
        int(counts.get("imported", 0)) > IMPORTED_RECENT_LIMIT
    )
    h._json(all_data)


def get_pipeline_search(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Operator search over artist/album across every status (#426)."""
    s = _server()
    query = params.get("q", [""])[0]
    rows = [s._serialize_row(r) for r in s._db().search_requests(query)]
    ids = [int(str(r["id"])) for r in rows]
    summaries = s._db().get_latest_download_summaries(ids)
    h._json({
        "query": query,
        "items": _attach_latest_download_summaries(rows, summaries),
        "total": len(rows),
    })


def get_pipeline_downloading(h: RouteHandler, params: dict[str, list[str]]) -> None:
    s = _server()
    counts = s._db().count_by_status()
    rows = [s._serialize_row(r) for r in s._db().get_by_status("downloading")]
    ids = [int(str(r["id"])) for r in rows]
    summaries = s._db().get_latest_download_summaries(ids)
    h._json({
        "counts": counts,
        "downloading": _attach_latest_download_summaries(rows, summaries),
    })


def get_pipeline_acquisition(
    h: RouteHandler,
    params: dict[str, list[str]],
) -> None:
    """Return active downloader/processor requests plus YouTube ingest."""
    del params
    s = _server()
    payload = s._db().get_acquisition(youtube_limit=50)
    acquisition = [
        s._serialize_row(row)
        for row in payload["acquisition"]
    ]
    ids = [int(str(row["id"])) for row in acquisition]
    summaries = s._db().get_latest_download_summaries(ids)
    h._json({
        "acquisition": _attach_latest_download_summaries(
            acquisition,
            summaries,
        ),
        "youtube_ingest": [
            s._serialize_row(row)
            for row in payload["youtube_ingest"]
        ],
    })


def _build_last_search_payload(
    search_history: list[dict[str, object]],
) -> dict[str, object] | None:
    """Build the ``last_search`` slice of the request-detail response.

    Single decode site (per ``.claude/rules/code-quality.md`` § Wire-boundary
    types) for the ``search_log.candidates`` JSONB blob: ``msgspec.convert``
    turns it into ``list[CandidateScore]`` here, and the response is
    re-encoded via ``msgspec.to_builtins`` for symmetric strictness. Older
    rows with ``candidates=NULL`` (or missing) read as ``[]`` — no
    ``ValidationError``. Returns ``None`` when the request has no
    search_log rows yet.
    """
    if not search_history:
        return None
    latest = search_history[0]  # get_search_history orders newest first
    raw_candidates = latest.get("candidates")
    candidates: list[CandidateScore]
    if raw_candidates is None:
        candidates = []
    else:
        try:
            candidates = msgspec.convert(
                raw_candidates, type=list[CandidateScore]
            )
        except msgspec.ValidationError as exc:
            # Mirrors the CLI's defensive guard in
            # scripts/pipeline_cli.py:_render_search_forensics_summary —
            # production writes via the same Struct so this should never trip,
            # but a corrupted historical row must not 500 the detail route.
            logger.warning(
                "search_log.candidates JSONB failed msgspec validation "
                "(request_id=%s, search_log_id=%s): %s",
                latest.get("request_id"), latest.get("id"), exc,
            )
            candidates = []
    # Top-20 by (matched_tracks DESC, avg_ratio DESC) — the full stored cap
    # (search_log.candidates persists at most 20). The long-tail console's
    # "peers seen" panel renders the wider slice; the compact detail view
    # shows the same ranking, just more rows. Shared ranking lives in
    # lib/quality/wire_types.py.
    top = top_candidates(candidates, limit=20)
    return {
        "variant": latest.get("variant"),
        "final_state": latest.get("final_state"),
        "outcome": latest.get("outcome"),
        "top_candidates": [msgspec.to_builtins(c) for c in top],
    }


def get_pipeline_detail(h: RouteHandler, params: dict[str, list[str]], req_id_str: str) -> None:
    s = _server()
    req_id = int(req_id_str)
    req = s._db().get_request(req_id)
    if not req:
        h._error("Not found", 404)
        return
    tracks = s._db().get_tracks(req_id)
    history = s._db().get_download_history(req_id)
    history_items = [item.to_dict() for item in build_download_history_rows(history)]
    search_history = s._db().get_search_history(req_id)
    last_search = _build_last_search_payload(search_history)
    request_payload = s._serialize_row(req)
    # The detail header's Quality row picks its grade from a fallback
    # chain over BOTH the installed copy and the last download, so it
    # needs both audit-only pairs and applies whichever matches the grade
    # it selected (issue #829 Phase 5 PR4). Each pair is derived by the
    # one shared rule from the measurement that produced ITS grade; an
    # absent pair keeps the historical accusing render.
    have_flags = evidence_accusation_flags(
        s._db().load_album_quality_evidence_by_id(req["current_evidence_id"])
    )
    candidate_flags = last_download_accusation_flags(
        history_items, req["last_download_spectral_grade"]
    )
    request_payload["current_spectral_accusation_admissible"] = (
        have_flags.admissible)
    request_payload["current_spectral_accusation_withheld"] = have_flags.withheld
    request_payload["last_download_spectral_accusation_admissible"] = (
        candidate_flags.admissible)
    request_payload["last_download_spectral_accusation_withheld"] = (
        candidate_flags.withheld)
    try:
        b = s._beets_db()
        current = resolve_request_current_library(req, b)
    except Exception:
        logger.exception(
            "current Beets display unavailable for request %s", req_id,
        )
        current = CurrentLibraryUnavailable("beets_unavailable")
    result: dict[str, object] = {
        "request": request_payload,
        "tracks": tracks,
        "history": history_items,
        "last_search": last_search,
        "current_library": msgspec.to_builtins(current_library_display(current)),
    }
    if isinstance(current, CurrentBeetsUnique):
        result["beets_tracks"] = [
            {
                "title": item.title,
                "track": item.track,
                "disc": item.disc,
                "length": item.length,
                "format": item.format,
                "bitrate": item.bitrate,
                "samplerate": item.samplerate,
                "bitdepth": item.bitdepth,
            }
            for item in sorted(
                current.items,
                key=lambda item: (item.disc or 0, item.track or 0, item.id),
            )
        ]
    h._json(result)


def get_pipeline_requests_by_rg(h: RouteHandler, params: dict[str, list[str]], rg_id: str) -> None:
    """``GET /api/pipeline/requests-by-rg/<rg_id>``.

    Returns the non-replaced ``album_requests`` rows sharing the given
    release group, in id-descending order. Used by the Browse-search
    inverted-click picker (R7) to ask the operator which existing
    request should be replaced.
    """
    db = _server()._db()
    rows = db.list_requests_in_release_group(rg_id, exclude_replaced=True)
    requests = [
        {
            "id": int(r["id"]),
            "mb_release_id": r.get("mb_release_id"),
            "mb_release_group_id": r.get("mb_release_group_id"),
            "status": r.get("status"),
            "artist_name": r.get("artist_name"),
            "album_title": r.get("album_title"),
            "processing_owner": _server()._serialize_row(r).get(
                "processing_owner"
            ),
        }
        for r in rows
    ]
    h._json({"requests": requests})


def get_pipeline_active_rgs(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """``GET /api/pipeline/active-rgs``.

    Returns the distinct set of ``mb_release_group_id`` values held by
    any non-replaced ``album_requests`` row. The frontend builds a Set
    from this list and uses ``set.has(row.release_group_id)`` per
    Browse-search row to compute the Replace button enable state.
    """
    db = _server()._db()
    ids = sorted(db.list_active_release_group_ids())
    h._json({"release_group_ids": ids})


def _serialize_import_job(job: ImportJob) -> dict[str, object]:
    return job.to_json_dict()


def get_import_jobs(h: RouteHandler, params: dict[str, list[str]]) -> None:
    status = params.get("status", [None])[0]
    request_id_raw = params.get("request_id", [None])[0]
    if status not in (
        None,
        "",
        "queued",
        "running",
        "recovery_required",
        "completed",
        "failed",
    ):
        h._error("Invalid import job status")
        return
    status = status or None
    try:
        request_id = int(request_id_raw) if request_id_raw else None
    except ValueError:
        h._error("Invalid request_id")
        return
    jobs = _server()._db().list_import_jobs(
        status=status,
        request_id=request_id,
        limit=50,
    )
    h._json({
        "jobs": [_serialize_import_job(job) for job in jobs],
        "counts": _server()._db().count_import_jobs_by_status(),
    })


def get_import_jobs_timeline(h: RouteHandler, params: dict[str, list[str]]) -> None:
    db = _server()._db()
    jobs = db.list_import_job_timeline(limit=50)
    serialized: list[dict[str, object]] = []
    for queue_position, job in enumerate(jobs):
        item = _serialize_import_job(job)
        item.update(msgspec.to_builtins(classify_import_job_display(
            job,
            queue_position=queue_position,
        )))
        request_id = item.get("request_id")
        if isinstance(request_id, (int, str)) and not isinstance(request_id, bool):
            req = db.get_request(int(request_id))
            if req:
                item["artist_name"] = req.get("artist_name")
                item["album_title"] = req.get("album_title")
                item["mb_release_id"] = req.get("mb_release_id")
        serialized.append(item)
    h._json({
        "jobs": serialized,
        "counts": db.count_import_jobs_by_status(),
    })


def get_import_job(h: RouteHandler, params: dict[str, list[str]], job_id_str: str) -> None:
    job = _server()._db().get_import_job(int(job_id_str))
    if job is None:
        h._error("Import job not found", 404)
        return
    h._json({"job": _serialize_import_job(job)})


def get_import_job_recovery(
    h: RouteHandler,
    params: dict[str, list[str]],
    job_id_str: str,
) -> None:
    """Return the shared automation recovery diagnostic observation."""
    from lib.import_job_recovery_service import (
        get_automation_recovery_detail,
    )

    server = _server()
    result = get_automation_recovery_detail(
        server._db(),
        server._beets_db(),
        int(job_id_str),
    )
    h._json(
        result.to_dict(),
        status=200 if result.outcome == "ok" else 404,
    )

# ── Route tables ─────────────────────────────────────────────────

ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/pipeline/log", get_pipeline_log,
        "Recent download_log rows with per-row classification badges + "
        "rolling found-search counts.",
        classified=True,
    ),
    route(
        "GET", "/api/pipeline/status", get_pipeline_status,
        "Status counts + the first 50 wanted requests for the dashboard.",
        classified=True,
    ),
    route(
        "GET", "/api/pipeline/all", get_pipeline_all,
        "Pipeline requests bucketed by status; latest download summary "
        "attached per row. The imported bucket is a recency window "
        "(newest 100; imported_total/imported_truncated flag the cap) — "
        "use /api/pipeline/search for the rest. include_replaced=true "
        "opts in to frozen audit rows.",
        classified=True,
    ),
    route(
        "GET", "/api/pipeline/search", get_pipeline_search,
        "Operator search over artist/album across every status "
        "(?q=substring, case-insensitive); latest download summary "
        "attached per row.",
        classified=True,
    ),
    route(
        "GET", "/api/pipeline/downloading", get_pipeline_downloading,
        "Transfer-owned pipeline requests currently in downloading status.",
        classified=True,
    ),
    route(
        "GET", "/api/pipeline/acquisition", get_pipeline_acquisition,
        "Active acquisition view: downloading and processing request rows "
        "with exact processing owners, plus active YouTube rescue ingests.",
        classified=True,
    ),
    route(
        "GET", "/api/import-jobs", get_import_jobs,
        "Recent import-queue jobs filtered by status / request_id.",
        classified=True,
    ),
    route(
        "GET", "/api/import-jobs/timeline", get_import_jobs_timeline,
        "Active import-queue jobs in claim order with request metadata and "
        "server-classified display fields.",
        classified=True,
    ),
    route(
        "GET", "/api/pipeline/active-rgs", get_pipeline_active_rgs,
        "Distinct release-group IDs held by any non-replaced request "
        "(Replace-button enable set).",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/pipeline/(\d+)$", get_pipeline_detail,
        "Full pipeline request detail — tracks, download history, last "
        "search, beets tracks if present.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/pipeline/requests-by-rg/([a-f0-9-]{36})$",
        get_pipeline_requests_by_rg,
        "Non-replaced album_requests rows sharing the given release "
        "group, id-descending.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/import-jobs/(\d+)$", get_import_job,
        "Single import-job detail by job id.",
        classified=True,
    ),
    pattern_route(
        "GET", r"^/api/import-jobs/(\d+)/recovery$",
        get_import_job_recovery,
        "Read-only recovery diagnostics for one exact automation owner.",
        classified=True,
    ),
]
