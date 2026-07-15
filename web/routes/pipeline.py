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
from collections.abc import Mapping, Sequence
from typing import cast

import msgspec

from web.routes._registry import RouteRegistration, pattern_route, route
from web.routes._server_access import _server

logger = logging.getLogger(__name__)

from web.download_history_view import (
    build_download_history_row,
    build_download_history_rows,
    classify_download_log_row,
)
from web.classify import classify_import_job_display
from lib.quality import CandidateScore, top_candidates

DEFAULT_PIPELINE_LOG_LIMIT = 50
MAX_PIPELINE_LOG_LIMIT = 500


def _project_current_library_have(
    item: dict[str, object],
    row: Mapping[str, object],
    _beets: Mapping[str, object],
) -> None:
    """Fill legacy HAVE only from a provably pre-attempt exact snapshot.

    HAVE is historical: what was on disk before this attempt. Successful and
    explicit import rows may have updated the request's current evidence, so
    they never receive an overlay. Non-mutating legacy rows may use the
    request's canonical evidence only when its measurement predates the
    attempt. Live Beets data has no historical timestamp and is never evidence
    for HAVE.
    """
    attempt_measurement_fields = (
        "existing_format",
        "existing_min_bitrate",
        "existing_avg_bitrate",
        "existing_median_bitrate",
        "existing_spectral_grade",
        "existing_spectral_bitrate",
        "existing_spectral_error",
        "existing_v0_probe_kind",
        "existing_v0_probe_min_bitrate",
        "existing_v0_probe_avg_bitrate",
        "existing_v0_probe_median_bitrate",
        "comparison_basis",
    )
    if item.get("existing_spectral_attempted") is True or any(
        item.get(field) is not None for field in attempt_measurement_fields
    ):
        return
    if item.get("outcome") in ("success", "force_import", "manual_import"):
        return

    current_projection = {
        "existing_format": row.get("_current_evidence_format"),
        "existing_min_bitrate": row.get("_current_evidence_min_bitrate"),
        "existing_avg_bitrate": row.get("_current_evidence_avg_bitrate"),
        "existing_median_bitrate": row.get(
            "_current_evidence_median_bitrate"
        ),
        "existing_spectral_grade": row.get(
            "_current_evidence_spectral_grade"
        ),
        "existing_spectral_bitrate": row.get(
            "_current_evidence_spectral_bitrate"
        ),
        "existing_v0_probe_kind": row.get(
            "_current_evidence_v0_probe_kind"
        ),
        "existing_v0_probe_min_bitrate": row.get(
            "_current_evidence_v0_probe_min_bitrate"
        ),
        "existing_v0_probe_avg_bitrate": row.get(
            "_current_evidence_v0_probe_avg_bitrate"
        ),
        "existing_v0_probe_median_bitrate": row.get(
            "_current_evidence_v0_probe_median_bitrate"
        ),
    }
    if (
        row.get("_current_evidence_id") is not None
        and row.get("_current_evidence_is_pre_attempt") is True
    ):
        item.update(current_projection)


def _project_linked_import_evidence(
    items: list[dict[str, object]],
    linked_successors: Sequence[Mapping[str, object]] = (),
) -> None:
    """Attach a successor import's measurements to its source audit row.

    Force/manual import rows explicitly point back through
    ``source_download_log_id``. That is the authoritative bridge from a kept
    wrong-match card to the conversion which later materialized those bytes;
    do not infer the relationship from matching albums or measurements.
    """
    by_id = {
        item.get("id"): item
        for item in items
        if isinstance(item.get("id"), int)
    }
    for successor in (*items, *linked_successors):
        if successor.get("outcome") not in (
            "success", "force_import", "manual_import"
        ):
            continue
        source_id = successor.get("source_download_log_id")
        origin = by_id.get(source_id)
        if origin is None or successor.get("materialized_format") is None:
            continue
        for field in (
            "existing_format",
            "existing_min_bitrate",
            "existing_avg_bitrate",
            "existing_median_bitrate",
            "existing_spectral_grade",
            "existing_spectral_bitrate",
            "existing_spectral_attempted",
            "existing_spectral_error",
            "existing_v0_probe_kind",
            "existing_v0_probe_min_bitrate",
            "existing_v0_probe_avg_bitrate",
            "existing_v0_probe_median_bitrate",
            "materialized_format",
            "materialized_min_bitrate",
            "materialized_avg_bitrate",
            "materialized_median_bitrate",
            "target_contract_format",
        ):
            if origin.get(field) is None:
                origin[field] = successor.get(field)


def _classify_pipeline_log_item(
    row: Mapping[str, object],
) -> dict[str, object]:
    classified_row = classify_download_log_row(row)
    return {
        **classified_row.entry.to_json_dict(),
        **cast(
            dict[str, object],
            msgspec.to_builtins(classified_row.classified),
        ),
    }


def _pipeline_log_limit(params: dict[str, list[str]]) -> int:
    raw = params.get("limit", [str(DEFAULT_PIPELINE_LOG_LIMIT)])[0]
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_PIPELINE_LOG_LIMIT
    return max(1, min(limit, MAX_PIPELINE_LOG_LIMIT))


# ── GET handlers ─────────────────────────────────────────────────


def get_pipeline_log(h, params: dict[str, list[str]]) -> None:
    outcome_filter = params.get("outcome", [None])[0]
    if outcome_filter not in (None, "imported", "rejected"):
        outcome_filter = None
    entries = _server()._db().get_log(
        limit=_pipeline_log_limit(params),
        outcome_filter=outcome_filter,
    )
    mbids = list(set(e["mb_release_id"] for e in entries if e.get("mb_release_id")))
    beets_info = _server().check_beets_library_detail(mbids) if mbids else {}
    result = []
    for e in entries:
        item = _classify_pipeline_log_item(e)
        mbid = item.get("mb_release_id")
        bi = beets_info.get(mbid) if isinstance(mbid, str) else None
        item["in_beets"] = bi is not None
        _project_current_library_have(item, e, bi or {})
        if bi:
            item["beets_format"] = bi.get("beets_format")
            item["beets_bitrate"] = bi.get("beets_bitrate")
            item["beets_avg_bitrate"] = bi.get("beets_avg_bitrate")
        result.append(item)
    source_ids = [
        int(item["id"])
        for item in result
        if isinstance(item.get("id"), int)
    ]
    linked_items = [
        _classify_pipeline_log_item(row)
        for row in _server()._db().get_linked_import_logs(source_ids)
    ]
    _project_linked_import_evidence(result, linked_items)
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


def get_pipeline_status(h, params: dict[str, list[str]]) -> None:
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
            }
            for w in wanted
        ],
    })


def _attach_latest_download_summaries(
    items: list[dict],
    summaries: dict[int, dict],
) -> list[dict]:
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


def get_pipeline_all(h, params: dict[str, list[str]]) -> None:
    s = _server()
    counts = s._db().count_by_status()
    all_data: dict[str, object] = {"counts": counts}
    status_items: dict[str, list[dict]] = {}
    all_ids: list[int] = []
    statuses: tuple[str, ...] = ("wanted", "downloading", "imported", "manual")
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


def get_pipeline_search(h, params: dict[str, list[str]]) -> None:
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


def get_pipeline_downloading(h, params: dict[str, list[str]]) -> None:
    s = _server()
    counts = s._db().count_by_status()
    rows = [s._serialize_row(r) for r in s._db().get_by_status("downloading")]
    ids = [int(str(r["id"])) for r in rows]
    summaries = s._db().get_latest_download_summaries(ids)
    youtube_ingest = [
        s._serialize_row(r)
        for r in s._db().list_active_youtube_rescues(limit=50)
    ]
    h._json({
        "counts": counts,
        "downloading": _attach_latest_download_summaries(rows, summaries),
        "youtube_ingest": youtube_ingest,
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


def get_pipeline_detail(h, params: dict[str, list[str]], req_id_str: str) -> None:
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
    result: dict[str, object] = {
        "request": s._serialize_row(req),
        "tracks": tracks,
        "history": history_items,
        "manual_reason": req.get("manual_reason"),
        "last_search": last_search,
    }
    mbid = req.get("mb_release_id")
    b = s._beets_db()
    if mbid and b:
        tracks = b.get_tracks_by_mb_release_id(mbid)
        if tracks is not None:
            result["beets_tracks"] = tracks
    h._json(result)


def get_pipeline_requests_by_rg(h, params: dict, rg_id: str) -> None:
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
        }
        for r in rows
    ]
    h._json({"requests": requests})


def get_pipeline_active_rgs(h, params: dict) -> None:
    """``GET /api/pipeline/active-rgs``.

    Returns the distinct set of ``mb_release_group_id`` values held by
    any non-replaced ``album_requests`` row. The frontend builds a Set
    from this list and uses ``set.has(row.release_group_id)`` per
    Browse-search row to compute the Replace button enable state.
    """
    db = _server()._db()
    ids = sorted(db.list_active_release_group_ids())
    h._json({"release_group_ids": ids})


def _serialize_import_job(job) -> dict[str, object]:
    if hasattr(job, "to_json_dict"):
        return job.to_json_dict()
    return dict(job)


def get_import_jobs(h, params: dict[str, list[str]]) -> None:
    status = params.get("status", [None])[0]
    request_id_raw = params.get("request_id", [None])[0]
    if status not in (None, "", "queued", "running", "completed", "failed"):
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


def get_import_jobs_timeline(h, params: dict[str, list[str]]) -> None:
    db = _server()._db()
    jobs = db.list_import_job_timeline(limit=50)
    serialized = []
    for queue_position, job in enumerate(jobs):
        item = _serialize_import_job(job)
        item.update(cast(
            dict[str, object],
            msgspec.to_builtins(classify_import_job_display(
                job,
                queue_position=queue_position,
            )),
        ))
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


def get_import_job(h, params: dict[str, list[str]], job_id_str: str) -> None:
    job = _server()._db().get_import_job(int(job_id_str))
    if job is None:
        h._error("Import job not found", 404)
        return
    h._json({"job": _serialize_import_job(job)})


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
        "Pipeline requests currently in the downloading status, plus "
        "active YouTube rescue ingests.",
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
]
