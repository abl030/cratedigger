"""Manual import route handlers — scan, import, wrong matches."""

import json
import shutil

from lib.manual_import import (
    scan_complete_folder,
    match_folders_to_requests,
    ImportRequest,
)
from lib.import_queue import (
    IMPORT_JOB_MANUAL,
    manual_import_dedupe_key,
    manual_import_payload,
)
from lib.util import resolve_failed_path
from web.routes.pipeline import _serialize_import_job


def _server():
    from web import server
    return server


def _parse_validation_result(vr_raw: object) -> dict[str, object]:
    """Parse a validation_result JSONB value into a plain dict."""
    if isinstance(vr_raw, dict):
        return vr_raw
    if not vr_raw:
        return {}
    return json.loads(str(vr_raw))


def _row_presence(
    row: dict[str, object],
    beets_info: dict[str, dict[str, object]],
) -> str:
    """Answer 'is this release on disk?' for a wrong-matches row.

    Returns ``'exact'`` if the pipeline row's ``mb_release_id`` appears
    in the batched exact-hit lookup (via ``check_beets_library_detail``
    → ``BeetsDB.check_mbids_detail``), otherwise ``'absent'``. Matches
    the vocabulary of ``BeetsDB.ReleaseLocation.kind`` (issues #121 /
    #123).

    Issue #123 deleted the fuzzy artist+album fallback that used to
    return ``'fuzzy'``. It conflated identity with presence and
    silently attributed stale quality fields from sibling pressings
    to the badge. 'In library' now means exact-ID match, period.
    """
    mbid = row.get("mb_release_id")
    if isinstance(mbid, str) and mbid and mbid in beets_info:
        return "exact"
    return "absent"


def _target_candidate(vr: dict[str, object]) -> dict[str, object] | None:
    """Return the target candidate from a validation_result payload."""
    raw_candidates = vr.get("candidates", [])
    if not isinstance(raw_candidates, list):
        return None

    candidates = [
        candidate for candidate in raw_candidates
        if isinstance(candidate, dict)
    ]
    target = next(
        (candidate for candidate in candidates if candidate.get("is_target")),
        None,
    )
    if target is not None:
        return target
    return candidates[0] if candidates else None


def get_manual_import_scan(h, params: dict[str, list[str]]) -> None:

    complete_dir = params.get("dir", ["/mnt/data/Media/Temp/Music/Complete"])[0]
    folders = scan_complete_folder(complete_dir)

    # Get wanted requests for matching
    pdb = _server()._db()
    wanted = pdb.get_by_status("wanted")
    requests = [
        ImportRequest(
            id=r["id"],
            artist_name=r["artist_name"],
            album_title=r["album_title"],
            mb_release_id=r.get("mb_release_id", ""),
        )
        for r in wanted
    ]

    matches = match_folders_to_requests(folders, requests)

    h._json({
        "folders": [
            {
                "name": f.name,
                "path": f.path,
                "artist": f.artist,
                "album": f.album,
                "file_count": f.file_count,
                "match": next(
                    ({"request_id": m.request.id,
                      "artist": m.request.artist_name,
                      "album": m.request.album_title,
                      "mb_release_id": m.request.mb_release_id,
                      "score": round(m.score, 2)}
                     for m in matches if m.folder.name == f.name),
                    None,
                ),
            }
            for f in folders
        ],
        "wanted_count": len(requests),
    })


def post_manual_import(h, body: dict) -> None:
    srv = _server()
    request_id = body.get("request_id")
    path = body.get("path")
    if not request_id or not path:
        h._error("Missing request_id or path")
        return

    req = srv._db().get_request(int(request_id))
    if not req:
        h._error(f"Request {request_id} not found", 404)
        return
    mbid = req["mb_release_id"]
    if not mbid:
        h._error("Request has no MusicBrainz release ID")
        return

    resolved_path = resolve_failed_path(str(path))
    if resolved_path is None:
        h._error(f"Files not found at: {path}")
        return

    job = srv._db().enqueue_import_job(
        IMPORT_JOB_MANUAL,
        request_id=int(request_id),
        dedupe_key=manual_import_dedupe_key(int(request_id), resolved_path),
        payload=manual_import_payload(failed_path=resolved_path),
        message=f"Manual import queued for {req['artist_name']} - {req['album_title']}",
    )

    h._json({
        "status": "queued",
        "message": "Import queued",
        "job_id": job.id,
        "job": _serialize_import_job(job),
        "deduped": bool(getattr(job, "deduped", False)),
        "request_id": request_id,
        "artist": req["artist_name"],
        "album": req["album_title"],
    }, status=202)


def _quality_summary(row: dict[str, object],
                     beets_info: dict[str, dict[str, object]],
                     presence: str,
                     ) -> dict[str, object]:
    """Describe the album's current on-disk quality for a group header.

    Beets is the source of truth for format and bitrate when the album is
    imported; the pipeline DB carries the spectral + verified-lossless signal
    (those never live in beets). We combine them so the user can see at a
    glance whether force-importing is worthwhile.

    On-disk quality is reported only when ``presence == "exact"`` (issues
    #121 / #123). The fuzzy artist+album fallback was deleted — 'in
    library' now means exact-ID match, so ``presence != "exact"`` and
    ``"absent"`` are synonymous here (kept as a string to preserve the
    ``ReleaseLocation.kind`` vocabulary for the read side).
    """
    status = str(row.get("request_status") or "wanted")
    if presence != "exact":
        return {
            "status": status,
            "min_bitrate": None,
            "format": None,
            "verified_lossless": False,
            "current_spectral_grade": None,
            "current_spectral_bitrate": None,
            "quality_label": None,
            "quality_rank": None,
        }

    srv = _server()
    mbid = row.get("mb_release_id")
    detail = beets_info.get(mbid) if isinstance(mbid, str) and mbid else None

    # Bitrate: prefer pipeline DB (kbps, always authoritative for spectral
    # classification), fall back to beets. For the quality label + rank,
    # prefer beets's actual value once imported.
    def _as_int(val: object) -> int | None:
        return val if isinstance(val, int) and not isinstance(val, bool) else None

    def _as_str(val: object) -> str | None:
        return val if isinstance(val, str) else None

    db_kbps = _as_int(row.get("request_min_bitrate"))
    beets_kbps = _as_int(detail.get("beets_bitrate")) if detail else None
    fmt = _as_str(detail.get("beets_format")) if detail else None

    label: str | None = None
    rank: str | None = None
    if fmt:
        # Label is only meaningful with a bitrate; rank is meaningful from
        # format alone (falls through to the bare-codec band table).
        if beets_kbps:
            from web.classify import quality_label as _ql
            label = _ql(fmt, beets_kbps)
        rank = srv.compute_library_rank(fmt, beets_kbps)

    return {
        "status": status,
        "min_bitrate": db_kbps if db_kbps is not None else beets_kbps,
        "format": fmt,
        "verified_lossless": bool(row.get("request_verified_lossless") or False),
        "current_spectral_grade": row.get("request_current_spectral_grade"),
        "current_spectral_bitrate": row.get("request_current_spectral_bitrate"),
        "quality_label": label,
        "quality_rank": rank,
    }


_IMPORT_SUCCESS_OUTCOMES = ("success", "force_import", "manual_import")


def _latest_import_summary(rows: list[dict[str, object]]
                           ) -> dict[str, object] | None:
    """Summary of the last successful import for a request.

    The expanded-group header describes what's currently on disk, not the most
    recent attempt. A rejection that happened after a successful import
    doesn't change what beets has — the earlier success is still the
    authoritative picture. Scan the newest-first history for the first
    success/force_import/manual_import row and surface its metadata.

    Returns ``None`` when the release has never been successfully imported.
    """
    if not rows:
        return None
    from datetime import datetime
    picked: dict[str, object] | None = None
    for row in rows:
        outcome = row.get("outcome")
        if isinstance(outcome, str) and outcome in _IMPORT_SUCCESS_OUTCOMES:
            picked = row
            break
    if picked is None:
        return None
    created_raw = picked.get("created_at")
    created: str | None = None
    if isinstance(created_raw, datetime):
        created = created_raw.isoformat()
    elif isinstance(created_raw, str):
        created = created_raw
    return {
        "id": picked.get("id"),
        "outcome": picked.get("outcome"),
        "created_at": created,
        "soulseek_username": picked.get("soulseek_username"),
        "actual_filetype": picked.get("actual_filetype"),
        "actual_min_bitrate": picked.get("actual_min_bitrate"),
        "beets_scenario": picked.get("beets_scenario"),
    }


def get_wrong_matches(h, params: dict[str, list[str]]) -> None:
    """Group wrong-match rejections by release (issue #113).

    Each ``album_requests`` row becomes one group; every rejected
    ``download_log`` entry with an on-disk ``failed_path`` becomes one entry
    inside its group. Groups with zero surviving entries are dropped so the
    UI only shows actionable work.

    Each group also carries an on-disk quality snapshot (format, bitrate,
    verified_lossless, spectral grade, rank tier) and the most-recent
    ``download_log`` row for the request, so the user can judge at a glance
    whether it's worth trying to force-import a rejected candidate.
    """
    srv = _server()
    pdb = srv._db()
    rows = pdb.get_wrong_matches()
    active_import_jobs = pdb.list_active_import_jobs(limit=200)
    active_jobs_by_log_id: dict[int, object] = {}
    active_jobs_by_request_id: dict[int, list[object]] = {}
    for job in active_import_jobs:
        payload = getattr(job, "payload", {}) or {}
        request_id = getattr(job, "request_id", None)
        if isinstance(request_id, int):
            active_jobs_by_request_id.setdefault(request_id, []).append(job)
        download_log_id = payload.get("download_log_id")
        if isinstance(download_log_id, int):
            active_jobs_by_log_id[download_log_id] = job
    mbids = [
        mbid for row in rows
        for mbid in [row.get("mb_release_id")]
        if isinstance(mbid, str) and mbid
    ]
    beets_info = srv.check_beets_library_detail(mbids) if mbids else {}

    groups: dict[int, dict[str, object]] = {}
    order: list[int] = []

    for row in rows:
        vr = _parse_validation_result(row.get("validation_result"))
        failed_path_raw = vr.get("failed_path")
        failed_path = failed_path_raw if isinstance(failed_path_raw, str) else ""
        resolved_path = resolve_failed_path(failed_path)
        files_exist = resolved_path is not None
        if not files_exist:
            continue

        request_id = row["request_id"]
        assert isinstance(request_id, int)
        group = groups.get(request_id)
        if group is None:
            # Single seam for 'is this release on disk?' (issues #121 /
            # #123). ``_row_presence`` now returns just ``"exact"`` or
            # ``"absent"`` — the badge and the quality strip both gate
            # on exact-ID match, with no fuzzy escape hatch. Untagged
            # legacy copies honestly read 'not in library' now.
            presence = _row_presence(row, beets_info)
            in_library = presence == "exact"
            group = {
                "request_id": request_id,
                "artist": row["artist_name"],
                "album": row["album_title"],
                "mb_release_id": row.get("mb_release_id"),
                "in_library": in_library,
                "pending_count": 0,
                "entries": [],
                "import_jobs": [
                    _serialize_import_job(job)
                    for job in active_jobs_by_request_id.get(request_id, [])
                ],
                "latest_import": None,  # filled in after the loop
                **_quality_summary(row, beets_info, presence),
            }
            groups[request_id] = group
            order.append(request_id)

        target = _target_candidate(vr)
        entries_list = group["entries"]
        assert isinstance(entries_list, list)
        entries_list.append({
            "download_log_id": row["download_log_id"],
            "failed_path": resolved_path or failed_path,
            "files_exist": files_exist,
            "distance": vr.get("distance"),
            "scenario": vr.get("scenario"),
            "detail": vr.get("detail"),
            "soulseek_username": row.get("soulseek_username")
                or vr.get("soulseek_username"),
            "candidate": target,
            "local_items": vr.get("items", []),
            "import_job": (
                _serialize_import_job(active_jobs_by_log_id[row["download_log_id"]])
                if row["download_log_id"] in active_jobs_by_log_id
                else None
            ),
        })
        group["pending_count"] = len(entries_list)

    # Enrich each group with a summary of the last successful import for the
    # request. Reuses the existing batch helper — returns newest-first per
    # request — and filters for success/force_import/manual_import so the
    # header describes what's on disk rather than the latest attempt.
    if order:
        history = pdb.get_download_history_batch(order)
        for rid in order:
            rows_for_req = history.get(rid) or []
            groups[rid]["latest_import"] = _latest_import_summary(rows_for_req)

    h._json({"groups": [groups[rid] for rid in order]})


def _delete_wrong_match_row(pdb, log_id: int) -> bool:
    """Shared helper: delete files for one wrong-match entry and clear its path.

    Returns ``True`` if the entry existed and was processed, ``False`` if the
    download_log row was missing. Used by both the single-row delete endpoint
    and the per-release bulk delete.
    """
    entry = pdb.get_download_log_entry(log_id)
    if not entry:
        return False
    vr = _parse_validation_result(entry.get("validation_result"))
    failed_path_raw = vr.get("failed_path")
    failed_path = failed_path_raw if isinstance(failed_path_raw, str) else ""
    resolved_path = resolve_failed_path(failed_path)
    if resolved_path is not None:
        shutil.rmtree(resolved_path, ignore_errors=True)
    pdb.clear_wrong_match_path(log_id)
    return True


def post_wrong_match_delete(h, body: dict) -> None:
    """Delete files for a wrong match and clear its failed_path."""
    log_id = body.get("download_log_id")
    if not log_id:
        h._error("Missing download_log_id")
        return

    pdb = _server()._db()
    if not _delete_wrong_match_row(pdb, int(log_id)):
        h._error(f"Download log entry {log_id} not found", 404)
        return

    h._json({"status": "ok", "download_log_id": log_id})


def post_wrong_match_delete_group(h, body: dict) -> None:
    """Delete every wrong-match candidate for one release (request_id).

    Iterates the current ``get_wrong_matches()`` set, filters to rows for the
    given ``request_id``, and deletes each in turn via the same helper as the
    single-row endpoint — so files on disk are removed and ``failed_path`` is
    cleared uniformly. Returns the count deleted so the UI can toast it.
    """
    request_id = body.get("request_id")
    if request_id is None:
        h._error("Missing request_id")
        return
    try:
        rid = int(request_id)
    except (TypeError, ValueError):
        h._error("request_id must be an integer")
        return

    pdb = _server()._db()
    rows = pdb.get_wrong_matches()
    log_ids: list[int] = []
    for r in rows:
        if r.get("request_id") != rid:
            continue
        lid = r.get("download_log_id")
        if isinstance(lid, int):
            log_ids.append(lid)
    deleted = 0
    for log_id in log_ids:
        if _delete_wrong_match_row(pdb, log_id):
            deleted += 1
    h._json({"status": "ok", "request_id": rid, "deleted": deleted})


GET_ROUTES: dict[str, object] = {
    "/api/manual-import/scan": get_manual_import_scan,
    "/api/wrong-matches": get_wrong_matches,
}
POST_ROUTES: dict[str, object] = {
    "/api/manual-import/import": post_manual_import,
    "/api/wrong-matches/delete": post_wrong_match_delete,
    "/api/wrong-matches/delete-group": post_wrong_match_delete_group,
}
