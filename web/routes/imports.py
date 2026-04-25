"""Manual import route handlers — scan, import, wrong matches."""

import json
import shutil
import msgspec

from lib.manual_import import (
    scan_complete_folder,
    match_folders_to_requests,
    ImportRequest,
)
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    force_import_dedupe_key,
    force_import_payload,
    manual_import_dedupe_key,
    manual_import_payload,
)
from lib.util import resolve_failed_path
from lib.wrong_matches import dismiss_wrong_match_source
from lib.wrong_match_triage import triage_wrong_match
from lib.import_preview import (
    ImportPreviewValues,
    preview_import_from_download_log,
    preview_import_from_path,
    preview_import_from_values,
)
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


def _threshold_milli(value: object) -> int:
    try:
        parsed = int(value) if isinstance(value, (str, int, float)) else 180
    except (TypeError, ValueError):
        parsed = 180
    return max(0, min(parsed, 999))


def _distance_value(vr: dict[str, object]) -> float | None:
    raw = vr.get("distance")
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(str(raw))
    except (TypeError, ValueError):
        return None


def _is_green_distance(vr: dict[str, object], threshold_milli: int) -> bool:
    distance = _distance_value(vr)
    return distance is not None and distance <= threshold_milli / 1000


def _validation_local_items(vr: dict[str, object]) -> list[dict[str, object]]:
    """Collect local item payloads from every place beets validation stores them."""
    items: list[dict[str, object]] = []

    def add_dicts(raw: object) -> None:
        if isinstance(raw, list):
            items.extend(
                item for item in raw
                if isinstance(item, dict)
            )

    add_dicts(vr.get("items"))

    raw_candidates = vr.get("candidates", [])
    if isinstance(raw_candidates, list):
        for candidate in raw_candidates:
            if not isinstance(candidate, dict):
                continue
            add_dicts(candidate.get("extra_items"))
            raw_mapping = candidate.get("mapping", [])
            if isinstance(raw_mapping, list):
                for match in raw_mapping:
                    if not isinstance(match, dict):
                        continue
                    item = match.get("item")
                    if isinstance(item, dict):
                        items.append(item)

    return items


def _item_looks_flac(item: dict[str, object]) -> bool:
    for key in ("format", "filetype", "actual_filetype", "slskd_filetype",
                "original_filetype"):
        value = item.get(key)
        if isinstance(value, str) and "flac" in value.lower():
            return True

    for key in ("path", "filename", "name"):
        value = item.get(key)
        if not isinstance(value, str):
            continue
        normalized = value.lower().split("?", 1)[0].split("#", 1)[0]
        if normalized.endswith(".flac"):
            return True

    return False


def _validation_download_is_non_flac(vr: dict[str, object]) -> bool:
    items = _validation_local_items(vr)
    return bool(items) and not any(_item_looks_flac(item) for item in items)


def _transparent_non_flac_wrong_match_targets(pdb, srv) -> list[dict[str, object]]:
    rows = pdb.get_wrong_matches()
    mbids = [
        mbid for row in rows
        for mbid in [row.get("mb_release_id")]
        if isinstance(mbid, str) and mbid
    ]
    beets_info = srv.check_beets_library_detail(mbids) if mbids else {}
    groups: dict[int, dict[str, object]] = {}

    for row in rows:
        request_id = row.get("request_id")
        log_id = row.get("download_log_id")
        if not isinstance(request_id, int) or not isinstance(log_id, int):
            continue

        vr = _parse_validation_result(row.get("validation_result"))
        failed_path_raw = vr.get("failed_path")
        failed_path = failed_path_raw if isinstance(failed_path_raw, str) else ""
        if resolve_failed_path(failed_path) is None:
            continue

        group = groups.get(request_id)
        if group is None:
            quality = _quality_summary(
                row,
                beets_info,
                _row_presence(row, beets_info),
            )
            group = {
                "request_id": request_id,
                "artist": row.get("artist_name"),
                "album": row.get("album_title"),
                "quality_rank": quality.get("quality_rank"),
                "download_log_ids": [],
                "all_non_flac": True,
            }
            groups[request_id] = group

        log_ids = group["download_log_ids"]
        assert isinstance(log_ids, list)
        log_ids.append(log_id)
        if not _validation_download_is_non_flac(vr):
            group["all_non_flac"] = False

    return [
        group for group in groups.values()
        if group.get("quality_rank") == "transparent"
        and group.get("all_non_flac")
        and group.get("download_log_ids")
    ]


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
        log_id = row.get("download_log_id")
        import_job = (
            _serialize_import_job(active_jobs_by_log_id[log_id])
            if isinstance(log_id, int) and log_id in active_jobs_by_log_id
            else None
        )
        entries_list.append({
            "download_log_id": log_id,
            "failed_path": resolved_path or failed_path,
            "files_exist": files_exist,
            "distance": vr.get("distance"),
            "scenario": vr.get("scenario"),
            "detail": vr.get("detail"),
            "soulseek_username": row.get("soulseek_username")
                or vr.get("soulseek_username"),
            "candidate": target,
            "local_items": vr.get("items", []),
            "import_job": import_job,
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


def post_wrong_match_delete_transparent_non_flac(h, body: dict) -> None:
    """Bulk-delete non-FLAC wrong matches when the exact library copy is transparent."""
    srv = _server()
    pdb = srv._db()
    targets = _transparent_non_flac_wrong_match_targets(pdb, srv)

    deleted = 0
    deleted_request_ids: list[int] = []
    skipped: list[dict[str, object]] = []
    for target in targets:
        request_deleted = 0
        log_ids = target.get("download_log_ids")
        if not isinstance(log_ids, list):
            continue
        for log_id in log_ids:
            if not isinstance(log_id, int):
                continue
            if _delete_wrong_match_row(pdb, log_id):
                deleted += 1
                request_deleted += 1
            else:
                skipped.append({
                    "download_log_id": log_id,
                    "reason": "delete_failed",
                })
        request_id = target.get("request_id")
        if request_deleted and isinstance(request_id, int):
            deleted_request_ids.append(request_id)

    h._json({
        "status": "ok",
        "groups_deleted": len(deleted_request_ids),
        "deleted": deleted,
        "deleted_request_ids": deleted_request_ids,
        "eligible_groups": len(targets),
        "skipped": skipped,
    })


def post_wrong_match_converge(h, body: dict) -> None:
    """Queue acceptable candidates and delete the rest for the release."""
    request_id = body.get("request_id")
    if request_id is None:
        h._error("Missing request_id")
        return
    try:
        rid = int(request_id)
    except (TypeError, ValueError):
        h._error("request_id must be an integer")
        return

    threshold_milli = _threshold_milli(body.get("threshold_milli"))
    # Converge is intentionally a one-click cleanup workflow: green rows are
    # queued, and non-green rows for the same release are removed immediately.
    # Keep accepting the legacy field from older clients, but do not let it
    # leave high-distance leftovers behind.
    delete_unmatched = True

    srv = _server()
    pdb = srv._db()
    req = pdb.get_request(rid)
    if not req:
        h._error(f"Request {rid} not found", 404)
        return

    selected: list[dict[str, object]] = []
    unmatched: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    jobs: list[dict[str, object]] = []
    unmatched_log_ids: list[int] = []
    deduped = 0
    dismissed = 0
    deleted = 0
    remaining = 0

    for row in pdb.get_wrong_matches():
        if row.get("request_id") != rid:
            continue
        lid = row.get("download_log_id")
        if not isinstance(lid, int):
            skipped.append({"download_log_id": None, "reason": "missing_log_id"})
            remaining += 1
            continue

        vr = _parse_validation_result(row.get("validation_result"))
        failed_path_raw = vr.get("failed_path")
        failed_path = failed_path_raw if isinstance(failed_path_raw, str) else ""
        distance = _distance_value(vr)
        green = _is_green_distance(vr, threshold_milli)

        if green:
            resolved_path = resolve_failed_path(failed_path)
            if resolved_path is None:
                skipped.append({
                    "download_log_id": lid,
                    "reason": "files_missing",
                })
                remaining += 1
                continue
            source_username_raw = (
                row.get("soulseek_username")
                or vr.get("soulseek_username")
            )
            source_username = (
                str(source_username_raw)
                if source_username_raw is not None else None
            )
            job = pdb.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=rid,
                dedupe_key=force_import_dedupe_key(lid),
                payload=force_import_payload(
                    download_log_id=lid,
                    failed_path=resolved_path,
                    source_username=source_username,
                ),
                message=(
                    f"Force import queued for "
                    f"{req['artist_name']} - {req['album_title']}"
                ),
            )
            if getattr(job, "deduped", False):
                deduped += 1
            jobs.append(_serialize_import_job(job))
            dismiss_result = dismiss_wrong_match_source(
                pdb,
                lid,
                failed_path_hint=resolved_path,
            )
            dismissed += int(dismiss_result.cleared_rows)
            selected.append({
                "download_log_id": lid,
                "distance": distance,
                "job_id": job.id,
                "deduped": bool(getattr(job, "deduped", False)),
            })
            continue

        unmatched.append({
            "download_log_id": lid,
            "distance": distance,
        })
        unmatched_log_ids.append(lid)

    if selected:
        for lid in unmatched_log_ids:
            if _delete_wrong_match_row(pdb, lid):
                deleted += 1
            else:
                skipped.append({
                    "download_log_id": lid,
                    "reason": "delete_failed",
                })
                remaining += 1
    else:
        for _lid in unmatched_log_ids:
            remaining += 1

    h._json({
        "status": "ok",
        "request_id": rid,
        "threshold_milli": threshold_milli,
        "threshold": threshold_milli / 1000,
        "delete_unmatched": delete_unmatched,
        "selected_count": len(selected),
        "unmatched_count": len(unmatched),
        "queued": len(jobs),
        "deduped": deduped,
        "dismissed": dismissed,
        "deleted": deleted,
        "remaining": remaining,
        "group_empty": remaining == 0,
        "selected": selected,
        "unmatched": unmatched,
        "skipped": skipped,
        "jobs": jobs,
    }, status=202)


def _preview_values_from_body(body: dict) -> ImportPreviewValues:
    raw_values = body.get("values")
    if raw_values is None and body.get("values_json"):
        raw_values = json.loads(str(body["values_json"]))
    if raw_values is None:
        raw_values = body
    if not isinstance(raw_values, dict):
        raise ValueError("values must be an object")
    return msgspec.convert(raw_values, type=ImportPreviewValues)


def post_import_preview(h, body: dict) -> None:
    """Preview either typed values, a request/path, or a download-log row."""
    has_values = any(k in body for k in ("values", "values_json", "is_flac", "min_bitrate"))
    has_download_log = body.get("download_log_id") is not None
    has_path = body.get("request_id") is not None and body.get("path")
    mode_count = sum(1 for value in (has_values, has_download_log, has_path) if value)
    if mode_count != 1:
        h._error("Provide exactly one preview mode: values, download_log_id, or request_id+path")
        return

    try:
        if has_values:
            from web.routes.pipeline import _runtime_rank_config
            preview = preview_import_from_values(
                _preview_values_from_body(body),
                cfg=_runtime_rank_config(),
            )
        elif has_download_log:
            preview = preview_import_from_download_log(
                _server()._db(),
                int(body["download_log_id"]),
            )
        else:
            preview = preview_import_from_path(
                _server()._db(),
                request_id=int(body["request_id"]),
                path=str(body["path"]),
                force=bool(body.get("force", True)),
                source_username=(
                    str(body["source_username"])
                    if body.get("source_username") is not None else None
                ),
            )
    except (ValueError, TypeError, msgspec.ValidationError) as exc:
        h._error(f"Invalid preview input: {exc}")
        return
    h._json(preview.to_dict())


def post_wrong_match_triage(h, body: dict) -> None:
    raw_id = body.get("download_log_id")
    if raw_id is None:
        h._error("Missing download_log_id")
        return
    try:
        result = triage_wrong_match(_server()._db(), int(raw_id))
    except (ValueError, TypeError) as exc:
        h._error(f"Invalid triage input: {exc}")
        return
    h._json(result.to_dict())


GET_ROUTES: dict[str, object] = {
    "/api/manual-import/scan": get_manual_import_scan,
    "/api/wrong-matches": get_wrong_matches,
}
POST_ROUTES: dict[str, object] = {
    "/api/manual-import/import": post_manual_import,
    "/api/import-preview": post_import_preview,
    "/api/wrong-matches/converge": post_wrong_match_converge,
    "/api/wrong-matches/triage": post_wrong_match_triage,
    "/api/wrong-matches/delete": post_wrong_match_delete,
    "/api/wrong-matches/delete-group": post_wrong_match_delete_group,
    "/api/wrong-matches/delete-transparent-non-flac": post_wrong_match_delete_transparent_non_flac,
}
