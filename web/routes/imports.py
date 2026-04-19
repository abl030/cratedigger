"""Manual import route handlers — scan, import, wrong matches."""

import json
import shutil

from lib.manual_import import (
    scan_complete_folder,
    match_folders_to_requests,
    ImportRequest,
)
from lib.util import resolve_failed_path


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


def _is_in_beets(
    row: dict[str, object],
    beets_info: dict[str, dict[str, object]],
) -> bool:
    """Check if the album exists in the beets library."""
    mbid = row.get("mb_release_id")
    if isinstance(mbid, str) and mbid and mbid in beets_info:
        return True
    artist = row.get("artist_name")
    album = row.get("album_title")
    if not isinstance(artist, str) or not isinstance(album, str):
        return False
    return _server().check_beets_by_artist_album(artist, album) is not None


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

    from lib.import_dispatch import dispatch_import_from_db
    outcome = dispatch_import_from_db(
        srv._db(), request_id=int(request_id), failed_path=path,
        force=False, outcome_label="manual_import",
    )

    h._json({
        "status": "ok" if outcome.success else "error",
        "message": outcome.message,
        "request_id": request_id,
        "artist": req["artist_name"],
        "album": req["album_title"],
    })


def get_wrong_matches(h, params: dict[str, list[str]]) -> None:
    """Group wrong-match rejections by release (issue #113).

    Each ``album_requests`` row becomes one group; every rejected
    ``download_log`` entry with an on-disk ``failed_path`` becomes one entry
    inside its group. Groups with zero surviving entries are dropped so the
    UI only shows actionable work.
    """
    srv = _server()
    pdb = srv._db()
    rows = pdb.get_wrong_matches()
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
            group = {
                "request_id": request_id,
                "artist": row["artist_name"],
                "album": row["album_title"],
                "mb_release_id": row.get("mb_release_id"),
                "in_library": _is_in_beets(row, beets_info),
                "pending_count": 0,
                "entries": [],
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
        })
        group["pending_count"] = len(entries_list)

    h._json({"groups": [groups[rid] for rid in order]})


def post_wrong_match_delete(h, body: dict) -> None:
    """Delete files for a wrong match and clear its failed_path."""
    log_id = body.get("download_log_id")
    if not log_id:
        h._error("Missing download_log_id")
        return

    pdb = _server()._db()
    entry = pdb.get_download_log_entry(int(log_id))
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    vr = _parse_validation_result(entry.get("validation_result"))
    failed_path_raw = vr.get("failed_path")
    failed_path = failed_path_raw if isinstance(failed_path_raw, str) else ""
    resolved_path = resolve_failed_path(failed_path)

    # Delete files from disk if they exist
    if resolved_path is not None:
        shutil.rmtree(resolved_path)

    # Clear failed_path so it stops appearing in wrong matches list
    pdb.clear_wrong_match_path(int(log_id))

    h._json({"status": "ok", "download_log_id": log_id})


GET_ROUTES: dict[str, object] = {
    "/api/manual-import/scan": get_manual_import_scan,
    "/api/wrong-matches": get_wrong_matches,
}
POST_ROUTES: dict[str, object] = {
    "/api/manual-import/import": post_manual_import,
    "/api/wrong-matches/delete": post_wrong_match_delete,
}
