"""Beets library route handlers — search, album detail, recent, delete."""

import logging
import os
import re

log = logging.getLogger("cratedigger-web")


def _server():
    from web import server
    return server


def _find_pipeline_request_for_release(
    pipeline_id: int | None,
    release_id: str,
) -> dict | None:
    """Resolve the pipeline request the UI wants purged, if any.

    Prefer the explicit ``pipeline_id`` from the frontend when present;
    fall back to the release ID so stale/missing row overlays do not turn
    the delete into a ghost imported row again.
    """
    srv = _server()
    if not srv.db:
        return None

    db = srv._db()
    if pipeline_id is not None:
        req = db.get_request(int(pipeline_id))
        if req:
            return req

    release_id = release_id.strip()
    if not release_id:
        return None

    req = db.get_request_by_mb_release_id(release_id)
    if req:
        return req

    if release_id.isdigit():
        req = db.get_request_by_discogs_release_id(release_id)
        if req:
            return req

    return None


def get_beets_search(h, params: dict[str, list[str]]) -> None:
    q = params.get("q", [""])[0].strip()
    if not q or len(q) < 2:
        h._error("Query too short")
        return
    b = _server()._beets_db()
    if not b:
        h._error("Beets DB not available")
        return
    albums = b.search_albums(q)
    _server()._enrich_with_pipeline(albums)
    h._json({"albums": albums})


def get_beets_album(h, params: dict[str, list[str]], album_id_str: str) -> None:
    album_id = int(album_id_str)
    b = _server()._beets_db()
    if not b:
        h._error("Beets DB not available")
        return
    detail = b.get_album_detail(album_id)
    if not detail:
        h._error("Not found", 404)
        return
    result: dict[str, object] = dict(detail)
    # Include pipeline download history if available
    mb_id = detail.get("mb_albumid")
    srv = _server()
    if mb_id and srv.db:
        req = srv._db().get_request_by_mb_release_id(mb_id)
        if req:
            history = srv._db().get_download_history(req["id"])
            result["pipeline_id"] = req["id"]
            result["pipeline_status"] = req["status"]
            result["pipeline_source"] = req.get("source")
            result["pipeline_min_bitrate"] = req.get("min_bitrate")
            result["search_filetype_override"] = req.get("search_filetype_override")
            result["target_format"] = req.get("target_format")
            result["upgrade_queued"] = (
                req["status"] == "wanted" and bool(req.get("search_filetype_override") or req.get("target_format"))
            )
            from web.classify import classify_log_entry as _clf, LogEntry as _LE
            dh = []
            for h_entry in history:
                he = _LE.from_row(h_entry)
                hi = he.to_json_dict()
                _c = _clf(he)
                hi["verdict"] = _c.verdict
                hi["downloaded_label"] = _c.downloaded_label
                dh.append(hi)
            result["download_history"] = dh
    h._json(result)


def get_beets_recent(h, params: dict[str, list[str]]) -> None:
    b = _server()._beets_db()
    if not b:
        h._error("Beets DB not available")
        return
    albums = b.get_recent()
    _server()._enrich_with_pipeline(albums)
    h._json({"albums": albums})


def post_beets_delete(h, body: dict) -> None:
    album_id = body.get("id")
    confirm = body.get("confirm")
    purge_pipeline = bool(body.get("purge_pipeline"))
    pipeline_id = body.get("pipeline_id")
    release_id = str(body.get("release_id") or "").strip()
    if not album_id:
        h._error("Missing id")
        return
    if confirm != "DELETE":
        h._error("Must send confirm: 'DELETE'")
        return
    srv = _server()
    if not srv.beets_db_path or not os.path.exists(srv.beets_db_path):
        h._error("Beets DB not available")
        return
    from lib.beets_db import BeetsDB
    try:
        with BeetsDB(srv.beets_db_path) as beets:
            if not beets.get_album_detail(int(album_id)):
                h._error("Album not found", 404)
                return
    except FileNotFoundError:
        h._error("Beets DB not available")
        return

    deleted_pipeline_id = None
    if purge_pipeline:
        req = _find_pipeline_request_for_release(
            int(pipeline_id) if pipeline_id is not None else None,
            release_id,
        )
        if req:
            try:
                srv._db().delete_request(int(req["id"]))
            except Exception:
                log.exception(
                    "Failed to purge pipeline request %s before beets delete",
                    req.get("id"),
                )
                h._error("Failed to purge pipeline request", 500)
                return
            deleted_pipeline_id = int(req["id"])

    try:
        album_name, artist_name, file_paths = BeetsDB.delete_album(srv.beets_db_path, int(album_id))
    except ValueError:
        h._error("Album not found", 404)
        return
    album_dir = os.path.dirname(file_paths[0]) if file_paths else None
    # Delete individual files from disk (safe — won't destroy shared directories)
    deleted_files = 0
    for path in file_paths:
        if os.path.isfile(path):
            os.remove(path)
            deleted_files += 1
    # Remove directory only if now empty (other albums may share it)
    if album_dir and os.path.isdir(album_dir):
        try:
            os.rmdir(album_dir)
        except OSError:
            pass  # not empty — other albums' files still there

    h._json({
        "status": "ok", "id": album_id,
        "album": album_name, "artist": artist_name,
        "deleted_files": deleted_files,
        "pipeline_deleted": deleted_pipeline_id is not None,
        "pipeline_id": deleted_pipeline_id,
    })


GET_ROUTES: dict[str, object] = {
    "/api/beets/search": get_beets_search,
    "/api/beets/recent": get_beets_recent,
}
GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
    (re.compile(r"^/api/beets/album/(\d+)$"), get_beets_album),
]
POST_ROUTES: dict[str, object] = {
    "/api/beets/delete": post_beets_delete,
}
