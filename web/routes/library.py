"""Beets library route handlers — search, album detail, recent, delete."""

import json, os, re, sys, sqlite3, shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _server():
    from web import server  # type: ignore[import-not-found]
    return server


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
    # Remap 'albumartist' to 'artist' for API compatibility
    result: dict[str, object] = {
        "id": detail["id"], "album": detail["album"],
        "artist": detail["albumartist"],
        "year": detail["year"], "mb_albumid": detail["mb_albumid"],
        "type": detail["type"], "label": detail["label"],
        "country": detail["country"], "artpath": detail["artpath"],
        "added": detail["added"], "tracks": detail["tracks"],
        "path": detail["path"],
    }
    # Include pipeline download history if available
    mb_id = detail.get("mb_albumid")
    srv = _server()
    if mb_id and srv.db:
        req = srv._db().get_request_by_mb_release_id(mb_id)
        if req:
            history = srv._db().get_download_history(req["id"])
            result["pipeline_id"] = req["id"]
            result["pipeline_status"] = req["status"]
            result["pipeline_source"] = req["source"]
            result["pipeline_min_bitrate"] = req.get("min_bitrate")
            result["upgrade_queued"] = (
                req["status"] == "wanted" and bool(req.get("quality_override"))
            )
            from classify import classify_log_entry as _clf, LogEntry as _LE
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
    conn = sqlite3.connect(srv.beets_db_path)
    # Get album path from items before deleting
    items = conn.execute(
        "SELECT path FROM items WHERE album_id = ?", (int(album_id),)
    ).fetchall()
    album_row = conn.execute(
        "SELECT album, albumartist FROM albums WHERE id = ?", (int(album_id),)
    ).fetchone()
    if not album_row:
        conn.close()
        h._error("Album not found", 404)
        return
    album_name = album_row[0]
    artist_name = album_row[1]
    # Get album directory from first track
    file_paths = [r[0].decode("utf-8", errors="replace") if isinstance(r[0], bytes) else r[0] for r in items]
    album_dir = os.path.dirname(file_paths[0]) if file_paths else None
    # Delete from beets DB
    conn.execute("DELETE FROM items WHERE album_id = ?", (int(album_id),))
    conn.execute("DELETE FROM albums WHERE id = ?", (int(album_id),))
    conn.commit()
    conn.close()
    # Delete files from disk
    deleted_files = 0
    if album_dir and os.path.isdir(album_dir):
        shutil.rmtree(album_dir)
        deleted_files = len(file_paths)
    h._json({
        "status": "ok", "id": album_id,
        "album": album_name, "artist": artist_name,
        "deleted_files": deleted_files,
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
