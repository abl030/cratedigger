"""Manual import route handlers — scan and import."""

import json, os, re, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _server():
    from web import server  # type: ignore[import-not-found]
    return server


def get_manual_import_scan(h, params: dict[str, list[str]]) -> None:
    from lib.manual_import import scan_complete_folder, match_folders_to_requests, ImportRequest

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
    from lib.manual_import import run_manual_import

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
    mbid = req.mb_release_id
    if not mbid:
        h._error("Request has no MusicBrainz release ID")
        return

    import_one_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "harness", "import_one.py")

    result = run_manual_import(
        request_id=int(request_id),
        mb_release_id=mbid,
        path=path,
        import_one_path=import_one_path,
        override_min_bitrate=req.min_bitrate,
    )

    # Log to download_log
    srv._db().log_download(
        request_id=int(request_id),
        outcome="manual_import" if result.success else "failed",
        import_result=result.import_result_json,
        staged_path=path,
    )

    # Update status on success
    if result.success:
        update_fields: dict[str, object] = {}
        if result.import_result_json:
            try:
                update_fields = srv._extract_import_fields(json.loads(result.import_result_json))
            except (json.JSONDecodeError, TypeError):
                pass
        srv._db().update_status(int(request_id), "imported", **update_fields)

    h._json({
        "status": "ok" if result.success else "error",
        "message": result.message,
        "exit_code": result.exit_code,
        "request_id": request_id,
        "artist": req.artist_name,
        "album": req.album_title,
    })


GET_ROUTES: dict[str, object] = {
    "/api/manual-import/scan": get_manual_import_scan,
}
POST_ROUTES: dict[str, object] = {
    "/api/manual-import/import": post_manual_import,
}
