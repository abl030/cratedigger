"""Beets library route handlers — search, album detail, recent, delete."""

import re


def _server():
    from web import server
    return server


def _find_pipeline_request_for_release(
    pipeline_id: int | None,
    release_id: str,
) -> dict | None:
    """Resolve one pipeline row through the shared library lookup seam."""
    from lib.library_delete_service import resolve_pipeline_request

    srv = _server()
    return resolve_pipeline_request(
        srv.db,
        pipeline_id=pipeline_id,
        release_id=release_id,
    )


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
        req = _find_pipeline_request_for_release(None, str(mb_id))
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
    from lib.library_delete_service import (
        DeleteBeetsFailure,
        DeleteBeetsDbUnavailable,
        DeleteAlbumNotFound,
        DeletePipelinePurgeFailure,
        DeletePostPurgeBeetsFailure,
        DeleteRequest,
        DeleteSuccess,
        delete_release_from_library,
    )

    album_id = body.get("id")
    confirm = body.get("confirm")
    if not album_id:
        h._error("Missing id")
        return
    if confirm != "DELETE":
        h._error("Must send confirm: 'DELETE'")
        return
    request = DeleteRequest(
        album_id=int(album_id),
        purge_pipeline=bool(body.get("purge_pipeline")),
        pipeline_id=(
            int(body["pipeline_id"])
            if body.get("pipeline_id") is not None
            else None
        ),
        release_id=str(body.get("release_id") or "").strip(),
    )
    srv = _server()
    result = delete_release_from_library(
        beets_db_path=srv.beets_db_path,
        pipeline_db=srv.db,
        request=request,
    )

    if isinstance(result, DeleteSuccess):
        h._json({
            "status": "ok",
            "id": result.album_id,
            "album": result.album_name,
            "artist": result.artist_name,
            "deleted_files": result.deleted_files,
            "pipeline_deleted": result.pipeline_deleted,
            "pipeline_id": result.deleted_pipeline_id,
        })
        return

    if isinstance(result, DeleteAlbumNotFound):
        h._error("Album not found", 404)
        return

    if isinstance(result, DeleteBeetsDbUnavailable):
        h._error("Beets DB not available")
        return

    if isinstance(result, DeletePipelinePurgeFailure):
        h._error("Failed to purge pipeline request", 500)
        return

    if isinstance(result, DeletePostPurgeBeetsFailure):
        h._error(
            "Pipeline request was removed, but delete from beets failed; "
            "check logs and disk state",
            500,
        )
        return

    assert isinstance(result, DeleteBeetsFailure)
    h._error("Delete from beets failed", 500)


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
