"""Beets library route handlers — search, album detail, recent, delete."""

import re
from typing import assert_never


def _server():
    from web import server
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
    from web.library_album_detail_service import load_library_album_detail

    album_id = int(album_id_str)
    srv = _server()
    b = srv._beets_db()
    if not b:
        h._error("Beets DB not available")
        return
    detail = load_library_album_detail(
        library_lookup=b,
        pipeline_db=srv.db,
        album_id=album_id,
    )
    if not detail:
        h._error("Not found", 404)
        return
    h._json(detail.to_dict())


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
        release_id=str(body.get("release_id") or ""),
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

    if isinstance(result, DeleteBeetsFailure):
        h._error("Delete from beets failed", 500)
        return

    assert_never(result)


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
