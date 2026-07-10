"""Beets library route handlers — album detail, delete."""

from typing import Literal, assert_never

from pydantic import BaseModel, Field

from web.routes._pydantic import parse_body
from web.routes._registry import RouteRegistration, pattern_route, route
from web.routes._server_access import _server


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
        pipeline_db=srv._db(),
        album_id=album_id,
    )
    if not detail:
        h._error("Not found", 404)
        return
    h._json(detail.to_dict())


class BeetsDeleteRequest(BaseModel):
    """HTTP body for ``POST /api/beets/delete``.

    ``confirm`` must be the literal string ``"DELETE"`` to avoid
    accidental destructive calls (the frontend dialog wires this).
    """

    id: int = Field(gt=0)
    confirm: Literal["DELETE"]
    purge_pipeline: bool = False
    pipeline_id: int | None = None
    release_id: str = ""


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

    req_body = parse_body(h, body, BeetsDeleteRequest)
    if req_body is None:
        return
    request = DeleteRequest(
        album_id=req_body.id,
        purge_pipeline=req_body.purge_pipeline,
        pipeline_id=req_body.pipeline_id,
        release_id=req_body.release_id,
    )
    srv = _server()
    result = delete_release_from_library(
        beets_db_path=srv.beets_db_path,
        pipeline_db=srv._db(),
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


ROUTES: list[RouteRegistration] = [
    pattern_route(
        "GET", r"^/api/beets/album/(\d+)$", get_beets_album,
        "Beets album detail — full tracks + library / pipeline overlay.",
        classified=True,
    ),
    route(
        "POST", "/api/beets/delete", post_beets_delete,
        "Delete a beets album (DESTRUCTIVE — files removed); optional "
        "pipeline purge. Requires confirm='DELETE'.",
        classified=True,
    ),
]
