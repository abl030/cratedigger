"""Beets library route handlers — album detail, delete."""

from typing import Literal, assert_never

from pydantic import BaseModel, Field

from web.routes._pydantic import parse_body
from web.routes._registry import RouteHandler, RouteRegistration, pattern_route, route
from web.routes._server_access import _server


def get_beets_album(
    h: RouteHandler, params: dict[str, list[str]], album_id_str: str,
) -> None:
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


def post_beets_delete(h: RouteHandler, body: dict[str, object]) -> None:
    from lib.destructive_release_service import (
        DeleteAlbumNotFound,
        DeleteIncomplete,
        DeleteImporterBusy,
        DeleteLockContended,
        DeletePipelinePurgeFailure,
        DeleteReleaseMismatch,
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
        expected_pipeline_id=req_body.pipeline_id,
        expected_release_id=req_body.release_id or None,
    )
    srv = _server()
    beets = srv._beets_db()
    if beets is None:
        h._error("Beets DB not available", 503)
        return
    result = delete_release_from_library(
        pipeline_db=srv._db(),
        beets_db=beets,
        request=request,
        beets_delete_fn=srv.beets_delete_fn,
        notify_fn=srv.delete_notify_fn,
    )

    if isinstance(result, DeleteSuccess):
        h._json({
            "status": "ok",
            "id": result.album_id,
            "album": result.album_name,
            "artist": result.artist_name,
            "deleted_files": result.deleted_files,
            "deleted_artifacts": result.deleted_artifacts,
            "pipeline_deleted": result.pipeline_deleted,
            "pipeline_id": result.deleted_pipeline_id,
            "preserved_paths": list(result.preserved_paths),
            "notifications": [
                {
                    "provider": item.provider,
                    "status": item.status,
                    "detail": item.detail,
                    "target": item.target,
                }
                for item in result.notifications
            ],
        })
        return

    if isinstance(result, DeleteAlbumNotFound):
        h._error("Album not found", 404)
        return

    if isinstance(result, DeleteReleaseMismatch):
        h._json({
            "error": "release_mismatch",
            "authoritative_release_id": result.authoritative_release_id,
            "authoritative_pipeline_id": result.authoritative_pipeline_id,
        }, status=422)
        return

    if isinstance(result, DeleteLockContended):
        h._json({
            "error": "destructive_operation_busy",
            "scope": result.scope,
        }, status=409)
        return

    if isinstance(result, DeleteImporterBusy):
        h._json({
            "error": "destructive_operation_busy",
            "pipeline_id": result.pipeline_request_id,
        }, status=409)
        return

    if isinstance(result, DeletePipelinePurgeFailure):
        h._json({
            "error": "pipeline_purge_failed",
            "status": "partial",
            "album_deleted": True,
            "id": result.album_id,
            "album": result.album_name,
            "artist": result.artist_name,
            "deleted_files": result.deleted_files,
            "deleted_artifacts": result.deleted_artifacts,
            "preserved_paths": list(result.preserved_paths),
            "pipeline_id": result.pipeline_request_id,
            "notifications": [
                {
                    "provider": item.provider,
                    "status": item.status,
                    "detail": item.detail,
                    "target": item.target,
                }
                for item in result.notifications
            ],
        }, status=500)
        return

    match result:
        case DeleteIncomplete():
            h._json({
                "error": "delete_incomplete",
                "id": result.album_id,
                "album": result.album_name,
                "artist": result.artist_name,
                "former_album_path": result.former_album_path,
                "pipeline_id": result.pipeline_request_id,
                "pipeline_status": result.pipeline_status,
                "acknowledgement_lost": result.acknowledgement_lost,
                "reason": result.reason,
                "detail": result.detail,
                "album_still_present": result.album_still_present,
                "deleted_files": result.deleted_files,
                "deleted_artifacts": result.deleted_artifacts,
                "remaining_owned_paths": list(result.remaining_owned_paths),
                "preserved_paths": list(result.preserved_paths),
            }, status=409)
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
        "Delete a server-resolved exact Beets album through the pinned Beets "
        "runtime, verify owned-artifact absence, optionally purge pipeline "
        "last, then notify Plex/Jellyfin. Requires confirm='DELETE'.",
        classified=True,
    ),
]
