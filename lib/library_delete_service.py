"""Typed service for the `/api/beets/delete` workflow.

Issue #153 moves the delete semantics out of `web/routes/library.py` so
the route becomes a thin request/response adapter around one explicit
service seam.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeAlias

from lib import beets_db

log = logging.getLogger("cratedigger-web")


class SupportsDeletePipelineDB(Protocol):
    """Minimal pipeline DB surface the delete workflow needs."""

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        ...

    def get_request_by_release_id(
        self,
        release_id: object | None,
    ) -> dict[str, Any] | None:
        ...

    def delete_request(self, request_id: int) -> None:
        ...


@dataclass(frozen=True)
class DeleteRequest:
    """Typed delete request extracted from the route body."""

    album_id: int
    purge_pipeline: bool = False
    pipeline_id: int | None = None
    release_id: str = ""


@dataclass(frozen=True)
class DeleteSuccess:
    """Delete succeeded in beets, disk cleanup, and optional purge."""

    album_id: int
    album_name: str
    artist_name: str
    deleted_files: int
    pipeline_deleted: bool
    deleted_pipeline_id: int | None


@dataclass(frozen=True)
class DeletePreflightFailure:
    """Delete aborted before any destructive mutation."""

    album_id: int
    reason: Literal["beets_db_unavailable", "album_not_found"]


@dataclass(frozen=True)
class DeletePipelinePurgeFailure:
    """Pipeline purge failed, so beets delete was intentionally skipped."""

    album_id: int
    pipeline_request_id: int


@dataclass(frozen=True)
class DeleteBeetsFailure:
    """Beets delete failed without a prior pipeline purge."""

    album_id: int


@dataclass(frozen=True)
class DeletePostPurgeBeetsFailure:
    """Beets delete failed after the pipeline row was already purged."""

    album_id: int
    deleted_pipeline_id: int


DeleteResult: TypeAlias = (
    DeleteSuccess
    | DeletePreflightFailure
    | DeletePipelinePurgeFailure
    | DeleteBeetsFailure
    | DeletePostPurgeBeetsFailure
)


def _resolve_pipeline_request(
    pipeline_db: SupportsDeletePipelineDB | None,
    request: DeleteRequest,
) -> dict[str, Any] | None:
    """Resolve the pipeline row to purge using the shared exact-release seam."""
    if pipeline_db is None:
        return None

    if request.pipeline_id is not None:
        req = pipeline_db.get_request(int(request.pipeline_id))
        if req:
            return req

    release_id = request.release_id.strip()
    if not release_id:
        return None
    return pipeline_db.get_request_by_release_id(release_id)


def _preflight_delete(
    beets_db_path: str | None,
    album_id: int,
) -> DeletePreflightFailure | None:
    """Verify the beets DB is present and the album still exists."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return DeletePreflightFailure(
            album_id=album_id,
            reason="beets_db_unavailable",
        )

    try:
        with beets_db.BeetsDB(beets_db_path) as beets:
            if not beets.get_album_detail(album_id):
                return DeletePreflightFailure(
                    album_id=album_id,
                    reason="album_not_found",
                )
    except FileNotFoundError:
        return DeletePreflightFailure(
            album_id=album_id,
            reason="beets_db_unavailable",
        )

    return None


def _delete_album_files(file_paths: list[str]) -> int:
    """Delete album files, then remove the album directory if it is empty."""
    album_dir = os.path.dirname(file_paths[0]) if file_paths else None
    deleted_files = 0
    for path in file_paths:
        if os.path.isfile(path):
            os.remove(path)
            deleted_files += 1
    if album_dir and os.path.isdir(album_dir):
        try:
            os.rmdir(album_dir)
        except OSError:
            pass
    return deleted_files


def delete_release_from_library(
    *,
    beets_db_path: str | None,
    pipeline_db: SupportsDeletePipelineDB | None,
    request: DeleteRequest,
) -> DeleteResult:
    """Delete a beets album plus optional pipeline state.

    The service owns:
    - beets preflight existence check
    - pipeline request resolution through the exact-release seam
    - purge-before-delete ordering
    - targeted partial-success classification
    - file deletion and empty-directory cleanup
    """
    preflight = _preflight_delete(beets_db_path, request.album_id)
    if preflight is not None:
        return preflight

    deleted_pipeline_id: int | None = None
    if request.purge_pipeline:
        req = _resolve_pipeline_request(pipeline_db, request)
        if req:
            deleted_pipeline_id = int(req["id"])
            try:
                assert pipeline_db is not None
                pipeline_db.delete_request(deleted_pipeline_id)
            except Exception:
                log.exception(
                    "Failed to purge pipeline request %s before beets delete",
                    deleted_pipeline_id,
                )
                return DeletePipelinePurgeFailure(
                    album_id=request.album_id,
                    pipeline_request_id=deleted_pipeline_id,
                )

    assert beets_db_path is not None
    try:
        album_name, artist_name, file_paths = beets_db.BeetsDB.delete_album(
            beets_db_path,
            request.album_id,
        )
        deleted_files = _delete_album_files(file_paths)
    except ValueError:
        return DeletePreflightFailure(
            album_id=request.album_id,
            reason="album_not_found",
        )
    except Exception:
        if deleted_pipeline_id is not None:
            log.exception(
                "Beets delete failed after purging pipeline request %s for album %s",
                deleted_pipeline_id,
                request.album_id,
            )
            return DeletePostPurgeBeetsFailure(
                album_id=request.album_id,
                deleted_pipeline_id=deleted_pipeline_id,
            )
        log.exception("Beets delete failed for album %s", request.album_id)
        return DeleteBeetsFailure(album_id=request.album_id)

    return DeleteSuccess(
        album_id=request.album_id,
        album_name=album_name,
        artist_name=artist_name,
        deleted_files=deleted_files,
        pipeline_deleted=deleted_pipeline_id is not None,
        deleted_pipeline_id=deleted_pipeline_id,
    )
