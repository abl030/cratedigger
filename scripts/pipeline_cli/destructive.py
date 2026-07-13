"""CLI adapters for server-rooted destructive release operations."""

from __future__ import annotations

import argparse
import json
import sys

from lib.beets_db import BeetsDB, DEFAULT_BEETS_DB
from lib.destructive_release_service import (
    BanSourceImporterBusy,
    BanSourceLockContended,
    BanSourceReleaseMismatch,
    BanSourceRequest,
    BanSourceRequestNotFound,
    BanSourceSuccess,
    DeleteAlbumNotFound,
    DeleteBeetsFailure,
    DeleteImporterBusy,
    DeleteLockContended,
    DeletePipelinePurgeFailure,
    DeletePostPurgeBeetsFailure,
    DeleteReleaseMismatch,
    DeleteRequest,
    DeleteSuccess,
    ban_source,
    delete_release_from_library,
)


def _beets_library_root() -> str:
    from lib.config import read_runtime_config
    return read_runtime_config().beets_directory


def _open_beets(path: str) -> BeetsDB:
    return BeetsDB(path, library_root=_beets_library_root())


def cmd_ban_source(db, args) -> int:
    """Ban one request's exact release and requeue it."""
    try:
        beets = _open_beets(args.beets_db)
    except FileNotFoundError as exc:
        print(json.dumps({"error": "beets_db_unavailable", "detail": str(exc)}))
        return 5
    with beets:
        result = ban_source(
            pipeline_db=db,
            beets_db=beets,
            request=BanSourceRequest(
                request_id=int(args.request_id),
                expected_release_id=args.release_id,
            ),
        )
    if isinstance(result, BanSourceSuccess):
        print(json.dumps({
            "status": "ok",
            "request_id": result.request_id,
            "release_id": result.release_id,
            "username": result.username,
            "beets_removed": result.beets_removed,
            "hashes_recorded": result.hashes_recorded,
            "cleanup_errors": len(result.cleanup_errors),
            "hash_capture_errors": len(result.hash_capture_errors),
        }))
        return 0
    if isinstance(result, BanSourceRequestNotFound):
        print(json.dumps({"error": "request_not_found"}))
        return 2
    if isinstance(result, BanSourceReleaseMismatch):
        print(json.dumps({
            "error": "release_mismatch",
            "authoritative_release_id": result.authoritative_release_id,
        }))
        return 3
    if isinstance(result, BanSourceLockContended):
        print(json.dumps({
            "error": "destructive_operation_busy",
            "scope": result.scope,
        }))
        return 4
    if isinstance(result, BanSourceImporterBusy):
        print(json.dumps({"error": "destructive_operation_busy"}))
        return 4
    raise AssertionError(f"Unhandled ban-source result: {result!r}")


def cmd_library_delete(db, args) -> int:
    """Delete one exact beets album with optional pipeline purge."""
    try:
        beets = _open_beets(args.beets_db)
    except FileNotFoundError as exc:
        print(json.dumps({"error": "beets_db_unavailable", "detail": str(exc)}))
        return 5
    with beets:
        result = delete_release_from_library(
            pipeline_db=db,
            beets_db=beets,
            request=DeleteRequest(
                album_id=int(args.album_id),
                purge_pipeline=bool(args.purge_pipeline),
                expected_pipeline_id=args.pipeline_id,
                expected_release_id=args.release_id,
            ),
        )
    if isinstance(result, DeleteSuccess):
        print(json.dumps({
            "status": "ok",
            "id": result.album_id,
            "album": result.album_name,
            "artist": result.artist_name,
            "deleted_files": result.deleted_files,
            "pipeline_deleted": result.pipeline_deleted,
            "pipeline_id": result.deleted_pipeline_id,
        }))
        return 0
    if isinstance(result, DeleteAlbumNotFound):
        print(json.dumps({"error": "album_not_found"}))
        return 2
    if isinstance(result, DeleteReleaseMismatch):
        print(json.dumps({
            "error": "release_mismatch",
            "authoritative_release_id": result.authoritative_release_id,
            "authoritative_pipeline_id": result.authoritative_pipeline_id,
        }))
        return 3
    if isinstance(result, DeleteLockContended):
        print(json.dumps({
            "error": "destructive_operation_busy",
            "scope": result.scope,
        }))
        return 4
    if isinstance(result, DeleteImporterBusy):
        print(json.dumps({
            "error": "destructive_operation_busy",
            "pipeline_id": result.pipeline_request_id,
        }))
        return 4
    if isinstance(result, DeletePipelinePurgeFailure):
        print(json.dumps({
            "error": "pipeline_purge_failed",
            "pipeline_id": result.pipeline_request_id,
        }), file=sys.stderr)
        return 1
    if isinstance(result, (DeletePostPurgeBeetsFailure, DeleteBeetsFailure)):
        print(json.dumps({"error": "destructive_operation_failed"}), file=sys.stderr)
        return 1
    raise AssertionError(f"Unhandled library-delete result: {result!r}")


def add_destructive_subparsers(sub: argparse._SubParsersAction) -> None:
    ban = sub.add_parser(
        "ban-source",
        help="Mark a request's server-resolved exact release as a bad rip, "
             "remove it from beets, and requeue it.",
    )
    ban.add_argument("request_id", type=int)
    ban.add_argument("--confirm", choices=("BAN",), required=True)
    ban.add_argument(
        "--release-id",
        default=None,
        help="Optional confirmation-only release ID; mismatch is rejected.",
    )
    ban.add_argument("--beets-db", default=DEFAULT_BEETS_DB)

    delete = sub.add_parser(
        "library-delete",
        help="Delete one exact beets album by server-owned album ID.",
    )
    delete.add_argument("album_id", type=int)
    delete.add_argument("--confirm", choices=("DELETE",), required=True)
    delete.add_argument("--purge-pipeline", action="store_true")
    delete.add_argument(
        "--pipeline-id",
        type=int,
        default=None,
        help="Optional confirmation-only pipeline request ID.",
    )
    delete.add_argument(
        "--release-id",
        default=None,
        help="Optional confirmation-only release ID.",
    )
    delete.add_argument("--beets-db", default=DEFAULT_BEETS_DB)
