"""CLI adapters for server-rooted destructive release operations."""

from __future__ import annotations

import argparse
import json
import sys

import msgspec

from lib.beets_db import BeetsDB, open_beets_db
from lib.destructive_release_service import (
    BanSourceCleanupIncomplete,
    BanSourceImporterBusy,
    BanSourceLockContended,
    BanSourceReleaseMismatch,
    BanSourceRequest,
    BanSourceRequestNotFound,
    BanSourceSuccess,
    BanSourceTransitionConflict,
    DeleteAlbumNotFound,
    DeleteIncomplete,
    DeleteImporterBusy,
    DeleteLockContended,
    DeletePipelinePurgeFailure,
    DeleteReleaseMismatch,
    DeleteRequest,
    DeleteSuccess,
    BeetsDeleteFn,
    DeleteNotifyFn,
    ban_source,
    delete_release_from_library,
)


class _BanSourceArgs(msgspec.Struct, frozen=True):
    request_id: int
    beets_db: str | None = None
    beets_directory: str | None = None
    release_id: str | None = None


class _LibraryDeleteArgs(msgspec.Struct, frozen=True):
    album_id: int
    beets_db: str | None = None
    beets_directory: str | None = None
    purge_pipeline: bool = False
    pipeline_id: int | None = None
    release_id: str | None = None


def _open_beets(path: str | None, library_root: str | None) -> BeetsDB:
    return open_beets_db(db_path=path, library_root=library_root)


def cmd_ban_source(db, args: object) -> int:
    """Ban an exact source; preserve unsearchable or requeue as wanted."""
    typed_args = msgspec.convert(vars(args), type=_BanSourceArgs)
    try:
        beets = _open_beets(
            typed_args.beets_db,
            typed_args.beets_directory,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(json.dumps({"error": "beets_db_unavailable", "detail": str(exc)}))
        return 5
    with beets:
        result = ban_source(
            pipeline_db=db,
            beets_db=beets,
            request=BanSourceRequest(
                request_id=typed_args.request_id,
                expected_release_id=typed_args.release_id,
            ),
        )
    if isinstance(result, BanSourceSuccess):
        print(json.dumps({
            "status": "ok",
            "request_id": result.request_id,
            "release_id": result.release_id,
            "request_status": result.request_status,
            "username": result.username,
            "beets_removed": result.beets_removed,
            "hashes_recorded": result.hashes_recorded,
            "cleanup_errors": len(result.cleanup_errors),
            "hash_capture_errors": len(result.hash_capture_errors),
        }))
        return 0
    if isinstance(result, BanSourceCleanupIncomplete):
        print(json.dumps({
            "error": "cleanup_incomplete",
            "status": "partial",
            "request_id": result.request_id,
            "release_id": result.release_id,
            "request_status": result.request_status,
            "username": result.username,
            "beets_removed": False,
            "hashes_recorded": result.hashes_recorded,
            "cleanup_errors": [
                {
                    "selector": failure.selector,
                    "reason": failure.reason,
                    "detail": failure.detail,
                }
                for failure in result.cleanup_errors
            ],
            "hash_capture_errors": [
                {
                    "track_path": failure.track_path,
                    "reason": failure.reason,
                }
                for failure in result.hash_capture_errors
            ],
        }))
        return 4
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
    match result:
        case BanSourceTransitionConflict():
            print(json.dumps({
                "error": "transition_conflict",
                "reason": result.conflict.kind.value,
                "expected_status": result.conflict.expected_status,
                "actual_status": result.conflict.actual_status,
                "target_status": result.conflict.target_status,
            }))
            return 4
    raise AssertionError(f"Unhandled ban-source result: {result!r}")


def cmd_library_delete(
    db,
    args: object,
    *,
    beets_delete_fn: BeetsDeleteFn | None = None,
    notify_fn: DeleteNotifyFn | None = None,
) -> int:
    """Delete one exact beets album with optional pipeline purge."""
    typed_args = msgspec.convert(vars(args), type=_LibraryDeleteArgs)
    try:
        beets = _open_beets(
            typed_args.beets_db,
            typed_args.beets_directory,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(json.dumps({"error": "beets_db_unavailable", "detail": str(exc)}))
        return 5
    with beets:
        result = delete_release_from_library(
            pipeline_db=db,
            beets_db=beets,
            request=DeleteRequest(
                album_id=typed_args.album_id,
                purge_pipeline=typed_args.purge_pipeline,
                expected_pipeline_id=typed_args.pipeline_id,
                expected_release_id=typed_args.release_id,
            ),
            beets_delete_fn=beets_delete_fn,
            notify_fn=notify_fn,
        )
    if isinstance(result, DeleteSuccess):
        print(json.dumps({
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
        }), file=sys.stderr)
        return 1
    match result:
        case DeleteIncomplete():
            print(json.dumps({
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
            }), file=sys.stderr)
            return 4
    raise AssertionError(f"Unhandled library-delete result: {result!r}")


def add_destructive_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    ban = sub.add_parser(
        "ban-source",
        help="Mark a request's server-resolved exact release as a bad rip, "
             "remove it from beets, preserve an unsearchable stop, or "
             "otherwise requeue it as wanted.",
    )
    ban.add_argument("request_id", type=int)
    ban.add_argument("--confirm", choices=("BAN",), required=True)
    ban.add_argument(
        "--release-id",
        default=None,
        help="Optional confirmation-only release ID; mismatch is rejected.",
    )
    ban.add_argument(
        "--beets-db", default=None,
        help="Explicit Beets SQLite override; requires --beets-directory.",
    )
    ban.add_argument(
        "--beets-directory", default=None,
        help="Library root paired with --beets-db.",
    )

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
    delete.add_argument(
        "--beets-db", default=None,
        help="Explicit Beets SQLite override; requires --beets-directory.",
    )
    delete.add_argument(
        "--beets-directory", default=None,
        help="Library root paired with --beets-db.",
    )
