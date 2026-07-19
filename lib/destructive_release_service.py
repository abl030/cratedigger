"""Server-rooted authority for destructive release operations.

Both public operations deliberately derive the release identity from one
server-owned row, acquire the importer's advisory locks in canonical order,
and perform the final identity/job checks while those locks are held.  HTTP
and CLI callers are adapters only; they never select what is deleted.
"""

from __future__ import annotations

import json
import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Literal, Protocol, TypeAlias

from lib import transitions
from lib.audio_hash import AudioHashError, hash_audio_content
from lib.beets_album_op import BeetsOpFailure
from lib.beets_delete import (
    BeetsDeleteFailed,
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
    run_beets_delete,
)
from lib.library_delete_notifiers import DeleteNotification, notify_library_delete
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    BadAudioHashInput,
    DownloadLogOutcome,
    release_id_to_lock_key,
)
from lib.quality import resolve_user_requeue_override
from lib.release_cleanup import remove_and_reset_release
from lib.release_identity import ReleaseIdentity, normalize_release_id


log = logging.getLogger("cratedigger")


class SupportsReleaseLookupDB(Protocol):
    """Pipeline lookup rooted in one canonical release identity."""

    def get_request_by_release_id(
        self, release_id: object | None,
    ) -> dict[str, Any] | None: ...


class SupportsDestructivePipelineDB(transitions.TransitionsDB, Protocol):
    """Pipeline DB surface shared by both destructive services."""

    def get_request_by_release_id(
        self, release_id: object | None,
    ) -> dict[str, Any] | None: ...
    def get_active_import_job_for_request(self, request_id: int) -> object | None: ...
    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...
    def delete_request(self, request_id: int) -> None: ...
    def get_recent_successful_uploader(self, request_id: int) -> str | None: ...
    def add_bad_audio_hashes(
        self,
        request_id: int,
        reported_username: str | None,
        reason: str | None,
        hashes: list[BadAudioHashInput],
    ) -> int: ...
    def add_denylist(
        self, request_id: int, username: str, reason: str | None = None,
    ) -> None: ...
    def clear_on_disk_quality_fields(self, request_id: int) -> None: ...
    def log_download(
        self,
        request_id: int,
        soulseek_username: str | None = None,
        *,
        beets_detail: str | None = None,
        outcome: DownloadLogOutcome | None = None,
        validation_result: Any = None,
    ) -> int: ...


class SupportsDestructiveBeetsDB(Protocol):
    """Beets surface needed for exact-identity destructive actions."""

    @property
    def library_db_path(self) -> str: ...
    @property
    def library_root(self) -> str: ...
    def get_album_detail(self, album_id: int) -> dict[str, object] | None: ...
    def album_and_items_absent(self, album_id: int) -> bool: ...
    def get_item_paths(self, mb_release_id: str) -> list[tuple[int, str]]: ...
    def locate(self, release_id: str) -> object: ...


class FinalizeRequestFn(Protocol):
    def __call__(
        self,
        db: transitions.TransitionsDB,
        request_id: int,
        transition: transitions.RequestTransition,
    ) -> transitions.TransitionResult: ...


def _distinct_identities(
    *values: object | None,
) -> tuple[ReleaseIdentity, ...] | None:
    """Return distinct identities, or ``None`` for malformed server state."""
    identities: list[ReleaseIdentity] = []
    for value in values:
        normalized = normalize_release_id(value)
        if not normalized:
            continue
        identity = ReleaseIdentity.from_id(normalized)
        if identity is None:
            # A nonempty field is authority-bearing even when malformed.  It
            # cannot be silently treated as absent: importer code may still
            # use the raw truthy value to choose a different RELEASE lock.
            return None
        if identity not in identities:
            identities.append(identity)
    return tuple(identities)


def _request_identity(row: dict[str, Any]) -> ReleaseIdentity | None:
    identities = _distinct_identities(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )
    if identities is None or len(identities) != 1:
        return None
    return identities[0]


def _album_identity(row: dict[str, object]) -> ReleaseIdentity | None:
    """Return one unambiguous identity; dual-source rows fail closed."""
    identities = _distinct_identities(
        row.get("mb_albumid"),
        row.get("discogs_albumid"),
    )
    if identities is None or len(identities) != 1:
        return None
    return identities[0]


def resolve_pipeline_request(
    pipeline_db: SupportsReleaseLookupDB | None,
    *,
    release_id: str,
) -> dict[str, Any] | None:
    """Resolve the pipeline overlay from a server-derived release ID."""
    if pipeline_db is None or not normalize_release_id(release_id):
        return None
    return pipeline_db.get_request_by_release_id(release_id)


@dataclass(frozen=True)
class HashCaptureFailure:
    track_path: str | None
    reason: str


@dataclass(frozen=True)
class BanSourceRequest:
    request_id: int
    expected_release_id: str | None = None


@dataclass(frozen=True)
class BanSourceSuccess:
    request_id: int
    release_id: str
    request_status: Literal["wanted", "unsearchable"]
    username: str | None
    beets_removed: bool
    hashes_recorded: int
    cleanup_errors: tuple[BeetsOpFailure, ...]
    hash_capture_errors: tuple[HashCaptureFailure, ...]


@dataclass(frozen=True)
class BanSourceRequestNotFound:
    request_id: int


@dataclass(frozen=True)
class BanSourceReleaseMismatch:
    request_id: int
    expected_release_id: str | None
    authoritative_release_id: str | None


@dataclass(frozen=True)
class BanSourceLockContended:
    request_id: int
    scope: Literal["request", "release"]


@dataclass(frozen=True)
class BanSourceImporterBusy:
    request_id: int


@dataclass(frozen=True)
class BanSourceTransitionConflict:
    request_id: int
    conflict: transitions.TransitionConflict


BanSourceResult: TypeAlias = (
    BanSourceSuccess
    | BanSourceRequestNotFound
    | BanSourceReleaseMismatch
    | BanSourceLockContended
    | BanSourceImporterBusy
    | BanSourceTransitionConflict
)


def _identity_matches(expected: str | None, actual: ReleaseIdentity | None) -> bool:
    if expected is None:
        return actual is not None
    expected_identity = ReleaseIdentity.from_id(expected)
    return expected_identity is not None and expected_identity == actual


def _ban_source_locked(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: BanSourceRequest,
    identity: ReleaseIdentity,
    finalize_request_fn: FinalizeRequestFn,
) -> BanSourceResult:
    """Run every bad-rip effect while IMPORT and RELEASE are both held."""
    current = pipeline_db.get_request(request.request_id)
    current_identity = _request_identity(current) if current is not None else None
    if current is None:
        return BanSourceRequestNotFound(request.request_id)
    if current_identity != identity or not _identity_matches(
        request.expected_release_id, current_identity,
    ):
        return BanSourceReleaseMismatch(
            request.request_id,
            request.expected_release_id,
            current_identity.release_id if current_identity else None,
        )
    if pipeline_db.get_active_import_job_for_request(request.request_id) is not None:
        return BanSourceImporterBusy(request.request_id)

    # Establish the lifecycle transition before any hash, denylist, beets, or
    # audit mutation. A stale/replaced row is therefore a true zero-effect
    # conflict, and this service can never report destructive success after a
    # failed request CAS.
    quality = resolve_user_requeue_override(current.get("search_filetype_override"))
    fields: dict[str, object] = {"search_filetype_override": quality}
    if current.get("min_bitrate") is not None:
        fields["min_bitrate"] = current["min_bitrate"]
    current_status = str(current["status"])
    request_status: Literal["wanted", "unsearchable"] = (
        "unsearchable" if current_status == "unsearchable" else "wanted"
    )
    transition = (
        transitions.RequestTransition.to_unsearchable_fields(
            from_status=current_status,
            fields=fields,
        )
        if current_status == "unsearchable"
        else transitions.RequestTransition.to_wanted_fields(
            from_status=current_status,
            fields=fields,
        )
    )
    transition_result = finalize_request_fn(
        pipeline_db,
        request.request_id,
        transition,
    )
    if isinstance(transition_result, transitions.TransitionConflict):
        return BanSourceTransitionConflict(
            request.request_id, transition_result)

    release_id = identity.release_id
    reported_username = pipeline_db.get_recent_successful_uploader(request.request_id)
    reason = "manually banned via operator action"
    hash_failures: list[HashCaptureFailure] = []
    hashes: list[BadAudioHashInput] = []
    item_paths = beets_db.get_item_paths(release_id)
    if not item_paths:
        hash_failures.append(HashCaptureFailure(None, "no_tracks_in_beets"))
    else:
        for _item_id, raw_path in item_paths:
            track_path = Path(raw_path)
            audio_format = track_path.suffix.lstrip(".").lower()
            try:
                digest = hash_audio_content(track_path, audio_format)
            except AudioHashError as exc:
                hash_failures.append(HashCaptureFailure(str(track_path), str(exc)))
                continue
            except Exception as exc:  # noqa: BLE001 -- one bad track is partial
                hash_failures.append(HashCaptureFailure(
                    str(track_path), f"unexpected error: {exc}",
                ))
                continue
            hashes.append(BadAudioHashInput(digest, audio_format))

    hashes_recorded = pipeline_db.add_bad_audio_hashes(
        request.request_id,
        reported_username,
        reason,
        hashes,
    ) if hashes else 0
    if reported_username:
        pipeline_db.add_denylist(request.request_id, reported_username, reason)

    cleanup = remove_and_reset_release(
        beets_db=beets_db,  # type: ignore[arg-type] -- structural BeetsDB surface
        pipeline_db=pipeline_db,
        release_id=release_id,
        request_id=request.request_id,
    )

    cleanup_errors = tuple(cleanup.selector_failures)
    validation_result = json.dumps({
        "scenario": "curator_ban",
        "hashes_recorded": hashes_recorded,
        "denylisted_username": reported_username,
        "reason": reason,
        "cleanup_errors": [
            {
                "selector": failure.selector,
                "reason": failure.reason,
                "detail": failure.detail,
            }
            for failure in cleanup_errors
        ],
        "hash_capture_errors": [failure.__dict__ for failure in hash_failures],
    })
    detail = (
        f"Marked bad rip; {hashes_recorded} hashes captured"
        if hashes_recorded else "Marked bad rip (no tracks hashed)"
    )
    pipeline_db.log_download(
        request_id=request.request_id,
        soulseek_username=reported_username,
        outcome="curator_ban",
        beets_detail=detail,
        validation_result=validation_result,
    )
    return BanSourceSuccess(
        request_id=request.request_id,
        release_id=release_id,
        request_status=request_status,
        username=reported_username,
        beets_removed=cleanup.beets_removed,
        hashes_recorded=hashes_recorded,
        cleanup_errors=cleanup_errors,
        hash_capture_errors=tuple(hash_failures),
    )


def ban_source(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: BanSourceRequest,
    finalize_request_fn: FinalizeRequestFn = transitions.finalize_request,
) -> BanSourceResult:
    """Mark one request's exact server-owned release as a bad rip."""
    # IMPORT is always outer when both namespaces are held.
    # See docs/advisory-locks.md.
    with pipeline_db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_IMPORT, request.request_id,
    ) as request_acquired:
        if not request_acquired:
            return BanSourceLockContended(request.request_id, "request")

        row = pipeline_db.get_request(request.request_id)
        if row is None:
            return BanSourceRequestNotFound(request.request_id)
        identity = _request_identity(row)
        if not _identity_matches(request.expected_release_id, identity):
            return BanSourceReleaseMismatch(
                request.request_id,
                request.expected_release_id,
                identity.release_id if identity else None,
            )
        assert identity is not None

        with pipeline_db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(identity.release_id),
        ) as release_acquired:
            if not release_acquired:
                return BanSourceLockContended(request.request_id, "release")
            return _ban_source_locked(
                pipeline_db=pipeline_db,
                beets_db=beets_db,
                request=request,
                identity=identity,
                finalize_request_fn=finalize_request_fn,
            )


@dataclass(frozen=True)
class DeleteRequest:
    album_id: int
    purge_pipeline: bool = False
    expected_pipeline_id: int | None = None
    expected_release_id: str | None = None


@dataclass(frozen=True)
class DeleteSuccess:
    album_id: int
    album_name: str
    artist_name: str
    former_album_path: str
    deleted_files: int
    deleted_artifacts: int
    pipeline_deleted: bool
    deleted_pipeline_id: int | None
    preserved_paths: tuple[str, ...]
    notifications: tuple[DeleteNotification, ...] = ()


@dataclass(frozen=True)
class DeleteAlbumNotFound:
    album_id: int


@dataclass(frozen=True)
class DeleteReleaseMismatch:
    album_id: int
    expected_pipeline_id: int | None
    expected_release_id: str | None
    authoritative_pipeline_id: int | None
    authoritative_release_id: str | None


@dataclass(frozen=True)
class DeleteLockContended:
    album_id: int
    scope: Literal["request", "release"]


@dataclass(frozen=True)
class DeleteImporterBusy:
    album_id: int
    pipeline_request_id: int


@dataclass(frozen=True)
class DeletePipelinePurgeFailure:
    album_id: int
    pipeline_request_id: int
    album_name: str
    artist_name: str
    former_album_path: str
    deleted_files: int
    deleted_artifacts: int
    preserved_paths: tuple[str, ...]
    notifications: tuple[DeleteNotification, ...] = ()


@dataclass(frozen=True)
class DeleteIncomplete:
    album_id: int
    album_name: str
    artist_name: str
    former_album_path: str
    pipeline_request_id: int | None
    pipeline_status: str | None
    acknowledgement_lost: bool
    reason: str
    detail: str
    album_still_present: bool
    deleted_files: int | None
    deleted_artifacts: int | None
    remaining_owned_paths: tuple[str, ...]
    preserved_paths: tuple[str, ...]


DeleteResult: TypeAlias = (
    DeleteSuccess
    | DeleteAlbumNotFound
    | DeleteReleaseMismatch
    | DeleteLockContended
    | DeleteImporterBusy
    | DeletePipelinePurgeFailure
    | DeleteIncomplete
)


BeetsDeleteFn = Callable[[BeetsDeleteRequest], BeetsDeleteOutcome]
DeleteNotifyFn = Callable[[str], tuple[DeleteNotification, ...]]
_ACK_AMBIGUOUS_DELETE_REASONS = frozenset({"subprocess_error", "protocol_error"})


def _default_delete_notify(path: str) -> tuple[DeleteNotification, ...]:
    from lib.config import read_runtime_config
    return notify_library_delete(read_runtime_config(), path)


def _delete_mismatch(
    request: DeleteRequest,
    identity: ReleaseIdentity | None,
    pipeline_row: dict[str, Any] | None,
) -> DeleteReleaseMismatch:
    return DeleteReleaseMismatch(
        album_id=request.album_id,
        expected_pipeline_id=request.expected_pipeline_id,
        expected_release_id=request.expected_release_id,
        authoritative_pipeline_id=(
            int(pipeline_row["id"]) if pipeline_row is not None else None
        ),
        authoritative_release_id=identity.release_id if identity else None,
    )


def _delete_confirmations_match(
    request: DeleteRequest,
    identity: ReleaseIdentity | None,
    pipeline_row: dict[str, Any] | None,
) -> bool:
    if identity is None or not _identity_matches(request.expected_release_id, identity):
        return False
    if request.expected_pipeline_id is None:
        return True
    if pipeline_row is None or int(pipeline_row["id"]) != request.expected_pipeline_id:
        return False
    return _request_identity(pipeline_row) == identity


def _preflight_former_album_path(detail: dict[str, object]) -> str:
    """Retain the exact album directory for incomplete operator recovery."""
    direct = detail.get("path")
    if isinstance(direct, str) and direct:
        return direct
    tracks = detail.get("tracks")
    if isinstance(tracks, list):
        for track in tracks:
            if not isinstance(track, dict):
                continue
            path = track.get("path")
            if isinstance(path, str) and path:
                return str(Path(path).parent)
    artpath = detail.get("artpath")
    if isinstance(artpath, str) and artpath:
        return str(Path(artpath).parent)
    return ""


def _incomplete_delete_detail(
    *,
    failed: BeetsDeleteFailed,
    former_album_path: str,
    pipeline_row: dict[str, Any] | None,
) -> str:
    """Explain the manual boundary when the child acknowledgement is ambiguous."""
    if failed.reason not in _ACK_AMBIGUOUS_DELETE_REASONS:
        return failed.detail
    if pipeline_row is None:
        pipeline_context = "No authoritative pipeline request was present to purge."
    else:
        pipeline_context = (
            f"Pipeline request #{int(pipeline_row['id'])} "
            f"({str(pipeline_row.get('status') or 'unknown')}) was preserved."
        )
    path_context = (
        f" Inspect the exact former album path {former_album_path!r} before "
        "explicit recovery."
        if former_album_path
        else " Inspect the library manually before explicit recovery."
    )
    return (
        "Beets acknowledgement was lost; filesystem deletion is unconfirmed "
        "and Beets metadata may be gone. Do not assume files were deleted. "
        f"{pipeline_context}{path_context} Child detail: {failed.detail}"
    )


def _delete_incomplete(
    *,
    album_id: int,
    preflight_detail: dict[str, object],
    pipeline_row: dict[str, Any] | None,
    reason: str,
    detail: str,
    album_still_present: bool,
    deleted_files: int | None,
    deleted_artifacts: int | None,
    remaining_owned_paths: tuple[str, ...],
    preserved_paths: tuple[str, ...],
) -> DeleteIncomplete:
    return DeleteIncomplete(
        album_id=album_id,
        album_name=str(preflight_detail.get("album") or ""),
        artist_name=str(preflight_detail.get("artist") or ""),
        former_album_path=_preflight_former_album_path(preflight_detail),
        pipeline_request_id=(
            int(pipeline_row["id"]) if pipeline_row is not None else None
        ),
        pipeline_status=(
            str(pipeline_row.get("status") or "unknown")
            if pipeline_row is not None
            else None
        ),
        acknowledgement_lost=reason in _ACK_AMBIGUOUS_DELETE_REASONS,
        reason=reason,
        detail=detail,
        album_still_present=album_still_present,
        deleted_files=deleted_files,
        deleted_artifacts=deleted_artifacts,
        remaining_owned_paths=remaining_owned_paths,
        preserved_paths=preserved_paths,
    )


def _delete_under_release_lock(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: DeleteRequest,
    identity: ReleaseIdentity,
    pipeline_row: dict[str, Any] | None,
    beets_delete_fn: BeetsDeleteFn,
) -> DeleteResult:
    # Both identities are re-read after lock acquisition. This is the final
    # authority check before any DB, beets, or filesystem mutation.
    detail = beets_db.get_album_detail(request.album_id)
    current_identity = _album_identity(detail) if detail is not None else None
    current_pipeline = pipeline_db.get_request_by_release_id(identity.release_id)
    if detail is None:
        return DeleteAlbumNotFound(request.album_id)
    if current_identity != identity or not _delete_confirmations_match(
        request, current_identity, current_pipeline,
    ):
        return _delete_mismatch(request, current_identity, current_pipeline)
    if (pipeline_row is None) != (current_pipeline is None):
        return _delete_mismatch(request, current_identity, current_pipeline)
    if pipeline_row is not None and current_pipeline is not None:
        if int(pipeline_row["id"]) != int(current_pipeline["id"]):
            return _delete_mismatch(request, current_identity, current_pipeline)
        if pipeline_db.get_active_import_job_for_request(
            int(current_pipeline["id"]),
        ) is not None:
            return DeleteImporterBusy(request.album_id, int(current_pipeline["id"]))

    beets_outcome = beets_delete_fn(BeetsDeleteRequest(
        album_id=request.album_id,
        expected_release_id=identity.release_id,
        library_db_path=beets_db.library_db_path,
        library_root=beets_db.library_root,
    ))
    if isinstance(beets_outcome, BeetsDeleteFailed):
        album_still_present = (
            beets_db.get_album_detail(request.album_id) is not None
        )
        former_album_path = _preflight_former_album_path(detail)
        acknowledgement_lost = (
            beets_outcome.reason in _ACK_AMBIGUOUS_DELETE_REASONS
        )
        return _delete_incomplete(
            album_id=request.album_id,
            preflight_detail=detail,
            pipeline_row=current_pipeline,
            reason=beets_outcome.reason,
            detail=_incomplete_delete_detail(
                failed=beets_outcome,
                former_album_path=former_album_path,
                pipeline_row=current_pipeline,
            ),
            album_still_present=album_still_present,
            deleted_files=(
                None if acknowledgement_lost else beets_outcome.deleted_tracks
            ),
            deleted_artifacts=(
                None if acknowledgement_lost else beets_outcome.deleted_artifacts
            ),
            remaining_owned_paths=beets_outcome.remaining_owned_paths,
            preserved_paths=beets_outcome.preserved_paths,
        )
    if not beets_db.album_and_items_absent(request.album_id):
        return _delete_incomplete(
            album_id=request.album_id,
            preflight_detail=detail,
            pipeline_row=current_pipeline,
            reason="postcondition_failed",
            detail="exact Beets album or item metadata survived the delete operation",
            album_still_present=(
                beets_db.get_album_detail(request.album_id) is not None
            ),
            deleted_files=beets_outcome.deleted_tracks,
            deleted_artifacts=beets_outcome.deleted_artifacts,
            remaining_owned_paths=(),
            preserved_paths=beets_outcome.preserved_paths,
        )

    deleted_pipeline_id: int | None = None
    if request.purge_pipeline and current_pipeline is not None:
        deleted_pipeline_id = int(current_pipeline["id"])
        try:
            pipeline_db.delete_request(deleted_pipeline_id)
        except Exception:  # noqa: BLE001 -- typed operator outcome
            log.exception("Failed to purge pipeline request %s", deleted_pipeline_id)
            return DeletePipelinePurgeFailure(
                album_id=request.album_id,
                pipeline_request_id=deleted_pipeline_id,
                album_name=beets_outcome.album_name,
                artist_name=beets_outcome.artist_name,
                former_album_path=beets_outcome.former_album_path,
                deleted_files=beets_outcome.deleted_tracks,
                deleted_artifacts=beets_outcome.deleted_artifacts,
                preserved_paths=beets_outcome.preserved_paths,
            )

    return DeleteSuccess(
        album_id=request.album_id,
        album_name=beets_outcome.album_name,
        artist_name=beets_outcome.artist_name,
        former_album_path=beets_outcome.former_album_path,
        deleted_files=beets_outcome.deleted_tracks,
        deleted_artifacts=beets_outcome.deleted_artifacts,
        pipeline_deleted=deleted_pipeline_id is not None,
        deleted_pipeline_id=deleted_pipeline_id,
        preserved_paths=beets_outcome.preserved_paths,
    )


def _delete_with_release_lock(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: DeleteRequest,
    identity: ReleaseIdentity,
    pipeline_row: dict[str, Any] | None,
    beets_delete_fn: BeetsDeleteFn,
) -> DeleteResult:
    with pipeline_db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_RELEASE,
        release_id_to_lock_key(identity.release_id),
    ) as release_acquired:
        if not release_acquired:
            return DeleteLockContended(request.album_id, "release")
        return _delete_under_release_lock(
            pipeline_db=pipeline_db,
            beets_db=beets_db,
            request=request,
            identity=identity,
            pipeline_row=pipeline_row,
            beets_delete_fn=beets_delete_fn,
        )


def _notify_completed_delete(
    result: DeleteResult,
    notify_fn: DeleteNotifyFn,
) -> DeleteResult:
    if isinstance(result, (DeleteSuccess, DeletePipelinePurgeFailure)):
        try:
            notifications = notify_fn(result.former_album_path)
        except Exception as exc:  # noqa: BLE001 -- deletion is already committed
            log.exception("Post-delete media notification failed")
            detail = f"notification boundary failed: {type(exc).__name__}: {exc}"
            notifications = (
                DeleteNotification("plex", "warning", detail),
                DeleteNotification("jellyfin", "warning", detail),
            )
        return replace(result, notifications=notifications)
    return result


def delete_release_from_library(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: DeleteRequest,
    beets_delete_fn: BeetsDeleteFn | None = None,
    notify_fn: DeleteNotifyFn | None = None,
) -> DeleteResult:
    """Delete the exact album identified by the server-owned beets row."""
    delete_op = beets_delete_fn or run_beets_delete
    notifier = notify_fn or _default_delete_notify
    detail = beets_db.get_album_detail(request.album_id)
    if detail is None:
        return DeleteAlbumNotFound(request.album_id)
    identity = _album_identity(detail)
    pipeline_row = (
        pipeline_db.get_request_by_release_id(identity.release_id)
        if identity is not None else None
    )
    if not _delete_confirmations_match(request, identity, pipeline_row):
        return _delete_mismatch(request, identity, pipeline_row)
    assert identity is not None

    if pipeline_row is None:
        result = _delete_with_release_lock(
            pipeline_db=pipeline_db,
            beets_db=beets_db,
            request=request,
            identity=identity,
            pipeline_row=None,
            beets_delete_fn=delete_op,
        )
        return _notify_completed_delete(result, notifier)

    request_id = int(pipeline_row["id"])
    # IMPORT outer, RELEASE inner. See docs/advisory-locks.md.
    with pipeline_db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_IMPORT, request_id,
    ) as request_acquired:
        if not request_acquired:
            return DeleteLockContended(request.album_id, "request")
        current_pipeline = pipeline_db.get_request(request_id)
        if (
            current_pipeline is None
            or _request_identity(current_pipeline) != identity
            or not _delete_confirmations_match(
                request, identity, current_pipeline,
            )
        ):
            return _delete_mismatch(request, identity, current_pipeline)
        result = _delete_with_release_lock(
            pipeline_db=pipeline_db,
            beets_db=beets_db,
            request=request,
            identity=identity,
            pipeline_row=current_pipeline,
            beets_delete_fn=delete_op,
        )
    return _notify_completed_delete(result, notifier)
