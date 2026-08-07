"""Server-rooted authority for destructive release operations.

Both public operations deliberately derive the release identity from one
server-owned row, acquire the importer's advisory locks in canonical order,
and perform the final identity/job checks while those locks are held.  HTTP
and CLI callers are adapters only; they never select what is deleted.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow

from lib import transitions
from lib.audio_hash import AudioHashError, hash_audio_content
from lib.beets_db import (
    CurrentBeetsAmbiguityReason,
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailed,
    BeetsDeleteFailureReason,
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
    run_beets_delete,
)
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    OwnerSessionIdentity,
    OwnerSessionProbe,
)
from lib.library_delete_notifiers import DeleteNotification, notify_library_delete
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    BadAudioHashInput,
    DownloadLogOutcome,
)
from lib.pipeline_db._core import AdvisoryLockSessionLost, OwnerSessionLost
from lib.quality import resolve_user_requeue_override
from lib.release_association_locks import release_identity_locks
from lib.release_identity import ReleaseIdentity
from lib.request_identity import acceptable_identities, resolve_current_for_request

log = logging.getLogger("cratedigger")


class SupportsDestructivePipelineDB(transitions.TransitionsDB, Protocol):
    """Pipeline DB surface shared by both destructive services."""

    def get_request_by_release_id(
        self, release_id: object | None,
    ) -> AlbumRequestRow | None: ...
    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...
    def _pin_owner_session(
        self, token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...
    def _probe_owner_session(
        self,
        identity: OwnerSessionIdentity,
        *,
        deadline_seconds: float = 0.75,
    ) -> OwnerSessionProbe: ...
    def delete_request(self, request_id: int) -> bool: ...
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
    def resolve_current_release(
        self, identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...
    def resolve_current_releases(
        self, identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...


class FinalizeRequestFn(Protocol):
    def __call__(
        self,
        db: transitions.TransitionsDB,
        request_id: int,
        transition: transitions.RequestTransition,
    ) -> transitions.TransitionResult: ...


BeetsDeleteFn = Callable[[BeetsDeleteRequest], BeetsDeleteOutcome]


def _request_identity(row: Mapping[str, Any]) -> ReleaseIdentity | None:
    return ReleaseIdentity.from_strict_fields(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )


def _album_identity(row: dict[str, object]) -> ReleaseIdentity | None:
    """Return one unambiguous identity; dual-source rows fail closed."""
    return ReleaseIdentity.from_strict_fields(
        row.get("mb_albumid"),
        row.get("discogs_albumid"),
    )


@dataclass(frozen=True)
class HashCaptureFailure:
    track_path: str | None
    reason: str


@dataclass(frozen=True)
class BanSourceCleanupFailure:
    """Exact pinned-delete failure surfaced to CLI, API, and audit."""

    selector: str
    reason: BeetsDeleteFailureReason
    detail: str


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
    cleanup_errors: tuple[BanSourceCleanupFailure, ...]
    hash_capture_errors: tuple[HashCaptureFailure, ...]


@dataclass(frozen=True)
class BanSourceCleanupIncomplete:
    """Bad-source evidence committed but the exact Beets release remains."""

    request_id: int
    release_id: str
    request_status: Literal["wanted", "unsearchable"]
    username: str | None
    beets_removed: Literal[False]
    hashes_recorded: int
    cleanup_errors: tuple[BanSourceCleanupFailure, ...]
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


@dataclass(frozen=True)
class BanSourceBeetsAmbiguous:
    """Current Beets cardinality cannot authorize any bad-rip mutation."""

    request_id: int
    release_id: str
    album_ids: tuple[int, ...]
    reason: CurrentBeetsAmbiguityReason


@dataclass
class _BanSourceEffectReceipt:
    """In-memory boundary between zero-effect contention and cleanup tail."""

    started: bool = False
    release_id: str = ""
    request_status: Literal["wanted", "unsearchable"] = "wanted"
    username: str | None = None
    hashes_recorded: int = 0
    hash_capture_errors: tuple[HashCaptureFailure, ...] = ()


type BanSourceResult = (
    BanSourceSuccess
    | BanSourceCleanupIncomplete
    | BanSourceRequestNotFound
    | BanSourceReleaseMismatch
    | BanSourceLockContended
    | BanSourceImporterBusy
    | BanSourceTransitionConflict
    | BanSourceBeetsAmbiguous
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
    beets_delete_fn: BeetsDeleteFn,
    cancellation_token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
    effect_receipt: _BanSourceEffectReceipt,
) -> BanSourceResult:
    """Run every bad-rip effect while IMPORT and RELEASE are both held."""
    cancellation_token.raise_if_cancelled()
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
    processing_locked = transitions.processing_locked_conflict(
        current,
        request.request_id,
        "ban_source",
        expected_status=str(current["status"]),
    )
    if processing_locked is not None:
        return BanSourceTransitionConflict(
            request.request_id,
            processing_locked,
        )

    # Resolve over the request's identity union (#1059). Bad Rip is the
    # sharpest case for this: on a Missing resolution it does NOT abort —
    # it denylists the uploader and requeues while removing nothing and
    # recording no bad-rip hashes. After a merge + mbsync retag the album is
    # on disk under the survivor, an acquisition-only resolve says Missing,
    # and the operator gets a half-done Bad Rip. This PR is what makes that
    # click likely, because the library panel beside the button now
    # correctly says the album IS installed.
    current_beets = resolve_current_for_request(beets_db, current)
    if current_beets is None:
        # Unreachable: the identity check above already proved the row has
        # one exact acceptable identity. Kept as a typed refusal rather than
        # an acquisition-only fallback, because "authority not established"
        # must never be laundered into a resolution on a destructive path
        # (#1059 invariant 6).
        return BanSourceReleaseMismatch(
            request.request_id,
            request.expected_release_id,
            current_identity.release_id if current_identity else None,
        )
    if isinstance(current_beets, CurrentBeetsAmbiguous):
        return BanSourceBeetsAmbiguous(
            request_id=request.request_id,
            release_id=identity.release_id,
            album_ids=current_beets.album_ids,
            reason=current_beets.reason,
        )

    # Establish the lifecycle transition before any hash, denylist, beets, or
    # audit mutation. A stale/replaced row is therefore a true zero-effect
    # conflict, and this service can never report destructive success after a
    # failed request CAS.
    quality = resolve_user_requeue_override(current.get("search_filetype_override"))
    fields: dict[str, object] = {
        "search_filetype_override": quality,
        "priority_started_at": datetime.now(UTC),
    }
    if current.get("min_bitrate") is not None:
        fields["min_bitrate"] = current["min_bitrate"]
    current_status = str(current["status"])
    request_status: Literal["wanted", "unsearchable"] = (
        "unsearchable" if current_status == "unsearchable" else "wanted"
    )
    effect_receipt.release_id = identity.release_id
    effect_receipt.request_status = request_status
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
    effect_receipt.started = True
    transition_result = finalize_request_fn(
        pipeline_db,
        request.request_id,
        transition,
    )
    if isinstance(transition_result, transitions.TransitionConflict):
        return BanSourceTransitionConflict(
            request.request_id, transition_result)
    cancellation_token.raise_if_cancelled()
    if not pipeline_db._probe_owner_session(owner_session_identity).live:
        raise AdvisoryLockSessionLost(
            "Ban Source lost owner session after lifecycle transition"
        )

    release_id = identity.release_id
    reported_username = pipeline_db.get_recent_successful_uploader(request.request_id)
    reason = "manually banned via operator action"
    hash_failures: list[HashCaptureFailure] = []
    hashes: list[BadAudioHashInput] = []
    current_items = (
        current_beets.items
        if isinstance(current_beets, CurrentBeetsUnique)
        else ()
    )
    if not current_items:
        hash_failures.append(HashCaptureFailure(None, "no_tracks_in_beets"))
    else:
        for item in current_items:
            track_path = Path(item.path)
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
    effect_receipt.hashes_recorded = hashes_recorded
    effect_receipt.hash_capture_errors = tuple(hash_failures)
    cancellation_token.raise_if_cancelled()
    if not pipeline_db._probe_owner_session(owner_session_identity).live:
        raise AdvisoryLockSessionLost("Ban Source lost owner session after hash capture")
    if reported_username:
        pipeline_db.add_denylist(request.request_id, reported_username, reason)
    effect_receipt.username = reported_username

    cleanup_errors: tuple[BanSourceCleanupFailure, ...] = ()
    beets_removed = False
    cleanup_absent_after = isinstance(current_beets, CurrentBeetsMissing)
    if isinstance(current_beets, CurrentBeetsUnique):
        delete_request = BeetsDeleteRequest(
            album_id=current_beets.album_id,
            # FILED, not requested (#1059). The delete child re-reads the
            # album's own mb_albumid and refuses any mismatch, so passing
            # the acquisition id here makes the removal a silent no-op on
            # exactly the merged albums the union exists to reach — and Bad
            # Rip has already committed the denylist and requeue by now.
            expected_release_id=current_beets.filed_identity.release_id,
            library_db_path=beets_db.library_db_path,
            library_root=beets_db.library_root,
        )
        if beets_delete_fn is run_beets_delete:
            delete_outcome = run_beets_delete(
                delete_request,
                cancellation_token=cancellation_token,
                owner_session_probe=lambda: bool(
                    pipeline_db._probe_owner_session(
                        owner_session_identity,
                    ).live,
                ),
            )
        else:
            delete_outcome = beets_delete_fn(delete_request)
        cancellation_token.raise_if_cancelled()
        if not pipeline_db._probe_owner_session(owner_session_identity).live:
            raise AdvisoryLockSessionLost(
                "Ban Source lost owner session after Beets acknowledgement"
            )
        if isinstance(delete_outcome, BeetsDeleteCompleted):
            beets_removed = True
            cleanup_absent_after = True
        else:
            cleanup_errors = (BanSourceCleanupFailure(
                selector=f"id:{current_beets.album_id}",
                reason=delete_outcome.reason,
                detail=delete_outcome.detail,
            ),)
    if cleanup_absent_after:
        cancellation_token.raise_if_cancelled()
        pipeline_db.clear_on_disk_quality_fields(request.request_id)

    validation_result = json.dumps({
        "scenario": "curator_ban",
        "hashes_recorded": hashes_recorded,
        "denylisted_username": reported_username,
        "reason": reason,
        "cleanup_absent_after": cleanup_absent_after,
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
    cancellation_token.raise_if_cancelled()
    if not pipeline_db._probe_owner_session(owner_session_identity).live:
        raise AdvisoryLockSessionLost("Ban Source lost owner session before audit")
    pipeline_db.log_download(
        request_id=request.request_id,
        soulseek_username=reported_username,
        outcome="curator_ban",
        beets_detail=detail,
        validation_result=validation_result,
    )
    if not cleanup_absent_after:
        return BanSourceCleanupIncomplete(
            request_id=request.request_id,
            release_id=release_id,
            request_status=request_status,
            username=reported_username,
            beets_removed=False,
            hashes_recorded=hashes_recorded,
            cleanup_errors=cleanup_errors,
            hash_capture_errors=tuple(hash_failures),
        )
    return BanSourceSuccess(
        request_id=request.request_id,
        release_id=release_id,
        request_status=request_status,
        username=reported_username,
        beets_removed=beets_removed,
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
    beets_delete_fn: BeetsDeleteFn | None = None,
) -> BanSourceResult:
    """Mark one request's exact server-owned release as a bad rip."""
    # IMPORT is always outer when both namespaces are held.
    # See docs/advisory-locks.md.
    token = CancellationToken()
    effect_receipt = _BanSourceEffectReceipt()
    try:
        with (
            pipeline_db._pin_owner_session(token) as owner_session_identity,
            pipeline_db.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT, request.request_id,
            ) as request_acquired,
        ):
            if not request_acquired:
                return BanSourceLockContended(request.request_id, "request")

            row = pipeline_db.get_request(request.request_id)
            if row is None:
                return BanSourceRequestNotFound(request.request_id)
            processing_locked = transitions.processing_locked_conflict(
                row,
                request.request_id,
                "ban_source",
                expected_status=str(row["status"]),
            )
            if processing_locked is not None:
                return BanSourceTransitionConflict(
                    request.request_id,
                    processing_locked,
                )
            identity = _request_identity(row)
            if not _identity_matches(request.expected_release_id, identity):
                return BanSourceReleaseMismatch(
                    request.request_id,
                    request.expected_release_id,
                    identity.release_id if identity else None,
                )
            assert identity is not None

            identities = acceptable_identities(row)
            with release_identity_locks(pipeline_db, identities) as release_locks:
                if not release_locks.acquired:
                    return BanSourceLockContended(request.request_id, "release")
                return _ban_source_locked(
                    pipeline_db=pipeline_db,
                    beets_db=beets_db,
                    request=request,
                    identity=identity,
                    finalize_request_fn=finalize_request_fn,
                    beets_delete_fn=beets_delete_fn or run_beets_delete,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                    effect_receipt=effect_receipt,
                )
    except (AdvisoryLockSessionLost, OwnerSessionLost, ExecutionCancelled):
        if effect_receipt.started:
            return BanSourceCleanupIncomplete(
                request_id=request.request_id,
                release_id=effect_receipt.release_id,
                request_status=effect_receipt.request_status,
                username=effect_receipt.username,
                beets_removed=False,
                hashes_recorded=effect_receipt.hashes_recorded,
                cleanup_errors=(BanSourceCleanupFailure(
                    selector="owner-session",
                    reason="subprocess_error",
                    detail="authority was lost after Bad Rip effects began; retry cleanup",
                ),),
                hash_capture_errors=effect_receipt.hash_capture_errors,
            )
        return BanSourceLockContended(request.request_id, "request")


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
class DeleteBeetsAmbiguous:
    """Current exact identity exists but cannot authorize one album."""

    album_id: int
    release_id: str
    album_ids: tuple[int, ...]
    reason: CurrentBeetsAmbiguityReason


@dataclass(frozen=True)
class DeleteBeetsUnavailable:
    """A request-row union response omitted its authoritative result."""

    album_id: int
    release_id: str
    reason: Literal["request_union_authority_unavailable"]


@dataclass(frozen=True)
class DeleteAlbumAuthorityMismatch:
    """The requested album PK is not the fresh exact-identity album PK."""

    album_id: int
    authoritative_album_id: int
    release_id: str


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


@dataclass
class _DeleteEffectReceipt:
    """Records whether a library mutation may have crossed its boundary."""

    started: bool = False
    preflight_detail: dict[str, object] | None = None
    former_album_path: str = ""
    pipeline_row: Mapping[str, object] | None = None


type DeleteResult = (
    DeleteSuccess
    | DeleteAlbumNotFound
    | DeleteReleaseMismatch
    | DeleteBeetsAmbiguous
    | DeleteBeetsUnavailable
    | DeleteAlbumAuthorityMismatch
    | DeleteLockContended
    | DeleteImporterBusy
    | DeletePipelinePurgeFailure
    | DeleteIncomplete
    | transitions.TransitionConflict
)


DeleteNotifyFn = Callable[[str], tuple[DeleteNotification, ...]]
_ACK_AMBIGUOUS_DELETE_REASONS = frozenset({"subprocess_error", "protocol_error"})


def _default_delete_notify(path: str) -> tuple[DeleteNotification, ...]:
    from lib.config import read_runtime_config
    return notify_library_delete(read_runtime_config(), path)


def _delete_mismatch(
    request: DeleteRequest,
    identity: ReleaseIdentity | None,
    pipeline_row: Mapping[str, Any] | None,
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
    pipeline_row: Mapping[str, Any] | None,
) -> bool:
    if identity is None or not _identity_matches(request.expected_release_id, identity):
        return False
    if (
        pipeline_row is not None
        and identity not in acceptable_identities(pipeline_row)
    ):
        return False
    if request.expected_pipeline_id is None:
        return True
    return (
        pipeline_row is not None
        and int(pipeline_row["id"]) == request.expected_pipeline_id
    )


def _incomplete_delete_detail(
    *,
    failed: BeetsDeleteFailed,
    former_album_path: str,
    pipeline_row: Mapping[str, Any] | None,
) -> str:
    """Explain the manual boundary when the child acknowledgement is ambiguous."""
    if failed.reason not in _ACK_AMBIGUOUS_DELETE_REASONS:
        return failed.detail
    if pipeline_row is None:
        pipeline_context = "No authoritative pipeline request was present to purge."
    else:
        pipeline_context = (
            f"Pipeline request #{int(pipeline_row['id'])} "
            f"({pipeline_row.get('status') or 'unknown'!s}) was preserved."
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
    former_album_path: str,
    pipeline_row: Mapping[str, Any] | None,
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
        former_album_path=former_album_path,
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
    pipeline_row: Mapping[str, Any] | None,
    preflight_detail: dict[str, object],
    beets_delete_fn: BeetsDeleteFn,
    cancellation_token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
    effect_receipt: _DeleteEffectReceipt,
) -> DeleteResult:
    cancellation_token.raise_if_cancelled()
    current_pipeline = (
        pipeline_db.get_request(int(pipeline_row["id"]))
        if pipeline_row is not None
        else pipeline_db.get_request_by_release_id(identity.release_id)
    )
    if not _delete_confirmations_match(
        request, identity, current_pipeline,
    ):
        return _delete_mismatch(request, identity, current_pipeline)
    if (pipeline_row is None) != (current_pipeline is None):
        return _delete_mismatch(request, identity, current_pipeline)
    if pipeline_row is not None and current_pipeline is not None:
        if int(pipeline_row["id"]) != int(current_pipeline["id"]):
            return _delete_mismatch(request, identity, current_pipeline)
        request_id = int(current_pipeline["id"])
        processing_locked = transitions.processing_locked_conflict(
            current_pipeline,
            request_id,
            "library_delete",
            expected_status=str(current_pipeline["status"]),
        )
        if processing_locked is not None:
            return processing_locked

    # This joined exact-identity snapshot is the final Beets authority before
    # the pinned mutation. Missing is not an invitation to delete by the stale
    # requested PK; every ambiguous topology is a typed zero-mutation result.
    # Union again (#1059): a library delete must find the album Beets really
    # holds, or it reports not-found for a pressing sitting on disk under
    # the merge survivor.
    current_beets = (
        resolve_current_for_request(beets_db, current_pipeline)
        if current_pipeline is not None
        else beets_db.resolve_current_release(identity)
    )
    if current_beets is None:
        # A request row supplied the union input, but the resolver did not
        # return an answer for it.  This is unavailable authority, not a
        # missing release: falling back to acquisition-only could delete a
        # sibling after a merge.  Exact fallback remains valid only for a
        # genuinely untracked Beets album (no pipeline row).
        return DeleteBeetsUnavailable(
            album_id=request.album_id,
            release_id=identity.release_id,
            reason="request_union_authority_unavailable",
        )
    if isinstance(current_beets, CurrentBeetsMissing):
        return DeleteAlbumNotFound(request.album_id)
    if isinstance(current_beets, CurrentBeetsAmbiguous):
        return DeleteBeetsAmbiguous(
            album_id=request.album_id,
            release_id=identity.release_id,
            album_ids=current_beets.album_ids,
            reason=current_beets.reason,
        )
    if current_beets.album_id != request.album_id:
        return DeleteAlbumAuthorityMismatch(
            album_id=request.album_id,
            authoritative_album_id=current_beets.album_id,
            release_id=identity.release_id,
        )

    delete_request = BeetsDeleteRequest(
        album_id=current_beets.album_id,
        # FILED, not requested — see the Bad Rip site above.
        expected_release_id=current_beets.filed_identity.release_id,
        library_db_path=beets_db.library_db_path,
        library_root=beets_db.library_root,
    )
    # Only the admitted production child receives a token/probe pair. Test
    # seams remain synchronous pure outcomes, but still observe cancellation
    # before and after their effect.
    try:
        effect_receipt.started = True
        effect_receipt.preflight_detail = preflight_detail
        effect_receipt.former_album_path = current_beets.album_path
        effect_receipt.pipeline_row = current_pipeline
        if beets_delete_fn is run_beets_delete:
            beets_outcome = run_beets_delete(
                delete_request,
                cancellation_token=cancellation_token,
                owner_session_probe=lambda: bool(
                    pipeline_db._probe_owner_session(
                        owner_session_identity,
                    ).live,
                ),
            )
        else:
            beets_outcome = beets_delete_fn(delete_request)
    except (AdvisoryLockSessionLost, ExecutionCancelled) as exc:
        # The child may have crossed its destructive boundary before its
        # acknowledgement became unavailable. Preserve the pipeline row and
        # expose an explicit retry manifest rather than claiming ordinary
        # lock contention.
        return _delete_incomplete(
            album_id=current_beets.album_id,
            preflight_detail=preflight_detail,
            former_album_path=current_beets.album_path,
            pipeline_row=current_pipeline,
            reason="subprocess_error",
            detail=f"Beets acknowledgement lost: {type(exc).__name__}: {exc}",
            album_still_present=True,
            deleted_files=None,
            deleted_artifacts=None,
            remaining_owned_paths=(),
            preserved_paths=(),
        )
    cancellation_token.raise_if_cancelled()
    if not pipeline_db._probe_owner_session(owner_session_identity).live:
        raise AdvisoryLockSessionLost(
            "library delete lost owner session after Beets acknowledgement"
        )
    if isinstance(beets_outcome, BeetsDeleteFailed):
        album_still_present = (
            beets_db.get_album_detail(current_beets.album_id) is not None
        )
        former_album_path = current_beets.album_path
        acknowledgement_lost = (
            beets_outcome.reason in _ACK_AMBIGUOUS_DELETE_REASONS
        )
        return _delete_incomplete(
            album_id=current_beets.album_id,
            preflight_detail=preflight_detail,
            former_album_path=current_beets.album_path,
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
    if not beets_db.album_and_items_absent(current_beets.album_id):
        return _delete_incomplete(
            album_id=current_beets.album_id,
            preflight_detail=preflight_detail,
            former_album_path=current_beets.album_path,
            pipeline_row=current_pipeline,
            reason="postcondition_failed",
            detail="exact Beets album or item metadata survived the delete operation",
            album_still_present=(
                beets_db.get_album_detail(current_beets.album_id) is not None
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
            if not pipeline_db.delete_request(deleted_pipeline_id):
                raise RuntimeError(
                    "pipeline purge lost its owner-null delete predicate"
                )
        except Exception:
            log.exception("Failed to purge pipeline request %s", deleted_pipeline_id)
            return DeletePipelinePurgeFailure(
                album_id=current_beets.album_id,
                pipeline_request_id=deleted_pipeline_id,
                album_name=beets_outcome.album_name,
                artist_name=beets_outcome.artist_name,
                former_album_path=beets_outcome.former_album_path,
                deleted_files=beets_outcome.deleted_tracks,
                deleted_artifacts=beets_outcome.deleted_artifacts,
                preserved_paths=beets_outcome.preserved_paths,
            )

    return DeleteSuccess(
        album_id=current_beets.album_id,
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
    pipeline_row: Mapping[str, Any] | None,
    preflight_detail: dict[str, object],
    beets_delete_fn: BeetsDeleteFn,
    cancellation_token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
    effect_receipt: _DeleteEffectReceipt,
) -> DeleteResult:
    # A request may be filed under its canonical survivor while retaining its
    # acquisition identity.  Purging that request removes *both* inverse
    # associations, so it must fence every current association, not merely
    # the filed Beets identity.  Library-only deletes have no request row and
    # therefore deliberately retain their single filed-identity scope.
    association_scope = (
        (*acceptable_identities(pipeline_row), identity)
        if pipeline_row is not None
        else (identity,)
    )
    with release_identity_locks(pipeline_db, association_scope) as release_locks:
        if not release_locks.acquired:
            return DeleteLockContended(request.album_id, "release")
        return _delete_under_release_lock(
            pipeline_db=pipeline_db,
            beets_db=beets_db,
            request=request,
            identity=identity,
            pipeline_row=pipeline_row,
            preflight_detail=preflight_detail,
            beets_delete_fn=beets_delete_fn,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
            effect_receipt=effect_receipt,
        )


def _notify_completed_delete(
    result: DeleteResult,
    notify_fn: DeleteNotifyFn,
) -> DeleteResult:
    if isinstance(result, (DeleteSuccess, DeletePipelinePurgeFailure)):
        try:
            notifications = notify_fn(result.former_album_path)
        except Exception as exc:
            log.exception("Post-delete media notification failed")
            detail = f"notification boundary failed: {type(exc).__name__}: {exc}"
            notifications = (
                DeleteNotification("plex", "warning", detail),
                DeleteNotification("jellyfin", "warning", detail),
            )
        return replace(result, notifications=notifications)
    return result


def _purge_confirmed_missing_pipeline_request(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: DeleteRequest,
    token: CancellationToken,
) -> DeleteResult:
    """Finish an exact pipeline purge after a prior delete lost its ack.

    This is deliberately narrower than ordinary library deletion: the caller
    supplies both immutable identities, Beets is re-read as absent while the
    same IMPORT/RELEASE authority is held, and no Beets child is launched.
    """
    if request.expected_pipeline_id is None or request.expected_release_id is None:
        return DeleteAlbumNotFound(request.album_id)
    supplied_identity = ReleaseIdentity.from_id(request.expected_release_id)
    if supplied_identity is None:
        return _delete_mismatch(request, None, None)

    def idempotent_missing_result() -> DeleteResult:
        """Authorize an acknowledged purge against its filed identity only."""
        try:
            current_beets = beets_db.resolve_current_release(supplied_identity)
        except Exception:  # noqa: BLE001 - read failure is unavailable authority
            current_beets = None
        # Keep this boundary dynamically checked: a malformed external Beets
        # adapter is unavailable authority, never proof of a missing album.
        if current_beets is None:
            return DeleteBeetsUnavailable(
                album_id=request.album_id,
                release_id=supplied_identity.release_id,
                reason="request_union_authority_unavailable",
            )
        if isinstance(current_beets, CurrentBeetsMissing):
            return DeleteSuccess(
                album_id=request.album_id,
                album_name="",
                artist_name="",
                former_album_path="",
                deleted_files=0,
                deleted_artifacts=0,
                pipeline_deleted=True,
                deleted_pipeline_id=request.expected_pipeline_id,
                preserved_paths=(),
            )
        if isinstance(current_beets, CurrentBeetsAmbiguous):
            return DeleteBeetsAmbiguous(
                album_id=request.album_id,
                release_id=supplied_identity.release_id,
                album_ids=current_beets.album_ids,
                reason=current_beets.reason,
            )
        return DeleteAlbumAuthorityMismatch(
            album_id=request.album_id,
            authoritative_album_id=current_beets.album_id,
            release_id=supplied_identity.release_id,
        )

    with pipeline_db._pin_owner_session(token), pipeline_db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_IMPORT, request.expected_pipeline_id,
    ) as import_acquired:
        if not import_acquired:
            return DeleteLockContended(request.album_id, "request")
        # This is deliberately a fresh request read under IMPORT. The caller
        # may be retrying after a lost delete acknowledgement, so neither its
        # original pipeline row nor its original filed identity is authority.
        pipeline_row = pipeline_db.get_request(request.expected_pipeline_id)
        if pipeline_row is None:
            # The durable row may already be gone, but that only proves an
            # earlier purge after the supplied filed identity is freshly
            # missing under its RELEASE authority. Never relaunch a child.
            with release_identity_locks(
                pipeline_db, (supplied_identity,),
            ) as release_locks:
                if not release_locks.acquired:
                    return DeleteLockContended(request.album_id, "release")
                return idempotent_missing_result()

        identity = _request_identity(pipeline_row)
        if supplied_identity not in acceptable_identities(pipeline_row):
            return _delete_mismatch(request, identity, pipeline_row)

        with release_identity_locks(
            pipeline_db, (*acceptable_identities(pipeline_row), supplied_identity),
        ) as release_locks:
            if not release_locks.acquired:
                return DeleteLockContended(request.album_id, "release")
            current = pipeline_db.get_request(request.expected_pipeline_id)
            current_identity = _request_identity(current) if current is not None else None
            if current is None:
                return idempotent_missing_result()
            if supplied_identity not in acceptable_identities(current):
                return _delete_mismatch(request, current_identity, current)
            current_beets = resolve_current_for_request(beets_db, current)
            if not isinstance(current_beets, CurrentBeetsMissing):
                if isinstance(current_beets, CurrentBeetsAmbiguous):
                    return DeleteBeetsAmbiguous(
                        album_id=request.album_id,
                        release_id=supplied_identity.release_id,
                        album_ids=current_beets.album_ids,
                        reason=current_beets.reason,
                    )
                if isinstance(current_beets, CurrentBeetsUnique):
                    return DeleteAlbumAuthorityMismatch(
                        album_id=request.album_id,
                        authoritative_album_id=current_beets.album_id,
                        release_id=supplied_identity.release_id,
                    )
                return DeleteBeetsUnavailable(
                    album_id=request.album_id,
                    release_id=supplied_identity.release_id,
                    reason="request_union_authority_unavailable",
                )
            if not pipeline_db.delete_request(request.expected_pipeline_id):
                return DeletePipelinePurgeFailure(
                    album_id=request.album_id,
                    pipeline_request_id=request.expected_pipeline_id,
                    album_name=str(current.get("album_title") or ""),
                    artist_name=str(current.get("artist_name") or ""),
                    former_album_path="",
                    deleted_files=0,
                    deleted_artifacts=0,
                    preserved_paths=(),
                )
            return DeleteSuccess(
                album_id=request.album_id,
                album_name=str(current.get("album_title") or ""),
                artist_name=str(current.get("artist_name") or ""),
                former_album_path="",
                deleted_files=0,
                deleted_artifacts=0,
                pipeline_deleted=True,
                deleted_pipeline_id=request.expected_pipeline_id,
                preserved_paths=(),
            )


def delete_release_from_library(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: DeleteRequest,
    beets_delete_fn: BeetsDeleteFn | None = None,
    notify_fn: DeleteNotifyFn | None = None,
) -> DeleteResult:
    effect_receipt = _DeleteEffectReceipt()
    try:
        return _delete_release_from_library(
            pipeline_db=pipeline_db,
            beets_db=beets_db,
            request=request,
            beets_delete_fn=beets_delete_fn,
            notify_fn=notify_fn,
            effect_receipt=effect_receipt,
        )
    except (AdvisoryLockSessionLost, OwnerSessionLost, ExecutionCancelled) as exc:
        if effect_receipt.started and effect_receipt.preflight_detail is not None:
            return _delete_incomplete(
                album_id=request.album_id,
                preflight_detail=effect_receipt.preflight_detail,
                former_album_path=effect_receipt.former_album_path,
                pipeline_row=effect_receipt.pipeline_row,
                reason="subprocess_error",
                detail=(
                    "Beets acknowledgement lost after launch: "
                    f"{type(exc).__name__}: {exc}"
                ),
                album_still_present=True,
                deleted_files=None,
                deleted_artifacts=None,
                remaining_owned_paths=(),
                preserved_paths=(),
            )
        # The server released any IMPORT/RELEASE locks with the dead session.
        # No statement may replay on a new backend as though it remained held.
        return DeleteLockContended(request.album_id, "request")


def _delete_release_from_library(
    *,
    pipeline_db: SupportsDestructivePipelineDB,
    beets_db: SupportsDestructiveBeetsDB,
    request: DeleteRequest,
    beets_delete_fn: BeetsDeleteFn | None = None,
    notify_fn: DeleteNotifyFn | None = None,
    effect_receipt: _DeleteEffectReceipt | None = None,
) -> DeleteResult:
    """Delete the exact album identified by the server-owned beets row."""
    delete_op = beets_delete_fn or run_beets_delete
    notifier = notify_fn or _default_delete_notify
    receipt = effect_receipt or _DeleteEffectReceipt()
    detail = beets_db.get_album_detail(request.album_id)
    if detail is None:
        if request.purge_pipeline:
            return _purge_confirmed_missing_pipeline_request(
                pipeline_db=pipeline_db,
                beets_db=beets_db,
                request=request,
                token=CancellationToken(),
            )
        return DeleteAlbumNotFound(request.album_id)
    identity = _album_identity(detail)
    pipeline_row = (
        pipeline_db.get_request(request.expected_pipeline_id)
        if request.expected_pipeline_id is not None
        else (
            pipeline_db.get_request_by_release_id(identity.release_id)
            if identity is not None else None
        )
    )
    if not _delete_confirmations_match(request, identity, pipeline_row):
        return _delete_mismatch(request, identity, pipeline_row)
    assert identity is not None

    token = CancellationToken()
    if pipeline_row is None:
        with pipeline_db._pin_owner_session(token) as owner_session_identity:
            result = _delete_with_release_lock(
                pipeline_db=pipeline_db,
                beets_db=beets_db,
                request=request,
                identity=identity,
                pipeline_row=None,
                preflight_detail=detail,
                beets_delete_fn=delete_op,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
                effect_receipt=receipt,
            )
        return _notify_completed_delete(result, notifier)

    request_id = int(pipeline_row["id"])
    # IMPORT outer, RELEASE inner. See docs/advisory-locks.md.
    with pipeline_db._pin_owner_session(token) as owner_session_identity, \
            pipeline_db.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT, request_id,
            ) as request_acquired:
        if not request_acquired:
            return DeleteLockContended(request.album_id, "request")
        current_pipeline = pipeline_db.get_request(request_id)
        if (
            current_pipeline is None
            or not _delete_confirmations_match(
                request, identity, current_pipeline,
            )
        ):
            return _delete_mismatch(request, identity, current_pipeline)
        processing_locked = transitions.processing_locked_conflict(
            current_pipeline,
            request_id,
            "library_delete",
            expected_status=str(current_pipeline["status"]),
        )
        if processing_locked is not None:
            return processing_locked
        result = _delete_with_release_lock(
            pipeline_db=pipeline_db,
            beets_db=beets_db,
            request=request,
            identity=identity,
            pipeline_row=current_pipeline,
            preflight_detail=detail,
            beets_delete_fn=delete_op,
            cancellation_token=token,
            owner_session_identity=owner_session_identity,
            effect_receipt=receipt,
        )
    return _notify_completed_delete(result, notifier)
