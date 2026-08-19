"""Configured-authority preflight for local-import queueing.

Sibling of ``lib.force_import_service`` (issue #1176 PR3): the same shape
(preflight, not execution authority — the preview worker reopens the
configured authority before reading it), the same processing-lock refusal,
the same MBID precondition. It differs only in which path authority resolves
the operator-named folder (``lib.fs_authority.
open_configured_local_import_directory``, issue #1176 PR2) and in what it
enqueues (``local_import`` payload keyed on ``request_id`` alone — there is
no ``download_log`` row behind a local import).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from lib import transitions
from lib.fs_authority import (
    FilesystemAuthorityError,
    LocalImportNotConfiguredError,
    open_configured_local_import_directory,
    refusal_is_indeterminate,
)
from lib.import_queue import (
    IMPORT_JOB_LOCAL,
    ImportJob,
    local_import_dedupe_key,
    local_import_payload,
)

if TYPE_CHECKING:
    from lib.pipeline_db._shared import ProcessingOwnerProjection
    from lib.pipeline_db.rows import AlbumRequestRow


RESULT_QUEUED = "queued"
RESULT_REQUEST_MISSING = "request_missing"
RESULT_REQUEST_MBID_MISSING = "request_mbid_missing"
RESULT_NOT_CONFIGURED = "not_configured"
"""The local-import lane has no configured authority at all
(``LocalImportNotConfiguredError``) — a distinct outcome from
``unauthorized_path`` so the route/CLI can surface the exact module option
the exception's own message already names
(``services.cratedigger.localImport.enable`` / ``.dir``) rather than a
generic containment refusal."""
RESULT_UNAUTHORIZED_PATH = "unauthorized_path"
RESULT_PATH_UNAVAILABLE = "path_unavailable"
"""The local-import authority could not OBSERVE the path (EACCES, EIO,
ESTALE …).

Deliberately not ``unauthorized_path``: 422 tells the operator the input is
semantically wrong; a refused probe is a transient world failure and gets
retryable vocabulary, exactly like the sibling force-import surface (issue
#1063).
"""
RESULT_PROCESSING_LOCKED = "processing_locked"

# There is no separate CLI exit-code table: ``pipeline-cli import-local``
# executes through ``POST /api/pipeline/import-local`` and derives its exit
# code from the status below, mirroring ``force-import`` (issue #1063).
LOCAL_IMPORT_HTTP_STATUS = {
    RESULT_QUEUED: 202,
    RESULT_REQUEST_MISSING: 404,
    RESULT_REQUEST_MBID_MISSING: 422,
    RESULT_NOT_CONFIGURED: 422,
    RESULT_UNAUTHORIZED_PATH: 422,
    RESULT_PATH_UNAVAILABLE: 503,
    RESULT_PROCESSING_LOCKED: 409,
}


class LocalImportDB(Protocol):
    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, object] | None = None,
        message: str | None = None,
    ) -> ImportJob: ...


@dataclass(frozen=True)
class LocalImportEnqueueResult:
    outcome: str
    request_id: int
    source_path: str | None = None
    detail: str | None = None
    job: ImportJob | None = None
    processing_owner: ProcessingOwnerProjection | None = None


def enqueue_local_import(
    db: LocalImportDB,
    cfg: object,
    *,
    request_id: int,
    source_path: str,
) -> LocalImportEnqueueResult:
    """Authorize and enqueue one operator-named local import.

    Intentionally a preflight, not execution authority: the preview worker
    reopens the configured local-import directory before reading it
    (``lib.fs_authority.open_configured_local_import_directory``), exactly
    as force-import's preflight re-resolves its quarantine authority.
    """
    request = db.get_request(request_id)
    if request is None:
        return LocalImportEnqueueResult(RESULT_REQUEST_MISSING, request_id)

    processing_locked = transitions.processing_locked_conflict(
        request,
        request_id,
        "local_import",
        expected_status=str(request["status"]),
    )
    if processing_locked is not None:
        owner = processing_locked.processing_owner
        if owner is None:
            raise RuntimeError(
                "processing conflict is missing its exact owner"
            )
        return LocalImportEnqueueResult(
            RESULT_PROCESSING_LOCKED,
            request_id,
            detail=(
                f"request {request_id} is owned by automation import job "
                f"{owner.job_id}"
            ),
            processing_owner=owner,
        )
    if not request.get("mb_release_id"):
        return LocalImportEnqueueResult(
            RESULT_REQUEST_MBID_MISSING,
            request_id,
            detail="Local import requires a MusicBrainz release ID",
        )

    try:
        with open_configured_local_import_directory(source_path, cfg) as opened:
            authorized_path = opened.display_path
    except LocalImportNotConfiguredError as exc:
        return LocalImportEnqueueResult(
            RESULT_NOT_CONFIGURED,
            request_id,
            source_path=source_path,
            detail=str(exc),
        )
    except FilesystemAuthorityError as exc:
        return LocalImportEnqueueResult(
            RESULT_PATH_UNAVAILABLE
            if refusal_is_indeterminate(exc.code) is True
            else RESULT_UNAUTHORIZED_PATH,
            request_id,
            source_path=source_path,
            detail=str(exc),
        )

    job = db.enqueue_import_job(
        IMPORT_JOB_LOCAL,
        request_id=request_id,
        dedupe_key=local_import_dedupe_key(request_id),
        payload=local_import_payload(
            source_path=authorized_path,
            request_id=request_id,
        ),
        message=(
            f"Local import queued for {request['artist_name']} - "
            f"{request['album_title']}"
        ),
    )
    return LocalImportEnqueueResult(
        RESULT_QUEUED,
        request_id,
        source_path=authorized_path,
        job=job,
    )
