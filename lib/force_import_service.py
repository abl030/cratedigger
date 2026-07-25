"""Configured-authority preflight for force-import queueing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TYPE_CHECKING

from lib.fs_authority import FilesystemAuthorityError, open_configured_quarantine_directory
from lib.import_queue import ImportJob, IMPORT_JOB_FORCE, force_import_dedupe_key, force_import_payload
from lib.processing_paths import normalize_source_dirs
from lib.validation_envelope import decode_validation_envelope

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow, DownloadLogWithEvidenceRow


RESULT_QUEUED = "queued"
RESULT_DOWNLOAD_LOG_MISSING = "download_log_missing"
RESULT_REQUEST_MISSING = "request_missing"
RESULT_REQUEST_MBID_MISSING = "request_mbid_missing"
RESULT_FAILED_PATH_MISSING = "failed_path_missing"
RESULT_UNAUTHORIZED_PATH = "unauthorized_path"

FORCE_IMPORT_EXIT_CODE = {
    RESULT_QUEUED: 0,
    RESULT_DOWNLOAD_LOG_MISSING: 2,
    RESULT_REQUEST_MISSING: 2,
    RESULT_REQUEST_MBID_MISSING: 3,
    RESULT_FAILED_PATH_MISSING: 3,
    RESULT_UNAUTHORIZED_PATH: 3,
}
FORCE_IMPORT_HTTP_STATUS = {
    RESULT_QUEUED: 202,
    RESULT_DOWNLOAD_LOG_MISSING: 404,
    RESULT_REQUEST_MISSING: 404,
    RESULT_REQUEST_MBID_MISSING: 422,
    RESULT_FAILED_PATH_MISSING: 422,
    RESULT_UNAUTHORIZED_PATH: 422,
}


class ForceImportDB(Protocol):
    def get_download_log_entry(self, log_id: int) -> "DownloadLogWithEvidenceRow | None": ...

    def get_request(self, request_id: int) -> "AlbumRequestRow | None": ...

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
class ForceImportEnqueueResult:
    outcome: str
    download_log_id: int
    request_id: int | None = None
    failed_path: str | None = None
    detail: str | None = None
    job: ImportJob | None = None


def enqueue_force_import(
    db: ForceImportDB,
    cfg: object,
    download_log_id: int,
) -> ForceImportEnqueueResult:
    """Authorize and enqueue one download-log-backed force import.

    This is intentionally a preflight, not execution authority: the preview
    worker reopens the configured quarantine directory before reading it.
    """
    entry = db.get_download_log_entry(download_log_id)
    if entry is None:
        return ForceImportEnqueueResult(RESULT_DOWNLOAD_LOG_MISSING, download_log_id)

    request_id = entry["request_id"]
    request = db.get_request(request_id)
    if request is None:
        return ForceImportEnqueueResult(
            RESULT_REQUEST_MISSING, download_log_id, request_id=request_id,
        )
    if not request.get("mb_release_id"):
        return ForceImportEnqueueResult(
            RESULT_REQUEST_MBID_MISSING,
            download_log_id,
            request_id=request_id,
            detail="Force import requires a MusicBrainz release ID",
        )

    validation = decode_validation_envelope(entry.get("validation_result"))
    failed_path = validation.failed_path
    if not failed_path:
        return ForceImportEnqueueResult(
            RESULT_FAILED_PATH_MISSING, download_log_id, request_id=request_id,
        )

    try:
        with open_configured_quarantine_directory(failed_path, cfg) as opened:
            authorized_path = opened.display_path
    except FilesystemAuthorityError as exc:
        return ForceImportEnqueueResult(
            RESULT_UNAUTHORIZED_PATH,
            download_log_id,
            request_id=request_id,
            failed_path=failed_path,
            detail=str(exc),
        )

    job = db.enqueue_import_job(
        IMPORT_JOB_FORCE,
        request_id=request_id,
        dedupe_key=force_import_dedupe_key(download_log_id),
        payload=force_import_payload(
            download_log_id=download_log_id,
            failed_path=authorized_path,
            source_username=(
                entry.get("soulseek_username") or validation.soulseek_username
            ),
            source_dirs=normalize_source_dirs(validation.source_dirs),
        ),
        message=(
            f"Force import queued for {request['artist_name']} - "
            f"{request['album_title']}"
        ),
    )
    return ForceImportEnqueueResult(
        RESULT_QUEUED,
        download_log_id,
        request_id=request_id,
        failed_path=authorized_path,
        job=job,
    )
