"""Reconstruct typed download rows from persisted active-download state."""

from __future__ import annotations

from collections.abc import Mapping

import logging
from typing import Any

from lib.grab_list import DownloadFile, GrabListEntry
from lib.import_manifest import manifest_trace_summary
from lib.quality import ActiveDownloadState
from lib.slskd_client import TransferSnapshot

logger = logging.getLogger("cratedigger")


def _restored_terminal_status(
    last_state: str | None,
    bytes_transferred: int,
    exception: str | None = None,
) -> TransferSnapshot | None:
    """Rehydrate terminal slskd observations persisted in JSONB state."""
    if not last_state or not last_state.startswith("Completed,"):
        return None
    return TransferSnapshot(
        state=last_state,
        bytes_transferred=bytes_transferred,
        exception=exception,
    )


def reconstruct_grab_list_entry(
    request: Mapping[str, Any],
    state: ActiveDownloadState,
    *,
    transfer_ids: dict[tuple[str, str], str] | None = None,
) -> GrabListEntry:
    """Rebuild a GrabListEntry from one DB row and its persisted state.

    This is the single projection used by poll/materialize workers and the
    disk reaper. Callers with a live slskd snapshot may supply ephemeral
    transfer IDs keyed by ``(username, filename)``; persisted-only callers
    leave them empty.
    """
    files = []
    for file_state in state.files:
        restored_status = _restored_terminal_status(
            file_state.last_state,
            file_state.bytes_transferred,
            file_state.last_exception,
        )
        files.append(DownloadFile(
            filename=file_state.filename,
            id=(transfer_ids or {}).get(
                (file_state.username, file_state.filename),
                "",
            ),
            file_dir=file_state.file_dir,
            username=file_state.username,
            size=file_state.size,
            disk_no=file_state.disk_no,
            disk_count=file_state.disk_count,
            retry=file_state.retry_count,
            bytes_transferred=file_state.bytes_transferred,
            last_state=file_state.last_state,
            last_exception=file_state.last_exception,
            status=restored_status,
            local_path=file_state.local_path,
        ))
    year = request.get("year")
    logger.info(
        "MANIFEST-TRACE reconstruct request=%s %s current_path=%s",
        request["id"],
        manifest_trace_summary(files),
        state.current_path,
    )
    return GrabListEntry(
        album_id=request["id"],
        files=files,
        filetype=state.filetype,
        title=request["album_title"],
        artist=request["artist_name"],
        year=str(year) if year else "",
        mb_release_id=request.get("mb_release_id") or "",
        db_request_id=request["id"],
        db_source=request.get("source"),
        db_search_filetype_override=request.get("search_filetype_override"),
        db_target_format=request.get("target_format"),
        import_folder=state.current_path,
    )
