"""Typed dataclasses for the download pipeline.

GrabListEntry — one album being downloaded.
DownloadFile  — one file within an album download.

Attribute-only access. No dict compatibility — use .field, not ["field"].
"""

from __future__ import annotations

from dataclasses import dataclass

from lib.quality import SpectralMeasurement
from lib.slskd_client import TransferSnapshot


@dataclass
class GrabListEntry:
    """A single entry in the grab list — one album being downloaded."""

    # Required (set by find_download)
    album_id: int
    files: list[DownloadFile]
    filetype: str               # "mp3", "flac", "mp3 v0", etc.
    title: str
    artist: str
    year: str                   # 4-char from releaseDate
    mb_release_id: str

    # Optional: DB mode
    db_request_id: int | None = None
    db_source: str | None = None           # "request" or "redownload"
    db_search_filetype_override: str | None = None
    db_target_format: str | None = None

    # Transient: process_completed_album
    import_folder: str | None = None
    download_spectral: SpectralMeasurement | None = None
    current_min_bitrate: int | None = None
    current_spectral: SpectralMeasurement | None = None



@dataclass
class DownloadFile:
    """A single file within a download — one track being transferred."""

    # Core (set in slskd_do_enqueue)
    filename: str           # Full soulseek path with backslashes
    id: str                 # slskd transfer ID
    file_dir: str           # Download directory on source user's system
    username: str           # Soulseek username
    size: int               # File size in bytes

    # Audio metadata (optional, from slskd search results)
    bitRate: int | None = None
    sampleRate: int | None = None
    bitDepth: int | None = None
    isVariableBitRate: bool | None = None

    # Multi-disc (optional, set in try_multi_enqueue)
    disk_no: int | None = None
    disk_count: int | None = None

    # Transient: poll_active_downloads
    status: TransferSnapshot | None = None   # typed slskd transfer snapshot (#468)
    retry: int | None = None     # retry counter, initialized on error
    bytes_transferred: int | None = None
    last_state: str | None = None
    # slskd's real per-transfer failure reason (issue #564), mirrored
    # from TransferSnapshot.exception and persisted alongside last_state.
    last_exception: str | None = None
    # slskd's authoritative post-rename local path, stamped from the
    # DownloadFileComplete event stream (issue #146 phase 1).
    local_path: str | None = None

    # Transient: process_completed_album
    import_path: str | None = None
