"""Typed dataclasses for the download pipeline.

GrabListEntry — one album being downloaded.
DownloadFile  — one file within an album download.

Attribute-only access. No dict compatibility — use .field, not ["field"].
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


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
    db_request_id: Optional[int] = None
    db_source: Optional[str] = None           # "request" or "redownload"
    db_quality_override: Optional[str] = None

    # Transient: process_completed_album
    import_folder: Optional[str] = None
    spectral_grade: Optional[str] = None
    spectral_bitrate: Optional[int] = None
    existing_min_bitrate: Optional[int] = None
    existing_spectral_bitrate: Optional[int] = None



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
    bitRate: Optional[int] = None
    sampleRate: Optional[int] = None
    bitDepth: Optional[int] = None
    isVariableBitRate: Optional[bool] = None

    # Multi-disc (optional, set in try_multi_enqueue)
    disk_no: Optional[int] = None
    disk_count: Optional[int] = None

    # Transient: poll_active_downloads
    status: Optional[dict] = None   # slskd status object with "state" key
    retry: Optional[int] = None     # retry counter, initialized on error

    # Transient: process_completed_album
    import_path: Optional[str] = None

