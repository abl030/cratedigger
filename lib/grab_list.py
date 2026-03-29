"""Typed dataclasses for the download pipeline.

GrabListEntry — one album being downloaded (replaces the grab_list dict).
DownloadFile  — one file within an album download (replaces file dicts).

Bridge methods (__getitem__, __setitem__, __contains__, get) allow
existing dict-access code to keep working during incremental migration.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Optional


# Maps underscore-prefixed dict keys to clean field names (GrabListEntry only).
_GRAB_ALIASES: dict[str, str] = {
    "_db_request_id": "db_request_id",
    "_db_source": "db_source",
    "_db_quality_override": "db_quality_override",
    "_spectral_grade": "spectral_grade",
    "_spectral_bitrate": "spectral_bitrate",
    "_existing_min_bitrate": "existing_min_bitrate",
    "_existing_spectral_bitrate": "existing_spectral_bitrate",
}


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

    # Transient: monitor_downloads
    count_start: Optional[float] = None
    rejected_retries: Optional[int] = None
    error_count: Optional[int] = None

    # Transient: process_completed_album
    import_folder: Optional[str] = None
    spectral_grade: Optional[str] = None
    spectral_bitrate: Optional[int] = None
    existing_min_bitrate: Optional[int] = None
    existing_spectral_bitrate: Optional[int] = None

    def _resolve_key(self, key: str) -> str:
        name = _GRAB_ALIASES.get(key, key)
        if name in _GRAB_FIELDS:
            return name
        raise KeyError(key)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, self._resolve_key(key))

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, self._resolve_key(key), value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        name = _GRAB_ALIASES.get(key, key)
        if name not in _GRAB_FIELDS:
            return False
        return getattr(self, name) is not None

    def get(self, key: str, default: Any = None) -> Any:
        try:
            value = self[key]
        except KeyError:
            return default
        return value if value is not None else default


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

    # Transient: monitor_downloads
    status: Optional[dict] = None   # slskd status object with "state" key
    retry: Optional[int] = None     # retry counter, initialized on error

    # Transient: process_completed_album
    import_path: Optional[str] = None

    def _resolve_key(self, key: str) -> str:
        if key in _FILE_FIELDS:
            return key
        raise KeyError(key)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, self._resolve_key(key))

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, self._resolve_key(key), value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key not in _FILE_FIELDS:
            return False
        return getattr(self, key) is not None

    def get(self, key: str, default: Any = None) -> Any:
        try:
            value = self[key]
        except KeyError:
            return default
        return value if value is not None else default


# Computed after both classes are defined.
_GRAB_FIELDS: frozenset[str] = frozenset(f.name for f in fields(GrabListEntry))
_FILE_FIELDS: frozenset[str] = frozenset(f.name for f in fields(DownloadFile))
