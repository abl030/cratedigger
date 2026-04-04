"""SoularrContext — runtime state container for the pipeline engine.

Replaces module-level globals in soularr.py. Functions extracted to
lib/download.py, lib/import_dispatch.py, etc. receive a SoularrContext
as their first parameter instead of reading globals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from album_source import DatabaseSource
    from lib.config import SoularrConfig


@dataclass
class SoularrContext:
    """All runtime state needed by the pipeline engine."""

    # --- Core dependencies (set once in main()) ---
    cfg: SoularrConfig
    slskd: Any  # slskd_api.SlskdClient — Any to avoid import
    pipeline_db_source: DatabaseSource

    # --- Runtime caches (reset each cycle) ---
    search_cache: dict[int, Any] = field(default_factory=dict)
    folder_cache: dict[str, Any] = field(default_factory=dict)
    user_upload_speed: dict[str, int] = field(default_factory=dict)
    broken_user: list[str] = field(default_factory=list)
    search_dir_audio_count: dict[str, dict[str, int]] = field(default_factory=dict)
    negative_matches: set[tuple[str, str, int, str]] = field(default_factory=set)
    current_album_cache: dict[int, Any] = field(default_factory=dict)
