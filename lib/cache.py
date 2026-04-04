"""Persist runtime caches across soularr runs.

Atomic save/load of folder_cache, user_upload_speed, search_dir_audio_count
to a JSON file in var_dir. folder_cache entries have a coarse TTL: if the
cache file is older than FOLDER_CACHE_TTL_SECONDS, folder_cache is not loaded
(but speed and count caches are still loaded since stale data is harmless).
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import SoularrContext

logger = logging.getLogger("soularr")

CACHE_FILENAME = "soularr_cache.json"
FOLDER_CACHE_TTL_SECONDS = 600  # 10 minutes


def cache_path(var_dir: str) -> str:
    return os.path.join(var_dir, CACHE_FILENAME)


def save_caches(ctx: SoularrContext, var_dir: str) -> None:
    """Save persistable caches to disk. Atomic write (tmp + rename)."""
    data: dict[str, Any] = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "folder_cache": ctx.folder_cache,
        "user_upload_speed": ctx.user_upload_speed,
        "search_dir_audio_count": ctx.search_dir_audio_count,
    }
    path = cache_path(var_dir)
    try:
        fd, tmp_path = tempfile.mkstemp(dir=var_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, path)
        except BaseException:
            # Clean up temp file on any failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception:
        logger.warning("Failed to save caches", exc_info=True)


def load_caches(ctx: SoularrContext, var_dir: str) -> None:
    """Load persisted caches into ctx. Missing or corrupt files are ignored."""
    path = cache_path(var_dir)
    if not os.path.exists(path):
        return

    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        logger.warning("Cache file corrupt or unreadable — starting fresh")
        return

    if not isinstance(data, dict):
        return

    # Check TTL for folder_cache
    saved_at_str = data.get("saved_at", "")
    folder_cache_fresh = False
    try:
        saved_at = datetime.fromisoformat(saved_at_str)
        age = (datetime.now(timezone.utc) - saved_at).total_seconds()
        folder_cache_fresh = age < FOLDER_CACHE_TTL_SECONDS
    except (ValueError, TypeError):
        pass

    if folder_cache_fresh:
        folder_cache = data.get("folder_cache")
        if isinstance(folder_cache, dict):
            ctx.folder_cache.update(folder_cache)
            logger.info(f"Loaded folder_cache: {sum(len(v) for v in folder_cache.values())} entries "
                        f"across {len(folder_cache)} users")

    speed = data.get("user_upload_speed")
    if isinstance(speed, dict):
        for k, v in speed.items():
            if isinstance(v, int):
                ctx.user_upload_speed[k] = v

    counts = data.get("search_dir_audio_count")
    if isinstance(counts, dict):
        for user, dirs in counts.items():
            if isinstance(dirs, dict):
                ctx.search_dir_audio_count[user] = {
                    d: c for d, c in dirs.items() if isinstance(c, int)
                }

    logger.info(f"Loaded caches from {path} (age: {saved_at_str})")
