"""Persist runtime caches across cratedigger runs.

Atomic save/load of folder_cache, user_upload_speed, search_dir_audio_count
to a JSON file in var_dir. Per-entry TTL: each entry is stamped when fetched,
and entries older than FOLDER_CACHE_TTL_SECONDS are evicted on load.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")

CACHE_FILENAME = "cratedigger_cache.json"
FOLDER_CACHE_TTL_SECONDS = 86400  # 24 hours


def cache_path(var_dir: str) -> str:
    return os.path.join(var_dir, CACHE_FILENAME)


def save_caches(ctx: CratediggerContext, var_dir: str) -> None:
    """Save persistable caches to disk. Atomic write (tmp + rename).

    Each entry is wrapped with its fetch timestamp for per-entry TTL eviction.
    """
    now = time.time()

    # folder_cache: {user: {dir: {"_ts": float, "d": data}}}
    wrapped_fc: dict[str, dict[str, Any]] = {}
    for user, dirs in ctx.folder_cache.items():
        wrapped_fc[user] = {}
        for d, data in dirs.items():
            ts = ctx._folder_cache_ts.get(user, {}).get(d, now)
            wrapped_fc[user][d] = {"_ts": ts, "d": data}

    # user_upload_speed: {user: {"_ts": float, "v": int}}
    wrapped_speed: dict[str, Any] = {}
    for user, speed in ctx.user_upload_speed.items():
        ts = ctx._upload_speed_ts.get(user, now)
        wrapped_speed[user] = {"_ts": ts, "v": speed}

    # search_dir_audio_count: {user: {dir: {"_ts": float, "v": int}}}
    wrapped_count: dict[str, dict[str, Any]] = {}
    for user, dirs in ctx.search_dir_audio_count.items():
        wrapped_count[user] = {}
        for d, count in dirs.items():
            ts = ctx._dir_audio_count_ts.get(user, {}).get(d, now)
            wrapped_count[user][d] = {"_ts": ts, "v": count}

    payload: dict[str, Any] = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "folder_cache": wrapped_fc,
        "user_upload_speed": wrapped_speed,
        "search_dir_audio_count": wrapped_count,
    }
    path = cache_path(var_dir)
    try:
        fd, tmp_path = tempfile.mkstemp(dir=var_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception:
        logger.warning("Failed to save caches", exc_info=True)


def load_caches(ctx: CratediggerContext, var_dir: str) -> None:
    """Load persisted caches into ctx. Evicts entries older than TTL.

    Records wall-clock duration on `ctx.cache_load_s` (issue #198 R15) so the
    cycle summary can attribute time to the JSON load tax. Stays at 0.0 when
    no file exists or the file is unreadable — those paths skip the load
    entirely so there's no meaningful duration to attribute.
    """
    path = cache_path(var_dir)
    if not os.path.exists(path):
        return

    # try/finally so cache_load_s is always credited once we've passed the
    # os.path.exists() guard, even if json.load or the data-walk raises.
    # Corrupt files exit via the inner `return` and still credit their
    # (tiny) parse-attempt cost — that's honest accounting, not a bug.
    load_start = time.monotonic()
    try:
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("Cache file corrupt or unreadable — starting fresh")
            return

        if not isinstance(data, dict):
            return

        now = time.time()
        cutoff = now - FOLDER_CACHE_TTL_SECONDS
        fc_loaded = 0
        fc_evicted = 0
        speed_loaded = 0
        count_loaded = 0

        # folder_cache
        fc = data.get("folder_cache")
        if isinstance(fc, dict):
            for user, dirs in fc.items():
                if not isinstance(dirs, dict):
                    continue
                for d, entry in dirs.items():
                    if isinstance(entry, dict) and "_ts" in entry:
                        ts = entry["_ts"]
                        if isinstance(ts, (int, float)) and ts >= cutoff:
                            ctx.folder_cache.setdefault(user, {})[d] = entry["d"]
                            ctx._folder_cache_ts.setdefault(user, {})[d] = ts
                            fc_loaded += 1
                        else:
                            fc_evicted += 1
                    else:
                        # Legacy format (no timestamp) — treat as fresh
                        ctx.folder_cache.setdefault(user, {})[d] = entry
                        ctx._folder_cache_ts.setdefault(user, {})[d] = now
                        fc_loaded += 1

        # user_upload_speed
        speed = data.get("user_upload_speed")
        if isinstance(speed, dict):
            for user, entry in speed.items():
                if isinstance(entry, dict) and "_ts" in entry:
                    ts = entry["_ts"]
                    if isinstance(ts, (int, float)) and ts >= cutoff:
                        v = entry.get("v")
                        if isinstance(v, int):
                            ctx.user_upload_speed[user] = v
                            ctx._upload_speed_ts[user] = ts
                            speed_loaded += 1
                elif isinstance(entry, int):
                    # Legacy format
                    ctx.user_upload_speed[user] = entry
                    ctx._upload_speed_ts[user] = now
                    speed_loaded += 1

        # search_dir_audio_count
        counts = data.get("search_dir_audio_count")
        if isinstance(counts, dict):
            for user, dirs in counts.items():
                if not isinstance(dirs, dict):
                    continue
                for d, entry in dirs.items():
                    if isinstance(entry, dict) and "_ts" in entry:
                        ts = entry["_ts"]
                        if isinstance(ts, (int, float)) and ts >= cutoff:
                            v = entry.get("v")
                            if isinstance(v, int):
                                ctx.search_dir_audio_count.setdefault(user, {})[d] = v
                                ctx._dir_audio_count_ts.setdefault(user, {})[d] = ts
                                count_loaded += 1
                    elif isinstance(entry, int):
                        # Legacy format
                        ctx.search_dir_audio_count.setdefault(user, {})[d] = entry
                        ctx._dir_audio_count_ts.setdefault(user, {})[d] = now
                        count_loaded += 1

        logger.info(f"Loaded caches from {path}: "
                    f"{fc_loaded} folder entries ({fc_evicted} evicted), "
                    f"{speed_loaded} speed entries, {count_loaded} count entries")
    finally:
        ctx.cache_load_s = time.monotonic() - load_start
        logger.info(f"Cache load elapsed: {ctx.cache_load_s:.3f}s")
