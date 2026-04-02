"""Redis cache layer for the Soularr web UI.

All operations are fail-safe — Redis being down means cache miss, never an error.
MB data cached 24h (mirror syncs daily at 3am). Beets/pipeline data cached 5min
with explicit invalidation on mutations.
"""

from __future__ import annotations

import hashlib
import json
import logging

log = logging.getLogger(__name__)

# TTL constants (seconds)
TTL_MB = 86400       # 24h — MB mirror data, effectively static
TTL_LIBRARY = 300    # 5min — beets/pipeline data, also invalidated on mutation

# Group → pattern mapping for bulk invalidation
_GROUP_PATTERNS: dict[str, str] = {
    "pipeline": "pipeline:*",
    "library": "library:*",
    "mb": "mb:*",
}

_redis: object | None = None


def init(host: str, port: int = 6379) -> None:
    """Connect to Redis. Call once at startup."""
    global _redis
    try:
        import redis  # type: ignore[import-untyped]
        _redis = redis.Redis(host=host, port=port, decode_responses=True,
                             socket_connect_timeout=1, socket_timeout=1)
        _redis.ping()  # type: ignore[union-attr]
        log.info("Redis connected: %s:%d", host, port)
    except Exception as e:
        log.warning("Redis unavailable (%s), running without cache", e)
        _redis = None


def cache_get(key: str) -> dict | list | None:
    """Get cached value. Returns None on miss or Redis error."""
    if _redis is None:
        return None
    try:
        raw = _redis.get(key)  # type: ignore[union-attr]
        if raw is None:
            return None
        return json.loads(raw)
    except Exception:
        return None


def cache_set(key: str, value: dict | list, ttl: int = TTL_MB) -> None:
    """Set cached value with TTL. Silently fails if Redis is down."""
    if _redis is None:
        return
    try:
        _redis.setex(key, ttl, json.dumps(value))  # type: ignore[union-attr]
    except Exception:
        pass


def invalidate(key: str) -> None:
    """Delete a single cache key."""
    if _redis is None:
        return
    try:
        _redis.delete(key)  # type: ignore[union-attr]
    except Exception:
        pass


def invalidate_pattern(pattern: str) -> None:
    """Delete all keys matching a glob pattern (e.g. 'library:*')."""
    if _redis is None:
        return
    try:
        cursor = 0
        while True:
            cursor, keys = _redis.scan(cursor=cursor, match=pattern, count=100)  # type: ignore[union-attr]
            if keys:
                _redis.delete(*keys)  # type: ignore[union-attr]
            if cursor == 0:
                break
    except Exception:
        pass


def invalidate_groups(*groups: str) -> None:
    """Invalidate all keys in named groups (e.g. 'pipeline', 'library')."""
    for group in groups:
        pattern = _GROUP_PATTERNS.get(group)
        if pattern:
            invalidate_pattern(pattern)


def key_hash(value: str) -> str:
    """Short hash for use in cache keys (e.g. search queries)."""
    return hashlib.md5(value.encode()).hexdigest()[:12]
