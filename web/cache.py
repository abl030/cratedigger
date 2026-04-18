"""Redis cache layer for the Soularr web UI.

Two separate namespaces:

- `meta:<key>` — PURE MusicBrainz / Discogs metadata. 24h TTL (mirrors
  sync daily). Populated via `memoize_meta()` inside `web/mb.py` and
  `web/discogs.py`. **Never** invalidated by pipeline / library
  writes — MB/Discogs metadata doesn't care about pipeline state.

- `web:<url>` — legacy routing-level cache for whole HTTP responses.
  Only used today for the pure-search endpoints (`/api/search`,
  `/api/discogs/search`). Overlay-baking endpoints (release, release-
  group, discogs master/release, beets, pipeline, library/artist,
  disambiguate, …) MUST NOT be cached here — that baked
  `pipeline_status` / `in_library` into the payload and leaked stale
  badges when soularr-the-pipeline updated Postgres outside the web
  UI's POST invalidation paths. See issue #101.

All operations fail-safe — Redis being down means cache miss, never
an error.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Callable

log = logging.getLogger(__name__)

# TTL constants (seconds)
TTL_MB = 86400       # 24h — MB/Discogs mirror data, effectively static
TTL_LIBRARY = 300    # 5min — legacy routing cache (now only for search)

# Key prefix for the pure-metadata namespace. Kept as a module constant
# so `meta_get` / `meta_set` / `memoize_meta` all agree, and callers /
# tests can assert on the final key shape.
_META_PREFIX = "meta:"

# Group → pattern mapping for bulk invalidation of routing-level responses.
# Pattern scope is deliberately limited to `web:` keys — the `meta:`
# namespace (pure MB/Discogs metadata) is NEVER invalidated by pipeline
# state changes because the metadata didn't change. A pipeline write
# only makes a cached overlay response stale; the metadata underneath
# is still correct.
_GROUP_PATTERNS: dict[str, list[str]] = {
    "pipeline": ["web:/api/pipeline*"],
    "library": ["web:/api/beets*", "web:/api/library*"],
    "mb": ["web:/api/search*", "web:/api/artist*", "web:/api/release*"],
    "discogs": ["web:/api/discogs*"],
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


# ── Routing-level cache (legacy `web:` namespace) ─────────────────────


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


# ── Pure-metadata cache (`meta:` namespace) ──────────────────────────
#
# These helpers prefix the key with `meta:` so the patterns in
# `_GROUP_PATTERNS` (all scoped to `web:`) cannot reach them. That is
# the whole point — an album transitioning wanted→downloading in
# Postgres MUST NOT flush the MB mirror lookup for that album's release
# group. Only the routing-cache responses under `web:` are overlay-
# baked and need group invalidation.


def meta_get(key: str) -> Any:
    """Get from the pure metadata cache. Key is prefixed with `meta:`."""
    if _redis is None:
        return None
    try:
        raw = _redis.get(f"{_META_PREFIX}{key}")  # type: ignore[union-attr]
        if raw is None:
            return None
        return json.loads(raw)
    except Exception:
        return None


def meta_set(key: str, value: Any, ttl: int = TTL_MB) -> None:
    """Set in the pure metadata cache. Key is prefixed with `meta:`."""
    if _redis is None:
        return
    try:
        _redis.setex(  # type: ignore[union-attr]
            f"{_META_PREFIX}{key}", ttl, json.dumps(value))
    except Exception:
        pass


def memoize_meta(key: str, fetch_fn: Callable[[], Any], ttl: int = TTL_MB) -> Any:
    """Return cached `meta:<key>` or call `fetch_fn()` and cache the result.

    With Redis absent (CLI context, tests), degrades to pass-through —
    every call runs `fetch_fn()` and nothing is cached.
    """
    cached = meta_get(key)
    if cached is not None:
        return cached
    result = fetch_fn()
    meta_set(key, result, ttl)
    return result


# ── Invalidation ──────────────────────────────────────────────────────


def invalidate(key: str) -> None:
    """Delete a single cache key."""
    if _redis is None:
        return
    try:
        _redis.delete(key)  # type: ignore[union-attr]
    except Exception:
        pass


def invalidate_pattern(pattern: str) -> None:
    """Delete all keys matching a glob pattern (e.g. 'library:*').

    Callers should use the `web:` or `meta:` prefix explicitly. Patterns
    without a prefix are allowed but discouraged — they can match across
    namespaces.
    """
    if _redis is None:
        return
    try:
        cursor = 0
        while True:
            cursor, keys = _redis.scan(  # type: ignore[union-attr]
                cursor=cursor, match=pattern, count=100)
            if keys:
                _redis.delete(*keys)  # type: ignore[union-attr]
            if cursor == 0:
                break
    except Exception:
        pass


def invalidate_groups(*groups: str) -> None:
    """Invalidate all routing-cache keys in named groups.

    Scope is the `web:` namespace only — `_GROUP_PATTERNS` patterns are
    all `web:/api/...`. The `meta:` namespace is out of reach by design.
    """
    for group in groups:
        patterns = _GROUP_PATTERNS.get(group, [])
        for pattern in patterns:
            invalidate_pattern(pattern)


def key_hash(value: str) -> str:
    """Short hash for use in cache keys (e.g. search queries)."""
    return hashlib.md5(value.encode()).hexdigest()[:12]
