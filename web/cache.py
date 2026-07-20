"""Redis cache layer for the Cratedigger web UI.

`meta:<key>` namespace — PURE MusicBrainz / Discogs metadata. 24h TTL
(mirrors sync daily). Populated via `memoize_meta()` inside `web/mb.py`
and `web/discogs.py`. **Never** invalidated by pipeline / library
writes — MB/Discogs metadata doesn't care about pipeline state.

All operations fail-safe — Redis being down means cache miss, never
an error.
"""

from __future__ import annotations

import copy
import json
import logging
import threading
from typing import Any, Callable

log = logging.getLogger(__name__)

# TTL constants (seconds)
TTL_MB = 86400       # 24h — MB/Discogs mirror data, effectively static

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


class _MetadataFlight:
    """One in-process fill shared by callers of the same metadata key."""

    def __init__(self) -> None:
        self.done = threading.Event()
        self.result: Any = None
        self.error: BaseException | None = None


_metadata_flights: dict[str, _MetadataFlight] = {}
_metadata_flights_lock = threading.Lock()


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def init(host: str, port: int = 6379) -> None:
    """Connect to Redis. Call once at startup."""
    global _redis
    try:
        import redis
        _redis = redis.Redis(host=host, port=port, decode_responses=True,
                             socket_connect_timeout=1, socket_timeout=1)
        _redis.ping()
        log.info("Redis connected: %s:%d", host, port)
    except Exception as e:
        log.warning("Redis unavailable (%s), running without cache", e)
        _redis = None


def redis_metrics() -> dict[str, Any]:
    """Return live Redis memory/key metrics for the pipeline dashboard.

    Missing Redis or a command failure reports status in-band. The dashboard is
    diagnostic; Redis problems must not break unrelated web UI routes.
    """
    if _redis is None:
        return {"enabled": False, "status": "disabled", "error": None}
    try:
        memory = _redis.info("memory")  # type: ignore[union-attr]
        keyspace = _redis.info("keyspace")  # type: ignore[union-attr]
        clients = _redis.info("clients")  # type: ignore[union-attr]
        dbsize = _redis.dbsize()  # type: ignore[union-attr]
        db0 = keyspace.get("db0", {}) if isinstance(keyspace, dict) else {}
        if not isinstance(db0, dict):
            db0 = {}
        used_memory = _int_or_none(memory.get("used_memory"))
        maxmemory = _int_or_none(memory.get("maxmemory"))
        return {
            "enabled": True,
            "status": "ok",
            "error": None,
            "used_memory_bytes": used_memory,
            "used_memory_human": memory.get("used_memory_human"),
            "used_memory_peak_bytes": _int_or_none(memory.get("used_memory_peak")),
            "used_memory_dataset_bytes": _int_or_none(
                memory.get("used_memory_dataset")
            ),
            "maxmemory_bytes": maxmemory,
            "memory_utilization": (
                used_memory / maxmemory if used_memory is not None and maxmemory else None
            ),
            "maxmemory_policy": memory.get("maxmemory_policy"),
            "fragmentation_ratio": _float_or_none(
                memory.get("mem_fragmentation_ratio")
            ),
            "key_count": _int_or_none(dbsize),
            "expires_count": _int_or_none(db0.get("expires")),
            "avg_ttl_ms": _int_or_none(db0.get("avg_ttl")),
            "connected_clients": _int_or_none(clients.get("connected_clients")),
        }
    except Exception as e:
        return {"enabled": True, "status": "error", "error": str(e)}


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


def memoize_meta(key: str, fetch_fn: Callable[[], Any], ttl: int = TTL_MB,
                 *, fresh: bool = False) -> Any:
    """Return cached `meta:<key>` or call `fetch_fn()` and cache the result.

    Concurrent non-fresh misses for the same key share one process-local
    fill. This still works when Redis is absent: overlapping callers share
    the in-flight result, while a later call retries because nothing was
    persisted.

    `fresh=True` skips the cache read and re-fetches live, then repopulates
    the cache with the fresh result. Use this on write paths (e.g. POST
    handlers that persist metadata into Postgres) where a 24h-old snapshot
    would silently bake stale artist/title/track data into the pipeline
    DB. Every `fresh=True` call still warms the cache for subsequent GETs.
    """
    if fresh:
        result = fetch_fn()
        meta_set(key, result, ttl)
        return result

    cached = meta_get(key)
    if cached is not None:
        return cached

    with _metadata_flights_lock:
        flight = _metadata_flights.get(key)
        if flight is None:
            flight = _MetadataFlight()
            _metadata_flights[key] = flight
            leader = True
        else:
            leader = False

    if not leader:
        flight.done.wait()
        if flight.error is not None:
            raise flight.error
        # The leader publishes a cache-shaped snapshot, never its mutable
        # working object. Every follower owns a separate nested copy so a
        # route overlay cannot leak into another caller's response.
        return copy.deepcopy(flight.result)

    try:
        # Close the stale-miss race: another process or a just-completed
        # local flight may have populated Redis between our first miss and
        # election. Only the elected leader performs this second read.
        cached = meta_get(key)
        if cached is not None:
            flight.result = copy.deepcopy(cached)
            return cached

        result = fetch_fn()
        snapshot = copy.deepcopy(result)
        meta_set(key, snapshot, ttl)
        flight.result = snapshot
        return result
    except BaseException as exc:
        # BaseException is deliberate: SystemExit/KeyboardInterrupt-style
        # termination must not strand followers or poison the key forever.
        flight.error = exc
        raise
    finally:
        with _metadata_flights_lock:
            if _metadata_flights.get(key) is flight:
                del _metadata_flights[key]
            flight.done.set()


# ── Invalidation ──────────────────────────────────────────────────────


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

