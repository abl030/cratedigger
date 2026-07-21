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
from typing import Any, Callable, TYPE_CHECKING

import msgspec

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

if TYPE_CHECKING:
    import redis as _redis_mod

_redis: "_redis_mod.Redis | None" = None


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
        _redis_call("ping")
        log.info("Redis connected: %s:%d", host, port)
    except Exception as e:
        log.warning("Redis unavailable (%s), running without cache", e)
        _redis = None


def _redis_call(method_name: str, /, *args: object, **kwargs: object) -> object:
    """Call a bound redis-py client method via ``getattr``.

    redis-py types every sync command with ``**kwargs: Unknown`` in its
    signature (shared with the async client), so a direct method
    reference (``_redis.info(...)``) propagates Unknown through pyright
    strict mode at every call site. ``getattr`` retrieves the exact same
    bound method at runtime (identical behavior) but types as ``Any``
    under typeshed's two-argument ``getattr`` overload, breaking the
    Unknown cascade without a suppression comment — same technique as
    ``lib.pipeline_db._shared.pg_execute_values``. Callers narrow the
    ``object`` result via ``_redis_dict`` / ``msgspec.convert`` same as
    before.
    """
    assert _redis is not None
    return getattr(_redis, method_name)(*args, **kwargs)


def _redis_dict(value: object) -> dict[str, object]:
    """Best-effort validation of a redis-py ``INFO``-style reply.

    redis-py types every sync command's return as ``Awaitable[Any] | Any``
    (the same method also serves the async client), so this is the
    wire-boundary decode site for this module. A malformed/non-dict shape
    degrades to ``{}`` — the same per-field fail-open contract the rest of
    this diagnostic-only endpoint already uses.
    """
    try:
        return msgspec.convert(value, type=dict[str, object])
    except msgspec.ValidationError:
        return {}


def redis_metrics() -> dict[str, Any]:
    """Return live Redis memory/key metrics for the pipeline dashboard.

    Missing Redis or a command failure reports status in-band. The dashboard is
    diagnostic; Redis problems must not break unrelated web UI routes.
    """
    if _redis is None:
        return {"enabled": False, "status": "disabled", "error": None}
    try:
        # redis-py types sync command returns as ``ResponseT`` (an
        # Awaitable union); narrow each to the sync shape we consume.
        memory_resp = _redis_call("info", "memory")
        keyspace_resp = _redis_call("info", "keyspace")
        clients_resp = _redis_call("info", "clients")
        dbsize = _redis_call("dbsize")
        memory = _redis_dict(memory_resp)
        clients = _redis_dict(clients_resp)
        keyspace = _redis_dict(keyspace_resp)
        db0 = _redis_dict(keyspace.get("db0", {}))
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
        raw = _redis.get(f"{_META_PREFIX}{key}")
        if not isinstance(raw, str):
            # decode_responses=True yields str hits; None on miss.
            return None
        return json.loads(raw)
    except Exception:
        return None


def meta_set(key: str, value: Any, ttl: int = TTL_MB) -> None:
    """Set in the pure metadata cache. Key is prefixed with `meta:`."""
    if _redis is None:
        return
    try:
        _redis.setex(
            f"{_META_PREFIX}{key}", ttl, json.dumps(value))
    except Exception:
        pass


def memoize_meta[T](key: str, fetch_fn: Callable[[], T], ttl: int = TTL_MB,
                 *, fresh: bool = False) -> T:
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
            resp = _redis_call("scan", cursor=cursor, match=pattern, count=100)
            # ``scan``'s sync return shape is ``(next_cursor, keys)``; the
            # redis-py stubs type it as ``Awaitable[Any] | Any`` (the same
            # method serves the async client), so this is the wire-boundary
            # decode site. A malformed shape raises and is swallowed by the
            # outer fail-safe ``except`` below, same as the prior
            # ``isinstance`` guard's silent break.
            cursor, keys = msgspec.convert(resp, type=tuple[int, list[str]])
            if keys:
                _redis.delete(*keys)
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

