"""Redis-backed peer cache for the pipeline.

This module is deliberately separate from ``web.cache``. The web cache uses
JSON/text Redis values; peer directory payloads are binary msgpack compressed
with zstd.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import threading
from typing import Any, TYPE_CHECKING

import msgspec
import redis
import zstandard as zstd

if TYPE_CHECKING:
    from lib.config import CratediggerConfig


logger = logging.getLogger("cratedigger")


@dataclass
class PeerCacheStats:
    cache_pos_hits: int = 0
    cache_neg_hits: int = 0
    cache_misses: int = 0
    cache_errors: int = 0
    cache_fuse_tripped: int = 0
    cache_write_errors: int = 0

    def merge(self, other: "PeerCacheStats") -> None:
        self.cache_pos_hits += other.cache_pos_hits
        self.cache_neg_hits += other.cache_neg_hits
        self.cache_misses += other.cache_misses
        self.cache_errors += other.cache_errors
        self.cache_fuse_tripped += other.cache_fuse_tripped
        self.cache_write_errors += other.cache_write_errors

    def copy(self) -> "PeerCacheStats":
        return PeerCacheStats(
            cache_pos_hits=self.cache_pos_hits,
            cache_neg_hits=self.cache_neg_hits,
            cache_misses=self.cache_misses,
            cache_errors=self.cache_errors,
            cache_fuse_tripped=self.cache_fuse_tripped,
            cache_write_errors=self.cache_write_errors,
        )


class PeerCacheFuse:
    def __init__(self, *, fused: bool = False) -> None:
        self.fused = fused
        self.lock = threading.Lock()


class PeerCache:
    def __init__(
        self,
        client: Any | None,
        *,
        ttl_seconds: int,
        speed_ttl_seconds: int,
        stats: PeerCacheStats | None = None,
        fuse: PeerCacheFuse | None = None,
    ) -> None:
        self.client = client
        self.ttl_seconds = max(1, int(ttl_seconds))
        self.speed_ttl_seconds = max(1, int(speed_ttl_seconds))
        self.stats = stats if stats is not None else PeerCacheStats()
        self._fuse = fuse if fuse is not None else PeerCacheFuse(fused=client is None)
        if client is None:
            with self._fuse.lock:
                self._fuse.fused = True
        self._stats_lock = threading.Lock()

    @property
    def available(self) -> bool:
        return self.client is not None and not self._is_fused()

    def fork(self) -> "PeerCache":
        return PeerCache(
            self.client,
            ttl_seconds=self.ttl_seconds,
            speed_ttl_seconds=self.speed_ttl_seconds,
            fuse=self._fuse,
        )

    def drain_stats(self) -> PeerCacheStats:
        with self._stats_lock:
            stats = self.stats.copy()
            self.stats = PeerCacheStats()
            return stats

    def get_directory(self, username: str, file_dir: str) -> Any | None:
        raw = self._get(self._dir_key(username, file_dir))
        if raw is None:
            return None
        if not isinstance(raw, bytes | bytearray):
            self._record(cache_misses=1)
            return None
        try:
            payload = zstd.ZstdDecompressor().decompress(bytes(raw))
            directory = msgspec.msgpack.decode(payload)
        except Exception:
            logger.debug("Failed to decode peer_dir cache payload", exc_info=True)
            self._record(cache_misses=1)
            return None
        self._record(cache_pos_hits=1)
        return directory

    def set_directory(self, username: str, file_dir: str, directory: Any) -> None:
        try:
            payload = msgspec.msgpack.encode(directory)
            compressed = zstd.ZstdCompressor().compress(payload)
        except Exception:
            logger.debug("Failed to encode peer_dir cache payload", exc_info=True)
            self._record(cache_write_errors=1)
            return
        if self._setex(self._dir_key(username, file_dir), self.ttl_seconds, compressed):
            self._delete(self._neg_key(username, file_dir))

    def has_negative(self, username: str, file_dir: str) -> bool:
        raw = self._get(self._neg_key(username, file_dir))
        if raw is None:
            return False
        self._record(cache_neg_hits=1)
        return True

    def set_negative(self, username: str, file_dir: str) -> None:
        self._setex(self._neg_key(username, file_dir), self.ttl_seconds, b"1")

    def get_upload_speed(self, username: str) -> int | None:
        return self._get_int(self._speed_key(username))

    def set_upload_speed(self, username: str, speed: int) -> None:
        self._setex(self._speed_key(username), self.speed_ttl_seconds, int(speed))

    def get_dir_audio_count(self, username: str, file_dir: str) -> int | None:
        return self._get_int(self._count_key(username, file_dir))

    def set_dir_audio_count(self, username: str, file_dir: str, count: int) -> None:
        self._setex(self._count_key(username, file_dir), self.ttl_seconds, int(count))

    def _get_int(self, key: str) -> int | None:
        raw = self._get(key)
        if raw is None:
            return None
        try:
            value = int(raw)
        except (TypeError, ValueError):
            logger.debug("Failed to decode integer peer-cache value for %s", key)
            self._record(cache_misses=1)
            return None
        self._record(cache_pos_hits=1)
        return value

    def _get(self, key: str) -> Any | None:
        if self.client is None or self._is_fused():
            self._record(cache_misses=1)
            return None
        try:
            raw = self.client.get(key)
        except Exception:
            self._record(cache_misses=1)
            self._trip_fuse()
            return None
        if raw is None:
            self._record(cache_misses=1)
            return None
        return raw

    def _setex(self, key: str, ttl: int, value: Any) -> bool:
        if self.client is None or self._is_fused():
            return False
        try:
            self.client.setex(key, ttl, value)
        except Exception:
            self._record(cache_write_errors=1)
            self._trip_fuse()
            return False
        return True

    def _delete(self, *keys: str) -> None:
        if self.client is None or self._is_fused():
            return
        try:
            self.client.delete(*keys)
        except Exception:
            self._record(cache_write_errors=1)
            self._trip_fuse()

    def _trip_fuse(self) -> None:
        self._record(cache_errors=1)
        with self._fuse.lock:
            already_fused = self._fuse.fused
            self._fuse.fused = True
        if not already_fused:
            self._record(cache_fuse_tripped=1)

    def _is_fused(self) -> bool:
        with self._fuse.lock:
            return self._fuse.fused

    def _record(
        self,
        *,
        cache_pos_hits: int = 0,
        cache_neg_hits: int = 0,
        cache_misses: int = 0,
        cache_errors: int = 0,
        cache_fuse_tripped: int = 0,
        cache_write_errors: int = 0,
    ) -> None:
        with self._stats_lock:
            self.stats.cache_pos_hits += cache_pos_hits
            self.stats.cache_neg_hits += cache_neg_hits
            self.stats.cache_misses += cache_misses
            self.stats.cache_errors += cache_errors
            self.stats.cache_fuse_tripped += cache_fuse_tripped
            self.stats.cache_write_errors += cache_write_errors

    @staticmethod
    def _dir_key(username: str, file_dir: str) -> str:
        return f"peer_dir:{username}:{file_dir}"

    @staticmethod
    def _neg_key(username: str, file_dir: str) -> str:
        return f"peer_dir_neg:{username}:{file_dir}"

    @staticmethod
    def _speed_key(username: str) -> str:
        return f"peer_speed:{username}"

    @staticmethod
    def _count_key(username: str, file_dir: str) -> str:
        return f"peer_dir_count:{username}:{file_dir}"


def connect_from_config(cfg: CratediggerConfig) -> PeerCache:
    stats = PeerCacheStats()
    try:
        client = redis.Redis(
            host=cfg.peer_cache_redis_host,
            port=cfg.peer_cache_redis_port,
            socket_connect_timeout=cfg.peer_cache_redis_connect_timeout_ms / 1000,
            socket_timeout=cfg.peer_cache_redis_operation_timeout_ms / 1000,
            decode_responses=False,
        )
        # redis-py's Redis.ping declares `**kwargs: Unknown` upstream, so a
        # direct call propagates Unknown through pyright strict mode.
        # getattr retrieves the identical bound method (behaviorally
        # identical) but types as Any under typeshed's two-argument
        # getattr overload, breaking the Unknown cascade without a
        # suppression comment — same technique as
        # lib.beets_distance._item_from_path_fn.
        getattr(client, "ping")()
    except Exception:
        stats.cache_errors += 1
        logger.info(
            "Redis peer cache unavailable at %s:%s; running cold-cache",
            cfg.peer_cache_redis_host,
            cfg.peer_cache_redis_port,
            exc_info=True,
        )
        client = None
    else:
        logger.info(
            "Redis peer cache connected at %s:%s",
            cfg.peer_cache_redis_host,
            cfg.peer_cache_redis_port,
        )
    return PeerCache(
        client,
        ttl_seconds=cfg.peer_cache_ttl_seconds,
        speed_ttl_seconds=cfg.peer_cache_speed_ttl_seconds,
        stats=stats,
    )


def merge_stats_into_context(ctx: Any, stats: PeerCacheStats) -> None:
    ctx.cache_pos_hits += stats.cache_pos_hits
    ctx.cache_neg_hits += stats.cache_neg_hits
    ctx.cache_misses += stats.cache_misses
    ctx.cache_errors += stats.cache_errors
    ctx.cache_fuse_tripped += stats.cache_fuse_tripped
    ctx.cache_write_errors += stats.cache_write_errors


def drain_stats_into_context(ctx: Any, cache: PeerCache | None) -> None:
    if cache is None:
        return
    merge_stats_into_context(ctx, cache.drain_stats())
