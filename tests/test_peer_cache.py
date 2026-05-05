from __future__ import annotations

import os
import time
import unittest
from typing import Any, cast
from unittest.mock import MagicMock, patch

import redis

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.peer_cache import PeerCache, PeerCacheStats, connect_from_config
from lib.search import SearchResult


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, tuple[Any, float | None]] = {}
        self.get_calls = 0
        self.setex_calls: list[tuple[str, int, Any]] = []
        self.delete_calls: list[tuple[str, ...]] = []

    def ping(self) -> bool:
        return True

    def get(self, key: str) -> Any | None:
        self.get_calls += 1
        entry = self.store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and expires_at <= time.time():
            self.store.pop(key, None)
            return None
        return value

    def setex(self, key: str, ttl: int, value: Any) -> None:
        self.setex_calls.append((key, ttl, value))
        self.store[key] = (value, time.time() + ttl)

    def delete(self, *keys: str) -> int:
        self.delete_calls.append(keys)
        deleted = 0
        for key in keys:
            if key in self.store:
                deleted += 1
                self.store.pop(key, None)
        return deleted

    def ttl(self, key: str) -> int:
        entry = self.store.get(key)
        if entry is None:
            return -2
        _value, expires_at = entry
        if expires_at is None:
            return -1
        return max(0, int(expires_at - time.time()))


class RaisingRedis(FakeRedis):
    def get(self, key: str) -> Any | None:
        self.get_calls += 1
        raise RuntimeError("redis unavailable")


class TestPeerCacheDirectories(unittest.TestCase):
    def test_directory_round_trips_as_compressed_bytes_and_clears_negative(self) -> None:
        redis = FakeRedis()
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        directory = {
            "directory": "Music:Artist/Album",
            "files": [
                {"filename": "01 - Track.flac", "size": 123, "bitRate": 991},
            ],
        }
        redis.setex("peer_dir_neg:user:Music:Artist/Album", 60, b"1")

        cache.set_directory("user", "Music:Artist/Album", directory)

        key = "peer_dir:user:Music:Artist/Album"
        raw = redis.get(key)
        self.assertIsInstance(raw, bytes)
        self.assertGreater(redis.ttl(key), 0)
        self.assertEqual(cache.get_directory("user", "Music:Artist/Album"), directory)
        self.assertEqual(cache.stats.cache_pos_hits, 1)
        self.assertIn(("peer_dir_neg:user:Music:Artist/Album",), redis.delete_calls)

    def test_missing_directory_counts_miss(self) -> None:
        cache = PeerCache(FakeRedis(), ttl_seconds=60, speed_ttl_seconds=10)

        self.assertIsNone(cache.get_directory("user", "missing"))

        self.assertEqual(cache.stats.cache_misses, 1)

    def test_malformed_directory_payload_counts_miss(self) -> None:
        redis = FakeRedis()
        redis.setex("peer_dir:user:dir", 60, b"not zstd")
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)

        self.assertIsNone(cache.get_directory("user", "dir"))

        self.assertEqual(cache.stats.cache_misses, 1)


class TestPeerCacheNegatives(unittest.TestCase):
    def test_negative_write_and_hit_use_ttl(self) -> None:
        redis = FakeRedis()
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)

        cache.set_negative("user", "dir")

        self.assertGreater(redis.ttl("peer_dir_neg:user:dir"), 0)
        self.assertTrue(cache.has_negative("user", "dir"))
        self.assertEqual(cache.stats.cache_neg_hits, 1)

    def test_negative_miss_counts_miss(self) -> None:
        cache = PeerCache(FakeRedis(), ttl_seconds=60, speed_ttl_seconds=10)

        self.assertFalse(cache.has_negative("user", "dir"))

        self.assertEqual(cache.stats.cache_misses, 1)


class TestPeerCacheScalars(unittest.TestCase):
    def test_upload_speed_and_dir_count_round_trip_as_ints(self) -> None:
        redis = FakeRedis()
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)

        cache.set_upload_speed("user", 320)
        cache.set_dir_audio_count("user", "dir", 14)

        self.assertEqual(redis.get("peer_speed:user"), 320)
        self.assertGreater(redis.ttl("peer_speed:user"), 0)
        self.assertEqual(redis.get("peer_dir_count:user:dir"), 14)
        self.assertEqual(cache.get_upload_speed("user"), 320)
        self.assertEqual(cache.get_dir_audio_count("user", "dir"), 14)
        self.assertEqual(cache.stats.cache_pos_hits, 2)

    def test_scalar_misses_count_misses(self) -> None:
        cache = PeerCache(FakeRedis(), ttl_seconds=60, speed_ttl_seconds=10)

        self.assertIsNone(cache.get_upload_speed("user"))
        self.assertIsNone(cache.get_dir_audio_count("user", "dir"))

        self.assertEqual(cache.stats.cache_misses, 2)


class TestPeerCacheFailures(unittest.TestCase):
    def test_command_failure_trips_fuse_and_avoids_repeated_gets(self) -> None:
        redis = RaisingRedis()
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)

        self.assertIsNone(cache.get_directory("user", "dir-a"))
        self.assertIsNone(cache.get_directory("user", "dir-b"))

        self.assertEqual(redis.get_calls, 1)
        self.assertEqual(cache.stats.cache_misses, 2)
        self.assertEqual(cache.stats.cache_errors, 1)
        self.assertEqual(cache.stats.cache_fuse_tripped, 1)

    def test_forked_caches_share_fuse_state(self) -> None:
        redis = RaisingRedis()
        parent = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        child = parent.fork()

        self.assertIsNone(child.get_directory("user", "dir-a"))
        self.assertIsNone(parent.get_directory("user", "dir-b"))

        self.assertEqual(redis.get_calls, 1)
        self.assertEqual(child.stats.cache_fuse_tripped, 1)
        self.assertEqual(parent.stats.cache_misses, 1)

    def test_write_failure_is_nonfatal_and_counted(self) -> None:
        class WriteRaisingRedis(FakeRedis):
            def setex(self, key: str, ttl: int, value: Any) -> None:
                raise RuntimeError("write failed")

        cache = PeerCache(WriteRaisingRedis(), ttl_seconds=60, speed_ttl_seconds=10)

        cache.set_upload_speed("user", 1)

        self.assertEqual(cache.stats.cache_write_errors, 1)

    def test_stats_merge_adds_fields(self) -> None:
        stats = PeerCacheStats(cache_pos_hits=1, cache_errors=2)
        stats.merge(PeerCacheStats(cache_pos_hits=3, cache_neg_hits=4, cache_misses=5))

        self.assertEqual(stats.cache_pos_hits, 4)
        self.assertEqual(stats.cache_neg_hits, 4)
        self.assertEqual(stats.cache_misses, 5)
        self.assertEqual(stats.cache_errors, 2)


class TestPeerCacheConnection(unittest.TestCase):
    def test_connect_from_config_uses_binary_client_and_tight_timeouts(self) -> None:
        created: dict[str, Any] = {}

        class CapturingRedis(FakeRedis):
            def __init__(self, **kwargs: Any) -> None:
                super().__init__()
                created.update(kwargs)

        cfg = CratediggerConfig(
            peer_cache_redis_host="10.0.0.5",
            peer_cache_redis_port=6380,
            peer_cache_ttl_seconds=123,
            peer_cache_speed_ttl_seconds=45,
            peer_cache_redis_connect_timeout_ms=250,
            peer_cache_redis_operation_timeout_ms=75,
        )

        with patch("lib.peer_cache.redis.Redis", CapturingRedis):
            cache = connect_from_config(cfg)

        self.assertIsNotNone(cache.client)
        self.assertEqual(cache.ttl_seconds, 123)
        self.assertEqual(cache.speed_ttl_seconds, 45)
        self.assertEqual(created["host"], "10.0.0.5")
        self.assertEqual(created["port"], 6380)
        self.assertFalse(created["decode_responses"])
        self.assertEqual(created["socket_connect_timeout"], 0.25)
        self.assertEqual(created["socket_timeout"], 0.075)


class TestSearchResultScalarMerge(unittest.TestCase):
    def _ctx(self, cache: PeerCache) -> CratediggerContext:
        return CratediggerContext(
            cfg=CratediggerConfig(),
            slskd=object(),
            pipeline_db_source=MagicMock(),
            peer_cache=cache,
        )

    def test_merge_search_result_fills_missing_scalars_from_redis(self) -> None:
        import cratedigger

        redis = FakeRedis()
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        cache.set_upload_speed("user1", 500)
        cache.set_dir_audio_count("user1", "dirA", 12)
        ctx = self._ctx(cache)
        result = SearchResult(
            album_id=1,
            success=True,
            cache_entries={"user1": {"flac": ["dirA"]}},
            upload_speeds={},
            dir_audio_counts={},
        )

        cratedigger._merge_search_result(result, ctx)

        self.assertEqual(ctx.user_upload_speed["user1"], 500)
        self.assertEqual(ctx.search_dir_audio_count["user1"]["dirA"], 12)
        self.assertEqual(ctx.cache_pos_hits, 2)

    def test_merge_search_result_writes_current_scalar_maxima_to_redis(self) -> None:
        import cratedigger

        redis = FakeRedis()
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        cache.set_upload_speed("user1", 100)
        cache.set_dir_audio_count("user1", "dirA", 3)
        ctx = self._ctx(cache)
        result = SearchResult(
            album_id=1,
            success=True,
            cache_entries={"user1": {"flac": ["dirA"]}},
            upload_speeds={"user1": 800},
            dir_audio_counts={"user1": {"dirA": 14}},
        )

        cratedigger._merge_search_result(result, ctx)

        self.assertEqual(ctx.user_upload_speed["user1"], 800)
        self.assertEqual(ctx.search_dir_audio_count["user1"]["dirA"], 14)
        self.assertEqual(redis.get("peer_speed:user1"), 800)
        self.assertEqual(redis.get("peer_dir_count:user1:dirA"), 14)

    def test_merge_search_result_preserves_higher_redis_scalars(self) -> None:
        import cratedigger

        redis = FakeRedis()
        cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        cache.set_upload_speed("user1", 900)
        cache.set_dir_audio_count("user1", "dirA", 20)
        ctx = self._ctx(cache)
        result = SearchResult(
            album_id=1,
            success=True,
            cache_entries={"user1": {"flac": ["dirA"]}},
            upload_speeds={"user1": 100},
            dir_audio_counts={"user1": {"dirA": 5}},
        )

        cratedigger._merge_search_result(result, ctx)

        self.assertEqual(ctx.user_upload_speed["user1"], 900)
        self.assertEqual(ctx.search_dir_audio_count["user1"]["dirA"], 20)
        self.assertEqual(redis.get("peer_speed:user1"), 900)
        self.assertEqual(redis.get("peer_dir_count:user1:dirA"), 20)


@unittest.skipUnless(
    os.environ.get("CRATEDIGGER_REAL_REDIS_PORT"),
    "set CRATEDIGGER_REAL_REDIS_PORT to run the real Redis peer-cache slice",
)
class TestPeerCacheRealRedis(unittest.TestCase):
    def test_real_redis_stores_directory_as_bytes_with_ttl(self) -> None:
        port = int(os.environ["CRATEDIGGER_REAL_REDIS_PORT"])
        host = os.environ.get("CRATEDIGGER_REAL_REDIS_HOST", "127.0.0.1")
        client = redis.Redis(host=host, port=port, decode_responses=False)
        cache = PeerCache(client, ttl_seconds=60, speed_ttl_seconds=10)
        username = f"real-test-{int(time.time() * 1000)}"
        file_dir = "Music:Artist/Album"
        key = f"peer_dir:{username}:{file_dir}"
        directory = {"directory": file_dir, "files": [{"filename": "01.flac"}]}
        self.addCleanup(client.delete, key)

        cache.set_directory(username, file_dir, directory)

        raw = client.get(key)
        self.assertIsInstance(raw, bytes)
        self.assertGreater(cast(int, client.ttl(key)), 0)
        self.assertEqual(cache.get_directory(username, file_dir), directory)
