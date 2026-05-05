"""Tests for the bounded parallel browse fan-out primitive (issue #198 U2).

The fan-out function `_fanout_browse_users` lives in `lib/browse.py` next to
`_browse_directories`. It accepts a flat list of `(username, file_dir)` work
items and submits each to a bounded `ThreadPoolExecutor`. Successful results
are written into `ctx.folder_cache`.

There is no client-side wave deadline — slskd's per-peer TCP read timeout is
the only authority on hung peers (the previous client deadlines were starving
the pipeline; removed 2026-05-02).

These tests pin down:
  * happy-path bucket population
  * pre-create-bucket invariant (no `setdefault` race across futures)
  * empty work, all-exceptions tolerance
  * concurrency cap honored
  * 1-user × N-dirs race regression (the case the pre-create fixes)
"""

from __future__ import annotations

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock

from lib.browse import _fanout_browse_users, get_browse_coordinator
from lib.context import CratediggerContext
from lib.peer_cache import PeerCache
from tests.fakes import FakeSlskdAPI
from tests.test_peer_cache import FakeRedis


def _make_ctx(slskd: Any) -> CratediggerContext:
    """Minimal context wired to a slskd fake — only fields the fan-out reads."""
    return CratediggerContext(
        cfg=MagicMock(),
        slskd=slskd,
        pipeline_db_source=MagicMock(),
    )


def _make_directory(dir_path: str) -> dict[str, Any]:
    """Slskd-shaped directory listing (single track)."""
    return {
        "directory": dir_path,
        "files": [{"filename": "01 - Track.flac", "size": 100}],
    }


class TestFanoutBrowseHappyPath(unittest.TestCase):
    def test_populates_cache_for_all_user_dir_pairs(self):
        """5 users × 3 dirs all return immediately → 15 cache entries."""
        slskd = FakeSlskdAPI()
        users = [f"user{i}" for i in range(5)]
        dirs = ["A", "B", "C"]
        work = []
        for u in users:
            for d in dirs:
                slskd.users.set_directory(u, d, [_make_directory(d)])
                work.append((u, d))

        ctx = _make_ctx(slskd)
        _fanout_browse_users(work, slskd, ctx, max_workers=8)

        self.assertEqual(set(ctx.folder_cache.keys()), set(users))
        for u in users:
            self.assertEqual(set(ctx.folder_cache[u].keys()), set(dirs))

    def test_redis_positive_hit_warms_hot_cache_without_slskd_browse(self):
        slskd = FakeSlskdAPI()
        redis = FakeRedis()
        peer_cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        directory = _make_directory("A")
        peer_cache.set_directory("user1", "A", directory)

        ctx = _make_ctx(slskd)
        ctx.peer_cache = peer_cache
        result = _fanout_browse_users([("user1", "A")], slskd, ctx, max_workers=4)

        self.assertEqual(slskd.users.directory_calls, [])
        self.assertEqual(ctx.folder_cache["user1"]["A"], directory)
        self.assertEqual(result.browse_attempts, 0)
        self.assertEqual(result.negative_skips, set())
        self.assertEqual(ctx.cache_pos_hits, 1)

    def test_redis_negative_hit_skips_slskd_without_hot_cache_write(self):
        slskd = FakeSlskdAPI()
        redis = FakeRedis()
        peer_cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)
        peer_cache.set_negative("user1", "A")

        ctx = _make_ctx(slskd)
        ctx.peer_cache = peer_cache
        result = _fanout_browse_users([("user1", "A")], slskd, ctx, max_workers=4)

        self.assertEqual(slskd.users.directory_calls, [])
        self.assertEqual(ctx.folder_cache["user1"], {})
        self.assertEqual(result.browse_attempts, 0)
        self.assertEqual(result.negative_skips, {("user1", "A")})
        self.assertEqual(ctx.cache_neg_hits, 1)

    def test_empty_browse_writes_persistent_negative(self):
        slskd = FakeSlskdAPI()
        redis = FakeRedis()
        peer_cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)

        ctx = _make_ctx(slskd)
        ctx.peer_cache = peer_cache
        result = _fanout_browse_users([("user1", "A")], slskd, ctx, max_workers=4)

        self.assertEqual(result.browse_attempts, 1)
        self.assertIn("peer_dir_neg:user1:A", redis.store)

    def test_exception_browse_does_not_write_persistent_negative(self):
        slskd = FakeSlskdAPI()
        slskd.users.set_directory_error("user1", "A", RuntimeError("slskd down"))
        redis = FakeRedis()
        peer_cache = PeerCache(redis, ttl_seconds=60, speed_ttl_seconds=10)

        ctx = _make_ctx(slskd)
        ctx.peer_cache = peer_cache
        result = _fanout_browse_users([("user1", "A")], slskd, ctx, max_workers=4)

        self.assertEqual(result.browse_attempts, 1)
        self.assertNotIn("peer_dir_neg:user1:A", redis.store)

    def test_pre_creates_user_buckets_for_every_work_item(self):
        """Every user in the wave must have an inner dict before any future writes.

        Pin the contract that fixes the `setdefault + nested-write` race: the
        function must pre-create `ctx.folder_cache[user] = {}` for every user
        in the wave before submitting any future. We probe this by checking
        that the inner dict exists for users whose dirs ALL fail — if buckets
        were created lazily on success, those users would be absent from
        `folder_cache` entirely.
        """
        slskd = FakeSlskdAPI()
        slskd.users.set_directory("user_ok", "A", [_make_directory("A")])
        slskd.users.set_directory_error("user_fail", "B", Exception("peer gone"))
        work = [("user_ok", "A"), ("user_fail", "B")]

        ctx = _make_ctx(slskd)
        _fanout_browse_users(work, slskd, ctx, max_workers=4)

        self.assertIn("user_ok", ctx.folder_cache)
        self.assertIn("user_fail", ctx.folder_cache)
        self.assertEqual(ctx.folder_cache["user_fail"], {})


class TestFanoutBrowseEdgeCases(unittest.TestCase):
    def test_empty_work_list_returns_no_exception(self):
        slskd = FakeSlskdAPI()
        ctx = _make_ctx(slskd)
        _fanout_browse_users([], slskd, ctx, max_workers=4)
        self.assertEqual(ctx.folder_cache, {})

    def test_all_peers_fail_with_exceptions_no_writes(self):
        """Per-task exceptions are swallowed by `_browse_one`; folder_cache stays empty."""
        slskd = FakeSlskdAPI()
        slskd.users.set_directory_error("user1", "A", RuntimeError("x"))
        slskd.users.set_directory_error("user2", "B", ConnectionError("y"))
        work = [("user1", "A"), ("user2", "B")]

        ctx = _make_ctx(slskd)
        _fanout_browse_users(work, slskd, ctx, max_workers=4)

        # Buckets pre-created but empty; no per-(user,dir) write succeeded.
        self.assertEqual(ctx.folder_cache["user1"], {})
        self.assertEqual(ctx.folder_cache["user2"], {})


class TestFanoutBrowseConcurrencyCap(unittest.TestCase):
    def test_max_workers_caps_in_flight_directory_calls(self):
        slskd = FakeSlskdAPI()
        peak = 0
        in_flight = 0
        lock = threading.Lock()

        def probe(delta: int) -> None:
            nonlocal peak, in_flight
            with lock:
                in_flight += delta
                if in_flight > peak:
                    peak = in_flight

        slskd.users.in_flight_probe = probe

        work = []
        for i in range(50):
            u, d = f"u{i}", f"d{i}"
            slskd.users.set_directory(u, d, [_make_directory(d)])
            slskd.users.set_directory_delay(u, d, 0.05)  # hold each call long enough to overlap
            work.append((u, d))

        ctx = _make_ctx(slskd)
        _fanout_browse_users(work, slskd, ctx, max_workers=4)

        self.assertLessEqual(peak, 4, f"max_workers=4 cap was violated; peak={peak}")
        # All 50 work items completed (no client deadline anymore).
        self.assertEqual(len(ctx.folder_cache), 50)

    def test_cap_is_global_across_concurrent_fanout_callers(self):
        slskd = FakeSlskdAPI()
        peak = 0
        in_flight = 0
        lock = threading.Lock()

        def probe(delta: int) -> None:
            nonlocal peak, in_flight
            with lock:
                in_flight += delta
                peak = max(peak, in_flight)

        slskd.users.in_flight_probe = probe
        work_a = []
        work_b = []
        for i in range(20):
            user = f"u{i}"
            directory = f"d{i}"
            slskd.users.set_directory(user, directory, [_make_directory(directory)])
            slskd.users.set_directory_delay(user, directory, 0.05)
            (work_a if i % 2 == 0 else work_b).append((user, directory))

        ctx = _make_ctx(slskd)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(_fanout_browse_users, work_a, slskd, ctx, 4)
            second = pool.submit(_fanout_browse_users, work_b, slskd, ctx, 4)
            first.result()
            second.result()

        self.assertLessEqual(peak, 4, f"global browse cap was violated; peak={peak}")
        self.assertEqual(len(slskd.users.directory_calls), 20)

    def test_duplicate_cold_directory_is_single_flighted(self):
        slskd = FakeSlskdAPI()
        slskd.users.set_directory("user1", "Album", [_make_directory("Album")])
        slskd.users.set_directory_delay("user1", "Album", 0.05)
        ctx = _make_ctx(slskd)
        work = [("user1", "Album")]

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(_fanout_browse_users, work, slskd, ctx, 4)
            second = pool.submit(_fanout_browse_users, work, slskd, ctx, 4)
            first.result()
            second.result()

        self.assertEqual(slskd.users.directory_calls, [("user1", "Album")])
        self.assertIn("Album", ctx.folder_cache["user1"])

    def test_reusing_coordinator_with_different_capacity_fails_loudly(self):
        slskd = FakeSlskdAPI()
        ctx = _make_ctx(slskd)

        get_browse_coordinator(ctx, 4)

        with self.assertRaises(ValueError):
            get_browse_coordinator(ctx, 8)

    def test_first_time_coordinator_creation_is_single(self):
        slskd = FakeSlskdAPI()
        ctx = _make_ctx(slskd)
        coordinators = []

        def get_one():
            coordinators.append(get_browse_coordinator(ctx, 4))

        threads = [threading.Thread(target=get_one) for _ in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual({id(coordinator) for coordinator in coordinators}, {
            id(ctx.browse_coordinator),
        })


class TestFanoutBrowseRaceRegression(unittest.TestCase):
    def test_one_user_eight_dirs_no_lost_entries_across_iterations(self):
        """Regression for the `setdefault + nested-write` race.

        With one user contributing 8 different dirs, all 8 futures share the
        same inner dict. The pre-create-buckets step removes the race; this
        test pins that no entries are lost across many iterations.
        """
        for iteration in range(50):
            slskd = FakeSlskdAPI()
            user = "user1"
            dirs = [f"d{i}" for i in range(8)]
            for d in dirs:
                slskd.users.set_directory(user, d, [_make_directory(d)])
            work = [(user, d) for d in dirs]

            ctx = _make_ctx(slskd)
            _fanout_browse_users(work, slskd, ctx, max_workers=8)

            self.assertEqual(
                len(ctx.folder_cache[user]), 8,
                f"iteration {iteration}: expected 8 entries, got {len(ctx.folder_cache[user])}",
            )


if __name__ == "__main__":
    unittest.main()
