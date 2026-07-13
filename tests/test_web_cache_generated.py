#!/usr/bin/env python3
"""Generated invariants for process-local metadata single-flight.

Across arbitrary cold/warm waves and cache keys, each cold key is fetched once,
every caller receives the value for its own key, and mutable results never
alias. Deterministic tests in ``test_web_cache`` pin stale-miss, failure,
BaseException, Redis-down, fresh-bypass, and abandoned-waiter paths.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st

from tests.test_web_cache import FakeRedis
from web import cache


class _GeneratedAbort(BaseException):
    """Non-Exception generated leader failure."""


def assert_singleflight_wave(
    expected_fetches: dict[str, int],
    actual_fetches: dict[str, int],
    requested_keys: list[str],
    results: list[dict[str, Any]],
) -> None:
    """Check fetch multiplicity, key routing, and caller-owned values."""
    if actual_fetches != expected_fetches:
        raise AssertionError(
            f"expected fetches {expected_fetches!r}, got {actual_fetches!r}")
    if [result["key"] for result in results] != requested_keys:
        raise AssertionError("a caller received another cache key's value")
    if len({id(result) for result in results}) != len(results):
        raise AssertionError("mutable result dictionaries alias across callers")
    rows = [result["rows"] for result in results]
    if len({id(value) for value in rows}) != len(rows):
        raise AssertionError("nested mutable results alias across callers")


class TestGeneratedMetadataSingleFlight(unittest.TestCase):
    def setUp(self) -> None:
        self._saved_redis = cache._redis
        cache._redis = FakeRedis()

    def tearDown(self) -> None:
        cache._redis = self._saved_redis

    @given(
        waves=st.lists(
            st.lists(
                st.sampled_from(("artist:a", "artist:b", "artist:c")),
                min_size=1,
                max_size=6,
            ),
            min_size=1,
            max_size=4,
        ),
        clear_before=st.lists(st.booleans(), min_size=1, max_size=4),
    )
    def test_arbitrary_key_and_caller_waves_share_one_cold_fill(
        self, waves: list[list[str]], clear_before: list[bool],
    ) -> None:
        redis = cache._redis
        assert isinstance(redis, FakeRedis)
        redis._store.clear()
        fetches: dict[str, int] = {}
        expected_fetches: dict[str, int] = {}
        warm: set[str] = set()
        lock = threading.Lock()

        for wave_number, keys in enumerate(waves):
            if clear_before[wave_number % len(clear_before)]:
                redis._store.clear()
                warm.clear()

            expected_wave_fetches = set(keys) - warm
            for key in expected_wave_fetches:
                expected_fetches[key] = expected_fetches.get(key, 0) + 1

            start = threading.Barrier(len(keys) + 1)

            def call(key: str) -> dict[str, Any]:
                start.wait(timeout=5)

                def fetch() -> dict[str, Any]:
                    with lock:
                        fetches[key] = fetches.get(key, 0) + 1
                        fill = fetches[key]
                    # Keep the leader in-flight so duplicate-key callers
                    # exercise the follower path instead of a later warm hit.
                    time.sleep(0.002)
                    return {"key": key, "rows": [{"fill": fill}]}

                return cache.memoize_meta(key, fetch)

            with ThreadPoolExecutor(max_workers=len(keys)) as pool:
                futures = [pool.submit(call, key) for key in keys]
                start.wait(timeout=5)
                results = [future.result(timeout=5) for future in futures]

            assert_singleflight_wave(
                expected_fetches, fetches, keys, results,
            )
            warm.update(keys)

    @given(
        key=st.sampled_from(("artist:a", "artist:b", "artist:c")),
        cached=st.recursive(
            st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=8)),
            lambda children: st.one_of(
                st.lists(children, max_size=4),
                st.dictionaries(st.text(max_size=5), children, max_size=4),
            ),
            max_leaves=8,
        ).filter(lambda value: value is not None),
    )
    def test_stale_miss_recheck_returns_arbitrary_just_filled_value(
        self, key: str, cached: Any,
    ) -> None:
        reads = [None, cached]
        with patch.object(
            cache, "meta_get", side_effect=lambda _key: reads.pop(0),
        ), patch.object(cache, "meta_set") as meta_set:
            result = cache.memoize_meta(
                key, lambda: self.fail("stale miss performed a duplicate fill"),
            )
        self.assertEqual(result, cached)
        meta_set.assert_not_called()

    @given(
        key=st.sampled_from(("artist:a", "artist:b", "artist:c")),
        callers=st.integers(min_value=1, max_value=5),
        abort=st.booleans(),
    )
    def test_failure_wave_is_shared_and_arbitrary_keys_retry(
        self, key: str, callers: int, abort: bool,
    ) -> None:
        redis = cache._redis
        assert isinstance(redis, FakeRedis)
        redis._store.clear()
        failure: BaseException = (
            _GeneratedAbort("abort") if abort else RuntimeError("failure")
        )
        fetches = 0
        started = threading.Barrier(callers + 1)

        def call() -> Any:
            started.wait(timeout=5)

            def fail() -> Any:
                nonlocal fetches
                fetches += 1
                time.sleep(0.002)
                raise failure

            return cache.memoize_meta(key, fail)

        with ThreadPoolExecutor(max_workers=callers) as pool:
            futures = [pool.submit(call) for _ in range(callers)]
            started.wait(timeout=5)
            raised: list[BaseException] = []
            for future in futures:
                try:
                    future.result(timeout=5)
                except BaseException as exc:
                    raised.append(exc)

        self.assertEqual(fetches, 1)
        self.assertEqual(len(raised), callers)
        self.assertTrue(all(exc is failure for exc in raised))
        self.assertEqual(
            cache.memoize_meta(key, lambda: {"retry": key}),
            {"retry": key},
        )

    @given(
        key=st.sampled_from(("artist:a", "artist:b", "artist:c")),
        callers=st.integers(min_value=1, max_value=5),
    )
    def test_redis_down_shares_each_overlap_then_refetches(
        self, key: str, callers: int,
    ) -> None:
        cache._redis = None
        fetches = 0
        started = threading.Barrier(callers + 1)

        def call() -> dict[str, Any]:
            started.wait(timeout=5)

            def fetch() -> dict[str, Any]:
                nonlocal fetches
                fetches += 1
                time.sleep(0.002)
                return {"key": key, "rows": []}

            return cache.memoize_meta(key, fetch)

        with ThreadPoolExecutor(max_workers=callers) as pool:
            futures = [pool.submit(call) for _ in range(callers)]
            started.wait(timeout=5)
            results = [future.result(timeout=5) for future in futures]

        self.assertEqual(fetches, 1)
        self.assertEqual(len({id(result) for result in results}), callers)

        def later_fetch() -> dict[str, Any]:
            nonlocal fetches
            fetches += 1
            return {"key": key, "rows": []}

        cache.memoize_meta(key, later_fetch)
        self.assertEqual(fetches, 2)


class TestSingleFlightCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_duplicate_fetch(self) -> None:
        with self.assertRaises(AssertionError):
            assert_singleflight_wave(
                {"artist:a": 1},
                {"artist:a": 2},
                ["artist:a", "artist:a"],
                [
                    {"key": "artist:a", "rows": []},
                    {"key": "artist:a", "rows": []},
                ],
            )

    def test_checker_rejects_mutable_alias(self) -> None:
        shared: dict[str, Any] = {"key": "artist:a", "rows": []}
        with self.assertRaises(AssertionError):
            assert_singleflight_wave(
                {"artist:a": 1},
                {"artist:a": 1},
                ["artist:a", "artist:a"],
                [shared, shared],
            )


if __name__ == "__main__":
    unittest.main()
