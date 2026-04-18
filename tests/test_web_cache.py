"""Tests for the web metadata cache — the `meta:` namespace.

The `meta:` namespace caches PURE MusicBrainz / Discogs metadata only.
It is deliberately separate from the old routing-level `web:` namespace
which baked per-user overlay state (`pipeline_status`, `in_library`,
`library_rank`, ...) into the cached payload. See issue #101.

These tests pin two properties:
  1. `meta_get` / `meta_set` / `memoize_meta` round-trip correctly and
     no-op when Redis is unavailable.
  2. Group invalidations for `pipeline` / `library` / `mb` / `discogs`
     do NOT touch `meta:` keys. MB/Discogs metadata is effectively
     immutable on our daily-sync'd mirrors, so pipeline-state changes
     must not flush the expensive metadata cache.
"""

from __future__ import annotations

import os
import sys
import time
import unittest
from typing import Any


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "web"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class FakeRedis:
    """Minimal Redis stub: in-memory store with TTL tracking and SCAN."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[str, float | None]] = {}

    def ping(self) -> bool:
        return True

    def get(self, key: str) -> str | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires = entry
        if expires is not None and time.time() > expires:
            self._store.pop(key, None)
            return None
        return value

    def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = (value, time.time() + ttl)

    def delete(self, *keys: str) -> int:
        n = 0
        for k in keys:
            if k in self._store:
                self._store.pop(k, None)
                n += 1
        return n

    def scan(self, cursor: int = 0, match: str | None = None,
             count: int = 100) -> tuple[int, list[str]]:
        import fnmatch
        pattern = match or "*"
        keys = [k for k in self._store if fnmatch.fnmatch(k, pattern)]
        return 0, keys


class _CacheTestBase(unittest.TestCase):

    def setUp(self) -> None:
        import web.cache as cache
        self.cache = cache
        self.fake = FakeRedis()
        self._saved = cache._redis
        cache._redis = self.fake

    def tearDown(self) -> None:
        self.cache._redis = self._saved


class TestMetaNamespace(_CacheTestBase):
    """meta_get / meta_set / memoize_meta round-trip."""

    def test_meta_set_then_get_returns_value(self) -> None:
        self.cache.meta_set("mb:release:abc", {"id": "abc", "title": "Foo"})
        self.assertEqual(
            self.cache.meta_get("mb:release:abc"),
            {"id": "abc", "title": "Foo"},
        )

    def test_meta_get_returns_none_on_miss(self) -> None:
        self.assertIsNone(self.cache.meta_get("mb:release:missing"))

    def test_meta_namespace_is_prefixed(self) -> None:
        """meta_set must store under meta: so it doesn't collide with
        the old web:* routing cache namespace."""
        self.cache.meta_set("mb:release:abc", {"x": 1})
        self.assertIn("meta:mb:release:abc", self.fake._store)
        self.assertNotIn("mb:release:abc", self.fake._store)
        self.assertNotIn("web:mb:release:abc", self.fake._store)

    def test_memoize_meta_calls_fetch_on_miss_only(self) -> None:
        calls: list[str] = []

        def _fetch() -> dict[str, Any]:
            calls.append("fetch")
            return {"id": "abc"}

        first = self.cache.memoize_meta("mb:release:abc", _fetch)
        second = self.cache.memoize_meta("mb:release:abc", _fetch)
        self.assertEqual(first, {"id": "abc"})
        self.assertEqual(second, {"id": "abc"})
        self.assertEqual(calls, ["fetch"],
                         "fetch must only run on the first call")

    def test_memoize_meta_honours_ttl(self) -> None:
        """TTL=1 means a second call after > 1s re-fetches."""
        calls: list[str] = []

        def _fetch() -> dict[str, Any]:
            calls.append("fetch")
            return {"n": len(calls)}

        self.cache.memoize_meta("mb:release:abc", _fetch, ttl=1)
        # Advance fake-redis clock
        self.fake._store["meta:mb:release:abc"] = (
            self.fake._store["meta:mb:release:abc"][0],
            time.time() - 1,
        )
        second = self.cache.memoize_meta("mb:release:abc", _fetch, ttl=1)
        self.assertEqual(len(calls), 2,
                         "expired entry must trigger a second fetch")
        self.assertEqual(second, {"n": 2})

    def test_meta_ops_noop_when_redis_absent(self) -> None:
        """With _redis=None (CLI / no Redis), meta helpers degrade cleanly."""
        self.cache._redis = None
        self.cache.meta_set("mb:release:abc", {"x": 1})
        self.assertIsNone(self.cache.meta_get("mb:release:abc"))

        calls: list[str] = []

        def _fetch() -> dict[str, Any]:
            calls.append("fetch")
            return {"ok": True}

        first = self.cache.memoize_meta("mb:release:abc", _fetch)
        second = self.cache.memoize_meta("mb:release:abc", _fetch)
        self.assertEqual(first, {"ok": True})
        self.assertEqual(second, {"ok": True})
        self.assertEqual(
            len(calls), 2,
            "no-op cache must call fetch every time (never cache-hit)")


class TestStartupFlushLeavesMetaAlone(unittest.TestCase):
    """Server startup flushes legacy `web:*` routing cache but MUST NOT
    touch the pure `meta:*` metadata namespace. Flushing it on every
    `systemctl restart soularr-web` would defeat the 24h cache —
    Codex review on PR #104.
    """

    def test_main_does_not_invalidate_meta_pattern(self) -> None:
        """Source-level regression guard. `main()` in web/server.py
        must not call `cache.invalidate_pattern('meta:*')`. If a future
        change wants to flush metadata, do it per-key via a version
        prefix bump, not a blanket startup wipe."""
        import ast
        import os

        server_py = os.path.join(
            os.path.dirname(__file__), "..", "web", "server.py")
        with open(server_py) as f:
            tree = ast.parse(f.read())

        main_fn = next(
            (n for n in ast.walk(tree)
             if isinstance(n, ast.FunctionDef) and n.name == "main"),
            None,
        )
        self.assertIsNotNone(main_fn,
                             "couldn't locate main() in web/server.py")
        assert main_fn is not None  # narrow for pyright

        for node in ast.walk(main_fn):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            is_invalidate = (
                isinstance(fn, ast.Attribute)
                and fn.attr == "invalidate_pattern"
            )
            if not is_invalidate:
                continue
            if not node.args:
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                self.assertNotEqual(
                    arg.value, "meta:*",
                    "main() must not flush meta:* on startup — that "
                    "defeats the 24h MB/Discogs metadata cache on every "
                    "restart. See Codex review on PR #104.")


class TestMemoizeMetaFresh(_CacheTestBase):
    """`fresh=True` is the bypass used by POST handlers that persist
    metadata into the pipeline DB. A 24h stale cache read on an add /
    upgrade path would silently bake yesterday's title / tracks into
    `album_requests`. Flagged by Codex review on issue #101.
    """

    def test_fresh_skips_cache_read_and_repopulates(self) -> None:
        calls: list[int] = []

        def _fetch_v1() -> dict:
            calls.append(1)
            return {"title": "Old Title"}

        def _fetch_v2() -> dict:
            calls.append(2)
            return {"title": "Corrected Title"}

        # Warm the cache via a normal GET-style call.
        first = self.cache.memoize_meta("mb:release:abc", _fetch_v1)
        self.assertEqual(first, {"title": "Old Title"})
        self.assertEqual(calls, [1])

        # fresh=True MUST bypass the read and re-fetch.
        second = self.cache.memoize_meta("mb:release:abc", _fetch_v2,
                                         fresh=True)
        self.assertEqual(second, {"title": "Corrected Title"},
                         "fresh=True must ignore the cached value")
        self.assertEqual(calls, [1, 2])

        # And it must have repopulated the cache — a subsequent GET
        # without fresh= sees the fresh value. Warms the cache on writes.
        third = self.cache.memoize_meta("mb:release:abc", _fetch_v2)
        self.assertEqual(third, {"title": "Corrected Title"})
        self.assertEqual(calls, [1, 2],
                         "cache must have been repopulated by fresh=True")


class TestMetaIsolatedFromGroupInvalidation(_CacheTestBase):
    """Core guarantee for issue #101: MB/Discogs metadata cache must
    survive pipeline-state / library-state invalidation events.

    Before the fix, /api/release/<id> etc. cached the baked overlay
    under web:*. A pipeline POST called invalidate_groups("pipeline",
    "library", "mb", "discogs") and dropped the entire entry. After
    the fix the overlay isn't cached — only pure metadata is — and
    group invalidations must NOT reach into the meta: namespace.
    """

    def _seed(self) -> None:
        # meta: entries we must PRESERVE across group invalidations
        self.cache.meta_set("mb:release:abc", {"id": "abc"})
        self.cache.meta_set("mb:release-group:rg-1", {"id": "rg-1"})
        self.cache.meta_set("discogs:release:99", {"id": 99})
        self.cache.meta_set("discogs:master:44", {"id": 44})
        # web: entries we SHOULD drop on invalidation (legacy routing cache)
        self.fake._store["web:/api/release/abc"] = ("{}", None)
        self.fake._store["web:/api/pipeline/status"] = ("{}", None)
        self.fake._store["web:/api/beets/search?q=x"] = ("{}", None)

    def _assert_meta_intact(self) -> None:
        self.assertEqual(self.cache.meta_get("mb:release:abc"), {"id": "abc"})
        self.assertEqual(self.cache.meta_get("mb:release-group:rg-1"), {"id": "rg-1"})
        self.assertEqual(self.cache.meta_get("discogs:release:99"), {"id": 99})
        self.assertEqual(self.cache.meta_get("discogs:master:44"), {"id": 44})

    def test_invalidate_pipeline_group_leaves_meta_intact(self) -> None:
        self._seed()
        self.cache.invalidate_groups("pipeline")
        self._assert_meta_intact()

    def test_invalidate_library_group_leaves_meta_intact(self) -> None:
        self._seed()
        self.cache.invalidate_groups("library")
        self._assert_meta_intact()

    def test_invalidate_mb_group_leaves_meta_intact(self) -> None:
        """Legacy `mb` group wiped web:/api/release* etc. The `meta:`
        namespace for MB metadata must NOT be affected — it's the
        expensive pure-metadata cache we want to keep warm across
        unrelated pipeline writes."""
        self._seed()
        self.cache.invalidate_groups("mb")
        self._assert_meta_intact()

    def test_invalidate_discogs_group_leaves_meta_intact(self) -> None:
        self._seed()
        self.cache.invalidate_groups("discogs")
        self._assert_meta_intact()

    def test_invalidate_all_four_groups_leaves_meta_intact(self) -> None:
        self._seed()
        self.cache.invalidate_groups("pipeline", "library", "mb", "discogs")
        self._assert_meta_intact()

    def test_invalidate_web_pattern_leaves_meta_intact(self) -> None:
        """server.py does `invalidate_pattern('web:*')` on startup to
        flush stale routing-cache responses. That MUST NOT reach into
        meta:, or every deploy would dump 24h of MB/Discogs cache."""
        self._seed()
        self.cache.invalidate_pattern("web:*")
        self._assert_meta_intact()


if __name__ == "__main__":
    unittest.main()
