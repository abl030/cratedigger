#!/usr/bin/env python3
"""Generated live-db metadata-mirror wiring contract."""

from __future__ import annotations

import ast
import inspect
import json
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz
from tests.test_web_cache import FakeRedis
from web import cache
from web.api_bases import PUBLIC_MB_ORIGIN
import web.api_bases
import web.discogs
import web.mb
import web.routes.browse
from web.routes.browse import get_artist_compare
from scripts.web_dev_server import (
    DevConfig,
    DevHTTPServer,
    DevHandler,
    configure_live_db_metadata,
)


def assert_metadata_wiring(config: DevConfig) -> None:
    """Configured origins must be exact and missing values must not stay stale."""
    expected_mb = config.mb_api or urllib.parse.urljoin(
        f"{PUBLIC_MB_ORIGIN.rstrip('/')}/", "ws/2",
    )
    assert web.mb.MB_API_BASE == expected_mb
    assert web.discogs.DISCOGS_API_BASE == config.discogs_api


def assert_missing_discogs_blocks(call_route: Callable[[], None]) -> None:
    """A missing mirror must reject before any warm-cache route result."""
    try:
        call_route()
    except web.discogs.DiscogsMirrorNotConfigured:
        return
    raise AssertionError("warm metadata cache bypassed missing Discogs config")


_DISCOGS_ROUTE_CACHE_USERS = {
    "get_artist_compare",
    "get_browse_resolve",
}


def assert_discogs_route_cache_inventory(source: str) -> None:
    """Every route-level cache with a transitive Discogs call guards first."""
    tree = ast.parse(source)
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    def calls_discogs(
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        seen: set[str],
    ) -> bool:
        if node.name in seen:
            return False
        seen.add(node.name)
        for descendant in ast.walk(node):
            if not isinstance(descendant, ast.Call):
                continue
            fn = descendant.func
            if (
                isinstance(fn, ast.Attribute)
                and isinstance(fn.value, ast.Name)
                and fn.value.id == "discogs_api"
                and fn.attr != "require_mirror_configured"
            ):
                return True
            if (
                isinstance(fn, ast.Name)
                and fn.id in functions
                and calls_discogs(functions[fn.id], seen)
            ):
                return True
        return False

    cached_routes: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
    for name, node in functions.items():
        has_route_cache = any(
            isinstance(descendant, ast.Call)
            and isinstance(descendant.func, ast.Attribute)
            and descendant.func.attr == "memoize_meta"
            for descendant in ast.walk(node)
        )
        if has_route_cache and calls_discogs(node, set()):
            cached_routes[name] = node

    if set(cached_routes) != _DISCOGS_ROUTE_CACHE_USERS:
        raise AssertionError(
            "Discogs-dependent route cache inventory drifted: "
            f"{sorted(cached_routes)} != {sorted(_DISCOGS_ROUTE_CACHE_USERS)}"
        )

    for name, node in cached_routes.items():
        guard_lines = [
            descendant.lineno
            for descendant in ast.walk(node)
            if isinstance(descendant, ast.Call)
            and isinstance(descendant.func, ast.Attribute)
            and isinstance(descendant.func.value, ast.Name)
            and descendant.func.value.id == "discogs_api"
            and descendant.func.attr == "require_mirror_configured"
        ]
        cache_lines = [
            descendant.lineno
            for descendant in ast.walk(node)
            if isinstance(descendant, ast.Call)
            and isinstance(descendant.func, ast.Attribute)
            and descendant.func.attr == "memoize_meta"
        ]
        if not guard_lines or min(guard_lines) >= min(cache_lines):
            raise AssertionError(
                f"{name} must validate Discogs before route-cache dispatch"
            )


class _RouteHandler:
    def __init__(self) -> None:
        self.payload: dict[str, Any] | None = None

    def _json(self, payload: dict[str, Any], status: int = 200) -> None:
        if status != 200:
            raise AssertionError(f"unexpected route status {status}")
        self.payload = payload

    def _error(self, message: str, status: int = 400) -> None:
        raise AssertionError(f"unexpected route error {status}: {message}")


class _QuietDevHandler(DevHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        del format, args


_HOST = st.text(
    alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
    min_size=1,
    max_size=12,
)
_ORIGIN = st.builds(
    lambda scheme, host, port: f"{scheme}://{host}.test:{port}",
    st.sampled_from(("http", "https")),
    _HOST,
    st.integers(min_value=1, max_value=65535),
)


def _config(*, mb_api: str | None, discogs_api: str | None) -> DevConfig:
    return DevConfig(
        data="live-db",
        scenario="generated",
        prod_base_url="https://music.ablz.au",
        dsn="postgresql://unused-by-metadata-wiring",
        beets_db=None,
        mb_api=mb_api,
        discogs_api=discogs_api,
        redis_host=None,
        redis_port=6379,
    )


class TestLiveDbMetadataWiringGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.saved = (
            web.mb.MB_API_BASE,
            web.discogs.DISCOGS_API_BASE,
        )
        self.saved_redis = cache._redis

    def tearDown(self) -> None:
        web.mb.MB_API_BASE, web.discogs.DISCOGS_API_BASE = self.saved
        cache._redis = self.saved_redis

    @given(
        stale_mb=_ORIGIN,
        stale_discogs=_ORIGIN,
        mb_origin=st.one_of(st.none(), _ORIGIN),
        discogs_origin=st.one_of(st.none(), _ORIGIN),
    )
    def test_each_configuration_exactly_replaces_prior_process_state(
        self,
        stale_mb: str,
        stale_discogs: str,
        mb_origin: str | None,
        discogs_origin: str | None,
    ) -> None:
        configure_live_db_metadata(_config(
            mb_api=f"{stale_mb}/old-ws",
            discogs_api=f"{stale_discogs}/old-api",
        ))
        config = _config(
            mb_api=f"{mb_origin}/ws/2" if mb_origin else None,
            discogs_api=discogs_origin,
        )
        configure_live_db_metadata(config)
        assert_metadata_wiring(config)

    @given(
        mbid=_HOST,
        discogs_id=st.integers(min_value=1, max_value=2_000_000_000),
        artist_name=_HOST,
    )
    def test_missing_discogs_rejects_before_arbitrary_warm_compare_cache(
        self, mbid: str, discogs_id: int, artist_name: str,
    ) -> None:
        config = _config(mb_api=None, discogs_api=None)
        configure_live_db_metadata(config)
        cache._redis = FakeRedis()
        cache.meta_set(
            f"artist:compare:v7:{mbid}:{discogs_id}",
            {
                "both": [],
                "mb_unpaired": [],
                "discogs_unpaired": [],
                "discogs_ungrouped_releases": [],
            },
        )
        cache.meta_set(f"mb:artist:{mbid}:name", artist_name)
        cache.meta_set(f"discogs:artist:{discogs_id}:name", artist_name)
        handler = _RouteHandler()
        params = {
            "name": [artist_name],
            "mbid": [mbid],
            "discogs_id": [str(discogs_id)],
        }

        assert_missing_discogs_blocks(
            lambda: get_artist_compare(handler, params),  # type: ignore[arg-type]
        )
        self.assertIsNone(handler.payload)


class TestBrowseResolveWarmCacheGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.saved_base = web.discogs.DISCOGS_API_BASE
        self.saved_mb_base = web.mb.MB_API_BASE
        self.saved_redis = cache._redis
        configure_live_db_metadata(_config(mb_api=None, discogs_api=None))
        cache._redis = FakeRedis()
        self.server = DevHTTPServer(
            ("127.0.0.1", 0),
            _QuietDevHandler,
            _config(mb_api=None, discogs_api=None),
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        web.discogs.DISCOGS_API_BASE = self.saved_base
        web.mb.MB_API_BASE = self.saved_mb_base
        cache._redis = self.saved_redis

    @given(
        discogs_id=st.integers(min_value=1, max_value=2_000_000_000),
        kind=st.sampled_from(("release", "master", "unknown")),
        marker=_HOST,
    )
    def test_missing_discogs_returns_503_before_arbitrary_warm_resolver_cache(
        self, discogs_id: int, kind: str, marker: str,
    ) -> None:
        cache._redis = FakeRedis()
        cache_key = f"browse-resolve:v2:discogs:{kind}:{discogs_id}"
        cache.meta_set(cache_key, {
            "source": "discogs",
            "kind": "master" if kind == "master" else "release",
            "artist_id": marker,
            "artist_name": marker,
            "is_va": False,
            "target_identity_kind": "work" if kind == "master" else "release",
            "expand_id": str(discogs_id),
            "leaf_id": None if kind == "master" else str(discogs_id),
        })
        self.assertIn(f"meta:{cache_key}", cache._redis._store)  # type: ignore[union-attr]

        url = (
            f"{self.base}/api/browse/resolve?source=discogs&"
            f"id={discogs_id}&kind={kind}"
        )
        # The pre-push gate runs every generated module in parallel. Keep this
        # bounded, but allow enough scheduling headroom for the server thread
        # when the host is saturated by the sharded fuzz burst.
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(url, timeout=10)
        self.assertEqual(raised.exception.code, 503)
        raised.exception.close()


class TestDiscogsRouteCacheInventory(unittest.TestCase):
    def test_exact_discogs_dependent_route_cache_inventory_is_guarded(self) -> None:
        assert_discogs_route_cache_inventory(inspect.getsource(web.routes.browse))

    def test_checker_rejects_resolver_guard_removed(self) -> None:
        source = inspect.getsource(web.routes.browse)
        guard = (
            '    if source == "discogs":\n'
            "        discogs_api.require_mirror_configured()\n"
        )
        mutant = source.replace(guard, "", 1)
        self.assertNotEqual(mutant, source)
        with self.assertRaises(AssertionError):
            assert_discogs_route_cache_inventory(mutant)


class TestMetadataWiringCheckerKnownBad(unittest.TestCase):
    def setUp(self) -> None:
        self.saved = (
            web.mb.MB_API_BASE,
            web.discogs.DISCOGS_API_BASE,
        )

    def tearDown(self) -> None:
        web.mb.MB_API_BASE, web.discogs.DISCOGS_API_BASE = self.saved

    def test_checker_rejects_swapped_origins(self) -> None:
        config = _config(
            mb_api="https://mb.test/ws/2",
            discogs_api="https://discogs.test",
        )
        web.mb.MB_API_BASE = config.discogs_api or ""
        web.discogs.DISCOGS_API_BASE = config.mb_api
        with self.assertRaises(AssertionError):
            assert_metadata_wiring(config)

    def test_checker_rejects_stale_discogs_when_configuration_is_missing(
        self,
    ) -> None:
        config = _config(mb_api=None, discogs_api=None)
        web.mb.MB_API_BASE = urllib.parse.urljoin(
            f"{PUBLIC_MB_ORIGIN.rstrip('/')}/", "ws/2",
        )
        web.discogs.DISCOGS_API_BASE = "https://stale-discogs.test"
        with self.assertRaises(AssertionError):
            assert_metadata_wiring(config)

    def test_missing_mb_uses_the_canonical_public_ws2_declaration(self) -> None:
        config = _config(mb_api=None, discogs_api=None)
        sentinel = "https://canonical-mb.test/custom-ws2"
        saved_public_base = web.api_bases.PUBLIC_MB_WS2_BASE
        try:
            web.api_bases.PUBLIC_MB_WS2_BASE = sentinel
            configure_live_db_metadata(config)
        finally:
            web.api_bases.PUBLIC_MB_WS2_BASE = saved_public_base
        self.assertEqual(web.mb.MB_API_BASE, sentinel)

    def test_warm_cache_guard_checker_rejects_a_silent_route(self) -> None:
        with self.assertRaises(AssertionError):
            assert_missing_discogs_blocks(lambda: None)


if __name__ == "__main__":
    unittest.main()
