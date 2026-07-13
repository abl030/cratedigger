#!/usr/bin/env python3
"""Generated live-db metadata-mirror wiring contract."""

from __future__ import annotations

import unittest
import urllib.parse
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
from web.routes.browse import get_artist_compare
from scripts.web_dev_server import (
    DevConfig,
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


class _RouteHandler:
    def __init__(self) -> None:
        self.payload: dict[str, Any] | None = None

    def _json(self, payload: dict[str, Any], status: int = 200) -> None:
        if status != 200:
            raise AssertionError(f"unexpected route status {status}")
        self.payload = payload

    def _error(self, message: str, status: int = 400) -> None:
        raise AssertionError(f"unexpected route error {status}: {message}")


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
            f"artist:compare:v4:{mbid}:{discogs_id}",
            {"both": [], "mb_only": [], "discogs_only": []},
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
