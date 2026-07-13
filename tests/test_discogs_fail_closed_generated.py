#!/usr/bin/env python3
"""Generated fail-closed matrix for all cached Discogs adapters."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.test_discogs_fail_closed import (
    PUBLIC_CACHED_DISCOGS_ADAPTERS,
    LeafDiscogsMirror,
    assert_missing_discogs_blocks,
    call_public_cached_adapter,
)
from tests.test_web_cache import FakeRedis
from web import cache, discogs


_QUERY = st.text(
    alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
    min_size=1,
    max_size=24,
)


class TestGeneratedPublicDiscogsFailClosed(unittest.TestCase):
    def setUp(self) -> None:
        self.saved_base = discogs.DISCOGS_API_BASE
        self.saved_redis = cache._redis

    def tearDown(self) -> None:
        discogs.DISCOGS_API_BASE = self.saved_base
        cache._redis = self.saved_redis

    @given(
        query=_QUERY,
        entity_id=st.integers(min_value=1, max_value=2_000_000_000),
    )
    def test_missing_configuration_precedes_every_arbitrary_warm_cache_entry(
        self, query: str, entity_id: int,
    ) -> None:
        for surface in PUBLIC_CACHED_DISCOGS_ADAPTERS:
            mirror = LeafDiscogsMirror()
            cache._redis = FakeRedis()
            discogs.DISCOGS_API_BASE = "https://discogs-mirror.test"
            with patch(
                "web.discogs.urllib.request.urlopen",
                side_effect=mirror.urlopen,
            ):
                call_public_cached_adapter(
                    surface, query=query, entity_id=entity_id,
                )
            if not mirror.urls or cache._redis.dbsize() == 0:  # type: ignore[union-attr]
                raise AssertionError(f"{surface} did not populate its metadata cache")

            discogs.DISCOGS_API_BASE = None

            def call_warm_adapter() -> object:
                return call_public_cached_adapter(
                    surface, query=query, entity_id=entity_id,
                )

            assert_missing_discogs_blocks(call_warm_adapter)


if __name__ == "__main__":
    unittest.main()
