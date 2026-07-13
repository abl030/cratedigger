#!/usr/bin/env python3
"""Generated live-db metadata-mirror wiring contract."""

from __future__ import annotations

import unittest

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz
import web.discogs
import web.mb
from scripts.web_dev_server import (
    DevConfig,
    configure_live_db_metadata,
)


def assert_metadata_wiring(config: DevConfig) -> None:
    """Configured origins must be exact and missing values must not stay stale."""
    expected_mb = config.mb_api or web.mb.DEFAULT_MB_API_BASE
    assert web.mb.MB_API_BASE == expected_mb
    assert web.discogs.DISCOGS_API_BASE == config.discogs_api


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

    def tearDown(self) -> None:
        web.mb.MB_API_BASE, web.discogs.DISCOGS_API_BASE = self.saved

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
        web.mb.MB_API_BASE = web.mb.DEFAULT_MB_API_BASE
        web.discogs.DISCOGS_API_BASE = "https://stale-discogs.test"
        with self.assertRaises(AssertionError):
            assert_metadata_wiring(config)


if __name__ == "__main__":
    unittest.main()
