"""slskd HTTP pool sizing tests."""

from __future__ import annotations

import logging
import os
import unittest
from dataclasses import replace
from typing import Any

import requests
import requests.adapters

import cratedigger
from lib.config import CratediggerConfig
from lib.slskd_client import (
    SLSKD_HTTP_TIMEOUT_S,
    SLSKD_HTTP_POOL_ADMIN_SLACK,
    configure_slskd_http_pool,
    derive_slskd_http_pool_size,
)
from unittest.mock import MagicMock, Mock, patch


class FakeSession:
    def __init__(self) -> None:
        self.adapters: dict[str, requests.adapters.HTTPAdapter] = {}
        self.mounted: list[tuple[str, requests.adapters.HTTPAdapter]] = []
        self.hooks = {"response": lambda response, *args, **kwargs: response.raise_for_status()}

    def mount(self, prefix: str, adapter: requests.adapters.HTTPAdapter) -> None:
        self.adapters[prefix] = adapter
        self.mounted.append((prefix, adapter))


class FakeApi:
    def __init__(self, session: FakeSession) -> None:
        self.session = session


class FakeSlskdClient:
    def __init__(self) -> None:
        self._session = FakeSession()
        self.users = FakeApi(self._session)
        self.searches = FakeApi(self._session)
        self.transfers = FakeApi(self._session)


def _cfg(**overrides):
    cfg = CratediggerConfig()
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


class TestSlskdHttpPoolSizing(unittest.TestCase):
    def test_pool_size_is_derived_from_concurrency_values(self):
        cfg = _cfg(
            browse_global_max_workers=32,
            search_max_inflight=4,
            page_size=10,
        )

        self.assertEqual(
            derive_slskd_http_pool_size(cfg),
            32 + 4 + 10 + SLSKD_HTTP_POOL_ADMIN_SLACK,
        )

    def test_configures_http_and_https_adapters_with_blocking_pool(self):
        client = FakeSlskdClient()
        cfg = _cfg(browse_global_max_workers=32, search_max_inflight=4, page_size=10)

        result = configure_slskd_http_pool(client, cfg)

        self.assertTrue(result.configured)
        self.assertEqual(result.sessions_configured, 1)
        self.assertGreaterEqual(result.pool_size, 46)
        for prefix in ("http://", "https://"):
            adapter: Any = client._session.adapters[prefix]
            self.assertEqual(adapter._pool_connections, result.pool_size)
            self.assertEqual(adapter._pool_maxsize, result.pool_size)
            self.assertTrue(adapter._pool_block)

    def test_minimal_concurrency_still_gets_headroom(self):
        cfg = _cfg(browse_global_max_workers=1, search_max_inflight=1, page_size=1)

        self.assertEqual(
            derive_slskd_http_pool_size(cfg),
            1 + 1 + 1 + SLSKD_HTTP_POOL_ADMIN_SLACK,
        )

    def test_missing_session_logs_diagnostic_without_crashing(self):
        with self.assertLogs("cratedigger", level=logging.WARNING) as captured:
            result = configure_slskd_http_pool(object(), _cfg())

        self.assertFalse(result.configured)
        self.assertEqual(result.sessions_configured, 0)
        self.assertIn("Could not configure slskd HTTP pool", captured.output[0])

    def test_installed_slskd_client_shape_is_configurable_without_network(self):
        # IMPORTANT: import as `conftest`, NOT `from tests import conftest`.
        # Both work, but they resolve to DIFFERENT module instances in
        # sys.modules (one loaded via the `tests/` sys.path entry, one via
        # the `tests` package), and conftest's top-level code runs twice.
        # The second run captures `slskd_api` AFTER other tests have mocked
        # it in sys.modules, so the snapshot is a MagicMock. Test files that
        # import conftest do so as `import conftest` (no package prefix), so
        # match that.
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import conftest

        slskd_api = conftest._real_slskd_api
        assert slskd_api is not None and not isinstance(slskd_api, Mock), (
            f"real slskd_api snapshot missing — conftest captured "
            f"{type(slskd_api).__name__!r} ({slskd_api!r}); "
            "tests mocked sys.modules['slskd_api'] before conftest ran"
        )

        client = slskd_api.SlskdClient(
            host="http://localhost:5030",
            api_key="test-key",
        )
        result = configure_slskd_http_pool(client, _cfg())

        self.assertTrue(result.configured)
        self.assertGreaterEqual(result.sessions_configured, 1)
        adapter: Any = client.users.session.adapters["http://"]
        self.assertEqual(adapter._pool_maxsize, result.pool_size)
        self.assertTrue(adapter._pool_block)
        self.assertTrue(hasattr(adapter, "timeout"))

    def test_cratedigger_client_factory_configures_pool_once(self):
        cfg = _cfg(
            slskd_host_url="http://slskd.example",
            slskd_api_key="secret",
            slskd_url_base="/base",
        )
        client = MagicMock()

        with patch.object(cratedigger.slskd_api, "SlskdClient", return_value=client) as cls, \
             patch.object(cratedigger, "configure_slskd_http_pool") as configure:
            result = cratedigger._create_slskd_client(cfg)

        self.assertIs(result, client)
        cls.assert_called_once_with(
            host="http://slskd.example",
            api_key="secret",
            url_base="/base",
            timeout=SLSKD_HTTP_TIMEOUT_S,
        )
        configure.assert_called_once_with(client, cfg)

    def test_http_error_response_hook_closes_response_before_reraising(self):
        class SentinelHttpError(Exception):
            pass

        client = FakeSlskdClient()
        configure_slskd_http_pool(client, _cfg())
        response = Mock()
        error = SentinelHttpError("boom")
        response.raise_for_status.side_effect = error

        with self.assertRaises(SentinelHttpError):
            client._session.hooks["response"](response)

        response.close.assert_called_once_with()

    def test_successful_response_hook_does_not_close_response(self):
        client = FakeSlskdClient()
        configure_slskd_http_pool(client, _cfg())
        response = Mock()
        response.raise_for_status.return_value = None

        client._session.hooks["response"](response)

        response.close.assert_not_called()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
