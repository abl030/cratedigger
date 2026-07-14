#!/usr/bin/env python3
"""Generated contract checks for targeted post-import Jellyfin refreshes."""

from __future__ import annotations

from email.message import Message
import io
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz
from lib.config import CratediggerConfig
from lib.util import trigger_jellyfin_scan


_SAFE_ID = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-",
    min_size=1,
    max_size=64,
)
_TOKEN = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-",
    min_size=1,
    max_size=64,
)


class _QueuedResponse:
    status = 204

    def __enter__(self) -> "_QueuedResponse":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def read(self) -> bytes:
        return b""


def _cfg(library_id: str, token: str) -> CratediggerConfig:
    return CratediggerConfig(
        jellyfin_url="http://jellyfin:8096",
        jellyfin_token=token,
        jellyfin_library_id=library_id,
    )


class TestTargetedJellyfinRefreshGenerated(unittest.TestCase):
    @given(library_id=_SAFE_ID, token=_TOKEN)
    def test_targeted_refresh_is_exact_safe_scoped_submission(
        self,
        library_id: str,
        token: str,
    ) -> None:
        calls: list[tuple[urllib.request.Request, float]] = []

        def urlopen(
            request: urllib.request.Request,
            timeout: float,
        ) -> _QueuedResponse:
            calls.append((request, timeout))
            return _QueuedResponse()

        with (
            patch("lib.util.urllib.request.urlopen", side_effect=urlopen),
            self.assertLogs("cratedigger", level="INFO"),
        ):
            result = trigger_jellyfin_scan(_cfg(library_id, token))

        self.assertIsNone(result)  # HTTP 204 means queued, never converged.
        self.assertEqual(len(calls), 1)
        request, timeout = calls[0]
        self.assertEqual(
            request.full_url,
            f"http://jellyfin:8096/Items/{library_id}/Refresh"
            "?metadataRefreshMode=Default"
            "&imageRefreshMode=Default"
            "&replaceAllMetadata=false"
            "&replaceAllImages=false",
        )
        self.assertNotIn("recursive", request.full_url)
        self.assertNotIn("/Library/Refresh", request.full_url)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("X-emby-token"), token)
        self.assertEqual(timeout, 10)

    @given(
        library_id=_SAFE_ID,
        token=_TOKEN,
        failure=st.sampled_from(("url", "http", "runtime")),
        http_status=st.sampled_from((404, 429, 500, 503)),
    )
    def test_targeted_refresh_contains_leaf_failures_without_broad_fallback(
        self,
        library_id: str,
        token: str,
        failure: str,
        http_status: int,
    ) -> None:
        calls: list[urllib.request.Request] = []

        def urlopen(
            request: urllib.request.Request,
            timeout: float,
        ) -> _QueuedResponse:
            calls.append(request)
            if failure == "url":
                raise urllib.error.URLError("unreachable")
            if failure == "http":
                raise urllib.error.HTTPError(
                    request.full_url,
                    http_status,
                    "Unavailable",
                    Message(),
                    io.BytesIO(),
                )
            raise RuntimeError("transport broke")

        with (
            patch("lib.util.urllib.request.urlopen", side_effect=urlopen),
            self.assertLogs("cratedigger", level="WARNING"),
        ):
            result = trigger_jellyfin_scan(_cfg(library_id, token))

        self.assertIsNone(result)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("/Library/Refresh", calls[0].full_url)
