#!/usr/bin/env python3
"""Generated contract checks for path-scoped post-import Jellyfin updates."""

from __future__ import annotations

from email.message import Message
import io
import json
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz
from lib.config import CratediggerConfig
from lib.util import trigger_jellyfin_scan


_SAFE_SEGMENT = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-",
    min_size=1,
    max_size=64,
).filter(lambda value: value not in {".", ".."})
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


def _cfg(token: str) -> CratediggerConfig:
    return CratediggerConfig(
        beets_directory="/music",
        jellyfin_url="http://jellyfin:8096",
        jellyfin_token=token,
        jellyfin_path_map="/music:/jellyfin/music",
    )


class TestTargetedJellyfinRefreshGenerated(unittest.TestCase):
    @given(artist=_SAFE_SEGMENT, album=_SAFE_SEGMENT, token=_TOKEN)
    def test_media_update_contains_only_the_changed_album(
        self,
        artist: str,
        album: str,
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
            result = trigger_jellyfin_scan(
                _cfg(token), f"/music/{artist}/{album}")

        self.assertIsNone(result)  # HTTP 204 means queued, never converged.
        self.assertEqual(len(calls), 1)
        request, timeout = calls[0]
        self.assertEqual(
            request.full_url,
            "http://jellyfin:8096/Library/Media/Updated",
        )
        self.assertIsInstance(request.data, bytes)
        assert isinstance(request.data, bytes)
        self.assertEqual(
            json.loads(request.data),
            {"Updates": [{
                "Path": f"/jellyfin/music/{artist}/{album}",
                "UpdateType": "Modified",
            }]},
        )
        self.assertNotIn("/Items/", request.full_url)
        self.assertNotEqual(
            request.full_url, "http://jellyfin:8096/Library/Refresh")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("X-emby-token"), token)
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(timeout, 10)

    @given(
        artist=_SAFE_SEGMENT,
        album=_SAFE_SEGMENT,
        token=_TOKEN,
        failure=st.sampled_from(("url", "http", "runtime")),
        http_status=st.sampled_from((404, 429, 500, 503)),
    )
    def test_targeted_refresh_contains_leaf_failures_without_broad_fallback(
        self,
        artist: str,
        album: str,
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
            result = trigger_jellyfin_scan(
                _cfg(token), f"/music/{artist}/{album}")

        self.assertIsNone(result)
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            calls[0].full_url,
            "http://jellyfin:8096/Library/Media/Updated",
        )
