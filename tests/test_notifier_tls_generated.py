#!/usr/bin/env python3
"""Generated fail-closed TLS contract for notifier HTTP leaves (issue #663).

The deterministic pin lives in ``tests/test_util.py``. This property drives
each real token-bearing Plex/Jellyfin urllib leaf with representative URL,
token, and payload variation. Both raw and urllib-wrapped certificate failures
must escape after exactly one default-context request.
"""

from __future__ import annotations

import ssl
import unittest
import urllib.error
from dataclasses import dataclass
from unittest.mock import patch

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from hypothesis import given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib import util


@dataclass(frozen=True)
class UrlopenAttempt:
    """The observable request contract at urllib's network edge."""

    timeout: object
    keyword_names: frozenset[str]


def assert_single_verified_attempt(attempts: list[UrlopenAttempt]) -> None:
    """Reject retry or custom-context notifier traces.

    The ordinary ``urlopen(req, timeout=15)`` form creates Python's default
    CA-verified context. A custom context is deliberately forbidden here: it
    can silently turn a fail-closed certificate error into a token-bearing
    request to an unverified peer.
    """
    if len(attempts) != 1:
        raise AssertionError(f"expected exactly one notifier attempt, got {attempts!r}")
    attempt = attempts[0]
    if attempt.timeout != 15:
        raise AssertionError(f"notifier timeout drifted: {attempt!r}")
    if attempt.keyword_names != frozenset({"timeout"}):
        raise AssertionError(f"notifier used non-default context/options: {attempt!r}")


_SAFE_TEXT = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-",
    min_size=1,
    max_size=12,
)
_LEAVES = st.sampled_from(("plex_xml", "plex_put", "jellyfin_get", "jellyfin_post"))


def _cfg(token: str) -> CratediggerConfig:
    return CratediggerConfig(
        plex_url="https://plex.example.test",
        plex_token=f"plex-{token}",
        jellyfin_url="https://jellyfin.example.test",
        jellyfin_token=f"jellyfin-{token}",
    )


def _invoke_leaf(
    leaf: str,
    cfg: CratediggerConfig,
    path_segment: str,
    query: str,
    payload_value: str,
) -> None:
    if leaf == "plex_xml":
        util._plex_fetch_xml(
            cfg, f"/library/sections/{path_segment}/search", query=query)
    elif leaf == "plex_put":
        util._plex_put(cfg, f"/library/sections/{path_segment}/all", id=query)
    elif leaf == "jellyfin_get":
        util._jellyfin_get_json(cfg, f"/Items/{path_segment}", searchTerm=query)
    else:
        util._jellyfin_post_json(
            cfg, f"/Items/{path_segment}", {"Id": payload_value})


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_single_verified_attempt_checker_rejects_context_bearing_attempt(self) -> None:
        with self.assertRaises(AssertionError):
            assert_single_verified_attempt([
                UrlopenAttempt(15, frozenset({"timeout", "context"})),
            ])

    def test_single_verified_attempt_checker_rejects_retry_trace(self) -> None:
        with self.assertRaises(AssertionError):
            assert_single_verified_attempt([
                UrlopenAttempt(15, frozenset({"timeout"})),
                UrlopenAttempt(15, frozenset({"timeout"})),
            ])


class TestGeneratedNotifierTlsFailClosed(unittest.TestCase):
    @given(
        leaf=_LEAVES,
        path_segment=_SAFE_TEXT,
        token=_SAFE_TEXT,
        query=_SAFE_TEXT,
        payload_value=_SAFE_TEXT,
        failure_shape=st.sampled_from(("raw", "urllib_wrapped")),
    )
    def test_certificate_error_has_one_default_context_attempt(
        self,
        leaf: str,
        path_segment: str,
        token: str,
        query: str,
        payload_value: str,
        failure_shape: str,
    ) -> None:
        raw_error = ssl.SSLCertVerificationError(1, "certificate verify failed")
        error = (raw_error if failure_shape == "raw"
                 else urllib.error.URLError(raw_error))
        with patch("lib.util.urllib.request.urlopen", side_effect=error) as urlopen:
            with self.assertRaises(type(error)) as raised:
                _invoke_leaf(leaf, _cfg(token), path_segment, query, payload_value)

        self.assertIs(raised.exception, error)
        attempts = [
            UrlopenAttempt(
                timeout=call.kwargs.get("timeout"),
                keyword_names=frozenset(call.kwargs),
            )
            for call in urlopen.call_args_list
        ]
        assert_single_verified_attempt(attempts)

        request, = urlopen.call_args.args
        if leaf.startswith("plex"):
            self.assertIn(f"X-Plex-Token=plex-{token}", request.full_url)
        else:
            self.assertEqual(
                request.get_header("X-emby-token"), f"jellyfin-{token}")
        if leaf == "plex_put":
            self.assertEqual(request.get_method(), "PUT")
        elif leaf == "jellyfin_post":
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.data, f'{{"Id": "{payload_value}"}}'.encode())
        else:
            self.assertEqual(request.get_method(), "GET")
