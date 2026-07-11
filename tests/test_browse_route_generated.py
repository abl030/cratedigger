#!/usr/bin/env python3
"""Generated contract patrol for retryable MusicBrainz artist failures."""

from __future__ import annotations

import email.message
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from hypothesis import given, strategies as st

from tests import _hypothesis_profiles  # noqa: F401 — registers active profile
from web.routes.browse import get_artist


_CLEAN_ERROR = "MusicBrainz fallback unavailable, retry"
_NOT_FOUND_ERROR = "MusicBrainz artist not found"
_REJECTED_ERROR = "MusicBrainz request rejected"


def assert_clean_retryable_failure(status: int, data: dict, raw_reason: str) -> None:
    """Assert the stable route contract and absence of adapter details."""
    if status != 503:
        raise AssertionError(f"expected 503, got {status}")
    if data != {"error": _CLEAN_ERROR, "retryable": True}:
        raise AssertionError(f"unstable retry payload: {data!r}")
    if raw_reason in json.dumps(data):
        raise AssertionError("raw MusicBrainz transport reason leaked")


def assert_clean_http_failure(
    status: int,
    data: dict,
    upstream_status: int,
    raw_reason: str,
) -> None:
    """Assert stable status-aware handling for real HTTPError failures."""
    if upstream_status == 404:
        expected_status = 404
        expected_data = {"error": _NOT_FOUND_ERROR, "retryable": False}
    elif upstream_status == 429 or 500 <= upstream_status <= 599:
        expected_status = 503
        expected_data = {"error": _CLEAN_ERROR, "retryable": True}
    else:
        expected_status = upstream_status
        expected_data = {"error": _REJECTED_ERROR, "retryable": False}
    if status != expected_status or data != expected_data:
        raise AssertionError(
            f"unstable HTTP failure contract: status={status} data={data!r}"
        )
    if raw_reason in json.dumps(data):
        raise AssertionError("raw MusicBrainz HTTP reason leaked")


class _RecordingHandler:
    def __init__(self) -> None:
        self.status: int | None = None
        self.data: dict | None = None

    def _json(self, data: dict, status: int = 200) -> None:
        self.status = status
        self.data = data


class TestArtistMusicBrainzFailureGenerated(unittest.TestCase):
    ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"

    def test_contract_checker_rejects_known_bad_payload(self):
        raw_reason = "raw transport secret"
        with self.assertRaisesRegex(AssertionError, "unstable retry payload"):
            assert_clean_retryable_failure(
                503,
                {"error": f"<urlopen error {raw_reason}>"},
                raw_reason,
            )

    def test_http_contract_checker_rejects_known_bad_payload(self):
        raw_reason = "raw HTTP secret"
        with self.assertRaisesRegex(AssertionError, "unstable HTTP failure contract"):
            assert_clean_http_failure(
                503,
                {"error": f"HTTP Error 404: {raw_reason}", "retryable": True},
                404,
                raw_reason,
            )

    @given(
        failing_call=st.sampled_from(("release_groups", "official_releases")),
        reason_suffix=st.text(
            alphabet=st.characters(
                min_codepoint=0x20,
                max_codepoint=0x7E,
                blacklist_characters="\r\n",
            ),
            min_size=1,
            max_size=80,
        ),
    )
    def test_all_transport_reasons_and_both_calls_keep_stable_contract(
        self,
        failing_call: str,
        reason_suffix: str,
    ) -> None:
        raw_reason = f"raw-mb-transport-secret::{reason_suffix}"
        handler = _RecordingHandler()
        with patch("web.server.mb_api") as mock_mb:
            if failing_call == "release_groups":
                mock_mb.get_artist_release_groups.side_effect = URLError(raw_reason)
            else:
                mock_mb.get_artist_release_groups.return_value = []
                mock_mb.get_official_release_group_ids.side_effect = URLError(raw_reason)
            get_artist(handler, {}, self.ARTIST_ID)  # type: ignore[arg-type]

        assert handler.status is not None
        assert handler.data is not None
        assert_clean_retryable_failure(handler.status, handler.data, raw_reason)

    @given(
        failing_call=st.sampled_from(("release_groups", "official_releases")),
        upstream_status=st.one_of(
            st.integers(min_value=400, max_value=499),
            st.integers(min_value=500, max_value=599),
        ),
        reason_suffix=st.text(
            alphabet=st.characters(
                min_codepoint=0x20,
                max_codepoint=0x7E,
                blacklist_characters="\r\n",
            ),
            min_size=1,
            max_size=80,
        ),
    )
    def test_http_statuses_and_both_calls_keep_clean_status_aware_contract(
        self,
        failing_call: str,
        upstream_status: int,
        reason_suffix: str,
    ) -> None:
        raw_reason = f"raw-mb-http-secret::{reason_suffix}"
        error = HTTPError(
            url="https://musicbrainz.invalid/artist",
            code=upstream_status,
            msg=raw_reason,
            hdrs=email.message.Message(),
            fp=None,
        )
        handler = _RecordingHandler()
        with patch("web.server.mb_api") as mock_mb:
            if failing_call == "release_groups":
                mock_mb.get_artist_release_groups.side_effect = error
            else:
                mock_mb.get_artist_release_groups.return_value = []
                mock_mb.get_official_release_group_ids.side_effect = error
            get_artist(handler, {}, self.ARTIST_ID)  # type: ignore[arg-type]

        assert handler.status is not None
        assert handler.data is not None
        assert_clean_http_failure(
            handler.status,
            handler.data,
            upstream_status,
            raw_reason,
        )
