#!/usr/bin/env python3
"""Generated hostile-XML coverage for the Plex response parser.

The deterministic DTD/entity pin lives in ``tests/test_util.py``.  This
property patrols arbitrary entity names through the real urllib/XML leaf:
Plex's response must be rejected after its one ordinary request.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from defusedxml.common import DefusedXmlException
from hypothesis import given, settings
from hypothesis import strategies as st

from lib import util
from lib.config import CratediggerConfig


_ENTITY_NAMES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz",
    min_size=1,
    max_size=20,
)


def assert_hostile_xml_rejected_after_one_request(
    *, request_count: int, rejected: bool,
) -> None:
    if not rejected:
        raise AssertionError("hostile XML was accepted")
    if request_count != 1:
        raise AssertionError(f"hostile XML made {request_count} requests, expected one")


class TestPlexXmlRejectsGeneratedEntities(unittest.TestCase):
    @settings(max_examples=40, deadline=None)
    @given(entity_name=_ENTITY_NAMES)
    def test_entity_declaration_is_rejected_after_one_request(
        self, entity_name: str,
    ) -> None:
        response = MagicMock()
        response.__enter__ = lambda value: value
        response.__exit__ = MagicMock(return_value=False)
        response.read.return_value = (
            f'<!DOCTYPE MediaContainer [<!ENTITY {entity_name} "blocked">]>'
            f'<MediaContainer title="&{entity_name};"/>'
        ).encode()
        cfg = CratediggerConfig(
            plex_url="https://plex.example.test",
            plex_token="token",
        )

        rejected = False
        with patch("lib.util.urllib.request.urlopen", return_value=response) as urlopen:
            try:
                util._plex_fetch_xml(cfg, "/library/sections/3/search", query="album")
            except DefusedXmlException:
                rejected = True

        assert_hostile_xml_rejected_after_one_request(
            request_count=urlopen.call_count, rejected=rejected)

    @settings(max_examples=40, deadline=None)
    @given(root_tag=_ENTITY_NAMES)
    def test_plain_dtd_is_rejected_after_one_request(self, root_tag: str) -> None:
        """The real notifier leaf rejects every DTD shape, not only entities."""
        response = MagicMock()
        response.__enter__ = lambda value: value
        response.__exit__ = MagicMock(return_value=False)
        response.read.return_value = (
            f"<!DOCTYPE {root_tag}><{root_tag}/>".encode())
        cfg = CratediggerConfig(
            plex_url="https://plex.example.test",
            plex_token="token",
        )

        rejected = False
        with patch("lib.util.urllib.request.urlopen", return_value=response) as urlopen:
            try:
                util._plex_fetch_xml(cfg, "/library/sections/3/search", query="album")
            except DefusedXmlException:
                rejected = True

        assert_hostile_xml_rejected_after_one_request(
            request_count=urlopen.call_count, rejected=rejected)


class TestPlexXmlOracleKnownBad(unittest.TestCase):
    def test_oracle_trips_when_hostile_xml_is_accepted(self) -> None:
        with self.assertRaisesRegex(AssertionError, "was accepted"):
            assert_hostile_xml_rejected_after_one_request(
                request_count=1, rejected=False)
