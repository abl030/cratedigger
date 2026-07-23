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

        with patch("lib.util.urllib.request.urlopen", return_value=response) as urlopen:
            with self.assertRaises(DefusedXmlException):
                util._plex_fetch_xml(cfg, "/library/sections/3/search", query="album")

        urlopen.assert_called_once()
