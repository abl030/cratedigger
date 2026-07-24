#!/usr/bin/env python3
"""Boundary tests for the one-shot Plex duplicate maintenance scripts."""

from __future__ import annotations

import tempfile
import unittest
from unittest.mock import MagicMock, patch

from defusedxml.common import DefusedXmlException
from scripts import plex_dupes_audit, plex_dupes_merge


class TestPlexDupesScriptBoundaries(unittest.TestCase):
    def test_token_requests_use_default_verified_urlopen_context(self) -> None:
        audit_response = MagicMock()
        audit_response.__enter__ = lambda value: value
        audit_response.__exit__ = MagicMock(return_value=False)
        audit_response.read.return_value = b"<MediaContainer/>"
        merge_response = MagicMock()
        merge_response.__enter__ = lambda value: value
        merge_response.__exit__ = MagicMock(return_value=False)
        merge_response.status = 200
        merge_response.read.return_value = b""

        with patch.object(
            plex_dupes_audit.urllib.request, "urlopen", return_value=audit_response,
        ) as audit_urlopen:
            self.assertEqual(
                plex_dupes_audit.fetch_children("123", "audit-token"),
                ("123", b"<MediaContainer/>"),
            )
        self.assertEqual(audit_urlopen.call_args.kwargs, {"timeout": 15})

        with patch.object(
            plex_dupes_merge.urllib.request, "urlopen", return_value=merge_response,
        ) as merge_urlopen:
            self.assertEqual(
                plex_dupes_merge.merge("1", ["2"], "merge-token"), (200, b""))
        self.assertEqual(merge_urlopen.call_args.kwargs, {"timeout": 30})

    def test_audit_rejects_hostile_saved_and_live_xml(self) -> None:
        hostile = (
            b'<!DOCTYPE MediaContainer [<!ENTITY forbidden "blocked">]>'
            b'<MediaContainer title="&forbidden;"/>'
        )

        with self.assertRaises(DefusedXmlException):
            plex_dupes_audit._parse_children_xml(hostile)

        with tempfile.NamedTemporaryFile() as xml_file:
            xml_file.write(hostile)
            xml_file.flush()
            with self.assertRaises(DefusedXmlException):
                plex_dupes_audit._load_albums(xml_file.name)

    def test_audit_rejects_plain_doctype_in_saved_and_live_xml(self) -> None:
        """The audit has no DTD use case, including a declaration without entities."""
        plain_doctype = b"<!DOCTYPE MediaContainer><MediaContainer/>"

        with self.assertRaises(DefusedXmlException):
            plex_dupes_audit._parse_children_xml(plain_doctype)

        with tempfile.NamedTemporaryFile() as xml_file:
            xml_file.write(plain_doctype)
            xml_file.flush()
            with self.assertRaises(DefusedXmlException):
                plex_dupes_audit._load_albums(xml_file.name)
