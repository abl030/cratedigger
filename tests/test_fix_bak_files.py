#!/usr/bin/env python3
"""Tests for the manual bad-extension repair helper."""

import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts import fix_bak_files


class TestDetectFormat(unittest.TestCase):
    def _ffprobe(self, payload: dict[str, object], returncode: int = 0):
        return SimpleNamespace(
            returncode=returncode,
            stdout=json.dumps(payload),
            stderr="",
        )

    def test_opus_codec_maps_to_opus_not_ogg_container(self):
        payload = {
            "streams": [{"codec_type": "audio", "codec_name": "opus"}],
            "format": {"format_name": "ogg"},
        }
        with patch("scripts.fix_bak_files.subprocess.run",
                   return_value=self._ffprobe(payload)):
            self.assertEqual(fix_bak_files.detect_format("/tmp/01.bak"), ".opus")

    def test_alac_codec_maps_to_m4a_not_mp3_default(self):
        payload = {
            "streams": [{"codec_type": "audio", "codec_name": "alac"}],
            "format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2"},
        }
        with patch("scripts.fix_bak_files.subprocess.run",
                   return_value=self._ffprobe(payload)):
            self.assertEqual(fix_bak_files.detect_format("/tmp/01.bak"), ".m4a")

    def test_unknown_probe_returns_none_instead_of_guessing_mp3(self):
        payload = {
            "streams": [{"codec_type": "audio", "codec_name": "mystery"}],
            "format": {"format_name": "mystery-container"},
        }
        with patch("scripts.fix_bak_files.subprocess.run",
                   return_value=self._ffprobe(payload)):
            self.assertIsNone(fix_bak_files.detect_format("/tmp/01.bak"))
