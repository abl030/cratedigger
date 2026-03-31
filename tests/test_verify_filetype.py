"""Tests for verify_filetype() after move to lib/quality.py.

These import directly from lib.quality — no AST extraction hack needed.
"""

import unittest


class TestVerifyFiletype(unittest.TestCase):
    """Verify that verify_filetype is importable from lib.quality and works."""

    def test_importable_from_quality(self):
        from lib.quality import verify_filetype
        self.assertTrue(callable(verify_filetype))

    def test_mp3_v0_vbr_245_matches(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.mp3", "bitRate": 245}
        self.assertTrue(verify_filetype(file, "mp3 v0"))

    def test_mp3_v0_rejects_cbr_256(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.mp3", "bitRate": 256}
        self.assertFalse(verify_filetype(file, "mp3 v0"))

    def test_flac_bitdepth_samplerate(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.flac", "bitDepth": 24, "sampleRate": 96000}
        self.assertTrue(verify_filetype(file, "flac 24/96"))

    def test_aac_min_bitrate_pass(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.aac", "bitRate": 256}
        self.assertTrue(verify_filetype(file, "aac 256+"))

    def test_aac_min_bitrate_fail(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.aac", "bitRate": 200}
        self.assertFalse(verify_filetype(file, "aac 256+"))

    def test_extension_mismatch(self):
        from lib.quality import verify_filetype
        file = {"filename": "track.flac", "bitRate": 320}
        self.assertFalse(verify_filetype(file, "mp3 320"))


if __name__ == "__main__":
    unittest.main()
