#!/usr/bin/env python3
"""Native-download codec labelling — the Opus-recorded-as-MP3 bug.

Live bug (request 4679, Darcie Haven — Angel of the Apocalypse, 2026-05-31):
a genuine Opus ~124 kbps download was rejected as a downgrade against an
existing MP3 CBR 128 (likely_transcode). Root cause: the harness stamped
EVERY native (non-converted) lossy download's measurement ``format`` with a
hardcoded ``"MP3"`` (``harness/import_one.py`` passed
``native_codec_family="MP3"`` to ``comparison_format_hint``). So the Opus was
scored on the MP3-VBR band table (acceptable floor 130 kbps) → 129 landed
POOR, losing to MP3-CBR-128 (ACCEPTABLE). A correct ``opus`` label classifies
TRANSPARENT (opus transparent threshold 112) and wins outright.

These tests pin: (1) the pure codec→family mapping, and (2) the real-audio
harness derivation — "here's an Opus file, what's it labelled?". Both must
say opus, not MP3.
"""

import os
import subprocess
import sys
import tempfile
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HARNESS_DIR = os.path.join(ROOT_DIR, "harness")
sys.path.insert(0, ROOT_DIR)


def _make_audio(path: str, codec_args: list[str], duration: int = 1) -> None:
    """Synthesize a 1s tone encoded with the given ffmpeg codec args."""
    cmd = [
        "ffmpeg", "-y", "-f", "lavfi",
        "-i", f"sine=frequency=440:duration={duration}",
        "-ac", "2", *codec_args, path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0 or not os.path.exists(path):
        raise RuntimeError(f"ffmpeg failed for {path}: {result.stderr}")


class TestNativeCodecFormatLabel(unittest.TestCase):
    """Pure mapping: ffprobe codec name / extension → rank-model family label.

    The label must be one the rank model's ``_codec_family_of`` keys on, so
    Opus and AAC downloads classify against their own band tables instead of
    the MP3 bands.
    """

    def _label(self, codec, ext=None):
        from lib.quality import native_codec_format_label
        return native_codec_format_label(codec, ext)

    def test_opus_codec_name_maps_to_opus(self):
        self.assertEqual(self._label("opus"), "opus")

    def test_aac_codec_name_maps_to_aac(self):
        self.assertEqual(self._label("aac"), "aac")

    def test_mp3_codec_name_maps_to_mp3(self):
        # Case-insensitive downstream; the literal stays the existing "MP3".
        self.assertEqual(self._label("mp3"), "MP3")

    def test_extension_fallback_when_codec_name_missing(self):
        self.assertEqual(self._label(None, ".opus"), "opus")
        self.assertEqual(self._label(None, "opus"), "opus")
        self.assertEqual(self._label(None, ".mp3"), "MP3")

    def test_codec_name_wins_over_extension(self):
        # .ogg container carrying opus → opus, not vorbis-guess from ext.
        self.assertEqual(self._label("opus", ".ogg"), "opus")

    def test_unmapped_codec_returns_none(self):
        # vorbis/flac/etc. have no lossy rank band here — None lets the
        # caller apply its conservative "MP3" fallback rather than this
        # function inventing a band.
        self.assertIsNone(self._label("vorbis"))
        self.assertIsNone(self._label(None, None))
        self.assertIsNone(self._label("", ""))


class TestDetectNativeCodecFamilyRealAudio(unittest.TestCase):
    """Real-audio: 'here's an Opus file — what's it labelled?'

    Drives the harness derivation against genuine ffmpeg-encoded files. The
    Opus case is the live bug: it MUST come back ``opus``, not ``MP3``.
    """

    def _detect(self, folder):
        from harness.import_one import _detect_native_codec_family
        return _detect_native_codec_family(folder)

    def test_opus_folder_labelled_opus_not_mp3(self):
        with tempfile.TemporaryDirectory() as d:
            _make_audio(os.path.join(d, "01.opus"),
                        ["-c:a", "libopus", "-b:a", "128k"])
            label = self._detect(d)
        self.assertEqual(
            label, "opus",
            f"Native Opus download mislabelled as {label!r} — this is the "
            "Darcie Haven bug (Opus scored on MP3 bands).",
        )

    def test_mp3_folder_labelled_mp3(self):
        with tempfile.TemporaryDirectory() as d:
            _make_audio(os.path.join(d, "01.mp3"),
                        ["-c:a", "libmp3lame", "-q:a", "0"])
            label = self._detect(d)
        self.assertEqual(label, "MP3")

    def test_aac_folder_labelled_aac(self):
        with tempfile.TemporaryDirectory() as d:
            _make_audio(os.path.join(d, "01.m4a"),
                        ["-c:a", "aac", "-b:a", "192k"])
            label = self._detect(d)
        self.assertEqual(label, "aac")

    def test_empty_folder_falls_back_to_mp3(self):
        # No probeable audio → conservative legacy fallback, never a crash.
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(self._detect(d), "MP3")

    def test_vorbis_folder_falls_back_to_mp3(self):
        # Known residual gap: Vorbis has no lossy rank band, so a native .ogg
        # download maps to None and the walk falls back to MP3-band scoring.
        # This pins the documented behaviour (and warns in the harness log) so
        # a future Vorbis band addition has a failing test to flip.
        with tempfile.TemporaryDirectory() as d:
            _make_audio(os.path.join(d, "01.ogg"),
                        ["-c:a", "libvorbis", "-q:a", "5"])
            self.assertEqual(self._detect(d), "MP3")

    def test_first_mappable_file_wins_in_mixed_lossy_folder(self):
        # Mixed lossy+lossy (opus + mp3) is not caught by the mixed-source
        # gate (that one is lossless+lossy only). Pin the deterministic
        # first-sorted-mappable-file-wins behaviour so a change is visible.
        with tempfile.TemporaryDirectory() as d:
            _make_audio(os.path.join(d, "01.mp3"),
                        ["-c:a", "libmp3lame", "-q:a", "0"])
            _make_audio(os.path.join(d, "02.opus"),
                        ["-c:a", "libopus", "-b:a", "128k"])
            # "01.mp3" sorts first → MP3.
            self.assertEqual(self._detect(d), "MP3")


if __name__ == "__main__":
    unittest.main()
