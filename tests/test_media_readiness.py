"""Regression pins for the one media-readiness boundary (issue #1062)."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from lib.media_readiness import (
    MediaReadinessError,
    flac_total_samples_only_changed,
    inspect_media,
    media_facts_for_path,
    normalize_media_metadata,
    prepare_media_readiness,
)
from tests.audio_fixtures import make_test_flac


def _streaminfo_span(raw: bytes) -> tuple[int, int]:
    assert raw[:4] == b"fLaC"
    assert raw[4] & 0x7F == 0
    assert int.from_bytes(raw[5:8], "big") == 34
    return 8, 42


def _zero_flac_duration_metadata(path: Path) -> bytes:
    raw = path.read_bytes()
    start, _ = _streaminfo_span(raw)
    mutated = bytearray(raw)
    # STREAMINFO: unknown min/max frame sizes, zero total samples and unknown
    # MD5 are legal incomplete metadata, not corrupt encoded audio.
    mutated[start + 4:start + 10] = b"\0" * 6
    combined = int.from_bytes(mutated[start + 10:start + 18], "big")
    mutated[start + 10:start + 18] = (combined & ~((1 << 36) - 1)).to_bytes(8, "big")
    mutated[start + 18:start + 34] = b"\0" * 16
    path.write_bytes(mutated)
    return bytes(mutated)


class TestFlacReadinessPin(unittest.TestCase):
    def test_decode_valid_zero_total_samples_is_repaired_header_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "01 - Five Suns.flac"
            make_test_flac(str(path), duration=1)
            broken = _zero_flac_duration_metadata(path)

            readiness = prepare_media_readiness(tmp)

            fact = readiness.files[0]
            self.assertEqual(readiness.normalized_paths, (str(path),))
            self.assertGreater(fact.sample_count, 0)
            self.assertAlmostEqual(fact.duration_seconds, 1.0, places=2)
            self.assertIsNotNone(fact.average_bitrate_kbps)
            repaired = path.read_bytes()
            self.assertTrue(flac_total_samples_only_changed(broken, repaired))
            start, _ = _streaminfo_span(repaired)
            self.assertEqual(repaired[start + 18:start + 34], b"\0" * 16)

    def test_header_only_checker_rejects_bit_depth_high_nibble_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "01.flac"
            make_test_flac(str(path), duration=1)
            before = path.read_bytes()
            changed = bytearray(before)
            start, _ = _streaminfo_span(before)
            # The high nibble shares byte 21 with the first total-samples
            # nibble, but belongs to the bit-depth declaration.
            changed[start + 13] ^= 0x10
            self.assertFalse(flac_total_samples_only_changed(before, bytes(changed)))

    def test_decode_valid_contradictory_sample_count_is_repaired(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "01.flac"
            make_test_flac(str(path), duration=1)
            before = bytearray(path.read_bytes())
            packed = int.from_bytes(before[18:26], "big")
            before[18:26] = ((packed & ~((1 << 36) - 1)) | 1).to_bytes(8, "big")
            path.write_bytes(before)

            readiness = normalize_media_metadata(tmp)

            self.assertGreater(readiness.files[0].sample_count, 1)
            self.assertTrue(flac_total_samples_only_changed(bytes(before), path.read_bytes()))

    def test_admitted_containers_expose_stream_derived_facts(self) -> None:
        """Ordinary MP3/AAC/Ogg/Opus/WAV/WMA remain readable without repair."""
        with tempfile.TemporaryDirectory() as tmp:
            specs = {
                "01.mp3": ("libmp3lame",),
                "02.m4a": ("aac",),
                "03.ogg": ("libvorbis",),
                "04.opus": ("libopus",),
                "05.wav": ("pcm_s16le",),
                "06.wma": ("wmav2",),
            }
            for name, (codec,) in specs.items():
                subprocess.run(
                    [
                        "ffmpeg", "-v", "error", "-nostdin", "-f", "lavfi",
                        "-i", "sine=frequency=440:duration=0.2", "-c:a", codec,
                        str(Path(tmp) / name),
                    ],
                    check=True,
                    timeout=30,
                )
            facts = inspect_media(tmp).files
            self.assertEqual(
                {Path(f.path).suffix for f in facts},
                {Path(name).suffix for name in specs},
            )
            for fact in facts:
                with self.subTest(container=fact.container):
                    self.assertGreater(fact.duration_seconds, 0)
                    self.assertGreater(fact.sample_count, 0)
                    self.assertIsNotNone(fact.average_bitrate_kbps)

    def test_container_is_detected_not_inferred_from_extension(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            disguised = Path(tmp) / "01 - mislabeled.ogg"
            make_test_flac(str(Path(tmp) / "source.flac"), duration=1)
            (Path(tmp) / "source.flac").replace(disguised)

            facts = media_facts_for_path(str(disguised))

            self.assertEqual(facts.codec, "flac")
            self.assertEqual(facts.container, "flac")

    def test_multiple_audio_streams_are_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "two-audio.m4a"
            subprocess.run(
                [
                    "ffmpeg", "-v", "error", "-nostdin",
                    "-f", "lavfi", "-i", "sine=frequency=440:duration=0.2",
                    "-f", "lavfi", "-i", "sine=frequency=880:duration=0.2",
                    "-map", "0:a", "-map", "1:a", "-c:a", "aac", str(path),
                ], check=True, timeout=30,
            )
            with self.assertRaises(MediaReadinessError) as raised:
                media_facts_for_path(str(path))
            self.assertEqual(raised.exception.kind, "ambiguous")

    def test_attached_artwork_does_not_make_audio_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cover = root / "cover.jpg"
            source = root / "source.mp3"
            path = root / "with-artwork.mp3"
            subprocess.run(
                ["ffmpeg", "-v", "error", "-nostdin", "-f", "lavfi", "-i", "color=c=blue:s=16x16", "-frames:v", "1", str(cover)],
                check=True, timeout=30,
            )
            subprocess.run(
                ["ffmpeg", "-v", "error", "-nostdin", "-f", "lavfi", "-i", "sine=frequency=440:duration=0.2", "-c:a", "libmp3lame", str(source)],
                check=True, timeout=30,
            )
            subprocess.run(
                [
                    "ffmpeg", "-v", "error", "-nostdin", "-i", str(source), "-i", str(cover),
                    "-map", "0:a", "-map", "1:v", "-c:a", "copy", "-c:v", "mjpeg",
                    "-disposition:v", "attached_pic", str(path),
                ], check=True, timeout=30,
            )
            facts = media_facts_for_path(str(path))
            self.assertEqual(facts.codec, "mp3")
            self.assertGreater(facts.sample_count, 0)
