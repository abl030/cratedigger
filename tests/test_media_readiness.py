"""Regression pins for the one media-readiness boundary (issue #1062)."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import IO
from unittest.mock import patch

from lib.media_readiness import (
    MediaReadinessError,
    average_bitrate_kbps_from_frames,
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

    def test_ffprobe_tool_failure_stays_measurement_failed(self) -> None:
        """A decode-valid source must not be blamed for an unavailable probe."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "01.wav"
            subprocess.run(
                [
                    "ffmpeg", "-v", "error", "-nostdin", "-f", "lavfi",
                    "-i", "sine=frequency=440:duration=0.1", str(path),
                ],
                check=True,
                timeout=30,
            )

            def fail_ffprobe(
                argv: list[str], **_kwargs: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if argv[0] == "ffprobe":
                    raise OSError("ffprobe unavailable")
                if argv[0] == "ffmpeg":
                    return subprocess.CompletedProcess(argv, 0, b"", b"")
                raise AssertionError(f"unexpected media tool: {argv[0]}")

            with patch(
                "lib.media_readiness.subprocess.run", side_effect=fail_ffprobe,
            ), self.assertRaises(MediaReadinessError) as raised:
                normalize_media_metadata(tmp)

            self.assertEqual(raised.exception.kind, "measurement_failed")
            self.assertIn("ffprobe failed", str(raised.exception))

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


class TestAverageBitrateDerivationPin(unittest.TestCase):
    """A constant-bitrate stream must never read one kbps low (Koppel, dl 39947).

    ``album_quality_evidence`` id 36856: ten genuinely 256 kbps tracks, one of
    which derived 255 because the rate was computed through a float
    ``duration`` that is not exactly representable. That single kbps broke
    per-track bitrate uniformity, flipped ``is_cbr`` to False, and (before
    issue #1145 collapsed the two MP3 ladders) routed the album onto the more
    generous VBR table, so it out-ranked and re-imported over itself. The
    ladder amplifier is gone; the reduction still has to be exact, because a
    kbps is still a band edge away from changing a tier.
    """

    # Track 04 of the live album, exactly as ffprobe reported it. The real
    # quotient is an integer: 8517888 * 8 == 266.184 s * 256 kbps * 1000.
    KOPPEL_TRACK_04 = (8_517_888, 12_776_832, 48_000)

    def test_exactly_representable_rate_is_not_floored_one_low(self) -> None:
        compressed_bytes, sample_count, sample_rate = self.KOPPEL_TRACK_04
        # The defect: routing through float seconds loses the exact quotient.
        self.assertEqual(
            int((compressed_bytes * 8) / (sample_count / sample_rate) / 1000),
            255,
            "fixture no longer reproduces the float-truncation world",
        )
        self.assertEqual(
            average_bitrate_kbps_from_frames(
                compressed_bytes, sample_count, sample_rate,
            ),
            256,
        )

    def test_rate_is_rounded_to_nearest_not_truncated(self) -> None:
        # A real stream rarely lands on an exact integer. Nearest-integer is
        # the honest report; flooring always biases a constant stream downward
        # and can break uniformity on a single track.
        #
        # Exactly one second at 44100 Hz, so kbps == bytes * 8 / 1000 and the
        # cases below are the byte counts that bracket the 191.5 rounding
        # boundary. Byte counts, not bit counts: the input is bytes, and a
        # "bits" spelling that is not a multiple of eight would be a fixture
        # that cannot exist.
        sample_rate, sample_count = 44_100, 44_100
        cases = (
            (23_950, 192),  # 191.600 kbps -> up
            (24_050, 192),  # 192.400 kbps -> down
            (23_937, 191),  # 191.496 kbps -> down, just under the boundary
            (23_938, 192),  # 191.504 kbps -> up, just over it
        )
        for compressed_bytes, expected in cases:
            with self.subTest(compressed_bytes=compressed_bytes):
                self.assertEqual(
                    average_bitrate_kbps_from_frames(
                        compressed_bytes, sample_count, sample_rate,
                    ),
                    expected,
                )

    def test_degenerate_inputs_withhold_a_rate(self) -> None:
        for label, args in (
            ("no samples", (1_000, 0, 44_100)),
            ("no sample rate", (1_000, 44_100, 0)),
            ("no audio bytes", (0, 44_100, 44_100)),
        ):
            with self.subTest(label):
                self.assertIsNone(average_bitrate_kbps_from_frames(*args))

    def test_the_reader_itself_reports_the_exact_rate(self) -> None:
        """The fix has to reach the call site, not just the helper.

        ``media_facts_for_path`` is what every consumer actually calls, and
        it kept its own float derivation until this change. Only ffprobe
        itself is replaced, at the subprocess leaf; its compact output is
        then parsed, validated and reduced entirely by production code.
        """
        compressed_bytes, sample_count, sample_rate = self.KOPPEL_TRACK_04
        probe_output = (
            f"index=0|codec_type=audio|codec_name=mp3"
            f"|sample_rate={sample_rate}|channels=2\n"
            f"stream_index=0|nb_samples={sample_count}\n"
            f"stream_index=0|size={compressed_bytes}\n"
            f"format_name=mp3\n"
        )

        def fake_ffprobe(
            argv: list[str], *, stdout: IO[str], **_kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            stdout.write(probe_output)
            return subprocess.CompletedProcess(argv, 0)

        with patch(
            "lib.media_readiness.subprocess.run", side_effect=fake_ffprobe,
        ):
            facts = media_facts_for_path("/nonexistent/04 Koppel.mp3")

        # Guards against a fixture that stopped reproducing the live world.
        self.assertEqual(facts.sample_count, sample_count)
        self.assertEqual(facts.compressed_audio_bytes, compressed_bytes)
        self.assertEqual(facts.average_bitrate_kbps, 256)
