"""Tests for lib.audio_hash.

These tests are the cross-host stability guard. The pinned constants below
were generated against the Nix-pinned ffmpeg/mutagen toolchain in this repo's
flake. If a future ``nix flake update`` shifts ffmpeg or mutagen and the
hashes drift, this test will fail BEFORE deploy — surface the drift, decide
whether to repin (and what existing bad_audio_hashes rows to invalidate), and
do not silently update the constant.
"""

from __future__ import annotations

import shutil
import unittest
from pathlib import Path

from lib.audio_hash import (
    AudioHashError,
    flac_streaminfo_md5,
    hash_audio_content,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "audio_hash"

# --- Pinned cross-host regression constants -----------------------------------
# Generated from `tests/fixtures/audio_hash/sine_440.*` against the Nix-pinned
# ffmpeg in nix/shell.nix. DO NOT update without understanding why the hash
# moved (Nix toolchain bump, fixture regen, ffmpeg flags change).
PINNED_FLAC_HASH = bytes.fromhex(
    "204bd5540eee11c2e26c9eb77bd81b3ce8e4fd4a0bdedde7ad1b94054a9a8891"
)
PINNED_MP3_HASH = bytes.fromhex(
    "8e17d4c879283197543ccc16a7ea97449a1dd5a5bc794acad083e1b99a19b7fe"
)
PINNED_M4A_HASH = bytes.fromhex(
    "0327a1996912dd14ca10b1587971c056a5b7c2bc9edf650ad427d2518c90e92d"
)
PINNED_OGG_HASH = bytes.fromhex(
    "798c18cb4e12e6fe6d72fe85d78bbde2c46abec985e96112936e0f17f3a60b41"
)


class TestHashAudioContentHappyPath(unittest.TestCase):
    """Happy-path: deterministic, format-aware hashing."""

    def test_returns_32_bytes(self) -> None:
        """Public contract: SHA-256 across all formats is exactly 32 bytes."""
        for name in ("sine_440.flac", "sine_440.mp3", "sine_440.m4a", "sine_440.ogg"):
            with self.subTest(fixture=name):
                h = hash_audio_content(FIXTURE_DIR / name)
                self.assertEqual(len(h), 32)
                self.assertIsInstance(h, bytes)

    def test_pinned_mp3_constant(self) -> None:
        """Cross-host regression: MP3 fixture hashes to a known constant.

        If this fails, the Nix flake's ffmpeg moved underneath us — investigate
        before changing the constant.
        """
        h = hash_audio_content(FIXTURE_DIR / "sine_440.mp3")
        self.assertEqual(h, PINNED_MP3_HASH)

    def test_pinned_flac_constant(self) -> None:
        """Cross-host regression: FLAC PCM-decoded hash is constant."""
        h = hash_audio_content(FIXTURE_DIR / "sine_440.flac")
        self.assertEqual(h, PINNED_FLAC_HASH)

    def test_pinned_m4a_constant(self) -> None:
        """Cross-host regression: M4A ADTS-stripped hash is constant."""
        h = hash_audio_content(FIXTURE_DIR / "sine_440.m4a")
        self.assertEqual(h, PINNED_M4A_HASH)

    def test_pinned_ogg_constant(self) -> None:
        """Cross-host regression: OGG -bitexact hash is constant."""
        h = hash_audio_content(FIXTURE_DIR / "sine_440.ogg")
        self.assertEqual(h, PINNED_OGG_HASH)

    def test_double_hash_same_file_returns_same_bytes(self) -> None:
        """Sanity: hashing the same file twice returns identical digests."""
        p = FIXTURE_DIR / "sine_440.mp3"
        self.assertEqual(hash_audio_content(p), hash_audio_content(p))

    def test_explicit_format_overrides_extension(self) -> None:
        """Explicit ``audio_format`` argument is authoritative over extension."""
        # If we pretend the FLAC is an MP3 (wrong format), ffmpeg with -c copy
        # cannot re-mux it to mp3 — it will fail. So we expect AudioHashError.
        with self.assertRaises(AudioHashError):
            hash_audio_content(FIXTURE_DIR / "sine_440.flac", audio_format="mp3")

    def test_format_inference_case_insensitive(self) -> None:
        """Extension-based format inference is case-insensitive."""
        with TempDir() as tmp:
            upper = tmp / "SINE.MP3"
            shutil.copy(FIXTURE_DIR / "sine_440.mp3", upper)
            h = hash_audio_content(upper)
            self.assertEqual(h, PINNED_MP3_HASH)


class TestHashAudioContentTagInvariance(unittest.TestCase):
    """Re-tagging audio content does not change the audio-frame hash."""

    def test_retagging_mp3_preserves_hash(self) -> None:
        """Mutating the MP3 ID3 title changes file bytes but not the hash."""
        with TempDir() as tmp:
            target = tmp / "tagged.mp3"
            shutil.copy(FIXTURE_DIR / "sine_440.mp3", target)
            before = hash_audio_content(target)

            from mutagen.id3 import ID3, TIT2  # type: ignore[import-untyped]
            from mutagen.mp3 import MP3  # type: ignore[import-untyped]

            audio = MP3(str(target), ID3=ID3)
            try:
                audio.add_tags()  # type: ignore[no-untyped-call]
            except Exception:  # pragma: no cover — already has tags
                pass
            assert audio.tags is not None
            audio.tags.add(TIT2(encoding=3, text="Totally Different Title"))
            audio.save()

            after = hash_audio_content(target)
            self.assertEqual(before, after, "ID3 retag must not change audio hash")
            self.assertEqual(before, PINNED_MP3_HASH)


class TestFlacEdgeCases(unittest.TestCase):
    """FLAC-specific behaviour: STREAMINFO presence and fallback."""

    def test_streaminfo_md5_returns_16_bytes(self) -> None:
        """The fixture FLAC has a non-zero STREAMINFO MD5."""
        md5 = flac_streaminfo_md5(FIXTURE_DIR / "sine_440.flac")
        self.assertIsNotNone(md5)
        assert md5 is not None
        self.assertEqual(len(md5), 16)
        self.assertNotEqual(md5, b"\x00" * 16)

    def test_zero_streaminfo_falls_through_to_pcm_decode(self) -> None:
        """A FLAC with zeroed STREAMINFO still produces a non-zero hash.

        Construct this by binary-patching the STREAMINFO MD5 region of the
        fixture. We don't need the file to remain valid for mutagen; the
        public API doesn't consult STREAMINFO at all (option (a) — always
        decode PCM), so the test verifies the documented behaviour: zero
        STREAMINFO does not poison the public hash.
        """
        with TempDir() as tmp:
            target = tmp / "zero_streaminfo.flac"
            shutil.copy(FIXTURE_DIR / "sine_440.flac", target)

            data = bytearray(target.read_bytes())
            # FLAC layout: 4-byte magic "fLaC", then a STREAMINFO metadata
            # block. The block header is 4 bytes; STREAMINFO body is 34 bytes,
            # last 16 of which are the md5_signature. So MD5 starts at:
            #   4 (magic) + 4 (header) + 34 - 16 = 26
            md5_offset = 4 + 4 + 34 - 16
            for i in range(16):
                data[md5_offset + i] = 0
            target.write_bytes(bytes(data))

            md5 = flac_streaminfo_md5(target)
            self.assertEqual(md5, b"\x00" * 16, "fixture should now have zero MD5")

            h = hash_audio_content(target)
            self.assertEqual(len(h), 32)
            self.assertNotEqual(h, b"\x00" * 32, "PCM-decoded hash must be non-zero")
            # And it must equal the original fixture's hash, since we only
            # changed STREAMINFO metadata, not audio frames.
            self.assertEqual(h, PINNED_FLAC_HASH)


class TestErrorPaths(unittest.TestCase):
    """All failure modes raise typed ``AudioHashError``."""

    def test_nonexistent_path_raises(self) -> None:
        with self.assertRaises(AudioHashError) as ctx:
            hash_audio_content(Path("/nonexistent/missing.mp3"))
        self.assertIn("does not exist", str(ctx.exception))

    def test_directory_path_raises(self) -> None:
        with self.assertRaises(AudioHashError):
            hash_audio_content(FIXTURE_DIR)

    def test_unsupported_extension_raises(self) -> None:
        with TempDir() as tmp:
            target = tmp / "audio.wma"
            target.write_bytes(b"\x00" * 100)
            with self.assertRaises(AudioHashError) as ctx:
                hash_audio_content(target)
            self.assertIn("unsupported format", str(ctx.exception).lower())

    def test_unsupported_explicit_format_raises(self) -> None:
        with self.assertRaises(AudioHashError) as ctx:
            hash_audio_content(FIXTURE_DIR / "sine_440.mp3", audio_format="wma")
        self.assertIn("unsupported format", str(ctx.exception).lower())

    def test_truncated_mp3_raises(self) -> None:
        """A file that ffmpeg cannot parse raises ``AudioHashError``."""
        with TempDir() as tmp:
            target = tmp / "broken.mp3"
            # 200 bytes of zeros — not a valid MP3, ffmpeg will fail.
            target.write_bytes(b"\x00" * 200)
            with self.assertRaises(AudioHashError):
                hash_audio_content(target)

    def test_random_bytes_with_flac_extension_raises(self) -> None:
        with TempDir() as tmp:
            target = tmp / "fake.flac"
            target.write_bytes(b"not a real flac file" + b"\x00" * 200)
            with self.assertRaises(AudioHashError):
                hash_audio_content(target)


class TestCrossFormatIsolation(unittest.TestCase):
    """Format-mismatch evasion is accepted: same source → different format → different hash.

    Documented in plan 2026-04-29-005 § Out of Scope. The lookup composite key
    is ``(hash, audio_format)``, so a poisoned MP3 hash cannot accidentally
    match against a FLAC entry. This test pins that property.
    """

    def test_flac_and_mp3_of_same_source_differ(self) -> None:
        flac_hash = hash_audio_content(FIXTURE_DIR / "sine_440.flac")
        mp3_hash = hash_audio_content(FIXTURE_DIR / "sine_440.mp3")
        m4a_hash = hash_audio_content(FIXTURE_DIR / "sine_440.m4a")
        ogg_hash = hash_audio_content(FIXTURE_DIR / "sine_440.ogg")

        all_hashes = {flac_hash, mp3_hash, m4a_hash, ogg_hash}
        self.assertEqual(
            len(all_hashes), 4, "all four format hashes must be distinct"
        )


# --- Helpers ------------------------------------------------------------------


class TempDir:
    """Context manager that yields a temp Path and cleans it up.

    Avoids importing ``tempfile.TemporaryDirectory`` everywhere and lets us
    annotate the yielded type as Path for pyright.
    """

    def __init__(self) -> None:
        import tempfile
        self._td = tempfile.TemporaryDirectory()

    def __enter__(self) -> Path:
        return Path(self._td.name)

    def __exit__(self, *exc: object) -> None:
        self._td.cleanup()


if __name__ == "__main__":
    unittest.main()
