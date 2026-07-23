#!/usr/bin/env python3
"""Generated audio-only validation contracts for issue #835."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from hypothesis import HealthCheck, example, given, settings, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.quality import AudioValidationReport, AudioValidationResult
from lib.util import validate_audio


def _first_flac_frame(data: bytes) -> int:
    """Return the first byte after the FLAC metadata blocks."""
    if not data.startswith(b"fLaC"):
        raise AssertionError("generated fixture is not FLAC")
    cursor = 4
    while True:
        if cursor + 4 > len(data):
            raise AssertionError("truncated FLAC metadata")
        is_last = bool(data[cursor] & 0x80)
        length = int.from_bytes(data[cursor + 1:cursor + 4], "big")
        cursor += 4 + length
        if is_last:
            return cursor


def assert_corrupt_audio_is_rejected(result: AudioValidationResult) -> None:
    """Reusable invariant: readable damaged audio is content failure."""
    if result.report.outcome != "audio_corrupt":
        raise AssertionError(
            f"damaged audio escaped as {result.report.outcome}",
        )
    if result.report.files_failed < 1:
        raise AssertionError("damaged audio has no failed-file audit")
    if not result.report.diagnostics:
        raise AssertionError("damaged audio has no bounded diagnostic")


class TestAudioValidationGenerated(unittest.TestCase):
    fixture_dir: tempfile.TemporaryDirectory[str]
    clean_path: Path
    clean_bytes: bytes
    frame_start: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture_dir = tempfile.TemporaryDirectory()
        cls.clean_path = Path(cls.fixture_dir.name) / "clean.flac"
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=997:sample_rate=44100:duration=2",
                "-c:a",
                "flac",
                "-compression_level",
                "5",
                "-y",
                os.fspath(cls.clean_path),
            ],
            check=True,
            capture_output=True,
        )
        cls.clean_bytes = cls.clean_path.read_bytes()
        cls.frame_start = _first_flac_frame(cls.clean_bytes)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.fixture_dir.cleanup()

    def _validate_mutation(self, relative_offset: int, mask: int):
        with tempfile.TemporaryDirectory() as album:
            mutated = bytearray(self.clean_bytes)
            offset = self.frame_start + relative_offset
            if offset >= len(mutated) - 2:
                raise AssertionError("generated mutation left audio frame data")
            mutated[offset] ^= mask
            Path(album, "track.flac").write_bytes(mutated)
            with self.assertLogs("cratedigger", level="WARNING"):
                return validate_audio(album)

    @example(relative_offset=96, mask=1)
    @given(
        relative_offset=st.integers(min_value=32, max_value=512),
        mask=st.sampled_from((1, 2, 4, 8, 16, 32, 64, 128)),
    )
    @settings(
        max_examples=(
            96
            if os.environ.get("CRATEDIGGER_HYPOTHESIS_PROFILE") == "fuzz"
            else 18
        ),
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_real_flac_frame_mutation_is_rejected(
        self,
        relative_offset: int,
        mask: int,
    ) -> None:
        assert_corrupt_audio_is_rejected(
            self._validate_mutation(relative_offset, mask),
        )

    def test_old_rc_zero_contract_is_known_bad(self) -> None:
        with tempfile.TemporaryDirectory() as album:
            mutated = bytearray(self.clean_bytes)
            mutated[self.frame_start + 96] ^= 1
            path = Path(album, "track.flac")
            path.write_bytes(mutated)
            old = subprocess.run(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-i",
                    os.fspath(path),
                    "-map",
                    "0:a",
                    "-f",
                    "null",
                    "-",
                ],
                capture_output=True,
            )
            self.assertEqual(
                old.returncode,
                0,
                old.stderr.decode("utf-8", "replace"),
            )

        known_bad = AudioValidationResult(
            AudioValidationReport(outcome="passed"),
        )
        with self.assertRaisesRegex(
            AssertionError,
            "damaged audio escaped as passed",
        ):
            assert_corrupt_audio_is_rejected(known_bad)

    def test_real_unset_streaminfo_md5_is_not_repaired_or_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as album:
            path = Path(album, "track.flac")
            unset_md5 = bytearray(self.clean_bytes)
            # STREAMINFO is the mandatory first metadata block. Its 16-byte
            # audio MD5 occupies payload bytes 18..33 (file offsets 26..41).
            unset_md5[26:42] = b"\x00" * 16
            path.write_bytes(unset_md5)
            before = path.read_bytes()
            result = validate_audio(album)
            after = path.read_bytes()

        self.assertTrue(result.valid)
        self.assertEqual(result.report.outcome, "passed")
        self.assertTrue(result.report.tool_version.startswith("ffmpeg version"))
        self.assertEqual(result.report.diagnostics, [])
        self.assertEqual(after, before)

    @given(stderr=st.binary(max_size=8192))
    def test_exit_zero_stderr_has_no_policy_or_audit_meaning(
        self,
        stderr: bytes,
    ) -> None:
        with tempfile.TemporaryDirectory() as album:
            Path(album, "track.flac").write_bytes(b"readable")
            with patch("lib.util.sp.run") as run:
                run.return_value = MagicMock(returncode=0, stderr=stderr)
                result = validate_audio(album)

        self.assertTrue(result.valid)
        self.assertEqual(result.report.outcome, "passed")
        self.assertEqual(result.report.files_failed, 0)
        self.assertEqual(result.report.diagnostics, [])
        self.assertEqual(result.report.omitted_diagnostics, 0)

    @example(return_code=69, stderr=b"decode_frame() failed")
    @given(
        return_code=st.integers(min_value=1, max_value=255),
        stderr=st.binary(max_size=4096),
    )
    def test_every_positive_exit_on_readable_bytes_is_bad_audio(
        self,
        return_code: int,
        stderr: bytes,
    ) -> None:
        with tempfile.TemporaryDirectory() as album:
            Path(album, "track.flac").write_bytes(b"readable")
            with (
                patch("lib.util.sp.run") as run,
                self.assertLogs("cratedigger", level="WARNING"),
            ):
                run.return_value = MagicMock(
                    returncode=return_code,
                    stderr=stderr,
                )
                result = validate_audio(album)

        assert_corrupt_audio_is_rejected(result)
        diagnostic = result.report.diagnostics[0]
        self.assertEqual(diagnostic.return_code, return_code)
        self.assertEqual(
            diagnostic.category,
            (
                "decode_error"
                if return_code == 69
                else "ffmpeg_failed_unclassified"
            ),
        )
        self.assertLessEqual(
            len(diagnostic.stderr_excerpt.encode()),
            2048,
        )
        self.assertEqual(diagnostic.stderr_bytes, len(stderr))
