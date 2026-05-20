"""Unit tests for ``lib.measurement`` helpers — focus: bad-audio-hash gate (U5).

Slice-level coverage for ``measure_preimport_state`` lives in
``tests/test_integration_slices.py::TestBadAudioHashSlice``. These tests
exercise the ``_check_bad_audio_hashes`` helper and the empty-table /
hashing-error / DB-error fall-through behavior of the gate.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from lib.measurement import _check_bad_audio_hashes, _iter_audio_files
from tests.fakes import FakePipelineDB


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "audio_hash"


class TestCheckBadAudioHashes(unittest.TestCase):
    """Direct tests of the per-track hash + lookup loop."""

    def test_hits_first_match_and_returns_id_and_path(self):
        """Single-track candidate whose hash matches a seeded bad hash."""
        from lib.audio_hash import hash_audio_content
        from lib.pipeline_db import BadAudioHashInput

        db = FakePipelineDB()
        mp3 = FIXTURE_DIR / "sine_440.mp3"
        digest = hash_audio_content(mp3, "mp3")
        db.add_bad_audio_hashes(
            request_id=99,
            reported_username="curator",
            reason="exemplar bad rip",
            hashes=[BadAudioHashInput(hash_value=digest, audio_format="mp3")],
        )

        match = _check_bad_audio_hashes([mp3], db)  # type: ignore[arg-type]

        self.assertIsNotNone(match)
        assert match is not None  # narrow for pyright
        self.assertEqual(match.bad_hash_id, 1)
        self.assertEqual(match.track_path, str(mp3))

    def test_returns_none_when_no_match(self):
        db = FakePipelineDB()
        match = _check_bad_audio_hashes(
            [FIXTURE_DIR / "sine_440.mp3"], db,  # type: ignore[arg-type]
        )
        self.assertIsNone(match)

    def test_partial_track_match_picks_the_matching_track(self):
        """12-track candidate, only one track matches — return that track."""
        from lib.audio_hash import hash_audio_content
        from lib.pipeline_db import BadAudioHashInput

        db = FakePipelineDB()
        mp3 = FIXTURE_DIR / "sine_440.mp3"
        digest = hash_audio_content(mp3, "mp3")
        db.add_bad_audio_hashes(
            request_id=99,
            reported_username=None,
            reason=None,
            hashes=[BadAudioHashInput(hash_value=digest, audio_format="mp3")],
        )

        # 11 imaginary tracks (paths that won't exist; their hash attempts
        # will fail and be skipped) plus the real fixture last.
        paths: list[Path] = [
            FIXTURE_DIR / f"missing_track_{i:02d}.mp3" for i in range(1, 12)
        ]
        paths.append(mp3)

        match = _check_bad_audio_hashes(paths, db)  # type: ignore[arg-type]
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.track_path, str(mp3))

    def test_hashing_failure_logs_and_continues(self):
        """When ``hash_audio_content`` raises, the gate continues; if no
        later track matches, returns None (gate falls through)."""
        from lib.audio_hash import AudioHashError

        db = FakePipelineDB()
        # Direct lookup mock; never reached because hashing always fails.
        with patch(
            "lib.measurement.hash_audio_content",
            side_effect=AudioHashError("boom"),
        ), patch.object(db, "lookup_bad_audio_hash") as lookup:
            match = _check_bad_audio_hashes(
                [FIXTURE_DIR / "sine_440.mp3"], db,  # type: ignore[arg-type]
            )
        self.assertIsNone(match)
        lookup.assert_not_called()

    def test_db_lookup_failure_logs_and_continues(self):
        """``lookup_bad_audio_hash`` raising must not crash the gate."""
        db = FakePipelineDB()
        with patch.object(
            db, "lookup_bad_audio_hash", side_effect=RuntimeError("db down"),
        ):
            match = _check_bad_audio_hashes(
                [FIXTURE_DIR / "sine_440.mp3"], db,  # type: ignore[arg-type]
            )
        self.assertIsNone(match)

    def test_skips_paths_without_extensions(self):
        """A path without an extension is skipped (not hashed)."""
        db = FakePipelineDB()
        with patch("lib.measurement.hash_audio_content") as h:
            match = _check_bad_audio_hashes(
                [Path("/tmp/no_extension")], db,  # type: ignore[arg-type]
            )
        self.assertIsNone(match)
        h.assert_not_called()


class TestIterAudioFiles(unittest.TestCase):
    """Directory-walk shape — must include nested layouts (multi-disc)."""

    def test_returns_empty_when_path_missing(self):
        self.assertEqual(_iter_audio_files("/tmp/this/does/not/exist/anywhere"),
                         [])

    def test_returns_supported_files_in_fixture_dir(self):
        files = _iter_audio_files(str(FIXTURE_DIR))
        names = sorted(p.name for p in files)
        self.assertIn("sine_440.flac", names)
        self.assertIn("sine_440.mp3", names)
        self.assertIn("sine_440.m4a", names)
        self.assertIn("sine_440.ogg", names)


class TestBadAudioHashGateFastPath(unittest.TestCase):
    """``measure_preimport_state`` empty-table fast-path: when
    ``has_any_bad_audio_hashes`` returns False, the gate must NOT hash
    candidate tracks or call ``lookup_bad_audio_hash``.

    Note (U3): the measurement helper still walks the filesystem to derive
    ``folder_layout`` / ``audio_file_count`` for the new evidence facts. That
    walk is cheap (no hashing) and is now part of every measurement. The
    fast-path that this test protects is the *hash* path — verified by
    asserting ``hash_audio_content`` and ``lookup_bad_audio_hash`` are not
    called.
    """

    def test_empty_table_skips_hashing_and_lookup(self):
        from lib.config import CratediggerConfig
        from lib.measurement import measure_preimport_state

        db = FakePipelineDB()
        # empty bad_audio_hashes table → fast-path skip
        cfg = CratediggerConfig(audio_check_mode="off")

        # Bypass spectral and existing-album lookups so we isolate the
        # bad-hash gate's fast-path skip.
        with patch("lib.measurement.hash_audio_content") as hashfn, \
             patch("lib.measurement._needs_spectral_check", return_value=False):
            measure_preimport_state(
                path=str(FIXTURE_DIR),
                mb_release_id="mbid-empty",
                label="Empty Table",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                db=cast(Any, db),
                request_id=42,
            )

        # ``has_any_bad_audio_hashes`` is the fast-path gate — it must be
        # called once. ``lookup_bad_audio_hash`` and ``hash_audio_content``
        # must NOT have been touched (the gate short-circuited because
        # the table is empty).
        self.assertEqual(db.has_any_bad_audio_hashes_calls, 1)
        self.assertEqual(db.lookup_bad_audio_hash_calls, [])
        hashfn.assert_not_called()


class TestMeasurePreimportState(unittest.TestCase):
    """U3: ``measure_preimport_state`` produces fact-only ``PreimportMeasurement``.

    The new pure measurement helper has no decision fields. These tests
    verify the measurement fields populate correctly for representative
    fixture shapes.
    """

    def test_audio_corrupt_short_circuits_with_facts(self):
        """audio_corrupt=True must flow through; spectral / file counts
        short-circuit, but the audio facts must be intact."""
        from lib.config import CratediggerConfig
        from lib.measurement import measure_preimport_state
        from lib.util import AudioValidationResult

        db = FakePipelineDB()
        cfg = CratediggerConfig(audio_check_mode="normal")
        bad_result = AudioValidationResult(
            valid=False, error="decode failed",
            failed_files=[("track01.mp3", "decode error")],
        )
        with patch("lib.measurement.validate_audio", return_value=bad_result), \
             patch("lib.measurement.repair_mp3_headers"):
            m = measure_preimport_state(
                path="/tmp/does-not-exist",
                mb_release_id="mbid-corrupt",
                label="Corrupt",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                db=cast(Any, db),
                request_id=42,
            )
        self.assertTrue(m.audio_corrupt)
        self.assertEqual(m.corrupt_files, ["track01.mp3"])
        # Short-circuit: spectral did not run.
        self.assertIsNone(m.download_spectral)

    def test_empty_directory_reports_zero_file_count(self):
        """No audio files → audio_file_count=0, layout='flat'."""
        from lib.config import CratediggerConfig
        from lib.measurement import measure_preimport_state

        cfg = CratediggerConfig(audio_check_mode="off")
        with patch("lib.measurement.repair_mp3_headers"), \
             patch("lib.measurement._iter_audio_files", return_value=[]), \
             patch("lib.measurement._needs_spectral_check", return_value=False):
            m = measure_preimport_state(
                path="/tmp/empty",
                mb_release_id="mbid-empty",
                label="Empty",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
            )
        self.assertEqual(m.audio_file_count, 0)
        self.assertEqual(m.folder_layout, "flat")
        self.assertFalse(m.audio_corrupt)
        self.assertIsNone(m.matched_bad_hash_id)

    def test_nested_layout_detected_via_inspection(self):
        """has_nested_audio=True in the inspection → folder_layout='nested'."""
        from pathlib import Path
        from lib.config import CratediggerConfig
        from lib.measurement import LocalFileInspection, measure_preimport_state

        cfg = CratediggerConfig(audio_check_mode="off")
        # Precomputed inspection signaling nested layout.
        inspection = LocalFileInspection(
            filetype="mp3", min_bitrate_bps=320_000,
            avg_bitrate_bps=320_000, is_vbr=False, has_nested_audio=True,
        )
        with patch("lib.measurement.repair_mp3_headers"), \
             patch(
                 "lib.measurement._iter_audio_files",
                 return_value=[Path("/tmp/album/CD1/01.mp3"),
                               Path("/tmp/album/CD2/01.mp3")],
             ), \
             patch("lib.measurement._needs_spectral_check", return_value=False):
            m = measure_preimport_state(
                path="/tmp/album",
                mb_release_id="mbid-nested",
                label="Nested",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                precomputed_inspection=inspection,
            )
        self.assertEqual(m.folder_layout, "nested")
        self.assertEqual(m.audio_file_count, 2)

    def test_filetype_band_and_bitrate_in_kbps(self):
        """filetype_band lowercased; min_bitrate_kbps in kbps not bps."""
        from lib.config import CratediggerConfig
        from lib.measurement import measure_preimport_state

        cfg = CratediggerConfig(audio_check_mode="off")
        with patch("lib.measurement.repair_mp3_headers"), \
             patch("lib.measurement._iter_audio_files", return_value=[]), \
             patch("lib.measurement._needs_spectral_check", return_value=False):
            m = measure_preimport_state(
                path="/tmp/album",
                mb_release_id="mbid-x",
                label="X",
                download_filetype="MP3, FLAC",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
            )
        self.assertEqual(m.filetype_band, "mp3, flac")
        self.assertEqual(m.min_bitrate_kbps, 320)


if __name__ == "__main__":
    unittest.main()
