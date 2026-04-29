"""Unit tests for ``lib.preimport`` helpers — focus: bad-audio-hash gate (U5).

Slice-level coverage for ``run_preimport_gates`` lives in
``tests/test_integration_slices.py::TestBadAudioHashSlice``. These tests
exercise the ``_check_bad_audio_hashes`` helper and the empty-table /
hashing-error / DB-error fall-through behavior of the gate.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.preimport import _check_bad_audio_hashes, _iter_audio_files
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
            "lib.preimport.hash_audio_content",
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
        with patch("lib.preimport.hash_audio_content") as h:
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
    """``run_preimport_gates`` empty-table fast-path: when
    ``has_any_bad_audio_hashes`` returns False, the gate must NOT hash
    candidate tracks or call ``lookup_bad_audio_hash``."""

    def test_empty_table_skips_hashing_and_lookup(self):
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        db = MagicMock()
        db.has_any_bad_audio_hashes.return_value = False

        cfg = CratediggerConfig(audio_check_mode="off")

        # Bypass spectral and existing-album lookups so we isolate the
        # bad-hash gate's fast-path skip.
        with patch("lib.preimport.hash_audio_content") as hashfn, \
             patch("lib.preimport._iter_audio_files") as walker, \
             patch("lib.preimport._needs_spectral_check", return_value=False):
            run_preimport_gates(
                path=str(FIXTURE_DIR),
                mb_release_id="mbid-empty",
                label="Empty Table",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,
                request_id=42,
                usernames={"user1"},
            )

        db.has_any_bad_audio_hashes.assert_called_once()
        db.lookup_bad_audio_hash.assert_not_called()
        hashfn.assert_not_called()
        walker.assert_not_called()


if __name__ == "__main__":
    unittest.main()
