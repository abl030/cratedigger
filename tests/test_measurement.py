"""Unit tests for ``lib.measurement`` helpers — focus: bad-audio-hash gate (U5).

Slice-level coverage for ``measure_preimport_state`` lives in
``tests/test_integration_slices.py::TestBadAudioHashSlice``. These tests
exercise the ``_check_bad_audio_hashes`` helper and the empty-table /
hashing-error / DB-error fall-through behavior of the gate.
"""

from __future__ import annotations

import unittest
import tempfile
from types import SimpleNamespace
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from lib.measurement import _check_bad_audio_hashes, _iter_audio_files
from tests.fakes import FakePipelineDB


class TestAttemptSpectralAudit(unittest.TestCase):
    """Attempt audit scans ordinary HAVE and preserves lossless provenance."""

    def test_shared_owner_keeps_lookup_and_analyzer_failures_independent(self):
        from lib.measurement import (
            ExistingSpectralAuditLookup,
            collect_release_attempt_spectral_audit,
        )
        from lib.quality import SpectralAnalysisDetail

        persisted = SpectralAnalysisDetail(attempted=False)

        def resolve(_mbid):
            return ExistingSpectralAuditLookup(path="existing")

        def candidate_fails(path: str) -> SpectralAnalysisDetail:
            if path == "candidate":
                raise RuntimeError("candidate failed")
            return SpectralAnalysisDetail(attempted=True, grade="suspect")

        candidate_failure = collect_release_attempt_spectral_audit(
            "candidate",
            "mbid",
            existing_spectral_evidence=persisted,
            preserve_existing_source_spectral=False,
            analyzer=candidate_fails,
            existing_resolver=resolve,
        )[0]
        assert candidate_failure.candidate is not None
        assert candidate_failure.existing is not None
        self.assertIn("candidate failed", candidate_failure.candidate.error or "")
        self.assertEqual(candidate_failure.existing.grade, "suspect")

        def existing_fails(path: str) -> SpectralAnalysisDetail:
            if path == "existing":
                raise RuntimeError("existing failed")
            return SpectralAnalysisDetail(attempted=True, grade="genuine")

        existing_failure = collect_release_attempt_spectral_audit(
            "candidate",
            "mbid",
            existing_spectral_evidence=persisted,
            preserve_existing_source_spectral=False,
            analyzer=existing_fails,
            existing_resolver=resolve,
        )[0]
        assert existing_failure.candidate is not None
        assert existing_failure.existing is not None
        self.assertEqual(existing_failure.candidate.grade, "genuine")
        self.assertIn("existing failed", existing_failure.existing.error or "")

        def lookup_fails(_mbid):
            raise RuntimeError("lookup failed")

        lookup_failure = collect_release_attempt_spectral_audit(
            "candidate",
            "mbid",
            existing_spectral_evidence=persisted,
            preserve_existing_source_spectral=False,
            analyzer=lambda _path: SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
            ),
            existing_resolver=lookup_fails,
        )[0]
        assert lookup_failure.candidate is not None
        assert lookup_failure.existing is not None
        self.assertEqual(lookup_failure.candidate.grade, "genuine")
        self.assertIn("lookup failed", lookup_failure.existing.error or "")

    def test_ordinary_existing_mp3_is_scanned_on_disk(self):
        """Non-lossless-converted HAVE is measured from the exact files."""
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.measurement import measure_preimport_state
        from lib.quality import SpectralAnalysisDetail
        from tests.fakes import FakeBeetsDB

        with tempfile.TemporaryDirectory() as candidate, \
             tempfile.TemporaryDirectory() as existing:
            Path(candidate, "01.mp3").write_bytes(b"candidate")
            Path(existing, "01.mp3").write_bytes(b"existing")
            beets = FakeBeetsDB()
            beets.set_album_info("mbid", AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                is_cbr=True,
                album_path=existing,
                format="MP3",
            ))
            calls: list[str] = []

            def analyze(path: str, trim_seconds: int = 30):
                del trim_seconds
                calls.append(path)
                return SimpleNamespace(
                    grade="suspect" if path == existing else "genuine",
                    estimated_bitrate_kbps=(128 if path == existing else None),
                    suspect_pct=(100.0 if path == existing else 0.0),
                    tracks=[],
                )

            with patch("lib.beets_db.BeetsDB", return_value=beets), \
                 patch("lib.measurement.spectral_analyze", side_effect=analyze):
                measured = measure_preimport_state(
                    path=candidate,
                    mb_release_id="mbid",
                    label="Gespenst - The Saint",
                    download_filetype="mp3",
                    download_min_bitrate_bps=219_000,
                    download_is_vbr=False,
                    cfg=CratediggerConfig(audio_check_mode="off"),
                    existing_spectral_evidence=SpectralAnalysisDetail(
                        attempted=False,
                    ),
                )

        self.assertEqual(calls, [candidate, existing])
        assert measured.spectral_audit.existing is not None
        self.assertTrue(measured.spectral_audit.existing.attempted)
        self.assertEqual(measured.spectral_audit.existing.grade, "suspect")
        self.assertEqual(measured.spectral_audit.existing.bitrate_kbps, 128)

    def test_lossless_converted_have_preserves_source_without_derivative_scan(self):
        """FLAC-derived HAVE keeps source evidence and skips installed Opus."""
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.measurement import measure_preimport_state
        from lib.quality import SpectralAnalysisDetail
        from tests.fakes import FakeBeetsDB

        with tempfile.TemporaryDirectory() as candidate, \
             tempfile.TemporaryDirectory() as existing:
            Path(candidate, "01.mp3").write_bytes(b"candidate")
            Path(existing, "01.opus").write_bytes(b"derivative")
            beets = FakeBeetsDB()
            beets.set_album_info("mbid", AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=122,
                avg_bitrate_kbps=127,
                median_bitrate_kbps=127,
                is_cbr=False,
                album_path=existing,
                format="Opus",
            ))
            persisted = SpectralAnalysisDetail(
                attempted=True,
                grade="likely_transcode",
                bitrate_kbps=224,
            )
            calls: list[str] = []

            def analyze(path: str, trim_seconds: int = 30):
                del trim_seconds
                calls.append(path)
                return SimpleNamespace(
                    grade="genuine",
                    estimated_bitrate_kbps=None,
                    suspect_pct=0.0,
                    tracks=[],
                )

            with patch("lib.beets_db.BeetsDB", return_value=beets), \
                 patch("lib.measurement.spectral_analyze", side_effect=analyze):
                measured = measure_preimport_state(
                    path=candidate,
                    mb_release_id="mbid",
                    label="Taboo VI",
                    download_filetype="mp3",
                    download_min_bitrate_bps=219_000,
                    download_is_vbr=False,
                    cfg=CratediggerConfig(audio_check_mode="off"),
                    existing_spectral_evidence=persisted,
                    preserve_existing_source_spectral=True,
                )

        self.assertEqual(calls, [candidate])
        self.assertIs(measured.spectral_audit.existing, persisted)

    def test_candidate_failure_does_not_hide_existing_measurement(self):
        from lib.measurement import collect_attempt_spectral_audit

        def analyze(path: str, trim_seconds: int = 30):
            if path == "/candidate":
                raise RuntimeError("candidate decode failed")
            return SimpleNamespace(
                grade="suspect",
                estimated_bitrate_kbps=128,
                suspect_pct=100.0,
                tracks=[],
            )

        with patch("lib.measurement.spectral_analyze", side_effect=analyze):
            audit = collect_attempt_spectral_audit("/candidate", "/existing")

        assert audit.candidate is not None
        assert audit.existing is not None
        self.assertEqual(audit.candidate.error, "RuntimeError: candidate decode failed")
        self.assertEqual(audit.existing.grade, "suspect")

    def test_normal_harness_collector_only_analyzes_candidate(self):
        from lib.measurement import collect_attempt_spectral_audit
        result = SimpleNamespace(
            grade="genuine", estimated_bitrate_kbps=None,
            suspect_pct=0.0, tracks=[],
        )
        calls: list[str] = []

        def analyze(path: str, trim_seconds: int = 30):
            calls.append(path)
            return result

        with patch("lib.measurement.spectral_analyze", side_effect=analyze):
            collect_attempt_spectral_audit(
                "/candidate",
                None,
            )
        self.assertEqual(calls, ["/candidate"])

    def test_malformed_track_preserves_album_facts_and_prior_track_detail(self):
        from lib.config import CratediggerConfig
        from lib.measurement import analyze_spectral_audit_path, measure_preimport_state

        result = SimpleNamespace(
            grade="suspect", estimated_bitrate_kbps=160,
            suspect_pct=75.0,
            tracks=[
                SimpleNamespace(
                    grade="genuine", hf_deficit_db=20.0,
                    cliff_detected=False, cliff_freq_hz=None,
                    estimated_bitrate_kbps=None, error=None,
                ),
                SimpleNamespace(
                    grade="suspect", hf_deficit_db="malformed",
                    cliff_detected=True, cliff_freq_hz=17000,
                    estimated_bitrate_kbps=128, error=None,
                ),
            ],
        )
        with patch("lib.measurement.spectral_analyze", return_value=result):
            detail = analyze_spectral_audit_path("/candidate")

        self.assertEqual(detail.grade, "suspect")
        self.assertEqual(detail.bitrate_kbps, 160)
        self.assertEqual(detail.suspect_pct, 75.0)
        self.assertEqual(len(detail.per_track), 1)
        self.assertIn("TypeError", detail.error or "")

        with tempfile.TemporaryDirectory() as candidate:
            Path(candidate, "01.mp3").write_bytes(b"candidate")
            with patch("lib.measurement.spectral_analyze", return_value=result):
                measured = measure_preimport_state(
                    path=candidate, mb_release_id="", label="test",
                    download_filetype="mp3", download_min_bitrate_bps=320_000,
                    download_is_vbr=False,
                    cfg=CratediggerConfig(audio_check_mode="off"),
                )
        self.assertIsNotNone(measured.download_spectral)
        assert measured.download_spectral is not None
        self.assertEqual(measured.download_spectral.grade, "suspect")
        self.assertEqual(measured.download_spectral.bitrate_kbps, 160)
        assert measured.spectral_audit.candidate is not None
        self.assertIn(
            "TypeError", measured.spectral_audit.candidate.error or "")

    def test_candidate_audit_failure_does_not_add_existing_decision_inputs(self):
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.measurement import measure_preimport_state
        from tests.fakes import FakeBeetsDB
        from lib.quality import SpectralAnalysisDetail

        with tempfile.TemporaryDirectory() as candidate, \
             tempfile.TemporaryDirectory() as existing:
            Path(candidate, "01.mp3").write_bytes(b"candidate")
            Path(existing, "01.mp3").write_bytes(b"existing")
            beets = FakeBeetsDB()
            beets.set_album_info("mbid", AlbumInfo(
                album_id=1, track_count=1, min_bitrate_kbps=320,
                is_cbr=True, album_path=existing, format="MP3",
            ))
            existing_result = SimpleNamespace(
                grade="suspect", estimated_bitrate_kbps=128,
                suspect_pct=100.0, tracks=[],
            )

            def analyze(path: str, trim_seconds: int = 30):
                if path == candidate:
                    raise RuntimeError("candidate failed")
                return existing_result

            with patch("lib.beets_db.BeetsDB", return_value=beets), \
                 patch("lib.measurement.spectral_analyze", side_effect=analyze):
                measured = measure_preimport_state(
                    path=candidate, mb_release_id="mbid", label="test",
                    download_filetype="mp3", download_min_bitrate_bps=320_000,
                    download_is_vbr=False,
                    cfg=CratediggerConfig(audio_check_mode="off"),
                    existing_spectral_evidence=SpectralAnalysisDetail(
                        attempted=True,
                        grade="suspect",
                        bitrate_kbps=128,
                    ),
                )

        self.assertIsNone(measured.download_spectral)
        self.assertIsNone(measured.existing_min_bitrate)
        self.assertIsNone(measured.existing_spectral)
        assert measured.spectral_audit.existing is not None
        self.assertEqual(measured.spectral_audit.existing.grade, "suspect")

    def test_attempt_audit_is_not_a_quality_decision_input(self):
        from lib.measurement import collect_attempt_spectral_audit
        from lib.quality import full_pipeline_decision

        def decide():
            return full_pipeline_decision(
                is_flac=False, min_bitrate=320, is_cbr=True, is_vbr=False,
                avg_bitrate=320, spectral_grade="genuine",
                spectral_bitrate=None, existing_min_bitrate=256,
                existing_spectral_grade="genuine",
                existing_spectral_bitrate=None, existing_format="mp3",
                new_format="mp3",
            )

        before = decide()
        result = SimpleNamespace(
            grade="suspect", estimated_bitrate_kbps=128,
            suspect_pct=100.0, tracks=[],
        )
        with patch("lib.measurement.spectral_analyze", return_value=result):
            collect_attempt_spectral_audit("/candidate", None)
        after = decide()
        self.assertEqual(before, after)


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
        with patch("lib.measurement.validate_audio", return_value=bad_result):
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
        with patch("lib.measurement._iter_audio_files", return_value=[]), \
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
        with patch(
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
        with patch("lib.measurement._iter_audio_files", return_value=[]), \
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
