#!/usr/bin/env python3
"""Tests for import_one.py pure stage decision functions.

These test the decision points extracted from main() — each stage function
takes data inputs and returns a StageResult without I/O.
"""

import io
import importlib
import os
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HARNESS_DIR = os.path.join(ROOT_DIR, "harness")

sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, HARNESS_DIR)


class TestImportBootstrap(unittest.TestCase):
    """Standalone harness imports should bootstrap the repo root so lib.* resolves.

    The bootstrap deliberately does NOT add ``lib/`` to sys.path — doing so
    would reintroduce the issue #95 dual-load footgun where a module is
    reachable under both ``quality`` and ``lib.quality``.
    """

    def test_standalone_invocation_resolves_lib_imports(self):
        """Running `python harness/import_one.py` directly must resolve lib.*.

        Python puts the script's directory (``harness/``) first on sys.path,
        not the repo root. ``_bootstrap_import_paths()`` inserts the repo
        root so ``from lib.X import Y`` resolves even without PYTHONPATH.
        """
        proc = subprocess.run(
            [sys.executable, "-c",
             "import sys, os\n"
             f"sys.path.insert(0, {HARNESS_DIR!r})\n"
             "import import_one\n"
             "assert 'lib.quality' in sys.modules\n"
             "assert 'lib.beets_db' in sys.modules\n"
             f"assert {ROOT_DIR!r} in sys.path\n"
             "print('OK')\n"],
            capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(
            proc.returncode, 0,
            f"Standalone import_one import failed:\nstdout:{proc.stdout}\nstderr:{proc.stderr}"
        )
        self.assertIn("OK", proc.stdout)


class TestPipelineDbUpdate(unittest.TestCase):
    @patch("lib.transitions.finalize_request")
    @patch("lib.pipeline_db.PipelineDB")
    def test_update_pipeline_db_routes_through_shared_finalizer(
        self,
        mock_db_cls,
        mock_finalize,
    ) -> None:
        from harness import import_one

        db = MagicMock()
        mock_db_cls.return_value = db

        import_one.update_pipeline_db(
            42,
            "imported",
            imported_path="/Beets/Artist/Album",
            distance=0.12,
            scenario="preflight_existing",
        )

        mock_finalize.assert_called_once()
        called_db, called_request_id, outcome = mock_finalize.call_args.args
        self.assertIs(called_db, db)
        self.assertEqual(called_request_id, 42)
        self.assertEqual(outcome.target_status, "imported")
        self.assertEqual(
            outcome.fields,
            {
                "imported_path": "/Beets/Artist/Album",
                "beets_distance": 0.12,
                "beets_scenario": "preflight_existing",
            },
        )
        db.close.assert_called_once()

    @patch("lib.transitions.finalize_request", side_effect=RuntimeError("boom"))
    @patch("lib.pipeline_db.PipelineDB")
    def test_update_pipeline_db_closes_db_when_finalizer_raises(
        self,
        mock_db_cls,
        _mock_finalize,
    ) -> None:
        from harness import import_one

        db = MagicMock()
        mock_db_cls.return_value = db
        stderr = io.StringIO()

        with patch("sys.stderr", stderr):
            import_one.update_pipeline_db(42, "imported")

        db.close.assert_called_once()
        self.assertIn("Pipeline DB update failed", stderr.getvalue())

    @patch("lib.transitions.finalize_request")
    @patch("lib.pipeline_db.PipelineDB")
    def test_update_pipeline_db_distinguishes_transition_whitelist_errors(
        self,
        mock_db_cls,
        mock_finalize,
    ) -> None:
        from harness import import_one

        db = MagicMock()
        mock_db_cls.return_value = db
        stderr = io.StringIO()

        with patch("sys.stderr", stderr):
            import_one.update_pipeline_db(
                42,
                "wanted",
                imported_path="/Beets/Artist/Album",
            )

        mock_finalize.assert_not_called()
        db.close.assert_called_once()
        self.assertIn("Pipeline DB transition rejected", stderr.getvalue())
        self.assertIn("imported_path", stderr.getvalue())

    @patch("lib.transitions.finalize_request")
    @patch("lib.pipeline_db.PipelineDB")
    def test_update_pipeline_db_rejects_unknown_status_and_closes_db(
        self,
        mock_db_cls,
        mock_finalize,
    ) -> None:
        from harness import import_one

        db = MagicMock()
        mock_db_cls.return_value = db
        stderr = io.StringIO()

        with patch("sys.stderr", stderr):
            import_one.update_pipeline_db(42, "queued")

        mock_finalize.assert_not_called()
        db.close.assert_called_once()
        self.assertIn("Pipeline DB transition rejected", stderr.getvalue())
        self.assertIn("queued", stderr.getvalue())


# ============================================================================
# StageResult
# ============================================================================

class TestStageResult(unittest.TestCase):
    """Test the StageResult dataclass."""

    def test_terminal_when_set(self):
        from harness.import_one import StageResult
        r = StageResult(decision="path_missing", exit_code=3, terminal=True)
        self.assertTrue(r.is_terminal)

    def test_not_terminal_when_continue(self):
        from harness.import_one import StageResult
        r = StageResult()
        self.assertFalse(r.is_terminal)

    def test_default_values(self):
        from harness.import_one import StageResult
        r = StageResult()
        self.assertEqual(r.decision, "continue")
        self.assertEqual(r.exit_code, 0)
        self.assertIsNone(r.error)
        self.assertFalse(r.terminal)


# ============================================================================
# preflight_decision
# ============================================================================

class TestPreflightDecision(unittest.TestCase):
    """Test the preflight stage decision logic (pure)."""

    def test_already_in_beets_no_path(self):
        from harness.import_one import preflight_decision
        r = preflight_decision(already_in_beets=True, path_exists=False)
        self.assertEqual(r.decision, "preflight_existing")
        self.assertEqual(r.exit_code, 0)

    def test_not_in_beets_no_path(self):
        from harness.import_one import preflight_decision
        r = preflight_decision(already_in_beets=False, path_exists=False)
        self.assertEqual(r.decision, "path_missing")
        self.assertEqual(r.exit_code, 3)

    def test_path_exists_continue(self):
        from harness.import_one import preflight_decision
        r = preflight_decision(already_in_beets=True, path_exists=True)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)

    def test_not_in_beets_path_exists(self):
        from harness.import_one import preflight_decision
        r = preflight_decision(already_in_beets=False, path_exists=True)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)


# ============================================================================
# conversion_decision
# ============================================================================

class TestConversionDecision(unittest.TestCase):
    """Test post-conversion decision (pure)."""

    def test_failed_conversion(self):
        from harness.import_one import conversion_decision
        r = conversion_decision(converted=3, failed=1)
        self.assertEqual(r.decision, "conversion_failed")
        self.assertEqual(r.exit_code, 1)
        self.assertTrue(r.is_terminal)

    def test_successful_conversion(self):
        from harness.import_one import conversion_decision
        r = conversion_decision(converted=3, failed=0)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)

    def test_no_flacs(self):
        from harness.import_one import conversion_decision
        r = conversion_decision(converted=0, failed=0)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)


class TestM4aAlacDetection(unittest.TestCase):
    """ALAC .m4a detection gates lossless conversion."""

    @patch("harness.import_one.subprocess.run")
    def test_parses_structured_ffprobe_codec_output(self, mock_run):
        """Structured ffprobe output must identify ALAC as lossless."""
        from harness.import_one import _is_m4a_alac

        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"streams":[{"codec_name":"alac"}]}',
            stderr="",
        )

        self.assertTrue(_is_m4a_alac("track.m4a"))
        cmd = mock_run.call_args.args[0]
        self.assertEqual(cmd[cmd.index("-of") + 1], "json")

    @patch("harness.import_one.subprocess.run")
    def test_non_alac_m4a_is_not_lossless(self, mock_run):
        from harness.import_one import _is_m4a_alac

        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"streams":[{"codec_name":"aac"}]}',
            stderr="",
        )

        self.assertFalse(_is_m4a_alac("track.m4a"))


# ============================================================================
# quality_decision_stage
# ============================================================================

class TestQualityDecisionStage(unittest.TestCase):
    """Test the quality comparison stage wrapper (combines pure functions).

    Uses AudioQualityMeasurement objects for new/existing.
    """

    def test_downgrade_exit_5(self):
        from harness.import_one import quality_decision_stage
        from lib.quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=192)
        existing = AudioQualityMeasurement(min_bitrate_kbps=320)
        r = quality_decision_stage(new, existing, is_transcode=False)
        self.assertEqual(r.decision, "downgrade")
        self.assertEqual(r.exit_code, 5)
        self.assertTrue(r.is_terminal)

    def test_transcode_downgrade_exit_6(self):
        from harness.import_one import quality_decision_stage
        from lib.quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=128)
        existing = AudioQualityMeasurement(min_bitrate_kbps=192)
        r = quality_decision_stage(new, existing, is_transcode=True)
        self.assertEqual(r.decision, "transcode_downgrade")
        self.assertEqual(r.exit_code, 6)
        self.assertTrue(r.is_terminal)

    def test_import_continues(self):
        from harness.import_one import quality_decision_stage
        from lib.quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=245, verified_lossless=True)
        existing = AudioQualityMeasurement(min_bitrate_kbps=192)
        r = quality_decision_stage(new, existing, is_transcode=False)
        self.assertEqual(r.decision, "import")
        self.assertEqual(r.exit_code, 0)
        self.assertFalse(r.is_terminal)

    def test_transcode_upgrade_continues(self):
        from harness.import_one import quality_decision_stage
        from lib.quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=245)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)
        r = quality_decision_stage(new, existing, is_transcode=True)
        self.assertEqual(r.decision, "transcode_upgrade")
        self.assertEqual(r.exit_code, 0)
        self.assertFalse(r.is_terminal)

    def test_first_import_no_existing(self):
        from harness.import_one import quality_decision_stage
        from lib.quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=245, verified_lossless=True)
        r = quality_decision_stage(new, None, is_transcode=False)
        self.assertEqual(r.decision, "import")
        self.assertFalse(r.is_terminal)

    def test_override_used_for_comparison(self):
        """Override bitrate should be used instead of existing when provided.
        Caller constructs existing with override bitrate already resolved."""
        from harness.import_one import quality_decision_stage
        from lib.quality import AudioQualityMeasurement
        # existing beets=320 but override=128 (spectral detected fake 320)
        # Caller resolves: existing gets 128. new=245 > 128, so upgrade.
        new = AudioQualityMeasurement(min_bitrate_kbps=245, verified_lossless=True)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)  # override applied by caller
        r = quality_decision_stage(new, existing, is_transcode=False)
        self.assertEqual(r.decision, "import")
        self.assertFalse(r.is_terminal)


class TestExistingMeasurementBuilder(unittest.TestCase):
    """Tests for import_one's existing-measurement wiring."""

    def test_override_replaces_avg_metric_too_for_cbr(self):
        """Spectral override drives every metric when existing is CBR.

        Issue #64 added MEDIAN as a third metric. For a monolithic CBR file
        (``is_cbr=True``), every bitrate field is the same value — so the
        spectral clamp must drive all three, otherwise a fake CBR 320 that's
        really 96 kbps audio would still out-rank a genuine V0 under the
        AVG or MEDIAN policy (see test_override_preserves_vbr_avg_and_median
        for the complementary case).
        """
        from harness.import_one import build_existing_measurement
        from lib.beets_db import AlbumInfo

        info = AlbumInfo(
            album_id=1,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Test",
        )
        m = build_existing_measurement(
            info,
            override_min_bitrate=128,
            existing_spectral_grade=None,
            existing_spectral_bitrate=None,
        )
        self.assertIsNotNone(m)
        assert m is not None
        self.assertEqual(m.min_bitrate_kbps, 128)
        self.assertEqual(
            m.avg_bitrate_kbps, 128,
            "override_min_bitrate must drive comparison under AVG for CBR existing")
        self.assertEqual(
            m.median_bitrate_kbps, 128,
            "override_min_bitrate must drive comparison under MEDIAN for CBR existing")

    def test_override_preserves_vbr_avg_and_median(self):
        """Override must NOT clobber avg/median when existing is genuinely VBR.

        Live reproduction: Unter Null - The Failure Epiphany (req 1749).
        Beets album with per-track bitrates spanning 152-310 kbps (avg 225)
        is unambiguously VBR. Spectral analysis produced ``likely_transcode``
        at 96 kbps — a false positive on industrial/electronic source that
        naturally lacks high-frequency content.

        Old behavior: override clobbered all three metrics to 96, making
        every 152 kbps CBR transcode "win" against ``existing.avg=96`` at
        compare_quality → imported → gate denied → requeued → repeat. The
        loop the user called out on 2026-04-21.

        New behavior: for VBR existing, keep the real ``avg`` / ``median``
        from beets. Only ``min`` takes the clamp (preserves the
        fake-CBR-320 protection for the CBR branch). The true 225 avg
        survives and a 152 CBR transcode now reads as a genuine downgrade.

        The complementary CBR case is test_override_replaces_avg_metric_too_for_cbr.
        """
        from harness.import_one import build_existing_measurement
        from lib.beets_db import AlbumInfo

        info = AlbumInfo(
            album_id=1,
            track_count=24,
            min_bitrate_kbps=152,
            avg_bitrate_kbps=225,
            median_bitrate_kbps=224,
            format="MP3",
            is_cbr=False,
            album_path="/Beets/Unter Null/2005 - The Failure Epiphany",
        )
        m = build_existing_measurement(
            info,
            override_min_bitrate=96,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=96,
        )
        self.assertIsNotNone(m)
        assert m is not None
        self.assertEqual(
            m.min_bitrate_kbps, 96,
            "min takes the spectral clamp (unchanged)")
        self.assertEqual(
            m.avg_bitrate_kbps, 225,
            "VBR existing must keep its real avg — clobbering it made "
            "1749 loop forever on same-quality transcodes")
        self.assertEqual(
            m.median_bitrate_kbps, 224,
            "VBR existing must keep its real median for the MEDIAN policy")

    def test_override_no_op_when_not_supplied(self):
        """No override → all three fields pass through unchanged (CBR or VBR)."""
        from harness.import_one import build_existing_measurement
        from lib.beets_db import AlbumInfo

        info = AlbumInfo(
            album_id=1,
            track_count=24,
            min_bitrate_kbps=152,
            avg_bitrate_kbps=225,
            median_bitrate_kbps=224,
            format="MP3",
            is_cbr=False,
            album_path="/Beets/Test",
        )
        m = build_existing_measurement(
            info,
            override_min_bitrate=None,
            existing_spectral_grade="genuine",
            existing_spectral_bitrate=None,
        )
        self.assertIsNotNone(m)
        assert m is not None
        self.assertEqual(m.min_bitrate_kbps, 152)
        self.assertEqual(m.avg_bitrate_kbps, 225)
        self.assertEqual(m.median_bitrate_kbps, 224)


# ============================================================================
# final_exit_decision
# ============================================================================

class TestFinalExitDecision(unittest.TestCase):
    """Test the final exit code after successful import."""

    def test_transcode_exit_6(self):
        from harness.import_one import final_exit_decision
        self.assertEqual(final_exit_decision(is_transcode=True), 6)

    def test_normal_exit_0(self):
        from harness.import_one import final_exit_decision
        self.assertEqual(final_exit_decision(is_transcode=False), 0)


# ============================================================================
# convert_lossless keep_source parameter
# ============================================================================

# ============================================================================
# conversion_target — single decision for all lossless conversion
# ============================================================================

class TestConversionTarget(unittest.TestCase):
    """Test conversion_target: what should lossless files become on disk?"""

    def _target(self, target_format=None, verified=False, vl_target=None):
        from harness.import_one import conversion_target
        return conversion_target(target_format, verified, vl_target)

    def test_default_is_none(self):
        """No target configured, not verified → None (keep V0)."""
        self.assertIsNone(self._target())

    def test_target_format_flac_keeps_lossless(self):
        self.assertEqual(self._target(target_format="flac"), "lossless")

    def test_target_format_flac_overrides_target(self):
        self.assertEqual(self._target(target_format="flac", verified=True,
                                      vl_target="opus 128"), "lossless")

    def test_target_format_lossless_keeps_lossless(self):
        self.assertEqual(self._target(target_format="lossless"), "lossless")

    def test_verified_with_target_returns_target(self):
        self.assertEqual(self._target(verified=True, vl_target="opus 128"),
                         "opus 128")

    def test_verified_without_target_returns_none(self):
        self.assertIsNone(self._target(verified=True, vl_target=None))

    def test_not_verified_with_target_returns_none(self):
        self.assertIsNone(self._target(verified=False, vl_target="opus 128"))


class TestShouldRunTargetConversion(unittest.TestCase):
    """Second conversion pass should skip the keep-lossless sentinel."""

    def test_none_skips_target_conversion(self):
        from harness.import_one import should_run_target_conversion
        self.assertFalse(should_run_target_conversion(None))

    def test_lossless_sentinel_skips_target_conversion(self):
        from harness.import_one import should_run_target_conversion
        self.assertFalse(should_run_target_conversion("lossless"))

    def test_real_target_runs_second_pass(self):
        from harness.import_one import should_run_target_conversion
        self.assertTrue(should_run_target_conversion("opus 128"))


# ============================================================================
# target_cleanup_decision — clean up sources when target conversion skipped
# ============================================================================

class TestTargetCleanupDecision(unittest.TestCase):
    """When a target was configured but skipped (transcode), source files must be cleaned up.

    Extended for issue #111 with ``preserve_source`` — when force/manual import
    asked the V0 pass to preserve originals until the quality decision, and the
    decision was non-terminal (import going ahead), we must still clean up before
    beets sees FLAC+MP3 and tries to catalog both.
    """

    def test_target_skipped_needs_cleanup(self):
        from harness.import_one import target_cleanup_decision
        self.assertTrue(target_cleanup_decision(
            target_achieved=False, target_was_configured=True, sources_kept=5))

    def test_no_target_configured_no_cleanup(self):
        from harness.import_one import target_cleanup_decision
        self.assertFalse(target_cleanup_decision(
            target_achieved=False, target_was_configured=False, sources_kept=5))

    def test_target_achieved_no_cleanup(self):
        from harness.import_one import target_cleanup_decision
        self.assertFalse(target_cleanup_decision(
            target_achieved=True, target_was_configured=True, sources_kept=5))

    def test_no_sources_no_cleanup(self):
        from harness.import_one import target_cleanup_decision
        self.assertFalse(target_cleanup_decision(
            target_achieved=False, target_was_configured=True, sources_kept=0))

    # --- Issue #111: preserve_source case (force/manual import) ---

    def test_preserve_source_no_target_needs_cleanup(self):
        """Force/manual import held sources past V0; decision was non-terminal
        so beets is about to run — clean FLACs so beets sees only V0 MP3s."""
        from harness.import_one import target_cleanup_decision
        self.assertTrue(target_cleanup_decision(
            target_achieved=False, target_was_configured=False, sources_kept=5,
            preserve_source=True))

    def test_preserve_source_retry_without_converted_still_cleans(self):
        """PR #112 Codex round 1 P2: on a retry of a previously-rejected
        force/manual attempt the V0 MP3s already exist, so
        ``convert_lossless`` skips and reports ``converted == 0``. The
        lossless originals from the prior run are still on disk and must
        still be cleaned before beets runs — otherwise beets sees a mixed
        FLAC+MP3 tree and won't evaluate the intended V0-only media.
        ``_remove_lossless_files`` is idempotent, so True with nothing to
        remove is a safe no-op."""
        from harness.import_one import target_cleanup_decision
        self.assertTrue(target_cleanup_decision(
            target_achieved=False, target_was_configured=False, sources_kept=0,
            preserve_source=True))

    def test_preserve_source_with_target_achieved_still_returns_true(self):
        """Target path already removed lossless files at line ~1049, so
        ``_remove_lossless_files`` is a no-op here. Returning True for the
        preserve_source mode is safe (idempotent) and keeps the predicate
        simple — the caller does not have to track which path cleaned."""
        from harness.import_one import target_cleanup_decision
        self.assertTrue(target_cleanup_decision(
            target_achieved=True, target_was_configured=True, sources_kept=5,
            preserve_source=True))

    def test_preserve_source_with_target_skipped_needs_cleanup(self):
        """Target was configured but skipped (transcode) — source cleanup needed
        regardless of preserve_source flag."""
        from harness.import_one import target_cleanup_decision
        self.assertTrue(target_cleanup_decision(
            target_achieved=False, target_was_configured=True, sources_kept=5,
            preserve_source=True))

    def test_no_preserve_source_no_target_no_cleanup(self):
        """Default auto-import path without target: source was already deleted
        in convert_lossless (keep_source=False), so no kept sources to clean."""
        from harness.import_one import target_cleanup_decision
        self.assertFalse(target_cleanup_decision(
            target_achieved=False, target_was_configured=False, sources_kept=5,
            preserve_source=False))




class TestConvertLosslessKeepSource(unittest.TestCase):
    """Test that keep_source=True preserves original lossless files."""

    def test_keep_source_preserves_flac(self):
        """With keep_source=True, FLAC files should remain after V0 conversion."""
        import tempfile
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = os.path.join(tmpdir, "track01.flac")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-y", flac_path],
                capture_output=True, timeout=30)
            self.assertTrue(os.path.exists(flac_path))
            converted, failed, ext = convert_lossless(tmpdir, V0_SPEC,
                                                      keep_source=True)
            self.assertEqual(converted, 1)
            self.assertEqual(failed, 0)
            self.assertTrue(os.path.exists(flac_path))
            mp3_path = os.path.join(tmpdir, "track01.mp3")
            self.assertTrue(os.path.exists(mp3_path))

    def test_default_removes_flac(self):
        """Default behavior (keep_source=False) removes FLAC after conversion."""
        import tempfile
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = os.path.join(tmpdir, "track01.flac")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-y", flac_path],
                capture_output=True, timeout=30)
            converted, failed, ext = convert_lossless(tmpdir, V0_SPEC)
            self.assertEqual(converted, 1)
            self.assertFalse(os.path.exists(flac_path))


# ============================================================================
# --preserve-source CLI flag — issue #111
# ============================================================================

class TestPreserveSourceFlag(unittest.TestCase):
    """The --preserve-source flag tells the V0 conversion to keep FLACs on
    disk until the quality decision. Force/manual-import sets this so a
    downgrade verdict does not silently destroy the user's source FLACs in
    failed_imports/.

    Verified by invoking ``import_one.py --help`` via subprocess — this
    exercises the *real* argparse construction inside ``main()`` rather than
    duplicating it.
    """

    def test_flag_present_in_help(self):
        import_script = os.path.join(HARNESS_DIR, "import_one.py")
        result = subprocess.run(
            [sys.executable, import_script, "--help"],
            capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--preserve-source", result.stdout)


# ============================================================================
# _find_target_candidate — int-vs-str MBID matching at the import stage
# ============================================================================

class TestFindTargetCandidate(unittest.TestCase):
    """Same int-vs-str trap as lib/beets.py::beets_validate (PR #98).

    The validation stage was fixed there; the import stage in
    import_one.py had a separate copy of the same broken comparison.
    Without coercion, every Discogs candidate that *passed* validation
    would still fail at import with `[SKIP] MBID … not in N candidates`.
    """

    def test_int_album_id_matches_str_target(self):
        """Discogs candidate with int album_id matches str DB mb_release_id."""
        from harness.import_one import _find_target_candidate
        cands = [{"album_id": 2085134, "distance": 0.05}]
        self.assertEqual(_find_target_candidate(cands, "2085134"), 0)

    def test_str_album_id_matches_str_target(self):
        """MusicBrainz UUID path still works."""
        from harness.import_one import _find_target_candidate
        uuid = "f100b6b0-6daa-4c9b-b33a-3e14c564cf58"
        cands = [{"album_id": uuid, "distance": 0.02}]
        self.assertEqual(_find_target_candidate(cands, uuid), 0)

    def test_no_match_returns_none(self):
        from harness.import_one import _find_target_candidate
        cands = [{"album_id": 999999}, {"album_id": "other-uuid"}]
        self.assertIsNone(_find_target_candidate(cands, "2085134"))

    def test_picks_first_match_when_multiple(self):
        """Stable ordering: first match wins."""
        from harness.import_one import _find_target_candidate
        cands = [
            {"album_id": "wrong"},
            {"album_id": 2085134},      # int, target match
            {"album_id": "2085134"},    # str, also match — but earlier wins
        ]
        self.assertEqual(_find_target_candidate(cands, "2085134"), 1)

    def test_empty_candidates_returns_none(self):
        from harness.import_one import _find_target_candidate
        self.assertIsNone(_find_target_candidate([], "2085134"))


if __name__ == "__main__":
    unittest.main()
