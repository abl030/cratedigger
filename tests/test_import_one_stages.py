#!/usr/bin/env python3
"""Tests for import_one.py pure stage decision functions.

These test the decision points extracted from main() — each stage function
takes data inputs and returns a StageResult without I/O.
"""

import importlib
import os
import subprocess
import sys
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LIB_DIR = os.path.join(ROOT_DIR, "lib")
HARNESS_DIR = os.path.join(ROOT_DIR, "harness")

sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, HARNESS_DIR)


class TestImportBootstrap(unittest.TestCase):
    """Standalone harness imports should bootstrap the repo root."""

    def test_harness_only_import_bootstraps_lib_package(self):
        module_names = ["import_one", "lib", "lib.beets_db", "lib.quality", "lib.spectral_check"]
        saved_modules = {name: sys.modules.get(name) for name in module_names}
        saved_path = list(sys.path)
        try:
            for name in module_names:
                sys.modules.pop(name, None)
            sys.path[:] = [p for p in sys.path if p not in (ROOT_DIR, LIB_DIR, HARNESS_DIR)]
            sys.path.insert(0, HARNESS_DIR)

            import_one = importlib.import_module("import_one")

            self.assertEqual(import_one.ROOT_DIR, ROOT_DIR)
            self.assertIn(ROOT_DIR, sys.path)
            self.assertIn(LIB_DIR, sys.path)
            spectral_check = importlib.import_module("lib.spectral_check")
            self.assertTrue(callable(spectral_check.analyze_album))
        finally:
            sys.path[:] = saved_path
            for name in module_names:
                sys.modules.pop(name, None)
            for name, module in saved_modules.items():
                if module is not None:
                    sys.modules[name] = module


# ============================================================================
# StageResult
# ============================================================================

class TestStageResult(unittest.TestCase):
    """Test the StageResult dataclass."""

    def test_terminal_when_set(self):
        from import_one import StageResult
        r = StageResult(decision="path_missing", exit_code=3, terminal=True)
        self.assertTrue(r.is_terminal)

    def test_not_terminal_when_continue(self):
        from import_one import StageResult
        r = StageResult()
        self.assertFalse(r.is_terminal)

    def test_default_values(self):
        from import_one import StageResult
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
        from import_one import preflight_decision
        r = preflight_decision(already_in_beets=True, path_exists=False)
        self.assertEqual(r.decision, "preflight_existing")
        self.assertEqual(r.exit_code, 0)

    def test_not_in_beets_no_path(self):
        from import_one import preflight_decision
        r = preflight_decision(already_in_beets=False, path_exists=False)
        self.assertEqual(r.decision, "path_missing")
        self.assertEqual(r.exit_code, 3)

    def test_path_exists_continue(self):
        from import_one import preflight_decision
        r = preflight_decision(already_in_beets=True, path_exists=True)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)

    def test_not_in_beets_path_exists(self):
        from import_one import preflight_decision
        r = preflight_decision(already_in_beets=False, path_exists=True)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)


# ============================================================================
# conversion_decision
# ============================================================================

class TestConversionDecision(unittest.TestCase):
    """Test post-conversion decision (pure)."""

    def test_failed_conversion(self):
        from import_one import conversion_decision
        r = conversion_decision(converted=3, failed=1)
        self.assertEqual(r.decision, "conversion_failed")
        self.assertEqual(r.exit_code, 1)
        self.assertTrue(r.is_terminal)

    def test_successful_conversion(self):
        from import_one import conversion_decision
        r = conversion_decision(converted=3, failed=0)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)

    def test_no_flacs(self):
        from import_one import conversion_decision
        r = conversion_decision(converted=0, failed=0)
        self.assertEqual(r.decision, "continue")
        self.assertFalse(r.is_terminal)


# ============================================================================
# quality_decision_stage
# ============================================================================

class TestQualityDecisionStage(unittest.TestCase):
    """Test the quality comparison stage wrapper (combines pure functions).

    Uses AudioQualityMeasurement objects for new/existing.
    """

    def test_downgrade_exit_5(self):
        from import_one import quality_decision_stage
        from quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=192)
        existing = AudioQualityMeasurement(min_bitrate_kbps=320)
        r = quality_decision_stage(new, existing, is_transcode=False)
        self.assertEqual(r.decision, "downgrade")
        self.assertEqual(r.exit_code, 5)
        self.assertTrue(r.is_terminal)

    def test_transcode_downgrade_exit_6(self):
        from import_one import quality_decision_stage
        from quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=128)
        existing = AudioQualityMeasurement(min_bitrate_kbps=192)
        r = quality_decision_stage(new, existing, is_transcode=True)
        self.assertEqual(r.decision, "transcode_downgrade")
        self.assertEqual(r.exit_code, 6)
        self.assertTrue(r.is_terminal)

    def test_import_continues(self):
        from import_one import quality_decision_stage
        from quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=245, verified_lossless=True)
        existing = AudioQualityMeasurement(min_bitrate_kbps=192)
        r = quality_decision_stage(new, existing, is_transcode=False)
        self.assertEqual(r.decision, "import")
        self.assertEqual(r.exit_code, 0)
        self.assertFalse(r.is_terminal)

    def test_transcode_upgrade_continues(self):
        from import_one import quality_decision_stage
        from quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=245)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)
        r = quality_decision_stage(new, existing, is_transcode=True)
        self.assertEqual(r.decision, "transcode_upgrade")
        self.assertEqual(r.exit_code, 0)
        self.assertFalse(r.is_terminal)

    def test_first_import_no_existing(self):
        from import_one import quality_decision_stage
        from quality import AudioQualityMeasurement
        new = AudioQualityMeasurement(min_bitrate_kbps=245, verified_lossless=True)
        r = quality_decision_stage(new, None, is_transcode=False)
        self.assertEqual(r.decision, "import")
        self.assertFalse(r.is_terminal)

    def test_override_used_for_comparison(self):
        """Override bitrate should be used instead of existing when provided.
        Caller constructs existing with override bitrate already resolved."""
        from import_one import quality_decision_stage
        from quality import AudioQualityMeasurement
        # existing beets=320 but override=128 (spectral detected fake 320)
        # Caller resolves: existing gets 128. new=245 > 128, so upgrade.
        new = AudioQualityMeasurement(min_bitrate_kbps=245, verified_lossless=True)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)  # override applied by caller
        r = quality_decision_stage(new, existing, is_transcode=False)
        self.assertEqual(r.decision, "import")
        self.assertFalse(r.is_terminal)


# ============================================================================
# final_exit_decision
# ============================================================================

class TestFinalExitDecision(unittest.TestCase):
    """Test the final exit code after successful import."""

    def test_transcode_exit_6(self):
        from import_one import final_exit_decision
        self.assertEqual(final_exit_decision(is_transcode=True), 6)

    def test_normal_exit_0(self):
        from import_one import final_exit_decision
        self.assertEqual(final_exit_decision(is_transcode=False), 0)


# ============================================================================
# convert_lossless_to_v0 keep_source parameter
# ============================================================================

# ============================================================================
# conversion_target — single decision for all lossless conversion
# ============================================================================

class TestConversionTarget(unittest.TestCase):
    """Test conversion_target: what should lossless files become on disk?"""

    def _target(self, target_format=None, verified=False, opus=False):
        from import_one import conversion_target
        return conversion_target(target_format, verified, opus)

    def test_default_is_v0(self):
        self.assertEqual(self._target(), "v0")

    def test_target_format_flac_keeps_flac(self):
        self.assertEqual(self._target(target_format="flac"), "flac")

    def test_target_format_flac_overrides_opus(self):
        self.assertEqual(self._target(target_format="flac", verified=True, opus=True), "flac")

    def test_verified_with_opus_returns_opus(self):
        self.assertEqual(self._target(verified=True, opus=True), "opus")

    def test_verified_without_opus_returns_v0(self):
        self.assertEqual(self._target(verified=True, opus=False), "v0")

    def test_not_verified_with_opus_returns_v0(self):
        self.assertEqual(self._target(verified=False, opus=True), "v0")


# ============================================================================
# opus_cleanup_decision — clean up FLAC when opus path skipped
# ============================================================================

class TestOpusCleanupDecision(unittest.TestCase):
    """When opus was requested but skipped (transcode), FLAC files must be cleaned up."""

    def test_opus_skipped_with_flag_needs_cleanup(self):
        from import_one import opus_cleanup_decision
        r = opus_cleanup_decision(opus_skipped=True,
                                  opus_conversion_enabled=True,
                                  converted=5)
        self.assertTrue(r)

    def test_opus_skipped_without_flag_no_cleanup(self):
        from import_one import opus_cleanup_decision
        r = opus_cleanup_decision(opus_skipped=True,
                                  opus_conversion_enabled=False,
                                  converted=5)
        self.assertFalse(r)

    def test_opus_not_skipped_no_cleanup(self):
        from import_one import opus_cleanup_decision
        r = opus_cleanup_decision(opus_skipped=False,
                                  opus_conversion_enabled=True,
                                  converted=5)
        self.assertFalse(r)

    def test_no_conversion_no_cleanup(self):
        from import_one import opus_cleanup_decision
        r = opus_cleanup_decision(opus_skipped=True,
                                  opus_conversion_enabled=True,
                                  converted=0)
        self.assertFalse(r)




class TestConvertV0KeepSource(unittest.TestCase):
    """Test that keep_source=True preserves original lossless files."""

    def test_keep_source_preserves_flac(self):
        """With keep_source=True, FLAC files should remain after V0 conversion."""
        import tempfile
        from import_one import convert_lossless_to_v0
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a small valid FLAC file via ffmpeg
            flac_path = os.path.join(tmpdir, "track01.flac")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-y", flac_path],
                capture_output=True, timeout=30)
            self.assertTrue(os.path.exists(flac_path))
            converted, failed, ext = convert_lossless_to_v0(tmpdir, keep_source=True)
            self.assertEqual(converted, 1)
            self.assertEqual(failed, 0)
            # FLAC still present
            self.assertTrue(os.path.exists(flac_path))
            # MP3 was created
            mp3_path = os.path.join(tmpdir, "track01.mp3")
            self.assertTrue(os.path.exists(mp3_path))

    def test_default_removes_flac(self):
        """Default behavior (keep_source=False) removes FLAC after conversion."""
        import tempfile
        from import_one import convert_lossless_to_v0
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = os.path.join(tmpdir, "track01.flac")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-y", flac_path],
                capture_output=True, timeout=30)
            converted, failed, ext = convert_lossless_to_v0(tmpdir)
            self.assertEqual(converted, 1)
            # FLAC removed (default behavior)
            self.assertFalse(os.path.exists(flac_path))


# ============================================================================
# convert_lossless_to_opus
# ============================================================================

class TestConvertLosslessToOpus(unittest.TestCase):
    """Test the Opus conversion function."""

    def test_converts_flac_to_opus(self):
        """FLAC files should be converted to .opus files."""
        import tempfile
        from import_one import convert_lossless_to_opus
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = os.path.join(tmpdir, "track01.flac")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-y", flac_path],
                capture_output=True, timeout=30)
            converted, failed = convert_lossless_to_opus(tmpdir)
            self.assertEqual(converted, 1)
            self.assertEqual(failed, 0)
            opus_path = os.path.join(tmpdir, "track01.opus")
            self.assertTrue(os.path.exists(opus_path))
            # Source FLAC is NOT deleted (caller manages lifecycle)
            self.assertTrue(os.path.exists(flac_path))

    def test_no_lossless_files_noop(self):
        """No lossless files → (0, 0)."""
        import tempfile
        from import_one import convert_lossless_to_opus
        with tempfile.TemporaryDirectory() as tmpdir:
            mp3_path = os.path.join(tmpdir, "track01.mp3")
            with open(mp3_path, "w") as f:
                f.write("not real")
            converted, failed = convert_lossless_to_opus(tmpdir)
            self.assertEqual(converted, 0)
            self.assertEqual(failed, 0)

    def test_dry_run_no_files_created(self):
        """Dry run should not create any Opus files."""
        import tempfile
        from import_one import convert_lossless_to_opus
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = os.path.join(tmpdir, "track01.flac")
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                 "-y", flac_path],
                capture_output=True, timeout=30)
            converted, failed = convert_lossless_to_opus(tmpdir, dry_run=True)
            self.assertEqual(converted, 1)
            self.assertEqual(failed, 0)
            opus_path = os.path.join(tmpdir, "track01.opus")
            self.assertFalse(os.path.exists(opus_path))


if __name__ == "__main__":
    unittest.main()
