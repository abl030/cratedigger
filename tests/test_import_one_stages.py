#!/usr/bin/env python3
"""Tests for import_one.py pure stage decision functions.

These test the decision points extracted from main() — each stage function
takes data inputs and returns a StageResult without I/O.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "harness"))


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


if __name__ == "__main__":
    unittest.main()
