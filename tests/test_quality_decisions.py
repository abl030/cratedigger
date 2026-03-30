#!/usr/bin/env python3
"""Unit tests for lib/quality.py pure decision functions.

These test every branch of the four decision functions directly,
independent of real audio fixtures or the full_pipeline_decision integrator.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import (
    spectral_import_decision,
    import_quality_decision,
    transcode_detection,
    quality_gate_decision,
    QUALITY_MIN_BITRATE_KBPS,
    TRANSCODE_MIN_BITRATE_KBPS,
)


# ============================================================================
# spectral_import_decision
# ============================================================================

class TestSpectralImportDecision(unittest.TestCase):
    """Test pre-import spectral decision (MP3/CBR path)."""

    # --- genuine / marginal always import ---

    def test_genuine_imports(self):
        self.assertEqual(spectral_import_decision("genuine", None, None), "import")

    def test_genuine_imports_regardless_of_bitrates(self):
        self.assertEqual(spectral_import_decision("genuine", 128, 256), "import")

    def test_marginal_imports(self):
        self.assertEqual(spectral_import_decision("marginal", 192, 256), "import")

    def test_marginal_imports_no_bitrates(self):
        self.assertEqual(spectral_import_decision("marginal", None, None), "import")

    # --- suspect: reject when not an upgrade ---

    def test_suspect_rejects_when_not_upgrade(self):
        self.assertEqual(
            spectral_import_decision("suspect", 128, 128), "reject")

    def test_suspect_rejects_when_worse(self):
        self.assertEqual(
            spectral_import_decision("suspect", 96, 128), "reject")

    def test_likely_transcode_rejects_when_not_upgrade(self):
        self.assertEqual(
            spectral_import_decision("likely_transcode", 160, 160), "reject")

    # --- suspect: upgrade when better ---

    def test_suspect_upgrades_when_better(self):
        self.assertEqual(
            spectral_import_decision("suspect", 192, 128), "import_upgrade")

    def test_likely_transcode_upgrades_when_better(self):
        self.assertEqual(
            spectral_import_decision("likely_transcode", 192, 96), "import_upgrade")

    # --- suspect: no existing ---

    def test_suspect_no_existing_zero(self):
        self.assertEqual(
            spectral_import_decision("suspect", 128, 0), "import_no_exist")

    def test_suspect_no_existing_none(self):
        self.assertEqual(
            spectral_import_decision("suspect", 128, None), "import_no_exist")

    def test_likely_transcode_no_existing(self):
        self.assertEqual(
            spectral_import_decision("likely_transcode", 96, None), "import_no_exist")

    # --- suspect: no new bitrate ---

    def test_suspect_no_new_bitrate_no_existing(self):
        """No cliff detected → spectral_bitrate=None, nothing on disk."""
        self.assertEqual(
            spectral_import_decision("suspect", None, None), "import_no_exist")

    def test_suspect_no_new_bitrate_with_existing(self):
        """No cliff detected → spectral_bitrate=None, something on disk."""
        self.assertEqual(
            spectral_import_decision("suspect", None, 128), "import")


# ============================================================================
# import_quality_decision
# ============================================================================

class TestImportQualityDecision(unittest.TestCase):
    """Test import decision (FLAC conversion / bitrate comparison path)."""

    # --- verified lossless always wins ---

    def test_verified_lossless_always_imports(self):
        self.assertEqual(
            import_quality_decision(240, 320, will_be_verified_lossless=True),
            "import")

    def test_verified_lossless_even_lower_bitrate(self):
        """V0 at 207kbps from genuine FLAC still imports over CBR 320."""
        self.assertEqual(
            import_quality_decision(207, 320, will_be_verified_lossless=True),
            "import")

    def test_verified_lossless_no_existing(self):
        self.assertEqual(
            import_quality_decision(240, None, will_be_verified_lossless=True),
            "import")

    # --- normal upgrade ---

    def test_upgrade_imports(self):
        self.assertEqual(
            import_quality_decision(256, 192), "import")

    def test_equal_bitrate_is_downgrade(self):
        self.assertEqual(
            import_quality_decision(320, 320), "downgrade")

    def test_lower_bitrate_is_downgrade(self):
        self.assertEqual(
            import_quality_decision(192, 320), "downgrade")

    # --- override_min_bitrate ---

    def test_override_replaces_existing(self):
        """Pipeline DB says existing is 128 (spectral), beets says 320."""
        self.assertEqual(
            import_quality_decision(240, 320, override_min_bitrate=128),
            "import")

    def test_override_causes_downgrade(self):
        self.assertEqual(
            import_quality_decision(100, 320, override_min_bitrate=128),
            "downgrade")

    # --- transcode scenarios ---

    def test_transcode_upgrade(self):
        self.assertEqual(
            import_quality_decision(192, 128, is_transcode=True),
            "transcode_upgrade")

    def test_transcode_downgrade(self):
        self.assertEqual(
            import_quality_decision(128, 192, is_transcode=True),
            "transcode_downgrade")

    def test_transcode_equal_is_downgrade(self):
        self.assertEqual(
            import_quality_decision(128, 128, is_transcode=True),
            "transcode_downgrade")

    def test_transcode_first_import(self):
        """No existing album — transcode is better than nothing."""
        self.assertEqual(
            import_quality_decision(150, None, is_transcode=True),
            "transcode_first")

    # --- first import (no existing) ---

    def test_first_import_no_existing(self):
        self.assertEqual(
            import_quality_decision(240, None), "import")

    def test_first_import_no_bitrates(self):
        self.assertEqual(
            import_quality_decision(None, None), "import")


# ============================================================================
# transcode_detection
# ============================================================================

class TestTranscodeDetection(unittest.TestCase):
    """Test post-conversion transcode detection."""

    def test_no_conversion_not_transcode(self):
        self.assertFalse(transcode_detection(0, 150))

    def test_none_bitrate_not_transcode(self):
        self.assertFalse(transcode_detection(5, None))

    def test_above_threshold_not_transcode(self):
        self.assertFalse(transcode_detection(10, 240))

    def test_at_threshold_not_transcode(self):
        self.assertFalse(transcode_detection(10, TRANSCODE_MIN_BITRATE_KBPS))

    def test_below_threshold_is_transcode(self):
        self.assertTrue(transcode_detection(10, 190))

    def test_way_below_threshold_is_transcode(self):
        self.assertTrue(transcode_detection(1, 96))

    def test_just_below_threshold_is_transcode(self):
        self.assertTrue(transcode_detection(5, TRANSCODE_MIN_BITRATE_KBPS - 1))


# ============================================================================
# quality_gate_decision
# ============================================================================

class TestQualityGateDecision(unittest.TestCase):
    """Test post-import quality gate."""

    # --- accept cases ---

    def test_vbr_above_threshold_accepts(self):
        self.assertEqual(
            quality_gate_decision(240, is_cbr=False, verified_lossless=False),
            "accept")

    def test_vbr_at_threshold_accepts(self):
        self.assertEqual(
            quality_gate_decision(QUALITY_MIN_BITRATE_KBPS, is_cbr=False, verified_lossless=False),
            "accept")

    def test_verified_lossless_accepts_regardless(self):
        """Verified lossless with low bitrate (quiet music) still accepts."""
        self.assertEqual(
            quality_gate_decision(180, is_cbr=False, verified_lossless=True),
            "accept")

    def test_verified_lossless_cbr_accepts(self):
        """verified_lossless + CBR = accept (we verified it)."""
        self.assertEqual(
            quality_gate_decision(320, is_cbr=True, verified_lossless=True),
            "accept")

    # --- requeue_upgrade cases ---

    def test_below_threshold_requeues_upgrade(self):
        self.assertEqual(
            quality_gate_decision(190, is_cbr=False, verified_lossless=False),
            "requeue_upgrade")

    def test_way_below_threshold_requeues(self):
        self.assertEqual(
            quality_gate_decision(96, is_cbr=False, verified_lossless=False),
            "requeue_upgrade")

    def test_spectral_override_requeues(self):
        """Beets says 320 but spectral says 128 → use spectral → requeue."""
        self.assertEqual(
            quality_gate_decision(320, is_cbr=True, verified_lossless=False,
                                  spectral_bitrate=128),
            "requeue_upgrade")

    def test_spectral_higher_than_bitrate_ignored(self):
        """Spectral says 256 but beets says 192 → use beets (lower) → requeue."""
        self.assertEqual(
            quality_gate_decision(192, is_cbr=False, verified_lossless=False,
                                  spectral_bitrate=256),
            "requeue_upgrade")

    # --- requeue_flac cases ---

    def test_cbr_above_threshold_requeues_flac(self):
        self.assertEqual(
            quality_gate_decision(320, is_cbr=True, verified_lossless=False),
            "requeue_flac")

    def test_cbr_256_requeues_flac(self):
        self.assertEqual(
            quality_gate_decision(256, is_cbr=True, verified_lossless=False),
            "requeue_flac")

    # --- edge: CBR below threshold → requeue_upgrade (not flac) ---

    def test_cbr_below_threshold_requeues_upgrade_not_flac(self):
        """CBR 192 → below threshold takes priority over CBR path."""
        self.assertEqual(
            quality_gate_decision(192, is_cbr=True, verified_lossless=False),
            "requeue_upgrade")

    # --- verified_lossless + spectral interaction ---

    def test_verified_lossless_overrides_spectral(self):
        """Verified lossless at 180kbps with spectral_bitrate=150 → still accept.
        verified_lossless forces gate_br to threshold after spectral override."""
        self.assertEqual(
            quality_gate_decision(180, is_cbr=False, verified_lossless=True,
                                  spectral_bitrate=150),
            "accept")


if __name__ == "__main__":
    unittest.main()
