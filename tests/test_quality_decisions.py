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
    is_verified_lossless,
    AudioQualityMeasurement,
    SpectralContext,
    DownloadInfo,
    rejected_download_tier,
    narrow_override_on_downgrade,
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

    # --- fallback to existing_min_bitrate when spectral is None ---

    def test_suspect_falls_back_to_existing_min_bitrate(self):
        """Existing files are genuine (no spectral bitrate) but have container bitrate.
        Should reject 96kbps transcode vs 128kbps genuine existing."""
        self.assertEqual(
            spectral_import_decision("likely_transcode", 96, None,
                                     existing_min_bitrate=128), "reject")

    def test_suspect_upgrade_vs_existing_min_bitrate(self):
        """Suspect 192kbps vs genuine existing 128kbps container → upgrade."""
        self.assertEqual(
            spectral_import_decision("suspect", 192, None,
                                     existing_min_bitrate=128), "import_upgrade")

    def test_fallback_not_used_when_spectral_exists(self):
        """When existing spectral bitrate is available, ignore fallback."""
        self.assertEqual(
            spectral_import_decision("suspect", 192, 128,
                                     existing_min_bitrate=64), "import_upgrade")

    def test_suspect_no_existing_at_all(self):
        """Neither spectral nor container bitrate → truly no existing."""
        self.assertEqual(
            spectral_import_decision("likely_transcode", 96, None,
                                     existing_min_bitrate=None), "import_no_exist")


# ============================================================================
# import_quality_decision
# ============================================================================

class TestImportQualityDecision(unittest.TestCase):
    """Test import decision (FLAC conversion / bitrate comparison path).

    Uses AudioQualityMeasurement objects for new/existing.
    The override concept is gone — callers construct existing with
    the resolved bitrate.
    """

    # --- verified lossless always wins ---

    def test_verified_lossless_always_imports(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=240, verified_lossless=True)
        existing = AudioQualityMeasurement(min_bitrate_kbps=320)
        self.assertEqual(import_quality_decision(new, existing), "import")

    def test_verified_lossless_even_lower_bitrate(self):
        """V0 at 207kbps from genuine FLAC still imports over CBR 320."""
        new = AudioQualityMeasurement(min_bitrate_kbps=207, verified_lossless=True)
        existing = AudioQualityMeasurement(min_bitrate_kbps=320)
        self.assertEqual(import_quality_decision(new, existing), "import")

    def test_verified_lossless_no_existing(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=240, verified_lossless=True)
        self.assertEqual(import_quality_decision(new, None), "import")

    # --- normal upgrade ---

    def test_upgrade_imports(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=256)
        existing = AudioQualityMeasurement(min_bitrate_kbps=192)
        self.assertEqual(import_quality_decision(new, existing), "import")

    def test_equal_bitrate_is_downgrade(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=320)
        existing = AudioQualityMeasurement(min_bitrate_kbps=320)
        self.assertEqual(import_quality_decision(new, existing), "downgrade")

    def test_lower_bitrate_is_downgrade(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=192)
        existing = AudioQualityMeasurement(min_bitrate_kbps=320)
        self.assertEqual(import_quality_decision(new, existing), "downgrade")

    # --- override is now the caller's responsibility ---

    def test_override_replaces_existing(self):
        """Pipeline DB says existing is 128 (spectral), beets says 320.
        Caller constructs existing with override bitrate already resolved."""
        new = AudioQualityMeasurement(min_bitrate_kbps=240)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)  # override applied by caller
        self.assertEqual(import_quality_decision(new, existing), "import")

    def test_override_causes_downgrade(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=100)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)  # override applied by caller
        self.assertEqual(import_quality_decision(new, existing), "downgrade")

    # --- transcode scenarios ---

    def test_transcode_upgrade(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=192)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)
        self.assertEqual(
            import_quality_decision(new, existing, is_transcode=True),
            "transcode_upgrade")

    def test_transcode_downgrade(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=128)
        existing = AudioQualityMeasurement(min_bitrate_kbps=192)
        self.assertEqual(
            import_quality_decision(new, existing, is_transcode=True),
            "transcode_downgrade")

    def test_transcode_equal_is_downgrade(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=128)
        existing = AudioQualityMeasurement(min_bitrate_kbps=128)
        self.assertEqual(
            import_quality_decision(new, existing, is_transcode=True),
            "transcode_downgrade")

    def test_transcode_first_import(self):
        """No existing album — transcode is better than nothing."""
        new = AudioQualityMeasurement(min_bitrate_kbps=150)
        self.assertEqual(
            import_quality_decision(new, None, is_transcode=True),
            "transcode_first")

    # --- first import (no existing) ---

    def test_first_import_no_existing(self):
        new = AudioQualityMeasurement(min_bitrate_kbps=240)
        self.assertEqual(import_quality_decision(new, None), "import")

    def test_first_import_no_bitrates(self):
        new = AudioQualityMeasurement()
        self.assertEqual(import_quality_decision(new, None), "import")


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

    # --- spectral grade override ---

    def test_spectral_genuine_overrides_low_bitrate(self):
        """Lo-fi lossless: genuine spectral + low V0 bitrate = NOT transcode."""
        self.assertFalse(transcode_detection(12, 190, spectral_grade="genuine"))

    def test_spectral_marginal_overrides_low_bitrate(self):
        """Lo-fi lossless: marginal spectral (demos/live) = NOT transcode."""
        self.assertFalse(transcode_detection(12, 190, spectral_grade="marginal"))

    def test_spectral_suspect_is_transcode_even_above_threshold(self):
        """Cliff detected: suspect grade = transcode even at high bitrate."""
        self.assertTrue(transcode_detection(12, 240, spectral_grade="suspect"))

    def test_spectral_likely_transcode_is_transcode(self):
        self.assertTrue(transcode_detection(12, 240, spectral_grade="likely_transcode"))

    def test_no_spectral_falls_back_to_threshold(self):
        """No spectral data — use bitrate threshold (backward compat)."""
        self.assertTrue(transcode_detection(12, 190, spectral_grade=None))
        self.assertFalse(transcode_detection(12, 240, spectral_grade=None))

    def test_spectral_no_conversion_still_false(self):
        """Zero conversions = not transcode regardless of spectral."""
        self.assertFalse(transcode_detection(0, 190, spectral_grade="suspect"))


# ============================================================================
# quality_gate_decision
# ============================================================================

class TestQualityGateDecision(unittest.TestCase):
    """Test post-import quality gate.

    Uses AudioQualityMeasurement to carry all params in one object.
    """

    # --- accept cases ---

    def test_vbr_above_threshold_accepts(self):
        m = AudioQualityMeasurement(min_bitrate_kbps=240, is_cbr=False)
        self.assertEqual(quality_gate_decision(m), "accept")

    def test_vbr_at_threshold_accepts(self):
        m = AudioQualityMeasurement(min_bitrate_kbps=QUALITY_MIN_BITRATE_KBPS, is_cbr=False)
        self.assertEqual(quality_gate_decision(m), "accept")

    def test_verified_lossless_accepts_regardless(self):
        """Verified lossless with low bitrate (quiet music) still accepts."""
        m = AudioQualityMeasurement(min_bitrate_kbps=180, verified_lossless=True)
        self.assertEqual(quality_gate_decision(m), "accept")

    def test_verified_lossless_cbr_accepts(self):
        """verified_lossless + CBR = accept (we verified it)."""
        m = AudioQualityMeasurement(min_bitrate_kbps=320, is_cbr=True, verified_lossless=True)
        self.assertEqual(quality_gate_decision(m), "accept")

    # --- requeue_upgrade cases ---

    def test_below_threshold_requeues_upgrade(self):
        m = AudioQualityMeasurement(min_bitrate_kbps=190)
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")

    def test_way_below_threshold_requeues(self):
        m = AudioQualityMeasurement(min_bitrate_kbps=96)
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")

    def test_spectral_override_requeues(self):
        """Beets says 320 but spectral says 128 → use spectral → requeue."""
        m = AudioQualityMeasurement(min_bitrate_kbps=320, is_cbr=True,
                                    spectral_bitrate_kbps=128)
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")

    def test_spectral_higher_than_bitrate_ignored(self):
        """Spectral says 256 but beets says 192 → use beets (lower) → requeue."""
        m = AudioQualityMeasurement(min_bitrate_kbps=192, spectral_bitrate_kbps=256)
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")

    # --- requeue_lossless cases ---

    def test_cbr_above_threshold_requeues_lossless(self):
        m = AudioQualityMeasurement(min_bitrate_kbps=320, is_cbr=True)
        self.assertEqual(quality_gate_decision(m), "requeue_lossless")

    def test_cbr_256_requeues_lossless(self):
        m = AudioQualityMeasurement(min_bitrate_kbps=256, is_cbr=True)
        self.assertEqual(quality_gate_decision(m), "requeue_lossless")

    # --- edge: CBR below threshold → requeue_upgrade (not flac) ---

    def test_cbr_below_threshold_requeues_upgrade_not_flac(self):
        """CBR 192 → below threshold takes priority over CBR path."""
        m = AudioQualityMeasurement(min_bitrate_kbps=192, is_cbr=True)
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")

    # --- verified_lossless + spectral interaction ---

    def test_verified_lossless_overrides_spectral(self):
        """Verified lossless at 180kbps with spectral_bitrate=150 → still accept.
        verified_lossless forces gate_br to threshold after spectral override."""
        m = AudioQualityMeasurement(min_bitrate_kbps=180, verified_lossless=True,
                                    spectral_bitrate_kbps=150)
        self.assertEqual(quality_gate_decision(m), "accept")

    # --- Opus path ---

    def test_opus_128_verified_lossless_accepts(self):
        """Opus 128kbps from verified lossless is the endgame — always accept."""
        m = AudioQualityMeasurement(min_bitrate_kbps=128, verified_lossless=True)
        self.assertEqual(quality_gate_decision(m), "accept")

    def test_opus_128_not_verified_requeues(self):
        """Opus 128kbps without verified_lossless should requeue (below threshold)."""
        m = AudioQualityMeasurement(min_bitrate_kbps=128)
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")

    # --- None min_bitrate ---

    def test_none_bitrate_requeues(self):
        """No measurable bitrate → requeue for upgrade."""
        m = AudioQualityMeasurement()
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")


# ============================================================================
# is_verified_lossless
# ============================================================================

class TestIsVerifiedLossless(unittest.TestCase):
    """Test verified_lossless derivation."""

    def test_gold_standard(self):
        """Converted FLAC + genuine spectral = verified."""
        self.assertTrue(is_verified_lossless(True, "flac", "genuine"))

    def test_flac_uppercase(self):
        self.assertTrue(is_verified_lossless(True, "FLAC", "genuine"))

    def test_not_converted(self):
        """MP3 download, no conversion — never verified."""
        self.assertFalse(is_verified_lossless(False, None, "genuine"))

    def test_not_flac_source(self):
        """Converted from something other than FLAC — not verified."""
        self.assertFalse(is_verified_lossless(True, "mp3", "genuine"))

    def test_suspect_spectral(self):
        """FLAC converted but spectral says suspect — fake FLAC, not verified."""
        self.assertFalse(is_verified_lossless(True, "flac", "suspect"))

    def test_likely_transcode(self):
        self.assertFalse(is_verified_lossless(True, "flac", "likely_transcode"))

    def test_marginal_spectral(self):
        """Marginal spectral is NOT verified — only genuine counts."""
        self.assertFalse(is_verified_lossless(True, "flac", "marginal"))

    def test_none_spectral(self):
        """No spectral data — can't verify."""
        self.assertFalse(is_verified_lossless(True, "flac", None))

    def test_none_filetype(self):
        self.assertFalse(is_verified_lossless(True, None, "genuine"))

    def test_all_none(self):
        self.assertFalse(is_verified_lossless(False, None, None))

    def test_alac_m4a_verified(self):
        """Converted ALAC (m4a) + genuine spectral = verified lossless."""
        self.assertTrue(is_verified_lossless(True, "m4a", "genuine"))

    def test_wav_verified(self):
        """Converted WAV + genuine spectral = verified lossless."""
        self.assertTrue(is_verified_lossless(True, "wav", "genuine"))

    def test_alac_suspect_not_verified(self):
        """ALAC converted but spectral suspect — not verified."""
        self.assertFalse(is_verified_lossless(True, "m4a", "suspect"))


# ============================================================================
# SpectralContext
# ============================================================================

class TestSpectralContext(unittest.TestCase):
    """Test SpectralContext dataclass."""

    def test_defaults(self):
        ctx = SpectralContext()
        self.assertFalse(ctx.needs_check)
        self.assertIsNone(ctx.grade)
        self.assertIsNone(ctx.bitrate)
        self.assertEqual(ctx.suspect_pct, 0.0)
        self.assertIsNone(ctx.existing_min_bitrate)
        self.assertIsNone(ctx.existing_spectral_bitrate)

    def test_full_construction(self):
        ctx = SpectralContext(
            needs_check=True,
            grade="suspect",
            bitrate=128,
            suspect_pct=75.0,
            existing_min_bitrate=320,
            existing_spectral_bitrate=160,
        )
        self.assertTrue(ctx.needs_check)
        self.assertEqual(ctx.grade, "suspect")
        self.assertEqual(ctx.bitrate, 128)

    def test_feeds_spectral_import_decision(self):
        """SpectralContext fields map directly to spectral_import_decision args."""
        ctx = SpectralContext(
            grade="suspect", bitrate=192,
            existing_spectral_bitrate=128)
        result = spectral_import_decision(
            ctx.grade, ctx.bitrate, ctx.existing_spectral_bitrate or 0,
            existing_min_bitrate=ctx.existing_min_bitrate)
        self.assertEqual(result, "import_upgrade")

    def test_no_check_needed(self):
        """VBR MP3 — no spectral check needed."""
        ctx = SpectralContext(needs_check=False)
        self.assertFalse(ctx.needs_check)
        self.assertIsNone(ctx.grade)


# ============================================================================
# full_pipeline_decision contract tests
# ============================================================================
# These lock the interface between full_pipeline_decision() and the web UI
# simulator. If a stage is added/removed or the result shape changes, these
# fail — forcing the simulator to be updated in sync.

from lib.quality import full_pipeline_decision, get_decision_tree
import inspect

# The exact keys the simulator reads from the result dict
EXPECTED_RESULT_KEYS = {
    "stage1_spectral", "stage2_import", "stage3_quality_gate",
    "final_status", "imported", "denylisted", "keep_searching",
    "target_final_format",
}

# Valid values for each stage (None means stage was skipped)
VALID_STAGE1 = {None, "import", "import_upgrade", "import_no_exist", "reject"}
VALID_STAGE2 = {None, "import", "downgrade", "transcode_upgrade",
                "transcode_downgrade", "transcode_first",
                "preflight_existing"}
VALID_STAGE3 = {None, "accept", "requeue_upgrade", "requeue_lossless"}
VALID_FINAL_STATUS = {None, "imported", "wanted"}

# The exact parameter names the simulator form submits
EXPECTED_PARAMS = {
    "is_flac", "min_bitrate", "is_cbr",
    "spectral_grade", "spectral_bitrate",
    "existing_min_bitrate", "existing_spectral_bitrate",
    "override_min_bitrate",
    "post_conversion_min_bitrate", "converted_count",
    "verified_lossless", "verified_lossless_target",
    "target_format",
}


class TestFullPipelineContract(unittest.TestCase):
    """Contract tests for full_pipeline_decision() — the web simulator depends
    on these exact keys, values, and parameter names."""

    def test_result_keys_match_contract(self):
        """Result dict must have exactly the keys the simulator expects."""
        r = full_pipeline_decision(is_flac=False, min_bitrate=256, is_cbr=False)
        self.assertEqual(set(r.keys()), EXPECTED_RESULT_KEYS)

    def test_parameter_names_match_contract(self):
        """Function signature must accept exactly the params the simulator sends."""
        sig = inspect.signature(full_pipeline_decision)
        actual_params = set(sig.parameters.keys())
        self.assertEqual(actual_params, EXPECTED_PARAMS)

    def test_stage1_values_in_contract(self):
        """Stage 1 spectral decisions must be from the known set."""
        # Run several representative cases
        cases = [
            dict(is_flac=False, min_bitrate=320, is_cbr=True,
                 spectral_grade="suspect", spectral_bitrate=160,
                 existing_spectral_bitrate=160),
            dict(is_flac=False, min_bitrate=320, is_cbr=True,
                 spectral_grade="genuine"),
            dict(is_flac=False, min_bitrate=256, is_cbr=False),
            dict(is_flac=False, min_bitrate=320, is_cbr=True,
                 spectral_grade="suspect", spectral_bitrate=200,
                 existing_spectral_bitrate=128),
        ]
        for kwargs in cases:
            r = full_pipeline_decision(**kwargs)
            self.assertIn(r["stage1_spectral"], VALID_STAGE1,
                          f"Unexpected stage1 value: {r['stage1_spectral']} for {kwargs}")

    def test_stage2_values_in_contract(self):
        """Stage 2 import decisions must be from the known set."""
        cases = [
            dict(is_flac=True, min_bitrate=0, is_cbr=False,
                 spectral_grade="genuine", converted_count=10,
                 post_conversion_min_bitrate=245),
            dict(is_flac=True, min_bitrate=0, is_cbr=False,
                 spectral_grade="genuine", converted_count=10,
                 post_conversion_min_bitrate=190),
            dict(is_flac=True, min_bitrate=0, is_cbr=False,
                 spectral_grade="genuine", converted_count=10,
                 post_conversion_min_bitrate=245, existing_min_bitrate=300),
            dict(is_flac=False, min_bitrate=256, is_cbr=False),
            dict(is_flac=False, min_bitrate=128, is_cbr=False,
                 existing_min_bitrate=256),
        ]
        for kwargs in cases:
            r = full_pipeline_decision(**kwargs)
            self.assertIn(r["stage2_import"], VALID_STAGE2,
                          f"Unexpected stage2 value: {r['stage2_import']} for {kwargs}")

    def test_stage3_values_in_contract(self):
        """Stage 3 quality gate decisions must be from the known set."""
        cases = [
            dict(is_flac=True, min_bitrate=0, is_cbr=False,
                 spectral_grade="genuine", converted_count=10,
                 post_conversion_min_bitrate=245),
            dict(is_flac=False, min_bitrate=320, is_cbr=True),
            dict(is_flac=False, min_bitrate=256, is_cbr=False),
            dict(is_flac=False, min_bitrate=180, is_cbr=False),
        ]
        for kwargs in cases:
            r = full_pipeline_decision(**kwargs)
            self.assertIn(r["stage3_quality_gate"], VALID_STAGE3,
                          f"Unexpected stage3 value: {r['stage3_quality_gate']} for {kwargs}")

    def test_final_status_values_in_contract(self):
        """final_status must be from the known set."""
        r1 = full_pipeline_decision(is_flac=False, min_bitrate=256, is_cbr=False)
        self.assertIn(r1["final_status"], VALID_FINAL_STATUS)
        r2 = full_pipeline_decision(is_flac=False, min_bitrate=128, is_cbr=False,
                                    existing_min_bitrate=256)
        self.assertIn(r2["final_status"], VALID_FINAL_STATUS)
        r3 = full_pipeline_decision(is_flac=False, min_bitrate=320, is_cbr=True)
        self.assertIn(r3["final_status"], VALID_FINAL_STATUS)

    def test_boolean_fields_are_bool(self):
        """imported, denylisted, keep_searching must be booleans."""
        r = full_pipeline_decision(is_flac=False, min_bitrate=256, is_cbr=False)
        for key in ("imported", "denylisted", "keep_searching"):
            self.assertIsInstance(r[key], bool, f"{key} should be bool")

    def test_decision_tree_stage_ids(self):
        """Decision tree must have the expected stages in order."""
        tree = get_decision_tree()
        ids = [s["id"] for s in tree["stages"]]
        self.assertEqual(ids, ["flac_spectral", "flac_convert", "transcode",
                               "verified_lossless", "target_conversion",
                               "mp3_spectral", "mp3_vbr_note",
                               "import_decision", "quality_gate", "dispatch"])

    def test_decision_tree_outcomes_match_valid_values(self):
        """Outcomes declared in the tree must match what the contract allows."""
        tree = get_decision_tree()
        stage_map = {s["id"]: s for s in tree["stages"]}
        # mp3_spectral stage outcomes must be subset of VALID_STAGE1
        spectral_outcomes = set(stage_map["mp3_spectral"]["outcomes"])
        self.assertTrue(spectral_outcomes <= (VALID_STAGE1 - {None}),
                        f"Tree spectral outcomes {spectral_outcomes} not in {VALID_STAGE1}")
        # import_decision outcomes must be subset of VALID_STAGE2
        import_outcomes = set(stage_map["import_decision"]["outcomes"])
        self.assertTrue(import_outcomes <= (VALID_STAGE2 - {None}),
                        f"Tree import outcomes {import_outcomes} not in {VALID_STAGE2}")
        # quality_gate outcomes must be subset of VALID_STAGE3
        gate_outcomes = set(stage_map["quality_gate"]["outcomes"])
        self.assertTrue(gate_outcomes <= (VALID_STAGE3 - {None}),
                        f"Tree gate outcomes {gate_outcomes} not in {VALID_STAGE3}")

    def test_decision_tree_constants_match_code(self):
        """Tree constants must match the actual module constants."""
        tree = get_decision_tree()
        consts = tree["constants"]
        self.assertEqual(consts["QUALITY_MIN_BITRATE_KBPS"],
                         QUALITY_MIN_BITRATE_KBPS)
        self.assertEqual(consts["TRANSCODE_MIN_BITRATE_KBPS"],
                         TRANSCODE_MIN_BITRATE_KBPS)

    def test_decision_tree_every_stage_has_rules(self):
        """Every stage must have at least one rule."""
        tree = get_decision_tree()
        for stage in tree["stages"]:
            self.assertTrue(len(stage["rules"]) > 0,
                            f"Stage {stage['id']} has no rules")

    def test_decision_tree_every_stage_has_path(self):
        """Every stage must declare a path for the branching diagram."""
        tree = get_decision_tree()
        valid_paths = set(tree["paths"]) | {"shared"}
        for stage in tree["stages"]:
            self.assertIn(stage.get("path"), valid_paths,
                          f"Stage {stage['id']} has invalid path")

    def test_target_conversion_genuine_flac(self):
        """Genuine FLAC + verified_lossless_target → target format, accepted."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine", converted_count=10,
            post_conversion_min_bitrate=245,
            verified_lossless_target="opus 128")
        self.assertEqual(r["target_final_format"], "opus 128")
        self.assertTrue(r["imported"])
        self.assertEqual(r["stage3_quality_gate"], "accept")

    def test_target_conversion_disabled(self):
        """Genuine FLAC without verified_lossless_target → keep V0."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine", converted_count=10,
            post_conversion_min_bitrate=245, verified_lossless_target=None)
        self.assertIsNone(r["target_final_format"])
        self.assertTrue(r["imported"])

    def test_target_conversion_transcode_skips(self):
        """Transcode FLAC + verified_lossless_target → no target conversion."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect", converted_count=10,
            post_conversion_min_bitrate=190,
            verified_lossless_target="aac 128")
        self.assertIsNone(r["target_final_format"])

    def test_target_conversion_mp3_skips(self):
        """MP3 path + verified_lossless_target → no target conversion."""
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=245, is_cbr=False,
            verified_lossless_target="mp3 v2")
        self.assertIsNone(r["target_final_format"])


# ============================================================================
# full_pipeline_decision with target_format
# ============================================================================

class TestFullPipelineTargetFormat(unittest.TestCase):
    """Test target_format="flac" path: skip conversion, keep FLAC on disk."""

    def test_flac_target_format_skips_conversion_and_imports(self):
        """target_format=flac + genuine FLAC → imported without conversion."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=900, is_cbr=False,
            spectral_grade="genuine",
            converted_count=0,  # no conversion happened
            target_format="flac")
        self.assertTrue(r["imported"])
        self.assertEqual(r["final_status"], "imported")
        self.assertEqual(r["stage3_quality_gate"], "accept")
        self.assertFalse(r["keep_searching"])

    def test_flac_target_format_verified_lossless(self):
        """target_format=flac + genuine FLAC → verified_lossless despite no conversion."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=900, is_cbr=False,
            spectral_grade="genuine",
            converted_count=0,
            target_format="flac")
        # Quality gate should see verified_lossless=True
        self.assertEqual(r["stage3_quality_gate"], "accept")

    def test_flac_target_format_mp3_download_unchanged(self):
        """target_format=flac but MP3 download → normal MP3 path (no effect)."""
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=240, is_cbr=False,
            target_format="flac")
        self.assertTrue(r["imported"])
        self.assertEqual(r["stage2_import"], "import")

    def test_flac_target_beats_existing_v0(self):
        """FLAC at 900kbps vs existing V0 at 245kbps → upgrade."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=900, is_cbr=False,
            spectral_grade="genuine",
            converted_count=0,
            existing_min_bitrate=245,
            target_format="flac")
        self.assertTrue(r["imported"])
        self.assertEqual(r["stage2_import"], "import")


# ============================================================================
# compute_effective_override_bitrate
# ============================================================================

class TestComputeEffectiveOverrideBitrate(unittest.TestCase):
    """Test the spectral/container override computation (pure)."""

    def _compute(self, container, spectral):
        from lib.quality import compute_effective_override_bitrate
        return compute_effective_override_bitrate(container, spectral)

    def test_spectral_lower_wins(self):
        self.assertEqual(self._compute(320, 128), 128)

    def test_container_lower_wins(self):
        self.assertEqual(self._compute(192, 256), 192)

    def test_no_spectral_returns_container(self):
        self.assertEqual(self._compute(320, None), 320)

    def test_no_container_no_spectral(self):
        self.assertIsNone(self._compute(None, None))

    def test_no_container_with_spectral(self):
        self.assertEqual(self._compute(None, 128), 128)


# ============================================================================
# dispatch_action
# ============================================================================

class TestDispatchAction(unittest.TestCase):
    """Test dispatch_action: map decision string to action flags."""

    def _action(self, decision):
        from lib.quality import dispatch_action
        return dispatch_action(decision)

    def test_import_action(self):
        a = self._action("import")
        self.assertTrue(a.mark_done)
        self.assertFalse(a.mark_failed)
        self.assertFalse(a.denylist)
        self.assertFalse(a.requeue)
        self.assertTrue(a.cleanup)
        self.assertTrue(a.trigger_meelo)
        self.assertTrue(a.run_quality_gate)

    def test_preflight_existing_action(self):
        a = self._action("preflight_existing")
        self.assertTrue(a.mark_done)
        self.assertTrue(a.trigger_meelo)
        self.assertTrue(a.run_quality_gate)

    def test_downgrade_action(self):
        a = self._action("downgrade")
        self.assertFalse(a.mark_done)
        self.assertTrue(a.mark_failed)
        self.assertTrue(a.denylist)
        self.assertFalse(a.requeue)
        self.assertTrue(a.cleanup)

    def test_transcode_upgrade_action(self):
        a = self._action("transcode_upgrade")
        self.assertTrue(a.mark_done)
        self.assertTrue(a.denylist)
        self.assertTrue(a.requeue)
        self.assertTrue(a.trigger_meelo)

    def test_transcode_downgrade_action(self):
        a = self._action("transcode_downgrade")
        self.assertFalse(a.mark_done)
        self.assertTrue(a.mark_failed)
        self.assertTrue(a.denylist)
        self.assertTrue(a.requeue)

    def test_transcode_first_action(self):
        a = self._action("transcode_first")
        self.assertTrue(a.mark_done)
        self.assertTrue(a.denylist)
        self.assertTrue(a.requeue)
        self.assertTrue(a.trigger_meelo)

    def test_unknown_decision_marks_failed(self):
        a = self._action("conversion_failed")
        self.assertTrue(a.mark_failed)
        self.assertFalse(a.denylist)

    def test_import_failed_action(self):
        a = self._action("import_failed")
        self.assertTrue(a.mark_failed)

    def test_target_conversion_failed_marks_failed(self):
        """Target conversion failure falls into catch-all → mark_failed."""
        a = self._action("target_conversion_failed")
        self.assertTrue(a.mark_failed)
        self.assertFalse(a.denylist)


# ============================================================================
# extract_usernames
# ============================================================================

class TestExtractUsernames(unittest.TestCase):
    """Test username extraction from file objects."""

    def _extract(self, files):
        from lib.quality import extract_usernames
        return extract_usernames(files)

    def _file(self, username):
        """Create a minimal file-like object with a username attribute."""
        from unittest.mock import MagicMock
        f = MagicMock()
        f.username = username
        return f

    def test_single_user(self):
        files = [self._file("alice"), self._file("alice")]
        self.assertEqual(self._extract(files), {"alice"})

    def test_multiple_users(self):
        files = [self._file("alice"), self._file("bob")]
        self.assertEqual(self._extract(files), {"alice", "bob"})

    def test_empty_username_excluded(self):
        files = [self._file(""), self._file("alice")]
        self.assertEqual(self._extract(files), {"alice"})

    def test_none_username_excluded(self):
        files = [self._file(None), self._file("alice")]
        self.assertEqual(self._extract(files), {"alice"})

    def test_empty_files(self):
        self.assertEqual(self._extract([]), set())


# ============================================================================
# dispatch_action contract test
# ============================================================================

class TestDispatchActionContract(unittest.TestCase):
    """Verify dispatch_action covers all import_decision outcomes."""

    def test_covers_import_decision_outcomes(self):
        from lib.quality import dispatch_action, get_decision_tree
        tree = get_decision_tree()
        import_stage = [s for s in tree["stages"] if s["id"] == "import_decision"][0]
        for outcome in import_stage["outcomes"]:
            a = dispatch_action(outcome)
            self.assertTrue(a.mark_done or a.mark_failed,
                            f"dispatch_action('{outcome}') must set mark_done or mark_failed")


# ============================================================================
# rejected_download_tier + narrow_override_on_downgrade
# ============================================================================

class TestRejectedDownloadTier(unittest.TestCase):
    """Test mapping from DownloadInfo to quality_override tier string."""

    def test_cbr_320_bps(self):
        """CBR 320 (bitrate in bps after import_one) → 'mp3 320'."""
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        self.assertEqual(rejected_download_tier(dl), "mp3 320")

    def test_cbr_320_kbps(self):
        """CBR 320 (bitrate in kbps from slskd) → 'mp3 320'."""
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320)
        self.assertEqual(rejected_download_tier(dl), "mp3 320")

    def test_cbr_256(self):
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=256000)
        self.assertEqual(rejected_download_tier(dl), "mp3 256")

    def test_vbr_mp3(self):
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=True, bitrate=245000)
        self.assertEqual(rejected_download_tier(dl), "mp3 v0")

    def test_flac(self):
        dl = DownloadInfo(slskd_filetype="flac", is_vbr=False, bitrate=1411000)
        self.assertEqual(rejected_download_tier(dl), "lossless")

    def test_converted_flac(self):
        """FLAC converted to V0 — tier is 'lossless' (the source format)."""
        dl = DownloadInfo(slskd_filetype="flac", was_converted=True,
                          is_vbr=True, bitrate=245000)
        self.assertEqual(rejected_download_tier(dl), "lossless")

    def test_empty_dl_info(self):
        dl = DownloadInfo()
        self.assertIsNone(rejected_download_tier(dl))

    def test_mp3_no_bitrate(self):
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=None)
        self.assertIsNone(rejected_download_tier(dl))


class TestNarrowOverrideOnDowngrade(unittest.TestCase):
    """Test narrowing quality_override after downgrade rejection."""

    def test_removes_320_from_upgrade_tiers(self):
        """Standard case: 'lossless,mp3 v0,mp3 320' + 320 → 'lossless,mp3 v0'."""
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        result = narrow_override_on_downgrade("lossless,mp3 v0,mp3 320", dl)
        self.assertEqual(result, "lossless,mp3 v0")

    def test_removes_lossless_from_override(self):
        dl = DownloadInfo(slskd_filetype="flac", is_vbr=False)
        result = narrow_override_on_downgrade("lossless,mp3 v0", dl)
        self.assertEqual(result, "mp3 v0")

    def test_removes_v0_from_override(self):
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=True, bitrate=245000)
        result = narrow_override_on_downgrade("lossless,mp3 v0,mp3 320", dl)
        self.assertEqual(result, "lossless,mp3 320")

    def test_no_change_when_tier_not_in_override(self):
        """320 download but override is 'lossless' only → no change."""
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        result = narrow_override_on_downgrade("lossless", dl)
        self.assertIsNone(result)

    def test_no_change_when_no_override(self):
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        result = narrow_override_on_downgrade(None, dl)
        self.assertIsNone(result)

    def test_wont_remove_last_tier(self):
        """'mp3 320' + 320 → None (don't narrow to empty)."""
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        result = narrow_override_on_downgrade("mp3 320", dl)
        self.assertIsNone(result)

    def test_handles_whitespace_in_override(self):
        dl = DownloadInfo(slskd_filetype="mp3", is_vbr=False, bitrate=320000)
        result = narrow_override_on_downgrade("lossless, mp3 v0, mp3 320", dl)
        self.assertEqual(result, "lossless,mp3 v0")


class TestRejectionBackfillOverride(unittest.TestCase):
    """Tests for rejection_backfill_override — breaks CBR 320 download loops.

    Two rules:
    - CBR above threshold: ALWAYS flac (CBR is unverifiable, spectral irrelevant)
    - VBR above threshold: flac ONLY when spectral is genuine (need to trust quality)
    """

    # --- Genuine spectral: flac for both CBR and VBR ---

    def test_cbr_320_genuine_returns_flac(self):
        from lib.quality import rejection_backfill_override, QUALITY_FLAC_ONLY
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_cbr_256_genuine_returns_flac(self):
        from lib.quality import rejection_backfill_override, QUALITY_FLAC_ONLY
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=256,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_vbr_240_genuine_returns_flac(self):
        from lib.quality import rejection_backfill_override, QUALITY_FLAC_ONLY
        result = rejection_backfill_override(
            is_cbr=False, min_bitrate_kbps=240,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_vbr_at_threshold_genuine_returns_flac(self):
        from lib.quality import rejection_backfill_override, QUALITY_FLAC_ONLY
        result = rejection_backfill_override(
            is_cbr=False, min_bitrate_kbps=210,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    # --- Not genuine: never backfill (spectral is the whole point) ---

    def test_cbr_320_suspect_returns_none(self):
        """Suspect 320: keep searching all tiers, might find genuine source."""
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="suspect", verified_lossless=False)
        self.assertIsNone(result)

    def test_cbr_320_marginal_returns_none(self):
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="marginal", verified_lossless=False)
        self.assertIsNone(result)

    def test_cbr_320_no_spectral_returns_none(self):
        """No spectral data: can't make the decision, keep all tiers."""
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade=None, verified_lossless=False)
        self.assertIsNone(result)

    def test_vbr_suspect_returns_none(self):
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=False, min_bitrate_kbps=240,
            spectral_grade="suspect", verified_lossless=False)
        self.assertIsNone(result)

    def test_vbr_no_spectral_returns_none(self):
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=False, min_bitrate_kbps=240,
            spectral_grade=None, verified_lossless=False)
        self.assertIsNone(result)

    # --- Below threshold: never backfill ---

    def test_vbr_below_threshold_returns_none(self):
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=False, min_bitrate_kbps=200,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    def test_cbr_192_below_threshold_returns_none(self):
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=192,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    # --- Guards ---

    def test_verified_lossless_returns_none(self):
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=True)
        self.assertIsNone(result)

    def test_none_bitrate_returns_none(self):
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=None,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    # --- Named scenarios ---

    def test_stars_of_the_lid_scenario(self):
        """Stars of the Lid: CBR 320 genuine on disk. Backfill fires."""
        from lib.quality import rejection_backfill_override, QUALITY_FLAC_ONLY
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_upgrade_button_no_spectral_scenario(self):
        """CBR 320, no spectral on disk. Backfill does NOT fire yet —
        needs spectral propagation from download first."""
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade=None, verified_lossless=False)
        self.assertIsNone(result)

    def test_upgrade_button_after_genuine_download(self):
        """CBR 320, spectral propagated from genuine download. NOW backfill fires."""
        from lib.quality import rejection_backfill_override, QUALITY_FLAC_ONLY
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_upgrade_button_after_suspect_download(self):
        """CBR 320, spectral propagated from suspect download. Backfill does NOT
        fire — keep searching all tiers, might find genuine source."""
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="suspect", verified_lossless=False)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
