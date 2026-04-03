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
    SpectralContext,
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
}

# Valid values for each stage (None means stage was skipped)
VALID_STAGE1 = {None, "import", "import_upgrade", "import_no_exist", "reject"}
VALID_STAGE2 = {None, "import", "downgrade", "transcode_upgrade",
                "transcode_downgrade", "transcode_first",
                "preflight_existing"}
VALID_STAGE3 = {None, "accept", "requeue_upgrade", "requeue_flac"}
VALID_FINAL_STATUS = {None, "imported", "wanted"}

# The exact parameter names the simulator form submits
EXPECTED_PARAMS = {
    "is_flac", "min_bitrate", "is_cbr",
    "spectral_grade", "spectral_bitrate",
    "existing_min_bitrate", "existing_spectral_bitrate",
    "override_min_bitrate",
    "post_conversion_min_bitrate", "converted_count",
    "verified_lossless",
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
                               "verified_lossless", "mp3_spectral",
                               "mp3_vbr_note", "import_decision",
                               "quality_gate", "dispatch"])

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


if __name__ == "__main__":
    unittest.main()
