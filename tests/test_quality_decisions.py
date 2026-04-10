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

    CASES = [
        # desc, grade, bitrate, existing_spectral, existing_min, expected
        ("genuine imports", "genuine", None, None, None, "import"),
        ("genuine ignores bitrates", "genuine", 128, 256, None, "import"),
        ("marginal imports", "marginal", 192, 256, None, "import"),
        ("marginal no bitrates", "marginal", None, None, None, "import"),
        ("suspect equal rejects", "suspect", 128, 128, None, "reject"),
        ("suspect worse rejects", "suspect", 96, 128, None, "reject"),
        ("likely transcode equal rejects", "likely_transcode", 160, 160, None, "reject"),
        ("suspect better upgrades", "suspect", 192, 128, None, "import_upgrade"),
        ("likely transcode better upgrades", "likely_transcode", 192, 96, None, "import_upgrade"),
        ("suspect no existing zero", "suspect", 128, 0, None, "import_no_exist"),
        ("suspect no existing none", "suspect", 128, None, None, "import_no_exist"),
        ("likely transcode no existing", "likely_transcode", 96, None, None, "import_no_exist"),
        ("suspect no new no existing", "suspect", None, None, None, "import_no_exist"),
        ("suspect no new with existing", "suspect", None, 128, None, "import"),
        ("fallback rejects", "likely_transcode", 96, None, 128, "reject"),
        ("fallback upgrades", "suspect", 192, None, 128, "import_upgrade"),
        ("spectral beats fallback", "suspect", 192, 128, 64, "import_upgrade"),
        ("no spectral or container existing", "likely_transcode", 96, None, None, "import_no_exist"),
    ]

    def test_spectral_import_decisions(self):
        for desc, grade, bitrate, existing_spectral, existing_min, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    spectral_import_decision(
                        grade,
                        bitrate,
                        existing_spectral,
                        existing_min_bitrate=existing_min,
                    ),
                    expected,
                )


# ============================================================================
# import_quality_decision
# ============================================================================

class TestImportQualityDecision(unittest.TestCase):
    """Test import decision (FLAC conversion / bitrate comparison path).

    Uses AudioQualityMeasurement objects for new/existing.
    The override concept is gone — callers construct existing with
    the resolved bitrate.
    """

    CASES = [
        # desc, new_kwargs, existing_kwargs, is_transcode, expected
        ("verified lossless wins", dict(min_bitrate_kbps=240, verified_lossless=True), dict(min_bitrate_kbps=320), False, "import"),
        ("verified lossless lower bitrate wins", dict(min_bitrate_kbps=207, verified_lossless=True), dict(min_bitrate_kbps=320), False, "import"),
        ("verified lossless no existing", dict(min_bitrate_kbps=240, verified_lossless=True), None, False, "import"),
        ("normal upgrade", dict(min_bitrate_kbps=256), dict(min_bitrate_kbps=192), False, "import"),
        ("equal bitrate downgrade", dict(min_bitrate_kbps=320), dict(min_bitrate_kbps=320), False, "downgrade"),
        ("lower bitrate downgrade", dict(min_bitrate_kbps=192), dict(min_bitrate_kbps=320), False, "downgrade"),
        ("override already resolved as upgrade", dict(min_bitrate_kbps=240), dict(min_bitrate_kbps=128), False, "import"),
        ("override already resolved as downgrade", dict(min_bitrate_kbps=100), dict(min_bitrate_kbps=128), False, "downgrade"),
        ("transcode upgrade", dict(min_bitrate_kbps=192), dict(min_bitrate_kbps=128), True, "transcode_upgrade"),
        ("transcode downgrade", dict(min_bitrate_kbps=128), dict(min_bitrate_kbps=192), True, "transcode_downgrade"),
        ("transcode equal downgrade", dict(min_bitrate_kbps=128), dict(min_bitrate_kbps=128), True, "transcode_downgrade"),
        ("transcode first import", dict(min_bitrate_kbps=150), None, True, "transcode_first"),
        ("first import", dict(min_bitrate_kbps=240), None, False, "import"),
        ("first import no bitrates", dict(), None, False, "import"),
    ]

    def test_import_quality_decisions(self):
        for desc, new_kwargs, existing_kwargs, is_transcode, expected in self.CASES:
            with self.subTest(desc=desc):
                new = AudioQualityMeasurement(**new_kwargs)
                existing = (
                    AudioQualityMeasurement(**existing_kwargs)
                    if existing_kwargs is not None
                    else None
                )
                self.assertEqual(
                    import_quality_decision(
                        new,
                        existing,
                        is_transcode=is_transcode,
                    ),
                    expected,
                )


# ============================================================================
# transcode_detection
# ============================================================================

class TestTranscodeDetection(unittest.TestCase):
    """Test post-conversion transcode detection."""

    CASES = [
        # desc, converted_count, min_bitrate, spectral_grade, expected
        ("no conversion", 0, 150, None, False),
        ("none bitrate", 5, None, None, False),
        ("above threshold", 10, 240, None, False),
        ("at threshold", 10, TRANSCODE_MIN_BITRATE_KBPS, None, False),
        ("below threshold", 10, 190, None, True),
        ("way below threshold", 1, 96, None, True),
        ("just below threshold", 5, TRANSCODE_MIN_BITRATE_KBPS - 1, None, True),
        ("genuine overrides low bitrate", 12, 190, "genuine", False),
        ("marginal overrides low bitrate", 12, 190, "marginal", False),
        ("suspect overrides high bitrate", 12, 240, "suspect", True),
        ("likely transcode overrides high bitrate", 12, 240, "likely_transcode", True),
        ("no spectral low bitrate fallback", 12, 190, None, True),
        ("no spectral high bitrate fallback", 12, 240, None, False),
        ("no conversion beats spectral", 0, 190, "suspect", False),
    ]

    def test_transcode_detection_cases(self):
        for desc, converted_count, min_bitrate, spectral_grade, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    transcode_detection(
                        converted_count,
                        min_bitrate,
                        spectral_grade=spectral_grade,
                    ),
                    expected,
                )


# ============================================================================
# quality_gate_decision
# ============================================================================

class TestQualityGateDecision(unittest.TestCase):
    """Test post-import quality gate via subTest table."""

    CASES = [
        # (description, measurement_kwargs, expected_decision)
        # --- accept ---
        ("VBR above threshold", dict(min_bitrate_kbps=240, is_cbr=False), "accept"),
        ("VBR at threshold", dict(min_bitrate_kbps=QUALITY_MIN_BITRATE_KBPS, is_cbr=False), "accept"),
        ("verified lossless low bitrate", dict(min_bitrate_kbps=180, verified_lossless=True), "accept"),
        ("verified lossless CBR", dict(min_bitrate_kbps=320, is_cbr=True, verified_lossless=True), "accept"),
        ("verified lossless overrides spectral", dict(min_bitrate_kbps=180, verified_lossless=True, spectral_bitrate_kbps=150), "accept"),
        ("opus 128 verified lossless", dict(min_bitrate_kbps=128, verified_lossless=True), "accept"),
        # --- requeue_upgrade ---
        ("below threshold", dict(min_bitrate_kbps=190), "requeue_upgrade"),
        ("way below threshold", dict(min_bitrate_kbps=96), "requeue_upgrade"),
        ("spectral override CBR", dict(min_bitrate_kbps=320, is_cbr=True, spectral_bitrate_kbps=128), "requeue_upgrade"),
        ("spectral higher ignored", dict(min_bitrate_kbps=192, spectral_bitrate_kbps=256), "requeue_upgrade"),
        ("CBR below threshold", dict(min_bitrate_kbps=192, is_cbr=True), "requeue_upgrade"),
        ("opus 128 not verified", dict(min_bitrate_kbps=128), "requeue_upgrade"),
        ("none bitrate", dict(), "requeue_upgrade"),
        # --- requeue_lossless ---
        ("CBR above threshold", dict(min_bitrate_kbps=320, is_cbr=True), "requeue_lossless"),
        ("CBR 256", dict(min_bitrate_kbps=256, is_cbr=True), "requeue_lossless"),
    ]

    def test_quality_gate_decisions(self):
        for desc, kwargs, expected in self.CASES:
            with self.subTest(desc=desc):
                m = AudioQualityMeasurement(**kwargs)
                self.assertEqual(quality_gate_decision(m), expected)


# ============================================================================
# is_verified_lossless
# ============================================================================

class TestIsVerifiedLossless(unittest.TestCase):
    """Test verified_lossless derivation."""

    CASES = [
        ("gold standard", True, "flac", "genuine", True),
        ("uppercase flac", True, "FLAC", "genuine", True),
        ("not converted", False, None, "genuine", False),
        ("not lossless source", True, "mp3", "genuine", False),
        ("suspect spectral", True, "flac", "suspect", False),
        ("likely transcode", True, "flac", "likely_transcode", False),
        ("marginal spectral", True, "flac", "marginal", False),
        ("none spectral", True, "flac", None, False),
        ("none filetype", True, None, "genuine", False),
        ("all none", False, None, None, False),
        ("alac m4a verified", True, "m4a", "genuine", True),
        ("wav verified", True, "wav", "genuine", True),
        ("alac suspect not verified", True, "m4a", "suspect", False),
    ]

    def test_verified_lossless_cases(self):
        for desc, was_converted, original_filetype, spectral_grade, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    is_verified_lossless(
                        was_converted,
                        original_filetype,
                        spectral_grade,
                    ),
                    expected,
                )


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

    def test_stage3_grade_aware_spectral_gate(self):
        """Full simulator must match production's grade-aware quality gate."""
        cases = [
            ("genuine ignores low spectral", "genuine", 160, "accept", "imported"),
            ("marginal ignores low spectral", "marginal", 160, "accept", "imported"),
            ("likely_transcode uses low spectral", "likely_transcode", 160,
             "requeue_upgrade", "wanted"),
            ("suspect uses low spectral", "suspect", 160, "requeue_upgrade", "wanted"),
        ]
        for desc, grade, spectral_br, expected_gate, expected_status in cases:
            with self.subTest(desc=desc):
                r = full_pipeline_decision(
                    is_flac=False,
                    min_bitrate=226,
                    is_cbr=False,
                    spectral_grade=grade,
                    spectral_bitrate=spectral_br,
                )
                self.assertEqual(r["stage3_quality_gate"], expected_gate)
                self.assertEqual(r["final_status"], expected_status)

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
    """Grade-aware spectral/container override computation (pure).

    Spectral bitrate only participates when grade is in SPECTRAL_TRANSCODE_GRADES
    (suspect / likely_transcode). For genuine/marginal/error/None/unknown grades
    the helper must return the container bitrate untouched — a genuine file with
    a low spectral cliff estimate must not drag the comparison bitrate down.
    """

    # (description, container, spectral, grade, expected)
    CASES = [
        ("spectral ignored when grade None",             320, 128, None,               320),
        ("spectral ignored when grade genuine",          320, 128, "genuine",          320),
        ("spectral ignored when grade marginal",         320, 128, "marginal",         320),
        ("spectral ignored when grade error",            320, 128, "error",            320),
        ("unknown grade treated as non-transcode",       320, 128, "weird_new_grade",  320),
        ("spectral lower wins when suspect",             320, 128, "suspect",          128),
        ("spectral lower wins when likely_transcode",    320, 128, "likely_transcode", 128),
        ("container lower wins when suspect",            192, 256, "suspect",          192),
        ("container lower wins when likely_transcode",   192, 256, "likely_transcode", 192),
        ("equal values when suspect",                    200, 200, "suspect",          200),
        ("no spectral returns container (genuine)",      320, None, "genuine",         320),
        ("no spectral returns container (suspect)",      320, None, "suspect",         320),
        ("no container, suspect spectral",               None, 128, "suspect",         128),
        ("no container, likely_transcode spectral",      None, 128, "likely_transcode", 128),
        ("no container, genuine spectral ignored",       None, 128, "genuine",         None),
        ("no container, grade None ignored",             None, 128, None,              None),
        ("both None, genuine",                           None, None, "genuine",        None),
        ("both None, suspect",                           None, None, "suspect",        None),
        ("both None, grade None",                        None, None, None,             None),
    ]

    def test_grade_aware_table(self):
        from lib.quality import compute_effective_override_bitrate
        for desc, container, spectral, grade, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    compute_effective_override_bitrate(container, spectral, grade),
                    expected,
                    f"{desc}: compute_effective_override_bitrate"
                    f"({container!r}, {spectral!r}, {grade!r}) "
                    f"expected {expected!r}",
                )

    def test_spectral_transcode_grades_constant(self):
        """Locks the set of grades that authorize spectral override."""
        from lib.quality import SPECTRAL_TRANSCODE_GRADES
        self.assertEqual(SPECTRAL_TRANSCODE_GRADES,
                         frozenset({"suspect", "likely_transcode"}))


# ============================================================================
# dispatch_action
# ============================================================================

class TestDispatchAction(unittest.TestCase):
    """Test dispatch_action: map decision string to action flags via subTest table."""

    # (decision, {flag: expected_value, ...})
    CASES = [
        ("import", dict(mark_done=True, record_rejection=False, denylist=False,
                        requeue=False, cleanup=True, trigger_meelo=True,
                        run_quality_gate=True)),
        ("preflight_existing", dict(mark_done=True, trigger_meelo=True,
                                    run_quality_gate=True)),
        ("downgrade", dict(mark_done=False, record_rejection=True, denylist=True,
                           requeue=False, cleanup=True)),
        ("transcode_upgrade", dict(mark_done=True, denylist=True, requeue=True,
                                   trigger_meelo=True)),
        ("transcode_downgrade", dict(mark_done=False, record_rejection=True,
                                     denylist=True, requeue=True)),
        ("transcode_first", dict(mark_done=True, denylist=True, requeue=True,
                                 trigger_meelo=True)),
        ("conversion_failed", dict(record_rejection=True, denylist=False)),
        ("import_failed", dict(record_rejection=True)),
        ("target_conversion_failed", dict(record_rejection=True, denylist=False)),
    ]

    def test_dispatch_action_flags(self):
        from lib.quality import dispatch_action
        for decision, expected in self.CASES:
            with self.subTest(decision=decision):
                action = dispatch_action(decision)
                for flag, value in expected.items():
                    self.assertEqual(
                        getattr(action, flag), value,
                        f"dispatch_action({decision!r}).{flag}: "
                        f"expected {value!r}, got {getattr(action, flag)!r}")


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
            self.assertTrue(a.mark_done or a.record_rejection,
                            f"dispatch_action('{outcome}') must set mark_done or "
                            "record_rejection")


# ============================================================================
# rejected_download_tier + narrow_override_on_downgrade
# ============================================================================

class TestRejectedDownloadTier(unittest.TestCase):
    """Test mapping from DownloadInfo to search_filetype_override tier string."""

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
    """Test narrowing search_filetype_override after downgrade rejection."""

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
