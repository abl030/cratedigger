#!/usr/bin/env python3
"""Unit tests for lib/quality.py pure decision functions.

These test every branch of the four decision functions directly,
independent of real audio fixtures or the full_pipeline_decision integrator.
"""

import json
import os
import sys
import unittest
from typing import Any

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
    # Codec-aware rank model (issue #60)
    QualityRank,
    RankBitrateMetric,
    CodecRankBands,
    QualityRankConfig,
    quality_rank,
    measurement_rank,
    gate_rank,
    compare_quality,
)


# ============================================================================
# spectral_import_decision
# ============================================================================

class TestSpectralGateTrigger(unittest.TestCase):
    """Test the pre-analysis "would spectral run?" decision (issue #93).

    Mirrors the live gate in lib.preimport._needs_spectral_check. Delivers
    the input the UI Decisions tab and pipeline-cli quality simulator need
    to explain which files go through spectral vs. skip.
    """

    THRESHOLD = 210

    def _run(self, *, is_flac, is_cbr, is_vbr=None, avg=None):
        from lib.quality import spectral_gate_trigger
        return spectral_gate_trigger(
            is_flac=is_flac, is_cbr=is_cbr, is_vbr=is_vbr,
            avg_bitrate_kbps=avg, vbr_threshold_kbps=self.THRESHOLD,
        )

    def test_flac_skips(self):
        """FLAC has its own flow (convert → V0 → transcode_detection)."""
        self.assertEqual(self._run(is_flac=True, is_cbr=False), "skipped_flac")
        self.assertEqual(self._run(is_flac=True, is_cbr=True), "skipped_flac")
        self.assertEqual(
            self._run(is_flac=True, is_cbr=False, is_vbr=True, avg=245),
            "skipped_flac",
            "FLAC always takes precedence over VBR avg")

    def test_cbr_mp3_always_runs(self):
        """CBR MP3 is the classic transcode-cliff case."""
        self.assertEqual(
            self._run(is_flac=False, is_cbr=True), "would_run")
        self.assertEqual(
            self._run(is_flac=False, is_cbr=True, avg=320), "would_run")
        self.assertEqual(
            self._run(is_flac=False, is_cbr=True, avg=128), "would_run")

    def test_vbr_threshold_table(self):
        """VBR MP3: gate skips only when avg is known and >= threshold."""
        CASES = [
            # (desc, avg, expected)
            ("avg unknown → would_run (conservative)",  None, "would_run"),
            ("Go! Team avg 182 < 210",                   182, "would_run"),
            ("just below threshold 209",                 209, "would_run"),
            ("at threshold 210 → high avg skip",         210, "skipped_vbr_high_avg"),
            ("genuine V0 245 → skip",                    245, "skipped_vbr_high_avg"),
            ("genuine V0 260 → skip",                    260, "skipped_vbr_high_avg"),
            ("lowfi 96 → would_run",                      96, "would_run"),
        ]
        for desc, avg, expected in CASES:
            with self.subTest(desc=desc, avg=avg):
                got = self._run(is_flac=False, is_cbr=False,
                                is_vbr=True, avg=avg)
                self.assertEqual(got, expected)

    def test_is_vbr_derived_from_is_cbr_when_omitted(self):
        """Legacy simulator callers that pass is_cbr without is_vbr get
        sensible default: is_vbr = not is_cbr."""
        self.assertEqual(
            self._run(is_flac=False, is_cbr=False), "would_run",
            "derived is_vbr=True with avg=None → gate still runs")
        self.assertEqual(
            self._run(is_flac=False, is_cbr=False, avg=245),
            "skipped_vbr_high_avg",
            "derived is_vbr=True with high avg → skip")

    def test_both_unknown_falls_back_to_would_run(self):
        """is_cbr=None AND is_vbr=None → conservative default."""
        self.assertEqual(
            self._run(is_flac=False, is_cbr=None),
            "would_run")


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
    """Codec-aware import decision (issue #60).

    Every row explicitly sets ``format`` + ``avg_bitrate_kbps`` on both
    measurements so the rank model has what it needs. The old blanket
    verified_lossless bypass is replaced by a tier-gated preference:
    ``verified_lossless=True`` still imports on "better" or "equivalent",
    but a "worse" verdict is blocked regardless — this prevents a
    deliberately-too-low verified_lossless_target from replacing a good
    existing album.
    """

    CASES = [
        # desc, new_kwargs, existing_kwargs, is_transcode, expected

        # --- Same-codec mono-codec regression cases ---
        ("V0 beats V2 (same codec family, different rank)",
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         dict(format="mp3 v2", avg_bitrate_kbps=190),
         False, "import"),
        ("V2 loses to V0",
         dict(format="mp3 v2", avg_bitrate_kbps=190),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "downgrade"),
        ("equal V0 labels → equivalent → downgrade without verified_lossless",
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "downgrade"),
        ("equal CBR 320 → equivalent → downgrade",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True),
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True),
         False, "downgrade"),
        ("CBR 192 loses to CBR 320",
         dict(format="mp3 192", avg_bitrate_kbps=192, is_cbr=True),
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True),
         False, "downgrade"),

        # --- Cross-codec equivalence (core #60 fix) ---
        ("Opus 128 equivalent to MP3 V0 → no verified → downgrade",
         dict(format="opus 128", avg_bitrate_kbps=130),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "downgrade"),
        ("Opus 128 equivalent to MP3 V0 + verified_lossless → import",
         dict(format="opus 128", avg_bitrate_kbps=130, verified_lossless=True),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "import"),
        ("FLAC→Opus 128 equivalent to MP3 CBR 320 + verified_lossless → import",
         dict(format="opus 128", avg_bitrate_kbps=130, verified_lossless=True),
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True),
         False, "import"),

        # --- verified_lossless guardrail (core #60 fix) ---
        ("Opus 64 verified CANNOT replace MP3 V0 245",
         dict(format="opus 64", avg_bitrate_kbps=64, verified_lossless=True),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "downgrade"),
        ("Opus 48 verified CANNOT replace MP3 CBR 320",
         dict(format="opus 48", avg_bitrate_kbps=48, verified_lossless=True),
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True),
         False, "downgrade"),

        # --- Lo-fi genuine V0 (label semantics preserved) ---
        ("lo-fi V0 (207) equivalent to dense V0 (245) + verified → import",
         dict(format="mp3 v0", avg_bitrate_kbps=207, verified_lossless=True),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "import"),

        # --- No existing album ---
        ("no existing → import",
         dict(format="mp3 v0", avg_bitrate_kbps=240), None, False, "import"),
        ("no existing transcode → transcode_first",
         dict(format="mp3 v0", avg_bitrate_kbps=150), None, True, "transcode_first"),

        # --- Transcode semantics ---
        ("transcode upgrade (better rank)",
         dict(format="mp3 v0", avg_bitrate_kbps=192),
         dict(format="mp3 192", avg_bitrate_kbps=128, is_cbr=True),
         True, "transcode_upgrade"),
        ("transcode downgrade (worse rank)",
         dict(format="mp3 128", avg_bitrate_kbps=128, is_cbr=True),
         dict(format="mp3 v0", avg_bitrate_kbps=192),
         True, "transcode_downgrade"),

        # --- Legacy format-less fallback via bare-codec path ---
        # When format is None on both sides, measurements fall to UNKNOWN rank
        # and compare_quality() uses the bare-codec bitrate tiebreaker with
        # tolerance. Tests here document that fallback explicitly.
        ("legacy no-format tie → equivalent → downgrade",
         dict(min_bitrate_kbps=320), dict(min_bitrate_kbps=320),
         False, "downgrade"),
        ("legacy no-format worse → downgrade",
         dict(min_bitrate_kbps=192), dict(min_bitrate_kbps=320),
         False, "downgrade"),
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
                    f"{desc}: new={new_kwargs} existing={existing_kwargs} "
                    f"is_transcode={is_transcode} expected {expected!r}")


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

    # ---- Issue #66: configurable spectral-fallback threshold -------------

    def test_default_constant_matches_default_cfg_mp3_vbr_excellent(self):
        """Legacy module constant must equal the default cfg's mp3_vbr.excellent.

        These are two different surfaces for the same number — if a future
        change tunes mp3_vbr.excellent without updating the legacy constant,
        the contract tests in test_decision_tree_constants_match_code break
        and the displayed transcode threshold drifts from the runtime
        threshold. Pin the equality so the divergence is loud.
        """
        defaults_excellent = QualityRankConfig.defaults().mp3_vbr.excellent
        self.assertEqual(TRANSCODE_MIN_BITRATE_KBPS, defaults_excellent)

    def test_transcode_detection_uses_cfg_mp3_vbr_excellent(self):
        """Custom cfg must shift the spectral-fallback threshold."""
        # Default cfg → threshold 210 → 200 is a transcode.
        default_cfg = QualityRankConfig.defaults()
        self.assertTrue(transcode_detection(
            10, 200, spectral_grade=None, cfg=default_cfg))

        # Lower the threshold to 180 → 200 is no longer a transcode.
        loose_cfg = QualityRankConfig(
            mp3_vbr=CodecRankBands(
                transparent=245, excellent=180, good=140, acceptable=100))
        self.assertFalse(transcode_detection(
            10, 200, spectral_grade=None, cfg=loose_cfg))

        # Raise the threshold to 240 → 230 becomes a transcode.
        strict_cfg = QualityRankConfig(
            mp3_vbr=CodecRankBands(
                transparent=300, excellent=240, good=180, acceptable=140))
        self.assertTrue(transcode_detection(
            10, 230, spectral_grade=None, cfg=strict_cfg))
        # And just above the strict threshold passes.
        self.assertFalse(transcode_detection(
            10, 240, spectral_grade=None, cfg=strict_cfg))

    def test_transcode_detection_cfg_does_not_override_spectral(self):
        """Spectral grade is still authoritative even with a custom cfg."""
        loose_cfg = QualityRankConfig(
            mp3_vbr=CodecRankBands(
                transparent=245, excellent=180, good=140, acceptable=100))
        # Spectral=suspect → transcode regardless of bitrate (240 > threshold).
        self.assertTrue(transcode_detection(
            10, 240, spectral_grade="suspect", cfg=loose_cfg))
        # Spectral=genuine → not transcode even when bitrate < threshold.
        self.assertFalse(transcode_detection(
            10, 100, spectral_grade="genuine", cfg=loose_cfg))

    def test_transcode_detection_default_cfg_when_omitted(self):
        """Omitting cfg must reproduce the legacy hardcoded behavior.

        Critical for backward compatibility — every existing caller that
        doesn't pass cfg keeps using the 210 kbps threshold.
        """
        # Same as the legacy "below threshold" case (210 - 20 = 190).
        self.assertTrue(transcode_detection(10, 190, spectral_grade=None))
        # Same as the legacy "at threshold" case.
        self.assertFalse(transcode_detection(
            10, TRANSCODE_MIN_BITRATE_KBPS, spectral_grade=None))


# ============================================================================
# quality_gate_decision
# ============================================================================

class TestQualityGateDecision(unittest.TestCase):
    """Codec-aware post-import quality gate (issue #60).

    Every row explicitly sets the ``format`` field so quality_rank()
    classifies against the right band table. The legacy blanket
    ``verified_lossless`` bypass is replaced by the rank model — lo-fi
    V0 reads as TRANSPARENT from the label, so the bypass is no longer
    needed for genuine lo-fi.
    """

    CASES = [
        # (description, measurement_kwargs, expected_decision)

        # --- accept: labels with TRANSPARENT rank (cross-codec equivalence) ---
        ("MP3 V0 label lo-fi accepts without bypass",
         dict(format="mp3 v0", avg_bitrate_kbps=207), "accept"),
        ("MP3 V0 label dense",
         dict(format="mp3 v0", avg_bitrate_kbps=245), "accept"),
        ("Opus 128 verified lossless",
         dict(format="opus 128", avg_bitrate_kbps=130, verified_lossless=True), "accept"),
        ("Opus 128 not verified (label still transparent)",
         dict(format="opus 128", avg_bitrate_kbps=130), "accept"),
        ("bare MP3 VBR above rank",
         dict(format="MP3", avg_bitrate_kbps=240, is_cbr=False), "accept"),

        # --- requeue_upgrade: rank below gate_min_rank (EXCELLENT) ---
        ("bare MP3 VBR below rank",
         dict(format="MP3", avg_bitrate_kbps=150, is_cbr=False), "requeue_upgrade"),
        ("Opus 64 verified (target too low)",
         dict(format="opus 64", avg_bitrate_kbps=64, verified_lossless=True), "requeue_upgrade"),
        ("Opus 48 verified (target far too low)",
         dict(format="opus 48", avg_bitrate_kbps=48, verified_lossless=True), "requeue_upgrade"),
        ("spectral clamp pulls CBR 320 down",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True,
              spectral_bitrate_kbps=128), "requeue_upgrade"),
        ("no format no bitrate → UNKNOWN",
         dict(), "requeue_upgrade"),

        # --- requeue_lossless: CBR at TRANSPARENT but unverified ---
        ("CBR 320 unverified → requeue_lossless",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True), "requeue_lossless"),
        ("bare MP3 CBR 320 unverified → requeue_lossless",
         dict(format="MP3", avg_bitrate_kbps=320, is_cbr=True), "requeue_lossless"),
        ("bare MP3 CBR 256 unverified → requeue_lossless",
         dict(format="MP3", avg_bitrate_kbps=256, is_cbr=True), "requeue_lossless"),

        # --- lossless accepts regardless ---
        ("FLAC accepts",
         dict(format="FLAC", avg_bitrate_kbps=900), "accept"),
        ("lossless label accepts with no bitrate",
         dict(format="flac"), "accept"),

        # --- legacy verified_lossless cases (still honoured via label if present) ---
        ("legacy no format, verified_lossless → UNKNOWN → requeue_upgrade",
         dict(min_bitrate_kbps=180, verified_lossless=True), "requeue_upgrade"),
    ]

    def test_quality_gate_decisions(self):
        for desc, kwargs, expected in self.CASES:
            with self.subTest(desc=desc):
                m = AudioQualityMeasurement(**kwargs)
                self.assertEqual(
                    quality_gate_decision(m), expected,
                    f"{desc}: {kwargs} expected {expected!r}")


# ============================================================================
# gate_rank — single source of truth for the gate's classified rank
# ============================================================================
#
# gate_rank() centralizes the spectral clamp that quality_gate_decision()
# previously inlined. The simulator and the gate must always agree on the
# displayed/decision rank — these tests pin that contract.

class TestGateRank(unittest.TestCase):
    """gate_rank: measurement_rank with the spectral clamp applied."""

    def test_no_spectral_matches_measurement_rank(self):
        """Without spectral, gate_rank must equal measurement_rank."""
        m = AudioQualityMeasurement(format="mp3 v0", avg_bitrate_kbps=245)
        cfg = QualityRankConfig.defaults()
        self.assertEqual(gate_rank(m, cfg), measurement_rank(m, cfg))

    def test_clamp_pulls_fake_cbr_down(self):
        """Fake CBR 320 with spectral=128 must clamp from TRANSPARENT to POOR."""
        m = AudioQualityMeasurement(
            format="mp3 320", avg_bitrate_kbps=320, is_cbr=True,
            spectral_bitrate_kbps=128)
        cfg = QualityRankConfig.defaults()
        # Without clamp, label "mp3 320" → TRANSPARENT
        self.assertEqual(measurement_rank(m, cfg), QualityRank.TRANSPARENT)
        # With clamp, spectral 128 against mp3_vbr.acceptable=130 → POOR
        self.assertEqual(gate_rank(m, cfg), QualityRank.POOR)

    def test_clamp_does_nothing_when_higher(self):
        """Spectral above measurement rank: no clamp."""
        m = AudioQualityMeasurement(
            format="mp3", avg_bitrate_kbps=140, is_cbr=False,
            spectral_bitrate_kbps=240)
        cfg = QualityRankConfig.defaults()
        # measurement: 140 → ACCEPTABLE; spectral 240 → EXCELLENT (higher); no clamp
        self.assertEqual(gate_rank(m, cfg), QualityRank.ACCEPTABLE)

    def test_afx_analord_regression(self):
        """AFX Analord 09 live scenario: VBR 245kbps + spectral=160 likely_transcode.

        Reproduces the exact case from the post-deploy reflection. The bare
        MP3 label at 245 kbps is TRANSPARENT, but the spectral clamp must
        pull it down to ACCEPTABLE so the gate's NEEDS UPGRADE verdict and
        the displayed rank label agree.
        """
        m = AudioQualityMeasurement(
            min_bitrate_kbps=213, avg_bitrate_kbps=245,
            format="MP3", is_cbr=False,
            spectral_bitrate_kbps=160)
        cfg = QualityRankConfig.defaults()
        rank = gate_rank(m, cfg)
        # Spectral 160 → mp3_vbr.acceptable=130, between acceptable/good → ACCEPTABLE
        self.assertEqual(rank, QualityRank.ACCEPTABLE)
        # And quality_gate_decision agrees
        self.assertEqual(quality_gate_decision(m, cfg), "requeue_upgrade")

    def test_gate_decision_matches_pinned_cases(self):
        """quality_gate_decision must agree with TestQualityGateDecision.CASES.

        Direct cross-check: call quality_gate_decision() (which internally
        consults gate_rank) and compare against the pinned CASE expectation.
        Avoids re-implementing the gate body in test code so the test can't
        silently drift if the gate logic changes.
        """
        cfg = QualityRankConfig.defaults()
        for desc, kwargs, expected in TestQualityGateDecision.CASES:
            with self.subTest(desc=desc):
                m = AudioQualityMeasurement(**kwargs)
                self.assertEqual(quality_gate_decision(m, cfg), expected,
                                 f"{desc}: quality_gate_decision diverges from CASE expectation")


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
    "stage0_spectral_gate",
    "stage1_spectral", "stage2_import", "stage3_quality_gate",
    "final_status", "imported", "denylisted", "keep_searching",
    "target_final_format",
}

# Valid values for each stage (None means stage was skipped)
VALID_STAGE0 = {None, "would_run", "skipped_vbr_high_avg", "skipped_flac"}
VALID_STAGE1 = {None, "import", "import_upgrade", "import_no_exist", "reject"}
VALID_STAGE2 = {None, "import", "downgrade", "transcode_upgrade",
                "transcode_downgrade", "transcode_first",
                "preflight_existing"}
VALID_STAGE3 = {None, "accept", "requeue_upgrade", "requeue_lossless"}
VALID_FINAL_STATUS = {None, "imported", "wanted"}

# The exact parameter names the simulator form submits
EXPECTED_PARAMS = {
    "is_flac", "min_bitrate", "is_cbr",
    "is_vbr", "avg_bitrate",
    "spectral_grade", "spectral_bitrate",
    "existing_min_bitrate", "existing_spectral_bitrate",
    "override_min_bitrate",
    "existing_format", "existing_is_cbr",
    "post_conversion_min_bitrate", "converted_count",
    "verified_lossless", "verified_lossless_target",
    "target_format",
    "new_format", "cfg",
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

    def test_stage0_values_in_contract(self):
        """Stage 0 gate trigger must be from the known set for every scenario."""
        # Each call is written out so pyright can type-check against the
        # full_pipeline_decision signature (bool vs int) — a **kwargs dict
        # collapses bool→int and breaks type narrowing.
        results = [
            # FLAC always skips the MP3 gate
            full_pipeline_decision(is_flac=True, min_bitrate=0, is_cbr=False),
            # CBR MP3 always gates
            full_pipeline_decision(is_flac=False, min_bitrate=320, is_cbr=True),
            # VBR MP3 with low avg gates (issue #93 Go! Team)
            full_pipeline_decision(is_flac=False, min_bitrate=126, is_cbr=False,
                                   is_vbr=True, avg_bitrate=182),
            # VBR MP3 with high avg skips (genuine V0)
            full_pipeline_decision(is_flac=False, min_bitrate=220, is_cbr=False,
                                   is_vbr=True, avg_bitrate=245),
            # VBR MP3 unknown avg (legacy or resumed) gates
            full_pipeline_decision(is_flac=False, min_bitrate=200, is_cbr=False,
                                   is_vbr=True),
        ]
        for r in results:
            self.assertIn(r["stage0_spectral_gate"], VALID_STAGE0,
                          f"Unexpected stage0 value: {r['stage0_spectral_gate']}")

    def test_stage0_high_avg_vbr_skips_stage1(self):
        """When stage 0 says skip, stage 1 must be None even if spectral
        was (accidentally) supplied — otherwise the simulator would
        misrepresent production behavior, which skips spectral entirely."""
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=220, is_cbr=False,
            is_vbr=True, avg_bitrate=245,
            # Caller supplied spectral_grade, but stage 0 says don't gate.
            spectral_grade="suspect", spectral_bitrate=192,
            existing_spectral_bitrate=100,
        )
        self.assertEqual(r["stage0_spectral_gate"], "skipped_vbr_high_avg")
        self.assertIsNone(
            r["stage1_spectral"],
            "stage 1 must not run when the gate trigger said skip — "
            "production's _needs_spectral_check would short-circuit before "
            "spectral_analyze is even called")

    def test_stage0_low_avg_vbr_runs_stage1(self):
        """Go! Team case: VBR avg 182 < 210 → stage 0 would_run → if
        spectral is provided, stage 1 executes and can reject."""
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=126, is_cbr=False,
            is_vbr=True, avg_bitrate=182,
            spectral_grade="likely_transcode", spectral_bitrate=96,
            existing_spectral_bitrate=128,
        )
        self.assertEqual(r["stage0_spectral_gate"], "would_run")
        # 96 <= 128 → reject in stage 1
        self.assertEqual(r["stage1_spectral"], "reject")
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])

    def test_stage0_flac_preserves_stage1(self):
        """FLAC path: stage 0 says skipped_flac but stage 1 (modeled as
        the FLAC post-conversion spectral decision) must still run when
        spectral data is provided."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            converted_count=10, post_conversion_min_bitrate=245,
        )
        self.assertEqual(r["stage0_spectral_gate"], "skipped_flac")
        # Genuine → stage 1 runs and says import
        self.assertEqual(r["stage1_spectral"], "import")

    def test_decision_tree_stage_ids(self):
        """Decision tree must have the expected stages in order."""
        tree = get_decision_tree()
        ids = [s["id"] for s in tree["stages"]]
        self.assertEqual(ids, ["flac_spectral", "flac_convert", "transcode",
                               "verified_lossless", "target_conversion",
                               "mp3_spectral_gate", "mp3_spectral",
                               "import_decision", "quality_gate", "dispatch"])

    def test_decision_tree_mp3_gate_exposes_threshold(self):
        """The new mp3_spectral_gate stage must surface the VBR threshold
        (issue #93) so the UI Decisions tab shows the same cutoff as the
        live production gate in lib/preimport._needs_spectral_check."""
        tree = get_decision_tree()
        stage_map = {s["id"]: s for s in tree["stages"]}
        self.assertIn("mp3_spectral_gate", stage_map,
                      "mp3_spectral_gate stage must exist")
        gate = stage_map["mp3_spectral_gate"]
        self.assertEqual(gate["function"], "spectral_gate_trigger")
        outcomes = set(gate["outcomes"])
        self.assertEqual(outcomes,
                         {"would_run", "skipped_vbr_high_avg", "skipped_flac"})
        # Threshold surfaces as part of at least one rule/note
        threshold = tree["constants"]["TRANSCODE_MIN_BITRATE_KBPS"]
        tree_text = str(gate["rules"]) + str(gate.get("note", ""))
        self.assertIn(str(threshold), tree_text,
                      f"threshold {threshold} must appear in gate stage text")

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
        """Tree constants must match the actual module constants under default cfg.

        With no cfg passed, get_decision_tree falls back to
        QualityRankConfig.defaults(), whose mp3_vbr.excellent equals the
        legacy TRANSCODE_MIN_BITRATE_KBPS constant (pinned by
        test_default_constant_matches_default_cfg_mp3_vbr_excellent).
        """
        tree = get_decision_tree()
        consts = tree["constants"]
        self.assertEqual(consts["QUALITY_MIN_BITRATE_KBPS"],
                         QUALITY_MIN_BITRATE_KBPS)
        self.assertEqual(consts["TRANSCODE_MIN_BITRATE_KBPS"],
                         TRANSCODE_MIN_BITRATE_KBPS)

    def test_decision_tree_custom_cfg_drives_transcode_threshold(self):
        """get_decision_tree(cfg=...) must surface cfg.mp3_vbr.excellent in
        the transcode stage so the web Decisions tab tracks runtime retuning.

        Issue #66 made transcode_detection() read cfg.mp3_vbr.excellent at
        call time, but the decision tree previously hardcoded the legacy
        constant. An operator who set mp3_vbr.excellent=170 would see a
        stale "< 210kbps" threshold in the UI while the actual gate ran
        at 170. This test pins the fix: the threshold surfaced to the UI
        must come from the same cfg the gate uses.
        """
        from lib.quality import CodecRankBands, QualityRankConfig

        custom_cfg = QualityRankConfig(
            mp3_vbr=CodecRankBands(
                transparent=245, excellent=170, good=140, acceptable=100))
        tree = get_decision_tree(cfg=custom_cfg)
        self.assertEqual(tree["constants"]["TRANSCODE_MIN_BITRATE_KBPS"], 170)

        # The transcode stage's rule text and note must also reference the
        # custom threshold — the UI reads these strings directly.
        transcode_stage = next(
            s for s in tree["stages"] if s["id"] == "transcode")
        fallback_rule = next(
            r for r in transcode_stage["rules"]
            if "no spectral" in r["condition"])
        self.assertIn("170kbps", fallback_rule["condition"])
        self.assertIn("170kbps", transcode_stage["note"])
        self.assertNotIn(f"{TRANSCODE_MIN_BITRATE_KBPS}kbps",
                         fallback_rule["condition"])

    def test_decision_tree_default_cfg_matches_legacy_constant(self):
        """Explicit None cfg must reproduce the legacy hardcoded threshold.

        Back-compat guard: any existing caller passing no cfg (or None)
        should see the same payload they saw before #66's follow-up. Pins
        the default surface against TRANSCODE_MIN_BITRATE_KBPS.
        """
        default_tree = get_decision_tree(cfg=None)
        self.assertEqual(
            default_tree["constants"]["TRANSCODE_MIN_BITRATE_KBPS"],
            TRANSCODE_MIN_BITRATE_KBPS)
        transcode_stage = next(
            s for s in default_tree["stages"] if s["id"] == "transcode")
        self.assertIn(
            f"{TRANSCODE_MIN_BITRATE_KBPS}kbps",
            transcode_stage["note"])

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

    def test_target_conversion_guardrail_blocks_low_target_before_import(self):
        """Low verified-lossless target must lose the import comparison itself."""
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine", converted_count=10,
            post_conversion_min_bitrate=245,
            existing_min_bitrate=245,
            existing_format="mp3 v0",
            verified_lossless_target="opus 64")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])
        self.assertEqual(r["final_status"], "imported")
        self.assertTrue(r["keep_searching"])


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

    # --- Rank-aware: cfg threading (post-deploy follow-up) ---
    #
    # Before this fix, rejection_backfill_override hardcoded
    # `min_bitrate_kbps >= QUALITY_MIN_BITRATE_KBPS` (210). Custom
    # gate_min_rank settings did not propagate to the backfill decision
    # — a cfg.gate_min_rank=GOOD operator would still see backfill fire
    # only above 210kbps. Decision functions must thread cfg.

    def test_custom_good_gate_lets_lower_vbr_backfill(self):
        """gate_min_rank=GOOD lowers the bar: 180kbps VBR genuine → backfill fires."""
        from lib.quality import rejection_backfill_override, QUALITY_LOSSLESS
        lenient = QualityRankConfig(gate_min_rank=QualityRank.GOOD)
        result = rejection_backfill_override(
            is_cbr=False, min_bitrate_kbps=180,
            spectral_grade="genuine", verified_lossless=False,
            cfg=lenient)
        # 180 against mp3_vbr (good=170) → GOOD, GOOD >= GOOD → backfill
        self.assertEqual(result, QUALITY_LOSSLESS)

    def test_custom_good_gate_default_blocks_lower_vbr(self):
        """Default gate_min_rank=EXCELLENT blocks the same 180kbps VBR."""
        from lib.quality import rejection_backfill_override
        result = rejection_backfill_override(
            is_cbr=False, min_bitrate_kbps=180,
            spectral_grade="genuine", verified_lossless=False)
        # 180 → GOOD, GOOD < EXCELLENT → no backfill
        self.assertIsNone(result)

    def test_custom_transparent_gate_blocks_excellent_cbr(self):
        """gate_min_rank=TRANSPARENT raises the bar: CBR 256 (EXCELLENT) no longer backfills."""
        from lib.quality import rejection_backfill_override
        strict = QualityRankConfig(gate_min_rank=QualityRank.TRANSPARENT)
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=256,
            spectral_grade="genuine", verified_lossless=False,
            cfg=strict)
        # 256 against mp3_cbr (transparent=320, excellent=256) → EXCELLENT,
        # EXCELLENT < TRANSPARENT → no backfill
        self.assertIsNone(result)

    def test_custom_transparent_gate_still_backfills_cbr_320(self):
        """gate_min_rank=TRANSPARENT still backfills CBR 320 (TRANSPARENT rank)."""
        from lib.quality import rejection_backfill_override, QUALITY_LOSSLESS
        strict = QualityRankConfig(gate_min_rank=QualityRank.TRANSPARENT)
        result = rejection_backfill_override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False,
            cfg=strict)
        self.assertEqual(result, QUALITY_LOSSLESS)


# ============================================================================
# Codec-aware quality rank model (issue #60)
# ============================================================================
#
# Every branch of quality_rank / measurement_rank / compare_quality has a
# direct subTest row. No numeric thresholds are hardcoded in the tests —
# we reference CFG = QualityRankConfig.defaults() so if a band moves in the
# defaults the tests move with it automatically.

CFG = QualityRankConfig.defaults()


class TestCodecRankBands(unittest.TestCase):
    """rank_for() exhaustively, plus the monotonic invariant."""

    # (description, transparent, excellent, good, acceptable, bitrate, expected)
    CASES = [
        ("exactly transparent threshold",   112, 88, 64, 48, 112, QualityRank.TRANSPARENT),
        ("above transparent",               112, 88, 64, 48, 200, QualityRank.TRANSPARENT),
        ("exactly excellent threshold",     112, 88, 64, 48,  88, QualityRank.EXCELLENT),
        ("between excellent and transparent", 112, 88, 64, 48, 100, QualityRank.EXCELLENT),
        ("exactly good threshold",          112, 88, 64, 48,  64, QualityRank.GOOD),
        ("between good and excellent",      112, 88, 64, 48,  80, QualityRank.GOOD),
        ("exactly acceptable threshold",    112, 88, 64, 48,  48, QualityRank.ACCEPTABLE),
        ("between acceptable and good",     112, 88, 64, 48,  56, QualityRank.ACCEPTABLE),
        ("below acceptable",                112, 88, 64, 48,  32, QualityRank.POOR),
        ("zero",                            112, 88, 64, 48,   0, QualityRank.POOR),
        ("None bitrate",                    112, 88, 64, 48,  None, QualityRank.UNKNOWN),
    ]

    def test_rank_for_table(self):
        for desc, t, e, g, a, br, expected in self.CASES:
            with self.subTest(desc=desc):
                bands = CodecRankBands(transparent=t, excellent=e, good=g, acceptable=a)
                self.assertEqual(bands.rank_for(br), expected)

    def test_monotonic_invariant(self):
        # Non-monotonic bands must raise at construction time.
        with self.assertRaises(ValueError):
            CodecRankBands(transparent=100, excellent=150, good=50, acceptable=25)
        with self.assertRaises(ValueError):
            CodecRankBands(transparent=100, excellent=90, good=95, acceptable=50)
        with self.assertRaises(ValueError):
            CodecRankBands(transparent=100, excellent=90, good=80, acceptable=-5)


class TestQualityRank(unittest.TestCase):
    """quality_rank() across every codec, every band, every resolution step.

    Uses default QualityRankConfig values for the classification — if the
    defaults change, individual rows may need updating, which is intentional
    (the defaults are the contract).
    """

    # (description, format_hint, bitrate_kbps, is_cbr, expected_rank)
    CASES = [
        # --- Step 1: both None → UNKNOWN ---
        ("None format + None bitrate",             None,            None, False, QualityRank.UNKNOWN),

        # --- Step 2: lossless family ---
        ("FLAC label",                             "FLAC",          1000, False, QualityRank.LOSSLESS),
        ("flac label lowercase",                   "flac",          1200, False, QualityRank.LOSSLESS),
        ("lossless label",                         "lossless",      1100, False, QualityRank.LOSSLESS),
        ("ALAC label",                             "ALAC",           900, False, QualityRank.LOSSLESS),
        ("WAV label",                              "WAV",           1411, False, QualityRank.LOSSLESS),
        ("flac with None bitrate",                 "flac",          None, False, QualityRank.LOSSLESS),

        # --- Step 3: explicit MP3 VBR quality label ---
        ("mp3 v0 lo-fi",                           "mp3 v0",         207, False, QualityRank.TRANSPARENT),
        ("mp3 v0 dense",                           "mp3 v0",         260, False, QualityRank.TRANSPARENT),
        ("mp3 v1 label",                           "mp3 v1",         220, False, QualityRank.EXCELLENT),
        ("mp3 v2 label",                           "mp3 v2",         190, False, QualityRank.EXCELLENT),
        ("mp3 v3 label",                           "mp3 v3",         170, False, QualityRank.GOOD),
        ("mp3 v4 label",                           "mp3 v4",         155, False, QualityRank.GOOD),
        ("mp3 v5 label",                           "mp3 v5",         130, False, QualityRank.ACCEPTABLE),
        ("mp3 v9 label",                           "mp3 v9",          65, False, QualityRank.ACCEPTABLE),

        # --- Step 4: explicit Opus bitrate label ---
        ("opus 128 label",                         "opus 128",        95, False, QualityRank.TRANSPARENT),
        ("opus 96 label",                          "opus 96",        100, False, QualityRank.EXCELLENT),
        ("opus 64 label",                          "opus 64",        100, False, QualityRank.GOOD),
        ("opus 48 label",                          "opus 48",        100, False, QualityRank.ACCEPTABLE),
        ("opus 32 label",                          "opus 32",        100, False, QualityRank.POOR),

        # --- Step 4: explicit MP3 CBR bitrate label (used for "mp3 320" style) ---
        ("mp3 320 label",                          "mp3 320",        320, True,  QualityRank.TRANSPARENT),
        ("mp3 256 label",                          "mp3 256",        256, True,  QualityRank.EXCELLENT),
        ("mp3 192 label",                          "mp3 192",        192, True,  QualityRank.GOOD),
        ("mp3 128 label",                          "mp3 128",        128, True,  QualityRank.ACCEPTABLE),

        # --- Step 4: explicit AAC bitrate label ---
        ("aac 192 label",                          "aac 192",        192, False, QualityRank.TRANSPARENT),
        ("aac 144 label",                          "aac 144",        144, False, QualityRank.EXCELLENT),
        ("aac 112 label",                          "aac 112",        112, False, QualityRank.GOOD),
        ("aac 80 label",                           "aac 80",          80, False, QualityRank.ACCEPTABLE),

        # --- Step 5: bare codec name + measured bitrate (beets items.format path) ---
        # Default mp3_vbr bands: transparent=245, excellent=210, good=170, acceptable=130
        ("MP3 VBR beets 260",                      "MP3",            260, False, QualityRank.TRANSPARENT),
        ("MP3 VBR beets 245",                      "MP3",            245, False, QualityRank.TRANSPARENT),
        ("MP3 VBR beets 220",                      "MP3",            220, False, QualityRank.EXCELLENT),
        ("MP3 VBR beets 210",                      "MP3",            210, False, QualityRank.EXCELLENT),
        ("MP3 VBR beets 180",                      "MP3",            180, False, QualityRank.GOOD),
        ("MP3 VBR beets 170",                      "MP3",            170, False, QualityRank.GOOD),
        ("MP3 VBR beets 140",                      "MP3",            140, False, QualityRank.ACCEPTABLE),
        ("MP3 VBR beets 130",                      "MP3",            130, False, QualityRank.ACCEPTABLE),
        ("MP3 VBR beets 100",                      "MP3",            100, False, QualityRank.POOR),
        ("MP3 CBR beets 320",                      "MP3",            320, True,  QualityRank.TRANSPARENT),
        ("MP3 CBR beets 256",                      "MP3",            256, True,  QualityRank.EXCELLENT),
        ("MP3 CBR beets 192",                      "MP3",            192, True,  QualityRank.GOOD),
        ("MP3 CBR beets 128",                      "MP3",            128, True,  QualityRank.ACCEPTABLE),
        ("Opus beets 120",                         "Opus",           120, False, QualityRank.TRANSPARENT),
        ("Opus beets 95",                          "Opus",            95, False, QualityRank.EXCELLENT),
        ("Opus beets 70",                          "Opus",            70, False, QualityRank.GOOD),
        ("Opus beets 50",                          "Opus",            50, False, QualityRank.ACCEPTABLE),
        ("AAC beets 200",                          "AAC",            200, False, QualityRank.TRANSPARENT),
        ("AAC beets 150",                          "AAC",            150, False, QualityRank.EXCELLENT),
        ("AAC beets 120",                          "AAC",            120, False, QualityRank.GOOD),

        # --- Step 6: unknown codec family ---
        ("unknown codec",                          "vorbis",         200, False, QualityRank.UNKNOWN),
        ("unknown codec with bitrate label",       "vorbis 192",     None, False, QualityRank.UNKNOWN),
        ("unknown codec with vbr-ish label",       "wma v0",         None, False, QualityRank.UNKNOWN),
        ("empty string format",                    "",               200, False, QualityRank.UNKNOWN),
        ("whitespace-only format",                 "   ",            200, False, QualityRank.UNKNOWN),

        # --- Edge: bare codec with None bitrate → UNKNOWN ---
        ("bare MP3 no bitrate",                    "MP3",            None, False, QualityRank.UNKNOWN),
        ("bare Opus no bitrate",                   "Opus",           None, False, QualityRank.UNKNOWN),
    ]

    def test_quality_rank_table(self):
        for desc, fmt, br, is_cbr, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    quality_rank(fmt, br, is_cbr, CFG), expected,
                    f"{desc}: quality_rank({fmt!r}, {br!r}, {is_cbr!r}) "
                    f"expected {expected!r}",
                )


class TestMeasurementRank(unittest.TestCase):
    """measurement_rank() — metric dispatch lives ONLY here."""

    def test_avg_preferred_over_min_when_both_present(self):
        m = AudioQualityMeasurement(
            min_bitrate_kbps=80, avg_bitrate_kbps=130, format="Opus")
        # Default config uses AVG; 130 → TRANSPARENT for Opus
        self.assertEqual(measurement_rank(m, CFG), QualityRank.TRANSPARENT)

    def test_falls_back_to_min_when_avg_is_none(self):
        m = AudioQualityMeasurement(
            min_bitrate_kbps=260, avg_bitrate_kbps=None, format="MP3")
        # Legacy measurement — AVG metric falls back to min.
        # 260 is above default mp3_vbr.transparent=245 → TRANSPARENT.
        self.assertEqual(measurement_rank(m, CFG), QualityRank.TRANSPARENT)

    def test_min_metric_uses_min(self):
        cfg = QualityRankConfig(bitrate_metric=RankBitrateMetric.MIN)
        m = AudioQualityMeasurement(
            min_bitrate_kbps=80, avg_bitrate_kbps=130, format="Opus")
        # MIN metric ignores the higher avg
        self.assertEqual(measurement_rank(m, cfg), QualityRank.GOOD)

    def test_none_both_bitrates(self):
        m = AudioQualityMeasurement(format="MP3")
        self.assertEqual(measurement_rank(m, CFG), QualityRank.UNKNOWN)

    # ---- MEDIAN metric (issue #64) ---------------------------------------
    # The median is robust against per-track outliers like a 60kbps interlude
    # or a 320kbps hidden track on an otherwise V0 album. The subtest table
    # below pins the dispatch behavior for every interesting combination.
    MEDIAN_CASES = [
        # (description, min, avg, median, format, expected_rank)
        ("median wins over outlier-low min — Opus 130 album",
         60, 128, 130, "Opus", QualityRank.TRANSPARENT),
        ("median wins over outlier-high avg — MP3 V0 album with one 320 hidden track",
         200, 230, 215, "MP3", QualityRank.EXCELLENT),
        ("median falls back to min when None",
         260, 260, None, "MP3", QualityRank.TRANSPARENT),
        ("median below acceptable → POOR",
         128, 128, 100, "MP3", QualityRank.POOR),
        ("median classifies bare Opus into GOOD band",
         60, 130, 70, "Opus", QualityRank.GOOD),
    ]

    def test_median_metric_table(self):
        cfg_median = QualityRankConfig(bitrate_metric=RankBitrateMetric.MEDIAN)
        for desc, mn, av, med, fmt, expected in self.MEDIAN_CASES:
            with self.subTest(desc=desc):
                m = AudioQualityMeasurement(
                    min_bitrate_kbps=mn,
                    avg_bitrate_kbps=av,
                    median_bitrate_kbps=med,
                    format=fmt,
                )
                self.assertEqual(measurement_rank(m, cfg_median), expected)

    def test_median_metric_does_not_affect_avg_default(self):
        """Setting median_bitrate_kbps must not change AVG-policy classification."""
        m = AudioQualityMeasurement(
            min_bitrate_kbps=80, avg_bitrate_kbps=130,
            median_bitrate_kbps=70, format="Opus")
        # Default AVG metric → still uses 130 → TRANSPARENT, ignoring median.
        self.assertEqual(measurement_rank(m, CFG), QualityRank.TRANSPARENT)

    def test_median_metric_falls_back_to_min_when_only_min_set(self):
        """Legacy measurements with only min populated still classify under MEDIAN."""
        cfg_median = QualityRankConfig(bitrate_metric=RankBitrateMetric.MEDIAN)
        m = AudioQualityMeasurement(min_bitrate_kbps=260, format="MP3")
        # 260 ≥ default mp3_vbr.transparent=245 → TRANSPARENT
        self.assertEqual(measurement_rank(m, cfg_median), QualityRank.TRANSPARENT)


class TestCompareQuality(unittest.TestCase):
    """compare_quality() covers all four outcome branches explicitly."""

    def _m(self, **kwargs: Any) -> AudioQualityMeasurement:
        return AudioQualityMeasurement(**kwargs)

    # (description, new_kwargs, existing_kwargs, expected)
    CASES = [
        # --- Different rank → trivial ---
        ("V0 beats V4",
         dict(format="mp3 v0", avg_bitrate_kbps=240),
         dict(format="mp3 v4", avg_bitrate_kbps=150),
         "better"),
        ("V4 loses to V0",
         dict(format="mp3 v4", avg_bitrate_kbps=150),
         dict(format="mp3 v0", avg_bitrate_kbps=240),
         "worse"),
        ("Opus 128 beats Opus 64",
         dict(format="opus 128", avg_bitrate_kbps=130),
         dict(format="opus 64",  avg_bitrate_kbps=60),
         "better"),

        # --- Same rank, different codec family → equivalent ---
        ("Opus 128 == MP3 V0",
         dict(format="opus 128", avg_bitrate_kbps=130),
         dict(format="mp3 v0",   avg_bitrate_kbps=240),
         "equivalent"),
        ("MP3 V0 == Opus 128 (reverse)",
         dict(format="mp3 v0",   avg_bitrate_kbps=240),
         dict(format="opus 128", avg_bitrate_kbps=130),
         "equivalent"),
        ("MP3 V0 == MP3 CBR 320",
         dict(format="mp3 v0",   avg_bitrate_kbps=240, is_cbr=False),
         dict(format="mp3 320",  avg_bitrate_kbps=320, is_cbr=True),
         "equivalent"),
        ("Opus 128 == AAC 192",
         dict(format="opus 128", avg_bitrate_kbps=130),
         dict(format="aac 192",  avg_bitrate_kbps=192),
         "equivalent"),

        # --- Same rank, same VBR label → equivalent regardless of bitrate ---
        ("lo-fi V0 == dense V0 (label rule)",
         dict(format="mp3 v0",   avg_bitrate_kbps=207),
         dict(format="mp3 v0",   avg_bitrate_kbps=245),
         "equivalent"),
        ("lo-fi V0 ≠ 'worse' even though 207 < 245",
         dict(format="mp3 v0",   avg_bitrate_kbps=207),
         dict(format="mp3 v0",   avg_bitrate_kbps=260),
         "equivalent"),

        # --- Same rank, same bare codec family, measurable bitrate ---
        # Default mp3_vbr bands: transparent=245, excellent=210
        ("bare MP3 260 > MP3 250 (same rank TRANSPARENT)",
         dict(format="MP3", avg_bitrate_kbps=260),
         dict(format="MP3", avg_bitrate_kbps=250),
         "better"),
        ("bare MP3 250 < MP3 260 (same rank)",
         dict(format="MP3", avg_bitrate_kbps=250),
         dict(format="MP3", avg_bitrate_kbps=260),
         "worse"),
        ("bare MP3 within tolerance → equivalent",
         dict(format="MP3", avg_bitrate_kbps=257),
         dict(format="MP3", avg_bitrate_kbps=260),
         "equivalent"),
        ("bare Opus 130 == Opus 128 within tolerance",
         dict(format="Opus", avg_bitrate_kbps=130),
         dict(format="Opus", avg_bitrate_kbps=128),
         "equivalent"),

        # --- Unknown measurements fall through ---
        ("both unknown format",
         dict(format=None, avg_bitrate_kbps=None),
         dict(format=None, avg_bitrate_kbps=None),
         "equivalent"),
        ("bare MP3 both None bitrate → equivalent guard",
         dict(format="MP3"),
         dict(format="MP3"),
         "equivalent"),
        ("bare Opus both None bitrate → equivalent guard",
         dict(format="Opus"),
         dict(format="Opus"),
         "equivalent"),

        # --- Lossless beats anything else ---
        ("FLAC beats MP3 V0",
         dict(format="FLAC", avg_bitrate_kbps=900),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         "better"),
        ("MP3 V0 loses to FLAC",
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         dict(format="FLAC", avg_bitrate_kbps=900),
         "worse"),
        ("FLAC == FLAC",
         dict(format="FLAC", avg_bitrate_kbps=900),
         dict(format="FLAC", avg_bitrate_kbps=1100),
         "equivalent"),
    ]

    def test_compare_quality_table(self):
        for desc, new_kw, existing_kw, expected in self.CASES:
            with self.subTest(desc=desc):
                result = compare_quality(
                    self._m(**new_kw), self._m(**existing_kw), CFG)
                self.assertEqual(
                    result, expected,
                    f"{desc}: new={new_kw} existing={existing_kw} "
                    f"expected {expected!r} got {result!r}")

    def test_min_metric_honored_in_comparison(self):
        """When cfg uses MIN, compare_quality must use min not avg."""
        cfg_min = QualityRankConfig(bitrate_metric=RankBitrateMetric.MIN)
        new = self._m(format="MP3", min_bitrate_kbps=240, avg_bitrate_kbps=250)
        existing = self._m(format="MP3", min_bitrate_kbps=210, avg_bitrate_kbps=260)
        # Under MIN: new=240, existing=210 → better
        self.assertEqual(compare_quality(new, existing, cfg_min), "better")
        # Under AVG: new=250, existing=260 → worse
        self.assertEqual(compare_quality(new, existing, CFG), "worse")

    def test_median_metric_honored_in_comparison(self):
        """When cfg uses MEDIAN, compare_quality must use median not avg/min.

        Issue #64: outlier-resistant comparisons. The new album has one
        very quiet interlude (min=60) but every other track sits above the
        existing album's median. Under MIN it would lose; under MEDIAN it
        wins because the typical track is better.
        """
        cfg_med = QualityRankConfig(bitrate_metric=RankBitrateMetric.MEDIAN)
        new = self._m(format="MP3",
                      min_bitrate_kbps=60, avg_bitrate_kbps=240,
                      median_bitrate_kbps=255)
        existing = self._m(format="MP3",
                           min_bitrate_kbps=210, avg_bitrate_kbps=215,
                           median_bitrate_kbps=215)
        # Under MEDIAN: new=255 (TRANSPARENT) vs existing=215 (EXCELLENT) → better
        self.assertEqual(compare_quality(new, existing, cfg_med), "better")
        # Under MIN: new=60 (POOR) vs existing=210 (EXCELLENT) → worse
        cfg_min = QualityRankConfig(bitrate_metric=RankBitrateMetric.MIN)
        self.assertEqual(compare_quality(new, existing, cfg_min), "worse")


class TestQualityRankConfigFromIni(unittest.TestCase):
    """Parse [Quality Ranks] section from config.ini — exhaustive edge cases."""

    def _parse(self, ini_body: str) -> QualityRankConfig:
        import configparser
        parser = configparser.RawConfigParser()
        parser.read_string(ini_body)
        return QualityRankConfig.from_ini(parser)

    def test_missing_section_returns_defaults(self):
        cfg = self._parse("[Other Section]\nkey = value\n")
        self.assertEqual(cfg, QualityRankConfig.defaults())

    def test_empty_section_returns_defaults(self):
        cfg = self._parse("[Quality Ranks]\n")
        self.assertEqual(cfg, QualityRankConfig.defaults())

    def test_partial_override_one_band(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "opus.transparent = 120\n"
        )
        self.assertEqual(cfg.opus.transparent, 120)
        # All other opus values stay at default
        self.assertEqual(cfg.opus.excellent, 88)
        self.assertEqual(cfg.opus.good, 64)
        self.assertEqual(cfg.opus.acceptable, 48)
        # And other codecs untouched
        self.assertEqual(cfg.mp3_vbr, QualityRankConfig.defaults().mp3_vbr)

    def test_full_override(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "bitrate_metric = min\n"
            "gate_min_rank = good\n"
            "within_rank_tolerance_kbps = 10\n"
            "opus.transparent = 120\n"
            "opus.excellent = 100\n"
            "opus.good = 80\n"
            "opus.acceptable = 60\n"
            "mp3_vbr.transparent = 220\n"
            "mp3_vbr.excellent = 180\n"
            "mp3_vbr.good = 140\n"
            "mp3_vbr.acceptable = 100\n"
            "mp3_cbr.transparent = 320\n"
            "mp3_cbr.excellent = 250\n"
            "mp3_cbr.good = 200\n"
            "mp3_cbr.acceptable = 130\n"
            "aac.transparent = 200\n"
            "aac.excellent = 150\n"
            "aac.good = 120\n"
            "aac.acceptable = 90\n"
        )
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.MIN)
        self.assertEqual(cfg.gate_min_rank, QualityRank.GOOD)
        self.assertEqual(cfg.within_rank_tolerance_kbps, 10)
        self.assertEqual(cfg.opus.transparent, 120)
        self.assertEqual(cfg.mp3_vbr.transparent, 220)
        self.assertEqual(cfg.mp3_cbr.excellent, 250)
        self.assertEqual(cfg.aac.acceptable, 90)

    def test_invalid_metric_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse("[Quality Ranks]\nbitrate_metric = harmonic_mean\n")
        self.assertIn("bitrate_metric", str(ctx.exception))

    def test_median_metric_parses(self):
        """`bitrate_metric = median` is a valid policy (issue #64)."""
        cfg = self._parse("[Quality Ranks]\nbitrate_metric = median\n")
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.MEDIAN)

    def test_invalid_rank_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse("[Quality Ranks]\ngate_min_rank = perfect\n")
        self.assertIn("gate_min_rank", str(ctx.exception))

    def test_non_integer_band_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse("[Quality Ranks]\nopus.transparent = not_a_number\n")
        self.assertIn("opus.transparent", str(ctx.exception))

    def test_non_monotonic_bands_raise(self):
        with self.assertRaises(ValueError):
            self._parse(
                "[Quality Ranks]\n"
                "opus.transparent = 50\n"
                "opus.excellent = 100\n"
            )

    def test_negative_tolerance_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse("[Quality Ranks]\nwithin_rank_tolerance_kbps = -3\n")
        self.assertIn("within_rank_tolerance_kbps", str(ctx.exception))

    def test_case_insensitive_metric_and_rank(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "bitrate_metric = AVG\n"
            "gate_min_rank = TRANSPARENT\n"
        )
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.AVG)
        self.assertEqual(cfg.gate_min_rank, QualityRank.TRANSPARENT)

    def test_empty_value_falls_through_to_default(self):
        """Empty `key =` should yield the default, matching _get_int behavior."""
        cfg = self._parse(
            "[Quality Ranks]\n"
            "bitrate_metric = \n"
            "gate_min_rank =    \n"
        )
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.AVG)
        self.assertEqual(cfg.gate_min_rank, QualityRank.EXCELLENT)

    def test_repo_config_ini_parses_cleanly(self):
        """The in-repo config.ini template must parse to a valid QualityRankConfig."""
        import configparser
        import os
        parser = configparser.RawConfigParser()
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        parser.read(os.path.join(repo_root, "config.ini"))
        cfg = QualityRankConfig.from_ini(parser)
        # Repo template uses the defaults — assert round-trip equality.
        self.assertEqual(cfg, QualityRankConfig.defaults())

    # ---- Issue #65: collection field parsing -----------------------------
    # mp3_vbr_levels / lossless_codecs / mixed_format_precedence are now
    # parseable from [Quality Ranks] as comma-separated values.

    def test_mp3_vbr_levels_parses_full_override(self):
        """All 10 V-level entries (V0..V9) must parse into the tuple."""
        cfg = self._parse(
            "[Quality Ranks]\n"
            "mp3_vbr_levels = TRANSPARENT,EXCELLENT,EXCELLENT,GOOD,GOOD,"
            "ACCEPTABLE,ACCEPTABLE,POOR,POOR,POOR\n"
        )
        self.assertEqual(len(cfg.mp3_vbr_levels), 10)
        self.assertEqual(cfg.mp3_vbr_levels[0], QualityRank.TRANSPARENT)
        self.assertEqual(cfg.mp3_vbr_levels[2], QualityRank.EXCELLENT)
        self.assertEqual(cfg.mp3_vbr_levels[7], QualityRank.POOR)

    def test_mp3_vbr_levels_case_insensitive_and_whitespace_tolerant(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "mp3_vbr_levels =  transparent , Excellent ,EXCELLENT, good,good,"
            " acceptable,ACCEPTABLE,acceptable,Acceptable,acceptable\n"
        )
        self.assertEqual(cfg.mp3_vbr_levels,
                         QualityRankConfig.defaults().mp3_vbr_levels)

    def test_mp3_vbr_levels_wrong_length_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse(
                "[Quality Ranks]\n"
                "mp3_vbr_levels = TRANSPARENT,EXCELLENT,GOOD\n"
            )
        self.assertIn("mp3_vbr_levels", str(ctx.exception))
        self.assertIn("10", str(ctx.exception))

    def test_mp3_vbr_levels_invalid_rank_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse(
                "[Quality Ranks]\n"
                "mp3_vbr_levels = TRANSPARENT,EXCELLENT,EXCELLENT,GOOD,GOOD,"
                "ACCEPTABLE,ACCEPTABLE,ACCEPTABLE,WHOOPS,ACCEPTABLE\n"
            )
        self.assertIn("mp3_vbr_levels", str(ctx.exception))
        self.assertIn("whoops", str(ctx.exception).lower())

    def test_lossless_codecs_parses(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "lossless_codecs = flac,alac,wav,ape,wavpack\n"
        )
        self.assertEqual(cfg.lossless_codecs,
                         frozenset({"flac", "alac", "wav", "ape", "wavpack"}))

    def test_lossless_codecs_lowercased_and_deduped(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "lossless_codecs = FLAC, Alac , wav , flac\n"
        )
        self.assertEqual(cfg.lossless_codecs,
                         frozenset({"flac", "alac", "wav"}))

    def test_lossless_codecs_empty_falls_through_to_default(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "lossless_codecs = \n"
        )
        self.assertEqual(cfg.lossless_codecs,
                         QualityRankConfig.defaults().lossless_codecs)

    def test_lossless_codecs_empty_list_raises(self):
        """An explicit empty list (just whitespace/commas) is a config error.

        Distinct from `key = ` (which means "use the default") because here
        the user is clearly trying to set the field but produced no values.
        """
        with self.assertRaises(ValueError) as ctx:
            self._parse(
                "[Quality Ranks]\n"
                "lossless_codecs = , ,\n"
            )
        self.assertIn("lossless_codecs", str(ctx.exception))

    def test_mixed_format_precedence_parses_and_preserves_order(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "mixed_format_precedence = aac, opus, mp3, flac\n"
        )
        # Order matters — the first match wins in _reduce_album_format.
        self.assertEqual(
            cfg.mixed_format_precedence, ("aac", "opus", "mp3", "flac"))

    def test_mixed_format_precedence_lowercased(self):
        cfg = self._parse(
            "[Quality Ranks]\n"
            "mixed_format_precedence = MP3,AAC,Opus,FLAC\n"
        )
        self.assertEqual(
            cfg.mixed_format_precedence, ("mp3", "aac", "opus", "flac"))

    def test_collection_partial_override(self):
        """Setting only one collection field leaves the others at defaults."""
        cfg = self._parse(
            "[Quality Ranks]\n"
            "lossless_codecs = flac,alac,ape\n"
        )
        defaults = QualityRankConfig.defaults()
        self.assertEqual(cfg.lossless_codecs,
                         frozenset({"flac", "alac", "ape"}))
        self.assertEqual(cfg.mp3_vbr_levels, defaults.mp3_vbr_levels)
        self.assertEqual(cfg.mixed_format_precedence,
                         defaults.mixed_format_precedence)

    def test_collection_full_override_round_trips_through_from_ini(self):
        """End-to-end: set all three collection fields and verify each one."""
        cfg = self._parse(
            "[Quality Ranks]\n"
            "mp3_vbr_levels = EXCELLENT,EXCELLENT,GOOD,GOOD,ACCEPTABLE,"
            "ACCEPTABLE,POOR,POOR,POOR,POOR\n"
            "lossless_codecs = flac,alac,wav,ape,dsf,wavpack\n"
            "mixed_format_precedence = aac,mp3,opus,flac\n"
        )
        self.assertEqual(cfg.mp3_vbr_levels[0], QualityRank.EXCELLENT)
        self.assertEqual(cfg.mp3_vbr_levels[6], QualityRank.POOR)
        self.assertIn("ape", cfg.lossless_codecs)
        self.assertEqual(
            cfg.mixed_format_precedence, ("aac", "mp3", "opus", "flac"))


class TestQualityRankConfigRoundTrip(unittest.TestCase):
    """to_json / from_json must round-trip identically."""

    def test_defaults_round_trip(self):
        original = QualityRankConfig.defaults()
        restored = QualityRankConfig.from_json(original.to_json())
        self.assertEqual(restored, original)

    def test_custom_round_trip(self):
        original = QualityRankConfig(
            bitrate_metric=RankBitrateMetric.MIN,
            gate_min_rank=QualityRank.TRANSPARENT,
            within_rank_tolerance_kbps=8,
            opus=CodecRankBands(transparent=120, excellent=100, good=80, acceptable=60),
        )
        payload = original.to_json()
        restored = QualityRankConfig.from_json(payload)
        self.assertEqual(restored, original)
        self.assertEqual(restored.opus.transparent, 120)

    def test_median_metric_round_trip(self):
        """RankBitrateMetric.MEDIAN survives the harness argv round-trip."""
        original = QualityRankConfig(bitrate_metric=RankBitrateMetric.MEDIAN)
        restored = QualityRankConfig.from_json(original.to_json())
        self.assertEqual(restored.bitrate_metric, RankBitrateMetric.MEDIAN)

    def test_json_shape_stable(self):
        """to_json() must emit the expected top-level keys."""
        import json
        payload = json.loads(QualityRankConfig.defaults().to_json())
        expected_keys = {
            "bitrate_metric", "gate_min_rank", "within_rank_tolerance_kbps",
            "opus", "mp3_vbr", "mp3_cbr", "aac",
            "mp3_vbr_levels", "lossless_codecs", "mixed_format_precedence",
        }
        self.assertEqual(set(payload.keys()), expected_keys)

    def test_json_rank_is_int(self):
        payload = json.loads(QualityRankConfig.defaults().to_json())
        self.assertIsInstance(payload["gate_min_rank"], int)
        for r in payload["mp3_vbr_levels"]:
            self.assertIsInstance(r, int)

    def test_custom_collections_round_trip(self):
        """Non-default mp3_vbr_levels / lossless_codecs / mixed_format_precedence
        survive JSON round-trip unchanged."""
        original = QualityRankConfig(
            mp3_vbr_levels=(
                QualityRank.EXCELLENT, QualityRank.GOOD, QualityRank.GOOD,
                QualityRank.ACCEPTABLE, QualityRank.ACCEPTABLE, QualityRank.POOR,
                QualityRank.POOR, QualityRank.POOR, QualityRank.POOR,
                QualityRank.POOR,
            ),
            lossless_codecs=frozenset({"flac", "ape", "dsf", "wavpack"}),
            mixed_format_precedence=("opus", "mp3", "flac"),
        )
        restored = QualityRankConfig.from_json(original.to_json())
        self.assertEqual(restored, original)
        self.assertEqual(restored.mp3_vbr_levels[0], QualityRank.EXCELLENT)
        self.assertIn("ape", restored.lossless_codecs)
        self.assertEqual(restored.mixed_format_precedence, ("opus", "mp3", "flac"))

    def test_from_json_invalid_json_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            QualityRankConfig.from_json("not valid json {")
        self.assertIn("invalid JSON", str(ctx.exception))

    def test_from_json_missing_key_raises_value_error(self):
        """Missing keys produce a value error, not a bare KeyError."""
        raw = '{"bitrate_metric": "avg"}'
        with self.assertRaises(ValueError) as ctx:
            QualityRankConfig.from_json(raw)
        self.assertIn("failed to reconstruct", str(ctx.exception))

    def test_from_json_invalid_rank_int_raises_value_error(self):
        """Out-of-range QualityRank ints raise ValueError, not blow up."""
        import json as _json
        payload = _json.loads(QualityRankConfig.defaults().to_json())
        payload["gate_min_rank"] = 9999  # invalid enum value
        with self.assertRaises(ValueError) as ctx:
            QualityRankConfig.from_json(_json.dumps(payload))
        self.assertIn("failed to reconstruct", str(ctx.exception))


class TestQualityRankConfigDefaults(unittest.TestCase):
    """Lock the default policy values so changes are explicit.

    **These values are mirrored in the Nix module at**
    ``nixosconfig/modules/nixos/services/soularr.nix`` →
    ``homelab.services.soularr.qualityRanks.*`` (see README §
    "Tuning the quality rank model" for the deployment surface and
    issue #67 for the rationale). If you change a default here, you
    MUST also update the Nix defaults, otherwise a fresh
    ``nixos-rebuild switch`` will revert the change on doc2.

    This class is the single source of truth for what
    ``QualityRankConfig.defaults()`` returns. The Nix mirror is a
    convenience for declarative visibility (you shouldn't have to open
    a Python dataclass to read your production config) — but Python
    stays authoritative. These pin tests fail loudly on any drift so
    the discrepancy surfaces before anyone ships with mismatched
    defaults.
    """

    def test_default_metric_is_avg(self):
        self.assertEqual(CFG.bitrate_metric, RankBitrateMetric.AVG)

    def test_default_gate_min_rank_is_excellent(self):
        self.assertEqual(CFG.gate_min_rank, QualityRank.EXCELLENT)

    def test_default_within_rank_tolerance(self):
        self.assertEqual(CFG.within_rank_tolerance_kbps, 5)

    def test_default_lossless_codecs(self):
        self.assertEqual(
            CFG.lossless_codecs,
            frozenset({"flac", "lossless", "alac", "wav"}))

    def test_default_mixed_format_precedence_worst_first(self):
        # MP3 is the "worst" (least trustworthy cross-codec) so it wins ties.
        self.assertEqual(CFG.mixed_format_precedence, ("mp3", "aac", "opus", "flac"))

    def test_default_mp3_vbr_levels_length_is_ten(self):
        self.assertEqual(len(CFG.mp3_vbr_levels), 10)

    def test_default_mp3_vbr_levels_full_tuple(self):
        """Pin the complete LAME V-level ladder — documented in README
        and mirrored in docs/quality-ranks.md band defaults example."""
        self.assertEqual(CFG.mp3_vbr_levels, (
            QualityRank.TRANSPARENT,  # V0
            QualityRank.EXCELLENT,    # V1
            QualityRank.EXCELLENT,    # V2
            QualityRank.GOOD,         # V3
            QualityRank.GOOD,         # V4
            QualityRank.ACCEPTABLE,   # V5
            QualityRank.ACCEPTABLE,   # V6
            QualityRank.ACCEPTABLE,   # V7
            QualityRank.ACCEPTABLE,   # V8
            QualityRank.ACCEPTABLE,   # V9
        ))

    def test_default_mp3_v0_is_transparent(self):
        self.assertEqual(CFG.mp3_vbr_levels[0], QualityRank.TRANSPARENT)

    def test_default_opus_bands(self):
        self.assertEqual(CFG.opus.transparent, 112)
        self.assertEqual(CFG.opus.excellent, 88)
        self.assertEqual(CFG.opus.good, 64)
        self.assertEqual(CFG.opus.acceptable, 48)

    def test_default_mp3_vbr_bands(self):
        """Legacy QUALITY_MIN_BITRATE_KBPS=210 is preserved at ``excellent``
        — see docs/quality-ranks.md and the
        test_default_constant_matches_default_cfg_mp3_vbr_excellent pin."""
        self.assertEqual(CFG.mp3_vbr.transparent, 245)
        self.assertEqual(CFG.mp3_vbr.excellent, 210)
        self.assertEqual(CFG.mp3_vbr.good, 170)
        self.assertEqual(CFG.mp3_vbr.acceptable, 130)

    def test_default_mp3_cbr_bands(self):
        """Unverifiable CBR is only transparent at 320 — we can't prove
        a CBR file came from lossless source."""
        self.assertEqual(CFG.mp3_cbr.transparent, 320)
        self.assertEqual(CFG.mp3_cbr.excellent, 256)
        self.assertEqual(CFG.mp3_cbr.good, 192)
        self.assertEqual(CFG.mp3_cbr.acceptable, 128)

    def test_default_aac_bands(self):
        """Hydrogenaudio consensus places the music quality ceiling at 192."""
        self.assertEqual(CFG.aac.transparent, 192)
        self.assertEqual(CFG.aac.excellent, 144)
        self.assertEqual(CFG.aac.good, 112)
        self.assertEqual(CFG.aac.acceptable, 80)


# ============================================================================
# detect_release_source
# ============================================================================

class TestDetectReleaseSource(unittest.TestCase):
    """Test source detection from release ID format."""

    CASES = [
        # desc, id_string, expected
        ("MB UUID", "89ad4ac3-39f7-470e-963a-56509c546377", "musicbrainz"),
        ("MB UUID uppercase", "89AD4AC3-39F7-470E-963A-56509C546377", "musicbrainz"),
        ("Discogs numeric", "2048516", "discogs"),
        ("Discogs large numeric", "13524141", "discogs"),
        ("Discogs single digit", "1", "discogs"),
        ("empty string", "", "unknown"),
        ("NONE string", "NONE", "unknown"),
        ("random text", "not-a-valid-id", "unknown"),
        ("partial UUID no hyphens", "89ad4ac339f7470e963a56509c546377", "unknown"),
    ]

    def test_detect_release_source(self):
        from lib.quality import detect_release_source
        for desc, id_string, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(detect_release_source(id_string), expected)


if __name__ == "__main__":
    unittest.main()
