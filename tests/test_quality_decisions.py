#!/usr/bin/env python3
"""Unit tests for the lib/quality/ pure decision functions.

These test every branch of the four decision functions directly,
independent of real audio fixtures or the full_pipeline_decision integrator.
"""

import json
import os
import sys
import unittest
from datetime import datetime, timezone
from typing import Any

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import (
    spectral_import_decision,
    import_quality_decision,
    transcode_detection,
    quality_gate_decision,
    is_verified_lossless,
    AudioQualityMeasurement,
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    DownloadInfo,
    rejected_download_tier,
    narrow_override_on_downgrade,
    resolve_rejection_search_override,
    # Codec-aware rank model (issue #60)
    QualityRank,
    RankBitrateMetric,
    CodecRankBands,
    QualityRankConfig,
    quality_rank,
    measurement_rank,
    gate_rank,
    compare_quality,
    build_existing_quality_measurement,
    measured_import_decision,
    MeasuredImportDecisionInput,
    provisional_lossless_decision,
    ProvisionalLosslessDecisionInput,
    V0ProbeEvidence,
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    DECISION_LOSSLESS_SOURCE_LOCKED,
    VerifiedLosslessProof,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
)
from lib.quality.decisions import post_import_search_action


# ============================================================================
# spectral_import_decision
# ============================================================================

class TestSpectralGateTrigger(unittest.TestCase):
    """Test the pre-analysis "would spectral run?" decision (issue #93).

    Mirrors the live gate in lib.measurement._needs_spectral_check. Delivers
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
    """Test pre-import spectral decision (MP3/CBR path).

    Spectral compares to spectral evidence only — never to container bitrate.
    Absence of an existing spectral measurement means "we haven't measured it
    yet", not "it's genuine". The decision returns import_no_exist when only
    one side has spectral evidence.
    """

    CASES = [
        # desc, grade, bitrate, existing_spectral, expected
        ("genuine imports", "genuine", None, None, "import"),
        ("genuine ignores bitrates", "genuine", 128, 256, "import"),
        ("marginal imports", "marginal", 192, 256, "import"),
        ("marginal no bitrates", "marginal", None, None, "import"),
        ("suspect equal rejects", "suspect", 128, 128, "reject"),
        ("suspect worse rejects", "suspect", 96, 128, "reject"),
        ("likely transcode equal rejects", "likely_transcode", 160, 160, "reject"),
        ("suspect better upgrades", "suspect", 192, 128, "import_upgrade"),
        ("likely transcode better upgrades", "likely_transcode", 192, 96, "import_upgrade"),
        ("suspect no existing zero", "suspect", 128, 0, "import_no_exist"),
        ("suspect no existing none", "suspect", 128, None, "import_no_exist"),
        ("likely transcode no existing", "likely_transcode", 96, None, "import_no_exist"),
        ("suspect no new no existing", "suspect", None, None, "import_no_exist"),
        ("suspect no new with existing", "suspect", None, 128, "import"),
        ("no spectral or container existing", "likely_transcode", 96, None, "import_no_exist"),
    ]

    def test_spectral_import_decisions(self):
        for desc, grade, bitrate, existing_spectral, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    spectral_import_decision(
                        grade,
                        bitrate,
                        existing_spectral,
                    ),
                    expected,
                )

    def test_signature_has_no_container_fallback(self):
        """Invariant: spectral_import_decision must compare spectral to
        spectral. The signature must not accept ``existing_min_bitrate`` —
        that footgun produces cross-evidence comparisons (e.g. a 160kbps
        spectral cliff being rejected by a 320kbps MP3 container nominal
        bitrate with no spectral measured)."""
        import inspect
        sig = inspect.signature(spectral_import_decision)
        self.assertNotIn(
            "existing_min_bitrate", sig.parameters,
            "spectral_import_decision must not accept a container-bitrate "
            "fallback parameter — see lib/quality/decisions.py for the rationale")


# ============================================================================
# U11: ``preimport_decide`` was folded into ``full_pipeline_decision_from_evidence``.
# Its folder/audio-integrity reject branches (audio_corrupt, bad_audio_hash,
# nested_layout, empty_fileset, mixed_source) are now early-exit rejects
# at the top of the unified decider. The pure-branch coverage lives in
# ``tests/test_quality_classification.py::TestPreimportFactRejects`` and
# the parity contract in
# ``TestLiveBugReproductionsThroughEvidencePipeline``.
# ============================================================================


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
         dict(format="opus 128", avg_bitrate_kbps=130, verified_lossless_proof=True),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "import"),
        ("FLAC→Opus 128 equivalent to MP3 CBR 320 + verified_lossless → import",
         dict(format="opus 128", avg_bitrate_kbps=130, verified_lossless_proof=True),
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True),
         False, "import"),

        # --- verified_lossless guardrail (core #60 fix) ---
        ("Opus 64 verified CANNOT replace MP3 V0 245",
         dict(format="opus 64", avg_bitrate_kbps=64, verified_lossless_proof=True),
         dict(format="mp3 v0", avg_bitrate_kbps=245),
         False, "downgrade"),
        ("Opus 48 verified CANNOT replace MP3 CBR 320",
         dict(format="opus 48", avg_bitrate_kbps=48, verified_lossless_proof=True),
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True),
         False, "downgrade"),

        # --- Lo-fi genuine V0 (label semantics preserved) ---
        ("lo-fi V0 (207) equivalent to dense V0 (245) + verified → import",
         dict(format="mp3 v0", avg_bitrate_kbps=207, verified_lossless_proof=True),
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
                measurement_kwargs = dict(new_kwargs)
                proof = bool(
                    measurement_kwargs.pop("verified_lossless_proof", False)
                )
                new = AudioQualityMeasurement(**measurement_kwargs)
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
                        verified_lossless_proof=proof,
                    ).decision,
                    expected,
                    f"{desc}: new={new_kwargs} existing={existing_kwargs} "
                    f"is_transcode={is_transcode} expected {expected!r}")


class TestMeasuredImportDecision(unittest.TestCase):
    """Shared reducer used by simulator, preview, and harness."""

    def test_reducer_matches_import_quality_decision(self):
        new = AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=245,
            median_bitrate_kbps=245,
            format="mp3 v0",
        )
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=192,
            avg_bitrate_kbps=192,
            median_bitrate_kbps=192,
            format="mp3 192",
            is_cbr=True,
        )

        result = measured_import_decision(
            MeasuredImportDecisionInput(new, existing)
        )

        self.assertEqual(result.decision, "import")
        self.assertTrue(result.would_import)
        self.assertFalse(result.confident_reject)
        self.assertEqual(result.stage_chain, ["stage2_import:import"])

    def test_reducer_classifies_terminal_rejects_for_preview_cleanup(self):
        new = AudioQualityMeasurement(
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            median_bitrate_kbps=128,
            format="mp3 128",
            is_cbr=True,
        )
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=245,
            median_bitrate_kbps=245,
            format="mp3 v0",
        )

        result = measured_import_decision(
            MeasuredImportDecisionInput(new, existing)
        )

        self.assertEqual(result.decision, "downgrade")
        self.assertEqual(result.exit_code, 5)
        self.assertTrue(result.confident_reject)
        self.assertTrue(result.cleanup_eligible)

    def test_existing_measurement_clamps_cbr_but_preserves_vbr_average(self):
        cbr = build_existing_quality_measurement(
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            override_min_bitrate=96,
        )
        vbr = build_existing_quality_measurement(
            min_bitrate_kbps=152,
            avg_bitrate_kbps=225,
            median_bitrate_kbps=230,
            format="MP3",
            is_cbr=False,
            override_min_bitrate=96,
        )

        self.assertIsNotNone(cbr)
        assert cbr is not None
        self.assertEqual(cbr.min_bitrate_kbps, 96)
        self.assertEqual(cbr.avg_bitrate_kbps, 96)
        self.assertEqual(cbr.median_bitrate_kbps, 96)
        self.assertIsNotNone(vbr)
        assert vbr is not None
        self.assertEqual(vbr.min_bitrate_kbps, 96)
        self.assertEqual(vbr.avg_bitrate_kbps, 225)
        self.assertEqual(vbr.median_bitrate_kbps, 230)


class TestProvisionalLosslessDecision(unittest.TestCase):
    """Suspect lossless-source V0 probe grind-up policy."""

    def _probe(
        self,
        avg: int | None,
        kind: str = "lossless_source_v0",
    ) -> V0ProbeEvidence:
        return V0ProbeEvidence(
            kind=kind,
            min_bitrate_kbps=avg - 10 if avg is not None else None,
            avg_bitrate_kbps=avg,
            median_bitrate_kbps=avg + 1 if avg is not None else None,
        )

    def _decide(
        self,
        *,
        avg: int | None = 250,
        existing: V0ProbeEvidence | None = None,
        grade: str = "suspect",
        supported: bool = True,
        kind: str = "lossless_source_v0",
        tolerance: int = 5,
    ) -> Any:
        cfg = QualityRankConfig(within_rank_tolerance_kbps=tolerance)
        return provisional_lossless_decision(
            ProvisionalLosslessDecisionInput(
                candidate_probe=self._probe(avg, kind=kind) if avg is not None else None,
                existing_probe=existing,
                spectral_grade=grade,
                supported_lossless_source=supported,
            ),
            cfg=cfg,
        )

    def test_missing_existing_probe_imports_provisionally(self):
        result = self._decide(avg=250, existing=None)
        self.assertEqual(result.decision, DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        self.assertTrue(result.would_import)
        self.assertFalse(result.confident_reject)

    def test_candidate_beating_existing_probe_by_tolerance_imports(self):
        result = self._decide(avg=228, existing=self._probe(171))
        self.assertEqual(result.decision, DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        self.assertTrue(result.would_import)
        self.assertIn("228", result.reason or "")
        self.assertIn("171", result.reason or "")

    def test_equal_or_within_tolerance_rejects_as_suspect_lossless_downgrade(self):
        for avg in (171, 175):
            with self.subTest(avg=avg):
                result = self._decide(avg=avg, existing=self._probe(171),
                                      tolerance=5)
                self.assertEqual(
                    result.decision,
                    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
                )
                self.assertTrue(result.confident_reject)
                self.assertTrue(result.cleanup_eligible)

    def test_above_tolerance_imports(self):
        result = self._decide(avg=177, existing=self._probe(171),
                              tolerance=5)
        self.assertEqual(result.decision, DECISION_PROVISIONAL_LOSSLESS_UPGRADE)

    def test_genuine_and_marginal_continue_existing_policy(self):
        for grade in ("genuine", "marginal"):
            with self.subTest(grade=grade):
                result = self._decide(avg=250, existing=self._probe(171),
                                      grade=grade)
                self.assertIsNone(result.decision)
                self.assertFalse(result.would_import)

    def test_only_shared_transcode_grades_enter_provisional_policy(self):
        """The provisional branch consumes the canonical grade set."""
        from lib.quality import SPECTRAL_TRANSCODE_GRADES

        for grade in SPECTRAL_TRANSCODE_GRADES:
            with self.subTest(grade=grade):
                self.assertEqual(
                    self._decide(grade=grade).decision,
                    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
                )
        for grade in ("genuine", "marginal", "error", "unknown"):
            with self.subTest(grade=grade):
                self.assertIsNone(self._decide(grade=grade).decision)

    def test_lossy_candidate_locks_when_existing_has_lossless_source_probe(self):
        # Message to Bears EP1 shape: existing on-disk is a transcoded opus
        # we made from a suspect FLAC, with the original lossless-source V0
        # probe (240kbps) recorded. A lossy candidate cannot produce a
        # comparable measurement, so it must be rejected outright.
        result = self._decide(avg=320, existing=self._probe(240),
                              supported=False)
        self.assertEqual(result.decision, DECISION_LOSSLESS_SOURCE_LOCKED)
        self.assertTrue(result.confident_reject)
        self.assertTrue(result.cleanup_eligible)
        self.assertIn("240kbps", result.reason or "")

    def test_lossy_candidate_locks_regardless_of_spectral_grade(self):
        # The lock is structural — a lossy candidate cannot be ground to V0
        # to compare against the recorded probe regardless of how clean its
        # own spectral looks. Grade is informational here, not load-bearing.
        for grade in ("genuine", "marginal", "suspect", "likely_transcode"):
            with self.subTest(grade=grade):
                result = self._decide(avg=320, existing=self._probe(240),
                                      supported=False, grade=grade)
                self.assertEqual(result.decision, DECISION_LOSSLESS_SOURCE_LOCKED)

    def test_lossy_candidate_passes_when_no_existing_probe(self):
        # No recorded lossless-source V0 probe means nothing to lock against —
        # the regular import_quality_decision path still runs.
        result = self._decide(avg=320, existing=None, supported=False)
        self.assertIsNone(result.decision)

    def test_native_lossy_research_probe_is_not_comparable(self):
        # Research probes (kind=native_lossy_research_v0) are audit-only and
        # must not trigger any provisional decision branch — neither upgrade
        # nor suspect_lossless_probe_missing — regardless of which side
        # carries the research kind.
        for grade in ("suspect", "likely_transcode", "genuine"):
            with self.subTest(side="candidate", grade=grade):
                result = self._decide(
                    avg=200, existing=self._probe(171),
                    grade=grade, supported=True,
                    kind="native_lossy_research_v0")
                self.assertNotEqual(
                    result.decision, DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        result = self._decide(
            avg=200,
            existing=V0ProbeEvidence(
                kind="native_lossy_research_v0",
                min_bitrate_kbps=160, avg_bitrate_kbps=171,
                median_bitrate_kbps=175),
            grade="suspect", supported=True)
        # Existing-side research probe → treated as if no comparable existing
        # probe exists, so suspect_lossless_probe_missing path applies.
        self.assertNotEqual(
            result.decision, DECISION_SUSPECT_LOSSLESS_DOWNGRADE)

    def test_supported_lossless_source_bypasses_lock(self):
        # The lock fires only on lossy candidates. A FLAC candidate facing
        # an existing lossless-source V0 probe must still be eligible to
        # override via the normal V0 grind-up comparison — never short-
        # circuit to lossless_source_locked. Guards against future refactors
        # that move the lock above the supported_lossless_source check.
        result = self._decide(avg=320, supported=True, grade="suspect",
                              existing=self._probe(240))
        self.assertNotEqual(result.decision, DECISION_LOSSLESS_SOURCE_LOCKED)

    def test_lossy_candidate_passes_when_existing_probe_is_research_only(self):
        # Only lossless_source_v0 probes are load-bearing evidence; on-disk
        # research probes don't lock. is_comparable_lossless_source_probe
        # is the single source of truth for what counts.
        result = self._decide(avg=320, supported=False,
                              existing=self._probe(300,
                                                   kind="on_disk_research_v0"))
        self.assertIsNone(result.decision)

    def test_research_existing_probe_is_not_comparable(self):
        # FLAC-side: a research-kind existing probe is not comparable, so
        # we treat existing as absent and import provisionally.
        result = self._decide(
            avg=250,
            existing=self._probe(300, kind="on_disk_research_v0"),
        )
        self.assertEqual(result.decision, DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        self.assertEqual(
            result.reason,
            "no existing comparable lossless-source V0 probe",
        )

    def test_missing_candidate_probe_rejects_distinctly(self):
        result = self._decide(avg=None, existing=self._probe(171))
        self.assertEqual(result.decision, DECISION_SUSPECT_LOSSLESS_PROBE_MISSING)
        self.assertTrue(result.confident_reject)


# ============================================================================
# transcode_detection
# ============================================================================

class TestTranscodeDetection(unittest.TestCase):
    """Test post-conversion transcode detection."""

    CASES = [
        # desc, converted_count, spectral_grade, expected
        ("no conversion", 0, None, False),
        ("affirmative genuine proof", 12, "genuine", False),
        ("affirmative marginal proof", 12, "marginal", False),
        ("suspect", 12, "suspect", True),
        ("likely transcode", 12, "likely_transcode", True),
        ("analysis error aborts", 12, "error", True),
        ("missing analysis aborts", 12, None, True),
        ("no conversion beats spectral", 0, "suspect", False),
    ]

    def test_transcode_detection_cases(self):
        for desc, converted_count, spectral_grade, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    transcode_detection(
                        converted_count,
                        spectral_grade=spectral_grade,
                    ),
                    expected,
                )

    def test_removed_bitrate_argument_is_rejected(self):
        """The deleted bitrate fallback cannot survive as a positional shim."""
        with self.assertRaises(TypeError):
            transcode_detection(12, 240)  # type: ignore[call-arg]

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

        # --- unverified retained copies stay wanted on full tiers ---
        ("MP3 V0 label lo-fi stays searchable without proof",
         dict(format="mp3 v0", avg_bitrate_kbps=207), "requeue_upgrade"),
        ("MP3 V0 label dense",
         dict(format="mp3 v0", avg_bitrate_kbps=245), "requeue_upgrade"),
        # --- verified-lossless proof is absolute, regardless of target rank ---
        ("Opus 128 verified lossless",
         dict(format="opus 128", avg_bitrate_kbps=130, verified_lossless_proof=True), "accept"),
        ("Opus 128 not verified stays searchable",
         dict(format="opus 128", avg_bitrate_kbps=130), "requeue_upgrade"),
        ("bare MP3 VBR above rank",
         dict(format="MP3", avg_bitrate_kbps=240, is_cbr=False), "requeue_upgrade"),

        # --- unverified copies remain nonterminal at every rank ---
        ("bare MP3 VBR below rank",
         dict(format="MP3", avg_bitrate_kbps=150, is_cbr=False), "requeue_upgrade"),
        ("Opus 64 verified (target too low)",
         dict(format="opus 64", avg_bitrate_kbps=64, verified_lossless_proof=True), "accept"),
        ("Opus 48 verified (target far too low)",
         dict(format="opus 48", avg_bitrate_kbps=48, verified_lossless_proof=True), "accept"),
        ("spectral clamp pulls CBR 320 down",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True,
              spectral_bitrate_kbps=128), "requeue_upgrade"),
        ("no format no bitrate → UNKNOWN",
         dict(), "requeue_upgrade"),

        # --- lossless narrowing requires transparent + genuine evidence ---
        ("CBR 320 unmeasured stays on full tiers",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True), "requeue_upgrade"),
        ("bare MP3 CBR 320 unmeasured stays on full tiers",
         dict(format="MP3", avg_bitrate_kbps=320, is_cbr=True), "requeue_upgrade"),
        ("bare MP3 CBR 256 unmeasured stays on full tiers",
         dict(format="MP3", avg_bitrate_kbps=256, is_cbr=True), "requeue_upgrade"),
        ("CBR 320 genuine narrows to lossless",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True,
              spectral_grade="genuine", spectral_subject="installed",
              spectral_provenance="measured"), "requeue_lossless"),
        ("CBR 320 genuine source-subject grade also narrows (D17)",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True,
              spectral_grade="genuine", spectral_subject="source",
              spectral_provenance="carried"), "requeue_lossless"),
        ("CBR 320 suspect stays on full tiers",
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True,
              spectral_grade="suspect", spectral_bitrate_kbps=192), "requeue_upgrade"),

        # A lossless container is not itself verified-lossless proof.
        ("unverified FLAC stays searchable",
         dict(format="FLAC", avg_bitrate_kbps=900), "requeue_upgrade"),
        ("unverified lossless label stays searchable",
         dict(format="flac"), "requeue_upgrade"),

        # --- legacy verified_lossless cases (still honoured via label if present) ---
        ("legacy no format with verified-lossless proof accepts",
         dict(min_bitrate_kbps=180, verified_lossless_proof=True), "accept"),
    ]

    def test_quality_gate_decisions(self):
        for desc, kwargs, expected in self.CASES:
            with self.subTest(desc=desc):
                measurement_kwargs = dict(kwargs)
                proof = bool(
                    measurement_kwargs.pop("verified_lossless_proof", False)
                )
                m = AudioQualityMeasurement(**measurement_kwargs)
                self.assertEqual(
                    quality_gate_decision(
                        m,
                        verified_lossless_proof=proof,
                    ), expected,
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

    def test_verified_lossless_ignores_stale_spectral_clamp(self):
        """Verified lossless is already source-proven; a stale pre-import
        spectral cliff must not requeue an accepted target conversion."""
        m = AudioQualityMeasurement(
            format="opus 128",
            avg_bitrate_kbps=141,
            spectral_bitrate_kbps=160,
        )
        cfg = QualityRankConfig.defaults()
        self.assertEqual(
            gate_rank(m, cfg, verified_lossless_proof=True),
            measurement_rank(m, cfg),
        )
        self.assertEqual(
            quality_gate_decision(m, cfg, verified_lossless_proof=True),
            "accept",
        )

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
                measurement_kwargs = dict(kwargs)
                proof = bool(
                    measurement_kwargs.pop("verified_lossless_proof", False)
                )
                m = AudioQualityMeasurement(**measurement_kwargs)
                self.assertEqual(quality_gate_decision(
                    m,
                    cfg,
                    verified_lossless_proof=proof,
                ), expected,
                                 f"{desc}: quality_gate_decision diverges from CASE expectation")


class TestPostImportSearchAction(unittest.TestCase):
    """Independent decision-to-state table for the post-import policy."""

    CASES = (
        ("accept", "imported", None, False),
        ("requeue_lossless", "wanted", "lossless", True),
        ("requeue_upgrade", "wanted", None, True),
        ("provisional_lossless_upgrade", "wanted", "lossless", True),
        ("transcode_upgrade", "wanted", None, True),
        ("transcode_first", "wanted", None, True),
    )

    def test_action_mapping_matches_independent_table(self):
        for decision, status, override, denylist in self.CASES:
            with self.subTest(decision=decision):
                action = post_import_search_action(decision)
                self.assertEqual(action.status, status)
                self.assertEqual(action.search_filetype_override, override)
                self.assertEqual(action.denylist, denylist)

    def test_unknown_decision_is_rejected(self):
        with self.assertRaises(ValueError):
            post_import_search_action("invented")


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


class TestMintVerifiedLosslessProof(unittest.TestCase):
    """Proof construction policy (the 2026-07-18 args.filetype crash owner)."""

    # (desc, will_be, was_converted_from, detected_format, grade,
    #  expected_source or None-for-no-proof)
    CASES = [
        ("not verified mints nothing", False, "flac", "FLAC", "genuine", None),
        ("converted flac", True, "flac", "FLAC", "genuine", "flac"),
        ("normalized alac keeps true origin", True, "alac", "FLAC", "genuine", "alac"),
        ("kept lossless falls to detected", True, None, "FLAC", "genuine", "flac"),
        ("detected lowercases", True, None, "WAV", "marginal", "wav"),
        ("undetectable falls to sentinel", True, None, "UNKNOWN", "genuine",
         "lossless_source"),
        ("nothing known falls to sentinel", True, None, None, "genuine",
         "lossless_source"),
        ("blank strings fall through", True, "  ", "", "genuine",
         "lossless_source"),
    ]

    def test_mint_cases(self):
        from lib.quality import mint_verified_lossless_proof

        for desc, will_be, converted_from, detected, grade, expected in self.CASES:
            with self.subTest(desc=desc):
                proof = mint_verified_lossless_proof(
                    will_be,
                    was_converted_from=converted_from,
                    detected_source_format=detected,
                    spectral_grade=grade,
                )
                if expected is None:
                    self.assertIsNone(proof)
                else:
                    assert proof is not None
                    self.assertEqual(proof.source, expected)
                    self.assertEqual(proof.provenance, "measured")
                    self.assertEqual(
                        proof.classifier, "spectral_verified_lossless")
                    self.assertEqual(proof.detail, grade)

    def test_v0_override_mint_records_the_disputed_grade(self):
        """A V0-override mint (suspect spectral, strong V0) keeps the
        suspect grade in ``detail`` for the audit trail."""
        from lib.quality import mint_verified_lossless_proof

        proof = mint_verified_lossless_proof(
            True,
            was_converted_from="flac",
            detected_source_format="FLAC",
            spectral_grade="suspect",
        )
        assert proof is not None
        self.assertEqual(proof.detail, "suspect")


# ============================================================================
# full_pipeline_decision contract tests
# ============================================================================
# These lock the interface between full_pipeline_decision() and the web UI
# simulator. If a stage is added/removed or the result shape changes, these
# fail — forcing the simulator to be updated in sync.

from lib.quality import full_pipeline_decision
import inspect

# The exact keys the simulator reads from the result dict
EXPECTED_RESULT_KEYS = {
    # Preimport gates (shared, run before the FLAC/MP3 branches) — issue #91
    "preimport_audio", "preimport_nested",
    # U11: bad_audio_hash + empty_fileset early branches read from evidence;
    # the flat-kwargs simulator leaves them None.
    # mixed_source (lossless+lossy in one folder) is also evidence-only.
    "preimport_bad_hash", "preimport_empty_fileset",
    "preimport_mixed_source",
    "stage0_spectral_gate",
    "stage1_spectral", "stage2_import", "stage3_quality_gate",
    "final_status", "imported", "denylisted", "keep_searching",
    "target_final_format", "verified_lossless",
    # The persisted comparison basis (plain builtins) from stage 2's
    # measured decision, None when no existing album was compared.
    "comparison_basis",
}

# Valid values for each stage (None means stage was skipped)
VALID_PREIMPORT_AUDIO = {None, "pass", "reject_corrupt", "skipped_off"}
VALID_PREIMPORT_NESTED = {None, "pass", "reject_nested", "skipped_auto"}
VALID_STAGE0 = {None, "would_run", "skipped_vbr_high_avg", "skipped_flac"}
VALID_STAGE1 = {None, "import", "import_upgrade", "import_no_exist", "reject"}
VALID_STAGE2 = {None, "import", "downgrade", "transcode_upgrade",
                "transcode_downgrade", "transcode_first",
                "preflight_existing",
                DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
                DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
                DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
                DECISION_LOSSLESS_SOURCE_LOCKED}
VALID_STAGE3 = {None, "accept", "requeue_upgrade", "requeue_lossless"}
VALID_FINAL_STATUS = {None, "imported", "wanted"}

# The exact parameter names the simulator form submits
EXPECTED_PARAMS = {
    "is_flac", "min_bitrate", "is_cbr",
    "is_vbr", "avg_bitrate",
    "spectral_grade", "spectral_bitrate",
    "existing_min_bitrate", "existing_avg_bitrate",
    "existing_spectral_grade", "existing_spectral_bitrate",
    "override_min_bitrate",
    "existing_format", "existing_is_cbr",
    "post_conversion_min_bitrate", "post_conversion_is_cbr", "converted_count",
    "candidate_verified_lossless_proof", "verified_lossless_target",
    "target_format",
    "new_format", "cfg",
    "candidate_v0_probe_avg", "candidate_v0_probe_min",
    "existing_v0_probe_avg",
    "candidate_v0_probe_kind", "existing_v0_probe_kind",
    "supported_lossless_source",
    # Preimport gate inputs (issue #91) — keep the simulator's picture of the
    # pipeline in sync with lib.measurement.measure_preimport_state + preimport_decide.
    "audio_check_mode", "audio_corrupt",
    "import_mode", "has_nested_audio",
    # U7 proof-bearing HAVE lock: an explicit simulator input, not a
    # request-scalar inference.
    "current_verified_lossless_proof",
}


class TestFullPipelineDecisionFromEvidence(unittest.TestCase):
    """Evidence-pair reducer coverage for neutral album-quality evidence."""

    def _evidence(
        self,
        *,
        owner_type: str,
        owner_id: int,
        min_bitrate: int,
        avg_bitrate: int | None = None,
        fmt: str,
        is_cbr: bool = False,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        container: str | None = None,
        codec: str | None = None,
        storage_format: str | None = None,
        v0_lineage: str | None = None,
        v0_min: int | None = None,
        v0_avg: int | None = None,
        v0_proof: str | None = None,
    ) -> AlbumQualityEvidence:
        container = container or fmt.lower().split()[0]
        codec = codec or container
        storage_format = storage_format or fmt
        metric = None
        if v0_lineage is not None:
            subject = (
                "source"
                if v0_lineage == "lossless_source"
                else "installed"
            )
            metric = AlbumQualityV0Metric(
                min_bitrate_kbps=v0_min,
                avg_bitrate_kbps=v0_avg,
                median_bitrate_kbps=v0_avg,
                subject=subject,
                provenance="measured",
            )
        # Post-migration 021: evidence is content-addressed by
        # (mb_release_id, snapshot_fingerprint). For pure decision tests we
        # don't care which entity points at the row — the decider reads the
        # facts off the struct itself — so synthesise unique addressing keys
        # from the legacy ``(owner_type, owner_id)`` kwargs to preserve the
        # uniqueness of distinct evidence shapes within a test.
        return AlbumQualityEvidence(
            mb_release_id=f"mbid-{owner_type}-{owner_id}",
            snapshot_fingerprint=f"sha256:{owner_type}-{owner_id}-{container}",
            source_path=f"/tmp/{owner_type}-{owner_id}",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate,
                avg_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
                median_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
                format=fmt,
                is_cbr=is_cbr,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate,
                spectral_subject=(
                    "installed"
                    if owner_type == "request_current" and spectral_grade is not None
                    else "source" if spectral_grade is not None else None
                ),
                spectral_provenance=(
                    "measured" if spectral_grade is not None else None
                ),
            ),
            measured_at=datetime(2026, 5, 14, tzinfo=timezone.utc),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path=f"{owner_type}-{owner_id}.{container}",
                    size_bytes=1,
                    mtime_ns=1,
                    extension=container,
                    container=container,
                    codec=codec,
                )
            ],
            codec=codec,
            container=container,
            storage_format=storage_format,
            v0_metric=metric,
        )

    def _suspect_lossless_candidate(self) -> AlbumQualityEvidence:
        return self._evidence(
            owner_type="download_log_candidate",
            owner_id=3291,
            min_bitrate=900,
            avg_bitrate=900,
            fmt="FLAC",
            spectral_grade="suspect",
            spectral_bitrate=128,
            container="flac",
            storage_format="flac",
            v0_lineage="lossless_source",
            v0_min=141,
            v0_avg=240,
            v0_proof="lossless_source_probe",
        )

    def _current_with_v0_lineage(self, lineage: str) -> AlbumQualityEvidence:
        return self._evidence(
            owner_type="request_current",
            owner_id=42,
            min_bitrate=116,
            avg_bitrate=131,
            fmt="Opus",
            spectral_grade="likely_transcode",
            spectral_bitrate=96,
            container="opus",
            storage_format="opus",
            v0_lineage=lineage,
            v0_min=211,
            v0_avg=260,
            v0_proof="neutral_v0_probe",
        )

    def test_current_proof_precedes_every_candidate_integrity_reject(self):
        candidate = self._evidence(
            owner_type="download_log_candidate",
            owner_id=7,
            min_bitrate=320,
            avg_bitrate=320,
            fmt="MP3",
            container="mp3",
        )
        current = self._evidence(
            owner_type="request_current",
            owner_id=42,
            min_bitrate=128,
            avg_bitrate=128,
            fmt="Opus",
            container="opus",
        )
        current = msgspec.structs.replace(
            current,
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="test",
                classifier="test",
            ),
        )
        mixed_file = AlbumQualityEvidenceFile(
            relative_path="bonus.flac",
            size_bytes=1,
            mtime_ns=1,
            extension="flac",
            container="flac",
            codec="flac",
        )
        variants = {
            "audio_corrupt": msgspec.structs.replace(
                candidate, audio_corrupt=True
            ),
            "bad_audio_hash": msgspec.structs.replace(
                candidate,
                matched_bad_audio_hash_id=1,
                matched_bad_audio_hash_path="01.mp3",
            ),
            "nested_layout": msgspec.structs.replace(
                candidate, folder_layout="nested"
            ),
            "empty_fileset": msgspec.structs.replace(
                candidate, files=[], audio_file_count=0
            ),
            "mixed_source": msgspec.structs.replace(
                candidate, files=[*candidate.files, mixed_file]
            ),
        }

        for integrity_fact, variant in variants.items():
            with self.subTest(integrity_fact=integrity_fact):
                result = full_pipeline_decision_from_evidence(variant, current)
                self.assertEqual(
                    result["stage2_import"], "verified_lossless_locked"
                )
                self.assertEqual(result["final_status"], "imported")
                self.assertFalse(result["imported"])
                self.assertFalse(result["denylisted"])
                self.assertFalse(result["keep_searching"])
                for key in (
                    "preimport_audio",
                    "preimport_bad_hash",
                    "preimport_nested",
                    "preimport_empty_fileset",
                    "preimport_mixed_source",
                ):
                    self.assertIsNone(result[key])

    def test_stale_preview_candidate_loses_when_recomputed_against_current_evidence(self):
        candidate = self._suspect_lossless_candidate()
        stale_preview = full_pipeline_decision_from_evidence(
            candidate,
            None,
            facts=AlbumQualityEvidenceDecisionFacts(
                import_mode="force",
                verified_lossless_target="opus 128",
            ),
        )
        fresh_action = full_pipeline_decision_from_evidence(
            candidate,
            self._current_with_v0_lineage("lossless_source"),
            facts=AlbumQualityEvidenceDecisionFacts(
                import_mode="force",
                verified_lossless_target="opus 128",
            ),
        )

        self.assertTrue(stale_preview["imported"])
        self.assertEqual(
            stale_preview["stage2_import"],
            DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
        )
        self.assertFalse(fresh_action["imported"])
        self.assertEqual(
            fresh_action["stage2_import"],
            DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
        )

    def test_force_distance_bypass_context_does_not_change_quality_decision(self):
        candidate = self._suspect_lossless_candidate()
        current = self._current_with_v0_lineage("lossless_source")

        normal = full_pipeline_decision_from_evidence(candidate, current)
        forced = full_pipeline_decision_from_evidence(
            candidate,
            current,
            facts=AlbumQualityEvidenceDecisionFacts(
                import_mode="force",
            ),
        )

        quality_keys = (
            "stage0_spectral_gate",
            "stage1_spectral",
            "stage2_import",
            "stage3_quality_gate",
            "final_status",
            "imported",
            "denylisted",
            "keep_searching",
            "target_final_format",
            "verified_lossless",
        )
        self.assertEqual(
            {key: normal[key] for key in quality_keys},
            {key: forced[key] for key in quality_keys},
        )

    def test_v0_policy_comparability_derives_from_neutral_source_lineage(self):
        candidate = self._suspect_lossless_candidate()
        native_research_current = self._current_with_v0_lineage("native_lossy_research")
        lossless_source_current = self._current_with_v0_lineage("lossless_source")
        native_metric = native_research_current.v0_metric
        lossless_metric = lossless_source_current.v0_metric
        self.assertIsNotNone(native_metric)
        self.assertIsNotNone(lossless_metric)
        assert native_metric is not None
        assert lossless_metric is not None

        self.assertEqual(
            native_metric.avg_bitrate_kbps,
            lossless_metric.avg_bitrate_kbps,
        )
        self.assertEqual(
            native_metric.provenance,
            lossless_metric.provenance,
        )

        native_result = full_pipeline_decision_from_evidence(
            candidate,
            native_research_current,
        )
        lossless_source_result = full_pipeline_decision_from_evidence(
            candidate,
            lossless_source_current,
        )

        self.assertEqual(
            native_result["stage2_import"],
            DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
        )
        self.assertEqual(
            lossless_source_result["stage2_import"],
            DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
        )

    def test_bare_m4a_container_does_not_prove_lossless_source(self):
        candidate = self._evidence(
            owner_type="download_log_candidate",
            owner_id=77,
            min_bitrate=256,
            avg_bitrate=256,
            fmt="AAC",
            container="m4a",
            codec="aac",
            storage_format="AAC",
        )
        result = full_pipeline_decision_from_evidence(
            candidate,
            self._current_with_v0_lineage("lossless_source"),
        )

        self.assertFalse(result["imported"])
        self.assertEqual(
            result["stage2_import"],
            DECISION_LOSSLESS_SOURCE_LOCKED,
        )

    def test_evidence_decision_name_uses_stage1_reject_not_final_status(self):
        candidate = self._evidence(
            owner_type="download_log_candidate",
            owner_id=91,
            min_bitrate=320,
            avg_bitrate=320,
            fmt="MP3",
            is_cbr=True,
            spectral_grade="suspect",
            spectral_bitrate=96,
        )
        current = self._evidence(
            owner_type="request_current",
            owner_id=42,
            min_bitrate=245,
            avg_bitrate=245,
            fmt="MP3",
            spectral_grade="genuine",
            spectral_bitrate=128,
        )
        result = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(result["stage1_spectral"], "reject")
        self.assertEqual(result["final_status"], "wanted")
        self.assertEqual(evidence_decision_name(result), "spectral_reject")


class TestPreimportAudioGate(unittest.TestCase):
    """Pure decision tests for the preimport audio-integrity gate (issue #91).

    Models the first gate in ``lib.measurement.measure_preimport_state``
    (validate_audio). The simulator must treat ``audio_check_mode=off`` as
    a distinct outcome from a passing check so operators can see when the
    gate is disabled in config.
    """

    # (desc, audio_check_mode, audio_corrupt, expected)
    CASES = [
        ("off short-circuits regardless of corrupt flag", "off", False, "skipped_off"),
        ("off skipped even when corrupt signalled",       "off", True,  "skipped_off"),
        ("normal + clean passes",                         "normal", False, "pass"),
        ("normal + corrupt rejects",                      "normal", True,  "reject_corrupt"),
        ("strict + corrupt rejects",                      "strict", True,  "reject_corrupt"),
    ]

    def test_preimport_audio_gate_cases(self):
        from lib.quality import preimport_audio_gate
        for desc, mode, corrupt, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    preimport_audio_gate(mode, corrupt),
                    expected)


class TestPreimportNestedGate(unittest.TestCase):
    """Pure decision tests for the preimport nested-layout gate (issue #91).

    Only the force/manual path rejects nested-audio layouts — the auto path
    always flattens before import_dispatch runs. This matches
    ``lib.dispatch.dispatch_import_from_db``.
    """

    # (desc, import_mode, has_nested_audio, expected)
    CASES = [
        ("auto never applies",                  "auto",   True,  "skipped_auto"),
        ("auto passes when flat",               "auto",   False, "skipped_auto"),
        ("force + flat passes",                 "force",  False, "pass"),
        ("force + nested rejects",              "force",  True,  "reject_nested"),
        ("manual + flat passes",                "manual", False, "pass"),
        ("manual + nested rejects",             "manual", True,  "reject_nested"),
    ]

    def test_preimport_nested_gate_cases(self):
        from lib.quality import preimport_nested_gate
        for desc, mode, nested, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    preimport_nested_gate(mode, nested),
                    expected)


class TestFullPipelinePreimportGates(unittest.TestCase):
    """``full_pipeline_decision`` must model preimport gates end-to-end (#91).

    Audio or nested rejects must short-circuit before stage 0/1/2/3 run and
    mirror the two live paths' post-reject state:

    * Auto path → `reject_and_requeue` transitions to "wanted" and bumps the
      validation counter → ``final_status='wanted'``, ``keep_searching=True``.
    * Force/manual path → `_record_rejection_and_maybe_requeue(requeue=False)`
      logs but does NOT transition → ``final_status=None`` (unchanged),
      ``keep_searching=False``.
    """

    def test_audio_corrupt_rejects_before_stages_auto(self):
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_check_mode="normal", audio_corrupt=True)
        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertFalse(r["imported"])
        # Auto path transitions back to "wanted" and denylists the source
        # (reject_and_requeue calls db.add_denylist for every username).
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])
        self.assertTrue(r["denylisted"])
        # Later stages must not have run.
        self.assertIsNone(r["stage0_spectral_gate"])
        self.assertIsNone(r["stage1_spectral"])
        self.assertIsNone(r["stage2_import"])
        self.assertIsNone(r["stage3_quality_gate"])

    def test_audio_corrupt_rejects_force_does_not_denylist(self):
        # Force/manual uses _record_rejection_and_maybe_requeue with
        # requeue=False; that helper does not denylist (comment: "denylisting
        # is handled by the caller via action.denylist"). No such action is
        # set for preimport audio rejects, so denylisted must be False.
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_check_mode="normal", audio_corrupt=True,
            import_mode="force")
        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertFalse(r["denylisted"])

    def test_audio_corrupt_rejects_force_preserves_status(self):
        # Force-import path uses requeue=False — the request's status is not
        # touched by the reject, so the simulator must report final_status=None.
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_check_mode="normal", audio_corrupt=True,
            import_mode="force")
        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertFalse(r["imported"])
        self.assertIsNone(r["final_status"])
        self.assertFalse(r["keep_searching"])

    def test_audio_corrupt_rejects_manual_preserves_status(self):
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_check_mode="normal", audio_corrupt=True,
            import_mode="manual")
        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertIsNone(r["final_status"])
        self.assertFalse(r["keep_searching"])

    def test_audio_check_off_skips_gate(self):
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_check_mode="off", audio_corrupt=True)
        self.assertEqual(r["preimport_audio"], "skipped_off")
        # With audio check off, downstream still runs.
        self.assertTrue(r["imported"])

    def test_audio_pass_default(self):
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False)
        self.assertEqual(r["preimport_audio"], "pass")
        self.assertTrue(r["imported"])

    def test_nested_layout_rejects_force_path(self):
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            import_mode="force", has_nested_audio=True)
        self.assertEqual(r["preimport_nested"], "reject_nested")
        self.assertFalse(r["imported"])
        # Force-import uses requeue=False — status stays as-is in the live DB.
        self.assertIsNone(r["final_status"])
        self.assertFalse(r["keep_searching"])
        self.assertIsNone(r["stage2_import"])

    def test_nested_layout_rejects_manual_path(self):
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            import_mode="manual", has_nested_audio=True)
        self.assertEqual(r["preimport_nested"], "reject_nested")
        self.assertFalse(r["imported"])
        self.assertIsNone(r["final_status"])
        self.assertFalse(r["keep_searching"])

    def test_nested_layout_irrelevant_on_auto_path(self):
        # Nested audio on auto path is impossible in production (flattened
        # upstream) — simulator must report "skipped_auto" so the Decisions
        # tab can't mislead operators into thinking auto rejects on nesting.
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            import_mode="auto", has_nested_audio=True)
        self.assertEqual(r["preimport_nested"], "skipped_auto")
        self.assertTrue(r["imported"])

    def test_nested_wins_over_audio_in_force_mode(self):
        # Live ordering (dispatch_import_from_db): nested check runs *before*
        # measure_preimport_state is even called, so a corrupt-AND-nested
        # folder is reported as nested_layout, not audio_corrupt. The simulator
        # must short-circuit the same way so operators are sent to the
        # right remediation (flatten the folder).
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_check_mode="normal", audio_corrupt=True,
            import_mode="force", has_nested_audio=True)
        self.assertEqual(r["preimport_nested"], "reject_nested")
        # Audio gate never ran.
        self.assertIsNone(r["preimport_audio"])
        self.assertFalse(r["imported"])
        self.assertIsNone(r["final_status"])


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

    def test_new_existing_spectral_grade_param_preserves_positional_compatibility(self):
        """New optional simulator fields are appended after older neighbors."""
        params = list(inspect.signature(full_pipeline_decision).parameters)
        self.assertLess(
            params.index("existing_spectral_bitrate"),
            params.index("existing_spectral_grade"),
        )

    def test_v0_probe_params_do_not_shift_positional_cfg(self):
        """The old trailing cfg positional slot must keep working."""
        params = list(inspect.signature(full_pipeline_decision).parameters)
        self.assertLess(params.index("cfg"), params.index("candidate_v0_probe_avg"))
        self.assertEqual(
            inspect.signature(full_pipeline_decision)
            .parameters["candidate_v0_probe_avg"].kind,
            inspect.Parameter.KEYWORD_ONLY,
        )

        cfg = QualityRankConfig.defaults()
        positional = [
            False,  # is_flac
            180,  # min_bitrate
            False,  # is_cbr
            True,  # is_vbr
            180,  # avg_bitrate
            None,  # spectral_grade
            None,  # spectral_bitrate
            0,  # existing_min_bitrate
            None,  # existing_avg_bitrate
            None,  # existing_spectral_grade
            None,  # existing_spectral_bitrate
            None,  # override_min_bitrate
            "MP3",  # existing_format
            False,  # existing_is_cbr
            None,  # post_conversion_min_bitrate
            0,  # converted_count
            False,  # candidate_verified_lossless_proof
            None,  # verified_lossless_target
            None,  # target_format
            "MP3",  # new_format
            "auto",  # audio_check_mode
            False,  # audio_corrupt
            "auto",  # import_mode
            False,  # has_nested_audio
            cfg,  # cfg
        ]

        r = full_pipeline_decision(*positional)
        self.assertEqual(r["stage2_import"], "import")

    def test_current_proof_cannot_become_candidate_proof(self):
        """The HAVE lock is not a candidate verification input or output."""
        result = full_pipeline_decision(
            is_flac=False,
            min_bitrate=245,
            is_cbr=False,
            new_format="MP3",
            candidate_verified_lossless_proof=False,
            current_verified_lossless_proof=True,
        )

        self.assertEqual(result["stage2_import"], "verified_lossless_locked")
        self.assertFalse(result["verified_lossless"])
        self.assertFalse(result["imported"])
        self.assertIsNone(result["stage3_quality_gate"])

    def test_current_proof_precedes_flat_preimport_rejects(self):
        cases: tuple[tuple[str, dict[str, Any]], ...] = (
            ("audio_corrupt", {"audio_corrupt": True}),
            ("nested_layout", {"has_nested_audio": True}),
        )
        for integrity_fact, kwargs in cases:
            with self.subTest(integrity_fact=integrity_fact):
                result = full_pipeline_decision(
                    is_flac=False,
                    min_bitrate=245,
                    is_cbr=False,
                    current_verified_lossless_proof=True,
                    **kwargs,
                )
                self.assertEqual(
                    result["stage2_import"], "verified_lossless_locked"
                )
                self.assertIsNone(result["preimport_audio"])
                self.assertIsNone(result["preimport_nested"])

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
            ("genuine below transparent stays full-tier", "genuine", 160,
             "requeue_upgrade", "wanted"),
            ("marginal never narrows", "marginal", 160,
             "requeue_upgrade", "wanted"),
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

    def test_avg_bitrate_flows_into_stage3_rank(self):
        """Issue #93 round 4: avg_bitrate must flow past stage 0 into the
        rank comparison. Under the default cfg.bitrate_metric=AVG policy,
        a VBR V0 at min=200 / avg=245 must rank as TRANSPARENT. Without
        verified-lossless proof it is retained while full-tier search continues.
        Pre-fix: simulator used min=200 as avg → GOOD < EXCELLENT → requeue.
        """
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=200, is_cbr=False,
            is_vbr=True, avg_bitrate=245,
        )
        # Stage 0: avg >= threshold → skip
        self.assertEqual(r["stage0_spectral_gate"], "skipped_vbr_high_avg")
        # Stage 2 uses AVG metric → 245 → TRANSPARENT → import
        self.assertEqual(r["stage2_import"], "import")
        # Stage 3 is search policy, not an acceptance floor.
        self.assertEqual(r["stage3_quality_gate"], "requeue_upgrade")
        self.assertEqual(r["final_status"], "wanted")

    def test_missing_avg_bitrate_falls_back_to_min(self):
        """Backward-compat: callers that don't pass avg_bitrate get the
        legacy behavior (avg == min). Protects existing scenarios that
        only supply min_bitrate."""
        # Same min=200 but no avg supplied → ranks on min=200 (GOOD < EXCELLENT)
        # so the gate requeues for upgrade.
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=200, is_cbr=False,
        )
        self.assertEqual(r["stage3_quality_gate"], "requeue_upgrade")
        self.assertEqual(r["final_status"], "wanted")

    def test_selected_bitrate_survives_dual_module_load(self):
        """Regression for the live deploy bug caught on PR #94.

        ``web/routes/pipeline.py`` used to do
        ``from quality import full_pipeline_decision`` while ``cfg`` was
        constructed via ``lib.config`` → ``lib.quality``. Python loaded
        quality.py twice (once as ``quality``, once as ``lib.quality``)
        and ``_selected_bitrate``'s ``cfg.bitrate_metric is
        RankBitrateMetric.AVG`` identity comparison was False — it
        silently fell through to min_bitrate, breaking AVG policy for
        VBR albums in the simulator.

        This test constructs a cfg whose bitrate_metric is a StrEnum
        member EQUAL but not IDENTICAL to the one the function's scope
        sees. The fix (use ``==`` not ``is``) must hold.
        """
        from enum import StrEnum
        from lib.quality import (AudioQualityMeasurement, QualityRankConfig,
                                 measurement_rank)

        # Fake RankBitrateMetric-equivalent StrEnum: same string values,
        # different class object. Under `is`, compares False; under `==`,
        # compares True.
        class ForeignRankBitrateMetric(StrEnum):
            MIN = "min"
            AVG = "avg"
            MEDIAN = "median"

        cfg = QualityRankConfig.defaults()
        # Replace cfg.bitrate_metric with the foreign-class equivalent.
        # dataclasses.replace since QualityRankConfig is frozen.
        import dataclasses
        foreign_cfg = dataclasses.replace(
            cfg, bitrate_metric=ForeignRankBitrateMetric.AVG)  # type: ignore[arg-type]

        m = AudioQualityMeasurement(
            min_bitrate_kbps=200,
            avg_bitrate_kbps=245,
            median_bitrate_kbps=245,
            format="MP3",
            is_cbr=False,
        )
        # With `==` comparison the AVG policy picks avg=245 → TRANSPARENT.
        # With `is` comparison it would fall through to min=200 → GOOD.
        self.assertEqual(
            measurement_rank(m, foreign_cfg).name, "TRANSPARENT",
            "cfg.bitrate_metric comparison must be ==, not is — or a "
            "cross-module-loaded cfg silently breaks the AVG policy")

    def test_existing_avg_bitrate_used_in_comparison(self):
        """existing_avg_bitrate flows into the existing measurement so the
        Stage 2 comparison uses the right metric under AVG policy."""
        # Existing is a VBR album at min=200 / avg=245 (TRANSPARENT).
        # Incoming MP3 at min=210 / avg=210 (EXCELLENT) → worse, reject.
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=210, is_cbr=False,
            is_vbr=True, avg_bitrate=210,
            existing_min_bitrate=200,
            existing_avg_bitrate=245,
        )
        # Stage 2 compares avg=210 (EXCELLENT) to existing avg=245 (TRANSPARENT)
        # → worse → downgrade
        self.assertEqual(r["stage2_import"], "downgrade")

    def test_equal_spectral_bucket_still_imports_higher_avg_mp3(self):
        """Grouper live shape: spectral ties at 96, but 219avg beats 128avg."""
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=209,
            is_cbr=False,
            is_vbr=True,
            avg_bitrate=219,
            spectral_grade="genuine",
            spectral_bitrate=96,
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            existing_spectral_bitrate=96,
            existing_format="MP3",
            existing_is_cbr=True,
            new_format="MP3",
        )
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])

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
            post_conversion_is_cbr=False,
            verified_lossless_target="aac 128",
            supported_lossless_source=False)
        self.assertIsNone(r["target_final_format"])

    def test_provisional_lossless_upgrade_uses_probe_avg(self):
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect", spectral_bitrate=160,
            converted_count=10,
            post_conversion_min_bitrate=228,
            candidate_v0_probe_avg=228,
            existing_v0_probe_avg=171,
            verified_lossless_target="opus 128",
        )
        self.assertEqual(
            r["stage2_import"], DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        self.assertTrue(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertEqual(r["target_final_format"], "opus 128")
        self.assertIsNone(r["stage3_quality_gate"])

    def test_fred_again_ae2_boundary_provisional_upgrade_stays_unverified(self):
        """AE2 (Fred again.., request 5219 / download 31854): suspect FLAC
        with V0 min 193 / avg 256 against anchor 248 imports as a
        provisional upgrade (256 beats 248 beyond tolerance) AND stays
        unverified — min 193 misses the hardcoded 200 override floor.
        The minimum-track guard is intentional (settled decision 10).
        """
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect", spectral_bitrate=160,
            converted_count=10,
            post_conversion_min_bitrate=193,
            candidate_v0_probe_avg=256,
            existing_v0_probe_avg=248,
        )
        self.assertEqual(
            r["stage2_import"], DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        self.assertTrue(r["imported"])
        self.assertFalse(r["verified_lossless"])
        self.assertTrue(r["keep_searching"])
        self.assertEqual(r["final_status"], "wanted")

    def test_provisional_lossless_uses_converted_v0_when_spectral_would_reject(self):
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="likely_transcode", spectral_bitrate=128,
            existing_spectral_bitrate=160,
            converted_count=10,
            post_conversion_min_bitrate=228,
            existing_v0_probe_avg=171,
        )
        self.assertEqual(r["stage1_spectral"], "reject")
        self.assertEqual(
            r["stage2_import"], DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        self.assertTrue(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_provisional_lossless_downgrade_rejects_within_tolerance(self):
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect", spectral_bitrate=160,
            converted_count=10,
            post_conversion_min_bitrate=175,
            candidate_v0_probe_avg=175,
            existing_v0_probe_avg=171,
        )
        self.assertEqual(
            r["stage2_import"], DECISION_SUSPECT_LOSSLESS_DOWNGRADE)
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])
        self.assertEqual(r["final_status"], "wanted")

    def test_live_mountain_goats_bride_first_provisional_source_import(self):
        """Mountain Goats - Bride / durandurfan, 2026-04-27 live shape.

        A suspect FLAC source with source-lineage V0 avg 214kbps and no
        comparable source probe imports provisionally, stores as opus 128,
        denylists the source, and keeps searching.
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            converted_count=1,
            post_conversion_min_bitrate=214,
            candidate_v0_probe_avg=214,
            existing_min_bitrate=320,
            existing_avg_bitrate=320,
            existing_format="MP3",
            existing_is_cbr=True,
            verified_lossless_target="opus 128",
        )

        self.assertEqual(r["stage0_spectral_gate"], "skipped_flac")
        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        self.assertEqual(
            r["stage2_import"], DECISION_PROVISIONAL_LOSSLESS_UPGRADE)
        self.assertTrue(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertEqual(r["target_final_format"], "opus 128")
        self.assertFalse(r["verified_lossless"])
        self.assertIsNone(r["stage3_quality_gate"])

    def test_live_sundowner_high_v0_likely_transcode_imports_as_verified(self):
        """Sundowner - Four One Five Two live shape.

        Spectral called the lossless source likely_transcode at ~160kbps, but
        the source-lineage V0 probe was avg=276/min=237. That is stronger
        evidence than the spectral false positive, so this must be a normal
        verified lossless import, not a provisional keep-searching import.
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=160,
            converted_count=12,
            post_conversion_min_bitrate=237,
            candidate_v0_probe_avg=276,
            candidate_v0_probe_min=237,
            existing_min_bitrate=None,
            existing_v0_probe_avg=None,
            verified_lossless_target="opus 128",
        )

        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["verified_lossless"])
        self.assertTrue(r["imported"])
        self.assertFalse(r["denylisted"])
        self.assertFalse(r["keep_searching"])
        self.assertEqual(r["final_status"], "imported")
        self.assertEqual(r["target_final_format"], "opus 128")
        self.assertEqual(r["stage3_quality_gate"], "accept")

    def test_live_creek_drank_cradle_lower_source_rejects_after_better_probe(self):
        """Iron & Wine - The Creek Drank the Cradle / maplebug shape.

        Once a better comparable lossless-source V0 probe exists from the
        earlier SPENCERTPSN import, a lower 171kbps source probe is a confident
        suspect-lossless downgrade even though the generic spectral stage would
        also reject at 96kbps.
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=96,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=96,
            converted_count=11,
            post_conversion_min_bitrate=165,
            candidate_v0_probe_avg=171,
            existing_v0_probe_avg=228,
            existing_min_bitrate=220,
            existing_avg_bitrate=228,
            existing_format="MP3",
            existing_is_cbr=False,
            verified_lossless_target="opus 128",
        )

        self.assertEqual(r["stage0_spectral_gate"], "skipped_flac")
        self.assertEqual(r["stage1_spectral"], "reject")
        self.assertEqual(
            r["stage2_import"], DECISION_SUSPECT_LOSSLESS_DOWNGRADE)
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertIsNone(r["target_final_format"])
        self.assertIsNone(r["stage3_quality_gate"])

    def test_lossy_candidate_locked_by_existing_lossless_source_probe(self):
        """Message to Bears EP1 / k1d_pr1mus shape, 2026-04-27 21:16.

        Existing on-disk file is opus 128 (avg ~131kbps) transcoded from a
        provisional suspect FLAC source whose lossless-source V0 probe was
        recorded at 240kbps. A subsequent lossy candidate at avg 205kbps with
        spectral cliff ~128kbps must be rejected by the lossless-source lock
        before measured_import_decision gets a chance to compare it against
        the on-disk avg, because there is no V0-comparable evidence the
        lossy side could produce.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=176,
            avg_bitrate=205,
            is_cbr=False,
            is_vbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            existing_min_bitrate=116,
            existing_avg_bitrate=131,
            existing_format="opus",
            existing_v0_probe_avg=240,
            verified_lossless_target="opus 128",
        )
        self.assertEqual(r["stage2_import"], DECISION_LOSSLESS_SOURCE_LOCKED)
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertIsNone(r["stage3_quality_gate"])

    def test_lossy_candidate_passes_when_no_lossless_source_probe(self):
        """Without a recorded lossless-source V0 probe the lock has nothing
        to anchor against — the legacy import_quality_decision path runs."""
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=320,
            avg_bitrate=320,
            is_cbr=True,
            existing_min_bitrate=192,
            existing_avg_bitrate=192,
            existing_is_cbr=True,
            existing_format="MP3",
            new_format="MP3",
        )
        self.assertNotEqual(r["stage2_import"], DECISION_LOSSLESS_SOURCE_LOCKED)
        self.assertTrue(r["imported"])

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
        # U6: transcoded-from-FLAC library rows now carry source spectral
        # post-U5. compute_effective_override_bitrate's behaviour for the
        # OPUS V2 case is unchanged (min(100, 128) = 100); the MP3 V0 case
        # shifts from container (~225) to spectral (128) — searches become
        # more permissive in line with the source's actual quality cliff.
        ("transcoded opus v2 row, lossless_source spectral", 100, 128, "likely_transcode", 100),
        ("transcoded mp3 v0 row, lossless_source spectral",  225, 128, "likely_transcode", 128),
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
                        cleanup=True, trigger_notifiers=True,
                        run_quality_gate=True)),
        ("preflight_existing", dict(mark_done=True, trigger_notifiers=True,
                                    run_quality_gate=True)),
        ("downgrade", dict(mark_done=False, record_rejection=True, denylist=True,
                           cleanup=True)),
        ("transcode_upgrade", dict(mark_done=True, denylist=False,
                                   trigger_notifiers=True)),
        ("transcode_downgrade", dict(mark_done=False, record_rejection=True,
                                     denylist=True)),
        ("transcode_first", dict(mark_done=True, denylist=False,
                                 trigger_notifiers=True)),
        ("provisional_lossless_upgrade",
         dict(mark_done=True, denylist=False,
              trigger_notifiers=True, run_quality_gate=False)),
        ("suspect_lossless_downgrade",
         dict(mark_done=False, record_rejection=True, denylist=True,
              cleanup=True)),
        ("suspect_lossless_probe_missing",
         dict(mark_done=False, record_rejection=True, denylist=True,
              cleanup=True)),
        ("lossless_source_locked",
         dict(mark_done=False, record_rejection=True, denylist=True,
              cleanup=True)),
        ("spectral_reject",
         dict(mark_done=False, record_rejection=True, denylist=True,
              cleanup=True)),
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
        from lib.quality import dispatch_action
        for outcome in VALID_STAGE2 - {None}:
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


class TestResolveRejectionSearchOverride(unittest.TestCase):
    """Transparent HAVE narrowing precedes ordinary per-tier convergence."""

    def setUp(self):
        self.cfg = QualityRankConfig.defaults()
        self.measurement = AudioQualityMeasurement(
            min_bitrate_kbps=self.cfg.mp3_cbr.transparent,
            avg_bitrate_kbps=self.cfg.mp3_cbr.transparent,
            format="MP3",
            is_cbr=True,
            spectral_grade="genuine",
        )
        self.download = DownloadInfo(
            filetype="mp3",
            bitrate=245000,
            is_vbr=True,
        )

    def test_trusted_transparent_have_wins_over_full_ladder(self):
        from lib.quality import SpectralAnalysisDetail, QUALITY_UPGRADE_TIERS

        resolution = resolve_rejection_search_override(
            decision="downgrade",
            current_override=QUALITY_UPGRADE_TIERS,
            dl_info=self.download,
            current_measurement=self.measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
            ),
            cfg=self.cfg,
        )
        self.assertEqual(resolution.override, "lossless")
        self.assertEqual(resolution.reason, "transparent_have")

    def test_missing_audit_falls_back_to_rejected_tier_removal(self):
        from lib.quality import QUALITY_UPGRADE_TIERS

        resolution = resolve_rejection_search_override(
            decision="downgrade",
            current_override=QUALITY_UPGRADE_TIERS,
            dl_info=self.download,
            current_measurement=self.measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=None,
            cfg=self.cfg,
        )
        self.assertEqual(
            resolution.override,
            "lossless,mp3 320,aac,opus,ogg",
        )
        self.assertEqual(resolution.reason, "rejected_tier")

    def test_transcode_downgrade_without_audit_preserves_override(self):
        from lib.quality import QUALITY_UPGRADE_TIERS

        resolution = resolve_rejection_search_override(
            decision="transcode_downgrade",
            current_override=QUALITY_UPGRADE_TIERS,
            dl_info=self.download,
            current_measurement=self.measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=None,
            cfg=self.cfg,
        )
        self.assertIsNone(resolution.override)
        self.assertEqual(resolution.reason, "preserve")


class TestNarrowOverrideOnLosslessSourceLock(unittest.TestCase):
    """Test narrowing search_filetype_override to lossless-only after the
    ``lossless_source_locked`` decision fires.

    Pure helper; deterministic on its single argument. See origin:
    ``docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md``
    R6 and AE7.
    """

    # (description, current_override, expected)
    CASES = [
        ("none → lossless", None, "lossless"),
        ("mp3 v0 → lossless", "mp3 v0", "lossless"),
        ("mp3 320 → lossless", "mp3 320", "lossless"),
        ("full ladder → lossless", "lossless,mp3 v0,mp3 320", "lossless"),
        ("already lossless → None (idempotent)", "lossless", None),
        ("empty string → lossless", "", "lossless"),
    ]

    def test_narrow_table(self):
        from lib.quality import narrow_override_on_lossless_source_lock
        for desc, current, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    narrow_override_on_lossless_source_lock(current),
                    expected,
                    f"{desc}: narrow_override_on_lossless_source_lock"
                    f"({current!r}) expected {expected!r}",
                )


class TestRejectionBackfillOverride(unittest.TestCase):
    """Historical MP3 cases retained at the transparent-only boundary."""

    def _override(
        self,
        *,
        is_cbr: bool,
        min_bitrate_kbps: int | None,
        spectral_grade: str | None,
        verified_lossless: bool,
        cfg: QualityRankConfig | None = None,
        format: str = "MP3",
    ) -> str | None:
        from lib.quality import SpectralAnalysisDetail, rejection_backfill_override

        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate_kbps,
            avg_bitrate_kbps=min_bitrate_kbps,
            median_bitrate_kbps=min_bitrate_kbps,
            format=format,
            is_cbr=is_cbr,
        )
        audit = SpectralAnalysisDetail(
            attempted=spectral_grade is not None,
            grade=spectral_grade,
        )
        return rejection_backfill_override(
            current_measurement=measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=audit,
            cfg=cfg,
        )

    # --- Genuine spectral: flac for both CBR and VBR ---

    def test_cbr_320_genuine_returns_flac(self):
        from lib.quality import QUALITY_FLAC_ONLY
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_cbr_256_genuine_stays_open_for_transparent_lossy(self):
        result = self._override(
            is_cbr=True, min_bitrate_kbps=256,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    def test_vbr_240_genuine_stays_open_for_transparent_lossy(self):
        result = self._override(
            is_cbr=False, min_bitrate_kbps=240,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    def test_vbr_at_old_gate_threshold_stays_open(self):
        result = self._override(
            is_cbr=False, min_bitrate_kbps=210,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    # --- Not genuine: never backfill (spectral is the whole point) ---

    def test_cbr_320_suspect_returns_none(self):
        """Suspect 320: keep searching all tiers, might find genuine source."""
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="suspect", verified_lossless=False)
        self.assertIsNone(result)

    def test_cbr_320_marginal_returns_none(self):
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="marginal", verified_lossless=False)
        self.assertIsNone(result)

    def test_cbr_320_no_spectral_returns_none(self):
        """No spectral data: can't make the decision, keep all tiers."""
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade=None, verified_lossless=False)
        self.assertIsNone(result)

    def test_vbr_suspect_returns_none(self):
        result = self._override(
            is_cbr=False, min_bitrate_kbps=240,
            spectral_grade="suspect", verified_lossless=False)
        self.assertIsNone(result)

    def test_vbr_no_spectral_returns_none(self):
        result = self._override(
            is_cbr=False, min_bitrate_kbps=240,
            spectral_grade=None, verified_lossless=False)
        self.assertIsNone(result)

    # --- Below threshold: never backfill ---

    def test_vbr_below_threshold_returns_none(self):
        result = self._override(
            is_cbr=False, min_bitrate_kbps=200,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    def test_cbr_192_below_threshold_returns_none(self):
        result = self._override(
            is_cbr=True, min_bitrate_kbps=192,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    # --- Guards ---

    def test_verified_lossless_derived_lossy_still_narrows(self):
        """Source provenance does not turn materialized MP3 into LOSSLESS rank."""
        from lib.quality import QUALITY_LOSSLESS
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=True)
        self.assertEqual(result, QUALITY_LOSSLESS)

    def test_lossless_container_returns_none(self):
        result = self._override(
            is_cbr=False, min_bitrate_kbps=1000,
            spectral_grade="genuine", verified_lossless=True,
            format="FLAC")
        self.assertIsNone(result)

    def test_none_bitrate_returns_none(self):
        result = self._override(
            is_cbr=True, min_bitrate_kbps=None,
            spectral_grade="genuine", verified_lossless=False)
        self.assertIsNone(result)

    # --- Named scenarios ---

    def test_stars_of_the_lid_scenario(self):
        """Stars of the Lid: CBR 320 genuine on disk. Backfill fires."""
        from lib.quality import QUALITY_FLAC_ONLY
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_upgrade_button_no_spectral_scenario(self):
        """CBR 320, no spectral on disk. Backfill does NOT fire yet —
        needs an independent HAVE audit first."""
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade=None, verified_lossless=False)
        self.assertIsNone(result)

    def test_upgrade_button_after_genuine_download(self):
        """CBR 320 with a trusted genuine HAVE audit backfills."""
        from lib.quality import QUALITY_FLAC_ONLY
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_FLAC_ONLY)

    def test_upgrade_button_after_suspect_download(self):
        """CBR 320 with a suspect HAVE audit keeps every lossy tier open."""
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="suspect", verified_lossless=False)
        self.assertIsNone(result)

    # --- Rank-aware: codec-band cfg threading ---
    #
    # Search narrowing follows the configured codec's TRANSPARENT boundary.

    def test_default_transparent_boundary_blocks_lower_vbr(self):
        """The canonical TRANSPARENT boundary blocks the same 180kbps VBR."""
        result = self._override(
            is_cbr=False, min_bitrate_kbps=180,
            spectral_grade="genuine", verified_lossless=False)
        # 180 → GOOD, not TRANSPARENT → no backfill
        self.assertIsNone(result)

    def test_excellent_cbr_does_not_meet_transparent_boundary(self):
        """CBR 256 stays open because narrowing is transparent-only."""
        result = self._override(
            is_cbr=True, min_bitrate_kbps=256,
            spectral_grade="genuine", verified_lossless=False)
        # 256 against mp3_cbr (transparent=320, excellent=256) → EXCELLENT,
        # EXCELLENT < TRANSPARENT → no backfill
        self.assertIsNone(result)

    def test_transparent_cbr_meets_narrowing_boundary(self):
        """CBR 320 narrows when affirmative evidence makes it transparent."""
        from lib.quality import QUALITY_LOSSLESS
        result = self._override(
            is_cbr=True, min_bitrate_kbps=320,
            spectral_grade="genuine", verified_lossless=False)
        self.assertEqual(result, QUALITY_LOSSLESS)


class TestTransparentGenuineLossyRejectionBackfill(unittest.TestCase):
    """Only trusted, transparent HAVE evidence closes lossy search tiers."""

    def _override(
        self,
        *,
        format: str,
        bitrate: int,
        is_cbr: bool = False,
        attempted: bool = True,
        grade: str | None = "genuine",
        error: str | None = None,
    ) -> str | None:
        from lib.quality import SpectralAnalysisDetail, rejection_backfill_override

        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            median_bitrate_kbps=bitrate,
            format=format,
            is_cbr=is_cbr,
        )
        audit = SpectralAnalysisDetail(
            attempted=attempted,
            grade=grade,
            error=error,
        )
        return rejection_backfill_override(
            current_measurement=measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=audit,
            cfg=QualityRankConfig.defaults(),
        )

    def test_codec_general_positive_matrix(self):
        cfg = QualityRankConfig.defaults()
        cases = [
            ("mp3 cbr 320", "MP3", cfg.mp3_cbr.transparent, True),
            ("mp3 v0 measurement", "MP3", cfg.mp3_vbr.transparent, False),
            ("opus transparent", "Opus", cfg.opus.transparent, False),
            ("aac transparent", "AAC", cfg.aac.transparent, False),
            ("vorbis transparent", "Vorbis", cfg.vorbis.transparent, False),
            ("wma transparent", "WMA", cfg.wma.transparent, False),
        ]
        for description, format, bitrate, is_cbr in cases:
            with self.subTest(description=description):
                self.assertEqual(
                    self._override(
                        format=format,
                        bitrate=bitrate,
                        is_cbr=is_cbr,
                    ),
                    "lossless",
                )

    def test_negative_matrix(self):
        cfg = QualityRankConfig.defaults()
        cases = [
            ("excellent mp3", "MP3", cfg.mp3_cbr.excellent, True, True, "genuine", None),
            ("transparent suspect", "MP3", cfg.mp3_cbr.transparent, True, True, "suspect", None),
            ("transparent marginal", "MP3", cfg.mp3_cbr.transparent, True, True, "marginal", None),
            ("failed audit", "MP3", cfg.mp3_cbr.transparent, True, True, None, "spectral failed"),
            ("missing audit", "MP3", cfg.mp3_cbr.transparent, True, False, None, None),
            ("vorbis transparent suspect", "Vorbis", cfg.vorbis.transparent, False, True, "suspect", None),
            ("vorbis transparent likely transcode", "Vorbis", cfg.vorbis.transparent, False, True, "likely_transcode", None),
            ("wma transparent suspect", "WMA", cfg.wma.transparent, False, True, "suspect", None),
            ("ogg has no rank band", "Ogg", 500, False, True, "genuine", None),
            ("lossless is already final", "FLAC", 1000, False, True, "genuine", None),
        ]
        for description, format, bitrate, is_cbr, attempted, grade, error in cases:
            with self.subTest(description=description):
                self.assertIsNone(self._override(
                    format=format,
                    bitrate=bitrate,
                    is_cbr=is_cbr,
                    attempted=attempted,
                    grade=grade,
                    error=error,
                ))

    def test_absent_attempt_audit_never_uses_measurement_grade(self):
        from lib.quality import rejection_backfill_override

        cfg = QualityRankConfig.defaults()
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=cfg.mp3_cbr.transparent,
            avg_bitrate_kbps=cfg.mp3_cbr.transparent,
            format="MP3",
            is_cbr=True,
            spectral_grade="genuine",
        )

        self.assertIsNone(rejection_backfill_override(
            current_measurement=measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=None,
            cfg=cfg,
        ))


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
        ("vorbis bitrate label ignores measurement",
         f"vorbis {CFG.vorbis.transparent}", 1, False, QualityRank.TRANSPARENT),
        ("wma bitrate label ignores measurement",
         f"wma {CFG.wma.transparent}", 1, False, QualityRank.TRANSPARENT),

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

        # Vorbis: exact and immediately below every configured band edge.
        ("Vorbis transparent edge", "Vorbis", CFG.vorbis.transparent, False, QualityRank.TRANSPARENT),
        ("Vorbis below transparent", "Vorbis", CFG.vorbis.transparent - 1, False, QualityRank.EXCELLENT),
        ("Vorbis excellent edge", "Vorbis", CFG.vorbis.excellent, False, QualityRank.EXCELLENT),
        ("Vorbis below excellent", "Vorbis", CFG.vorbis.excellent - 1, False, QualityRank.GOOD),
        ("Vorbis good edge", "Vorbis", CFG.vorbis.good, False, QualityRank.GOOD),
        ("Vorbis below good", "Vorbis", CFG.vorbis.good - 1, False, QualityRank.ACCEPTABLE),
        ("Vorbis acceptable edge", "Vorbis", CFG.vorbis.acceptable, False, QualityRank.ACCEPTABLE),
        ("Vorbis below acceptable", "Vorbis", CFG.vorbis.acceptable - 1, False, QualityRank.POOR),

        # WMA: exact and immediately below every configured band edge.
        ("WMA transparent edge", "WMA", CFG.wma.transparent, False, QualityRank.TRANSPARENT),
        ("WMA below transparent", "WMA", CFG.wma.transparent - 1, False, QualityRank.EXCELLENT),
        ("WMA excellent edge", "WMA", CFG.wma.excellent, False, QualityRank.EXCELLENT),
        ("WMA below excellent", "WMA", CFG.wma.excellent - 1, False, QualityRank.GOOD),
        ("WMA good edge", "WMA", CFG.wma.good, False, QualityRank.GOOD),
        ("WMA below good", "WMA", CFG.wma.good - 1, False, QualityRank.ACCEPTABLE),
        ("WMA acceptable edge", "WMA", CFG.wma.acceptable, False, QualityRank.ACCEPTABLE),
        ("WMA below acceptable", "WMA", CFG.wma.acceptable - 1, False, QualityRank.POOR),

        # --- Step 6: unknown codec family ---
        ("unknown codec",                          "musepack",       200, False, QualityRank.UNKNOWN),
        ("unknown codec with bitrate label",       "musepack 192",  None, False, QualityRank.UNKNOWN),
        ("unsupported WMA vbr-ish label",          "wma v0",         None, False, QualityRank.UNKNOWN),
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

    def test_vorbis_and_wma_ignore_cbr_inference(self):
        for family, bands in (("Vorbis", CFG.vorbis), ("WMA", CFG.wma)):
            for bitrate in (
                bands.transparent, bands.transparent - 1,
                bands.excellent, bands.excellent - 1,
                bands.good, bands.good - 1,
                bands.acceptable, bands.acceptable - 1,
            ):
                with self.subTest(family=family, bitrate=bitrate):
                    self.assertEqual(
                        quality_rank(family, bitrate, False, CFG),
                        quality_rank(family, bitrate, True, CFG),
                    )

    def test_cross_codec_band_parity(self):
        cases = [
            ("transparent", "Vorbis", CFG.vorbis.transparent,
             "AAC", CFG.aac.transparent),
            ("excellent", "Vorbis", CFG.vorbis.excellent,
             "WMA", CFG.wma.excellent),
            ("good", "Vorbis", CFG.vorbis.good,
             "WMA", CFG.wma.good),
            ("acceptable", "Vorbis", CFG.vorbis.acceptable,
             "WMA", CFG.wma.acceptable),
        ]
        for desc, left_fmt, left_br, right_fmt, right_br in cases:
            with self.subTest(desc=desc):
                self.assertEqual(
                    quality_rank(left_fmt, left_br, False, CFG),
                    quality_rank(right_fmt, right_br, False, CFG),
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
                    self._m(**new_kw), self._m(**existing_kw), CFG).verdict
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
        self.assertEqual(compare_quality(new, existing, cfg_min).verdict, "better")
        # Under AVG: new=250, existing=260 → worse
        self.assertEqual(compare_quality(new, existing, CFG).verdict, "worse")

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
        self.assertEqual(compare_quality(new, existing, cfg_med).verdict, "better")
        # Under MIN: new=60 (POOR) vs existing=210 (EXCELLENT) → worse
        cfg_min = QualityRankConfig(bitrate_metric=RankBitrateMetric.MIN)
        self.assertEqual(compare_quality(new, existing, cfg_min).verdict, "worse")


class TestCompareQualitySharedSpectralBucket(unittest.TestCase):
    """Shared-spectral bucket: when BOTH measurements carry
    ``spectral_bitrate_kbps``, the comparison clamps each side's rank bucket
    to ``min(selected_metric, spectral)``.

    Independent spectral estimates that agree are stronger evidence than
    either alone, but same-bucket tie-breaks still use the raw configured
    bitrate metric so the pipeline can converge upward when spectral is too
    pessimistic. A single stale estimate (only one side) does NOT fire the
    clamp; that case still follows ``compute_effective_override_bitrate``'s
    grade-gated rule elsewhere in the pipeline. The clamp is also guarded
    against one important asymmetry: a transcode-grade candidate cannot use a
    higher spectral floor to beat a non-transcode-grade existing album when
    its real selected-metric rank is lower.

    Regression guard for the Grouper case: a shared 96kbps floor must not
    erase 219avg vs 128avg. Brian Eno's shared-floor scenario remains in this
    matrix too: it documents that equal buckets still let avg progress upward.
    """

    def _m(self, **kwargs: Any) -> AudioQualityMeasurement:
        return AudioQualityMeasurement(**kwargs)

    # (description, new_kwargs, existing_kwargs, expected)
    CASES = [
        # --- Both sides agree on 96 kbps floor → same bucket, avg wins ---
        # The Eno case: inflated container avg on new, existing uniform
        # at the floor, both spectral=96. Clamp drags both ranks to the
        # same bucket; raw avg is still the tiebreaker.
        ("Eno shape: both spectral=96, new avg=290, existing avg=128",
         dict(format="MP3", avg_bitrate_kbps=290, min_bitrate_kbps=128,
              spectral_bitrate_kbps=96),
         dict(format="MP3", avg_bitrate_kbps=128, min_bitrate_kbps=128,
              spectral_bitrate_kbps=96),
         "better"),
        ("both spectral=96, equal containers → still equivalent",
         dict(format="MP3", avg_bitrate_kbps=128, spectral_bitrate_kbps=96),
         dict(format="MP3", avg_bitrate_kbps=128, spectral_bitrate_kbps=96),
         "equivalent"),

        # --- Same spectral bucket still allows raw-metric progress ---
        ("new clamped rank == existing clamped rank → raw avg tiebreaker wins",
         dict(format="MP3", avg_bitrate_kbps=290, spectral_bitrate_kbps=96),
         dict(format="MP3", avg_bitrate_kbps=128, spectral_bitrate_kbps=96),
         "better"),

        # --- Different floors → clamped comparison decides ---
        ("new spectral=160 > existing spectral=96 → better after clamp",
         dict(format="MP3", avg_bitrate_kbps=290, spectral_bitrate_kbps=160),
         dict(format="MP3", avg_bitrate_kbps=128, spectral_bitrate_kbps=96),
         "better"),
        ("new spectral rank below existing spectral rank → worse after clamp",
         dict(format="MP3", avg_bitrate_kbps=290, spectral_bitrate_kbps=64),
         dict(format="MP3", avg_bitrate_kbps=170, spectral_bitrate_kbps=170),
         "worse"),

        # --- Only one side has spectral: clamp does NOT fire ---
        # Springsteen shape: existing CBR 320 has a stale 96 estimate, new
        # MP3 V0 240 has no spectral. The container-based comparison wins
        # and the existing 320 beats the 240 — test_springsteen_genuine_but_96kbps
        # (simulator) pins this at the full-pipeline level; this confirms
        # the rule holds inside compare_quality itself.
        ("existing-only spectral → no clamp, container comparison",
         dict(format="mp3 v0", avg_bitrate_kbps=240, is_cbr=False),
         dict(format="mp3 320", avg_bitrate_kbps=320, is_cbr=True,
              spectral_bitrate_kbps=96),
         "equivalent"),  # V0 and 320 are same-rank different-family → equivalent
        ("new-only spectral → no clamp either way",
         dict(format="MP3", avg_bitrate_kbps=290, is_cbr=False,
              spectral_bitrate_kbps=96),
         dict(format="MP3", avg_bitrate_kbps=128, is_cbr=False),
         "better"),  # Container comparison: 290 > 128

        # --- Label equivalence still short-circuits same-rank ties ---
        # Explicit labels are quality contracts; within the same rank tier,
        # they stay equivalent regardless of raw-bitrate deltas.
        ("both explicit labels + both spectral=96 → equivalent",
         dict(format="mp3 v0", avg_bitrate_kbps=240,
              spectral_bitrate_kbps=96),
         dict(format="mp3 v0", avg_bitrate_kbps=245,
              spectral_bitrate_kbps=96),
         "equivalent"),
    ]

    def test_shared_spectral_clamp_table(self):
        for desc, new_kw, existing_kw, expected in self.CASES:
            with self.subTest(desc=desc):
                result = compare_quality(
                    self._m(**new_kw), self._m(**existing_kw), CFG).verdict
                self.assertEqual(
                    result, expected,
                    f"{desc}: new={new_kw} existing={existing_kw} "
                    f"expected {expected!r} got {result!r}")

    def test_same_grade_clamp_still_uses_shared_spectral_floor(self):
        """The bucket still fires when both sides share a non-transcode grade.

        Unlike ``compute_effective_override_bitrate`` (which gates on
        SPECTRAL_TRANSCODE_GRADES), same-grade agreement between two
        independent estimates is still corroborating evidence. Verified by
        passing grade=genuine on both sides and confirming the rank bucket
        still applies while the raw avg tiebreaker wins.
        """
        new = self._m(format="MP3", avg_bitrate_kbps=290,
                      spectral_grade="genuine", spectral_bitrate_kbps=96)
        existing = self._m(format="MP3", avg_bitrate_kbps=128,
                           spectral_grade="genuine", spectral_bitrate_kbps=96)
        self.assertEqual(compare_quality(new, existing, CFG).verdict, "better")

    def test_transcode_candidate_cannot_spectral_floor_past_lower_real_rank(self):
        """Muse live shape: spectral floor improved, real quality rank regressed.

        The candidate is likely_transcode at ~160k spectral, while the existing
        album is genuine with a partial ~128k spectral floor. The shared
        spectral bucket alone would call 160 > 128 an upgrade, but the actual
        selected metric regressed from TRANSPARENT avg=261 to GOOD avg=196.
        That must be a downgrade, not an import.
        """
        new = self._m(
            format="MP3",
            min_bitrate_kbps=171,
            avg_bitrate_kbps=196,
            median_bitrate_kbps=196,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate_kbps=160,
        )
        existing = self._m(
            format="MP3",
            min_bitrate_kbps=246,
            avg_bitrate_kbps=261,
            median_bitrate_kbps=259,
            is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate_kbps=128,
        )

        self.assertEqual(compare_quality(new, existing, CFG).verdict, "worse")
        self.assertEqual(import_quality_decision(new, existing, cfg=CFG).decision,
                         "downgrade")

    def test_transcode_guard_requires_known_non_transcode_existing_grade(self):
        """Unknown existing grade keeps the backward-compatible shared bucket."""
        new = self._m(
            format="MP3",
            avg_bitrate_kbps=196,
            spectral_grade="likely_transcode",
            spectral_bitrate_kbps=160,
        )
        existing = self._m(
            format="MP3",
            avg_bitrate_kbps=261,
            spectral_bitrate_kbps=128,
        )

        self.assertEqual(compare_quality(new, existing, CFG).verdict, "better")

    def test_transcode_candidate_can_still_import_when_real_rank_does_not_regress(self):
        """Bay of Biscay shape: spectral and actual selected metric both improve."""
        new = self._m(
            format="MP3",
            min_bitrate_kbps=119,
            avg_bitrate_kbps=179,
            median_bitrate_kbps=181,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate_kbps=160,
        )
        existing = self._m(
            format="MP3",
            min_bitrate_kbps=128,
            avg_bitrate_kbps=172,
            median_bitrate_kbps=192,
            is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate_kbps=128,
        )

        self.assertEqual(compare_quality(new, existing, CFG).verdict, "better")
        self.assertEqual(import_quality_decision(new, existing, cfg=CFG).decision,
                         "import")

    def test_transcode_over_transcode_still_uses_shared_spectral_floor(self):
        """The guard only covers transcode-grade over known non-transcode."""
        new = self._m(
            format="MP3",
            avg_bitrate_kbps=196,
            spectral_grade="likely_transcode",
            spectral_bitrate_kbps=160,
        )
        existing = self._m(
            format="MP3",
            avg_bitrate_kbps=261,
            spectral_grade="suspect",
            spectral_bitrate_kbps=128,
        )

        self.assertEqual(compare_quality(new, existing, CFG).verdict, "better")

    def test_equal_spectral_bucket_keeps_raw_avg_tiebreaker(self):
        """Grouper live case: equal spectral floor must not erase avg upgrade."""
        new = self._m(
            format="MP3",
            min_bitrate_kbps=209,
            avg_bitrate_kbps=219,
            median_bitrate_kbps=216,
            is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate_kbps=96,
        )
        existing = self._m(
            format="MP3",
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            median_bitrate_kbps=128,
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate_kbps=96,
        )
        self.assertEqual(compare_quality(new, existing, CFG).verdict, "better")


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
            "vorbis.transparent = 200\n"
            "vorbis.excellent = 170\n"
            "vorbis.good = 120\n"
            "vorbis.acceptable = 100\n"
            "wma.transparent = 321\n"
            "wma.excellent = 257\n"
            "wma.good = 193\n"
            "wma.acceptable = 129\n"
        )
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.MIN)
        self.assertEqual(cfg.within_rank_tolerance_kbps, 10)
        self.assertEqual(cfg.opus.transparent, 120)
        self.assertEqual(cfg.mp3_vbr.transparent, 220)
        self.assertEqual(cfg.mp3_cbr.excellent, 250)
        self.assertEqual(cfg.aac.acceptable, 90)
        self.assertEqual(cfg.vorbis.excellent, 170)
        self.assertEqual(cfg.wma.acceptable, 129)

    def test_invalid_metric_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse("[Quality Ranks]\nbitrate_metric = harmonic_mean\n")
        self.assertIn("bitrate_metric", str(ctx.exception))

    def test_median_metric_parses(self):
        """`bitrate_metric = median` is a valid policy (issue #64)."""
        cfg = self._parse("[Quality Ranks]\nbitrate_metric = median\n")
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.MEDIAN)

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

    def test_case_insensitive_metric(self):
        cfg = self._parse("[Quality Ranks]\nbitrate_metric = AVG\n")
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.AVG)

    def test_empty_value_falls_through_to_default(self):
        """Empty `key =` should yield the default, matching _get_int behavior."""
        cfg = self._parse(
            "[Quality Ranks]\n"
            "bitrate_metric = \n"
        )
        self.assertEqual(cfg.bitrate_metric, RankBitrateMetric.AVG)

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
            within_rank_tolerance_kbps=8,
            opus=CodecRankBands(transparent=120, excellent=100, good=80, acceptable=60),
            vorbis=CodecRankBands(transparent=200, excellent=170, good=120, acceptable=100),
            wma=CodecRankBands(transparent=321, excellent=257, good=193, acceptable=129),
        )
        payload = original.to_json()
        restored = QualityRankConfig.from_json(payload)
        self.assertEqual(restored, original)
        self.assertEqual(restored.opus.transparent, 120)
        self.assertEqual(restored.vorbis.excellent, 170)
        self.assertEqual(restored.wma.acceptable, 129)

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
            "bitrate_metric", "within_rank_tolerance_kbps",
            "opus", "mp3_vbr", "mp3_cbr", "aac", "vorbis", "wma",
            "mp3_vbr_levels", "lossless_codecs", "mixed_format_precedence",
        }
        self.assertEqual(set(payload.keys()), expected_keys)

    def test_json_vbr_ranks_are_ints(self):
        payload = json.loads(QualityRankConfig.defaults().to_json())
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

    def test_from_json_invalid_vbr_rank_int_raises_value_error(self):
        """Out-of-range VBR rank ints raise ValueError, not a bare enum error."""
        import json as _json
        payload = _json.loads(QualityRankConfig.defaults().to_json())
        payload["mp3_vbr_levels"][0] = 9999
        with self.assertRaises(ValueError) as ctx:
            QualityRankConfig.from_json(_json.dumps(payload))
        self.assertIn("failed to reconstruct", str(ctx.exception))


class TestQualityRankConfigDefaults(unittest.TestCase):
    """Lock the default policy values so changes are explicit.

    **These values are mirrored in the upstream NixOS module at**
    ``nix/module.nix`` → ``services.cratedigger.qualityRanks.*`` (see README
    § "Tuning the quality rank model" for the deployment surface and
    issue #67 for the rationale). If you change a default here, you
    MUST also update the Nix defaults, otherwise a fresh
    ``nixos-rebuild switch`` will revert the change on any consumer.

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

    def test_default_within_rank_tolerance(self):
        self.assertEqual(CFG.within_rank_tolerance_kbps, 5)

    def test_default_lossless_codecs(self):
        self.assertEqual(
            CFG.lossless_codecs,
            frozenset({"flac", "lossless", "alac", "wav"}))

    def test_default_mixed_format_precedence_worst_first(self):
        self.assertEqual(
            CFG.mixed_format_precedence,
            ("wma", "mp3", "vorbis", "aac", "opus", "flac"),
        )

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

    def test_default_vorbis_bands(self):
        self.assertEqual(CFG.vorbis.transparent, 192)
        self.assertEqual(CFG.vorbis.excellent, 160)
        self.assertEqual(CFG.vorbis.good, 112)
        self.assertEqual(CFG.vorbis.acceptable, 96)

    def test_default_wma_bands(self):
        self.assertEqual(CFG.wma.transparent, 320)
        self.assertEqual(CFG.wma.excellent, 256)
        self.assertEqual(CFG.wma.good, 192)
        self.assertEqual(CFG.wma.acceptable, 128)


# ============================================================================
# detect_release_source
# ============================================================================

class TestDetectReleaseSource(unittest.TestCase):
    """Test source detection from release ID format."""

    CASES = [
        # desc, id_string, expected
        ("MB UUID", "89ad4ac3-39f7-470e-963a-56509c546377", "musicbrainz"),
        ("MB UUID uppercase", "89AD4AC3-39F7-470E-963A-56509C546377", "musicbrainz"),
        ("MB UUID uppercase with whitespace", " 89AD4AC3-39F7-470E-963A-56509C546377 ", "musicbrainz"),
        ("Discogs numeric", "2048516", "discogs"),
        ("Discogs large numeric", "13524141", "discogs"),
        ("Discogs single digit", "1", "discogs"),
        ("Discogs numeric with whitespace", " 2048516 ", "discogs"),
        ("empty string", "", "unknown"),
        ("zero sentinel", "0", "unknown"),
        ("NONE string", "NONE", "unknown"),
        ("random text", "not-a-valid-id", "unknown"),
        ("partial UUID no hyphens", "89ad4ac339f7470e963a56509c546377", "unknown"),
    ]

    def test_detect_release_source(self):
        from lib.release_identity import detect_release_source
        for desc, id_string, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(detect_release_source(id_string), expected)


if __name__ == "__main__":
    unittest.main()
