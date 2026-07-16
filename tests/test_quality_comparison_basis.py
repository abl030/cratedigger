"""QualityComparisonBasis — the decision's own explanation, persisted.

Pins invariants I1-I4 from the comparison-basis feature: every branch of
``compare_quality`` emits a basis naming the branch that fired, the per-side
ranks, and the values that decided it; ``import_quality_decision`` threads
the basis and records the verified-lossless bypass; ``measured_import_decision``
surfaces it; ``ImportResult`` round-trips it across the JSONB wire boundary.

Generated twin: tests/test_quality_generated.py (basis-consistency property).
"""

import unittest

import msgspec

from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    MeasuredImportDecisionInput,
    QualityComparisonBasis,
    QualityRankConfig,
    compare_quality,
    import_quality_decision,
    measured_import_decision,
)


def _m(**kwargs) -> AudioQualityMeasurement:
    return AudioQualityMeasurement(**kwargs)


CFG = QualityRankConfig.defaults()  # bitrate_metric=avg, mp3_vbr 245/210/170/130


class TestCompareQualityBasisBranches(unittest.TestCase):
    """Each compare_quality branch emits a truthful, branch-aware basis."""

    # (desc, new, existing, expected-basis-fields)
    CASES = [
        (
            "rank upgrade on avg — the Say Hello to My Kids case (dl 36608)",
            _m(min_bitrate_kbps=194, avg_bitrate_kbps=288, format="MP3"),
            _m(min_bitrate_kbps=194, avg_bitrate_kbps=196, format="MP3"),
            dict(verdict="better", branch="rank",
                 new_rank="transparent", existing_rank="good",
                 new_value_kbps=288, existing_value_kbps=196,
                 new_metric="avg", existing_metric="avg",
                 spectral_clamped=False),
        ),
        (
            "rank downgrade mirrors the same basis",
            _m(min_bitrate_kbps=194, avg_bitrate_kbps=196, format="MP3"),
            _m(min_bitrate_kbps=194, avg_bitrate_kbps=288, format="MP3"),
            dict(verdict="worse", branch="rank",
                 new_rank="good", existing_rank="transparent",
                 new_value_kbps=196, existing_value_kbps=288),
        ),
        (
            "lossless vs lossless is equivalent by identity, not bitrate",
            _m(format="flac"),
            _m(format="flac"),
            dict(verdict="equivalent", branch="lossless_same_rank",
                 new_rank="lossless", existing_rank="lossless"),
        ),
        (
            "cross-family same rank: opus transparent vs mp3 transparent",
            _m(avg_bitrate_kbps=120, format="opus"),
            _m(avg_bitrate_kbps=250, format="MP3"),
            dict(verdict="equivalent", branch="cross_family_same_rank",
                 new_rank="transparent", existing_rank="transparent"),
        ),
        (
            "explicit label is a contract: mp3 v0 vs bare MP3 transparent",
            _m(avg_bitrate_kbps=207, format="mp3 v0"),
            _m(avg_bitrate_kbps=250, format="MP3"),
            dict(verdict="equivalent", branch="label_contract_same_rank",
                 new_rank="transparent", existing_rank="transparent",
                 new_metric="contract", new_value_kbps=None,
                 existing_metric="avg", existing_value_kbps=250),
        ),
        (
            "Gas November 89: Opus target is a contract, not the V0 proxy min",
            _m(min_bitrate_kbps=191, avg_bitrate_kbps=224,
               median_bitrate_kbps=237, format="opus 128"),
            _m(min_bitrate_kbps=128, avg_bitrate_kbps=128,
               median_bitrate_kbps=128, format="MP3", is_cbr=True),
            dict(verdict="better", branch="rank",
                 new_rank="transparent", existing_rank="acceptable",
                 new_metric="contract", new_value_kbps=128,
                 existing_metric="avg", existing_value_kbps=128),
        ),
        (
            "same-rank tiebreak better: raw metric delta beyond tolerance",
            _m(avg_bitrate_kbps=260, format="MP3"),
            _m(avg_bitrate_kbps=250, format="MP3"),
            dict(verdict="better", branch="metric_tiebreak",
                 new_rank="transparent", existing_rank="transparent",
                 new_value_kbps=260, existing_value_kbps=250,
                 tolerance_kbps=5),
        ),
        (
            "same-rank tiebreak within tolerance is equivalent",
            _m(avg_bitrate_kbps=250, format="MP3"),
            _m(avg_bitrate_kbps=248, format="MP3"),
            dict(verdict="equivalent", branch="metric_tiebreak",
                 new_value_kbps=250, existing_value_kbps=248),
        ),
        (
            "both sides unmeasurable: metric_missing equivalence",
            _m(format="MP3"),
            _m(format="MP3"),
            dict(verdict="equivalent", branch="metric_missing",
                 new_rank="unknown", existing_rank="unknown",
                 new_value_kbps=None, existing_value_kbps=None),
        ),
        (
            "transcode-grade candidate regressing real rank is worse pre-clamp",
            _m(avg_bitrate_kbps=180, format="MP3", spectral_grade="suspect"),
            _m(avg_bitrate_kbps=250, format="MP3", spectral_grade="genuine"),
            dict(verdict="worse", branch="transcode_rank_regression",
                 new_rank="good", existing_rank="transparent",
                 new_value_kbps=180, existing_value_kbps=250),
        ),
        (
            "shared-spectral clamp deciding rank shows the clamped values",
            _m(avg_bitrate_kbps=288, format="MP3",
               spectral_grade="genuine", spectral_bitrate_kbps=250),
            _m(avg_bitrate_kbps=196, format="MP3",
               spectral_grade="genuine", spectral_bitrate_kbps=250),
            dict(verdict="better", branch="rank",
                 new_rank="transparent", existing_rank="good",
                 new_value_kbps=250, existing_value_kbps=196,
                 spectral_clamped=True),
        ),
        (
            "shared-spectral clamp landing same-rank tie-breaks on RAW metric",
            _m(avg_bitrate_kbps=288, format="MP3",
               spectral_grade="genuine", spectral_bitrate_kbps=200),
            _m(avg_bitrate_kbps=196, format="MP3",
               spectral_grade="genuine", spectral_bitrate_kbps=240),
            dict(verdict="better", branch="metric_tiebreak",
                 new_rank="good", existing_rank="good",
                 new_value_kbps=288, existing_value_kbps=196,
                 spectral_clamped=True),
        ),
        (
            "per-side metric fallback: legacy existing with only min says so",
            _m(min_bitrate_kbps=194, avg_bitrate_kbps=288, format="MP3"),
            _m(min_bitrate_kbps=194, format="MP3"),
            dict(verdict="better", branch="rank",
                 new_rank="transparent", existing_rank="good",
                 new_value_kbps=288, existing_value_kbps=194,
                 new_metric="avg", existing_metric="min"),
        ),
    ]

    def test_branch_table(self):
        for desc, new, existing, expected in self.CASES:
            with self.subTest(desc=desc):
                basis = compare_quality(new, existing, CFG)
                self.assertIsInstance(basis, QualityComparisonBasis)
                for field_name, want in expected.items():
                    self.assertEqual(
                        getattr(basis, field_name), want,
                        f"{desc}: basis.{field_name}")

    def test_formats_carried_for_display_lowercase_normalized(self):
        """Format hints are lowercase-normalized at emission: the simulator
        and evidence twins spell the same hint differently ("flac"/"FLAC")
        and the parity property compares bases verbatim."""
        basis = compare_quality(
            _m(avg_bitrate_kbps=288, format="MP3"),
            _m(avg_bitrate_kbps=196, format="mp3 v0"),
            CFG,
        )
        self.assertEqual(basis.new_format, "mp3")
        self.assertEqual(basis.existing_format, "mp3 v0")

    def test_verified_lossless_bypass_defaults_false_from_compare(self):
        """compare_quality never sets the bypass — that's the caller's fact."""
        basis = compare_quality(
            _m(avg_bitrate_kbps=250, format="MP3"),
            _m(avg_bitrate_kbps=248, format="MP3"),
            CFG,
        )
        self.assertFalse(basis.verified_lossless_bypass)


class TestImportQualityDecisionBasis(unittest.TestCase):
    """Decision threading: basis rides the result, bypass is recorded."""

    def test_no_existing_has_no_basis(self):
        result = import_quality_decision(
            _m(avg_bitrate_kbps=288, format="MP3"), None, cfg=CFG)
        self.assertEqual(result.decision, "import")
        self.assertIsNone(result.basis)

    def test_no_existing_transcode_first_has_no_basis(self):
        result = import_quality_decision(
            _m(avg_bitrate_kbps=288, format="MP3"), None,
            is_transcode=True, cfg=CFG)
        self.assertEqual(result.decision, "transcode_first")
        self.assertIsNone(result.basis)

    def test_better_threads_basis_without_bypass(self):
        result = import_quality_decision(
            _m(avg_bitrate_kbps=288, format="MP3"),
            _m(avg_bitrate_kbps=196, format="MP3"), cfg=CFG)
        self.assertEqual(result.decision, "import")
        assert result.basis is not None
        self.assertEqual(result.basis.verdict, "better")
        self.assertFalse(result.basis.verified_lossless_bypass)

    def test_equivalent_verified_lossless_records_bypass(self):
        result = import_quality_decision(
            _m(avg_bitrate_kbps=250, format="MP3"),
            _m(avg_bitrate_kbps=248, format="MP3"),
            cfg=CFG,
            verified_lossless_proof=True,
        )
        self.assertEqual(result.decision, "import")
        assert result.basis is not None
        self.assertEqual(result.basis.verdict, "equivalent")
        self.assertTrue(result.basis.verified_lossless_bypass)

    def test_better_verified_lossless_does_not_claim_bypass(self):
        """The bypass flag means the bypass CHANGED the outcome — not merely
        that verified_lossless was true."""
        result = import_quality_decision(
            _m(avg_bitrate_kbps=288, format="MP3"),
            _m(avg_bitrate_kbps=196, format="MP3"),
            cfg=CFG,
            verified_lossless_proof=True,
        )
        self.assertEqual(result.decision, "import")
        assert result.basis is not None
        self.assertFalse(result.basis.verified_lossless_bypass)

    def test_equivalent_without_vl_is_downgrade_no_bypass(self):
        result = import_quality_decision(
            _m(avg_bitrate_kbps=250, format="MP3"),
            _m(avg_bitrate_kbps=248, format="MP3"), cfg=CFG)
        self.assertEqual(result.decision, "downgrade")
        assert result.basis is not None
        self.assertFalse(result.basis.verified_lossless_bypass)

    def test_worse_verified_lossless_still_blocked_no_bypass(self):
        result = import_quality_decision(
            _m(avg_bitrate_kbps=196, format="MP3"),
            _m(avg_bitrate_kbps=288, format="MP3"),
            cfg=CFG,
            verified_lossless_proof=True,
        )
        self.assertEqual(result.decision, "downgrade")
        assert result.basis is not None
        self.assertEqual(result.basis.verdict, "worse")
        self.assertFalse(result.basis.verified_lossless_bypass)

    def test_transcode_variants_carry_same_basis(self):
        result = import_quality_decision(
            _m(avg_bitrate_kbps=288, format="MP3"),
            _m(avg_bitrate_kbps=196, format="MP3"),
            is_transcode=True, cfg=CFG)
        self.assertEqual(result.decision, "transcode_upgrade")
        assert result.basis is not None
        self.assertEqual(result.basis.verdict, "better")


class TestMeasuredImportDecisionBasis(unittest.TestCase):
    """I1: basis present iff an existing measurement was compared."""

    def test_with_existing_surfaces_basis(self):
        result = measured_import_decision(
            MeasuredImportDecisionInput(
                _m(avg_bitrate_kbps=288, format="MP3"),
                _m(avg_bitrate_kbps=196, format="MP3"),
            ),
            cfg=CFG,
        )
        self.assertEqual(result.decision, "import")
        assert result.comparison_basis is not None
        self.assertEqual(result.comparison_basis.verdict, "better")

    def test_without_existing_has_none(self):
        result = measured_import_decision(
            MeasuredImportDecisionInput(
                _m(avg_bitrate_kbps=288, format="MP3"), None),
            cfg=CFG,
        )
        self.assertIsNone(result.comparison_basis)


class TestImportResultBasisWireBoundary(unittest.TestCase):
    """I5: the basis round-trips the JSONB boundary; drift is rejected."""

    def _basis(self) -> QualityComparisonBasis:
        return compare_quality(
            _m(avg_bitrate_kbps=288, format="MP3"),
            _m(avg_bitrate_kbps=196, format="MP3"),
            CFG,
        )

    def test_round_trip_preserves_every_field(self):
        ir = ImportResult(decision="import", comparison_basis=self._basis())
        decoded = ImportResult.from_json(ir.to_json())
        self.assertEqual(decoded.comparison_basis, self._basis())

    def test_absent_field_decodes_as_none(self):
        """Historical JSONB rows predate the field — must decode cleanly."""
        ir = ImportResult(decision="import")
        raw = msgspec.json.decode(ir.to_json().encode())
        raw.pop("comparison_basis", None)
        decoded = ImportResult.from_dict(raw)
        self.assertIsNone(decoded.comparison_basis)

    def test_wrong_type_at_boundary_raises(self):
        ir = ImportResult(decision="import", comparison_basis=self._basis())
        raw = msgspec.json.decode(ir.to_json().encode())
        raw["comparison_basis"]["verdict"] = 123
        with self.assertRaises(msgspec.ValidationError):
            ImportResult.from_dict(raw)


if __name__ == "__main__":
    unittest.main()
