"""Exact tolerance boundaries and source-probe comparison explanations."""

import unittest
from dataclasses import replace

from lib.quality.compare import compare_quality
from lib.quality.evidence_types import (
    AudioQualityMeasurement,
    QualityComparisonBasis,
    TargetQualityContract,
    V0ProbeEvidence,
)
from lib.quality.ranks import QualityRankConfig


class TestComparisonBasisContract(unittest.TestCase):
    def test_equal_classes_with_one_missing_raw_metric_remain_equivalent(self):
        for new_rate, existing_rate in ((None, 320), (320, None)):
            with self.subTest(new_rate=new_rate):
                new = AudioQualityMeasurement(
                    format="MP3", avg_bitrate_kbps=new_rate,
                    spectral_grade="likely_transcode", spectral_bitrate_kbps=128)
                existing = AudioQualityMeasurement(
                    format="MP3", avg_bitrate_kbps=existing_rate,
                    spectral_grade="likely_transcode", spectral_bitrate_kbps=128)
                basis = compare_quality(new, existing, QualityRankConfig.defaults())
                self.assertEqual(basis.verdict, "equivalent")
                self.assertEqual(basis.branch, "metric_missing")
                self.assertEqual(basis.new_value_kbps, new_rate)
                self.assertEqual(basis.existing_value_kbps, existing_rate)
                self.assertIs(basis.spectral_clamped, True)

    def test_unknown_and_lossless_same_rank_have_no_spectral_clamp(self):
        for format_hint, rank, branch in (
            ("UNMAPPED", "unknown", "metric_missing"),
            ("FLAC", "lossless", "lossless_same_rank"),
        ):
            with self.subTest(format=format_hint):
                basis = compare_quality(
                    AudioQualityMeasurement(format=format_hint, avg_bitrate_kbps=800),
                    AudioQualityMeasurement(format=format_hint, avg_bitrate_kbps=1100),
                    QualityRankConfig.defaults(),
                )
                self.assertEqual(basis.branch, branch)
                self.assertEqual((basis.new_rank, basis.existing_rank), (rank, rank))
                self.assertEqual(basis.verdict, "equivalent")
                self.assertIs(basis.spectral_clamped, False)

    def test_shared_classes_report_the_same_rank_spectral_downgrade(self):
        # MP3 classes 192 and 224 share the good rank but remain distinct
        # measured bounds. Identical raw 320k containers cannot erase them.
        basis = compare_quality(
            AudioQualityMeasurement(
                format="MP3", avg_bitrate_kbps=320,
                spectral_grade="likely_transcode", spectral_bitrate_kbps=192,
            ),
            AudioQualityMeasurement(
                format="MP3", avg_bitrate_kbps=320,
                spectral_grade="likely_transcode", spectral_bitrate_kbps=224,
            ),
            QualityRankConfig.defaults(),
        )
        self.assertEqual(basis, QualityComparisonBasis(
            verdict="worse", branch="spectral_tiebreak",
            new_rank="good", existing_rank="good",
            new_metric="avg", existing_metric="avg",
            new_value_kbps=192, existing_value_kbps=224,
            new_format="mp3", existing_format="mp3", spectral_clamped=True,
        ))

    def test_existing_explicit_label_preserves_the_new_measured_value(self):
        # The existing basis table puts the contract on the candidate. This
        # mirror must retain the native candidate's measured average.
        # The legacy API also accepts a stored V0 label with later spectral
        # measurements: the label names its encoding, not lossless ancestry.
        for label, declared_value, existing_class in (
            ("MP3 V0", None, None), ("MP3 320", 320, None),
            ("MP3 V0", None, 128),
        ):
            with self.subTest(label=label, existing_class=existing_class):
                grade = "likely_transcode" if existing_class is not None else None
                new = AudioQualityMeasurement(
                    format="MP3", avg_bitrate_kbps=320, spectral_grade=grade,
                    spectral_bitrate_kbps=320 if existing_class is not None else None,
                )
                existing = AudioQualityMeasurement(
                    format=label, avg_bitrate_kbps=207, spectral_grade=grade,
                    spectral_bitrate_kbps=existing_class,
                )
                self.assertEqual(
                    compare_quality(new, existing, QualityRankConfig.defaults()),
                    QualityComparisonBasis(
                        verdict="equivalent", branch="label_contract_same_rank",
                        new_rank="transparent", existing_rank="transparent",
                        new_metric="avg", existing_metric="contract",
                        new_value_kbps=320, existing_value_kbps=declared_value,
                        new_format="mp3", existing_format=label.lower(),
                        spectral_clamped=existing_class is not None,
                    ),
                )

    def test_one_class_same_family_reports_effective_values_and_tolerance(self):
        # Class 160 and the clean raw values share the acceptable band.
        # Class 192 versus clean 191 also crosses the good-band edge.
        cases = (
            (160, 160, 5, "equivalent", None),
            (160, 155, 5, "equivalent", 5),
            (160, 154, 5, "better", None),
            (160, 165, 5, "equivalent", 5),
            (160, 166, 5, "worse", None),
            (160, 159, 0, "better", None),
            (192, 191, 5, "equivalent", 5),
        )
        for bound, clean_rate, tolerance, forward_verdict, applied_tolerance in cases:
            bounded = AudioQualityMeasurement(
                format="MP3", min_bitrate_kbps=320, avg_bitrate_kbps=320,
                spectral_grade="likely_transcode", spectral_bitrate_kbps=bound,
                codec_family="mp3",
            )
            bound_rank = "good" if bound == 192 else "acceptable"
            clean = AudioQualityMeasurement(
                format="MP3", min_bitrate_kbps=clean_rate,
                avg_bitrate_kbps=clean_rate, spectral_grade="genuine",
                codec_family="mp3",
            )
            cfg = replace(QualityRankConfig.defaults(), within_rank_tolerance_kbps=tolerance)
            for reversed_roles in (False, True):
                with self.subTest(rate=clean_rate, tolerance=tolerance, reversed=reversed_roles):
                    new, existing = (clean, bounded) if reversed_roles else (bounded, clean)
                    verdict = (
                        {"better": "worse", "worse": "better", "equivalent": "equivalent"}[
                            forward_verdict
                        ] if reversed_roles else forward_verdict
                    )
                    self.assertEqual(compare_quality(new, existing, cfg), QualityComparisonBasis(
                        verdict=verdict,
                        branch="spectral_existing_bound" if reversed_roles else "spectral_candidate_bound",
                        new_rank="acceptable" if reversed_roles else bound_rank,
                        existing_rank=bound_rank if reversed_roles else "acceptable",
                        new_metric="avg", existing_metric="avg",
                        new_value_kbps=clean_rate if reversed_roles else bound,
                        existing_value_kbps=bound if reversed_roles else clean_rate,
                        new_format="mp3", existing_format="mp3",
                        spectral_clamped=True, tolerance_kbps=applied_tolerance,
                    ))

    def test_raw_same_family_tolerance_boundary_and_both_directions(self):
        # Every value is inside the default excellent MP3 band 256..319.
        # At zero tolerance a one-kbps difference remains directional.
        for lower, tolerance, forward_verdict in (
            (280, 5, "equivalent"), (275, 5, "equivalent"),
            (274, 5, "better"), (279, 0, "better"),
        ):
            for reversed_roles in (False, True):
                with self.subTest(lower=lower, tolerance=tolerance, reversed=reversed_roles):
                    new_rate, existing_rate = (lower, 280) if reversed_roles else (280, lower)
                    new = AudioQualityMeasurement(format="MP3", avg_bitrate_kbps=new_rate)
                    existing = AudioQualityMeasurement(format="MP3", avg_bitrate_kbps=existing_rate)
                    cfg = replace(QualityRankConfig.defaults(), within_rank_tolerance_kbps=tolerance)
                    verdict = "worse" if reversed_roles and forward_verdict == "better" else forward_verdict
                    self.assertEqual(compare_quality(new, existing, cfg), QualityComparisonBasis(
                        verdict=verdict, branch="metric_tiebreak",
                        new_rank="excellent", existing_rank="excellent",
                        new_metric="avg", existing_metric="avg",
                        new_value_kbps=new_rate, existing_value_kbps=existing_rate,
                        new_format="mp3", existing_format="mp3",
                        spectral_clamped=False, tolerance_kbps=tolerance,
                    ))

    def test_lossless_source_probe_and_target_keep_their_own_audit_fields(self):
        # Source 800k describes FLAC bytes. The temporary V0 statistic ranks
        # a measured MP3 projection; an explicit Opus target ranks its 128k
        # contract instead. Neither audit may label the FLAC 800k as MP3.
        mp3_projection = TargetQualityContract.from_projection("MP3", projected_is_cbr=False)
        cases = (
            (
                "transcode real-rank guard uses target and source probe",
                "suspect", 192, mp3_projection,
                AudioQualityMeasurement(format="MP3", avg_bitrate_kbps=260, spectral_grade="genuine"),
                QualityComparisonBasis(
                    verdict="worse", branch="transcode_rank_regression",
                    new_rank="good", existing_rank="excellent",
                    new_metric="avg", existing_metric="avg",
                    new_value_kbps=192, existing_value_kbps=260,
                    new_format="mp3", existing_format="mp3", spectral_clamped=False,
                ),
            ),
            (
                "clean measured projection compares the probe statistic",
                "genuine", 280, mp3_projection,
                AudioQualityMeasurement(format="MP3", avg_bitrate_kbps=260, spectral_grade="genuine"),
                QualityComparisonBasis(
                    verdict="better", branch="metric_tiebreak",
                    new_rank="excellent", existing_rank="excellent",
                    new_metric="avg", existing_metric="avg",
                    new_value_kbps=280, existing_value_kbps=260,
                    new_format="mp3", existing_format="mp3",
                    spectral_clamped=False, tolerance_kbps=5,
                ),
            ),
            (
                "existing class compares against the clean source probe",
                "genuine", 155, mp3_projection,
                AudioQualityMeasurement(
                    format="MP3", avg_bitrate_kbps=320,
                    spectral_grade="likely_transcode", spectral_bitrate_kbps=160,
                    codec_family="mp3",
                ),
                QualityComparisonBasis(
                    verdict="equivalent", branch="spectral_existing_bound",
                    new_rank="acceptable", existing_rank="acceptable",
                    new_metric="avg", existing_metric="avg",
                    new_value_kbps=155, existing_value_kbps=160,
                    new_format="mp3", existing_format="mp3",
                    spectral_clamped=True, tolerance_kbps=5,
                ),
            ),
            (
                "numeric target records its contract rather than the V0 probe",
                "suspect", 192, TargetQualityContract.from_explicit_label("Opus 128"),
                AudioQualityMeasurement(format="MP3", avg_bitrate_kbps=320, spectral_grade="genuine"),
                QualityComparisonBasis(
                    verdict="equivalent", branch="cross_family_same_rank",
                    new_rank="transparent", existing_rank="transparent",
                    new_metric="contract", existing_metric="avg",
                    new_value_kbps=128, existing_value_kbps=320,
                    new_format="opus 128", existing_format="mp3", spectral_clamped=False,
                ),
            ),
        )
        for description, grade, probe_rate, target, existing, expected in cases:
            with self.subTest(case=description):
                source = AudioQualityMeasurement(
                    format="FLAC", min_bitrate_kbps=800, spectral_grade=grade,
                    spectral_subject="source", spectral_provenance="measured",
                    codec_family="lossless",
                )
                probe = V0ProbeEvidence(
                    kind="lossless_source_v0", min_bitrate_kbps=120,
                    avg_bitrate_kbps=probe_rate,
                )
                self.assertEqual(compare_quality(
                    source, existing, QualityRankConfig.defaults(),
                    new_target_contract=target, new_v0_probe=probe,
                ), expected)


if __name__ == "__main__":
    unittest.main()
