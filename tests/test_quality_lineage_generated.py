"""Generated contracts for source, probe, target, and output lineage."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

from harness.import_one import projected_is_cbr_from_bitrates
from lib.quality import (
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    ImportResult,
    MeasuredImportDecisionInput,
    QualityRankConfig,
    TargetQualityContract,
    V0ProbeEvidence,
    full_pipeline_decision,
    measured_import_decision,
    quality_gate_decision,
)
from lib.quality_evidence import evidence_from_import_result


def assert_source_target_lineage(result: ImportResult) -> None:
    """Independent checker for the unambiguous v3 measurement shape."""

    source = result.source_measurement
    if source is not None:
        if source.format is not None and len(source.format.strip().split()) != 1:
            raise AssertionError("source measurement must use a bare codec label")
        if source.was_converted_from is not None:
            raise AssertionError("source measurement must not carry output lineage")
    output = result.materialized_measurement
    if (
        output is not None
        and output.format is not None
        and len(output.format.strip().split()) != 1
    ):
        raise AssertionError("output measurement must use a bare codec label")


class TestQualityLineagePins(unittest.TestCase):
    def test_flac_to_opus_keeps_four_facts_separate(self):
        source = AudioQualityMeasurement(
            min_bitrate_kbps=742,
            avg_bitrate_kbps=811,
            median_bitrate_kbps=803,
            format="FLAC",
            verified_lossless=True,
        )
        probe = V0ProbeEvidence(
            kind="lossless_source_v0",
            min_bitrate_kbps=191,
            avg_bitrate_kbps=224,
            median_bitrate_kbps=237,
        )
        output = AudioQualityMeasurement(
            min_bitrate_kbps=121,
            avg_bitrate_kbps=128,
            median_bitrate_kbps=127,
            format="Opus",
            was_converted_from="flac",
        )
        result = ImportResult(
            source_measurement=source,
            v0_probe=probe,
            target_quality_contract=TargetQualityContract.from_format("opus 128"),
            materialized_measurement=output,
        )

        assert_source_target_lineage(result)
        decoded = ImportResult.from_json(result.to_json())
        self.assertEqual(decoded.version, 3)
        self.assertEqual(decoded.source_measurement, source)
        self.assertEqual(decoded.v0_probe, probe)
        self.assertIsNotNone(decoded.target_quality_contract)
        assert decoded.target_quality_contract is not None
        self.assertEqual(decoded.target_quality_contract.format, "opus 128")
        self.assertEqual(decoded.materialized_measurement, output)

    def test_flac_to_mp3_v_level_keeps_contract_out_of_measurement(self):
        result = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=706,
                avg_bitrate_kbps=768,
                median_bitrate_kbps=755,
                format="FLAC",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=188,
                avg_bitrate_kbps=230,
                median_bitrate_kbps=232,
            ),
            target_quality_contract=TargetQualityContract.from_format("mp3 v2"),
        )

        assert_source_target_lineage(result)
        self.assertIsNotNone(result.source_measurement)
        self.assertIsNotNone(result.target_quality_contract)
        assert result.source_measurement is not None
        assert result.target_quality_contract is not None
        self.assertEqual(result.source_measurement.format, "FLAC")
        self.assertEqual(result.target_quality_contract.format, "mp3 v2")

    def test_native_lossy_research_probe_does_not_replace_source(self):
        source = AudioQualityMeasurement(
            min_bitrate_kbps=117,
            avg_bitrate_kbps=126,
            median_bitrate_kbps=125,
            format="Opus",
        )
        result = ImportResult(
            source_measurement=source,
            v0_probe=V0ProbeEvidence(
                kind="native_lossy_research_v0",
                min_bitrate_kbps=180,
                avg_bitrate_kbps=211,
                median_bitrate_kbps=214,
            ),
        )

        assert_source_target_lineage(result)
        self.assertEqual(result.source_measurement, source)
        self.assertIsNone(result.target_quality_contract)

    def test_keep_lossless_contract_can_name_same_bare_source_codec(self):
        result = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=742,
                avg_bitrate_kbps=811,
                format="FLAC",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=191,
                avg_bitrate_kbps=224,
            ),
            target_quality_contract=TargetQualityContract.from_format("flac"),
        )

        self.assertEqual(ImportResult.from_json(result.to_json()), result)

    def test_target_contract_preserves_comparison_and_gate_verdicts(self):
        cfg = QualityRankConfig.defaults()
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=128,
            avg_bitrate_kbps=130,
            format="Opus",
        )
        probe = V0ProbeEvidence(
            kind="lossless_source_v0",
            min_bitrate_kbps=191,
            avg_bitrate_kbps=224,
        )
        contract = TargetQualityContract.from_format("opus 128")
        legacy = AudioQualityMeasurement(
            min_bitrate_kbps=191,
            avg_bitrate_kbps=224,
            format="opus 128",
            verified_lossless=True,
        )
        source = AudioQualityMeasurement(
            min_bitrate_kbps=742,
            avg_bitrate_kbps=811,
            format="FLAC",
            verified_lossless=True,
        )

        old_decision = measured_import_decision(
            MeasuredImportDecisionInput(legacy, existing), cfg=cfg
        )
        new_decision = measured_import_decision(
            MeasuredImportDecisionInput(
                source, existing, False, contract, probe
            ),
            cfg=cfg,
        )
        self.assertEqual(new_decision, old_decision)
        output = AudioQualityMeasurement(
            min_bitrate_kbps=121,
            avg_bitrate_kbps=128,
            format="Opus",
            verified_lossless=True,
        )
        self.assertEqual(
            quality_gate_decision(output, cfg=cfg, target_contract=contract),
            quality_gate_decision(
                AudioQualityMeasurement(
                    min_bitrate_kbps=121,
                    avg_bitrate_kbps=128,
                    format="opus 128",
                    verified_lossless=True,
                ),
                cfg=cfg,
            ),
        )

    def test_single_track_bare_mp3_preserves_legacy_cbr_projection(self):
        projected_bitrates = [128]
        projected_is_cbr = projected_is_cbr_from_bitrates(projected_bitrates)
        contract = TargetQualityContract.from_format(
            "MP3", projected_is_cbr=projected_is_cbr
        )
        self.assertTrue(contract.is_cbr)
        source = AudioQualityMeasurement(
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            format="FLAC",
            is_cbr=True,
        )
        current = AudioQualityMeasurement(
            min_bitrate_kbps=123,
            avg_bitrate_kbps=123,
            format="MP3",
            is_cbr=False,
        )
        proxy = AudioQualityMeasurement(
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            format="MP3",
            is_cbr=projected_is_cbr,
        )
        cfg = QualityRankConfig.defaults()

        projected = measured_import_decision(
            MeasuredImportDecisionInput(source, current, True, contract, None),
            cfg=cfg,
        )
        legacy = measured_import_decision(
            MeasuredImportDecisionInput(proxy, current, True), cfg=cfg
        )
        wrong_bare_mp3 = measured_import_decision(
            MeasuredImportDecisionInput(
                source,
                current,
                True,
                TargetQualityContract.from_format("MP3"),
                None,
            ),
            cfg=cfg,
        )
        self.assertEqual(projected, legacy)
        self.assertEqual(projected.decision, "transcode_upgrade")
        self.assertEqual(wrong_bare_mp3.decision, "transcode_downgrade")

        pipeline = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=False,
            existing_min_bitrate=123,
            existing_avg_bitrate=123,
            existing_format="MP3",
            existing_is_cbr=False,
            post_conversion_min_bitrate=128,
            post_conversion_is_cbr=projected_is_cbr,
            converted_count=1,
        )
        self.assertEqual(pipeline["stage2_import"], "transcode_upgrade")

    def test_projection_mode_covers_one_multi_same_and_multi_different(self):
        cases = (
            ([128], True),
            ([128, 128], True),
            ([128, 129], False),
        )
        for bitrates, expected in cases:
            with self.subTest(bitrates=bitrates):
                mode = projected_is_cbr_from_bitrates(bitrates)
                contract = TargetQualityContract.from_format(
                    "MP3", projected_is_cbr=mode
                )
                self.assertEqual(mode, expected)
                self.assertEqual(contract.is_cbr, expected)

    def test_numeric_mp3_target_is_explicitly_cbr(self):
        self.assertTrue(TargetQualityContract.from_format("mp3 192").is_cbr)
        self.assertFalse(TargetQualityContract.from_format("mp3 v2").is_cbr)

    def test_early_downgrade_keeps_projected_target_for_dispatch_audit(self):
        decision = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=True,
            avg_bitrate=820,
            spectral_grade="genuine",
            existing_min_bitrate=900,
            existing_avg_bitrate=900,
            existing_format="FLAC",
            converted_count=1,
            post_conversion_min_bitrate=220,
            verified_lossless_target="opus 128",
            candidate_v0_probe_min=220,
            candidate_v0_probe_avg=240,
        )

        self.assertEqual(decision["stage2_import"], "downgrade")
        self.assertEqual(decision["target_final_format"], "opus 128")


class TestQualityLineageGenerated(unittest.TestCase):
    @given(
        projected_bitrates=st.lists(
            st.integers(min_value=32, max_value=200),
            min_size=1,
            max_size=8,
        ),
        existing=st.integers(min_value=32, max_value=320),
        existing_is_cbr=st.booleans(),
    )
    @example(
        projected_bitrates=[128],
        existing=123,
        existing_is_cbr=False,
    )
    def test_full_pipeline_preserves_legacy_projection_mode(
        self,
        projected_bitrates: list[int],
        existing: int,
        existing_is_cbr: bool,
    ) -> None:
        projected_min = min(projected_bitrates)
        projected_is_cbr = projected_is_cbr_from_bitrates(projected_bitrates)
        result = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=False,
            existing_min_bitrate=existing,
            existing_avg_bitrate=existing,
            existing_format="MP3",
            existing_is_cbr=existing_is_cbr,
            post_conversion_min_bitrate=projected_min,
            post_conversion_is_cbr=projected_is_cbr,
            converted_count=len(projected_bitrates),
        )
        legacy = measured_import_decision(
            MeasuredImportDecisionInput(
                AudioQualityMeasurement(
                    min_bitrate_kbps=projected_min,
                    avg_bitrate_kbps=projected_min,
                    format="MP3",
                    is_cbr=projected_is_cbr,
                ),
                AudioQualityMeasurement(
                    min_bitrate_kbps=existing,
                    avg_bitrate_kbps=existing,
                    format="MP3",
                    is_cbr=existing_is_cbr,
                ),
                True,
            ),
            cfg=QualityRankConfig.defaults(),
        )
        self.assertEqual(result["stage2_import"], legacy.decision)

    @given(
        source_min=st.integers(min_value=1, max_value=5000),
        source_avg=st.integers(min_value=1, max_value=5000),
        probe_min=st.integers(min_value=1, max_value=500),
        probe_avg=st.integers(min_value=1, max_value=500),
        target=st.sampled_from(["opus 128", "mp3 v0", "mp3 v2"]),
    )
    def test_lossless_source_probe_and_target_remain_disjoint(
        self,
        source_min: int,
        source_avg: int,
        probe_min: int,
        probe_avg: int,
        target: str,
    ) -> None:
        result = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=source_min,
                avg_bitrate_kbps=source_avg,
                format="FLAC",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=probe_min,
                avg_bitrate_kbps=probe_avg,
            ),
            target_quality_contract=TargetQualityContract.from_format(target),
        )

        decoded = ImportResult.from_json(result.to_json())
        assert_source_target_lineage(decoded)
        assert decoded.source_measurement is not None
        assert decoded.v0_probe is not None
        self.assertEqual(decoded.source_measurement.min_bitrate_kbps, source_min)
        self.assertEqual(decoded.source_measurement.avg_bitrate_kbps, source_avg)
        self.assertEqual(decoded.v0_probe.min_bitrate_kbps, probe_min)
        self.assertEqual(decoded.v0_probe.avg_bitrate_kbps, probe_avg)
        built = evidence_from_import_result(
            mb_release_id="generated-mbid",
            source_path="/generated/source",
            import_result=decoded,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=source_avg,
                    mtime_ns=source_min,
                    extension="flac",
                    container="flac",
                    codec="flac",
                )
            ],
        )
        self.assertEqual(built.status, "ready")
        assert built.evidence is not None
        self.assertEqual(built.evidence.measurement, decoded.source_measurement)
        assert decoded.target_quality_contract is not None
        self.assertEqual(
            built.evidence.target_is_cbr,
            decoded.target_quality_contract.is_cbr,
        )
        assert built.evidence.v0_metric is not None
        self.assertEqual(built.evidence.v0_metric.min_bitrate_kbps, probe_min)
        self.assertEqual(built.evidence.v0_metric.avg_bitrate_kbps, probe_avg)

    @given(
        source_min=st.integers(min_value=1, max_value=500),
        source_avg=st.integers(min_value=1, max_value=500),
        probe_min=st.integers(min_value=1, max_value=500),
        probe_avg=st.integers(min_value=1, max_value=500),
        codec=st.sampled_from(["MP3", "Opus", "AAC"]),
    )
    def test_native_lossy_research_probe_never_changes_decision(
        self,
        source_min: int,
        source_avg: int,
        probe_min: int,
        probe_avg: int,
        codec: str,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        source = AudioQualityMeasurement(
            min_bitrate_kbps=source_min,
            avg_bitrate_kbps=source_avg,
            format=codec,
        )
        current = AudioQualityMeasurement(
            min_bitrate_kbps=192,
            avg_bitrate_kbps=224,
            format="MP3",
        )
        research = V0ProbeEvidence(
            kind="native_lossy_research_v0",
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
        )
        baseline = measured_import_decision(
            MeasuredImportDecisionInput(source, current), cfg=cfg
        )
        with_research = measured_import_decision(
            MeasuredImportDecisionInput(
                source, current, False, None, research
            ),
            cfg=cfg,
        )
        self.assertEqual(with_research, baseline)

    @given(
        proxy_min=st.integers(min_value=1, max_value=500),
        proxy_avg=st.integers(min_value=1, max_value=500),
        target=st.sampled_from(["opus 128", "mp3 v0", "mp3 v2"]),
    )
    def test_new_wire_rows_reject_target_labelled_proxy_measurements(
        self,
        proxy_min: int,
        proxy_avg: int,
        target: str,
    ) -> None:
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=proxy_min,
                avg_bitrate_kbps=proxy_avg,
                format=target,
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=proxy_min,
                avg_bitrate_kbps=proxy_avg,
            ),
            target_quality_contract=TargetQualityContract.from_format(target),
        )
        with self.assertRaisesRegex(ValueError, "bare measured codec label"):
            planted_bad.to_json()

    @given(
        source_min=st.integers(min_value=300, max_value=5000),
        source_avg=st.integers(min_value=300, max_value=5000),
        probe_min=st.integers(min_value=80, max_value=320),
        probe_avg=st.integers(min_value=80, max_value=320),
        existing_min=st.integers(min_value=32, max_value=400),
        existing_avg=st.integers(min_value=32, max_value=400),
        target=st.sampled_from(["opus 128", "mp3 v0", "mp3 v2"]),
    )
    def test_contract_projection_preserves_old_decision_and_gate_policy(
        self,
        source_min: int,
        source_avg: int,
        probe_min: int,
        probe_avg: int,
        existing_min: int,
        existing_avg: int,
        target: str,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=existing_min,
            avg_bitrate_kbps=existing_avg,
            format="MP3",
        )
        proxy = AudioQualityMeasurement(
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
            format=target,
            verified_lossless=True,
        )
        source = AudioQualityMeasurement(
            min_bitrate_kbps=source_min,
            avg_bitrate_kbps=source_avg,
            format="FLAC",
            verified_lossless=True,
        )
        probe = V0ProbeEvidence(
            kind="lossless_source_v0",
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
        )
        contract = TargetQualityContract.from_format(target)

        old_decision = measured_import_decision(
            MeasuredImportDecisionInput(proxy, existing), cfg=cfg
        )
        new_decision = measured_import_decision(
            MeasuredImportDecisionInput(
                source, existing, False, contract, probe
            ),
            cfg=cfg,
        )
        self.assertEqual(new_decision, old_decision)

        output = AudioQualityMeasurement(
            min_bitrate_kbps=probe_min,
            avg_bitrate_kbps=probe_avg,
            format=target.split()[0],
            verified_lossless=True,
        )
        self.assertEqual(
            quality_gate_decision(output, cfg=cfg, target_contract=contract),
            quality_gate_decision(proxy, cfg=cfg),
        )

    @given(
        source_is_cbr=st.booleans(),
        output_is_cbr=st.booleans(),
        projected_bitrates=st.lists(
            st.integers(min_value=32, max_value=320),
            min_size=1,
            max_size=8,
        ),
        existing=st.integers(min_value=32, max_value=320),
    )
    def test_bare_mp3_projection_mode_is_independent_of_source_and_output(
        self,
        source_is_cbr: bool,
        output_is_cbr: bool,
        projected_bitrates: list[int],
        existing: int,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        bitrate = min(projected_bitrates)
        projected_is_cbr = projected_is_cbr_from_bitrates(projected_bitrates)
        contract = TargetQualityContract.from_format(
            "MP3", projected_is_cbr=projected_is_cbr
        )
        source = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format="FLAC",
            is_cbr=source_is_cbr,
        )
        current = AudioQualityMeasurement(
            min_bitrate_kbps=existing,
            avg_bitrate_kbps=existing,
            format="MP3",
            is_cbr=False,
        )
        legacy_projection = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format="MP3",
            is_cbr=projected_is_cbr,
        )
        self.assertEqual(
            measured_import_decision(
                MeasuredImportDecisionInput(
                    source, current, False, contract, None
                ),
                cfg=cfg,
            ),
            measured_import_decision(
                MeasuredImportDecisionInput(legacy_projection, current),
                cfg=cfg,
            ),
        )
        output = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            format="MP3",
            is_cbr=output_is_cbr,
        )
        self.assertEqual(
            quality_gate_decision(output, cfg=cfg, target_contract=contract),
            quality_gate_decision(legacy_projection, cfg=cfg),
        )

    @given(
        explicit_label=st.sampled_from(
            ["opus 128", "mp3 v0", "mp3 192", "aac 128"]
        ),
        bitrate=st.integers(min_value=1, max_value=500),
    )
    def test_target_absence_never_allows_explicit_source_measurement(
        self, explicit_label: str, bitrate: int
    ) -> None:
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=bitrate,
                format=explicit_label,
            )
        )
        with self.assertRaisesRegex(ValueError, "bare measured codec label"):
            planted_bad.to_json()
        built = evidence_from_import_result(
            mb_release_id="bad-target-absent",
            source_path="/bad",
            import_result=planted_bad,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.mp3",
                    size_bytes=1,
                    mtime_ns=1,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                )
            ],
        )
        self.assertEqual(built.status, "incomplete")

    @given(source_codec=st.sampled_from(["FLAC", "WAV", "ALAC"]))
    def test_source_measurement_never_carries_output_lineage(
        self, source_codec: str
    ) -> None:
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                format=source_codec,
                was_converted_from=source_codec.lower(),
            )
        )
        with self.assertRaisesRegex(ValueError, "was_converted_from"):
            planted_bad.to_json()
        built = evidence_from_import_result(
            mb_release_id="bad-source-lineage",
            source_path="/bad",
            import_result=planted_bad,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=1,
                    mtime_ns=1,
                    extension="flac",
                    container="flac",
                    codec="flac",
                )
            ],
        )
        self.assertEqual(built.status, "incomplete")


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_source_target_checker_rejects_target_labelled_proxy(self):
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=191,
                avg_bitrate_kbps=224,
                format="opus 128",
            ),
            v0_probe=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=191,
                avg_bitrate_kbps=224,
            ),
            target_quality_contract=TargetQualityContract.from_format("opus 128"),
        )

        with self.assertRaisesRegex(AssertionError, "bare codec"):
            assert_source_target_lineage(planted_bad)

    def test_checker_rejects_target_absent_explicit_source(self):
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=191,
                format="opus 128",
            )
        )
        with self.assertRaisesRegex(AssertionError, "bare codec"):
            assert_source_target_lineage(planted_bad)

    def test_checker_rejects_source_output_lineage(self):
        planted_bad = ImportResult(
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                format="FLAC",
                was_converted_from="flac",
            )
        )
        with self.assertRaisesRegex(AssertionError, "output lineage"):
            assert_source_target_lineage(planted_bad)
