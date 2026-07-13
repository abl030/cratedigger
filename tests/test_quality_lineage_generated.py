"""Generated contracts for source, probe, target, and output lineage."""

from __future__ import annotations

import unittest

from hypothesis import given, strategies as st

from lib.quality import (
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    ImportResult,
    MeasuredImportDecisionInput,
    QualityRankConfig,
    TargetQualityContract,
    V0ProbeEvidence,
    measured_import_decision,
    quality_gate_decision,
)
from lib.quality_evidence import evidence_from_import_result


def assert_source_target_lineage(result: ImportResult) -> None:
    """Reject a source measurement wearing its target contract label."""

    source = result.source_measurement
    target = result.target_quality_contract
    if (
        source is not None
        and target is not None
        and source.format is not None
        and source.format.strip().lower() == target.format.strip().lower()
        and " " in source.format.strip()
        and any(char.isdigit() for char in source.format)
    ):
        raise AssertionError("source measurement must not wear target contract label")


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
            target_quality_contract=TargetQualityContract(format="opus 128"),
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
            target_quality_contract=TargetQualityContract(format="mp3 v2"),
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
            target_quality_contract=TargetQualityContract(format="flac"),
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
        contract = TargetQualityContract(format="opus 128")
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


class TestQualityLineageGenerated(unittest.TestCase):
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
            target_quality_contract=TargetQualityContract(format=target),
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
            target_format=target,
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
            target_quality_contract=TargetQualityContract(format=target),
        )
        with self.assertRaisesRegex(ValueError, "target contract label"):
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
        contract = TargetQualityContract(format=target)

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
            target_quality_contract=TargetQualityContract(format="opus 128"),
        )

        with self.assertRaisesRegex(AssertionError, "target contract"):
            assert_source_target_lineage(planted_bad)
