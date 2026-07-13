"""Generated audit for decision proxy versus materialized output bitrate.

Invariant: once an ImportResult carries a materialized measurement,
download_log ``actual_*`` fields describe that on-disk output and never the
candidate/V0 proxy used to authorize conversion.
"""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.dispatch import _populate_dl_info_from_import_result
from lib.quality import (
    AudioQualityMeasurement,
    ConversionInfo,
    DownloadInfo,
    ImportResult,
    QualityRankConfig,
    compare_quality,
)


def assert_actual_matches_materialized(
    dl_info: DownloadInfo,
    materialized: AudioQualityMeasurement,
) -> None:
    if dl_info.actual_min_bitrate != materialized.min_bitrate_kbps:
        raise AssertionError(
            "actual bitrate did not come from materialized output: "
            f"actual={dl_info.actual_min_bitrate!r} "
            f"materialized={materialized.min_bitrate_kbps!r}"
        )


def assert_explicit_contract_is_not_a_measured_metric(basis) -> None:
    if basis.new_metric != "contract":
        raise AssertionError(
            "explicit target leaked a measured proxy into its basis: "
            f"metric={basis.new_metric!r} value={basis.new_value_kbps!r}"
        )


class TestGeneratedMaterializedQualityAudit(unittest.TestCase):
    @given(
        proxy_min=st.integers(min_value=1, max_value=3_000),
        output_min=st.integers(min_value=1, max_value=3_000),
        output_avg=st.integers(min_value=1, max_value=3_000),
        output_median=st.integers(min_value=1, max_value=3_000),
        output_codec=st.sampled_from(["Opus", "MP3", "AAC", "FLAC"]),
    )
    @example(
        proxy_min=191,
        output_min=102,
        output_avg=132,
        output_median=144,
        output_codec="Opus",
    )
    def test_materialized_output_always_owns_actual_fields(
        self,
        proxy_min: int,
        output_min: int,
        output_avg: int,
        output_median: int,
        output_codec: str,
    ) -> None:
        materialized = AudioQualityMeasurement(
            min_bitrate_kbps=output_min,
            avg_bitrate_kbps=output_avg,
            median_bitrate_kbps=output_median,
            format=output_codec,
        )
        result = ImportResult(
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=proxy_min,
                avg_bitrate_kbps=proxy_min,
                median_bitrate_kbps=proxy_min,
                format="opus 128",
            ),
            materialized_measurement=materialized,
            conversion=ConversionInfo(
                was_converted=True,
                original_filetype="flac",
                target_filetype=output_codec.lower(),
            ),
        )
        dl_info = DownloadInfo(filetype="flac")

        _populate_dl_info_from_import_result(dl_info, result)

        assert_actual_matches_materialized(dl_info, materialized)

    @given(
        target=st.sampled_from(["opus 64", "opus 96", "opus 128", "mp3 v0", "mp3 320"]),
        proxy_min=st.integers(min_value=1, max_value=3_000),
        proxy_avg=st.integers(min_value=1, max_value=3_000),
    )
    @example(target="opus 128", proxy_min=191, proxy_avg=224)
    def test_explicit_target_basis_is_always_contract_provenance(
        self,
        target: str,
        proxy_min: int,
        proxy_avg: int,
    ) -> None:
        basis = compare_quality(
            AudioQualityMeasurement(
                min_bitrate_kbps=proxy_min,
                avg_bitrate_kbps=proxy_avg,
                format=target,
            ),
            AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                format="MP3",
                is_cbr=True,
            ),
            QualityRankConfig.defaults(),
        )
        assert_explicit_contract_is_not_a_measured_metric(basis)


class TestMaterializedQualityAuditCheckerTripsOnViolations(unittest.TestCase):
    def test_checker_rejects_preview_proxy_as_actual(self) -> None:
        materialized = AudioQualityMeasurement(
            min_bitrate_kbps=102,
            avg_bitrate_kbps=132,
            format="Opus",
        )
        mutant = DownloadInfo(filetype="opus", actual_min_bitrate=191)
        with self.assertRaisesRegex(AssertionError, "materialized output"):
            assert_actual_matches_materialized(mutant, materialized)

    def test_contract_checker_rejects_v0_proxy_metric(self) -> None:
        from lib.quality import QualityComparisonBasis

        mutant = QualityComparisonBasis(
            verdict="better",
            branch="rank",
            new_rank="transparent",
            existing_rank="acceptable",
            new_metric="min",
            new_value_kbps=191,
            new_format="opus 128",
        )
        with self.assertRaisesRegex(AssertionError, "measured proxy"):
            assert_explicit_contract_is_not_a_measured_metric(mutant)


if __name__ == "__main__":
    unittest.main()
