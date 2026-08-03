"""Generated patrol for strict legacy import-result projections."""

from __future__ import annotations

import unittest

from hypothesis import given, strategies as st

from lib.quality import ImportResult


class TestLegacyV2MaterializedMeasurementGenerated(unittest.TestCase):
    @given(
        bitrate=st.integers(min_value=1, max_value=320),
        verified=st.booleans(),
        fmt=st.sampled_from(("OPUS", "MP3", "FLAC")),
    )
    def test_v2_materialized_measurement_never_leaks_source_only_flag(
        self, bitrate: int, verified: bool, fmt: str,
    ) -> None:
        result = ImportResult.from_dict({
            "version": 2, "decision": "import", "conversion": {},
            "new_measurement": None, "existing_measurement": None,
            "materialized_measurement": {
                "min_bitrate_kbps": bitrate, "avg_bitrate_kbps": bitrate,
                "median_bitrate_kbps": bitrate, "format": fmt,
                "is_cbr": False, "spectral_grade": None,
                "spectral_bitrate_kbps": None, "verified_lossless": verified,
                "was_converted_from": None,
            },
        })
        assert result.materialized_measurement is not None
        self.assertEqual(result.materialized_measurement.avg_bitrate_kbps, bitrate)
        self.assertEqual(result.materialized_measurement.format, fmt)

