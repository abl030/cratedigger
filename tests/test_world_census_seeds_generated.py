"""Contracts for every committed production-world census seed."""

from __future__ import annotations

import unittest

from tests.world_model.census_seeds import (
    EVIDENCE_DRIFT_FACT_SEEDS,
    EVIDENCE_DRIFT_MUTATION_SEEDS,
    MISSING_CURRENT_EVIDENCE_FORMAT_SEEDS,
    MISSING_CURRENT_EVIDENCE_IDENTITY_SEEDS,
    MISSING_CURRENT_EVIDENCE_LEGACY_BITRATE_SEEDS,
    MISSING_CURRENT_EVIDENCE_LEGACY_SPECTRAL_SEEDS,
    MISSING_CURRENT_EVIDENCE_ORIGIN_SEEDS,
    MISSING_CURRENT_EVIDENCE_SEARCH_OVERRIDE_SEEDS,
    MISSING_CURRENT_EVIDENCE_STATUS_SEEDS,
    MISSING_CURRENT_EVIDENCE_TARGET_FORMAT_SEEDS,
    WORLD_CENSUS_SEEDS,
    assert_census_seed_anonymized,
    assert_evidence_drift_seed_anonymized,
    assert_missing_current_evidence_seed_anonymized,
)


class TestWorldCensusSeedsGenerated(unittest.TestCase):
    def test_every_sampled_seed_remains_anonymized(self) -> None:
        for seed in WORLD_CENSUS_SEEDS:
            with self.subTest(seed=seed):
                assert_census_seed_anonymized(seed)

    def test_every_sampled_evidence_drift_seed_remains_anonymized(self) -> None:
        seeds = (
            *EVIDENCE_DRIFT_MUTATION_SEEDS,
            *EVIDENCE_DRIFT_FACT_SEEDS,
        )
        for seed in seeds:
            with self.subTest(seed=seed):
                assert_evidence_drift_seed_anonymized(seed)

    def test_every_sampled_missing_evidence_seed_remains_anonymized(self) -> None:
        seeds = (
            *MISSING_CURRENT_EVIDENCE_ORIGIN_SEEDS,
            *MISSING_CURRENT_EVIDENCE_STATUS_SEEDS,
            *MISSING_CURRENT_EVIDENCE_IDENTITY_SEEDS,
            *MISSING_CURRENT_EVIDENCE_FORMAT_SEEDS,
            *MISSING_CURRENT_EVIDENCE_SEARCH_OVERRIDE_SEEDS,
            *MISSING_CURRENT_EVIDENCE_TARGET_FORMAT_SEEDS,
            *MISSING_CURRENT_EVIDENCE_LEGACY_SPECTRAL_SEEDS,
            *MISSING_CURRENT_EVIDENCE_LEGACY_BITRATE_SEEDS,
        )
        for seed in seeds:
            with self.subTest(seed=seed):
                assert_missing_current_evidence_seed_anonymized(seed)


if __name__ == "__main__":
    unittest.main()
