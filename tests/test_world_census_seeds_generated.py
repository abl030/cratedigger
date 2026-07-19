"""Generated contracts for every committed production-world census seed."""

from __future__ import annotations

import unittest

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from tests.world_model.census_seeds import (
    WORLD_CENSUS_SEEDS,
    WorldCensusSeed,
    assert_census_seed_anonymized,
)


class TestWorldCensusSeedsGenerated(unittest.TestCase):
    @given(seed=st.sampled_from(WORLD_CENSUS_SEEDS))
    def test_every_sampled_seed_remains_anonymized(
        self,
        seed: WorldCensusSeed,
    ) -> None:
        assert_census_seed_anonymized(seed)


if __name__ == "__main__":
    unittest.main()
