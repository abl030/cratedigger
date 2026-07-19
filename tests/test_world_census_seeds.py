"""Deterministic pins for the anonymized production-world census (#743)."""

from __future__ import annotations

from dataclasses import replace
import unittest

from tests.world_model.census_seeds import (
    STATEFUL_WORLD_CENSUS_SEEDS,
    WORLD_CENSUS_SEEDS,
    assert_census_seed_anonymized,
)


class TestWorldCensusSeeds(unittest.TestCase):
    def test_corpus_covers_live_status_lineage_and_legacy_policy_shapes(self) -> None:
        self.assertEqual(
            {seed.status for seed in WORLD_CENSUS_SEEDS},
            {"downloading", "imported", "replaced", "unsearchable", "wanted"},
        )
        self.assertEqual(
            {seed.lineage_version for seed in WORLD_CENSUS_SEEDS},
            {0, 1, 3, 4},
        )
        self.assertIn(
            "lossless,mp3 v0,mp3 320,aac,opus,ogg",
            {seed.search_override for seed in WORLD_CENSUS_SEEDS},
        )
        self.assertIn("Opus", {seed.storage_format for seed in WORLD_CENSUS_SEEDS})
        self.assertIn("MP3", {seed.storage_format for seed in WORLD_CENSUS_SEEDS})
        self.assertTrue(all(seed.observed_rows > 0 for seed in WORLD_CENSUS_SEEDS))

    def test_committed_census_contains_no_production_identity_or_path(self) -> None:
        for seed in WORLD_CENSUS_SEEDS:
            assert_census_seed_anonymized(seed)

    def test_stateful_subset_is_drawn_only_from_captured_request_shapes(self) -> None:
        self.assertTrue(STATEFUL_WORLD_CENSUS_SEEDS)
        self.assertTrue(
            set(STATEFUL_WORLD_CENSUS_SEEDS).issubset(WORLD_CENSUS_SEEDS)
        )
        self.assertEqual(
            {seed.status for seed in STATEFUL_WORLD_CENSUS_SEEDS},
            {"imported", "wanted"},
        )

    def test_anonymization_checker_trips_on_a_planted_path(self) -> None:
        known_bad = replace(
            WORLD_CENSUS_SEEDS[0],
            name="mnt_virtio_music_private",
        )

        with self.assertRaisesRegex(AssertionError, "anonymized"):
            assert_census_seed_anonymized(known_bad)


if __name__ == "__main__":
    unittest.main()
