"""Deterministic pins for the anonymized production-world census (#743)."""

from __future__ import annotations

from dataclasses import replace
import unittest

from tests.world_model.census_seeds import (
    EVIDENCE_DRIFT_FACT_SEEDS,
    EVIDENCE_DRIFT_MUTATION_SEEDS,
    STATEFUL_WORLD_CENSUS_SEEDS,
    WORLD_CENSUS_SEEDS,
    assert_census_seed_anonymized,
    assert_evidence_drift_seed_anonymized,
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

    def test_live_evidence_drift_corpus_covers_all_238_findings(self) -> None:
        self.assertEqual(
            {seed.mutation for seed in EVIDENCE_DRIFT_MUTATION_SEEDS},
            {
                "codec_replacement",
                "filename_rename",
                "same_name_size_drift",
                "file_count_drift",
            },
        )
        self.assertEqual(
            {
                (seed.name, seed.observed_rows, seed.initial_codec)
                for seed in EVIDENCE_DRIFT_MUTATION_SEEDS
                if seed.mutation == "codec_replacement"
            },
            {
                ("mp3_to_opus_replacement", 109, "mp3"),
                ("m4a_to_opus_replacement", 2, "m4a"),
            },
        )
        self.assertEqual(
            sum(seed.observed_rows for seed in EVIDENCE_DRIFT_MUTATION_SEEDS),
            238,
        )
        self.assertEqual(
            sum(seed.observed_rows for seed in EVIDENCE_DRIFT_FACT_SEEDS),
            238,
        )
        self.assertEqual(
            {
                (seed.spectral_subject, seed.v0_subject)
                for seed in EVIDENCE_DRIFT_FACT_SEEDS
            },
            {
                ("source", "source"),
                ("installed", "installed"),
                ("installed", None),
                (None, None),
            },
        )

    def test_evidence_drift_corpus_contains_no_identity_or_path(self) -> None:
        for seed in (
            *EVIDENCE_DRIFT_MUTATION_SEEDS,
            *EVIDENCE_DRIFT_FACT_SEEDS,
        ):
            assert_evidence_drift_seed_anonymized(seed)


if __name__ == "__main__":
    unittest.main()
