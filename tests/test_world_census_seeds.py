"""Deterministic pins for the anonymized production-world census (#743)."""

from __future__ import annotations

from dataclasses import replace
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
    STATEFUL_WORLD_CENSUS_SEEDS,
    WORLD_CENSUS_SEEDS,
    assert_census_seed_anonymized,
    assert_evidence_drift_seed_anonymized,
    assert_missing_current_evidence_seed_anonymized,
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
        self.assertIn("both", {seed.identity_shape for seed in WORLD_CENSUS_SEEDS})
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
        self.assertNotIn(
            "both",
            {seed.identity_shape for seed in STATEFUL_WORLD_CENSUS_SEEDS},
            "conflicting dual-provider rows remain census and pinned-operation "
            "fixtures, but cannot be clean state-machine starting worlds",
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

    def test_missing_current_evidence_census_preserves_every_live_axis(self) -> None:
        axes = (
            MISSING_CURRENT_EVIDENCE_ORIGIN_SEEDS,
            MISSING_CURRENT_EVIDENCE_STATUS_SEEDS,
            MISSING_CURRENT_EVIDENCE_IDENTITY_SEEDS,
            MISSING_CURRENT_EVIDENCE_FORMAT_SEEDS,
            MISSING_CURRENT_EVIDENCE_SEARCH_OVERRIDE_SEEDS,
            MISSING_CURRENT_EVIDENCE_TARGET_FORMAT_SEEDS,
            MISSING_CURRENT_EVIDENCE_LEGACY_SPECTRAL_SEEDS,
            MISSING_CURRENT_EVIDENCE_LEGACY_BITRATE_SEEDS,
        )
        self.assertTrue(all(sum(seed.observed_rows for seed in axis) == 429 for axis in axes))
        self.assertEqual(
            {seed.status for seed in MISSING_CURRENT_EVIDENCE_STATUS_SEEDS},
            {"wanted", "unsearchable"},
        )
        self.assertEqual(
            {seed.identity_shape for seed in MISSING_CURRENT_EVIDENCE_IDENTITY_SEEDS},
            {"musicbrainz", "discogs"},
        )
        self.assertEqual(
            {seed.codec for seed in MISSING_CURRENT_EVIDENCE_FORMAT_SEEDS},
            {"mp3", "m4a", "ogg", "opus", "wma"},
        )

    def test_missing_current_evidence_census_preserves_exact_live_marginals(self) -> None:
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.origin)
             for seed in MISSING_CURRENT_EVIDENCE_ORIGIN_SEEDS],
            [
                ("pre_rekey_existing_library_add", 249, "pre_rekey_existing_library_add"),
                ("pre_rekey_completed_import", 60, "pre_rekey_completed_import"),
                ("post_rekey_existing_library_add", 120, "post_rekey_existing_library_add"),
            ],
        )
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.status)
             for seed in MISSING_CURRENT_EVIDENCE_STATUS_SEEDS],
            [("wanted", 427, "wanted"), ("unsearchable", 2, "unsearchable")],
        )
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.identity_shape)
             for seed in MISSING_CURRENT_EVIDENCE_IDENTITY_SEEDS],
            [("musicbrainz", 362, "musicbrainz"), ("discogs", 67, "discogs")],
        )
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.codec, seed.installed_format)
             for seed in MISSING_CURRENT_EVIDENCE_FORMAT_SEEDS],
            [
                ("mp3", 404, "mp3", "MP3"),
                ("aac_m4a", 14, "m4a", "AAC"),
                ("ogg", 7, "ogg", "OGG"),
                ("opus", 3, "opus", "Opus"),
                ("windows_media_wma", 1, "wma", "Windows Media"),
            ],
        )
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.search_override)
             for seed in MISSING_CURRENT_EVIDENCE_SEARCH_OVERRIDE_SEEDS],
            [
                ("default", 207, None),
                ("lossless_mp3_v0_mp3_320", 129, "lossless,mp3 v0,mp3 320"),
                ("lossless", 89, "lossless"),
                ("lossless_mp3_v0", 2, "lossless,mp3 v0"),
                ("full_legacy_ladder", 2, "lossless,mp3 v0,mp3 320,aac,opus,ogg"),
            ],
        )
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.target_format)
             for seed in MISSING_CURRENT_EVIDENCE_TARGET_FORMAT_SEEDS],
            [("default", 424, None), ("lossless", 5, "lossless")],
        )
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.spectral_grade, seed.has_bitrate)
             for seed in MISSING_CURRENT_EVIDENCE_LEGACY_SPECTRAL_SEEDS],
            [
                ("absent_no_bitrate", 246, None, False),
                ("genuine_no_bitrate", 80, "genuine", False),
                ("likely_transcode_bitrate", 64, "likely_transcode", True),
                ("genuine_bitrate", 30, "genuine", True),
                ("suspect_bitrate", 4, "suspect", True),
                ("likely_transcode_no_bitrate", 3, "likely_transcode", False),
                ("suspect_no_bitrate", 2, "suspect", False),
            ],
        )
        self.assertEqual(
            [(seed.name, seed.observed_rows, seed.has_min_bitrate,
              seed.has_prev_min_bitrate)
             for seed in MISSING_CURRENT_EVIDENCE_LEGACY_BITRATE_SEEDS],
            [
                ("min_and_prev_present", 190, True, True),
                ("both_absent", 189, False, False),
                ("min_present_prev_absent", 50, True, False),
            ],
        )

    def test_missing_current_evidence_census_contains_no_identity_or_path(self) -> None:
        for axis in (
            MISSING_CURRENT_EVIDENCE_ORIGIN_SEEDS,
            MISSING_CURRENT_EVIDENCE_STATUS_SEEDS,
            MISSING_CURRENT_EVIDENCE_IDENTITY_SEEDS,
            MISSING_CURRENT_EVIDENCE_FORMAT_SEEDS,
            MISSING_CURRENT_EVIDENCE_SEARCH_OVERRIDE_SEEDS,
            MISSING_CURRENT_EVIDENCE_TARGET_FORMAT_SEEDS,
            MISSING_CURRENT_EVIDENCE_LEGACY_SPECTRAL_SEEDS,
            MISSING_CURRENT_EVIDENCE_LEGACY_BITRATE_SEEDS,
        ):
            for seed in axis:
                assert_missing_current_evidence_seed_anonymized(seed)

    def test_missing_current_evidence_anonymizer_rejects_raw_field_leakage(self) -> None:
        known_bads = (
            replace(
                MISSING_CURRENT_EVIDENCE_IDENTITY_SEEDS[0],
                identity_shape="123456789",
            ),
            replace(
                MISSING_CURRENT_EVIDENCE_FORMAT_SEEDS[0],
                installed_format="Artist Album Title",
            ),
            replace(
                MISSING_CURRENT_EVIDENCE_ORIGIN_SEEDS[0],
                origin="/mnt/virtio/Music/private",
            ),
            replace(
                MISSING_CURRENT_EVIDENCE_STATUS_SEEDS[0],
                status="12345678-1234-1234-1234-123456789abc",
            ),
            replace(
                MISSING_CURRENT_EVIDENCE_SEARCH_OVERRIDE_SEEDS[0],
                search_override='{"request_id": 42}',
            ),
        )
        for known_bad in known_bads:
            with self.subTest(seed=known_bad.name):
                with self.assertRaisesRegex(AssertionError, "anonymized"):
                    assert_missing_current_evidence_seed_anonymized(known_bad)


if __name__ == "__main__":
    unittest.main()
