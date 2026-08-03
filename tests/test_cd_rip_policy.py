import unittest

import msgspec

from lib.quality import (
    AccurateRipBitMatch,
    AlbumQualityEvidenceDecisionFacts,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    CdRipBitVerification,
    CdTocIdentity,
    full_pipeline_decision,
    full_pipeline_decision_from_evidence,
)
from tests.helpers import make_album_quality_evidence


class CdRipCanonicalPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cd_rip = CdRipBitVerification(
            toc=CdTocIdentity(
                track_offsets_sectors=[0],
                leadout_sector=470,
                accuraterip_id="000001d6-000003ac-02000601",
                musicbrainz_disc_id="disc-proof",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/example.bin",
                checksum_version="arv2",
                read_offset_samples=108,
                track_confidences=[42],
                track_checksums=[0x12345678],
                response_sha256="a" * 64,
            ),
        )
        self.candidate = make_album_quality_evidence(
            mb_release_id="exact-release",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                format="FLAC",
            ),
            files=[AlbumQualityEvidenceFile(
                relative_path="01.flac",
                size_bytes=100,
                mtime_ns=1,
                extension="flac",
                container="flac",
                codec="flac",
            )],
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=self.cd_rip.verified_lossless_proof(),
            cd_rip_verification=self.cd_rip,
        )
        self.facts = AlbumQualityEvidenceDecisionFacts(target_format="flac")

    def _current(
        self,
        *,
        bitrate: int,
        fmt: str,
        extension: str,
        is_cbr: bool = False,
    ):
        return make_album_quality_evidence(
            mb_release_id="exact-release",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=bitrate,
                avg_bitrate_kbps=bitrate,
                median_bitrate_kbps=bitrate,
                format=fmt,
                is_cbr=is_cbr,
            ),
            files=[AlbumQualityEvidenceFile(
                relative_path=f"01.{extension}",
                size_bytes=101,
                mtime_ns=2,
                extension=extension,
                container=extension,
                codec=extension,
            )],
            codec=extension,
            container=extension,
            storage_format=fmt,
        )

    def test_absent_evidence_is_exactly_the_pre_feature_baseline(self) -> None:
        absent = msgspec.structs.replace(
            self.candidate,
            verified_lossless_proof=None,
            cd_rip_verification=None,
        )

        actual = full_pipeline_decision_from_evidence(
            absent,
            None,
            facts=self.facts,
        )
        baseline = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=False,
            avg_bitrate=None,
            target_format="flac",
            new_format="FLAC",
        )

        self.assertEqual(actual, baseline)

    def test_positive_evidence_flips_only_verified_authenticity_input(self) -> None:
        without = full_pipeline_decision_from_evidence(
            msgspec.structs.replace(
                self.candidate,
                verified_lossless_proof=None,
                cd_rip_verification=None,
            ),
            None,
            facts=self.facts,
        )
        with_proof = full_pipeline_decision_from_evidence(
            self.candidate,
            None,
            facts=self.facts,
        )
        simulator = full_pipeline_decision(
            is_flac=True,
            min_bitrate=800,
            is_cbr=False,
            avg_bitrate=None,
            target_format="flac",
            new_format="FLAC",
            candidate_verified_lossless_proof=True,
        )

        self.assertFalse(without["verified_lossless"])
        self.assertTrue(with_proof["verified_lossless"])
        self.assertEqual(with_proof, simulator)
        self.assertEqual(without["final_status"], "wanted")
        self.assertEqual(with_proof["final_status"], "imported")

    def test_cd_proof_does_not_bypass_decoded_audio_integrity(self) -> None:
        corrupt = make_album_quality_evidence(
            mb_release_id="exact-release",
            measurement=self.candidate.measurement,
            files=self.candidate.files,
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=self.cd_rip.verified_lossless_proof(),
            cd_rip_verification=self.cd_rip,
            audio_corrupt=True,
            audio_error="decoder rejected frame",
        )

        result = full_pipeline_decision_from_evidence(
            corrupt,
            None,
            facts=self.facts,
        )

        self.assertEqual(result["preimport_audio"], "reject_corrupt")
        self.assertFalse(result["imported"])
        self.assertFalse(result["verified_lossless"])

    def test_carried_proof_imports_equivalent_kept_lossless_and_records_bypass(
        self,
    ) -> None:
        result = full_pipeline_decision_from_evidence(
            self.candidate,
            self._current(bitrate=800, fmt="FLAC", extension="flac"),
            facts=self.facts,
        )

        self.assertEqual(result["stage2_import"], "import")
        self.assertTrue(result["imported"])
        self.assertTrue(result["comparison_basis"]["verified_lossless_bypass"])
        self.assertFalse(result["denylisted"])

    def test_carried_proof_imports_equivalent_configured_target(self) -> None:
        candidate = msgspec.structs.replace(
            self.candidate,
            target_format="mp3 128",
            target_is_cbr=False,
        )
        result = full_pipeline_decision_from_evidence(
            candidate,
            self._current(
                bitrate=128,
                fmt="MP3",
                extension="mp3",
                is_cbr=True,
            ),
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target="mp3 128",
                target_format="mp3 128",
                converted_count=1,
                post_conversion_min_bitrate=128,
                post_conversion_is_cbr=False,
            ),
        )

        self.assertEqual(result["stage2_import"], "transcode_upgrade")
        self.assertTrue(result["imported"])
        basis = result["comparison_basis"]
        self.assertEqual(basis["verdict"], "equivalent")
        self.assertEqual(basis["new_format"], "mp3 128")
        self.assertEqual(basis["new_metric"], "contract")
        self.assertEqual(basis["new_value_kbps"], 128)
        self.assertTrue(basis["verified_lossless_bypass"])

    def test_cd_proof_cannot_bypass_worse_configured_target(self) -> None:
        candidate = msgspec.structs.replace(
            self.candidate,
            target_format="mp3 128",
            target_is_cbr=False,
        )
        result = full_pipeline_decision_from_evidence(
            candidate,
            self._current(bitrate=192, fmt="MP3", extension="mp3"),
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target="mp3 128",
                target_format="mp3 128",
                converted_count=1,
                post_conversion_min_bitrate=128,
                post_conversion_is_cbr=False,
            ),
        )

        self.assertEqual(result["stage2_import"], "transcode_downgrade")
        self.assertFalse(result["imported"])
        basis = result["comparison_basis"]
        self.assertEqual(basis["verdict"], "worse")
        self.assertEqual(basis["new_format"], "mp3 128")
        self.assertEqual(basis["new_metric"], "contract")
        self.assertEqual(basis["new_value_kbps"], 128)
        self.assertFalse(basis["verified_lossless_bypass"])
        self.assertEqual(result["final_status"], "wanted")

    def test_installed_verified_lossless_proof_lock_remains_absolute(self) -> None:
        current = msgspec.structs.replace(
            self._current(bitrate=128, fmt="Opus", extension="opus"),
            verified_lossless_proof=self.cd_rip.verified_lossless_proof(),
            cd_rip_verification=self.cd_rip,
        )

        result = full_pipeline_decision_from_evidence(
            self.candidate,
            current,
            facts=self.facts,
        )

        self.assertEqual(result["stage2_import"], "verified_lossless_locked")
        self.assertFalse(result["imported"])
        self.assertEqual(result["final_status"], "imported")


if __name__ == "__main__":
    unittest.main()
