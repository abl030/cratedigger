"""Bounds and checker qualification for native cross-codec comparisons."""

import unittest

import msgspec

from lib.quality import QualityRankConfig, compare_quality
from lib.quality.pipeline import full_pipeline_decision_from_evidence
from tests.cross_codec_quality_helpers import (
    assert_no_cross_codec_reversal,
    decide_native_pair,
    native_encode,
)


class TestCrossCodecQuality(unittest.TestCase):
    def test_clean_vorbis_cannot_trade_places_with_a_classed_mp3(self):
        mp3 = native_encode("mp3", 320, is_cbr=True, cliff_hz=15500)
        vorbis = native_encode("vorbis", 160)
        vorbis = msgspec.structs.replace(vorbis, measurement=msgspec.structs.replace(
            vorbis.measurement, spectral_grade="genuine",
            spectral_bitrate_kbps=None, cliff_hz=None))
        forward = decide_native_pair(mp3, vorbis)
        backward = decide_native_pair(vorbis, mp3)
        self.assertFalse(forward["imported"])
        self.assertTrue(backward["imported"])
        self.assertEqual(forward["comparison_basis"]["new_value_kbps"], 128)
        assert_no_cross_codec_reversal(forward, backward)

    def test_unresolved_mixed_and_conflicting_codecs_withhold_the_bound(self):
        mp3 = native_encode("mp3", 320, is_cbr=True, cliff_hz=15500)
        aac = native_encode("aac", 127, cliff_hz=15000)
        cases = (
            msgspec.structs.replace(aac, filetype_band="mixed_lossy"),
            msgspec.structs.replace(aac, measurement=msgspec.structs.replace(
                aac.measurement, format="UNKNOWN", codec_family=None),
                storage_format="UNKNOWN"),
            msgspec.structs.replace(aac, measurement=msgspec.structs.replace(
                aac.measurement, codec_family="lossless")),
        )
        for raw in cases:
            with self.subTest(raw=raw.measurement.format, band=raw.filetype_band):
                result = full_pipeline_decision_from_evidence(mp3, raw)
                self.assertEqual(result["comparison_basis"]["new_value_kbps"], 320)
                self.assertFalse(result["comparison_basis"]["spectral_clamped"])

    def test_reversal_checker_rejects_each_bad_clause(self):
        mp3 = native_encode("mp3", 320, is_cbr=True, cliff_hz=15500)
        aac = native_encode("aac", 127, cliff_hz=15000)
        forward = decide_native_pair(mp3, aac)
        backward = decide_native_pair(aac, mp3)
        assert_no_cross_codec_reversal(forward, backward)
        with self.assertRaisesRegex(AssertionError, "both replacements import"):
            assert_no_cross_codec_reversal(dict(forward, imported=True), backward)
        wrong_value = dict(forward["comparison_basis"], new_value_kbps=320)
        with self.assertRaisesRegex(AssertionError, "encode quality changes"):
            assert_no_cross_codec_reversal(
                dict(forward, comparison_basis=wrong_value), backward)

    def test_same_rank_uses_the_bound_in_both_comparison_explanations(self):
        """The generated poor-rank world covers AAC and Opus semantics."""
        mp3 = native_encode("mp3", 97, is_cbr=True, cliff_hz=12500)
        for codec in ("aac", "opus"):
            with self.subTest(codec=codec):
                raw = native_encode(codec, 32, cliff_hz=15000)
                forward = decide_native_pair(mp3, raw)
                backward = decide_native_pair(raw, mp3)
                self.assertFalse(forward["imported"])
                self.assertFalse(backward["imported"])
                self.assertEqual(forward["comparison_basis"]["new_value_kbps"], 96)
                self.assertEqual(backward["comparison_basis"]["existing_value_kbps"], 96)
                self.assertTrue(forward["comparison_basis"]["spectral_clamped"])
                self.assertTrue(backward["comparison_basis"]["spectral_clamped"])
                assert_no_cross_codec_reversal(forward, backward)
                direct_forward = compare_quality(
                    mp3.measurement, raw.measurement, QualityRankConfig.defaults())
                direct_backward = compare_quality(
                    raw.measurement, mp3.measurement, QualityRankConfig.defaults())
                self.assertEqual(direct_forward.new_value_kbps, 96)
                self.assertEqual(direct_backward.existing_value_kbps, 96)

    def test_explicit_labels_and_class_label_conflicts_withhold_the_bound(self):
        mp3 = native_encode("mp3", 320, is_cbr=True, cliff_hz=15500)
        aac = native_encode("aac", 127, cliff_hz=15000)
        opus = native_encode("opus", 32)
        labelled_mp3 = msgspec.structs.replace(mp3, measurement=msgspec.structs.replace(
            mp3.measurement, format="MP3 320"), storage_format="MP3 320")
        labelled_aac = msgspec.structs.replace(aac, measurement=msgspec.structs.replace(
            aac.measurement, format="AAC 128"), storage_format="AAC 128")
        conflicting_mp3 = msgspec.structs.replace(mp3, measurement=msgspec.structs.replace(
            mp3.measurement, format="AAC"), storage_format="AAC")
        for candidate, current in (
            (labelled_mp3, aac), (mp3, labelled_aac), (conflicting_mp3, opus),
        ):
            with self.subTest(candidate=candidate.measurement.format, current=current.measurement.format):
                # Explicit labels belong to the comparison API, while evidence
                # admission requires bare measured codecs.
                basis = compare_quality(
                    candidate.measurement, current.measurement, QualityRankConfig.defaults(),
                )
                self.assertFalse(basis.spectral_clamped)
                self.assertEqual(basis.new_value_kbps, 320)

    def test_class_may_equal_but_never_raise_its_measured_bitrate(self):
        raw = native_encode("aac", 32)
        for bitrate, bounded in ((96, False), (128, True)):
            with self.subTest(bitrate=bitrate):
                classed = native_encode("mp3", bitrate, is_cbr=True, cliff_hz=15500)
                result = full_pipeline_decision_from_evidence(classed, raw)
                self.assertEqual(result["comparison_basis"]["new_value_kbps"], bitrate)
                self.assertIs(result["comparison_basis"]["spectral_clamped"], bounded)
