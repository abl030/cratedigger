"""An unchanged native encode keeps its rank when its import role changes."""

import unittest

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.quality import QualityRankConfig, compare_quality
from tests.cross_codec_quality_helpers import (
    assert_no_cross_codec_reversal,
    decide_native_pair,
    native_encode,
)


class TestCrossCodecQualityGenerated(unittest.TestCase):
    @given(
        class_codec=st.sampled_from(("mp3", "vorbis")),
        raw_codec=st.sampled_from(("aac", "opus", "mp3", "vorbis")),
        class_bitrate=st.integers(96, 500), raw_bitrate=st.integers(32, 384),
        is_cbr=st.booleans(),
        cliff_hz=st.sampled_from((None, 12500, 15500, 16500, 17500)),
        raw_spectral=st.integers(1, 1000),
    )
    @example(class_codec="mp3", raw_codec="aac", class_bitrate=320,
             raw_bitrate=127, is_cbr=True, cliff_hz=15500, raw_spectral=96)
    @example(class_codec="mp3", raw_codec="aac", class_bitrate=256,
             raw_bitrate=112, is_cbr=True, cliff_hz=15500, raw_spectral=96)
    @example(class_codec="mp3", raw_codec="aac", class_bitrate=97,
             raw_bitrate=32, is_cbr=True, cliff_hz=12500, raw_spectral=1)
    def test_native_cross_codec_roles_agree(
        self, class_codec, raw_codec, class_bitrate, raw_bitrate,
        is_cbr, cliff_hz, raw_spectral,
    ):
        classed = native_encode(class_codec, class_bitrate,
                               is_cbr=is_cbr, cliff_hz=cliff_hz)
        if raw_codec == class_codec:
            raw_codec = "vorbis" if class_codec == "mp3" else "mp3"
        raw = native_encode(raw_codec, raw_bitrate,
                            cliff_hz=15000, spectral_bitrate=raw_spectral)
        if raw_codec in ("mp3", "vorbis"):
            raw = msgspec.structs.replace(raw, measurement=msgspec.structs.replace(
                raw.measurement, spectral_grade="genuine",
                spectral_bitrate_kbps=None, cliff_hz=None))
        forward = decide_native_pair(classed, raw)
        backward = decide_native_pair(raw, classed)
        if forward["comparison_basis"]["branch"] == "transcode_rank_regression":
            # The earlier clean-HAVE guard records the raw rank regression
            # that already rejected this candidate, before any class binds.
            self.assertFalse(forward["imported"])
        else:
            assert_no_cross_codec_reversal(forward, backward)
        self.assertEqual(forward["comparison_basis"]["existing_value_kbps"], raw_bitrate)
        self.assertEqual(backward["comparison_basis"]["new_value_kbps"], raw_bitrate)
        direct_forward = compare_quality(
            classed.measurement, raw.measurement, QualityRankConfig.defaults())
        direct_backward = compare_quality(
            raw.measurement, classed.measurement, QualityRankConfig.defaults())
        if direct_forward.branch != "transcode_rank_regression":
            self.assertEqual(
                (direct_forward.new_value_kbps, direct_forward.existing_value_kbps,
                 direct_forward.new_rank, direct_forward.existing_rank),
                (direct_backward.existing_value_kbps, direct_backward.new_value_kbps,
                 direct_backward.existing_rank, direct_backward.new_rank),
            )
