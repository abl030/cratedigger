"""Generated invariant for transparent genuine lossy search narrowing."""

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.quality import (
    AudioQualityMeasurement,
    QUALITY_LOSSLESS,
    QualityRank,
    QualityRankConfig,
    SpectralAnalysisDetail,
    measurement_rank,
    rejection_backfill_override,
)


CFG = QualityRankConfig.defaults()


def assert_transparent_have_search_override(
    measurement: AudioQualityMeasurement,
    audit: SpectralAnalysisDetail | None,
    actual: str | None,
    cfg: QualityRankConfig,
) -> None:
    trusted_genuine = (
        audit is not None
        and audit.attempted
        and audit.error is None
        and audit.grade == "genuine"
    )
    expected = (
        QUALITY_LOSSLESS
        if trusted_genuine
        and measurement_rank(measurement, cfg) == QualityRank.TRANSPARENT
        else None
    )
    if actual != expected:
        raise AssertionError(
            "transparent HAVE search override drifted: "
            f"expected={expected!r}, actual={actual!r}, "
            f"measurement={measurement!r}, audit={audit!r}"
        )


@st.composite
def have_worlds(draw):
    cfg = QualityRankConfig.defaults()
    format = draw(st.sampled_from(["MP3", "AAC", "Opus", "Ogg", "FLAC", None]))
    bitrate = draw(st.one_of(
        st.none(),
        st.integers(min_value=0, max_value=1200),
    ))
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=bitrate,
        avg_bitrate_kbps=draw(st.one_of(st.none(), st.just(bitrate))),
        median_bitrate_kbps=draw(st.one_of(st.none(), st.just(bitrate))),
        format=format,
        is_cbr=draw(st.booleans()),
        spectral_grade=draw(st.one_of(st.none(), st.sampled_from([
            "genuine", "marginal", "suspect", "likely_transcode",
        ]))),
        verified_lossless=draw(st.booleans()),
        was_converted_from=draw(st.one_of(
            st.none(), st.sampled_from(["flac", "alac", "wav", "mp3"])
        )),
    )
    audit = draw(st.one_of(
        st.none(),
        st.builds(
            SpectralAnalysisDetail,
            attempted=st.booleans(),
            grade=st.one_of(st.none(), st.sampled_from([
                "genuine", "marginal", "suspect", "likely_transcode",
            ])),
            error=st.one_of(st.none(), st.just("spectral failed")),
        ),
    ))
    return measurement, audit, cfg


class TestTransparentHaveSearchOverrideChecker(unittest.TestCase):
    def test_checker_rejects_missing_lossless_override(self):
        cfg = QualityRankConfig.defaults()
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=cfg.mp3_cbr.transparent,
            avg_bitrate_kbps=cfg.mp3_cbr.transparent,
            format="MP3",
            is_cbr=True,
        )
        audit = SpectralAnalysisDetail(attempted=True, grade="genuine")
        with self.assertRaisesRegex(AssertionError, "search override drifted"):
            assert_transparent_have_search_override(
                measurement,
                audit,
                None,
                cfg,
            )


class TestGeneratedTransparentHaveSearchOverride(unittest.TestCase):
    @example(world=(
        AudioQualityMeasurement(
            min_bitrate_kbps=CFG.mp3_cbr.transparent,
            avg_bitrate_kbps=CFG.mp3_cbr.transparent,
            format="MP3",
            is_cbr=True,
            spectral_grade="genuine",
        ),
        None,
        CFG,
    ))
    @example(world=(
        AudioQualityMeasurement(
            min_bitrate_kbps=CFG.mp3_cbr.excellent,
            avg_bitrate_kbps=CFG.mp3_cbr.excellent,
            format="MP3",
            is_cbr=True,
        ),
        SpectralAnalysisDetail(attempted=True, grade="genuine"),
        CFG,
    ))
    @given(world=have_worlds())
    def test_override_matches_canonical_rank_and_trusted_audit(self, world):
        measurement, audit, cfg = world
        actual = rejection_backfill_override(
            current_measurement=measurement,
            spectral_evidence_source="attempt_have_audit",
            have_spectral_audit=audit,
            cfg=cfg,
        )
        assert_transparent_have_search_override(
            measurement,
            audit,
            actual,
            cfg,
        )
