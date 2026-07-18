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
    format = draw(st.sampled_from([
        "MP3", "AAC", "Opus", "Vorbis", "WMA", "Ogg", "FLAC", None,
    ]))
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
            min_bitrate_kbps=CFG.vorbis.transparent,
            avg_bitrate_kbps=CFG.vorbis.transparent,
            format="Vorbis",
            spectral_grade="suspect",
        ),
        SpectralAnalysisDetail(attempted=True, grade="suspect"),
        CFG,
    ))
    @example(world=(
        AudioQualityMeasurement(
            min_bitrate_kbps=CFG.wma.transparent,
            avg_bitrate_kbps=CFG.wma.transparent,
            format="WMA",
            is_cbr=True,
            spectral_grade="likely_transcode",
        ),
        SpectralAnalysisDetail(attempted=True, grade="likely_transcode"),
        CFG,
    ))
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


# ---------------------------------------------------------------------------
# No-widening invariant (issue #711 provisional surfacing, 2026-07-18): no
# override-resolution path may ADD a lossy search tier. Redirecting scope to
# "lossless" is legal narrowing (toward the terminal goal); everything else
# in the result must already have been allowed by the input. This is the
# structural guarantee that a provisional install's lossless-only scope can
# never silently re-widen into MP3 tiers.
# ---------------------------------------------------------------------------


def _tiers(override):
    return {t.strip() for t in override.split(",") if t.strip()}


def assert_no_lossy_tier_added(input_override, result_override) -> None:
    """Checker: result tiers ⊆ input tiers ∪ {"lossless"}."""
    if result_override is None:
        return
    out = _tiers(result_override)
    allowed = {"lossless"} if input_override is None else (
        _tiers(input_override) | {"lossless"}
    )
    added = out - allowed
    if added:
        raise AssertionError(
            f"override resolution widened with lossy tiers {sorted(added)}: "
            f"{input_override!r} -> {result_override!r}"
        )


_override_csv = st.one_of(
    st.none(),
    st.sampled_from([
        "lossless",
        "lossless,mp3 v0,mp3 320",
        "lossless,mp3 v0,mp3 320,aac,opus,ogg",
        "mp3 v0,mp3 320",
        "mp3 320",
    ]),
    st.lists(
        st.sampled_from(
            ["lossless", "mp3 v0", "mp3 320", "mp3 256", "aac", "opus", "ogg"]
        ),
        min_size=1, max_size=5, unique=True,
    ).map(",".join),
)


@st.composite
def rejection_worlds(draw):
    from tests.helpers import make_download_info

    override = draw(_override_csv)
    dl_info = make_download_info(
        filetype=draw(st.one_of(
            st.none(),
            st.sampled_from(["flac", "mp3", "mp3 320", "aac", "opus"]),
        )),
        bitrate=draw(st.one_of(
            st.none(), st.integers(min_value=0, max_value=1200))),
        is_vbr=draw(st.booleans()),
        was_converted=draw(st.booleans()),
        slskd_filetype=draw(st.one_of(
            st.none(),
            st.sampled_from(["flac", "mp3", "mp3 vbr", "aac"]),
        )),
    )
    decision = draw(st.sampled_from([
        "downgrade", "transcode_downgrade", "reject", "import", None,
    ]))
    measurement, audit, cfg = draw(have_worlds())
    source = draw(st.sampled_from(
        ["attempt_have_audit", "linked_current_evidence"]))
    return override, dl_info, decision, measurement, audit, source, cfg


class TestGeneratedNoOverrideWidening(unittest.TestCase):
    @given(world=rejection_worlds())
    def test_resolution_chain_never_adds_a_lossy_tier(self, world):
        from lib.quality import (
            narrow_override_on_downgrade,
            resolve_rejection_search_override,
        )

        override, dl_info, decision, measurement, audit, source, cfg = world

        narrowed = narrow_override_on_downgrade(override, dl_info)
        assert_no_lossy_tier_added(override, narrowed)

        resolution = resolve_rejection_search_override(
            decision=decision,
            current_override=override,
            dl_info=dl_info,
            current_measurement=measurement,
            spectral_evidence_source=source,
            have_spectral_audit=audit,
            cfg=cfg,
        )
        assert_no_lossy_tier_added(override, resolution.override)


class TestNoWideningCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_a_lossy_tier_is_added(self):
        with self.assertRaisesRegex(AssertionError, "widened"):
            assert_no_lossy_tier_added("lossless", "lossless,mp3 320")

    def test_trips_when_unrestricted_result_invents_lossy_scope(self):
        with self.assertRaisesRegex(AssertionError, "widened"):
            assert_no_lossy_tier_added(None, "mp3 v0")

    def test_accepts_redirect_to_lossless(self):
        assert_no_lossy_tier_added("mp3 v0,mp3 320", "lossless")

    def test_accepts_pure_narrowing(self):
        assert_no_lossy_tier_added("lossless,mp3 v0,mp3 320", "lossless,mp3 v0")
