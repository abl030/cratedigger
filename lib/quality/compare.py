"""Pairwise quality comparison (compare_quality) and format-hint helpers.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from typing import Optional

from lib.quality.evidence_types import (
    AudioQualityMeasurement,
    QualityComparisonBasis,
    SPECTRAL_TRANSCODE_GRADES,
    TargetQualityContract,
    V0ProbeEvidence,
)
from lib.quality.ranks import (
    QualityRank,
    QualityRankConfig,
    _codec_family_of,
    _parse_bitrate_label,
    _parse_vbr_level,
    _selected_bitrate,
    _selected_bitrate_with_source,
    _selected_quality_bitrate_with_source,
    measurement_rank,
    quality_rank,
)


def _is_explicit_label(format_hint: Optional[str]) -> bool:
    """True if format_hint carries an explicit quality contract (VBR or bitrate).

    "mp3 v0" / "opus 128" / "mp3 320" are contracts. "MP3" / "Opus" / "FLAC"
    are bare codec names from beets items.format. Within the same rank tier,
    a contract + anything is equivalent — only bare-vs-bare compares on bitrate.
    """
    if format_hint is None:
        return False
    if _parse_vbr_level(format_hint) is not None:
        return True
    if _parse_bitrate_label(format_hint) is not None:
        return True
    return False


def comparison_format_hint(
    *,
    explicit_format: str | None = None,
    target_format: str | None = None,
    verified_lossless_target: str | None = None,
    converted_count: int = 0,
    is_transcode: bool = False,
    native_codec_family: str | None = None,
) -> str | None:
    """Format hint to use for the pre-import quality comparison.

    This keeps production import_one.py and the simulator on the same rules:
    compare the quality of what would actually end up on disk, not just the
    temporary V0 verification artifact.
    """
    if explicit_format is not None:
        return explicit_format
    if target_format in ("flac", "lossless"):
        return "flac"
    if converted_count > 0 and not is_transcode:
        return verified_lossless_target or "mp3 v0"
    if converted_count > 0:
        return "MP3"
    return native_codec_family


# Probed-codec / extension → native-lossy rank-model format label. Only codecs
# with a lossy rank band table are mapped; everything else returns None.
_NATIVE_CODEC_LABELS: dict[str, str] = {
    "opus": "opus",
    "aac": "aac",
    "vorbis": "vorbis",
    "wma": "wma",
    "wmav1": "wma",
    "wmav2": "wma",
    "wmapro": "wma",
    "wmavoice": "wma",
    "mp3": "MP3",
    "mp3float": "MP3",
}
_NATIVE_EXT_LABELS: dict[str, str] = {
    "opus": "opus",
    "aac": "aac",
    "m4a": "aac",
    "wma": "wma",
    "mp3": "MP3",
}


def native_codec_format_label(
    codec: Optional[str], ext: Optional[str] = None
) -> Optional[str]:
    """Map a probed codec name (or file-extension fallback) to the native-lossy
    ``AudioQualityMeasurement.format`` label the rank model keys on.

    Returns a label ``_codec_family_of`` recognises (for example ``"opus"``,
    ``"vorbis"``, ``"wma"``, or ``"MP3"``), or None for codecs with no lossy
    rank band. The probed codec name wins over the extension — an Opus stream
    in an ``.ogg`` container is "opus", not vorbis.

    This is the fix for the Opus-recorded-as-MP3 bug: native lossy downloads
    used to be hardcoded to "MP3", so a genuine Opus 124 was scored on the
    MP3-VBR band table and rejected as a downgrade against an MP3 128.
    """
    def _norm(value: Optional[str]) -> Optional[str]:
        return (value or "").strip().lower().lstrip(".") or None

    codec_norm = _norm(codec)
    if codec_norm is not None:
        # A probed codec name is authoritative — if it has no lossy band we
        # return None rather than guessing from the (possibly generic)
        # container extension.
        return _NATIVE_CODEC_LABELS.get(codec_norm)

    ext_norm = _norm(ext)
    if ext_norm is not None:
        return _NATIVE_EXT_LABELS.get(ext_norm)
    return None


def _shared_spectral_bitrates(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    new_v0_probe: V0ProbeEvidence | None = None,
) -> "tuple[Optional[int], Optional[int]] | None":
    """Return rank-bucket bitrates when BOTH sides carry spectral estimates.

    The clamp takes ``min(selected_metric, spectral_bitrate)`` per side — the
    spectral estimate becomes an upper bound on the rank bucket. Same-bucket
    tie-breaks still use the raw configured bitrate metric in
    ``compare_quality()``; otherwise an equal spectral floor would erase a
    real avg-bitrate upgrade and stop the pipeline from grinding upward when
    spectral analysis is too pessimistic.

    This is deliberately *narrow*: it only fires when new and existing both
    measured a spectral floor, so a stale estimate on only one side
    (Springsteen shape: existing CBR 320 genuine+96, new MP3 V0 240 no
    spectral) keeps the container comparison — the rule that
    ``test_springsteen_genuine_but_96kbps`` pins.

    Mostly grade-tolerant by design. ``compute_effective_override_bitrate``
    gates the clamp on ``SPECTRAL_TRANSCODE_GRADES`` because on a single-sided
    override a genuine grade can't be distinguished from natural lo-fi
    rolloff. Here, both sides carrying estimates is usually corroborating
    evidence (Eno case, ``download_log.id=3291``). The caller still guards the
    asymmetric case where a transcode-grade candidate would otherwise use a
    higher spectral floor to replace a non-transcode-grade existing album with
    a higher real quality rank.
    """
    if (new.spectral_bitrate_kbps is None
            or existing.spectral_bitrate_kbps is None):
        return None
    new_br = _selected_quality_bitrate_with_source(new, cfg, new_v0_probe)[0]
    existing_br = _selected_bitrate(existing, cfg)
    new_br = (min(new_br, new.spectral_bitrate_kbps)
              if new_br is not None else new.spectral_bitrate_kbps)
    existing_br = (min(existing_br, existing.spectral_bitrate_kbps)
                   if existing_br is not None
                   else existing.spectral_bitrate_kbps)
    return new_br, existing_br


def _transcode_candidate_real_rank_regresses(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    new_target_contract: TargetQualityContract | None = None,
    new_v0_probe: V0ProbeEvidence | None = None,
) -> bool:
    """Whether a transcode-grade candidate is lower real rank than existing.

    Shared spectral floors are useful supporting evidence, but they must not
    launder a lower-rank transcode over a higher-rank non-transcode existing
    album. Compare the real configured measurement rank before the spectral
    clamp for that asymmetric grade transition only.
    """
    if new.spectral_grade not in SPECTRAL_TRANSCODE_GRADES:
        return False
    if existing.spectral_grade is None:
        return False
    if existing.spectral_grade in SPECTRAL_TRANSCODE_GRADES:
        return False
    return measurement_rank(
        new,
        cfg,
        target_contract=new_target_contract,
        v0_probe=new_v0_probe,
    ) < measurement_rank(existing, cfg)


def compare_quality(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    new_target_contract: TargetQualityContract | None = None,
    new_v0_probe: V0ProbeEvidence | None = None,
) -> QualityComparisonBasis:
    """Codec-aware quality comparison.

    Primary key is the QualityRank. Within the same rank:
    - LOSSLESS → always "equivalent" (bitrate variance has no quality meaning).
    - Different codec families → "equivalent" (Opus 128 vs MP3 V0 are
      perceptually indistinguishable at the TRANSPARENT band).
    - Same codec family, either side carries an explicit label ("mp3 v0" /
      "opus 128" / "mp3 320") → "equivalent". Labels are quality contracts
      and within the same rank tier are perceptually equivalent regardless of
      bitrate deltas (a 207 kbps V0 on lo-fi and a 245 kbps V0 on dense material
      are both TRANSPARENT — this is the lo-fi genuine V0 case).
    - Same codec family, both bare codec names → compare the configured metric
      with cfg.within_rank_tolerance_kbps tolerance.

    Shared-spectral bucket: when BOTH measurements carry ``spectral_bitrate_kbps``,
    clamp each side's classified bitrate to ``min(selected_metric, spectral)``
    for rank only. Same-rank tie-breaks still use the raw configured metric
    so higher-average files can replace lower-average files within the same
    spectral bucket. This keeps spectral as a demotion signal without letting
    a pessimistic estimate permanently freeze the album at the first source
    that happened to land in that bucket. See ``_shared_spectral_bitrates``
    for the narrow guard that keeps the Springsteen case (single stale
    estimate) on the container path. A transcode-grade candidate over a
    non-transcode-grade existing album has one extra guard: if its real
    selected-metric rank is lower before the spectral clamp, it is worse.

    Returns a ``QualityComparisonBasis`` — the verdict plus the branch that
    fired and the values that decided it, emitted HERE per-branch so the
    persisted explanation can never disagree with the decision (the request
    6039 lesson: any re-derivation outside this function eventually lies).
    Callers that only need the verdict read ``.verdict``.

    Pure function. No I/O, no hardcoded numbers — every threshold comes from cfg.
    """
    new_br, new_metric = _selected_quality_bitrate_with_source(
        new, cfg, new_v0_probe
    )
    existing_br, existing_metric = _selected_bitrate_with_source(existing, cfg)
    new_format = (
        new_target_contract.format
        if new_target_contract is not None
        else new.format
    )

    def _truthful_display_value(
        measurement: AudioQualityMeasurement,
        metric: str,
        value: Optional[int],
    ) -> tuple[str, Optional[int]]:
        """Name the evidence that actually classified one side.

        Explicit labels are encoder/storage contracts. Their rank ignores the
        measured bitrate, so persisting ``min 191k`` beside ``opus 128`` lies:
        191k may be a temporary V0 proxy. Numeric contracts retain their
        declared value for machine-readable audit; V-level contracts need no
        synthetic kbps value because the format label is the complete fact.
        """
        format_hint = (
            new_format if measurement is new else measurement.format
        )
        if _is_explicit_label(format_hint):
            declared = (
                _parse_bitrate_label(format_hint)
                if format_hint is not None else None
            )
            return "contract", declared
        return metric, value

    def _basis(
        verdict: str,
        branch: str,
        new_rank: QualityRank,
        existing_rank: QualityRank,
        new_value: Optional[int] = None,
        existing_value: Optional[int] = None,
        spectral_clamped: bool = False,
        tolerance_kbps: Optional[int] = None,
    ) -> QualityComparisonBasis:
        display_new_metric, display_new_value = _truthful_display_value(
            new, new_metric, new_value,
        )
        display_existing_metric, display_existing_value = (
            _truthful_display_value(
                existing, existing_metric, existing_value,
            )
        )
        return QualityComparisonBasis(
            verdict=verdict,
            branch=branch,
            new_rank=new_rank.name.lower(),
            existing_rank=existing_rank.name.lower(),
            new_metric=display_new_metric,
            existing_metric=display_existing_metric,
            new_value_kbps=display_new_value,
            existing_value_kbps=display_existing_value,
            # Lowercase-normalized: the hint's casing differs between the
            # simulator and evidence twins ("flac" vs "FLAC") while meaning
            # the same thing — display upper-cases, parity compares.
            new_format=new_format.lower() if new_format else None,
            existing_format=existing.format.lower() if existing.format else None,
            spectral_clamped=spectral_clamped,
            tolerance_kbps=tolerance_kbps,
        )

    if _transcode_candidate_real_rank_regresses(
        new,
        existing,
        cfg,
        new_target_contract=new_target_contract,
        new_v0_probe=new_v0_probe,
    ):
        return _basis(
            "worse", "transcode_rank_regression",
            measurement_rank(
                new,
                cfg,
                target_contract=new_target_contract,
                v0_probe=new_v0_probe,
            ), measurement_rank(existing, cfg),
            new_value=new_br, existing_value=existing_br,
        )

    shared = _shared_spectral_bitrates(
        new, existing, cfg, new_v0_probe=new_v0_probe
    )
    if shared is not None:
        clamped_new_br, clamped_existing_br = shared
        projected_is_cbr = (
            new_target_contract.is_cbr
            if new_target_contract is not None
            else new.is_cbr
        )
        new_rank = quality_rank(
            new_format, clamped_new_br, projected_is_cbr, cfg
        )
        existing_rank = quality_rank(
            existing.format, clamped_existing_br, existing.is_cbr, cfg)
        rank_new_value, rank_existing_value = clamped_new_br, clamped_existing_br
        spectral_clamped = True
    else:
        new_rank = measurement_rank(
            new,
            cfg,
            target_contract=new_target_contract,
            v0_probe=new_v0_probe,
        )
        existing_rank = measurement_rank(existing, cfg)
        rank_new_value, rank_existing_value = new_br, existing_br
        spectral_clamped = False

    if new_rank > existing_rank:
        return _basis(
            "better", "rank", new_rank, existing_rank,
            new_value=rank_new_value, existing_value=rank_existing_value,
            spectral_clamped=spectral_clamped,
        )
    if new_rank < existing_rank:
        return _basis(
            "worse", "rank", new_rank, existing_rank,
            new_value=rank_new_value, existing_value=rank_existing_value,
            spectral_clamped=spectral_clamped,
        )

    # Same rank. UNKNOWN has no orderable quality evidence, so measured
    # bitrate cannot turn one unmapped codec into an upgrade over another.
    # Keep the existing ``metric_missing`` basis vocabulary: the metrics are
    # deliberately not comparable for this rank even when byte probes found
    # numeric bitrates.
    if new_rank == QualityRank.UNKNOWN:
        return _basis(
            "equivalent", "metric_missing", new_rank, existing_rank,
            spectral_clamped=spectral_clamped,
        )

    # LOSSLESS is always equivalent — FLAC bitrates vary with sample rate and
    # bit depth, not quality.
    if new_rank == QualityRank.LOSSLESS:
        return _basis(
            "equivalent", "lossless_same_rank", new_rank, existing_rank,
            spectral_clamped=spectral_clamped,
        )

    new_family = _codec_family_of(new_format)
    existing_family = _codec_family_of(existing.format)

    # Different codec families at the same rank: perceptually equivalent.
    if new_family != existing_family:
        return _basis(
            "equivalent", "cross_family_same_rank", new_rank, existing_rank,
            new_value=new_br, existing_value=existing_br,
            spectral_clamped=spectral_clamped,
        )

    # Same codec family. If either side has an explicit label, the label is
    # authoritative — within the same rank tier they are equivalent.
    if _is_explicit_label(new_format) or _is_explicit_label(existing.format):
        return _basis(
            "equivalent", "label_contract_same_rank", new_rank, existing_rank,
            new_value=new_br, existing_value=existing_br,
            spectral_clamped=spectral_clamped,
        )

    # Both bare codec names — compare the chosen raw metric with tolerance.
    # When the shared-spectral bucket fired, rank has already been demoted by
    # the spectral floor. The tiebreaker deliberately stays on the raw metric
    # so equal spectral buckets can still converge upward by bitrate.
    if new_br is None or existing_br is None:
        return _basis(
            "equivalent", "metric_missing", new_rank, existing_rank,
            new_value=new_br, existing_value=existing_br,
            spectral_clamped=spectral_clamped,
        )
    delta = new_br - existing_br
    verdict = (
        "equivalent" if abs(delta) <= cfg.within_rank_tolerance_kbps
        else ("better" if delta > 0 else "worse")
    )
    return _basis(
        verdict, "metric_tiebreak", new_rank, existing_rank,
        new_value=new_br, existing_value=existing_br,
        spectral_clamped=spectral_clamped,
        tolerance_kbps=cfg.within_rank_tolerance_kbps,
    )
