"""Pairwise quality comparison (compare_quality) and format-hint helpers.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from typing import Literal, Optional

from lib.quality.evidence_types import (
    AudioQualityMeasurement,
    SPECTRAL_TRANSCODE_GRADES,
)
from lib.quality.ranks import (
    QualityRank,
    QualityRankConfig,
    _codec_family_of,
    _parse_bitrate_label,
    _parse_vbr_level,
    _selected_bitrate,
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
# with a lossy rank band table are mapped; everything else returns None so the
# caller applies its conservative legacy "MP3" fallback rather than this
# function inventing a band.
_NATIVE_CODEC_LABELS: dict[str, str] = {
    "opus": "opus",
    "aac": "aac",
    "mp3": "MP3",
    "mp3float": "MP3",
}
_NATIVE_EXT_LABELS: dict[str, str] = {
    "opus": "opus",
    "aac": "aac",
    "m4a": "aac",
    "mp3": "MP3",
}


def native_codec_format_label(
    codec: Optional[str], ext: Optional[str] = None
) -> Optional[str]:
    """Map a probed codec name (or file-extension fallback) to the native-lossy
    ``AudioQualityMeasurement.format`` label the rank model keys on.

    Returns a label ``_codec_family_of`` recognises ("opus" / "aac" / "MP3"),
    or None for codecs with no lossy rank band (vorbis, unknown) so the caller
    can fall back conservatively. The probed codec name wins over the
    extension — an Opus stream in an ``.ogg`` container is "opus", not vorbis.

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
    new_br = _selected_bitrate(new, cfg)
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
    return measurement_rank(new, cfg) < measurement_rank(existing, cfg)


def compare_quality(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
) -> Literal["better", "worse", "equivalent"]:
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

    Pure function. No I/O, no hardcoded numbers — every threshold comes from cfg.
    """
    if _transcode_candidate_real_rank_regresses(new, existing, cfg):
        return "worse"

    shared = _shared_spectral_bitrates(new, existing, cfg)
    if shared is not None:
        clamped_new_br, clamped_existing_br = shared
        new_rank = quality_rank(new.format, clamped_new_br, new.is_cbr, cfg)
        existing_rank = quality_rank(
            existing.format, clamped_existing_br, existing.is_cbr, cfg)
    else:
        new_rank = measurement_rank(new, cfg)
        existing_rank = measurement_rank(existing, cfg)

    if new_rank > existing_rank:
        return "better"
    if new_rank < existing_rank:
        return "worse"

    # Same rank. LOSSLESS is always equivalent — FLAC bitrates vary with sample
    # rate and bit depth, not quality.
    if new_rank == QualityRank.LOSSLESS:
        return "equivalent"

    new_family = _codec_family_of(new.format)
    existing_family = _codec_family_of(existing.format)

    # Different codec families at the same rank: perceptually equivalent.
    if new_family != existing_family:
        return "equivalent"

    # Same codec family. If either side has an explicit label, the label is
    # authoritative — within the same rank tier they are equivalent.
    if _is_explicit_label(new.format) or _is_explicit_label(existing.format):
        return "equivalent"

    # Both bare codec names — compare the chosen raw metric with tolerance.
    # When the shared-spectral bucket fired, rank has already been demoted by
    # the spectral floor. The tiebreaker deliberately stays on the raw metric
    # so equal spectral buckets can still converge upward by bitrate.
    new_br = _selected_bitrate(new, cfg)
    existing_br = _selected_bitrate(existing, cfg)
    if new_br is None or existing_br is None:
        return "equivalent"
    delta = new_br - existing_br
    if abs(delta) <= cfg.within_rank_tolerance_kbps:
        return "equivalent"
    return "better" if delta > 0 else "worse"
