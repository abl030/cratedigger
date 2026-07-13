"""Codec-aware quality rank model: QualityRank, QualityRankConfig, rank functions.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

import configparser
import json
from dataclasses import dataclass, field, asdict
from enum import IntEnum, StrEnum
from typing import Any, Optional

from lib.quality.evidence_types import (
    AudioQualityMeasurement,
    TargetQualityContract,
    V0_PROBE_LOSSLESS_SOURCE,
    V0ProbeEvidence,
)


# ---------------------------------------------------------------------------
# Codec-aware quality rank model (issue #60)
# ---------------------------------------------------------------------------
#
# The pipeline needs to compare audio quality across codecs (Opus 128 ≈ MP3 V0)
# and apply a tier floor when verified_lossless targets would otherwise bypass
# all guardrails (e.g. a FLAC → Opus 64 target replacing a genuine MP3 V0).
#
# Every numeric threshold, codec set, and policy knob lives in QualityRankConfig.
# Grep the decision path for a bare kbps value and you should find zero hits
# outside log strings — everything routes through cfg.quality_ranks.<field>.
#
# Spectral cliff detection and transcode_detection() continue to use min
# bitrate regardless of QualityRankConfig.bitrate_metric — those care about
# the worst track, not the album average. Rank classification is different
# because a single quiet track in a legitimately encoded VBR album should not
# drag the whole album down a rank.


class RankBitrateMetric(StrEnum):
    """Which per-album bitrate statistic feeds into quality_rank() classification.

    MIN    — minimum per-track bitrate. Legacy behavior. Conservative and prone
             to VBR false negatives on albums with genuinely quiet tracks.
    AVG    — album-mean per-track bitrate. Recommended for VBR codecs. Default.
    MEDIAN — middle per-track bitrate. Robust against per-track outliers
             (intro/outro silence, hidden tracks, very short interludes) where
             a single low track would drag MIN down and a few skewed tracks
             could pull AVG away from the typical track quality.

    measurement_rank() is the only function that dispatches on this enum.
    Each metric has a matching field on AudioQualityMeasurement / AlbumInfo;
    when the configured metric's field is None, measurement_rank() falls back
    to min_bitrate_kbps so legacy callers still classify correctly.
    """
    MIN = "min"
    AVG = "avg"
    MEDIAN = "median"


class QualityRank(IntEnum):
    """Perceptual quality bands. IntEnum so > / >= comparisons work naturally.

    Integer spacing leaves room for inserting new bands without reshuffling.
    Nothing persists the integer value — rank is always recomputed from a
    measurement + config, so a future insertion is safe.
    """
    UNKNOWN     = 0
    POOR        = 20
    ACCEPTABLE  = 30
    GOOD        = 40
    EXCELLENT   = 50
    TRANSPARENT = 60
    LOSSLESS    = 100


_RANK_NAME_TO_VALUE: dict[str, QualityRank] = {
    "unknown":     QualityRank.UNKNOWN,
    "poor":        QualityRank.POOR,
    "acceptable":  QualityRank.ACCEPTABLE,
    "good":        QualityRank.GOOD,
    "excellent":   QualityRank.EXCELLENT,
    "transparent": QualityRank.TRANSPARENT,
    "lossless":    QualityRank.LOSSLESS,
}


@dataclass(frozen=True)
class CodecRankBands:
    """Bitrate thresholds (kbps) for a single codec family.

    A measurement's rank is the highest band whose threshold the configured
    metric meets or exceeds. Thresholds must be monotonically non-increasing:
    transparent >= excellent >= good >= acceptable >= 0.
    Values below ``acceptable`` are classified POOR.
    """
    transparent: int
    excellent: int
    good: int
    acceptable: int

    def __post_init__(self) -> None:
        if not (self.transparent >= self.excellent >= self.good
                >= self.acceptable >= 0):
            raise ValueError(
                f"CodecRankBands must be monotonic "
                f"(transparent >= excellent >= good >= acceptable >= 0): {self}")

    def rank_for(self, bitrate_kbps: Optional[int]) -> QualityRank:
        """Classify a bitrate against this codec's band table."""
        if bitrate_kbps is None:
            return QualityRank.UNKNOWN
        if bitrate_kbps >= self.transparent:
            return QualityRank.TRANSPARENT
        if bitrate_kbps >= self.excellent:
            return QualityRank.EXCELLENT
        if bitrate_kbps >= self.good:
            return QualityRank.GOOD
        if bitrate_kbps >= self.acceptable:
            return QualityRank.ACCEPTABLE
        return QualityRank.POOR


@dataclass(frozen=True)
class QualityRankConfig:
    """Every knob for the codec-aware rank model.

    This is the ONLY place numeric quality thresholds live. If you grep the
    rank decision path for a hardcoded kbps value you should find zero hits
    outside log strings — everything routes through cfg.quality_ranks.<field>.

    Defaults are documented in docs/quality-ranks.md. Summary:

    - Opus ``transparent=112``: ``ffmpeg -b:a 128k`` unconstrained VBR averages
      120-135 kbps on typical music; 112 leaves headroom for sparse material.
      ``excellent=88`` matches Opus 96 quality (hydrogenaudio/Kamedo2 4.65/5).
    - MP3 VBR ``transparent=210``: matches the legacy
      ``QUALITY_MIN_BITRATE_KBPS`` constant; V2 averages ~190 → excellent at 170.
    - MP3 CBR ``transparent=320``: unverifiable CBR is only transparent at 320.
    - AAC ``transparent=192``: hydrogenaudio consensus ceiling for music.
    """
    # --- Policy ---
    bitrate_metric: RankBitrateMetric = RankBitrateMetric.AVG
    gate_min_rank: QualityRank = QualityRank.EXCELLENT
    within_rank_tolerance_kbps: int = 5

    # --- Per-codec band tables ---
    # Defaults are tuned to preserve the legacy 210 kbps gate threshold for
    # bare-codec MP3 VBR measurements (old QUALITY_MIN_BITRATE_KBPS) while
    # adding perceptual tiers above and below. Explicit labels like "mp3 v0"
    # / "opus 128" bypass the band tables via the V-level / declared-bitrate
    # resolution steps — those classify by contract.
    opus:    CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=112, excellent=88, good=64, acceptable=48))
    mp3_vbr: CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=245, excellent=210, good=170, acceptable=130))
    mp3_cbr: CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=320, excellent=256, good=192, acceptable=128))
    aac:     CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=192, excellent=144, good=112, acceptable=80))

    # --- LAME VBR V-level → rank (10-tuple indexed by V0..V9) ---
    # V-level semantics are a LAME encoder contract, but surfacing them here
    # keeps the entire policy in one dataclass per the no-magic-numbers rule.
    mp3_vbr_levels: tuple[QualityRank, ...] = (
        QualityRank.TRANSPARENT,  # V0
        QualityRank.EXCELLENT,    # V1
        QualityRank.EXCELLENT,    # V2
        QualityRank.GOOD,         # V3
        QualityRank.GOOD,         # V4
        QualityRank.ACCEPTABLE,   # V5
        QualityRank.ACCEPTABLE,   # V6
        QualityRank.ACCEPTABLE,   # V7
        QualityRank.ACCEPTABLE,   # V8
        QualityRank.ACCEPTABLE,   # V9
    )

    # --- Lossless codec identity ---
    lossless_codecs: frozenset[str] = frozenset({"flac", "lossless", "alac", "wav"})

    # --- Mixed-format album precedence (worst codec wins for classification) ---
    # When an album has multiple formats on disk (rare), pick the lowest-rank
    # codec as the album's "canonical" codec so the rank stays conservative.
    mixed_format_precedence: tuple[str, ...] = ("mp3", "aac", "opus", "flac")

    @classmethod
    def defaults(cls) -> "QualityRankConfig":
        return cls()

    # ------------------------------------------------------------------
    # [Quality Ranks] config.ini parsing
    # ------------------------------------------------------------------

    @classmethod
    def from_ini(
        cls,
        parser: configparser.RawConfigParser,
        section: str = "Quality Ranks",
    ) -> "QualityRankConfig":
        """Parse a [Quality Ranks] section into a QualityRankConfig.

        Every key is optional — missing keys fall back to the field's default
        value, so users can customize one codec or one band without writing
        out the entire section.

        Key names (all lowercase, codec-prefixed for bands):

            bitrate_metric            = min | avg | median
            gate_min_rank             = unknown|poor|acceptable|good|excellent|transparent|lossless
            within_rank_tolerance_kbps = <int>
            <codec>.<band>            = <int>
              codecs: opus, mp3_vbr, mp3_cbr, aac
              bands:  transparent, excellent, good, acceptable
            mp3_vbr_levels            = <rank>,<rank>,... (exactly 10, V0..V9)
            lossless_codecs           = <codec>,<codec>,... (set, lowercased)
            mixed_format_precedence   = <codec>,<codec>,... (ordered list, lowercased)

        Invalid values raise ValueError at parse time with a diagnostic that
        names the offending key. Missing section silently returns defaults.
        """
        base = cls.defaults()
        if not parser.has_section(section):
            return base

        def _get_str(key: str, default: str) -> str:
            raw = parser.get(section, key, fallback=None)
            if raw is None or raw.strip() == "":
                return default
            return raw.strip()

        def _get_int(key: str, default: int) -> int:
            raw = parser.get(section, key, fallback=None)
            if raw is None or raw.strip() == "":
                return default
            try:
                return int(raw.strip())
            except ValueError as exc:
                raise ValueError(
                    f"[{section}] {key}: expected integer, got {raw!r}") from exc

        # --- Policy ---
        metric_str = _get_str("bitrate_metric", base.bitrate_metric.value).lower()
        try:
            metric = RankBitrateMetric(metric_str)
        except ValueError as exc:
            raise ValueError(
                f"[{section}] bitrate_metric: expected one of "
                f"{[m.value for m in RankBitrateMetric]}, got {metric_str!r}"
            ) from exc

        rank_str = _get_str("gate_min_rank", base.gate_min_rank.name.lower()).lower()
        if rank_str not in _RANK_NAME_TO_VALUE:
            raise ValueError(
                f"[{section}] gate_min_rank: expected one of "
                f"{sorted(_RANK_NAME_TO_VALUE.keys())}, got {rank_str!r}")
        gate_min_rank = _RANK_NAME_TO_VALUE[rank_str]

        tolerance = _get_int("within_rank_tolerance_kbps", base.within_rank_tolerance_kbps)
        if tolerance < 0:
            raise ValueError(
                f"[{section}] within_rank_tolerance_kbps: must be >= 0, got {tolerance}")

        # --- Codec bands ---
        def _get_bands(codec: str, default: CodecRankBands) -> CodecRankBands:
            return CodecRankBands(
                transparent=_get_int(f"{codec}.transparent", default.transparent),
                excellent=_get_int(f"{codec}.excellent", default.excellent),
                good=_get_int(f"{codec}.good", default.good),
                acceptable=_get_int(f"{codec}.acceptable", default.acceptable),
            )

        try:
            opus = _get_bands("opus", base.opus)
            mp3_vbr = _get_bands("mp3_vbr", base.mp3_vbr)
            mp3_cbr = _get_bands("mp3_cbr", base.mp3_cbr)
            aac = _get_bands("aac", base.aac)
        except ValueError as exc:
            # CodecRankBands.__post_init__ raises on non-monotonic; re-wrap
            # with section context.
            raise ValueError(f"[{section}] invalid codec bands: {exc}") from exc

        # --- Collection fields (issue #65) ---
        # CSV with leading/trailing whitespace tolerated. An unset key (raw
        # is None) and an empty `key = ` both fall through to the default;
        # an explicit list with no usable values (e.g. just commas) is a
        # config error so the user gets a diagnostic instead of silently
        # losing all lossless codecs.
        def _split_csv(key: str) -> list[str] | None:
            raw = parser.get(section, key, fallback=None)
            if raw is None or raw.strip() == "":
                return None
            parts = [p.strip() for p in raw.split(",")]
            parts = [p for p in parts if p]
            if not parts:
                raise ValueError(
                    f"[{section}] {key}: list contains no usable entries "
                    f"(got {raw!r})")
            return parts

        # mp3_vbr_levels — exactly 10 entries, each a QualityRank name.
        levels_parts = _split_csv("mp3_vbr_levels")
        if levels_parts is None:
            mp3_vbr_levels = base.mp3_vbr_levels
        else:
            if len(levels_parts) != 10:
                raise ValueError(
                    f"[{section}] mp3_vbr_levels: expected exactly 10 ranks "
                    f"(V0..V9), got {len(levels_parts)}")
            parsed_levels: list[QualityRank] = []
            for i, name in enumerate(levels_parts):
                key_norm = name.lower()
                if key_norm not in _RANK_NAME_TO_VALUE:
                    raise ValueError(
                        f"[{section}] mp3_vbr_levels: entry {i} (V{i}) "
                        f"is {name!r}, expected one of "
                        f"{sorted(_RANK_NAME_TO_VALUE.keys())}")
                parsed_levels.append(_RANK_NAME_TO_VALUE[key_norm])
            mp3_vbr_levels = tuple(parsed_levels)

        # lossless_codecs — frozenset, lowercased, deduplicated.
        lossless_parts = _split_csv("lossless_codecs")
        if lossless_parts is None:
            lossless_codecs = base.lossless_codecs
        else:
            lossless_codecs = frozenset(p.lower() for p in lossless_parts)

        # mixed_format_precedence — ordered tuple, lowercased. Order matters
        # because _reduce_album_format takes the first match.
        precedence_parts = _split_csv("mixed_format_precedence")
        if precedence_parts is None:
            mixed_format_precedence = base.mixed_format_precedence
        else:
            mixed_format_precedence = tuple(p.lower() for p in precedence_parts)

        return cls(
            bitrate_metric=metric,
            gate_min_rank=gate_min_rank,
            within_rank_tolerance_kbps=tolerance,
            opus=opus,
            mp3_vbr=mp3_vbr,
            mp3_cbr=mp3_cbr,
            aac=aac,
            mp3_vbr_levels=mp3_vbr_levels,
            lossless_codecs=lossless_codecs,
            mixed_format_precedence=mixed_format_precedence,
        )

    # ------------------------------------------------------------------
    # JSON round-trip (used by the import_one.py argv protocol)
    # ------------------------------------------------------------------

    def to_json(self) -> str:
        """Serialize to JSON for the --quality-rank-config harness argv."""
        payload: dict[str, Any] = {
            "bitrate_metric": self.bitrate_metric.value,
            "gate_min_rank": int(self.gate_min_rank),
            "within_rank_tolerance_kbps": self.within_rank_tolerance_kbps,
            "opus": asdict(self.opus),
            "mp3_vbr": asdict(self.mp3_vbr),
            "mp3_cbr": asdict(self.mp3_cbr),
            "aac": asdict(self.aac),
            "mp3_vbr_levels": [int(r) for r in self.mp3_vbr_levels],
            "lossless_codecs": sorted(self.lossless_codecs),
            "mixed_format_precedence": list(self.mixed_format_precedence),
        }
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> "QualityRankConfig":
        """Inverse of to_json().

        Missing keys / invalid enum values raise ValueError with a
        QualityRankConfig-qualified diagnostic so the harness operator can
        identify which field corrupted the argv round-trip. Used by
        harness/import_one.py to deserialize the --quality-rank-config argv
        blob emitted by dispatch_import_core().
        """
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"QualityRankConfig.from_json: invalid JSON: {exc}") from exc
        try:
            return cls(
                bitrate_metric=RankBitrateMetric(payload["bitrate_metric"]),
                gate_min_rank=QualityRank(int(payload["gate_min_rank"])),
                within_rank_tolerance_kbps=int(payload["within_rank_tolerance_kbps"]),
                opus=CodecRankBands(**payload["opus"]),
                mp3_vbr=CodecRankBands(**payload["mp3_vbr"]),
                mp3_cbr=CodecRankBands(**payload["mp3_cbr"]),
                aac=CodecRankBands(**payload["aac"]),
                mp3_vbr_levels=tuple(
                    QualityRank(int(r)) for r in payload["mp3_vbr_levels"]),
                lossless_codecs=frozenset(payload["lossless_codecs"]),
                mixed_format_precedence=tuple(payload["mixed_format_precedence"]),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError(
                f"QualityRankConfig.from_json: failed to reconstruct config: "
                f"{type(exc).__name__}: {exc}") from exc


# Known codec family names produced by _codec_family_of().
_KNOWN_CODEC_FAMILIES: frozenset[str] = frozenset(
    {"opus", "mp3", "aac", "flac", "alac", "wav", "lossless", "unknown"})


def _codec_family_of(format_hint: Optional[str]) -> str:
    """First token of format, lowercased — "opus 128" → "opus", "MP3" → "mp3"."""
    if format_hint is None:
        return "unknown"
    first = format_hint.strip().lower().split(None, 1)
    if not first or not first[0]:
        return "unknown"
    token = first[0]
    if token in _KNOWN_CODEC_FAMILIES:
        return token
    return "unknown"


def _parse_vbr_level(format_hint: str) -> Optional[int]:
    """Parse V-level from a label like "mp3 v0" / "mp3 v9". Returns None otherwise."""
    parts = format_hint.strip().lower().split()
    if len(parts) < 2 or parts[0] != "mp3":
        return None
    quality = parts[1]
    if len(quality) >= 2 and quality[0] == "v" and quality[1:].isdigit():
        level = int(quality[1:])
        if 0 <= level <= 9:
            return level
    return None


def _parse_bitrate_label(format_hint: str) -> Optional[int]:
    """Parse a numeric bitrate from a label like "opus 128" / "mp3 320"."""
    parts = format_hint.strip().lower().split()
    if len(parts) < 2:
        return None
    quality = parts[1]
    if quality.isdigit():
        return int(quality)
    return None


def quality_rank(
    format_hint: Optional[str],
    bitrate_kbps: Optional[int],
    is_cbr: bool,
    cfg: QualityRankConfig,
) -> QualityRank:
    """Classify a measurement into a QualityRank (pure, no I/O).

    Args:
        format_hint: Either a label like "opus 128" / "mp3 v0" / "mp3 320" /
            "flac" (from ImportResult.final_format / album_requests.final_format)
            OR a bare codec string like "MP3" / "Opus" / "FLAC" / "AAC" (from
            beets items.format). None → UNKNOWN.
        bitrate_kbps: The bitrate value to classify. The caller has already
            selected this value per cfg.bitrate_metric — this function does
            NOT dispatch on the metric. Use measurement_rank() as the entry
            point for measurements.
        is_cbr: True if all tracks share the same bitrate. Affects MP3 family
            routing (VBR vs CBR bands).
        cfg: Rank bands and policy.

    Resolution order:
        1. format_hint is None and bitrate_kbps is None → UNKNOWN.
        2. First token of format_hint in cfg.lossless_codecs → LOSSLESS.
        3. Explicit VBR label ("mp3 v0"): index into cfg.mp3_vbr_levels.
           Label is self-certifying — bitrate is irrelevant here.
        4. Explicit bitrate label ("opus 128"): classify declared bitrate
           against the matching codec's CodecRankBands. The label is a
           contract — we converted to this target, so the declaration wins
           over any measured bitrate.
        5. Bare codec name ("MP3" / "Opus" / "AAC"): classify the measured
           bitrate_kbps against the matching band table. "MP3" + is_cbr=True
           → cfg.mp3_cbr, otherwise cfg.mp3_vbr. Opus and AAC always use
           their own VBR-ish bands.
        6. Unknown codec → UNKNOWN (never promote garbage).
    """
    if format_hint is None and bitrate_kbps is None:
        return QualityRank.UNKNOWN

    family = _codec_family_of(format_hint)

    # Step 2 — lossless
    if family in cfg.lossless_codecs:
        return QualityRank.LOSSLESS

    # Step 3 — explicit VBR V-level label
    if format_hint is not None:
        vbr_level = _parse_vbr_level(format_hint)
        if vbr_level is not None:
            return cfg.mp3_vbr_levels[vbr_level]

    # Step 4 — explicit bitrate label ("opus 128" / "mp3 320" / "aac 192")
    if format_hint is not None:
        declared = _parse_bitrate_label(format_hint)
        if declared is not None:
            if family == "opus":
                return cfg.opus.rank_for(declared)
            if family == "mp3":
                # A "mp3 320"-style label is by convention CBR.
                return cfg.mp3_cbr.rank_for(declared)
            if family == "aac":
                return cfg.aac.rank_for(declared)
            return QualityRank.UNKNOWN

    # Step 5 — bare codec name + measured bitrate
    if family == "opus":
        return cfg.opus.rank_for(bitrate_kbps)
    if family == "aac":
        return cfg.aac.rank_for(bitrate_kbps)
    if family == "mp3":
        if is_cbr:
            return cfg.mp3_cbr.rank_for(bitrate_kbps)
        return cfg.mp3_vbr.rank_for(bitrate_kbps)

    # Step 6 — unknown codec, refuse to promote
    return QualityRank.UNKNOWN


def measurement_rank(
    m: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    target_contract: TargetQualityContract | None = None,
    v0_probe: V0ProbeEvidence | None = None,
) -> QualityRank:
    """Pick the configured bitrate metric from m and classify it.

    This is the ONLY function that dispatches on cfg.bitrate_metric. Each
    metric has a matching field on AudioQualityMeasurement: AVG → avg,
    MEDIAN → median, MIN → min.

    Falls back to min_bitrate_kbps when the configured metric's field is
    None — so legacy measurements (which only populate min) continue to
    classify correctly regardless of the configured policy.
    """
    bitrate, _metric = _selected_quality_bitrate_with_source(m, cfg, v0_probe)
    format_hint = target_contract.format if target_contract is not None else m.format
    return quality_rank(format_hint, bitrate, m.is_cbr, cfg)


def _selected_quality_bitrate_with_source(
    measurement: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    v0_probe: V0ProbeEvidence | None = None,
) -> tuple[Optional[int], str]:
    """Select a statistic without copying probe values into a measurement."""

    if v0_probe is None or v0_probe.kind != V0_PROBE_LOSSLESS_SOURCE:
        return _selected_bitrate_with_source(measurement, cfg)
    if cfg.bitrate_metric == RankBitrateMetric.AVG and v0_probe.avg_bitrate_kbps is not None:
        return v0_probe.avg_bitrate_kbps, RankBitrateMetric.AVG.value
    if (
        cfg.bitrate_metric == RankBitrateMetric.MEDIAN
        and v0_probe.median_bitrate_kbps is not None
    ):
        return v0_probe.median_bitrate_kbps, RankBitrateMetric.MEDIAN.value
    return v0_probe.min_bitrate_kbps, RankBitrateMetric.MIN.value


def _selected_bitrate(m: AudioQualityMeasurement,
                      cfg: QualityRankConfig) -> Optional[int]:
    """Return the bitrate value measurement_rank() would classify for m.

    Used by compare_quality() for the same-rank, same-codec tiebreaker.
    Keeps the metric dispatch in one place — compare_quality does not
    peek into m.avg / m.median / m.min directly.
    """
    return _selected_bitrate_with_source(m, cfg)[0]


def _selected_bitrate_with_source(
    m: AudioQualityMeasurement,
    cfg: QualityRankConfig,
) -> "tuple[Optional[int], str]":
    """(value, stat-name) pair for the metric measurement_rank() classifies.

    The stat name ("min" / "avg" / "median") records the statistic ACTUALLY
    used — the configured metric falls back to min when its field is
    unmeasured, and the persisted QualityComparisonBasis must say which one
    each side really classified (a basis claiming "avg" for a min value is
    the same display lie the basis exists to kill).
    """
    # Use `==` not `is`: RankBitrateMetric is a StrEnum. Historically the
    # project loaded modules under two names (``lib.quality`` and bare
    # ``quality`` via a PYTHONPATH ``lib`` entry) — an enum member from
    # one copy compared equal by string value but NOT identical to the
    # other's, so identity-compare silently fell through to min_bitrate,
    # breaking the AVG policy in the web simulator for VBR albums (issue
    # #93 post-deploy probe). The dual-load is gone (#445 item 3; pinned
    # by tests/test_no_dual_load.py), but `==` remains the correct
    # comparison for a StrEnum either way.
    if cfg.bitrate_metric == RankBitrateMetric.AVG and m.avg_bitrate_kbps is not None:
        return m.avg_bitrate_kbps, RankBitrateMetric.AVG.value
    if (cfg.bitrate_metric == RankBitrateMetric.MEDIAN
            and m.median_bitrate_kbps is not None):
        return m.median_bitrate_kbps, RankBitrateMetric.MEDIAN.value
    return m.min_bitrate_kbps, RankBitrateMetric.MIN.value


# ---------------------------------------------------------------------------
# Post-import quality gate (runs after successful import in cratedigger.py)
# ---------------------------------------------------------------------------

def gate_rank(
    current: AudioQualityMeasurement,
    cfg: "QualityRankConfig",
    *,
    target_contract: TargetQualityContract | None = None,
) -> QualityRank:
    """Rank used by ``quality_gate_decision()`` — measurement rank with the
    spectral clamp applied.

    This is the single source of truth for "what rank does the gate see".
    Both ``quality_gate_decision()`` and the simulator (``pipeline-cli quality``)
    call this so the displayed rank label and the actual gate verdict can
    never disagree.

    Verified lossless sources skip the spectral clamp: the source has already
    been proven by the lossless/V0 path, and production persists the current
    spectral bitrate as the actual imported bitrate for those rows rather than
    the pre-import cliff estimate.

    Spectral clamp: when the current measurement carries a spectral estimate
    (set upstream only when the grade is suspect/likely_transcode — see
    ``_check_quality_gate_core()``), classify that estimate against the MP3
    VBR band table and take the lower rank. This catches fake 320s and
    legacy low-spectral transcodes.
    """
    rank = measurement_rank(current, cfg, target_contract=target_contract)
    if current.verified_lossless:
        return rank
    if current.spectral_bitrate_kbps is not None:
        spectral_rank = quality_rank(
            "mp3", current.spectral_bitrate_kbps, is_cbr=False, cfg=cfg)
        if spectral_rank < rank:
            rank = spectral_rank
    return rank
