"""Filetype specs / identity matching + search-tier helpers.

Extracted from the monolithic ``lib/quality.py`` (issue #477).  This module
also owns the pure policy that maps trusted current quality evidence to search
tier overrides.
"""

from dataclasses import dataclass
from typing import Any, Literal, Optional, Sequence

from lib.quality.evidence_types import (
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
)
from lib.quality.import_result_types import SpectralAnalysisDetail
from lib.quality.ranks import QualityRank, QualityRankConfig, measurement_rank


QUALITY_UPGRADE_TIERS = "lossless,mp3 v0,mp3 320,aac,opus,ogg"
QUALITY_LOSSLESS = "lossless"

# Deprecated aliases — keep for old code that references them
QUALITY_FLAC_ONLY = QUALITY_LOSSLESS


def search_tiers(search_filetype_override: str | None,
                 config_allowed: list[str]) -> tuple[list[str], bool]:
    """Return (filetypes_to_search, allow_catch_all) from a search_filetype_override.

    NULL override = use global config + allow catch-all fallback.
    Any CSV override = search exactly those tiers, no catch-all.
    """
    if not search_filetype_override:
        return list(config_allowed), True
    return [t.strip() for t in search_filetype_override.split(",")], False


def effective_search_tiers(
    search_filetype_override: str | None,
    target_format: str | None,
    config_allowed: list[str],
) -> tuple[list[str], bool]:
    """Compute effective search tiers merging both override sources.

    Priority: search_filetype_override > target_format > config defaults.
    """
    if search_filetype_override:
        return search_tiers(search_filetype_override, config_allowed)
    if target_format:
        return search_tiers(target_format, config_allowed)
    return search_tiers(None, config_allowed)


def should_clear_lossless_search_override(
    *,
    new_target_format: str | None,
    old_target_format: str | None,
    search_filetype_override: str | None,
) -> bool:
    """Should changing intent clear a stale lossless-only search override?

    This only clears the transient override when the user is explicitly
    turning off a previously requested lossless-on-disk intent.
    """
    old_keep_lossless = old_target_format in ("flac", "lossless")
    return (
        new_target_format is None
        and old_keep_lossless
        and search_filetype_override == QUALITY_LOSSLESS
    )


def resolve_user_requeue_override(existing_override: str | None) -> str:
    """Pick ``search_filetype_override`` for a user-initiated requeue.

    Preserves a stricter existing override — e.g. ``"lossless"`` set by the
    quality gate after a CBR 320 import — so user actions (Upgrade button,
    status reset back to wanted, ban-source) don't re-open search tiers the
    gate intentionally closed. Falls back to :data:`QUALITY_UPGRADE_TIERS`
    only when no override is currently set.

    Without this, clicking Upgrade on an imported album would overwrite a
    gate-narrowed ``"lossless"`` with the full upgrade ladder, re-enqueuing
    MP3 320 sources that the pipeline has already established can't produce
    an upgrade — each one gets rejected as a downgrade and the user sees a
    loop.
    """
    return existing_override or QUALITY_UPGRADE_TIERS


def resolve_retained_search_override(
    existing_override: str | None,
    proposed_override: str | None,
) -> str | None:
    """Keep an existing lossless-only scope after a retained import.

    A successful unverified import may narrow ordinary search to lossless, but
    it may not re-open lossy tiers that an earlier quality decision already
    removed. Verified-lossless terminal acceptance and explicit user intent
    changes own their separate override-clearing paths.
    """

    if existing_override == QUALITY_LOSSLESS:
        return QUALITY_LOSSLESS
    return proposed_override


def rejection_backfill_override(
    *,
    current_measurement: AudioQualityMeasurement | None,
    spectral_evidence_source: Literal[
        "attempt_have_audit", "linked_current_evidence"
    ],
    have_spectral_audit: SpectralAnalysisDetail | None = None,
    cfg: "QualityRankConfig | None" = None,
) -> str | None:
    """Constrain a transparent, spectrally genuine HAVE copy to lossless.

    ``current_measurement`` is the exact installed release. Import callers
    must select ``attempt_have_audit`` and pass their independently collected
    HAVE audit; a missing/incomplete/failed audit never falls back to the
    measurement's persisted spectral fields. Validation and diagnostic
    callers select ``linked_current_evidence`` only after loading the request's
    complete, exact-release evidence row.

    The threshold is deliberately the canonical ``TRANSPARENT`` rank: merely
    excellent lossy copies can still be improved by another lossy source.
    Codecs without a rank band stay ``UNKNOWN`` and fail open to continued
    searching.
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()
    if current_measurement is None:
        return None

    if spectral_evidence_source == "attempt_have_audit":
        if have_spectral_audit is None:
            return None
        if (
            not have_spectral_audit.attempted
            or have_spectral_audit.error is not None
        ):
            return None
        spectral_grade = have_spectral_audit.grade
    elif spectral_evidence_source == "linked_current_evidence":
        spectral_grade = current_measurement.spectral_grade
    else:
        return None
    if spectral_grade != "genuine":
        return None

    if measurement_rank(current_measurement, cfg) == QualityRank.TRANSPARENT:
        return QUALITY_LOSSLESS
    return None


# ---------------------------------------------------------------------------
# AudioFileSpec — single source of truth for filetype identity
# ---------------------------------------------------------------------------

# Extension → default codec. Most are 1:1; .m4a is ambiguous (resolved by heuristic).
_EXT_TO_CODEC: dict[str, str] = {
    "mp3": "mp3",
    "flac": "flac",
    "ogg": "ogg",
    "opus": "opus",
    "aac": "aac",
    "m4a": "aac",   # default; override to "alac" via heuristic
    "wma": "wma",
    "wav": "wav",
}

# Config DSL name → (codec, canonical extension)
_CONFIG_NAME_TO_CODEC: dict[str, tuple[str, str]] = {
    "mp3": ("mp3", "mp3"),
    "flac": ("flac", "flac"),
    "ogg": ("ogg", "ogg"),
    "opus": ("opus", "opus"),
    "aac": ("aac", "aac"),
    "alac": ("alac", "m4a"),
    "wma": ("wma", "wma"),
    "wav": ("wav", "wav"),
    "m4a": ("aac", "m4a"),
}

# Codec → canonical extension (for filename construction)
CODEC_TO_EXT: dict[str, str] = {
    "mp3": "mp3",
    "flac": "flac",
    "ogg": "ogg",
    "opus": "opus",
    "aac": "aac",
    "alac": "m4a",
    "wma": "wma",
    "wav": "wav",
}

# Canonical set of audio extensions (bare: "mp3", "flac", "m4a", ...)
AUDIO_EXTENSIONS: frozenset[str] = frozenset(_EXT_TO_CODEC.keys())

# Same but dotted (".mp3", ".flac", ".m4a", ...) for os.path.splitext consumers
AUDIO_EXTENSIONS_DOTTED: frozenset[str] = frozenset(f".{e}" for e in AUDIO_EXTENSIONS)

# Codecs that are lossless by definition
LOSSLESS_CODECS: frozenset[str] = frozenset({"flac", "alac", "wav"})

# Container-level lossless/lossy sets used by the mixed-source preimport
# reject in ``full_pipeline_decision_from_evidence``. Container is the
# discriminator (codec is too noisy: ``.m4a`` can hold ALAC or AAC). We
# duplicate the sets from ``lib.quality_evidence`` deliberately —
# importing them would invert the existing one-way dependency
# (``quality_evidence -> quality``). The mixed-source check has no other
# need for the broader filetype-band machinery.
_MIXED_REJECT_LOSSLESS_CONTAINERS: frozenset[str] = frozenset(
    {"flac", "alac", "wav", "aiff", "ape"}
)
_MIXED_REJECT_LOSSY_CONTAINERS: frozenset[str] = frozenset(
    {"mp3", "aac", "m4a", "ogg", "opus", "wma"}
)


def has_mixed_lossless_and_lossy(
    files: "Sequence[AlbumQualityEvidenceFile]",
) -> bool:
    """True when the snapshot contains both lossless and lossy containers.

    Cratedigger stays release-based: a folder that ships 15 FLACs + 2 MP3
    bonus tracks must be rejected outright, not partially imported. The
    historical bug (request 4445 Fast Times at Barrington High) was that
    ``determine_verified_lossless`` saw ``converted_count=15`` and stamped
    the whole album as verified-lossless, which then poisoned wrong-match
    cleanup against future fully-FLAC candidates. See
    ``TestPreimportFactRejects::test_mixed_source_routes_through_full_pipeline``.
    """
    if not files:
        return False
    containers = {f.container.lower() for f in files if f.container}
    return bool(
        containers & _MIXED_REJECT_LOSSLESS_CONTAINERS
        and containers & _MIXED_REJECT_LOSSY_CONTAINERS
    )

def _m4a_codec_heuristic(
    bitrate: Optional[int],
    bit_depth: Optional[int],
    sample_rate: Optional[int],
) -> str:
    """Guess whether a .m4a file is ALAC or AAC from slskd metadata.

    ALAC (lossless): bitRate > 700kbps, or bitDepth present.
    AAC (lossy): typically 64-320kbps.
    """
    if bit_depth is not None and bit_depth > 0:
        return "alac"
    if bitrate is not None and bitrate >= 700:
        return "alac"
    return "aac"


@dataclass(frozen=True)
class AudioFileSpec:
    """Single source of truth for filetype identity.

    Two forms:
    A. Filter (from config): codec + quality set, audio metadata None.
       Created via parse_filetype_config("mp3 v0").
    B. Identity (from slskd): codec + audio metadata set, quality None.
       Created via file_identity(slskd_file_dict).

    filetype_matches(identity, filter) replaces verify_filetype().
    """
    codec: str
    extension: str
    quality: Optional[str] = None
    bitrate: Optional[int] = None
    sample_rate: Optional[int] = None
    bit_depth: Optional[int] = None
    is_variable_bitrate: Optional[bool] = None

    @property
    def lossless(self) -> bool:
        """True for codecs that are lossless by definition."""
        return self.codec in LOSSLESS_CODECS

    @property
    def config_string(self) -> str:
        """Reconstruct the config DSL string, e.g. 'mp3 v0', 'alac'."""
        if self.quality:
            return f"{self.codec} {self.quality}"
        return self.codec


# Sentinel: matches any audio file (used for catch-all "download anything" mode)
CATCH_ALL_SPEC = AudioFileSpec(codec="*", extension="*")


def parse_filetype_config(config_str: str) -> AudioFileSpec:
    """Parse a config DSL string like 'mp3 v0' or 'alac' into AudioFileSpec.

    This is the FILTER form — quality is set, audio metadata is not.
    Use '*' or 'any' for catch-all mode (matches any audio file).
    Use 'lossless' to match any lossless codec (flac, alac, wav).
    """
    parts = config_str.strip().split(" ", 1)
    name = parts[0].lower()

    if name in ("*", "any"):
        return CATCH_ALL_SPEC
    if name == "lossless":
        return AudioFileSpec(codec="lossless", extension="*")

    quality = parts[1].strip() if len(parts) > 1 else None
    codec, extension = _CONFIG_NAME_TO_CODEC.get(name, (name, name))
    return AudioFileSpec(codec=codec, extension=extension, quality=quality)


def file_identity(file: dict[str, Any] | Any) -> AudioFileSpec:
    """Construct an AudioFileSpec from a raw slskd file dict.

    This is the IDENTITY form — audio metadata is set, quality is not.
    """
    filename = file["filename"]
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    bitrate = file.get("bitRate")
    sample_rate = file.get("sampleRate")
    bit_depth = file.get("bitDepth")
    is_vbr = file.get("isVariableBitRate")

    codec = _EXT_TO_CODEC.get(ext, ext)

    if ext == "m4a":
        codec = _m4a_codec_heuristic(bitrate, bit_depth, sample_rate)

    return AudioFileSpec(
        codec=codec,
        extension=ext,
        bitrate=bitrate,
        sample_rate=sample_rate,
        bit_depth=bit_depth,
        is_variable_bitrate=is_vbr,
    )


def filetype_matches(identity: AudioFileSpec, filter_spec: AudioFileSpec) -> bool:
    """Does a file identity match a filetype filter?

    Replaces the old verify_filetype() internals.  Pure function.
    """
    if filter_spec.codec == "*":
        return True

    # "lossless" virtual tier — matches any lossless codec
    if filter_spec.codec == "lossless":
        return identity.codec in LOSSLESS_CODECS

    if identity.codec != filter_spec.codec:
        return False

    if filter_spec.quality is None:
        return True

    quality = filter_spec.quality

    # Bitdepth/samplerate pair (e.g. "24/96")
    if "/" in quality:
        parts = quality.split("/")
        try:
            req_depth = parts[0]
            req_rate = str(int(float(parts[1]) * 1000))
        except (ValueError, IndexError):
            return False
        if identity.bit_depth is not None and identity.sample_rate is not None:
            return (str(identity.bit_depth) == req_depth and
                    str(identity.sample_rate) == req_rate)
        return False

    # VBR preset (e.g. "v0", "v2")
    if quality.lower() in ("v0", "v2"):
        if identity.bitrate is None:
            return False
        cbr_values = {128, 160, 192, 224, 256, 320}
        is_vbr = identity.bitrate not in cbr_values
        if identity.is_variable_bitrate is not None:
            is_vbr = identity.is_variable_bitrate
        if not is_vbr:
            return False
        if quality.lower() == "v0":
            return 220 <= identity.bitrate <= 280
        else:
            return 170 <= identity.bitrate <= 220

    # Minimum bitrate (e.g. "256+")
    if quality.endswith("+"):
        try:
            min_bitrate = int(quality[:-1])
        except ValueError:
            return False
        return identity.bitrate is not None and identity.bitrate >= min_bitrate

    # Exact bitrate (e.g. "320")
    return identity.bitrate is not None and str(identity.bitrate) == quality


def audio_file_matches(
    file: dict[str, Any] | Any,
    allowed_filetype: str | AudioFileSpec,
) -> bool:
    """Return whether a raw slskd file matches a configured audio tier.

    Unlike ``filetype_matches`` directly, this rejects non-audio extensions
    before catch-all matching. That keeps ``*`` useful for "any audio" without
    treating covers and README files as enqueueable tracks.
    """
    filename = file["filename"]
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in AUDIO_EXTENSIONS:
        return False
    filter_spec = (
        parse_filetype_config(allowed_filetype)
        if isinstance(allowed_filetype, str)
        else allowed_filetype
    )
    return filetype_matches(file_identity(file), filter_spec)


def search_cache_keys_for_identity(
    identity: AudioFileSpec,
    configured_matches: list[str],
) -> tuple[str, ...]:
    """Return search-cache keys for a slskd file identity.

    ``search_filetype_override='lossless'`` is a persisted virtual tier used by
    the quality gate. Runtime ``allowed_filetypes`` usually contains concrete
    codecs (``flac``, ``alac``, ``wav``), so cache construction must also expose
    a ``lossless`` key for lossless files or the enqueue path has no exact key
    to look up.
    """
    keys = list(configured_matches)
    if identity.lossless and QUALITY_LOSSLESS not in keys:
        keys.append(QUALITY_LOSSLESS)
    return tuple(keys)
