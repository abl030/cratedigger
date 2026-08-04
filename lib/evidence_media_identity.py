"""Canonical media facts shared by Beets and current-evidence policy.

This is intentionally the small evidence-boundary vocabulary, not a global
format registry.  Ranking, search, and spectral calibration retain their own
domain-specific classifications.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

# Beets' admitted ``items.format`` vocabulary. It is generally a codec family,
# except for the product/container labels below. Unknown labels deliberately
# retain their source spelling so downstream validation remains fail-closed.
BEETS_CODEC_LABELS: Mapping[str, str] = MappingProxyType({
    "mp3": "mp3",
    "aac": "aac",
    "alac": "alac",
    "flac": "flac",
    "opus": "opus",
    "vorbis": "vorbis",
    "wav": "wav",
    "wma": "wma",
    "windows media": "wma",
    "ogg": "vorbis",
})
BEETS_FORMAT_ALIASES: Mapping[str, str] = MappingProxyType({
    observed: canonical
    for observed, canonical in BEETS_CODEC_LABELS.items()
    if observed != canonical
})

# These are codec facts, rather than filename-extension policy.  In
# particular, M4A is absent because it is an ambiguous container: it may hold
# AAC or ALAC and needs the authoritative codec label from Beets.
EVIDENCE_LOSSLESS_CODECS = frozenset({"flac", "alac", "wav"})

# Snapshot containers remain distinct from codecs.  The pair table is the
# authority check for a preserved source measurement; a known lossy container
# alone cannot authorize preservation.
LOSSY_CODECS_BY_CONTAINER: Mapping[str, frozenset[str]] = MappingProxyType({
    "mp3": frozenset({"mp3"}),
    "aac": frozenset({"aac"}),
    "m4a": frozenset({"aac"}),
    "ogg": frozenset({"vorbis", "opus"}),
    "opus": frozenset({"opus"}),
    "wma": frozenset({"wma"}),
})

EVIDENCE_LOSSY_CODECS: frozenset[str] = frozenset(
    codec
    for codecs in LOSSY_CODECS_BY_CONTAINER.values()
    for codec in codecs
)
EVIDENCE_LOSSLESS_CONTAINERS = frozenset({"flac", "alac", "wav", "aiff", "ape"})
EVIDENCE_LOSSY_CONTAINERS = frozenset(LOSSY_CODECS_BY_CONTAINER)


def canonical_beets_format(observed: str) -> str:
    """Return Beets' evidence-facing canonical format without guessing unknowns."""

    return BEETS_FORMAT_ALIASES.get(observed.lower(), observed)


def canonical_beets_codec(observed: str) -> str:
    """Return a lowercase canonical codec for aggregate authority checks."""

    normalized = observed.strip().lower()
    return BEETS_CODEC_LABELS.get(normalized, normalized)


def is_lossless_evidence_codec(codec: str | None) -> bool:
    """Whether a canonical evidence codec is native lossless."""

    return codec is not None and codec.strip().lower() in EVIDENCE_LOSSLESS_CODECS


def authoritative_lossy_media_pair(
    container: str | None,
    codec: str | None,
) -> bool:
    """Whether an installed container/codec pair is known lossy.

    Both facts must be present.  This deliberately rejects ambiguous M4A
    without an AAC codec and native ALAC in an M4A container.
    """

    if container is None or codec is None:
        return False
    canonical_codec = codec.strip().lower()
    allowed_codecs = LOSSY_CODECS_BY_CONTAINER.get(
        container.strip().lower(),
        frozenset(),
    )
    return (
        canonical_codec in EVIDENCE_LOSSY_CODECS
        and canonical_codec in allowed_codecs
    )
