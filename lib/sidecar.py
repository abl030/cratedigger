"""Verified-lossless album sidecar — the producer half of issue #184.

A ``cratedigger.json`` written into the beets album folder of every
verified-lossless import, so the evidence reseeds across the Soulseek network
and other Cratediggers can find a known-good lossless source without grinding.

The sidecar is **derived state**: it is rebuilt entirely from the
content-addressed ``AlbumQualityEvidence`` row (CLAUDE.md "everything is
derived"). Nothing here is canonical — if beets clobbers the file, regenerate
it from the evidence. This module owns only the on-disk schema and the pure
mapping; filesystem writes and DB reads live in ``lib/sidecar_service.py``.

Wire-boundary type per ``.claude/rules/code-quality.md``: ``AlbumSidecar`` and
its nested Structs are ``msgspec.Struct`` because the payload crosses JSON onto
disk and is consumed by other Cratediggers — the strict decoder is what
catches type drift between producer and consumer.
"""

from __future__ import annotations

from datetime import datetime

import msgspec

from lib.quality import AlbumQualityEvidence

# Non-hidden, recognisable name so other Cratediggers browsing a peer's slskd
# shares can actually see it — a dotfile would be filtered by many clients and
# defeat the whole network-propagation purpose.
SIDECAR_FILENAME = "cratedigger.json"
SIDECAR_GENERATOR = "cratedigger"
# Bump when the on-disk schema changes shape, so consumers can branch on it.
SIDECAR_SCHEMA_VERSION = 2


class SidecarTrack(msgspec.Struct, frozen=True):
    """One audio file in the verified-lossless album, by on-disk layout."""

    relative_path: str
    extension: str
    container: str
    codec: str | None = None
    size_bytes: int = 0


class SidecarQuality(msgspec.Struct, frozen=True):
    """Flattened quality summary mirrored from the evidence measurement."""

    codec: str | None = None
    container: str | None = None
    storage_format: str | None = None
    target_format: str | None = None
    spectral_grade: str | None = None
    spectral_bitrate_kbps: int | None = None
    min_bitrate_kbps: int | None = None
    avg_bitrate_kbps: int | None = None
    median_bitrate_kbps: int | None = None
    is_cbr: bool = False
    was_converted_from: str | None = None


class SidecarV0Metric(msgspec.Struct, frozen=True):
    """Neutral V0 probe metric using the v2 two-axis vocabulary.

    Schema v1 called this marker ``source_lineage``. That historical name is
    not reinterpreted: sidecars are derived and newly regenerated payloads use
    the honest ``subject`` field instead.
    """

    subject: str
    provenance: str
    min_bitrate_kbps: int | None = None
    avg_bitrate_kbps: int | None = None
    median_bitrate_kbps: int | None = None


class SidecarProof(msgspec.Struct, frozen=True):
    """Verified-lossless proof using the v2 provenance vocabulary.

    Schema v1 called this field ``proof_origin``. New derived payloads expose
    the proof provenance directly and do not silently change the meaning of a
    v1 key.
    """

    provenance: str
    source: str
    classifier: str
    detail: str | None = None


class AlbumSidecar(msgspec.Struct, frozen=True):
    """The on-disk ``cratedigger.json`` payload for a verified-lossless album.

    Strict-pressing identity is preserved by ``mb_release_id`` — a consumer
    matches the sidecar to a specific release, never a sibling pressing.
    """

    schema_version: int
    generator: str
    mb_release_id: str
    generated_at: datetime
    verified_lossless: bool
    quality: SidecarQuality
    tracks: list[SidecarTrack]
    audio_file_count: int = 0
    proof: SidecarProof | None = None
    v0_metric: SidecarV0Metric | None = None
    source_username: str | None = None


def should_write_sidecar(evidence: AlbumQualityEvidence) -> bool:
    """Gate: only verified-lossless albums get a sidecar (issue #184 scope)."""
    return evidence.verified_lossless_proof is not None


def build_sidecar(
    evidence: AlbumQualityEvidence,
    *,
    source_username: str | None,
    generated_at: datetime,
    generator: str = SIDECAR_GENERATOR,
) -> AlbumSidecar:
    """Map a verified-lossless evidence row onto the sidecar payload.

    Pure: no I/O, no clock read (``generated_at`` is injected). Callers gate
    on :func:`should_write_sidecar` first; this builds whatever it is given.
    """
    m = evidence.measurement
    quality = SidecarQuality(
        codec=evidence.codec,
        container=evidence.container,
        storage_format=evidence.storage_format,
        target_format=evidence.target_format,
        spectral_grade=m.spectral_grade,
        spectral_bitrate_kbps=m.spectral_bitrate_kbps,
        min_bitrate_kbps=m.min_bitrate_kbps,
        avg_bitrate_kbps=m.avg_bitrate_kbps,
        median_bitrate_kbps=m.median_bitrate_kbps,
        is_cbr=m.is_cbr,
        was_converted_from=m.was_converted_from,
    )
    tracks = [
        SidecarTrack(
            relative_path=f.relative_path,
            extension=f.extension,
            container=f.container,
            codec=f.codec,
            size_bytes=f.size_bytes,
        )
        for f in evidence.files
    ]
    proof = None
    if evidence.verified_lossless_proof is not None:
        p = evidence.verified_lossless_proof
        proof = SidecarProof(
            provenance=p.provenance,
            source=p.source,
            classifier=p.classifier,
            detail=p.detail,
        )
    v0_metric = None
    if evidence.v0_metric is not None:
        v = evidence.v0_metric
        v0_metric = SidecarV0Metric(
            min_bitrate_kbps=v.min_bitrate_kbps,
            avg_bitrate_kbps=v.avg_bitrate_kbps,
            median_bitrate_kbps=v.median_bitrate_kbps,
            subject=v.subject,
            provenance=v.provenance,
        )
    return AlbumSidecar(
        schema_version=SIDECAR_SCHEMA_VERSION,
        generator=generator,
        mb_release_id=evidence.mb_release_id,
        generated_at=generated_at,
        verified_lossless=evidence.verified_lossless_proof is not None,
        quality=quality,
        tracks=tracks,
        audio_file_count=evidence.audio_file_count,
        proof=proof,
        v0_metric=v0_metric,
        source_username=source_username,
    )
