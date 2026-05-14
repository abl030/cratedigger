"""Album-quality evidence construction and persistence helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from lib.quality import (
    ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
    ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
    ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
    V0_PROBE_LOSSLESS_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH,
    V0_PROBE_ON_DISK_RESEARCH,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityEvidenceOwner,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    ImportResult,
    V0ProbeEvidence,
    VerifiedLosslessProof,
)


_AUDIO_EXTENSIONS = {
    ".aac",
    ".aiff",
    ".alac",
    ".ape",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".opus",
    ".wav",
    ".wma",
}

_NEUTRAL_V0_LINEAGE = {
    V0_PROBE_LOSSLESS_SOURCE: "lossless_source",
    V0_PROBE_NATIVE_LOSSY_RESEARCH: "native_lossy_research",
    V0_PROBE_ON_DISK_RESEARCH: "on_disk_research",
}


@dataclass(frozen=True)
class EvidenceBuildResult:
    """Result of trying to build evidence from a fileset."""

    evidence: AlbumQualityEvidence | None
    status: str
    reason: str | None = None

    @property
    def available(self) -> bool:
        return self.evidence is not None


def candidate_owner(
    *,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
) -> AlbumQualityEvidenceOwner | None:
    """Return the persisted candidate owner for an import candidate."""

    if download_log_id is not None:
        return AlbumQualityEvidenceOwner(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            owner_id=download_log_id,
        )
    if import_job_id is not None:
        return AlbumQualityEvidenceOwner(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
            owner_id=import_job_id,
        )
    return None


def request_current_owner(request_id: int) -> AlbumQualityEvidenceOwner:
    return AlbumQualityEvidenceOwner(
        owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
        owner_id=request_id,
    )


def snapshot_audio_files(root: str) -> list[AlbumQualityEvidenceFile]:
    """Build sorted active snapshot rows for audio files under ``root``."""

    if not os.path.isdir(root):
        return []
    files: list[AlbumQualityEvidenceFile] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in _AUDIO_EXTENSIONS:
                continue
            full_path = os.path.join(dirpath, filename)
            try:
                stat = os.stat(full_path)
            except OSError:
                continue
            relative_path = os.path.relpath(full_path, root)
            container = ext.lstrip(".")
            files.append(
                AlbumQualityEvidenceFile(
                    relative_path=relative_path,
                    size_bytes=int(stat.st_size),
                    mtime_ns=int(stat.st_mtime_ns),
                    extension=container,
                    container=container,
                    codec=container,
                )
            )
    return sorted(files, key=lambda f: f.relative_path)


def neutral_v0_metric_from_probe(
    probe: V0ProbeEvidence | None,
) -> AlbumQualityV0Metric | None:
    """Convert legacy probe evidence into the neutral persisted shape."""

    if probe is None:
        return None
    lineage = _NEUTRAL_V0_LINEAGE.get(probe.kind, "unknown_v0_source")
    return AlbumQualityV0Metric(
        min_bitrate_kbps=probe.min_bitrate_kbps,
        avg_bitrate_kbps=probe.avg_bitrate_kbps,
        median_bitrate_kbps=probe.median_bitrate_kbps,
        source_lineage=lineage,
        source_provenance=probe.kind or None,
        proof_provenance=(
            "lossless-source probe"
            if probe.kind == V0_PROBE_LOSSLESS_SOURCE
            else None
        ),
    )


def verified_lossless_proof_from_import_result(
    import_result: ImportResult,
) -> VerifiedLosslessProof | None:
    measurement = import_result.new_measurement
    if measurement is None or not measurement.verified_lossless:
        return None
    return VerifiedLosslessProof(
        proof_origin="import_result",
        source=(
            measurement.was_converted_from
            or import_result.conversion.original_filetype
            or "lossless_source"
        ),
        classifier="spectral_verified_lossless",
        detail=measurement.spectral_grade,
    )


def legacy_verified_lossless_proof_from_request(
    request_row: dict[str, Any] | None,
) -> VerifiedLosslessProof | None:
    """Seed current proof provenance from the narrow allowed legacy boolean."""

    if not request_row or not request_row.get("verified_lossless"):
        return None
    return VerifiedLosslessProof(
        proof_origin="legacy_request_seed",
        source="album_requests.verified_lossless",
        classifier="legacy_verified_lossless",
        detail="seeded while creating request_current evidence",
    )


def evidence_from_import_result(
    *,
    owner: AlbumQualityEvidenceOwner,
    source_path: str,
    import_result: ImportResult | None,
    measured_at: datetime | None = None,
    target_format: str | None = None,
) -> EvidenceBuildResult:
    """Build candidate evidence from an ``ImportResult`` and source folder."""

    if import_result is None or import_result.new_measurement is None:
        return EvidenceBuildResult(None, "incomplete", "missing new measurement")
    files = snapshot_audio_files(source_path)
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")
    measurement = import_result.new_measurement
    proof = verified_lossless_proof_from_import_result(import_result)
    evidence = AlbumQualityEvidence(
        owner=owner,
        measurement=measurement,
        measured_at=measured_at or datetime.now(timezone.utc),
        files=files,
        codec=files[0].codec,
        container=files[0].container,
        storage_format=measurement.format,
        target_format=target_format,
        v0_metric=neutral_v0_metric_from_probe(import_result.v0_probe),
        verified_lossless_proof=proof,
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def evidence_from_album_info(
    *,
    owner: AlbumQualityEvidenceOwner,
    album_info: Any,
    request_row: dict[str, Any] | None = None,
    measured_at: datetime | None = None,
) -> EvidenceBuildResult:
    """Build current evidence from Beets ``AlbumInfo``-shaped data."""

    album_path = getattr(album_info, "album_path", "")
    files = snapshot_audio_files(str(album_path))
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")
    proof = legacy_verified_lossless_proof_from_request(request_row)
    verified_lossless = proof is not None
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=getattr(album_info, "min_bitrate_kbps", None),
        avg_bitrate_kbps=getattr(album_info, "avg_bitrate_kbps", None),
        median_bitrate_kbps=getattr(album_info, "median_bitrate_kbps", None),
        format=getattr(album_info, "format", None) or None,
        is_cbr=bool(getattr(album_info, "is_cbr", False)),
        verified_lossless=verified_lossless,
    )
    evidence = AlbumQualityEvidence(
        owner=owner,
        measurement=measurement,
        measured_at=measured_at or datetime.now(timezone.utc),
        files=files,
        codec=files[0].codec,
        container=files[0].container,
        storage_format=measurement.format,
        verified_lossless_proof=proof,
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def persist_candidate_evidence_from_import_result(
    db: Any,
    *,
    source_path: str,
    import_result: ImportResult | None,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    target_format: str | None = None,
) -> EvidenceBuildResult:
    owner = candidate_owner(
        download_log_id=download_log_id,
        import_job_id=import_job_id,
    )
    if owner is None:
        return EvidenceBuildResult(None, "unowned", "no persisted candidate owner")
    result = evidence_from_import_result(
        owner=owner,
        source_path=source_path,
        import_result=import_result,
        target_format=target_format,
    )
    if result.evidence is not None:
        db.upsert_album_quality_evidence(result.evidence)
    return result


def backfill_current_evidence_from_album_info(
    db: Any,
    *,
    request_id: int,
    album_info: Any,
) -> EvidenceBuildResult:
    request_row = db.get_request(request_id)
    result = evidence_from_album_info(
        owner=request_current_owner(request_id),
        album_info=album_info,
        request_row=request_row,
    )
    if result.evidence is not None:
        db.upsert_album_quality_evidence(result.evidence)
    return result
