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
    V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
    V0_SOURCE_LINEAGE_NATIVE_LOSSY_RESEARCH,
    V0_SOURCE_LINEAGE_ON_DISK_RESEARCH,
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
    V0_PROBE_LOSSLESS_SOURCE: V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH: V0_SOURCE_LINEAGE_NATIVE_LOSSY_RESEARCH,
    V0_PROBE_ON_DISK_RESEARCH: V0_SOURCE_LINEAGE_ON_DISK_RESEARCH,
}


def _optional_int(value: object | None) -> int | None:
    return value if isinstance(value, int) else None


class SnapshotAudioFilesError(OSError):
    """Raised when a source fileset cannot be snapshotted completely."""


@dataclass(frozen=True)
class EvidenceBuildResult:
    """Result of trying to build evidence from a fileset."""

    evidence: AlbumQualityEvidence | None
    status: str
    reason: str | None = None

    @property
    def available(self) -> bool:
        return self.evidence is not None


def _candidate_owners(
    *,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
) -> list[AlbumQualityEvidenceOwner]:
    """Return persisted candidate owners for an import candidate."""

    owners: list[AlbumQualityEvidenceOwner] = []
    if import_job_id is not None:
        owners.append(AlbumQualityEvidenceOwner(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
            owner_id=import_job_id,
        ))
    if download_log_id is not None:
        owners.append(AlbumQualityEvidenceOwner(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            owner_id=download_log_id,
        ))
    return owners


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
    walk_errors: list[str] = []

    def onerror(exc: OSError) -> None:
        walk_errors.append(str(exc))

    for dirpath, _dirnames, filenames in os.walk(root, onerror=onerror):
        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in _AUDIO_EXTENSIONS:
                continue
            full_path = os.path.join(dirpath, filename)
            try:
                stat = os.stat(full_path)
            except OSError as exc:
                raise SnapshotAudioFilesError(
                    f"could not stat audio file {full_path}: {exc}"
                ) from exc
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
    if walk_errors:
        raise SnapshotAudioFilesError("; ".join(walk_errors))
    return sorted(files, key=lambda f: f.relative_path)


def audio_snapshot_matches(
    root: str,
    files: list[AlbumQualityEvidenceFile],
) -> bool:
    """Return whether ``root`` still has the recorded active audio snapshot."""

    try:
        current = snapshot_audio_files(root)
    except OSError:
        return False
    expected = sorted(files, key=lambda f: f.relative_path)
    return current == expected


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


def lossless_source_v0_probe_from_metric(
    metric: AlbumQualityV0Metric | None,
) -> V0ProbeEvidence | None:
    if (
        metric is None
        or metric.source_lineage != V0_SOURCE_LINEAGE_LOSSLESS_SOURCE
        or metric.avg_bitrate_kbps is None
    ):
        return None
    return V0ProbeEvidence(
        kind=V0_PROBE_LOSSLESS_SOURCE,
        min_bitrate_kbps=metric.min_bitrate_kbps,
        avg_bitrate_kbps=metric.avg_bitrate_kbps,
        median_bitrate_kbps=metric.median_bitrate_kbps,
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


def legacy_current_v0_metric_from_request(
    request_row: dict[str, Any] | None,
) -> AlbumQualityV0Metric | None:
    """Seed neutral current V0 evidence from request-row source metrics."""

    if not request_row:
        return None

    min_bitrate = _optional_int(
        request_row.get("current_lossless_source_v0_probe_min_bitrate")
    )
    avg_bitrate = _optional_int(
        request_row.get("current_lossless_source_v0_probe_avg_bitrate")
    )
    median_bitrate = _optional_int(
        request_row.get("current_lossless_source_v0_probe_median_bitrate")
    )
    if min_bitrate is None and avg_bitrate is None and median_bitrate is None:
        return None
    return AlbumQualityV0Metric(
        min_bitrate_kbps=min_bitrate,
        avg_bitrate_kbps=avg_bitrate,
        median_bitrate_kbps=median_bitrate,
        source_lineage=V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
        source_provenance="album_requests.current_lossless_source_v0_probe",
        proof_provenance="legacy_current_v0_probe_seed",
    )


def legacy_current_lossless_v0_probe_from_request(
    request_row: dict[str, Any] | None,
) -> V0ProbeEvidence | None:
    """Build comparable source-probe evidence from legacy request columns."""

    if not request_row:
        return None
    avg_bitrate = _optional_int(
        request_row.get("current_lossless_source_v0_probe_avg_bitrate")
    )
    if avg_bitrate is None:
        return None
    return V0ProbeEvidence(
        kind=V0_PROBE_LOSSLESS_SOURCE,
        min_bitrate_kbps=_optional_int(
            request_row.get("current_lossless_source_v0_probe_min_bitrate")
        ),
        avg_bitrate_kbps=avg_bitrate,
        median_bitrate_kbps=_optional_int(
            request_row.get("current_lossless_source_v0_probe_median_bitrate")
        ),
    )


def evidence_from_import_result(
    *,
    owner: AlbumQualityEvidenceOwner,
    source_path: str,
    import_result: ImportResult | None,
    measured_at: datetime | None = None,
    target_format: str | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
) -> EvidenceBuildResult:
    """Build candidate evidence from an ``ImportResult`` and source folder."""

    if import_result is None or import_result.new_measurement is None:
        return EvidenceBuildResult(None, "incomplete", "missing new measurement")
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
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
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    measured_at: datetime | None = None,
) -> EvidenceBuildResult:
    """Build current evidence from Beets ``AlbumInfo``-shaped data."""

    album_path = getattr(album_info, "album_path", "")
    try:
        files = snapshot_audio_files(str(album_path))
    except OSError as exc:
        return EvidenceBuildResult(None, "failed", str(exc))
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")
    proof = verified_lossless_proof or legacy_verified_lossless_proof_from_request(
        request_row
    )
    verified_lossless = proof is not None
    spectral_grade = None
    spectral_bitrate = None
    if request_row:
        grade_raw = request_row.get("current_spectral_grade")
        bitrate_raw = request_row.get("current_spectral_bitrate")
        spectral_grade = grade_raw if isinstance(grade_raw, str) else None
        spectral_bitrate = bitrate_raw if isinstance(bitrate_raw, int) else None
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=getattr(album_info, "min_bitrate_kbps", None),
        avg_bitrate_kbps=getattr(album_info, "avg_bitrate_kbps", None),
        median_bitrate_kbps=getattr(album_info, "median_bitrate_kbps", None),
        format=getattr(album_info, "format", None) or None,
        is_cbr=bool(getattr(album_info, "is_cbr", False)),
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=spectral_bitrate,
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
        v0_metric=legacy_current_v0_metric_from_request(request_row),
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
    files: list[AlbumQualityEvidenceFile] | None = None,
) -> EvidenceBuildResult:
    owners = _candidate_owners(
        download_log_id=download_log_id,
        import_job_id=import_job_id,
    )
    if not owners:
        return EvidenceBuildResult(None, "unowned", "no persisted candidate owner")
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    result = evidence_from_import_result(
        owner=owners[0],
        source_path=source_path,
        import_result=import_result,
        target_format=target_format,
        files=files,
    )
    if result.evidence is not None:
        for owner in owners:
            db.upsert_album_quality_evidence(AlbumQualityEvidence(
                owner=owner,
                measurement=result.evidence.measurement,
                measured_at=result.evidence.measured_at,
                files=result.evidence.files,
                codec=result.evidence.codec,
                container=result.evidence.container,
                storage_format=result.evidence.storage_format,
                target_format=result.evidence.target_format,
                v0_metric=result.evidence.v0_metric,
                verified_lossless_proof=result.evidence.verified_lossless_proof,
            ))
    return result


def backfill_current_evidence_from_album_info(
    db: Any,
    *,
    request_id: int,
    album_info: Any,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    preserve_existing_verified_lossless_proof: bool = True,
) -> EvidenceBuildResult:
    request_row = db.get_request(request_id)
    if verified_lossless_proof is None and preserve_existing_verified_lossless_proof:
        existing = db.load_album_quality_evidence(request_current_owner(request_id))
        if (
            existing is not None
            and existing.measurement.verified_lossless
            and existing.verified_lossless_proof is not None
        ):
            verified_lossless_proof = existing.verified_lossless_proof
    result = evidence_from_album_info(
        owner=request_current_owner(request_id),
        album_info=album_info,
        request_row=request_row,
        verified_lossless_proof=verified_lossless_proof,
    )
    if result.evidence is not None:
        db.upsert_album_quality_evidence(result.evidence)
    return result


def load_candidate_evidence_for_source(
    db: Any,
    *,
    source_path: str,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
) -> EvidenceBuildResult:
    """Load stored candidate evidence and require source snapshot freshness."""

    owners = _candidate_owners(
        download_log_id=download_log_id,
        import_job_id=import_job_id,
    )
    if not owners:
        return EvidenceBuildResult(None, "unowned", "no candidate owner")

    missing: list[str] = []
    for owner in owners:
        evidence = db.load_album_quality_evidence(owner)
        if evidence is None:
            missing.append(f"{owner.owner_type}:{owner.owner_id}")
            continue
        if not audio_snapshot_matches(source_path, evidence.files):
            return EvidenceBuildResult(
                None,
                "stale",
                f"candidate source changed since evidence capture: "
                f"{owner.owner_type}:{owner.owner_id}",
            )
        errors = evidence.policy_incomplete_reasons()
        if errors:
            return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
        return EvidenceBuildResult(evidence, "ready")

    return EvidenceBuildResult(
        None,
        "missing",
        "no candidate evidence found for " + ", ".join(missing),
    )


def load_or_backfill_current_evidence(
    db: Any,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: Any = None,
    preloaded_evidence: AlbumQualityEvidence | None = None,
    preloaded: bool = False,
) -> EvidenceBuildResult:
    """Load current Beets evidence, backfilling when absent or incomplete."""

    from lib.beets_db import BeetsDB
    from lib.quality import QualityRankConfig

    owner = request_current_owner(request_id)
    existing = (
        preloaded_evidence
        if preloaded
        else db.load_album_quality_evidence(owner)
    )
    if existing is not None:
        errors = existing.policy_incomplete_reasons()
        if not errors:
            return EvidenceBuildResult(existing, "ready")

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    with BeetsDB() as beets:
        album_info = beets.get_album_info(mb_release_id, cfg)
    if album_info is None:
        return EvidenceBuildResult(None, "empty_current", "album not in beets")

    return backfill_current_evidence_from_album_info(
        db,
        request_id=request_id,
        album_info=album_info,
    )
