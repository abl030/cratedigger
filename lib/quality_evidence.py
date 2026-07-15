"""Album-quality evidence construction and persistence helpers."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from lib.quality import (
    LOSSLESS_CODECS,
    V0_PROBE_LOSSLESS_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH,
    V0_PROBE_ON_DISK_RESEARCH,
    V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
    V0_SOURCE_LINEAGE_NATIVE_LOSSY_RESEARCH,
    V0_SOURCE_LINEAGE_ON_DISK_RESEARCH,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    ImportResult,
    V0ProbeEvidence,
    VerifiedLosslessProof,
)

if TYPE_CHECKING:
    from lib.measurement import PreimportMeasurement


@runtime_checkable
class QualityEvidenceDB(Protocol):
    """The PipelineDB surface the evidence persist/load helpers use (#409).

    Shared by ``lib/import_evidence.py`` (which forwards its handle into
    these loaders) and extended by ``WrongMatchCleanupDB`` for the same
    reason. Parity tests live in ``tests/test_quality_evidence.py``.
    """

    def get_request(self, request_id: int) -> dict[str, Any] | None: ...

    def upsert_album_quality_evidence(
        self, evidence: AlbumQualityEvidence,
    ) -> None: ...

    def find_album_quality_evidence(
        self,
        *,
        mb_release_id: str,
        snapshot_fingerprint: str,
    ) -> AlbumQualityEvidence | None: ...

    def load_album_quality_evidence_by_id(
        self, evidence_id: int | None,
    ) -> AlbumQualityEvidence | None: ...

    def set_import_job_candidate_evidence(
        self, import_job_id: int, evidence_id: int | None,
    ) -> None: ...

    def set_download_log_candidate_evidence(
        self, download_log_id: int, evidence_id: int | None,
    ) -> None: ...

    def set_request_current_evidence(
        self,
        request_id: int,
        evidence_id: int | None,
        *,
        expected_status: str | None = None,
    ) -> bool: ...

    def get_import_job_candidate_evidence_id(
        self, import_job_id: int,
    ) -> int | None: ...

    def get_download_log_candidate_evidence_id(
        self, download_log_id: int,
    ) -> int | None: ...

    def get_request_current_evidence_id(
        self, request_id: int,
    ) -> int | None: ...


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


_LOSSLESS_CONTAINERS = {"flac", "alac", "wav", "aiff", "ape"}
_LOSSY_CONTAINERS = {"mp3", "aac", "m4a", "ogg", "opus", "wma"}


def derive_folder_layout(files: list[AlbumQualityEvidenceFile]) -> str:
    """Return 'nested' if any snapshot file lives in a subdirectory.

    Pure helper used by U1's evidence-construction sites. ``relative_path``
    is always a relative POSIX-shaped path; a forward slash anywhere in it
    indicates a multi-disc / nested layout that the decision function
    rejects in U6.
    """

    for file in files:
        if "/" in file.relative_path:
            return "nested"
    return "flat"


def derive_filetype_band(files: list[AlbumQualityEvidenceFile]) -> str:
    """Classify a snapshot fileset into a coarse filetype band.

    Returns one of ``""`` (empty fileset), ``"flac"``, ``"mp3"``,
    ``"mixed_lossless"``, ``"mixed_lossy"``, or ``"mixed"`` (lossy + lossless
    combined). Container is the discriminator — codec is too noisy.
    """

    if not files:
        return ""
    containers = {file.container.lower() for file in files if file.container}
    if not containers:
        return ""
    if len(containers) == 1:
        return next(iter(containers))
    lossless_hits = containers & _LOSSLESS_CONTAINERS
    lossy_hits = containers & _LOSSY_CONTAINERS
    if lossless_hits and lossy_hits:
        return "mixed"
    if lossless_hits:
        return "mixed_lossless"
    if lossy_hits:
        return "mixed_lossy"
    return "mixed"


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


def snapshot_fingerprint(files: list[AlbumQualityEvidenceFile]) -> str:
    """SHA-256 fingerprint of an audio inventory used as the evidence row key.

    This is the canonical addressing key for ``album_quality_evidence`` after
    the rekey landed in plan ``2026-05-16-002`` (U1/U2/U3). The exact formula
    is load-bearing: U2's SQL migration computes the same hash from each
    row's ``album_quality_evidence_files`` records, so a Python-vs-SQL drift
    here would scramble post-deploy lookup and break dedupe.

    Formula (must be mirrored exactly by U2's migration):

    1. For each file, build a tuple ``[relative_path, size_bytes, extension,
       container, codec]`` as a JSON array. ``codec`` may be ``None`` and is
       rendered as JSON ``null``.
    2. Sort the per-file tuples by ``relative_path`` ascending.
    3. JSON-encode the sorted list with ``sort_keys=False``,
       ``separators=(",", ":")`` (no whitespace), ``ensure_ascii=False``.
       Each file becomes e.g. ``["track01.flac",12345,"flac","flac","flac"]``.
    4. SHA-256 hex digest of the UTF-8 bytes of that JSON string.

    Fields chosen mirror ``_snapshot_match_key`` so freshness and identity
    stay coherent. ``mtime_ns`` is deliberately excluded — see the
    ``_snapshot_match_key`` docstring for why (ID3 tag mutation, virtiofs
    flake). ``decode_ok`` is excluded too: it is per-file evidence written
    by the measurement gate, not an identity attribute.

    The empty list hashes the JSON encoding of ``[]`` (``"[]"`` → a stable,
    defined 64-char digest), not an error.
    """

    payload: list[list[Any]] = sorted(
        (
            [
                file.relative_path,
                file.size_bytes,
                file.extension,
                file.container,
                file.codec,
            ]
            for file in files
        ),
        key=lambda row: row[0],
    )
    encoded = json.dumps(
        payload,
        sort_keys=False,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _snapshot_match_key(
    file: AlbumQualityEvidenceFile,
) -> tuple[str, int, str, str, str | None]:
    """Stable identity tuple for snapshot equality.

    Excludes ``mtime_ns`` because virtiofs has been observed to return
    slightly different ``st_mtime_ns`` between back-to-back ``stat``
    calls on the same file. Size + path + extension/container/codec is
    sufficient to detect any content change that matters here.
    ``mtime_ns`` stays in the persisted struct as a forensic field but
    does not gate freshness.
    """
    return (
        file.relative_path,
        file.size_bytes,
        file.extension,
        file.container,
        file.codec,
    )


def audio_snapshot_matches(
    root: str,
    files: list[AlbumQualityEvidenceFile],
) -> bool:
    """Return whether ``root`` still has the recorded active audio snapshot.

    Compares on stable identity (path/size/codec) only. See
    :func:`_snapshot_match_key` for why ``mtime_ns`` is excluded.
    """

    try:
        current = snapshot_audio_files(root)
    except OSError:
        return False
    expected = sorted(files, key=lambda f: f.relative_path)
    return [_snapshot_match_key(f) for f in current] == [
        _snapshot_match_key(f) for f in expected
    ]


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


def audit_v0_probe_from_metric(
    metric: AlbumQualityV0Metric | None,
) -> V0ProbeEvidence | None:
    """Build ``V0ProbeEvidence`` from a persisted v0_metric for audit/log use.

    Returns a probe for *any* metric with the exact persisted audit kind for
    its source lineage. Policy code that needs a comparable probe must keep
    filtering via :func:`is_comparable_lossless_source_probe` — this helper
    exists so audit/UI surfaces can read a probe from *every* download,
    including native-lossy and on-disk research probes.
    """

    if metric is None:
        return None
    # download_log.v0_probe_kind CHECK constraint (migration 007) only
    # accepts the three persisted audit kinds. ``neutral_v0_research`` is an
    # in-memory policy marker and must never be written to the DB.
    kind_by_lineage = {
        V0_SOURCE_LINEAGE_LOSSLESS_SOURCE: V0_PROBE_LOSSLESS_SOURCE,
        V0_SOURCE_LINEAGE_NATIVE_LOSSY_RESEARCH:
            V0_PROBE_NATIVE_LOSSY_RESEARCH,
        V0_SOURCE_LINEAGE_ON_DISK_RESEARCH: V0_PROBE_ON_DISK_RESEARCH,
    }
    kind = kind_by_lineage.get(
        metric.source_lineage or "",
        V0_PROBE_NATIVE_LOSSY_RESEARCH,
    )
    return V0ProbeEvidence(
        kind=kind,
        min_bitrate_kbps=metric.min_bitrate_kbps,
        avg_bitrate_kbps=metric.avg_bitrate_kbps,
        median_bitrate_kbps=metric.median_bitrate_kbps,
    )


def verified_lossless_proof_from_import_result(
    import_result: ImportResult,
) -> VerifiedLosslessProof | None:
    measurement = import_result.source_measurement
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


def _apply_measurement_facts_to_files(
    files: list[AlbumQualityEvidenceFile],
    measurement: "PreimportMeasurement",
) -> list[AlbumQualityEvidenceFile]:
    """Stamp ``decode_ok=False`` on snapshot files listed in measurement.corrupt_files.

    ``snapshot_audio_files`` defaults ``decode_ok=True`` because the snapshot
    helper does not run ffmpeg. The preimport measurement is the authority on
    audio integrity, so when it reports corrupt files we propagate that fact
    into the snapshot rows before persisting evidence. This lets the
    importer's ``full_pipeline_decision_from_evidence`` (U11) consume
    ``decode_ok=False`` flags as the per-file evidence for ``audio_corrupt``.
    """
    if not measurement.corrupt_files:
        return files
    corrupt_set = {os.path.basename(name) for name in measurement.corrupt_files}
    # Also accept full relative paths if measurement reported them that way.
    corrupt_set.update(measurement.corrupt_files)
    out: list[AlbumQualityEvidenceFile] = []
    for f in files:
        if (
            f.relative_path in corrupt_set
            or os.path.basename(f.relative_path) in corrupt_set
        ):
            out.append(AlbumQualityEvidenceFile(
                relative_path=f.relative_path,
                size_bytes=f.size_bytes,
                mtime_ns=f.mtime_ns,
                extension=f.extension,
                container=f.container,
                codec=f.codec,
                decode_ok=False,
            ))
        else:
            out.append(f)
    return out


def _filetype_band_to_format(filetype_band: str) -> str | None:
    """Derive an ``AudioQualityMeasurement.format`` label from a filetype band.

    Used for the measurement-only evidence path (audio_corrupt / bad_hash /
    nested / empty), where the harness never ran and there is no measured
    format string. The result must be specific enough that the importer's
    ``policy_incomplete_reasons`` check passes (``measurement.format`` must not
    be None). For mixed filetypes we pick the dominant lossless/lossy container.
    """
    band = (filetype_band or "").strip().lower()
    if not band:
        return None
    if band in ("flac", "alac", "wav", "aiff", "ape"):
        return band.upper()
    if band in ("mp3", "aac", "m4a", "ogg", "opus", "wma"):
        return band.upper()
    if band == "mixed_lossless":
        return "FLAC"
    if band == "mixed_lossy":
        return "MP3"
    if band == "mixed":
        return "MP3"
    # Comma-separated extensions from inspect_local_files (e.g. "mp3, flac")
    first = band.split(",")[0].strip()
    if first:
        return first.upper()
    return None


def evidence_from_import_result(
    *,
    mb_release_id: str,
    source_path: str,
    import_result: ImportResult | None,
    measured_at: datetime | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
    measurement: "PreimportMeasurement | None" = None,
) -> EvidenceBuildResult:
    """Build candidate evidence from an ``ImportResult`` and source folder.

    When ``measurement`` (a ``PreimportMeasurement``) is supplied, its U1
    facts (``audio_corrupt``, ``folder_layout``, ``audio_file_count``,
    ``filetype_band``, ``matched_bad_audio_hash_*``) override the values
    derived from the snapshot files. The measurement is the authority for
    these facts because it ran the real gates (ffmpeg decode, mp3val,
    bad-hash lookup) — the snapshot helper only knows file sizes and paths.
    """

    if import_result is None or import_result.source_measurement is None:
        return EvidenceBuildResult(None, "incomplete", "missing source measurement")
    try:
        import_result.validate_new_row()
    except ValueError as exc:
        return EvidenceBuildResult(None, "incomplete", str(exc))
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")
    if measurement is not None and measurement.audio_corrupt:
        files = _apply_measurement_facts_to_files(files, measurement)
    audio_measurement = import_result.source_measurement
    target_contract = import_result.target_quality_contract
    # V3 target policy is owned by the harness result. The request row often
    # has no explicit target because the configured verified-lossless target
    # supplies it; trusting the request here loses the contract end-to-end.
    target_format = (
        target_contract.format if target_contract is not None else None
    )
    target_is_cbr = (
        target_contract.is_cbr if target_contract is not None else None
    )
    proof = verified_lossless_proof_from_import_result(import_result)
    audio_corrupt = any(not file.decode_ok for file in files)
    if measurement is not None:
        audio_corrupt = audio_corrupt or measurement.audio_corrupt
        folder_layout = measurement.folder_layout
        audio_file_count = (
            measurement.audio_file_count
            if measurement.audio_file_count else len(files)
        )
        filetype_band = (
            measurement.filetype_band or derive_filetype_band(files)
        )
        matched_bad_hash_id = measurement.matched_bad_hash_id
        matched_bad_hash_path = measurement.matched_bad_track_path
    else:
        folder_layout = derive_folder_layout(files)
        audio_file_count = len(files)
        filetype_band = derive_filetype_band(files)
        matched_bad_hash_id = None
        matched_bad_hash_path = None
    evidence = AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_path,
        measurement=audio_measurement,
        measured_at=measured_at or datetime.now(timezone.utc),
        files=files,
        codec=files[0].codec,
        container=files[0].container,
        storage_format=audio_measurement.format,
        target_format=target_format,
        target_is_cbr=target_is_cbr,
        lineage_version=3,
        v0_metric=(
            neutral_v0_metric_from_probe(import_result.v0_probe)
        ),
        verified_lossless_proof=proof,
        audio_corrupt=audio_corrupt,
        folder_layout=folder_layout,
        audio_file_count=audio_file_count,
        filetype_band=filetype_band,
        matched_bad_audio_hash_id=matched_bad_hash_id,
        matched_bad_audio_hash_path=matched_bad_hash_path,
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def evidence_from_measurement(
    *,
    mb_release_id: str,
    source_path: str,
    measurement: "PreimportMeasurement",
    measured_at: datetime | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
) -> EvidenceBuildResult:
    """Build candidate evidence purely from a ``PreimportMeasurement``.

    Used by the preview worker when the harness cannot or should not run
    (audio_corrupt, bad_audio_hash, nested_layout, empty_fileset). The
    measurement carries every U1 fact the importer's
    ``full_pipeline_decision_from_evidence`` (U11) needs to reject:
    ``audio_corrupt``, ``matched_bad_audio_hash_*``, ``folder_layout``,
    ``audio_file_count``, and the spectral measurements.

    The synthesized ``AudioQualityMeasurement`` only carries enough data to
    satisfy ``AlbumQualityEvidence.policy_incomplete_reasons`` (format + at
    least one bitrate metric). The importer rejects on the U1 facts upstream
    of the quality gate, so the synthesized measurement never drives an
    accept decision.

    When ``audio_file_count=0`` and ``files`` is empty, returns ``empty_fileset``
    evidence — ``AlbumQualityEvidence.storage_validation_errors`` accepts this
    case (the explicit empty-inventory signal).
    """

    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    files = _apply_measurement_facts_to_files(files, measurement)
    audio_file_count = (
        measurement.audio_file_count
        if measurement.audio_file_count else len(files)
    )
    # Synthesize a minimal AudioQualityMeasurement. The importer rejects on
    # the U1 facts (audio_corrupt, nested, etc.) before reading these,
    # but ``policy_incomplete_reasons`` requires format + a bitrate metric.
    filetype_band = measurement.filetype_band or derive_filetype_band(files)
    format_label = _filetype_band_to_format(filetype_band) or "MP3"
    min_bitrate_kbps = measurement.min_bitrate_kbps
    if min_bitrate_kbps is None:
        # Fall back to a placeholder so policy_incomplete_reasons passes.
        # The actual value never drives a decision: the importer rejects on
        # audio_corrupt/nested/empty/bad_hash/spectral_reject before reading
        # min_bitrate_kbps.
        min_bitrate_kbps = 0
    download_spectral = measurement.download_spectral
    audio_measurement = AudioQualityMeasurement(
        min_bitrate_kbps=min_bitrate_kbps,
        avg_bitrate_kbps=min_bitrate_kbps,
        median_bitrate_kbps=min_bitrate_kbps,
        format=format_label,
        is_cbr=measurement.is_vbr is False,
        spectral_grade=(
            download_spectral.grade if download_spectral is not None else None
        ),
        spectral_bitrate_kbps=(
            download_spectral.bitrate_kbps if download_spectral is not None else None
        ),
    )
    codec = files[0].codec if files else None
    container = files[0].container if files else None
    evidence = AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_path,
        measurement=audio_measurement,
        measured_at=measured_at or datetime.now(timezone.utc),
        files=files,
        codec=codec,
        container=container,
        storage_format=audio_measurement.format,
        # This path exists only for facts rejected before target policy is
        # consulted. It has no projected files, so both target fields stay
        # absent instead of fabricating a bitrate mode.
        target_format=None,
        target_is_cbr=None,
        lineage_version=3,
        v0_metric=None,
        verified_lossless_proof=None,
        audio_corrupt=measurement.audio_corrupt,
        folder_layout=measurement.folder_layout,
        audio_file_count=audio_file_count,
        filetype_band=filetype_band,
        matched_bad_audio_hash_id=measurement.matched_bad_hash_id,
        matched_bad_audio_hash_path=measurement.matched_bad_track_path,
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def evidence_from_album_info(
    *,
    mb_release_id: str,
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
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=str(album_path) or "",
        measurement=measurement,
        measured_at=measured_at or datetime.now(timezone.utc),
        files=files,
        codec=files[0].codec,
        container=files[0].container,
        storage_format=measurement.format,
        lineage_version=3,
        v0_metric=legacy_current_v0_metric_from_request(request_row),
        verified_lossless_proof=proof,
        audio_corrupt=any(not file.decode_ok for file in files),
        folder_layout=derive_folder_layout(files),
        audio_file_count=len(files),
        filetype_band=derive_filetype_band(files),
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def persist_candidate_evidence_from_import_result(
    db: QualityEvidenceDB,
    *,
    mb_release_id: str,
    source_path: str,
    import_result: ImportResult | None,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
    measurement: "PreimportMeasurement | None" = None,
) -> EvidenceBuildResult:
    """Persist content-addressed candidate evidence and write addressing FKs.

    After upsert (keyed by ``(mb_release_id, snapshot_fingerprint)``), writes
    the surviving evidence row's id back to ``import_jobs.candidate_evidence_id``
    and/or ``download_log.candidate_evidence_id`` so triage and importer can
    look up evidence via FK chain.
    """
    if download_log_id is None and import_job_id is None:
        return EvidenceBuildResult(None, "unowned", "no persisted candidate owner")
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    result = evidence_from_import_result(
        mb_release_id=mb_release_id,
        source_path=source_path,
        import_result=import_result,
        files=files,
        measurement=measurement,
    )
    if result.evidence is not None:
        db.upsert_album_quality_evidence(result.evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=result.evidence.mb_release_id,
            snapshot_fingerprint=result.evidence.snapshot_fingerprint,
        )
        if persisted is not None and persisted.id is not None:
            if import_job_id is not None:
                db.set_import_job_candidate_evidence(import_job_id, persisted.id)
            if download_log_id is not None:
                db.set_download_log_candidate_evidence(
                    download_log_id, persisted.id
                )
    return result


def persist_candidate_evidence_from_measurement(
    db: QualityEvidenceDB,
    *,
    mb_release_id: str,
    source_path: str,
    measurement: "PreimportMeasurement",
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
) -> EvidenceBuildResult:
    """Persist measurement-only candidate evidence (no ImportResult required).

    Mirrors ``persist_candidate_evidence_from_import_result`` for the preview
    code path that never invoked the harness (audio_corrupt / bad_audio_hash /
    nested_layout / empty_fileset). The importer's
    ``full_pipeline_decision_from_evidence`` (U11) reads the persisted U1
    facts and rejects via its four-fact early-exit branches upstream of the
    quality gate.
    """
    if download_log_id is None and import_job_id is None:
        return EvidenceBuildResult(None, "unowned", "no persisted candidate owner")
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    result = evidence_from_measurement(
        mb_release_id=mb_release_id,
        source_path=source_path,
        measurement=measurement,
        files=files,
    )
    if result.evidence is not None:
        db.upsert_album_quality_evidence(result.evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=result.evidence.mb_release_id,
            snapshot_fingerprint=result.evidence.snapshot_fingerprint,
        )
        if persisted is not None and persisted.id is not None:
            if import_job_id is not None:
                db.set_import_job_candidate_evidence(import_job_id, persisted.id)
            if download_log_id is not None:
                db.set_download_log_candidate_evidence(
                    download_log_id, persisted.id
                )
    return result


def propagate_candidate_evidence_to_current(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    candidate_evidence: AlbumQualityEvidence,
    album_info: Any,
    measured_at: datetime | None = None,
) -> EvidenceBuildResult:
    """Build new library-side evidence by propagating candidate measurement payload.

    Post-import propagation path (U10). The candidate evidence row that the
    importer worked on already paid for expensive measurements (spectral
    analysis, V0 lineage, bad-audio-hash matches, verified-lossless proof).
    After the file rename or transcode to the library path, this helper
    builds the new library-side evidence row without re-measuring — see
    ``CLAUDE.md`` § "Decision architecture" and the U10 design doc.

    Field policy:

    * Always re-derived from the library snapshot: ``snapshot_fingerprint``,
      ``source_path``, ``files``, ``codec``, ``container``, ``storage_format``,
      ``folder_layout``, ``audio_file_count``, ``filetype_band``,
      ``audio_corrupt`` (from files[*].decode_ok), ``measured_at`` (now).
    * Always re-derived from ``album_info``: ``min_bitrate_kbps``,
      ``avg_bitrate_kbps``, ``median_bitrate_kbps``, ``format``, ``is_cbr``.
      Beets's per-track bitrate measurements describe the on-disk files at
      the library path — for renamed-only this is the same audio as the
      candidate's measurement (a dual-check that catches drift); for
      transcoded imports this describes the V0/Opus output.
    * **Propagated when renamed-only OR when the source is a lossless
      codec (FLAC / ALAC / WAV); stripped on non-lossless transcoded
      imports:** ``spectral_grade``, ``spectral_bitrate_kbps`` (on the
      inner measurement); ``v0_metric``, ``matched_bad_audio_hash_id``,
      ``matched_bad_audio_hash_path`` (on the outer evidence row). The
      gate is lossless-source-only because the source-side spectral / V0
      lineage is only meaningfully comparable against future candidates
      when the source audio was lossless to begin with. For non-lossless
      transcoded imports (MP3 → Opus etc.) the source-side fields
      describe lossy audio that has no comparable role in subsequent
      candidate comparisons; storing them on the library row provides
      no decision value and would mislead future triage.
    * Propagated in ALL cases: ``verified_lossless`` and
      ``verified_lossless_proof``. Output ``was_converted_from`` is derived
      from the candidate source codec for v3 evidence, with the historical
      measurement field retained only as a legacy fallback. Verified lineage
      survives transcode by definition; a V0 transcoded from a verified-
      lossless FLAC is still verified-lossless by lineage.
    """

    album_path = getattr(album_info, "album_path", "")
    try:
        files = snapshot_audio_files(str(album_path))
    except OSError as exc:
        return EvidenceBuildResult(None, "failed", str(exc))
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")

    # Lossless-source gate: source-side fields propagate when the source
    # codec is lossless (FLAC/ALAC/WAV) OR when the import is renamed-only
    # (same codec in / out). Strip otherwise. See `docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`.
    source_codec = (candidate_evidence.codec or "").lower() or None
    library_codec_from_files = files[0].codec
    library_codec = (library_codec_from_files or "").lower() or None
    is_transcode = (
        source_codec is not None
        and library_codec is not None
        and source_codec != library_codec
    )
    # ``codec`` is labelled by container extension (snapshot_audio_files), so
    # ``.m4a`` records ``"m4a"`` for BOTH lossless ALAC and lossy AAC — the
    # container string alone cannot prove the source was lossless. The
    # authoritative signal is the candidate's V0 probe lineage: the harness
    # only grinds a ``lossless_source`` probe after ffprobe confirms a
    # lossless container (ALAC/FLAC/WAV). Trust that lineage so an
    # ALAC-in-m4a anchor survives the transcode instead of being stripped as
    # if it were AAC — otherwise the next candidate sees no comparable probe
    # and imports for free, ratcheting the V0 anchor downward (request 5219,
    # Fred again.. *Actual Life 3*: 253 ALAC anchor wiped, 239 FLAC imported).
    source_v0 = candidate_evidence.v0_metric
    source_has_lossless_v0_lineage = (
        source_v0 is not None
        and source_v0.source_lineage == V0_SOURCE_LINEAGE_LOSSLESS_SOURCE
    )
    source_is_lossless = (
        (source_codec in LOSSLESS_CODECS if source_codec else False)
        or source_has_lossless_v0_lineage
    )
    strip_source_fields = is_transcode and not source_is_lossless

    candidate_measurement = candidate_evidence.measurement
    measured_source_format = (
        candidate_measurement.format or source_codec or ""
    ).strip().lower()
    output_source_format = (
        (measured_source_format if is_transcode else None)
        if candidate_evidence.lineage_version == 3
        else (
            candidate_measurement.was_converted_from
            or (measured_source_format if is_transcode else None)
        )
    )
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=getattr(album_info, "min_bitrate_kbps", None),
        avg_bitrate_kbps=getattr(album_info, "avg_bitrate_kbps", None),
        median_bitrate_kbps=getattr(album_info, "median_bitrate_kbps", None),
        format=getattr(album_info, "format", None) or None,
        is_cbr=bool(getattr(album_info, "is_cbr", False)),
        spectral_grade=(
            None if strip_source_fields else candidate_measurement.spectral_grade
        ),
        spectral_bitrate_kbps=(
            None if strip_source_fields
            else candidate_measurement.spectral_bitrate_kbps
        ),
        verified_lossless=candidate_measurement.verified_lossless,
        was_converted_from=output_source_format,
    )

    library_filetype_band = derive_filetype_band(files)
    library_container_from_files = files[0].container

    evidence = AlbumQualityEvidence(
        mb_release_id=candidate_evidence.mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=str(album_path) or "",
        measurement=measurement,
        measured_at=measured_at or datetime.now(timezone.utc),
        files=files,
        codec=library_codec_from_files,
        container=library_container_from_files,
        storage_format=measurement.format,
        target_format=None,
        lineage_version=3,
        v0_metric=None if strip_source_fields else candidate_evidence.v0_metric,
        verified_lossless_proof=candidate_evidence.verified_lossless_proof,
        audio_corrupt=any(not file.decode_ok for file in files),
        folder_layout=derive_folder_layout(files),
        audio_file_count=len(files),
        filetype_band=library_filetype_band,
        matched_bad_audio_hash_id=(
            None if strip_source_fields
            else candidate_evidence.matched_bad_audio_hash_id
        ),
        matched_bad_audio_hash_path=(
            None if strip_source_fields
            else candidate_evidence.matched_bad_audio_hash_path
        ),
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))

    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    if persisted is not None and persisted.id is not None:
        request_row = db.get_request(request_id)
        if request_row is None:
            return EvidenceBuildResult(
                evidence,
                "stale_request",
                "request disappeared before current evidence link",
            )
        expected_status = str(request_row["status"])
        if expected_status == "replaced" or not db.set_request_current_evidence(
            request_id,
            persisted.id,
            expected_status=expected_status,
        ):
            return EvidenceBuildResult(
                evidence,
                "stale_request",
                "request state changed before current evidence link",
            )
    return EvidenceBuildResult(evidence, "ready")


def backfill_current_evidence_from_album_info(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    album_info: Any,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    preserve_existing_verified_lossless_proof: bool = True,
) -> EvidenceBuildResult:
    """Build current evidence from beets, upsert, and write request FK.

    Identity is ``(mb_release_id, snapshot_fingerprint)``. Once persisted the
    surviving row id is written to ``album_requests.current_evidence_id`` so
    downstream readers can fetch via FK rather than scanning by mbid.
    """
    request_row = db.get_request(request_id)
    if verified_lossless_proof is None and preserve_existing_verified_lossless_proof:
        existing_id = db.get_request_current_evidence_id(request_id)
        existing = (
            db.load_album_quality_evidence_by_id(existing_id)
            if existing_id is not None
            else None
        )
        if (
            existing is not None
            and existing.measurement.verified_lossless
            and existing.verified_lossless_proof is not None
        ):
            verified_lossless_proof = existing.verified_lossless_proof
    result = evidence_from_album_info(
        mb_release_id=mb_release_id,
        album_info=album_info,
        request_row=request_row,
        verified_lossless_proof=verified_lossless_proof,
    )
    if result.evidence is not None:
        db.upsert_album_quality_evidence(result.evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=result.evidence.mb_release_id,
            snapshot_fingerprint=result.evidence.snapshot_fingerprint,
        )
        if persisted is not None and persisted.id is not None:
            if request_row is None:
                return EvidenceBuildResult(
                    result.evidence,
                    "stale_request",
                    "request disappeared before current evidence link",
                )
            expected_status = str(request_row["status"])
            if expected_status == "replaced" or not db.set_request_current_evidence(
                request_id,
                persisted.id,
                expected_status=expected_status,
            ):
                return EvidenceBuildResult(
                    result.evidence,
                    "stale_request",
                    "request state changed before current evidence link",
                )
    return result


def load_candidate_evidence_for_source(
    db: QualityEvidenceDB,
    *,
    source_path: str,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
) -> EvidenceBuildResult:
    """Load stored candidate evidence via the FK chain and verify freshness.

    Walks explicit ownership only: ``import_jobs.candidate_evidence_id`` when
    ``import_job_id`` is provided, then ``download_log.candidate_evidence_id``.
    It never falls back to another job on the same request. Once a candidate
    evidence row is found, ``audio_snapshot_matches`` confirms it still
    describes the audio at ``source_path``.
    """

    if download_log_id is None and import_job_id is None:
        return EvidenceBuildResult(None, "unowned", "no candidate owner")

    evidence_id: int | None = None
    if import_job_id is not None:
        evidence_id = db.get_import_job_candidate_evidence_id(import_job_id)
    if evidence_id is None and download_log_id is not None:
        evidence_id = db.get_download_log_candidate_evidence_id(download_log_id)

    if evidence_id is None:
        return EvidenceBuildResult(
            None,
            "missing",
            "no candidate evidence found via FK chain",
        )

    evidence = db.load_album_quality_evidence_by_id(evidence_id)
    if evidence is None:
        return EvidenceBuildResult(
            None,
            "missing",
            f"candidate evidence id {evidence_id} not found",
        )
    if not audio_snapshot_matches(source_path, evidence.files):
        return EvidenceBuildResult(
            None,
            "stale",
            "candidate source changed since evidence capture",
        )
    errors = evidence.policy_incomplete_reasons()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def load_or_backfill_current_evidence(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: Any = None,
    preloaded_evidence: AlbumQualityEvidence | None = None,
    preloaded: bool = False,
    beets_library_root: str = "",
) -> EvidenceBuildResult:
    """Load current Beets evidence, backfilling when absent or incomplete."""

    from lib.beets_db import BeetsDB
    from lib.quality import QualityRankConfig

    if preloaded:
        existing = preloaded_evidence
    else:
        existing_id = db.get_request_current_evidence_id(request_id)
        existing = (
            db.load_album_quality_evidence_by_id(existing_id)
            if existing_id is not None
            else None
        )
    if existing is not None:
        errors = existing.policy_incomplete_reasons()
        if not errors:
            return EvidenceBuildResult(existing, "ready")

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    with BeetsDB(library_root=beets_library_root) as beets:
        album_info = beets.get_album_info(mb_release_id, cfg)
    if album_info is None:
        return EvidenceBuildResult(None, "empty_current", "album not in beets")

    return backfill_current_evidence_from_album_info(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        album_info=album_info,
    )
