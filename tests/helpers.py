"""Shared test helpers — canonical mock data builders.

Builders for structured data used across tests. Use these instead of
hand-rolling dicts or dataclass constructors with many fields.
"""

from __future__ import annotations

import json
import msgspec
import os
import requests
import types
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import MagicMock, patch

from lib.grab_list import DownloadFile, GrabListEntry
from lib.quality import (
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    ActiveDownloadFileState,
    ActiveDownloadState,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    CodecRankBands,
    ConversionInfo,
    DisambiguationFailure,
    DownloadInfo,
    ImportResult,
    PostflightInfo,
    QualityRank,
    QualityRankConfig,
    RankBitrateMetric,
    SpectralMeasurement,
    TargetQualityContract,
    VerifiedLosslessProof,
    V0ProbeEvidence,
    ValidationResult,
    legacy_unrecorded_audio_validation_report,
)
from lib.quality_evidence import snapshot_fingerprint
from lib.slskd_client import DownloadDirectory, DownloadUser, TransferSnapshot


@contextmanager
def disposable_beets_storage_pair() -> Generator[tuple[str, str], None, None]:
    """Create a real empty Beets DB and its paired library root for one test."""
    from beets import library as beets_library

    with tempfile.TemporaryDirectory(prefix="cratedigger-test-beets-") as root:
        library_root = os.path.join(root, "library")
        library_db = os.path.join(root, "beets-library.db")
        os.mkdir(library_root)
        library = beets_library.Library(library_db, library_root)
        library._close()
        yield library_db, library_root


def make_request_row(**overrides: Any) -> dict[str, Any]:
    """Return a complete album_requests row dict with sensible defaults.

    Mirrors the shape of PipelineDB.get_request() (SELECT * FROM album_requests).
    Use keyword overrides to set specific fields for your test scenario.
    """
    row: dict[str, Any] = {
        "id": 1,
        "mb_release_id": "test-mbid-0001",
        "mb_release_group_id": None,
        "mb_artist_id": None,
        "discogs_release_id": None,
        "artist_name": "Test Artist",
        "album_title": "Test Album",
        "year": 2024,
        # Migration 026 — release-group's first-release year (U3 / R9).
        "release_group_year": None,
        # Migration 028 — VA detection flag (U4). NOT NULL DEFAULT FALSE.
        "is_va_compilation": False,
        # Migration 032 — label catalog number (PR1 U4). NULL when unresolved.
        "catalog_number": None,
        "country": "US",
        "format": None,
        "source": "request",
        "source_path": None,
        "reasoning": None,
        "status": "wanted",
        "search_attempts": 0,
        "download_attempts": 0,
        "validation_attempts": 0,
        "last_attempt_at": None,
        "next_retry_after": None,
        "beets_distance": None,
        "beets_scenario": None,
        "search_filetype_override": None,
        "target_format": None,
        "final_format": None,
        "min_bitrate": None,
        "prev_min_bitrate": None,
        "last_download_spectral_bitrate": None,
        "last_download_spectral_grade": None,
        "verified_lossless": False,
        "current_spectral_grade": None,
        "current_spectral_bitrate": None,
        "current_lossless_source_v0_probe_min_bitrate": None,
        "current_lossless_source_v0_probe_avg_bitrate": None,
        "current_lossless_source_v0_probe_median_bitrate": None,
        "active_download_state": None,
        # U1 persisted-search-plans cursor fields (migration 014).
        "active_plan_id": None,
        "next_plan_ordinal": 0,
        "plan_cycle_count": 0,
        # Migration 028 / U12 — failure_class materialised at plan-wrap.
        "failure_class": None,
        # Migration 028 / U13 — unfindable detection state. All nullable;
        # the four-category taxonomy is populated by the daily detection
        # job (lib/unfindable_detection_service.py).
        "unfindable_category": None,
        "unfindable_categorised_at": None,
        "last_artist_probe_at": None,
        "last_artist_probe_match_count": None,
        # Migration 028 / U14 — long-tail-rescue audit fields. Populated
        # when an unfindable-categorised request finally imports.
        "rescued_at": None,
        "prior_unfindable_category": None,
        # Migration 021 addressing FK.
        "current_evidence_id": None,
        # Migration 023 — supersede lineage.
        "replaces_request_id": None,
        "created_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "priority_started_at": None,
        "updated_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    }
    row.update(overrides)
    if "mb_release_id" not in overrides:
        # Default derives from the row id (id=1 → "test-mbid-0001") so
        # multi-row fixtures get distinct mbids and don't collide with
        # the UNIQUE(mb_release_id) FakePipelineDB enforces (#445 item 4).
        rid = row["id"]
        suffix = f"{rid:04d}" if isinstance(rid, int) else str(rid)
        row["mb_release_id"] = f"test-mbid-{suffix}"
    return row


def make_album_quality_evidence(
    *,
    mb_release_id: str = "test-mbid-0001",
    source_path: str = "/tmp/test-staged",
    measured_at: datetime | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
    measurement: AudioQualityMeasurement | None = None,
    v0_metric: AlbumQualityV0Metric | None = None,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    codec: str | None = "mp3",
    container: str | None = "mp3",
    storage_format: str | None = "MP3",
    target_format: str | None = None,
    target_is_cbr: bool | None = None,
    lineage_version: int = 4,
    on_disk_v0_research_attempted: bool = False,
    current_enrichment_required: bool = False,
    audio_corrupt: bool = False,
    audio_error: str | None = None,
    audio_validation: AudioValidationReport | None = None,
) -> AlbumQualityEvidence:
    """Build production-shaped active album-quality evidence.

    Migration 021: evidence is content-addressed by
    ``(mb_release_id, snapshot_fingerprint)``. The fingerprint is computed
    from ``files`` using the canonical helper, so the builder always
    produces a self-consistent row.
    """
    if measured_at is None:
        measured_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    if files is None:
        files = [
            AlbumQualityEvidenceFile(
                relative_path="01 - Track.mp3",
                size_bytes=123456,
                mtime_ns=1_700_000_000_000_000_000,
                extension="mp3",
                container="mp3",
                codec="mp3",
            ),
        ]
    if measurement is None:
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="MP3",
            spectral_grade="genuine",
            spectral_bitrate_kbps=None,
        )
    if (
        lineage_version == 4
        and measurement.spectral_grade is not None
        and measurement.spectral_subject is None
    ):
        measurement = msgspec.structs.replace(
            measurement,
            spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
            spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
        )
    if audio_validation is None and audio_corrupt:
        audio_validation = make_audio_corrupt_validation_report(
            files[0].relative_path if files else "",
            detail=audio_error or "synthetic decode failure",
            files_checked=len(files),
        )
        if files:
            files = [
                msgspec.structs.replace(file, decode_ok=index != 0)
                for index, file in enumerate(files)
            ]
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_path,
        measurement=measurement,
        measured_at=measured_at,
        files=files,
        codec=codec,
        container=container,
        storage_format=storage_format,
        target_format=target_format,
        target_is_cbr=(
            target_is_cbr
            if target_is_cbr is not None
            else (
                TargetQualityContract.from_explicit_label(target_format).is_cbr
                if target_format is not None
                else None
            )
        ),
        lineage_version=lineage_version,
        v0_metric=v0_metric,
        on_disk_v0_research_attempted=on_disk_v0_research_attempted,
        current_enrichment_required=current_enrichment_required,
        verified_lossless_proof=verified_lossless_proof,
        audio_validation=(
            audio_validation
            if audio_validation is not None
            else legacy_unrecorded_audio_validation_report()
        ),
        audio_corrupt=audio_corrupt,
        audio_error=audio_error,
    )


def make_audio_corrupt_validation_report(
    relative_path: str,
    *,
    detail: str = "synthetic decode failure",
    return_code: int = 69,
    files_checked: int = 1,
) -> AudioValidationReport:
    """Build one production-shaped corrupt-audio report for tests."""
    return AudioValidationReport(
        outcome="audio_corrupt",
        files_checked=files_checked,
        files_failed=1,
        diagnostics=[
            AudioToolDiagnostic(
                relative_path=relative_path,
                category="decode_error",
                return_code=return_code,
                stderr_excerpt=detail,
            ),
        ],
    )


def finalize_claimed_dispatch(db: Any, job: Any, outcome: Any) -> Any:
    """Apply a direct dispatch result through the production queue owner."""
    from scripts.importer import process_claimed_job

    return process_claimed_job(
        db,
        job,
        execute_fn=lambda *_args, **_kwargs: outcome,
    )


def build_parity_candidate_evidence(
    *,
    is_flac: bool,
    min_bitrate: int,
    is_cbr: bool,
    avg_bitrate: int | None = None,
    spectral_grade: str | None = None,
    spectral_bitrate: int | None = None,
    post_conversion_min_bitrate: int | None = None,
    candidate_v0_probe_avg: int | None = None,
    candidate_v0_probe_min: int | None = None,
    native_codec: str = "mp3",
    native_format: str = "MP3",
    mb_release_id: str = "mbid-parity-candidate",
    audio_corrupt: bool = False,
    folder_layout: str = "flat",
    audio_file_count: int | None = None,
    matched_bad_audio_hash_id: int | None = None,
    matched_bad_audio_hash_path: str | None = None,
    snapshot_fingerprint: str = "sha256:candidate-fingerprint",
) -> AlbumQualityEvidence:
    """Build an ``AlbumQualityEvidence`` candidate row matching the
    simulator's flat-kwargs shape (post-U2/U3 schema).

    This is the canonical simulator-world → evidence-row mapping. The
    hand-written parity tests in ``tests/test_quality_classification.py``
    and the generated parity property in ``tests/test_quality_generated.py``
    both consume it, so a divergence between the decision twins can never
    hide behind two different world encodings.
    """
    # Candidate evidence always describes the downloaded source bytes.
    # Conversion policy/output stay on the target contract and decision facts;
    # a temporary V0 probe must never make a FLAC source wear an MP3 label.
    if is_flac and post_conversion_min_bitrate is not None:
        container = "flac"
        codec = "flac"
        storage_format = "flac"
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate or 900,
            avg_bitrate_kbps=min_bitrate or 900,
            median_bitrate_kbps=min_bitrate or 900,
            format="FLAC",
            is_cbr=False,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        )
    elif is_flac:
        container = codec = "flac"
        storage_format = "flac"
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate or 900,
            avg_bitrate_kbps=min_bitrate or 900,
            median_bitrate_kbps=min_bitrate or 900,
            format="FLAC",
            is_cbr=False,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        )
    else:
        container = codec = native_codec
        storage_format = native_format.lower()
        _avg = avg_bitrate if avg_bitrate is not None else min_bitrate
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=_avg,
            median_bitrate_kbps=_avg,
            format=native_format,
            is_cbr=is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        )

    v0_metric = None
    if candidate_v0_probe_avg is not None or candidate_v0_probe_min is not None:
        v0_metric = AlbumQualityV0Metric(
            min_bitrate_kbps=candidate_v0_probe_min,
            avg_bitrate_kbps=candidate_v0_probe_avg,
            median_bitrate_kbps=candidate_v0_probe_avg,
            subject=EVIDENCE_SUBJECT_SOURCE,
            provenance=EVIDENCE_PROVENANCE_MEASURED,
        )

    files = [AlbumQualityEvidenceFile(
        relative_path=f"01.{container}",
        size_bytes=1, mtime_ns=1,
        extension=container, container=container, codec=codec,
    )]
    audio_validation = legacy_unrecorded_audio_validation_report()
    if audio_corrupt:
        files = [msgspec.structs.replace(files[0], decode_ok=False)]
        audio_validation = make_audio_corrupt_validation_report(
            files[0].relative_path,
        )
    # ``audio_file_count`` defaults to len(files) for the standard
    # parity scenarios. Tests covering empty_fileset explicitly pass
    # ``audio_file_count=0`` and override ``files`` separately.
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint,
        source_path="/Incoming/auto-import/candidate",
        measurement=measurement,
        measured_at=datetime(2026, 5, 16, tzinfo=timezone.utc),
        files=files,
        codec=codec,
        container=container,
        storage_format=storage_format,
        v0_metric=v0_metric,
        audio_validation=audio_validation,
        audio_corrupt=audio_corrupt,
        folder_layout=folder_layout,
        audio_file_count=(
            audio_file_count if audio_file_count is not None else len(files)
        ),
        filetype_band=storage_format,
        matched_bad_audio_hash_id=matched_bad_audio_hash_id,
        matched_bad_audio_hash_path=matched_bad_audio_hash_path,
    )


def build_parity_current_evidence(
    *,
    min_bitrate: int | None,
    avg_bitrate: int | None = None,
    format: str = "MP3",
    is_cbr: bool = False,
    spectral_grade: str | None = None,
    spectral_bitrate: int | None = None,
    mb_release_id: str = "mbid-parity-candidate",
    v0_metric: AlbumQualityV0Metric | None = None,
    matched_bad_audio_hash_id: int | None = None,
    matched_bad_audio_hash_path: str | None = None,
) -> AlbumQualityEvidence | None:
    """Build the existing-album evidence row for parity scenarios.

    Returns ``None`` when ``min_bitrate`` is ``None`` — the fresh-request
    shape where no current album exists.
    """
    if min_bitrate is None:
        return None

    container = format.lower().split()[0]
    files = [AlbumQualityEvidenceFile(
        relative_path=f"01.{container}",
        size_bytes=1, mtime_ns=1,
        extension=container, container=container, codec=container,
    )]
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint="sha256:current-fingerprint",
        source_path="/Beets/current",
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
            median_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
            format=format,
            is_cbr=is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_INSTALLED
                if spectral_grade is not None
                else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        ),
        measured_at=datetime(2026, 5, 16, tzinfo=timezone.utc),
        files=files,
        codec=container,
        container=container,
        storage_format=format.lower(),
        audio_file_count=len(files),
        filetype_band=format.lower(),
        v0_metric=v0_metric,
        matched_bad_audio_hash_id=matched_bad_audio_hash_id,
        matched_bad_audio_hash_path=matched_bad_audio_hash_path,
    )


def make_file_complete_event_data(
    *,
    username: str,
    filename: str,
    local_filename: str,
    transfer_id: str = "t-1",
    size: int = 1000,
) -> str:
    """The JSON ``data`` string of a slskd DownloadFileComplete event,
    exactly as the live feed emits it (camelCase, nested transfer DTO)."""
    return json.dumps({
        "version": 0,
        "localFilename": local_filename,
        "remoteFilename": filename,
        "transfer": {
            "id": transfer_id,
            "username": username,
            "filename": filename,
            "size": size,
        },
    })


def make_active_download_file_state(
    username: str = "peer1",
    filename: str = "music\\Artist\\Album\\01 track.flac",
    size: int = 1000,
) -> ActiveDownloadFileState:
    return ActiveDownloadFileState(
        username=username,
        filename=filename,
        file_dir=filename.rsplit("\\", 1)[0] if "\\" in filename else "music",
        size=size,
    )


def make_active_download_state_json(
    files: list[ActiveDownloadFileState],
    filetype: str = "flac",
) -> str:
    return ActiveDownloadState(
        filetype=filetype,
        enqueued_at="2026-07-01T00:00:00+00:00",
        files=files,
    ).to_json()


def make_evidence(
    mb_release_id: str = "test-mbid-0001",
    files: list[AlbumQualityEvidenceFile] | None = None,
    **overrides: Any,
) -> AlbumQualityEvidence:
    """Concise builder for content-addressed evidence rows.

    Mirrors :func:`make_album_quality_evidence` with a positional-first
    signature optimised for the post-021 rekey: pass ``mb_release_id`` and
    ``files``, get back a fully-formed row with the snapshot fingerprint
    already computed.
    """
    return make_album_quality_evidence(
        mb_release_id=mb_release_id,
        files=files,
        **overrides,
    )


def make_import_result(
    decision: str = "import",
    new_min_bitrate: int = 245,
    prev_min_bitrate: int | None = None,
    was_converted: bool = False,
    original_filetype: str | None = None,
    target_filetype: str | None = None,
    spectral_grade: str = "genuine",
    spectral_bitrate: int | None = None,
    verified_lossless: bool | None = None,
    error: str | None = None,
    imported_path: str | None = None,
    disambiguated: bool = False,
    disambiguation_failure: DisambiguationFailure | None = None,
    final_format: str | None = None,
    v0_probe: V0ProbeEvidence | None = None,
    existing_v0_probe: V0ProbeEvidence | None = None,
) -> ImportResult:
    """Build an ImportResult with sensible defaults."""
    if verified_lossless is None:
        verified_lossless = was_converted and spectral_grade == "genuine"
    return ImportResult(
        decision=decision,
        error=error,
        source_measurement=AudioQualityMeasurement(
            min_bitrate_kbps=new_min_bitrate,
            avg_bitrate_kbps=new_min_bitrate,
            median_bitrate_kbps=new_min_bitrate,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
            format=(original_filetype or "FLAC").upper() if was_converted else None,
        ),
        verified_lossless_proof=(
            VerifiedLosslessProof(
                provenance=EVIDENCE_PROVENANCE_MEASURED,
                source=original_filetype or "lossless_source",
                classifier="test_helper",
                detail=spectral_grade,
            )
            if verified_lossless else None
        ),
        current_measurement=(AudioQualityMeasurement(
                                  min_bitrate_kbps=prev_min_bitrate,
                                  avg_bitrate_kbps=prev_min_bitrate,
                                  median_bitrate_kbps=prev_min_bitrate)
                              if prev_min_bitrate is not None else None),
        conversion=ConversionInfo(
            was_converted=was_converted,
            original_filetype=original_filetype or "",
            target_filetype=target_filetype or "",
        ),
        postflight=PostflightInfo(
            imported_path=imported_path,
            disambiguated=disambiguated,
            disambiguation_failure=disambiguation_failure,
        ),
        final_format=final_format,
        target_quality_contract=(
            TargetQualityContract.from_explicit_label(final_format)
            if was_converted and final_format
            else None
        ),
        v0_probe=v0_probe,
        existing_v0_probe=existing_v0_probe,
    )


def make_quality_rank_config(
    *,
    bitrate_metric: RankBitrateMetric | None = None,
    within_rank_tolerance_kbps: int | None = None,
    opus: CodecRankBands | None = None,
    mp3_vbr: CodecRankBands | None = None,
    mp3_cbr: CodecRankBands | None = None,
    aac: CodecRankBands | None = None,
) -> QualityRankConfig:
    """Build a QualityRankConfig with test-friendly overrides.

    Defaults match QualityRankConfig.defaults() — override individual fields
    to test metric swaps or custom codec bands. Use
    this instead of constructing QualityRankConfig directly so tests stay
    stable when the dataclass grows new fields.
    """
    base = QualityRankConfig.defaults()
    return QualityRankConfig(
        bitrate_metric=bitrate_metric if bitrate_metric is not None else base.bitrate_metric,
        within_rank_tolerance_kbps=(
            within_rank_tolerance_kbps
            if within_rank_tolerance_kbps is not None
            else base.within_rank_tolerance_kbps
        ),
        opus=opus if opus is not None else base.opus,
        mp3_vbr=mp3_vbr if mp3_vbr is not None else base.mp3_vbr,
        mp3_cbr=mp3_cbr if mp3_cbr is not None else base.mp3_cbr,
        aac=aac if aac is not None else base.aac,
        mp3_vbr_levels=base.mp3_vbr_levels,
        lossless_codecs=base.lossless_codecs,
        mixed_format_precedence=base.mixed_format_precedence,
    )


def make_download_info(
    username: str | None = None,
    filetype: str | None = None,
    bitrate: int | None = None,
    download_spectral: SpectralMeasurement | None = None,
    current_spectral: SpectralMeasurement | None = None,
    existing_min_bitrate: int | None = None,
    **overrides: Any,
) -> DownloadInfo:
    """Build a DownloadInfo with sensible defaults."""
    di = DownloadInfo(
        username=username,
        filetype=filetype,
        bitrate=bitrate,
        download_spectral=download_spectral,
        current_spectral=current_spectral,
        existing_min_bitrate=existing_min_bitrate,
    )
    for k, v in overrides.items():
        setattr(di, k, v)
    return di


def make_download_file(
    filename: str = "01 - Track.mp3",
    id: str = "file-id-1",
    file_dir: str = "user1\\Music",
    username: str = "user1",
    size: int = 5_000_000,
    bitRate: int | None = 320,
    sampleRate: int | None = 44100,
    bitDepth: int | None = None,
    isVariableBitRate: bool | None = None,
    last_state: str | None = None,
    last_exception: str | None = None,
    bytes_transferred: int | None = None,
    retry: int | None = None,
) -> DownloadFile:
    """Build a real DownloadFile with sensible defaults.

    ``last_state``/``last_exception``/``bytes_transferred``/``retry`` are
    the persisted poll-state fields (issue #564) — default ``None`` like
    ``DownloadFile`` itself; pass overrides for scenarios that need
    pre-seeded failure evidence.
    """
    return DownloadFile(
        filename=filename,
        id=id,
        file_dir=file_dir,
        username=username,
        size=size,
        bitRate=bitRate,
        sampleRate=sampleRate,
        bitDepth=bitDepth,
        isVariableBitRate=isVariableBitRate,
        last_state=last_state,
        last_exception=last_exception,
        bytes_transferred=bytes_transferred,
        retry=retry,
    )


def make_transfer_snapshot(**overrides: Any) -> TransferSnapshot:
    """Build a TransferSnapshot (DownloadFile.status, issue #468) with a
    sensible default state. Every other field defaults per the Struct
    itself — pass overrides for the fields a scenario cares about."""
    defaults: dict[str, Any] = {"state": "Completed, Succeeded"}
    defaults.update(overrides)
    return TransferSnapshot(**defaults)


def make_download_directory(**overrides: Any) -> DownloadDirectory:
    """Build a DownloadDirectory — one directory row of the
    get_all_downloads() envelope (issue #507) — with an empty file list
    by default."""
    defaults: dict[str, Any] = {"directory": "user1\\Music", "files": []}
    defaults.update(overrides)
    return DownloadDirectory(**defaults)


def make_download_user(**overrides: Any) -> DownloadUser:
    """Build a DownloadUser — one user-group row of the
    get_all_downloads() envelope (issue #507) — with an empty directory
    list by default."""
    defaults: dict[str, Any] = {"username": "user1", "directories": []}
    defaults.update(overrides)
    return DownloadUser(**defaults)


def make_grab_list_entry(
    album_id: int = 1,
    files: list[DownloadFile] | None = None,
    filetype: str = "mp3",
    title: str = "Test Album",
    artist: str = "Test Artist",
    year: str = "2020",
    mb_release_id: str = "test-mbid",
    db_request_id: int | None = None,
    db_source: str | None = None,
    db_search_filetype_override: str | None = None,
    db_target_format: str | None = None,
    download_spectral: SpectralMeasurement | None = None,
    current_min_bitrate: int | None = None,
    current_spectral: SpectralMeasurement | None = None,
) -> GrabListEntry:
    """Build a real GrabListEntry with sensible defaults."""
    return GrabListEntry(
        album_id=album_id,
        files=files if files is not None else [make_download_file()],
        filetype=filetype,
        title=title,
        artist=artist,
        year=year,
        mb_release_id=mb_release_id,
        db_request_id=db_request_id,
        db_source=db_source,
        db_search_filetype_override=db_search_filetype_override,
        db_target_format=db_target_format,
        download_spectral=download_spectral,
        current_min_bitrate=current_min_bitrate,
        current_spectral=current_spectral,
    )


def make_validation_result(**overrides: Any) -> ValidationResult:
    """Build a ValidationResult with sensible defaults.

    Uses keyword overrides like make_request_row.
    """
    defaults: dict[str, Any] = {
        "valid": True,
        "distance": 0.05,
        "scenario": "strong_match",
    }
    defaults.update(overrides)
    return ValidationResult(**defaults)


# ---------------------------------------------------------------------------
# Shared context wiring
# ---------------------------------------------------------------------------

def make_ctx_with_fake_db(
    fake_db: Any,
    *,
    cfg: Any = None,
    slskd: Any = None,
) -> Any:
    """Build a CratediggerContext wired to a FakePipelineDB.

    The fake is wrapped in a ``FakePipelineDBSource`` so production code
    that calls ``ctx.pipeline_db_source._get_db()`` (or any of the source's
    higher-level methods) hits a typed surface, not a MagicMock that
    silently accepts arbitrary attribute access.
    """
    from lib.context import CratediggerContext
    from tests.fakes import FakePipelineDBSource
    source = FakePipelineDBSource(fake_db)
    return CratediggerContext(
        cfg=cfg if cfg is not None else MagicMock(),
        slskd=slskd if slskd is not None else MagicMock(),
        pipeline_db_source=source,
    )


def noop_quality_gate(**_kwargs: Any) -> None:
    """No-op quality-gate stub for ``dispatch_import_core(quality_gate_fn=...)``.

    Replaces the legacy module-attribute patch on
    ``_check_quality_gate_core`` for dispatch tests that don't care
    about the post-import quality gate's side effects — they want a
    no-op so the dispatch decision tree runs end-to-end without
    inspecting beets DB state."""
    return None


def make_requests_http_error(
    body: str,
    *,
    status_code: int = 500,
) -> "requests.HTTPError":
    """Build a real requests HTTPError with its immutable response supplied.

    ``requests.HTTPError.response`` is read-only in current stubs.  Passing a
    real ``Response`` to the constructor also keeps test doubles faithful to
    the slskd client's production exception contract.
    """
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode()
    return requests.HTTPError(f"{status_code} Server Error", response=response)


class RecordingQualityGate:
    """Recorder ``quality_gate_fn`` stub. Replaces the legacy
    module-attribute patch on ``_check_quality_gate_core`` (paired with
    ``as mock_gate``) for tests that assert
    ``mock_gate.assert_called_once()``.

    Records each invocation's kwargs (the gate is keyword-only) so tests
    can assert call counts and arguments."""

    def __init__(self, *, result: object | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.result = result

    def __call__(self, **kwargs: Any) -> object | None:
        self.calls.append(kwargs)
        return self.result

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def assert_called_once(self) -> None:
        if len(self.calls) != 1:
            raise AssertionError(
                f"expected quality_gate_fn called exactly once, got {len(self.calls)}"
            )

    def assert_not_called(self) -> None:
        if self.calls:
            raise AssertionError(
                f"expected quality_gate_fn not called, got {len(self.calls)} call(s)"
            )


@contextmanager
def patch_dispatch_externals():
    """Patch external edges shared by all dispatch_import_core tests.

    Patches: sp.run, the evidence-rejection cleanup seam, trigger_plex_scan,
    and trigger_jellyfin_scan.

    Does NOT patch parse_import_result, _check_quality_gate_core,
    BeetsDB, or read_runtime_config — callers nest those as needed.

    Yields a SimpleNamespace with attributes: run, cleanup, plex, jellyfin.
    run is pre-configured with returncode=0, stdout="", stderr="".

    Importer post-commit cleanup is exercised through real inputs or its
    dedicated queue-owner seam; this helper does not patch that owned code.
    """
    cleanup = MagicMock()
    with patch("lib.dispatch.subprocess_runner.sp.run") as run, \
         patch("lib.dispatch.outcome_actions._cleanup_staged_dir", cleanup), \
         patch("lib.util.trigger_plex_scan") as plex, \
         patch("lib.util.trigger_jellyfin_scan") as jellyfin:
        run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        yield types.SimpleNamespace(
            run=run, cleanup=cleanup, plex=plex,
            jellyfin=jellyfin)
