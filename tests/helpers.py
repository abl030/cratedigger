"""Shared test helpers — canonical mock data builders.

Builders for structured data used across tests. Use these instead of
hand-rolling dicts or dataclass constructors with many fields.
"""

from __future__ import annotations

from typing import Any

from lib.quality import (
    AudioQualityMeasurement,
    ConversionInfo,
    DownloadInfo,
    ImportResult,
    PostflightInfo,
    SpectralMeasurement,
)


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
        "imported_path": None,
        "search_filetype_override": None,
        "target_format": None,
        "min_bitrate": None,
        "prev_min_bitrate": None,
        "lidarr_album_id": None,
        "lidarr_artist_id": None,
        "last_download_spectral_bitrate": None,
        "last_download_spectral_grade": None,
        "verified_lossless": False,
        "current_spectral_grade": None,
        "current_spectral_bitrate": None,
        "active_download_state": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    row.update(overrides)
    return row


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
    final_format: str | None = None,
) -> ImportResult:
    """Build an ImportResult with sensible defaults."""
    if verified_lossless is None:
        verified_lossless = was_converted and spectral_grade == "genuine"
    return ImportResult(
        decision=decision,
        error=error,
        new_measurement=AudioQualityMeasurement(
            min_bitrate_kbps=new_min_bitrate,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            verified_lossless=verified_lossless,
            was_converted_from=original_filetype if was_converted else None,
        ),
        existing_measurement=(AudioQualityMeasurement(min_bitrate_kbps=prev_min_bitrate)
                              if prev_min_bitrate is not None else None),
        conversion=ConversionInfo(
            was_converted=was_converted,
            original_filetype=original_filetype or "",
            target_filetype=target_filetype or "",
        ),
        postflight=PostflightInfo(
            imported_path=imported_path,
            disambiguated=disambiguated,
        ),
        final_format=final_format,
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
