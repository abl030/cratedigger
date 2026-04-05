"""Shared test helpers — canonical mock data builders."""

from __future__ import annotations

from typing import Any


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
        "quality_override": None,
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
