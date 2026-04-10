"""Shared test helpers — canonical mock data builders.

Builders for structured data used across tests. Use these instead of
hand-rolling dicts or dataclass constructors with many fields.
"""

from __future__ import annotations

import types
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

from lib.grab_list import DownloadFile, GrabListEntry
from lib.quality import (
    AudioQualityMeasurement,
    ConversionInfo,
    DownloadInfo,
    ImportResult,
    PostflightInfo,
    SpectralContext,
    SpectralMeasurement,
    ValidationResult,
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
) -> DownloadFile:
    """Build a real DownloadFile with sensible defaults."""
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
    )


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


def make_spectral_context(
    needs_check: bool = False,
    grade: str | None = None,
    bitrate: int | None = None,
    suspect_pct: float = 0.0,
    existing_min_bitrate: int | None = None,
    existing_spectral_bitrate: int | None = None,
    existing_spectral_grade: str | None = None,
) -> SpectralContext:
    """Build a SpectralContext with sensible defaults."""
    return SpectralContext(
        needs_check=needs_check,
        grade=grade,
        bitrate=bitrate,
        suspect_pct=suspect_pct,
        existing_min_bitrate=existing_min_bitrate,
        existing_spectral_bitrate=existing_spectral_bitrate,
        existing_spectral_grade=existing_spectral_grade,
    )


# ---------------------------------------------------------------------------
# Shared context wiring
# ---------------------------------------------------------------------------

def make_ctx_with_fake_db(fake_db: Any, *, cfg: Any = None) -> Any:
    """Build a SoularrContext wired to a FakePipelineDB.

    The fake is wired via pipeline_db_source._get_db() so production code
    that calls ctx.pipeline_db_source._get_db() gets the fake.
    """
    from lib.context import SoularrContext
    mock_source = MagicMock()
    mock_source._get_db.return_value = fake_db
    return SoularrContext(
        cfg=cfg if cfg is not None else MagicMock(),
        slskd=MagicMock(),
        pipeline_db_source=mock_source,
    )


@contextmanager
def patch_dispatch_externals():
    """Patch external edges shared by all dispatch_import_core tests.

    Patches: sp.run, _cleanup_staged_dir, trigger_meelo_scan,
    trigger_plex_scan, cleanup_disambiguation_orphans.

    Does NOT patch parse_import_result, _check_quality_gate_core,
    BeetsDB, or _read_runtime_config — callers nest those as needed.

    Yields a SimpleNamespace with attributes: run, cleanup, meelo, plex, orphans.
    run is pre-configured with returncode=0, stdout="", stderr="".
    """
    with patch("lib.import_dispatch.sp.run") as run, \
         patch("lib.import_dispatch._cleanup_staged_dir") as cleanup, \
         patch("lib.util.trigger_meelo_scan") as meelo, \
         patch("lib.util.trigger_plex_scan") as plex, \
         patch("lib.import_dispatch.cleanup_disambiguation_orphans",
               return_value=[]) as orphans:
        run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        yield types.SimpleNamespace(
            run=run, cleanup=cleanup, meelo=meelo, plex=plex, orphans=orphans)
