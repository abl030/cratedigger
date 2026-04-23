"""Typed service seam for `/api/beets/album/<id>` detail shaping.

Issue #155 moves the library-detail payload shaping out of
`web/routes/library.py` so the route only validates params, delegates to
this service, and serializes one owned contract.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol, cast

import msgspec

from lib.library_delete_service import (
    SupportsLibraryPipelineLookupDB,
    resolve_pipeline_request,
)
from lib.release_identity import detect_release_source, frontend_release_id
from web.download_history_view import DownloadHistoryViewRow, build_download_history_rows


class SupportsLibraryAlbumDetailLookup(Protocol):
    """Minimal beets-facing surface for one library album detail lookup."""

    def get_album_detail(self, album_id: int) -> dict[str, object] | None:
        ...


class SupportsLibraryAlbumDetailPipelineDB(
    SupportsLibraryPipelineLookupDB,
    Protocol,
):
    """Pipeline DB surface needed for library album detail overlays."""

    def get_download_history(self, request_id: int) -> list[dict[str, object]]:
        ...


def _timestamp(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    raise TypeError(
        "LibraryAlbumDetail added must be datetime|float|int, "
        f"got {type(value).__name__}"
    )


def _track_formats(tracks: Sequence["LibraryAlbumTrack"]) -> str:
    seen: set[str] = set()
    formats: list[str] = []
    for track in tracks:
        raw = str(track.format or "")
        if not raw or raw in seen:
            continue
        seen.add(raw)
        formats.append(raw)
    return ",".join(formats)


def _min_track_bitrate(tracks: Sequence["LibraryAlbumTrack"]) -> int | None:
    bitrates = [
        track.bitrate
        for track in tracks
        if isinstance(track.bitrate, int) and track.bitrate > 0
    ]
    return min(bitrates) if bitrates else None


class LibraryAlbumTrack(msgspec.Struct, frozen=True):
    """Typed frontend contract for one library album track."""

    disc: int | None
    track: int | None
    title: str | None
    length: float | None
    format: str
    bitrate: int | None
    samplerate: int | None
    bitdepth: int | None


class LibraryAlbumDetail(msgspec.Struct, frozen=True):
    """Owned `/api/beets/album/<id>` contract.

    Field notes:
    - `mb_albumid` is the single release key the frontend uses for actions.
      It intentionally carries either a MusicBrainz UUID or a Discogs numeric
      release ID string.
    - `release_group_title`, `track_count`, `formats`, and `min_bitrate` are
      guaranteed even when the raw beets detail row omits them; this service
      owns those frontend-compatibility fallbacks.
    - Pipeline overlay fields are always present so the frontend does not need
      `dict.get()` shape branching.
    """

    id: int
    album: str
    artist: str
    year: int | None
    mb_albumid: str | None
    track_count: int
    mb_releasegroupid: str | None
    release_group_title: str
    added: float | None
    formats: str
    min_bitrate: int | None
    type: str
    label: str
    country: str | None
    source: str
    path: str | None
    tracks: list[LibraryAlbumTrack]
    pipeline_id: int | None
    pipeline_status: str | None
    pipeline_source: str | None
    pipeline_min_bitrate: int | None
    search_filetype_override: str | None
    target_format: str | None
    upgrade_queued: bool
    download_history: list[DownloadHistoryViewRow]

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], msgspec.to_builtins(self))


def build_library_album_detail(
    *,
    detail_row: Mapping[str, object],
    pipeline_request: Mapping[str, object] | None,
    download_history: Sequence[Mapping[str, object]],
) -> LibraryAlbumDetail:
    """Build the owned library-detail contract from raw beets + pipeline rows."""
    tracks = [
        msgspec.convert(
            {
                "disc": track.get("disc"),
                "track": track.get("track"),
                "title": track.get("title"),
                "length": track.get("length"),
                "format": str(track.get("format") or ""),
                "bitrate": track.get("bitrate"),
                "samplerate": track.get("samplerate"),
                "bitdepth": track.get("bitdepth"),
            },
            type=LibraryAlbumTrack,
        )
        for track in cast(Sequence[Mapping[str, object]], detail_row.get("tracks") or [])
    ]
    frontend_id = frontend_release_id(
        detail_row.get("mb_albumid"),
        detail_row.get("discogs_albumid"),
    )
    raw_formats = str(detail_row.get("formats") or "")
    source = str(detail_row.get("source") or detect_release_source(frontend_id))
    history_items = build_download_history_rows(download_history)
    return msgspec.convert(
        {
            "id": detail_row["id"],
            "album": detail_row["album"],
            "artist": detail_row["artist"],
            "year": detail_row.get("year"),
            "mb_albumid": frontend_id,
            "track_count": detail_row.get("track_count") or len(tracks),
            "mb_releasegroupid": detail_row.get("mb_releasegroupid"),
            "release_group_title": (
                detail_row.get("release_group_title") or detail_row["album"]
            ),
            "added": _timestamp(detail_row.get("added")),
            "formats": raw_formats or _track_formats(tracks),
            "min_bitrate": detail_row.get("min_bitrate") or _min_track_bitrate(tracks),
            "type": str(detail_row.get("type") or ""),
            "label": str(detail_row.get("label") or ""),
            "country": detail_row.get("country"),
            "source": source or "unknown",
            "path": detail_row.get("path"),
            "tracks": tracks,
            "pipeline_id": pipeline_request.get("id") if pipeline_request else None,
            "pipeline_status": (
                pipeline_request.get("status") if pipeline_request else None
            ),
            "pipeline_source": (
                pipeline_request.get("source") if pipeline_request else None
            ),
            "pipeline_min_bitrate": (
                pipeline_request.get("min_bitrate") if pipeline_request else None
            ),
            "search_filetype_override": (
                pipeline_request.get("search_filetype_override")
                if pipeline_request
                else None
            ),
            "target_format": (
                pipeline_request.get("target_format") if pipeline_request else None
            ),
            "upgrade_queued": bool(
                pipeline_request
                and pipeline_request.get("status") == "wanted"
                and (
                    pipeline_request.get("search_filetype_override")
                    or pipeline_request.get("target_format")
                )
            ),
            "download_history": history_items,
        },
        type=LibraryAlbumDetail,
    )


def load_library_album_detail(
    *,
    library_lookup: SupportsLibraryAlbumDetailLookup,
    pipeline_db: SupportsLibraryAlbumDetailPipelineDB | None,
    album_id: int,
) -> LibraryAlbumDetail | None:
    """Load and shape one `/api/beets/album/<id>` response."""
    detail = library_lookup.get_album_detail(album_id)
    if detail is None:
        return None

    release_id = frontend_release_id(
        detail.get("mb_albumid"),
        detail.get("discogs_albumid"),
    )
    pipeline_request = (
        resolve_pipeline_request(
            pipeline_db,
            pipeline_id=None,
            release_id=release_id or "",
        )
        if release_id
        else None
    )
    history = (
        pipeline_db.get_download_history(int(pipeline_request["id"]))
        if pipeline_db is not None and pipeline_request is not None
        else []
    )
    return build_library_album_detail(
        detail_row=detail,
        pipeline_request=pipeline_request,
        download_history=history,
    )
