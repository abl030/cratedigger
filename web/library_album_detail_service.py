"""Typed service seam for `/api/beets/album/<id>` detail shaping.

Issue #155 moves the library-detail payload shaping out of
`web/routes/library.py` so the route only validates params, delegates to
this service, and serializes one owned contract.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, TypeGuard

import msgspec

from lib.convergence_service import ConvergenceSignal
from lib.json_narrow import is_object_list as _is_object_list
from lib.pipeline_db._shared import ProcessingOwnerProjection
from lib.release_identity import (
    ReleaseIdentity,
    detect_release_source,
    frontend_release_id,
    normalize_release_id,
)
from web.download_history_view import (
    DownloadHistoryViewRow,
    build_download_history_rows,
)
from web.library_album_row import (
    _pipeline_upgrade_queued,
    select_exact_library_request_attachment,
)

if TYPE_CHECKING:
    from lib.pipeline_db.rows import ArtistRequestRow, DownloadLogWithEvidenceRow


class SupportsLibraryAlbumDetailLookup(Protocol):
    """Minimal beets-facing surface for one library album detail lookup."""

    def get_album_detail(self, album_id: int) -> dict[str, object] | None:
        ...


class SupportsLibraryAlbumDetailPipelineDB(
    Protocol,
):
    """Pipeline DB surface needed for library album detail overlays."""

    def list_library_request_candidates(
        self,
        release_ids: list[str],
    ) -> list[ArtistRequestRow]:
        ...

    def get_download_history(
        self, request_id: int,
    ) -> list[DownloadLogWithEvidenceRow]:
        ...

    def get_convergence_signals(
        self, request_ids: list[int],
    ) -> dict[int, ConvergenceSignal]: ...


def _timestamp(value: object | None) -> float | str | None:
    """Preserve legacy beets ``added`` shapes while normalizing datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, str):
        return value
    raise TypeError(
        "LibraryAlbumDetail added must be datetime|float|int|str, "
        f"got {type(value).__name__}"
    )


def _track_formats(tracks: Sequence[LibraryAlbumTrack]) -> str:
    seen: set[str] = set()
    formats: list[str] = []
    for track in tracks:
        raw = str(track.format or "")
        if not raw or raw in seen:
            continue
        seen.add(raw)
        formats.append(raw)
    return ",".join(formats)


def _min_track_bitrate(tracks: Sequence[LibraryAlbumTrack]) -> int | None:
    bitrates = [
        track.bitrate
        for track in tracks
        if isinstance(track.bitrate, int) and track.bitrate > 0
    ]
    return min(bitrates) if bitrates else None


def _detail_release_id(detail_row: Mapping[str, object]) -> str | None:
    """Preserve exact-release IDs across canonical and unknown legacy shapes.

    When both legacy columns carry non-canonical non-empty strings, prefer the
    ``mb_albumid`` value so pipeline lookup matches the pre-service route.
    """
    frontend_id = frontend_release_id(
        detail_row.get("mb_albumid"),
        detail_row.get("discogs_albumid"),
    )
    if frontend_id:
        return frontend_id

    legacy_id = normalize_release_id(detail_row.get("mb_albumid"))
    if legacy_id:
        return legacy_id

    discogs_id = normalize_release_id(detail_row.get("discogs_albumid"))
    return discogs_id or None


class LibraryAlbumTrack(msgspec.Struct, frozen=True):
    """Typed frontend contract for one library album track."""

    id: int | None
    artist: str | None
    disc: int | None
    track: int | None
    title: str | None
    length: float | None
    format: str | None
    bitrate: int | None
    samplerate: int | None
    bitdepth: int | None
    path: str | None


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
    added: float | str | None
    formats: str
    min_bitrate: int | None
    type: str
    label: str
    country: str | None
    source: str
    artpath: str | None
    path: str | None
    tracks: list[LibraryAlbumTrack]
    pipeline_id: int | None
    pipeline_status: str | None
    processing_owner: ProcessingOwnerProjection | None
    pipeline_source: str | None
    pipeline_min_bitrate: int | None
    search_filetype_override: str | None
    target_format: str | None
    upgrade_queued: bool
    download_history: list[DownloadHistoryViewRow]
    convergence: ConvergenceSignal | None = None

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


def _is_str_object_mapping(value: object) -> TypeGuard[Mapping[str, object]]:
    """Narrow to ``Mapping[str, object]`` with a precise type, not the
    generic-erased ``Mapping[Unknown, Unknown]`` a bare
    ``isinstance(value, Mapping)`` narrows to under strict pyright."""
    return isinstance(value, Mapping)


def _detail_track_rows(
    detail_row: Mapping[str, object],
) -> list[Mapping[str, object]]:
    """Narrow the raw ``tracks`` value to the row mappings it carries."""
    raw = detail_row.get("tracks")
    if not _is_object_list(raw):
        return []
    return [row for row in raw if _is_str_object_mapping(row)]


def build_library_album_detail(
    *,
    detail_row: Mapping[str, object],
    pipeline_request: Mapping[str, object] | None,
    download_history: Sequence[Mapping[str, object]],
    attached_identity: ReleaseIdentity | None = None,
    convergence: ConvergenceSignal | None = None,
) -> LibraryAlbumDetail:
    """Build the owned library-detail contract from raw beets + pipeline rows."""
    observation_identities = ReleaseIdentity.all_from_observation_fields(
        detail_row.get("mb_albumid"),
        detail_row.get("discogs_albumid"),
    )
    effective_attachment = attached_identity
    if pipeline_request is not None and effective_attachment is None:
        effective_attachment = ReleaseIdentity.from_strict_fields(
            pipeline_request.get("mb_release_id"),
            pipeline_request.get("discogs_release_id"),
        )
    if pipeline_request is None:
        if effective_attachment is not None:
            raise ValueError("library detail attachment has no pipeline request")
    elif effective_attachment not in observation_identities:
        raise ValueError(
            "attached pipeline identity is not observed on Beets album"
        )
    tracks = [
        msgspec.convert(
            {
                "id": track.get("id"),
                "artist": track.get("artist"),
                "disc": track.get("disc"),
                "track": track.get("track"),
                "title": track.get("title"),
                "length": track.get("length"),
                "format": (
                    None
                    if track.get("format") is None
                    else str(track.get("format"))
                ),
                "bitrate": track.get("bitrate"),
                "samplerate": track.get("samplerate"),
                "bitdepth": track.get("bitdepth"),
                "path": track.get("path"),
            },
            type=LibraryAlbumTrack,
        )
        for track in _detail_track_rows(detail_row)
    ]
    frontend_id = (
        effective_attachment.release_id
        if effective_attachment is not None
        else _detail_release_id(detail_row)
    )
    raw_formats = str(detail_row.get("formats") or "")
    source = (
        effective_attachment.source
        if effective_attachment is not None
        else str(detail_row.get("source") or detect_release_source(frontend_id))
    )
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
            "artpath": detail_row.get("artpath"),
            "path": detail_row.get("path"),
            "tracks": tracks,
            "pipeline_id": pipeline_request.get("id") if pipeline_request else None,
            "pipeline_status": (
                pipeline_request.get("status") if pipeline_request else None
            ),
            "processing_owner": (
                pipeline_request.get("processing_owner")
                if pipeline_request
                else None
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
            "upgrade_queued": _pipeline_upgrade_queued(pipeline_request),
            "download_history": history_items,
            "convergence": convergence,
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

    observation_identities = ReleaseIdentity.all_from_observation_fields(
        detail.get("mb_albumid"),
        detail.get("discogs_albumid"),
    )
    requests_by_identity: dict[
        tuple[str, str],
        list[Mapping[str, object]],
    ] = {}
    if pipeline_db is not None:
        candidates = pipeline_db.list_library_request_candidates([
            identity.release_id for identity in observation_identities
        ])
        observed_keys = {identity.key for identity in observation_identities}
        for request in candidates:
            request_identity = ReleaseIdentity.from_strict_fields(
                request.get("mb_release_id"),
                request.get("discogs_release_id"),
            )
            if request_identity is not None and request_identity.key in observed_keys:
                requests_by_identity.setdefault(request_identity.key, []).append(
                    request
                )
    attachment = select_exact_library_request_attachment(
        observation_identities,
        requests_by_identity,
    )
    pipeline_request = attachment.request if attachment is not None else None
    history = (
        pipeline_db.get_download_history(attachment.request_id)
        if pipeline_db is not None and attachment is not None
        else []
    )
    convergence = (
        pipeline_db.get_convergence_signals([attachment.request_id]).get(
            attachment.request_id
        )
        if pipeline_db is not None and attachment is not None
        else None
    )
    return build_library_album_detail(
        detail_row=detail,
        pipeline_request=pipeline_request,
        download_history=history,
        attached_identity=attachment.identity if attachment is not None else None,
        convergence=convergence,
    )
