"""Typed service seam for `/api/library/artist` row shaping.

Issue #155 moves the merge / dedup / sort logic out of
`web/routes/browse.py` so the route only validates params, calls this
service, and serializes the typed row contract.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from lib.release_identity import ReleaseIdentity
from web.library_album_row import LibraryAlbumRow


class SupportsLibraryArtistLookup(Protocol):
    """Minimal beets-facing surface for artist-scoped library rows."""

    def get_library_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        ...


class SupportsLibraryArtistPipelineDB(Protocol):
    """Minimal pipeline DB surface for artist-scoped library rows."""

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        ...

    def get_track_counts(self, request_ids: list[int]) -> dict[int, int]:
        ...


def _library_album_sort_key(
    row: LibraryAlbumRow,
) -> tuple[bool, int, str, str, int, int]:
    """Deterministic chronological-ish ordering for merged library rows."""
    year_num = row.year if isinstance(row.year, int) else 0
    country = row.country or ""
    beets_key = row.beets_album_id if isinstance(row.beets_album_id, int) else -1
    pipeline_key = row.pipeline_id if isinstance(row.pipeline_id, int) else -1
    return (
        row.year is None,
        year_num,
        row.album.casefold(),
        country.casefold(),
        beets_key,
        pipeline_key,
    )


def _pipeline_rows_by_identity(
    pipeline_rows: Sequence[Mapping[str, object]],
) -> dict[tuple[str, str], Mapping[str, object]]:
    rows_by_identity: dict[tuple[str, str], Mapping[str, object]] = {}
    for row in pipeline_rows:
        identity = ReleaseIdentity.from_fields(
            row.get("mb_release_id"),
            row.get("discogs_release_id"),
        )
        if identity is None:
            continue
        rows_by_identity[identity.key] = row
    return rows_by_identity


def _request_id(row: Mapping[str, object]) -> int:
    """Typed request-id extraction for pipeline rows."""
    raw = row["id"]
    if isinstance(raw, int):
        return raw
    raise TypeError(
        "Pipeline request rows for /api/library/artist must carry an int id, "
        f"got {type(raw).__name__}"
    )


def build_library_artist_rows(
    *,
    library_albums: Sequence[Mapping[str, object]],
    pipeline_rows: Sequence[Mapping[str, object]],
    track_counts: Mapping[int, int],
    rank_fn: Callable[[str | None, int | None], str],
) -> list[LibraryAlbumRow]:
    """Merge beets + pipeline artist rows behind one typed seam."""
    pipeline_by_identity = _pipeline_rows_by_identity(pipeline_rows)
    rows: list[LibraryAlbumRow] = []
    seen_release_ids: set[tuple[str, str]] = set()

    for album in library_albums:
        identity = ReleaseIdentity.from_fields(
            album.get("mb_albumid"),
            album.get("discogs_albumid"),
        )
        row = LibraryAlbumRow.from_beets_album_with_pipeline(
            album,
            pipeline_row=pipeline_by_identity.get(identity.key) if identity else None,
            rank_fn=rank_fn,
        )
        rows.append(row)
        if row.identity:
            seen_release_ids.add(row.identity.key)

    for pipeline_row in pipeline_rows:
        request_id = _request_id(pipeline_row)
        row = LibraryAlbumRow.from_pipeline_request(
            pipeline_row,
            track_count=track_counts.get(request_id, 0),
        )
        if row.identity and row.identity.key in seen_release_ids:
            continue
        rows.append(row)

    rows.sort(key=_library_album_sort_key)
    return rows


def list_library_artist_rows(
    *,
    library_lookup: SupportsLibraryArtistLookup,
    pipeline_db: SupportsLibraryArtistPipelineDB | None,
    artist_name: str,
    mb_artist_id: str,
    rank_fn: Callable[[str | None, int | None], str],
) -> list[LibraryAlbumRow]:
    """Load and shape `/api/library/artist` rows for one artist."""
    library_albums = library_lookup.get_library_artist(artist_name, mb_artist_id)
    if pipeline_db is None:
        return build_library_artist_rows(
            library_albums=library_albums,
            pipeline_rows=[],
            track_counts={},
            rank_fn=rank_fn,
        )

    pipeline_rows = pipeline_db.list_requests_by_artist(artist_name, mb_artist_id)
    request_ids = [_request_id(row) for row in pipeline_rows]
    track_counts = pipeline_db.get_track_counts(request_ids) if request_ids else {}
    return build_library_artist_rows(
        library_albums=library_albums,
        pipeline_rows=pipeline_rows,
        track_counts=track_counts,
        rank_fn=rank_fn,
    )
