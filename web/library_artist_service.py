"""Typed service seam for `/api/library/artist` row shaping.

Issue #155 moves the merge / dedup / sort logic out of
`web/routes/browse.py` so the route only validates params, calls this
service, and serializes the typed row contract.

The service intentionally reads pipeline rows before beets rows. That
preserves the pre-refactor concurrency behavior where an import that
finishes between the two reads can still collapse onto the later beets
row instead of momentarily surfacing as pipeline-only.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Protocol

import msgspec

from lib.convergence_service import ConvergenceSignal
from lib.release_identity import ReleaseIdentity
from web.library_album_row import (
    LibraryAlbumRow,
    select_exact_library_request_attachment,
)

if TYPE_CHECKING:
    from lib.pipeline_db.rows import ArtistRequestRow


class SupportsLibraryArtistLookup(Protocol):
    """Minimal beets-facing surface for artist-scoped library rows.

    The production caller passes the ``web.server`` module, while tests
    pass stub objects. The Protocol models the shared callable surface.
    """

    def get_library_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        ...

    def get_library_releases(
        self,
        release_ids: list[str],
    ) -> list[dict[str, object]]:
        ...


class SupportsLibraryArtistPipelineDB(Protocol):
    """Minimal pipeline DB surface for artist-scoped library rows."""

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[ArtistRequestRow]:
        ...

    def get_track_counts(self, request_ids: list[int]) -> dict[int, int]:
        ...

    def list_library_request_candidates(
        self,
        release_ids: list[str],
    ) -> list[ArtistRequestRow]:
        ...

    def get_convergence_signals(
        self, request_ids: list[int] | None = None,
    ) -> dict[int, ConvergenceSignal]: ...


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
) -> dict[tuple[str, str], tuple[Mapping[str, object], ...]]:
    mutable: dict[tuple[str, str], list[Mapping[str, object]]] = {}
    for row in pipeline_rows:
        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"),
            row.get("discogs_release_id"),
        )
        if identity is None:
            continue
        mutable.setdefault(identity.key, []).append(row)
    return {
        identity_key: tuple(rows)
        for identity_key, rows in mutable.items()
    }


def _library_identity_keys(
    album: Mapping[str, object],
) -> tuple[tuple[str, str], ...]:
    """Every exact identity a non-destructive Beets row may represent."""
    return tuple(
        identity.key
        for identity in _library_identities(album)
    )


def _library_identities(
    album: Mapping[str, object],
) -> tuple[ReleaseIdentity, ...]:
    """Every exact identity observed on a non-destructive Beets row."""
    return ReleaseIdentity.all_from_observation_fields(
        album.get("mb_albumid"),
        album.get("discogs_albumid"),
    )


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
    pipeline_candidates: Sequence[Mapping[str, object]] | None = None,
    convergence_signals: Mapping[int, ConvergenceSignal] | None = None,
) -> list[LibraryAlbumRow]:
    """Merge beets + pipeline artist rows behind one typed seam."""
    pipeline_by_identity = _pipeline_rows_by_identity(
        pipeline_rows if pipeline_candidates is None else pipeline_candidates
    )
    rows: list[LibraryAlbumRow] = []
    seen_release_ids: set[tuple[str, str]] = set()

    for album in library_albums:
        observation_identities = _library_identities(album)
        attachment = select_exact_library_request_attachment(
            observation_identities,
            pipeline_by_identity,
        )
        row = LibraryAlbumRow.from_beets_album_with_pipeline(
            album,
            pipeline_row=attachment.request if attachment is not None else None,
            rank_fn=rank_fn,
            attached_identity=(
                attachment.identity if attachment is not None else None
            ),
        )
        rows.append(row)
        seen_release_ids.update(identity.key for identity in observation_identities)

    for pipeline_row in pipeline_rows:
        request_id = _request_id(pipeline_row)
        row = LibraryAlbumRow.from_pipeline_request(
            pipeline_row,
            track_count=track_counts.get(request_id, 0),
        )
        if row.identity and row.identity.key in seen_release_ids:
            continue
        rows.append(row)

    if convergence_signals:
        rows = [
            msgspec.structs.replace(
                row,
                convergence=convergence_signals.get(row.pipeline_id),
            )
            if row.pipeline_id is not None else row
            for row in rows
        ]
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
    if pipeline_db is None:
        library_albums = library_lookup.get_library_artist(artist_name, mb_artist_id)
        return build_library_artist_rows(
            library_albums=library_albums,
            pipeline_rows=[],
            track_counts={},
            rank_fn=rank_fn,
        )

    pipeline_rows = pipeline_db.list_requests_by_artist(artist_name, mb_artist_id)
    request_ids = [_request_id(row) for row in pipeline_rows]
    track_counts = pipeline_db.get_track_counts(request_ids) if request_ids else {}
    # Keep the original pipeline-first ordering so a concurrent import
    # that lands between the two reads still collapses onto the beets row.
    library_albums = library_lookup.get_library_artist(artist_name, mb_artist_id)
    artist_library_identity_keys = {
        identity_key
        for album in library_albums
        for identity_key in _library_identity_keys(album)
    }
    request_release_ids = [
        identity.release_id
        for row in pipeline_rows
        if (
            identity := ReleaseIdentity.from_strict_fields(
                row.get("mb_release_id"), row.get("discogs_release_id")
            )
        ) is not None
        and identity.key not in artist_library_identity_keys
    ]
    exact_library_albums = (
        library_lookup.get_library_releases(request_release_ids)
        if request_release_ids
        else []
    )
    library_by_album: dict[tuple[str, object], Mapping[str, object]] = {}
    unidentified_library_albums: list[Mapping[str, object]] = []
    for album in [*library_albums, *exact_library_albums]:
        identity_keys = _library_identity_keys(album)
        if not identity_keys:
            unidentified_library_albums.append(album)
        else:
            album_id = album.get("id")
            dedupe_key: tuple[str, object] = (
                ("id", album_id)
                if isinstance(album_id, int)
                else ("identity", identity_keys)
            )
            library_by_album[dedupe_key] = album
    merged_library_albums = [
        *unidentified_library_albums,
        *library_by_album.values(),
    ]
    displayed_release_ids = list(dict.fromkeys(
        identity.release_id
        for album in merged_library_albums
        for identity in _library_identities(album)
    ))
    pipeline_candidates: Sequence[Mapping[str, object]] = (
        pipeline_db.list_library_request_candidates(displayed_release_ids)
        if displayed_release_ids
        else []
    )
    displayed_request_ids = list(dict.fromkeys(
        _request_id(row) for row in [*pipeline_rows, *pipeline_candidates]
    ))
    convergence_signals = pipeline_db.get_convergence_signals(
        displayed_request_ids
    ) if displayed_request_ids else {}
    return build_library_artist_rows(
        library_albums=merged_library_albums,
        pipeline_rows=pipeline_rows,
        track_counts=track_counts,
        rank_fn=rank_fn,
        pipeline_candidates=pipeline_candidates,
        convergence_signals=convergence_signals,
    )
