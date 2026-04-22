"""Owned /api/library/artist album-row contract."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import cast

import msgspec

from lib.release_identity import ReleaseIdentity, detect_release_source, frontend_release_id


def _pipeline_upgrade_queued(row: Mapping[str, object] | None) -> bool:
    return bool(
        row
        and row.get("status") == "wanted"
        and (row.get("search_filetype_override") or row.get("target_format"))
    )


def _timestamp(value: object | None) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    raise TypeError(
        f"LibraryAlbumRow timestamp fields must be datetime|float|int, got {type(value).__name__}"
    )


def _bitrate_kbps(min_bitrate_bps: object | None) -> int | None:
    if isinstance(min_bitrate_bps, int):
        return min_bitrate_bps // 1000
    return None


class LibraryAlbumRow(msgspec.Struct, frozen=True):
    """Typed wire contract for `/api/library/artist` `albums[]`.

    Field notes the frontend depends on:
    - ``mb_albumid`` is the single release key the UI uses for actions. It
      intentionally carries either a MusicBrainz UUID or a Discogs numeric
      release ID string.
    - ``release_group_title`` is always populated. When the pipeline has no
      real release-group title yet, we mirror ``album`` so library grouping
      stays stable.
    - ``library_rank`` is only meaningful for beets-backed rows. Pipeline-only
      rows carry ``None`` to keep the schema uniform.
    - ``source`` keeps the historical row-provenance semantics: beets-backed
      rows expose release origin (``musicbrainz`` / ``discogs`` / ``unknown``),
      while pipeline-only rows mirror the pipeline request source
      (``request`` / ``redownload``), with ``"unknown"`` as the explicit
      fallback when a malformed pipeline row has no source.
    """

    id: int
    album: str
    artist: str
    year: int | None
    mb_albumid: str | None
    track_count: int
    mb_releasegroupid: str | None
    release_group_title: str
    added: float
    formats: str
    min_bitrate: int | None
    type: str
    label: str
    country: str | None
    source: str
    in_library: bool
    beets_album_id: int | None
    pipeline_status: str | None
    pipeline_id: int | None
    upgrade_queued: bool
    library_rank: str | None

    @property
    def identity(self) -> ReleaseIdentity | None:
        # ``mb_albumid`` is already the canonical frontend release key; both
        # constructors collapse MB/discogs source IDs into this one field.
        return ReleaseIdentity.from_id(self.mb_albumid)

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], msgspec.to_builtins(self))

    @classmethod
    def from_beets_album(
        cls,
        album: Mapping[str, object],
        *,
        rank_fn: Callable[[str | None, int | None], str],
    ) -> "LibraryAlbumRow":
        frontend_id = frontend_release_id(
            album.get("mb_albumid"),
            album.get("discogs_albumid"),
        )
        formats = str(album.get("formats") or "")
        min_bitrate = album.get("min_bitrate")
        return msgspec.convert(
            {
                "id": album["id"],
                "album": album["album"],
                "artist": album["artist"],
                "year": album.get("year"),
                "mb_albumid": frontend_id,
                "track_count": album["track_count"],
                "mb_releasegroupid": album.get("mb_releasegroupid"),
                "release_group_title": album.get("release_group_title") or album["album"],
                "added": _timestamp(album.get("added")),
                "formats": formats,
                "min_bitrate": min_bitrate,
                "type": str(album.get("type") or ""),
                "label": str(album.get("label") or ""),
                "country": album.get("country"),
                "source": detect_release_source(frontend_id),
                "in_library": True,
                "beets_album_id": album["id"],
                "pipeline_status": None,
                "pipeline_id": None,
                "upgrade_queued": False,
                "library_rank": rank_fn(formats, _bitrate_kbps(min_bitrate)),
            },
            type=cls,
        )

    @classmethod
    def from_beets_album_with_pipeline(
        cls,
        album: Mapping[str, object],
        *,
        pipeline_row: Mapping[str, object] | None,
        rank_fn: Callable[[str | None, int | None], str],
    ) -> "LibraryAlbumRow":
        return cls.from_beets_album(album, rank_fn=rank_fn).with_pipeline_request(
            pipeline_row
        )

    @classmethod
    def from_pipeline_request(
        cls,
        row: Mapping[str, object],
        *,
        track_count: int,
    ) -> "LibraryAlbumRow":
        release_id = frontend_release_id(
            row.get("mb_release_id"),
            row.get("discogs_release_id"),
        )
        min_bitrate = row.get("min_bitrate")
        return msgspec.convert(
            {
                "id": row["id"],
                "album": row["album_title"],
                "artist": row["artist_name"],
                "year": row.get("year"),
                "mb_albumid": release_id,
                "track_count": track_count,
                "mb_releasegroupid": row.get("mb_release_group_id"),
                "release_group_title": row["album_title"],
                "added": _timestamp(row.get("created_at")),
                "formats": str(row.get("format") or ""),
                "min_bitrate": min_bitrate * 1000 if isinstance(min_bitrate, int) else None,
                "type": "album",
                "label": "",
                "country": row.get("country"),
                "source": str(row.get("source") or "unknown"),
                "in_library": False,
                "beets_album_id": None,
                "pipeline_status": row.get("status"),
                "pipeline_id": row["id"],
                "upgrade_queued": _pipeline_upgrade_queued(row),
                "library_rank": None,
            },
            type=cls,
        )

    def with_pipeline_request(
        self,
        pipeline_row: Mapping[str, object] | None,
    ) -> "LibraryAlbumRow":
        if not pipeline_row:
            return self
        row = self.to_dict()
        row["pipeline_status"] = pipeline_row.get("status")
        row["pipeline_id"] = pipeline_row["id"]
        row["upgrade_queued"] = _pipeline_upgrade_queued(pipeline_row)
        return msgspec.convert(row, type=type(self))
