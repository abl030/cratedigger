"""Owned /api/library/artist album-row contract."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import msgspec

from lib.convergence_service import ConvergenceSignal
from lib.pipeline_db._shared import ProcessingOwnerProjection
from lib.release_identity import (
    ReleaseIdentity,
    detect_release_source,
)


def _pipeline_upgrade_queued(row: Mapping[str, object] | None) -> bool:
    """Whether a pipeline request row is a wanted quality upgrade.

    The one owner for this decision (issue #1355 item 6): both the list-row
    projection here and `web/library_album_detail_service.py`'s detail
    projection call this function rather than each spelling the check.
    """
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


def _bitrate_kbps(bitrate_bps: object | None) -> int:
    if isinstance(bitrate_bps, int):
        return bitrate_bps // 1000
    return 0


def _pipeline_fact(row: Mapping[str, object], key: str) -> bool:
    value = row[key]
    if isinstance(value, bool):
        return value
    raise TypeError(
        f"LibraryAlbumRow pipeline fact {key!r} must be bool, "
        f"got {type(value).__name__}"
    )


def _pipeline_facts(
    row: Mapping[str, object],
) -> tuple[bool, bool, bool]:
    """Map the three independent request facts through one typed seam."""
    return (
        _pipeline_fact(row, "has_captured_history"),
        _pipeline_fact(row, "verified_lossless"),
        _pipeline_fact(row, "provisional_lossless"),
    )


@dataclass(frozen=True)
class ExactLibraryRequestAttachment:
    """One exact request selected for a Beets library observation."""

    identity: ReleaseIdentity
    request: Mapping[str, object]

    @property
    def request_id(self) -> int:
        request_id = self.request.get("id")
        if not isinstance(request_id, int):
            raise TypeError(
                "exact library request attachment must carry an int id, "
                f"got {type(request_id).__name__}"
            )
        return request_id


class AmbiguousLibraryRequestAttachmentError(ValueError):
    """More than one exact request identity matches one Beets album."""

    def __init__(self, matches: tuple[ExactLibraryRequestAttachment, ...]) -> None:
        self.matches = matches
        self.identities = tuple(match.identity for match in matches)
        self.request_ids = tuple(match.request_id for match in matches)
        labels = ", ".join(
            f"{identity.source}:{identity.release_id}"
            for identity in self.identities
        )
        super().__init__(f"multiple exact library request attachments: {labels}")


def select_exact_library_request_attachment(
    observation_identities: tuple[ReleaseIdentity, ...],
    requests_by_identity: Mapping[
        tuple[str, str],
        Sequence[Mapping[str, object]],
    ],
) -> ExactLibraryRequestAttachment | None:
    """Select the only exact request observed on a Beets album.

    A primary Beets tag has no authority over another observed source. Zero
    exact request matches leaves the album untracked; two matches are an
    ambiguity and must not silently choose either request.
    """
    matches = tuple(
        ExactLibraryRequestAttachment(identity=identity, request=request)
        for identity in observation_identities
        for request in requests_by_identity.get(identity.key, ())
    )
    if len(matches) > 1:
        raise AmbiguousLibraryRequestAttachmentError(matches)
    return matches[0] if matches else None


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
    - ``min_bitrate`` remains the bps floor. ``avg_bitrate`` is the positive-
      track mean used for current-state badges and ``library_rank``.
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
    avg_bitrate: int | None
    type: str
    label: str
    country: str | None
    source: str
    in_library: bool
    beets_album_id: int | None
    pipeline_status: str | None
    pipeline_id: int | None
    processing_owner: ProcessingOwnerProjection | None
    upgrade_queued: bool
    library_rank: str | None
    has_captured_history: bool
    pipeline_verified_lossless: bool
    pipeline_provisional: bool
    convergence: ConvergenceSignal | None = None

    @property
    def identity(self) -> ReleaseIdentity | None:
        # ``mb_albumid`` is already the canonical frontend release key; both
        # constructors collapse MB/discogs source IDs into this one field.
        return ReleaseIdentity.from_id(self.mb_albumid)

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)

    @classmethod
    def from_beets_album(
        cls,
        album: Mapping[str, object],
        *,
        rank_fn: Callable[[str | None, int | None], str],
    ) -> LibraryAlbumRow:
        identities = ReleaseIdentity.all_from_observation_fields(
            album.get("mb_albumid"),
            album.get("discogs_albumid"),
        )
        frontend_id = identities[0].release_id if identities else None
        formats = str(album.get("formats") or "")
        min_bitrate = album.get("min_bitrate")
        avg_bitrate = album.get("avg_bitrate")
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
                "avg_bitrate": avg_bitrate,
                "type": str(album.get("type") or ""),
                "label": str(album.get("label") or ""),
                "country": album.get("country"),
                "source": detect_release_source(frontend_id),
                "in_library": True,
                "beets_album_id": album["id"],
                "pipeline_status": None,
                "pipeline_id": None,
                "processing_owner": None,
                "upgrade_queued": False,
                "library_rank": rank_fn(formats, _bitrate_kbps(avg_bitrate)),
                "has_captured_history": False,
                "pipeline_verified_lossless": False,
                "pipeline_provisional": False,
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
        attached_identity: ReleaseIdentity | None = None,
    ) -> LibraryAlbumRow:
        row = cls.from_beets_album(album, rank_fn=rank_fn)
        if pipeline_row is None:
            return row

        observation_identities = ReleaseIdentity.all_from_observation_fields(
            album.get("mb_albumid"),
            album.get("discogs_albumid"),
        )
        if attached_identity not in observation_identities:
            raise ValueError(
                "attached pipeline identity is not observed on Beets album"
            )

        overlaid = row.with_pipeline_request(pipeline_row)
        assert attached_identity is not None
        return msgspec.structs.replace(
            overlaid,
            mb_albumid=attached_identity.release_id,
            source=attached_identity.source,
        )

    @classmethod
    def from_pipeline_request(
        cls,
        row: Mapping[str, object],
        *,
        track_count: int,
    ) -> LibraryAlbumRow:
        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"),
            row.get("discogs_release_id"),
        )
        release_id = identity.release_id if identity is not None else None
        min_bitrate = row.get("min_bitrate")
        captured, verified, provisional = _pipeline_facts(row)
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
                "avg_bitrate": None,
                "type": "album",
                "label": "",
                "country": row.get("country"),
                "source": (
                    str(row.get("source") or "unknown")
                    if identity is not None
                    else "unknown"
                ),
                "in_library": False,
                "beets_album_id": None,
                "pipeline_status": row.get("status"),
                "pipeline_id": row["id"],
                "processing_owner": row.get("processing_owner"),
                "upgrade_queued": _pipeline_upgrade_queued(row),
                "library_rank": None,
                "has_captured_history": captured,
                "pipeline_verified_lossless": verified,
                "pipeline_provisional": provisional,
            },
            type=cls,
        )

    def with_pipeline_request(
        self,
        pipeline_row: Mapping[str, object] | None,
    ) -> LibraryAlbumRow:
        if not pipeline_row:
            return self
        row = self.to_dict()
        row["pipeline_status"] = pipeline_row.get("status")
        row["pipeline_id"] = pipeline_row["id"]
        row["processing_owner"] = pipeline_row.get("processing_owner")
        row["upgrade_queued"] = _pipeline_upgrade_queued(pipeline_row)
        captured, verified, provisional = _pipeline_facts(pipeline_row)
        row["has_captured_history"] = captured
        row["pipeline_verified_lossless"] = verified
        row["pipeline_provisional"] = provisional
        return msgspec.convert(row, type=type(self))
