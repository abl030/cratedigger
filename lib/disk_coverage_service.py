"""Pipeline-vs-beets disk coverage reporting.

Answers the operator question "which active pipeline rows are not uniquely
present in Beets?" without treating ``album_requests.status`` as disk state.
"""

from collections import Counter
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

import msgspec

from lib.beets_db import (
    CurrentBeetsAmbiguityReason,
    CurrentBeetsAmbiguous,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.release_identity import ReleaseIdentity

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow


class DiskCoverageRow(msgspec.Struct, kw_only=True):
    id: int
    status: str
    artist_name: str | None
    album_title: str | None
    mb_release_id: str | None
    discogs_release_id: str | None
    #: The row's real exact-release source ("musicbrainz" / "discogs"), or
    #: ``None`` if it names no valid identity at all. Derived from the
    #: VALUE's shape via ``ReleaseIdentity.from_strict_fields`` (#1089
    #: MAJOR-A, review round 3; N4, review round 4) — NEVER from which
    #: column is non-null: production Discogs rows duplicate the numeric id
    #: into BOTH ``mb_release_id`` and ``discogs_release_id`` (see
    #: ``ReleaseIdentity.from_strict_fields``'s own docstring), so
    #: ``mb_release_id`` truthiness alone falsely reads every Discogs-
    #: sourced drift row as MB-sourced. STRICT, not the lenient
    #: ``from_fields``: this must match ``MergeRekeyService.rekey_request``'s
    #: own admission test exactly, or a row with a real MB UUID plus a
    #: conflicting numeric Discogs id would render a button the service
    #: refuses (``from_fields`` picks ``mb_release_id`` without checking
    #: for a conflict; ``from_strict_fields`` fails closed to ``None``).
    source: str | None
    resolution: (
        "DiskCoverageMissingResolution | DiskCoverageAmbiguousResolution"
    )


class DiskCoverageMissingResolution(msgspec.Struct, kw_only=True):
    """No exact current Beets album was observed for the request."""

    kind: Literal["missing"] = "missing"


class DiskCoverageAmbiguousResolution(msgspec.Struct, kw_only=True):
    """Exact Beets membership exists but cannot name one current album."""

    kind: Literal["ambiguous"] = "ambiguous"
    album_ids: tuple[int, ...]
    reason: CurrentBeetsAmbiguityReason


class BeetsUntrackedAlbum(msgspec.Struct, kw_only=True):
    id: int
    album: str | None
    albumartist: str | None
    mb_albumid: str | None
    discogs_albumid: str | None


class DiskCoverageCounts(msgspec.Struct, kw_only=True):
    active_total: int
    on_disk_total: int
    off_disk_total: int
    by_status: dict[str, int]
    on_disk_by_status: dict[str, int]
    off_disk_by_status: dict[str, int]
    inverse_total: int | None = None


class DiskCoverageResult(msgspec.Struct, kw_only=True):
    counts: DiskCoverageCounts
    off_disk: list[DiskCoverageRow] | None = None
    inverse: list[BeetsUntrackedAlbum] | None = None


def _release_ids_for_request(row: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    for key in ("mb_release_id", "discogs_release_id"):
        value = row.get(key)
        if value is not None and str(value):
            ids.append(str(value))
    return ids


def _release_ids_for_beets_album(row: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for key in ("mb_albumid", "discogs_albumid"):
        value = row.get(key)
        if value is not None and str(value) and str(value) != "0":
            ids.add(str(value))
    return ids


def _request_row(
    row: Mapping[str, Any],
    resolution: DiskCoverageMissingResolution | DiskCoverageAmbiguousResolution,
) -> DiskCoverageRow:
    identity = ReleaseIdentity.from_strict_fields(
        row.get("mb_release_id"), row.get("discogs_release_id"),
    )
    return DiskCoverageRow(
        id=int(row["id"]),
        status=str(row.get("status") or ""),
        artist_name=row.get("artist_name"),
        album_title=row.get("album_title"),
        mb_release_id=(
            str(row["mb_release_id"])
            if row.get("mb_release_id") is not None else None
        ),
        discogs_release_id=(
            str(row["discogs_release_id"])
            if row.get("discogs_release_id") is not None else None
        ),
        source=identity.source if identity is not None else None,
        resolution=resolution,
    )


def _beets_row(row: dict[str, Any]) -> BeetsUntrackedAlbum:
    return BeetsUntrackedAlbum(
        id=int(row["id"]),
        album=row.get("album"),
        albumartist=row.get("albumartist"),
        mb_albumid=(
            str(row["mb_albumid"])
            if row.get("mb_albumid") is not None else None
        ),
        discogs_albumid=(
            str(row["discogs_albumid"])
            if row.get("discogs_albumid") is not None else None
        ),
    )


@runtime_checkable
class DiskCoveragePipelineDB(Protocol):
    """The PipelineDB surface disk_coverage uses (#409)."""

    def list_non_replaced_requests(self) -> "list[AlbumRequestRow]": ...


@runtime_checkable
class DiskCoverageBeetsDB(Protocol):
    """The BeetsDB surface disk_coverage uses (#409) — the first
    BeetsDB-side protocol; ``BeetsDB`` and ``FakeBeetsDB`` satisfy it
    structurally."""

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...

    def list_release_identities(self) -> list[dict[str, object]]: ...


def disk_coverage(
    pipeline_db: DiskCoveragePipelineDB,
    beets_db: DiskCoverageBeetsDB | None,
    *,
    include_rows: bool = True,
    include_inverse: bool = False,
) -> DiskCoverageResult:
    """Return exact-ID disk coverage for non-replaced pipeline rows.

    ``album_requests.status`` is intentionally ignored except for grouping.
    Presence is determined by one batched
    :meth:`BeetsDB.resolve_current_releases` observation of strict exact
    identities. A unique resolver result is on disk; missing and ambiguous
    results both remain in the historical off-disk count, with emitted rows
    retaining their distinct resolution evidence.
    """
    rows = pipeline_db.list_non_replaced_requests()
    request_ids: dict[int, set[str]] = {}
    request_identities: dict[int, ReleaseIdentity | None] = {}
    identities: list[ReleaseIdentity] = []
    for row in rows:
        release_ids = _release_ids_for_request(row)
        request_ids[int(row["id"])] = set(release_ids)
        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"), row.get("discogs_release_id"),
        )
        request_identities[int(row["id"])] = identity
        if identity is not None:
            identities.append(identity)

    resolutions = (
        beets_db.resolve_current_releases(identities) if beets_db else {}
    )

    by_status: Counter[str] = Counter()
    on_disk_by_status: Counter[str] = Counter()
    off_disk_by_status: Counter[str] = Counter()
    off_disk_rows: list[DiskCoverageRow] = []
    on_disk_total = 0

    for row in rows:
        status = str(row.get("status") or "")
        by_status[status] += 1
        identity = request_identities[int(row["id"])]
        current = resolutions.get(identity) if identity is not None else None
        if isinstance(current, CurrentBeetsUnique):
            on_disk_total += 1
            on_disk_by_status[status] += 1
        else:
            off_disk_by_status[status] += 1
            if include_rows:
                resolution: (
                    DiskCoverageMissingResolution
                    | DiskCoverageAmbiguousResolution
                )
                if isinstance(current, CurrentBeetsAmbiguous):
                    resolution = DiskCoverageAmbiguousResolution(
                        album_ids=current.album_ids,
                        reason=current.reason,
                    )
                else:
                    resolution = DiskCoverageMissingResolution()
                off_disk_rows.append(_request_row(row, resolution))

    inverse_rows: list[BeetsUntrackedAlbum] | None = None
    if include_inverse:
        inverse_rows = []
        pipeline_release_ids = {
            release_id for ids in request_ids.values() for release_id in ids
        }
        for row in beets_db.list_release_identities() if beets_db else []:
            if not (_release_ids_for_beets_album(row) & pipeline_release_ids):
                inverse_rows.append(_beets_row(row))

    counts = DiskCoverageCounts(
        active_total=len(rows),
        on_disk_total=on_disk_total,
        off_disk_total=len(rows) - on_disk_total,
        by_status=dict(sorted(by_status.items())),
        on_disk_by_status=dict(sorted(on_disk_by_status.items())),
        off_disk_by_status=dict(sorted(off_disk_by_status.items())),
        inverse_total=len(inverse_rows) if inverse_rows is not None else None,
    )
    return DiskCoverageResult(
        counts=counts,
        off_disk=off_disk_rows if include_rows else None,
        inverse=inverse_rows,
    )
