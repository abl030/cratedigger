"""Pipeline-vs-beets disk coverage reporting.

Answers the operator question "which active pipeline rows are not actually
present in beets?" without treating ``album_requests.status`` as disk state.
"""

from collections import Counter
from typing import Any, Protocol, runtime_checkable

import msgspec


class DiskCoverageRow(msgspec.Struct, kw_only=True):
    id: int
    status: str
    artist_name: str | None
    album_title: str | None
    mb_release_id: str | None
    discogs_release_id: str | None


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


def _release_ids_for_request(row: dict[str, Any]) -> list[str]:
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


def _request_row(row: dict[str, Any]) -> DiskCoverageRow:
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

    def list_non_replaced_requests(self) -> list[dict[str, Any]]: ...


@runtime_checkable
class DiskCoverageBeetsDB(Protocol):
    """The BeetsDB surface disk_coverage uses (#409) — the first
    BeetsDB-side protocol; ``BeetsDB`` and ``FakeBeetsDB`` satisfy it
    structurally."""

    def check_mbids(self, mbids: list[str]) -> set[str]: ...

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
    Presence is determined by exact beets identity: MB UUIDs in
    ``albums.mb_albumid`` and Discogs numerics in either ``discogs_albumid``
    or legacy ``mb_albumid`` via ``BeetsDB.check_mbids``.
    """
    rows = pipeline_db.list_non_replaced_requests()
    request_ids: dict[int, set[str]] = {}
    all_release_ids: list[str] = []
    for row in rows:
        release_ids = _release_ids_for_request(row)
        request_ids[int(row["id"])] = set(release_ids)
        all_release_ids.extend(release_ids)

    matched_ids = set(beets_db.check_mbids(all_release_ids)) if beets_db else set()

    by_status: Counter[str] = Counter()
    on_disk_by_status: Counter[str] = Counter()
    off_disk_by_status: Counter[str] = Counter()
    off_disk_rows: list[DiskCoverageRow] = []
    on_disk_total = 0

    for row in rows:
        status = str(row.get("status") or "")
        by_status[status] += 1
        on_disk = bool(request_ids[int(row["id"])] & matched_ids)
        if on_disk:
            on_disk_total += 1
            on_disk_by_status[status] += 1
        else:
            off_disk_by_status[status] += 1
            if include_rows:
                off_disk_rows.append(_request_row(row))

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
