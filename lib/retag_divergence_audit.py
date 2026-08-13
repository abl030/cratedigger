"""Read-only census of the retag ``-W`` divergence cohort (#1093 item 1).

The import-time MusicBrainz merge retag (`lib/beets_retag.py`) runs
``beet modify -a -M -W -y``. ``-W`` is deliberate and stays: it keeps the
retag to exactly one ``Album.store()`` transaction instead of a partial
per-file write race (see ``lib/beets_retag.py``'s own module docstring). The
accepted cost is that a successful retag moves the Beets DB's ``mb_albumid``
without touching any installed file's tag — so after a successful retag
whose subsequent import REJECTS, the DB names the survivor while every
installed file still carries the merged-away id. Nothing normally re-derives
that split: if anything later advances a track's mtime and an operator runs
``beet update``, beets copies the (still-stale) file tag back onto the ALBUM
row for every ``Album.item_keys`` field, ``mb_albumid`` included — reverting
the DB to the merged-away id while the pipeline's request row still names
the survivor. The next import at the survivor then lands a SECOND album and
routes through ``import_no_exist``, silently skipping the downgrade guard.

This module does not fix that — it makes the cohort visible. It is a
read-only census: "every Beets album whose DB ``mb_albumid`` identity
disagrees with what its installed files' tags actually say." It never
writes to Beets, PostgreSQL, or the filesystem, and it never decides
anything; an operator interprets and acts on the report.

Per-item classification (:data:`RetagDivergenceItemClass`):

- ``agrees``: the DB and the (readable) file tag name the same identity, or
  both are empty. The common case; not reported.
- ``diverges``: the DB names an identity and the file tag does not match it
  (including an empty file tag) — the ``-W`` residual shape this instrument
  exists to surface.
- ``file_tag_present_db_absent``: the DB has no ``mb_albumid`` at all (a
  Discogs-sourced album, or one whose MB id was explicitly neutralized) but
  the file tag still carries a nonempty value. #570's Discogs neutralization
  backfill ran WITHOUT ``-W`` (see ``.claude/memory/project_570_...``), so
  this bucket is expected to be near-zero — measured honestly, never
  assumed or filtered.
- ``unreadable``: the file was never verified to agree. Two distinct causes
  share this class, distinguishable by ``detail``: a genuine read/parse
  failure, and a stored path that resolved outside the configured library
  root (refused before any read was attempted — #1093 review finding 7;
  ``lib/beets_db.py::BeetsDB.list_album_mb_identities`` never hands this
  module a path it has not verified contained). Fail closed either way: an
  unreadable file is never counted as agreeing, whatever the DB says.

Per-album classification (:data:`RetagDivergenceAlbumClass`) aggregates its
items by a fixed DISPLAY precedence — ``unreadable`` > ``diverges`` >
``file_tag_present_db_absent`` > ``agrees`` — plus ``empty`` for the real,
reachable zero-item album row (T7 in ``tests/test_beets_retag.py``). This is
"the single worst fact about this album" for the listing; it is deliberately
NOT what decides the report's ``status`` (see below) — an album a single
unreadable file makes DISPLAY as ``unreadable`` might still contain a
genuine ``diverges`` item, and the report's headline answer must not miss
that (#1093 review findings 3 and 4). Only non-agreeing albums are listed;
full counts are always reported regardless.

``status`` answers "does this report contain a genuine identity mismatch",
independent of unreadable/empty/excluded findings that merely mean the
census could not fully vouch for some albums:

- ``clean``: nothing listed at all.
- ``divergence_found``: at least one album contains an item classified
  ``diverges`` or ``file_tag_present_db_absent`` — a real identity
  disagreement. This is decided independent of precedence and of whatever
  ELSE is wrong with that album (or any other), so a permission error on
  one album's files can never mask a genuine divergence found elsewhere,
  and can never manufacture one either.
- ``incomplete``: no genuine divergence anywhere, but the scan could not
  fully vouch for the library — at least one album is listed solely for a
  non-divergence reason (unreadable, empty, or refused-path items), or the
  scan itself was time-bounded and stopped before finishing (``complete``
  is ``False`` in that case; see ``deadline_seconds``).
- ``beets_unavailable``: the Beets authority itself could not be opened or
  queried.

``deadline_seconds`` bounds a scan's wall-clock time (#1093 review finding
2): the API route passes one so a single HTTP request can never outlive a
reverse proxy's read timeout on the full ~93k-item live library (measured:
an unbounded full census took ~196s; nginx's ``proxy_read_timeout`` default
is 60s). ``pipeline-cli audit retag-divergence`` passes none — the CLI is
always the full, unbounded census. A bounded scan that runs out of time
reports ``complete=False`` over the albums it reached, exactly like an
unavailable Beets authority; the report SHAPE never changes.
"""

from __future__ import annotations

import importlib
import time
from collections.abc import Callable, Sequence
from contextlib import closing
from typing import Literal, Protocol, runtime_checkable

import msgspec

from lib.beets_db import BeetsAlbumIdentityRow, beets_authority_availability_category
from lib.release_identity import normalize_release_id

RetagDivergenceItemClass = Literal[
    "agrees", "diverges", "file_tag_present_db_absent", "unreadable",
]

RetagDivergenceAlbumClass = Literal[
    "agrees", "diverges", "file_tag_present_db_absent", "unreadable", "empty",
]

RetagDivergenceStatus = Literal[
    "clean", "divergence_found", "incomplete", "beets_unavailable",
]

#: Worst-first DISPLAY precedence — see the module docstring for why this is
#: NOT what decides ``status``.
_ALBUM_CLASS_PRECEDENCE: tuple[RetagDivergenceItemClass, ...] = (
    "unreadable", "diverges", "file_tag_present_db_absent",
)

#: The two item classes that mean a genuine identity mismatch, as opposed to
#: "the census could not vouch for this file/album" (unreadable, empty).
_DIVERGENT_ITEM_CLASSES: frozenset[RetagDivergenceItemClass] = frozenset({
    "diverges", "file_tag_present_db_absent",
})

#: Fixed detail text for a path refused by containment — never a raised
#: exception, since the file is never opened (#1093 review finding 7).
REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL: Literal[
    "refused: stored path resolves outside the configured library root"
] = "refused: stored path resolves outside the configured library root"


class TagReadOk(msgspec.Struct, frozen=True):
    """A file's tag was read successfully."""

    mb_albumid: str


class TagReadUnreadable(msgspec.Struct, frozen=True):
    """A file's tag could not be read — fail closed, never "agrees"."""

    detail: str


type TagReadOutcome = TagReadOk | TagReadUnreadable


def read_mb_albumid_tag(path: str) -> str:
    """Real leaf reader: the installed file's own ``mb_albumid`` tag.

    Raises on any read/parse failure (corrupt file, unsupported format, a
    world failure such as EIO/ESTALE) — the caller wraps every call and
    converts a raised exception into :class:`TagReadUnreadable`, so this
    function stays a total-looking leaf that is honest about failing.
    Never invoked for a path :meth:`BeetsDB.list_album_mb_identities` has
    already refused for containment.

    Resolved via ``importlib`` rather than a top-level ``import mediafile``:
    unlike ``beets`` itself, the ``mediafile`` package ships no type stubs,
    and a static import would trip strict Pyright's missing-stub check for
    every consumer of this module. ``getattr`` keeps the untyped surface
    (the class, then the field) from propagating Unknown into this file —
    the same idiom ``lib/beets_distance.py::_item_field`` uses for beets'
    own untyped ``Item.get``.
    """
    media_file_cls = getattr(  # noqa: B009 - mediafile ships no type stubs
        importlib.import_module("mediafile"), "MediaFile",
    )
    media = media_file_cls(path)
    raw = getattr(media, "mb_albumid")  # noqa: B009 - upstream field is untyped
    return str(raw or "")


def _read_tag_outcome(
    read_tag: Callable[[str], str], path: str,
) -> TagReadOutcome:
    try:
        return TagReadOk(mb_albumid=read_tag(path))
    except Exception as exc:  # noqa: BLE001 - leaf boundary, fail closed
        return TagReadUnreadable(detail=f"{type(exc).__name__}: {exc}")


def classify_retag_divergence_item(
    *, db_mb_albumid: str, file_tag: TagReadOutcome,
) -> RetagDivergenceItemClass:
    """Pure per-item classifier — the one place this comparison happens."""
    if isinstance(file_tag, TagReadUnreadable):
        return "unreadable"
    db_norm = normalize_release_id(db_mb_albumid)
    file_norm = normalize_release_id(file_tag.mb_albumid)
    if not db_norm:
        return "file_tag_present_db_absent" if file_norm else "agrees"
    return "agrees" if file_norm == db_norm else "diverges"


class RetagDivergenceItem(msgspec.Struct, frozen=True):
    """One installed file's tag classified against its album's DB identity."""

    path: str
    item_class: RetagDivergenceItemClass
    #: Populated iff ``item_class != "unreadable"`` — the tag as read.
    file_mb_albumid: str | None
    #: Populated iff ``item_class == "unreadable"`` — why: a read/parse
    #: failure's exception text, or :data:`REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL`
    #: when the path was refused before any read was attempted.
    detail: str | None


def album_class_from_items(
    items: Sequence[RetagDivergenceItem],
) -> RetagDivergenceAlbumClass:
    """Aggregate one album's DISPLAY class from its items by fixed
    precedence. NOT the report's ``status`` signal — see module docstring."""
    if not items:
        return "empty"
    present = {item.item_class for item in items}
    for candidate in _ALBUM_CLASS_PRECEDENCE:
        if candidate in present:
            return candidate
    return "agrees"


def _has_divergent_item(items: Sequence[RetagDivergenceItem]) -> bool:
    return any(item.item_class in _DIVERGENT_ITEM_CLASSES for item in items)


class RetagDivergenceAlbum(msgspec.Struct, frozen=True):
    """One non-agreeing album — listed with every item's classification."""

    album_id: int
    db_mb_albumid: str
    album_class: RetagDivergenceAlbumClass
    item_count: int
    items: tuple[RetagDivergenceItem, ...]


class RetagDivergenceCounts(msgspec.Struct, frozen=True):
    albums_scanned: int
    items_read: int
    items_unreadable: int
    #: Albums containing >=1 ``diverges`` item — an INDEPENDENT presence
    #: count, not gated by the album's DISPLAY precedence class (#1093
    #: review finding 4: an album with both an unreadable item and a
    #: diverging one must still count here, even though its display class
    #: reads ``unreadable``).
    albums_diverging: int
    #: Same independence as ``albums_diverging``, for
    #: ``file_tag_present_db_absent``.
    albums_file_tag_present_db_absent: int
    #: Albums whose DISPLAY class is ``unreadable`` (top precedence, so this
    #: one count IS exactly "contains >=1 unreadable item" too).
    albums_unreadable: int
    albums_empty: int


class RetagDivergenceReport(msgspec.Struct, frozen=True):
    status: RetagDivergenceStatus
    complete: bool
    counts: RetagDivergenceCounts
    #: Only non-agreeing albums — sorted by ``album_id`` for determinism.
    albums: tuple[RetagDivergenceAlbum, ...]
    #: Populated iff ``status == "beets_unavailable"``.
    unavailable_detail: str | None = None


def _build_album(
    row: BeetsAlbumIdentityRow, *, read_tag: Callable[[str], str],
) -> RetagDivergenceAlbum:
    read_items = tuple(
        _build_item(row.mb_albumid, path, read_tag=read_tag)
        for path in row.item_paths
    )
    refused_items = tuple(
        RetagDivergenceItem(
            path=path, item_class="unreadable",
            file_mb_albumid=None,
            detail=REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL,
        )
        for path in row.refused_paths
    )
    items = read_items + refused_items
    return RetagDivergenceAlbum(
        album_id=row.album_id,
        db_mb_albumid=row.mb_albumid,
        album_class=album_class_from_items(items),
        item_count=len(items),
        items=items,
    )


def _build_item(
    db_mb_albumid: str, path: str, *, read_tag: Callable[[str], str],
) -> RetagDivergenceItem:
    outcome = _read_tag_outcome(read_tag, path)
    item_class = classify_retag_divergence_item(
        db_mb_albumid=db_mb_albumid, file_tag=outcome,
    )
    if isinstance(outcome, TagReadUnreadable):
        return RetagDivergenceItem(
            path=path, item_class=item_class,
            file_mb_albumid=None, detail=outcome.detail,
        )
    return RetagDivergenceItem(
        path=path, item_class=item_class,
        file_mb_albumid=outcome.mb_albumid, detail=None,
    )


@runtime_checkable
class RetagDivergenceBeetsDB(Protocol):
    def list_album_mb_identities(self) -> list[BeetsAlbumIdentityRow]: ...


class OwnedRetagDivergenceBeetsDB(RetagDivergenceBeetsDB, Protocol):
    def close(self) -> None: ...


type RetagDivergenceBeetsFactory = Callable[[], OwnedRetagDivergenceBeetsDB]


def scan_retag_divergence(
    beets: RetagDivergenceBeetsDB,
    *,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    deadline_seconds: float | None = None,
    time_fn: Callable[[], float] = time.monotonic,
) -> RetagDivergenceReport:
    """Census every Beets album's DB identity against its file tags.

    Pure composition over an already-open, already-readable ``beets``
    handle — no availability mediation here; see
    :func:`scan_retag_divergence_from_factory` /
    :func:`scan_retag_divergence_from_borrowed_factory` for that.

    ``deadline_seconds=None`` (the CLI default) never truncates. A configured
    deadline is checked once per album, before that album's items are read —
    coarse-grained, but albums are small (rarely >20 items) so overshoot is
    bounded. A truncated scan reports ``complete=False`` over exactly the
    albums it reached; ``albums_scanned``/``items_read``/etc. describe only
    that reached prefix, never the full row count.
    """
    rows = sorted(beets.list_album_mb_identities(), key=lambda row: row.album_id)
    deadline = None if deadline_seconds is None else time_fn() + deadline_seconds

    built: list[RetagDivergenceAlbum] = []
    truncated = False
    for row in rows:
        if deadline is not None and time_fn() >= deadline:
            truncated = True
            break
        built.append(_build_album(row, read_tag=read_tag))

    items_read = sum(album.item_count for album in built)
    items_unreadable = sum(
        1
        for album in built
        for item in album.items
        if item.item_class == "unreadable"
    )
    albums = tuple(album for album in built if album.album_class != "agrees")
    has_divergence = any(_has_divergent_item(album.items) for album in albums)
    counts = RetagDivergenceCounts(
        albums_scanned=len(built),
        items_read=items_read,
        items_unreadable=items_unreadable,
        albums_diverging=sum(
            1 for album in albums
            if any(item.item_class == "diverges" for item in album.items)),
        albums_file_tag_present_db_absent=sum(
            1 for album in albums
            if any(
                item.item_class == "file_tag_present_db_absent"
                for item in album.items
            )),
        albums_unreadable=sum(
            1 for album in albums if album.album_class == "unreadable"),
        albums_empty=sum(1 for album in albums if album.album_class == "empty"),
    )
    if has_divergence:
        status: RetagDivergenceStatus = "divergence_found"
    elif albums or truncated:
        status = "incomplete"
    else:
        status = "clean"
    return RetagDivergenceReport(
        status=status,
        complete=not truncated,
        counts=counts,
        albums=albums,
    )


def _empty_counts() -> RetagDivergenceCounts:
    return RetagDivergenceCounts(0, 0, 0, 0, 0, 0, 0)


def _unavailable_report(category: str) -> RetagDivergenceReport:
    return RetagDivergenceReport(
        status="beets_unavailable",
        complete=False,
        counts=_empty_counts(),
        albums=(),
        unavailable_detail=f"current Beets authority unavailable ({category})",
    )


class _BeetsAuthorityUnavailable(Exception):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def _open_beets_authority(
    beets_factory: RetagDivergenceBeetsFactory,
) -> OwnedRetagDivergenceBeetsDB | _BeetsAuthorityUnavailable:
    try:
        return beets_factory()
    except Exception as exc:
        category = beets_authority_availability_category(exc)
        if category is None:
            raise
        return _BeetsAuthorityUnavailable(category)


def _scan_with_mediated_beets(
    beets: RetagDivergenceBeetsDB,
    *,
    read_tag: Callable[[str], str],
    deadline_seconds: float | None,
    time_fn: Callable[[], float],
) -> RetagDivergenceReport | _BeetsAuthorityUnavailable:
    try:
        return scan_retag_divergence(
            beets, read_tag=read_tag,
            deadline_seconds=deadline_seconds, time_fn=time_fn,
        )
    except Exception as exc:
        category = beets_authority_availability_category(exc)
        if category is None:
            raise
        return _BeetsAuthorityUnavailable(category)


def scan_retag_divergence_from_factory(
    beets_factory: RetagDivergenceBeetsFactory,
    *,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    deadline_seconds: float | None = None,
    time_fn: Callable[[], float] = time.monotonic,
) -> RetagDivergenceReport:
    """Own Beets open/query/close; type only expected unavailability."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_report(opened.category)
    with closing(opened):
        result = _scan_with_mediated_beets(
            opened, read_tag=read_tag,
            deadline_seconds=deadline_seconds, time_fn=time_fn,
        )
    if isinstance(result, RetagDivergenceReport):
        return result
    return _unavailable_report(result.category)


def scan_retag_divergence_from_borrowed_factory(
    beets_factory: RetagDivergenceBeetsFactory,
    *,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    deadline_seconds: float | None = None,
    time_fn: Callable[[], float] = time.monotonic,
) -> RetagDivergenceReport:
    """Mediate a server-owned Beets handle without closing its lifecycle."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_report(opened.category)
    result = _scan_with_mediated_beets(
        opened, read_tag=read_tag,
        deadline_seconds=deadline_seconds, time_fn=time_fn,
    )
    if isinstance(result, RetagDivergenceReport):
        return result
    return _unavailable_report(result.category)


__all__ = [
    "REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL",
    "OwnedRetagDivergenceBeetsDB",
    "RetagDivergenceAlbum",
    "RetagDivergenceAlbumClass",
    "RetagDivergenceBeetsDB",
    "RetagDivergenceBeetsFactory",
    "RetagDivergenceCounts",
    "RetagDivergenceItem",
    "RetagDivergenceItemClass",
    "RetagDivergenceReport",
    "RetagDivergenceStatus",
    "TagReadOk",
    "TagReadOutcome",
    "TagReadUnreadable",
    "album_class_from_items",
    "classify_retag_divergence_item",
    "read_mb_albumid_tag",
    "scan_retag_divergence",
    "scan_retag_divergence_from_borrowed_factory",
    "scan_retag_divergence_from_factory",
]
