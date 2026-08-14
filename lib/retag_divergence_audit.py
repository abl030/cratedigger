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
  failure, and a stored path REFUSED by containment before any read was
  attempted (#1093 review round 2, finding 7 — see
  ``lib/beets_db.py::BeetsDB._contained_or_refused``; this module never
  calls ``read_tag`` for a refused path). Fail closed either way: an
  unreadable file is never counted as agreeing, whatever the DB says.
  Containment is a LEXICAL path-string check (``os.path.abspath`` +
  ``os.path.commonpath``), not a symlink-resolving one — a symlink INSIDE
  the library root that points outside it is CONTAINED (and gets opened);
  a library root that is itself a symlink refuses an absolute path naming
  the real target. This matches the pre-existing
  ``BeetsDB.resolve_current_releases`` mechanism deliberately; see
  :data:`REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL`.

Per-album classification (:data:`RetagDivergenceAlbumClass`) aggregates its
items by a fixed DISPLAY precedence — ``unreadable`` > ``diverges`` >
``file_tag_present_db_absent`` > ``agrees`` — plus ``empty`` for the real,
reachable zero-item album row (T7 in ``tests/test_beets_retag.py``). This is
"the single worst fact about this album" for the listing; it is deliberately
NOT what decides the report's ``status`` (see below) — an album a single
unreadable file makes DISPLAY as ``unreadable`` might still contain a
genuine ``diverges`` item, and the report's headline answer must not miss
that (#1093 review round 2, findings 3 and 4). Only non-agreeing albums are
listed; full counts are always reported regardless.

``status`` answers "does this report contain a genuine identity mismatch",
independent of unreadable/empty/refused findings that merely mean the
census could not fully vouch for some albums:

- ``clean``: nothing listed at all, this call started from the TRUE
  beginning of the library (``after_album_id is None`` on input — see
  below), AND it ran to completion (``complete`` is ``True``). Only a call
  meeting all three conditions has actually answered the whole-library
  question; a single response is never allowed to claim more than it
  itself computed (#1093 review round 5, finding 1).
- ``divergence_found``: at least one album contains an item classified
  ``diverges`` or ``file_tag_present_db_absent`` — a real identity
  disagreement. This is decided independent of precedence and of whatever
  ELSE is wrong with that album (or any other), so a permission error on
  one album's files can never mask a genuine divergence found elsewhere,
  and can never manufacture one either. Takes priority over every other
  condition below, including a cursor being set.
- ``incomplete``: no genuine divergence anywhere, but this call could not
  vouch for the WHOLE library — at least one album is listed solely for a
  non-divergence reason (unreadable, empty, or refused-path items); the
  scan itself was time-bounded and stopped before finishing (``complete``
  is ``False`` in that case; see ``deadline_seconds``); OR this call
  started from a resume cursor (``after_album_id is not None`` on input),
  which means it never looked at whatever the cursor skipped, however
  clean the range it DID scan turned out to be. This is NOT "clean" — the
  world blocked a complete answer, so callers must not read it as "no
  divergence" (#1093 review round 4, finding 5; round 5, finding 1; see
  ``scripts/pipeline_cli/audit.py``/``web/routes/retag_divergence_audit.py``
  for the exit-code/status-code mapping this drives).
- ``beets_unavailable``: the Beets authority itself could not be opened or
  queried.

``deadline_seconds`` bounds ONLY the per-album item-read loop below — not
the whole scan (#1093 review round 4, finding 3). ``beets.
list_album_mb_identities()`` (one DB fetch before the loop starts; ~3.2s
measured live) and the caller's JSON encode of the result both run
UNBOUNDED, outside this timer. The API route passes a deadline so one HTTP
request can never let the bounded LOOP outlive a reverse proxy's read
timeout; ``pipeline-cli audit retag-divergence`` passes none — the CLI is
always the full, unbounded census. A bounded scan that runs out of loop
time reports ``complete=False`` over the albums it reached, exactly like an
unavailable Beets authority; the report SHAPE never changes.

``after_album_id``/``next_after_album_id`` let a bounded caller RESUME a
truncated scan across multiple calls, but NO SINGLE call in that chain —
not even the one that reaches the end — is itself allowed to report
``clean``, because each one only ever vouches for the range it scanned
(see ``status`` above). A caller that wants a genuine whole-library
verdict must accumulate across the chain itself: start with
``after_album_id=None``, keep resuming with each report's
``next_after_album_id`` until it comes back ``None``, and conclude
"library-wide clean" only if EVERY page in that chain reported no
divergence — see :func:`scan_retag_divergence`.
"""

from __future__ import annotations

import importlib
import re
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
#: exception, since the file is never opened (#1093 review round 2, finding
#: 7). Describes a LEXICAL (path-string) containment check, not a
#: symlink-resolving one — see the module docstring's ``unreadable`` bullet.
REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL: Literal[
    "refused: stored path is not lexically contained within the configured "
    "library root (path-string check; symlinks are not followed)"
] = (
    "refused: stored path is not lexically contained within the configured "
    "library root (path-string check; symlinks are not followed)"
)

#: Sentinel resume cursor meaning "before the first row" — used ONLY as an
#: OUTPUT ``next_after_album_id`` when a scan truncates before processing
#: even one album AND its own input ``after_album_id`` was ``None`` (#1093
#: review round 5, finding 3). Beets' ``albums`` table is a SQLite
#: ``INTEGER PRIMARY KEY`` (rowid alias); every real album id is >= 1, so
#: ``row.album_id > 0`` is unfiltered — passing ``0`` back as
#: ``after_album_id`` behaves identically to passing ``None``, while still
#: being a real ``int`` that keeps the ``complete == (next_after_album_id
#: is None)`` invariant true even when zero progress was made.
_LIBRARY_START_CURSOR = 0

#: Strict ASCII-digit resume-cursor grammar — deliberately narrower than
#: Python's bare ``int()``, which silently accepts a leading sign,
#: underscore digit-grouping, surrounding whitespace, and non-ASCII digit
#: characters (``int("１_0")== 10``, ``int(" 7 ") == 7``). A cursor is a
#: read-only replay value, not user arithmetic, so none of that leniency is
#: wanted; a malformed or reinterpreted cursor should be REFUSED, not
#: silently coerced to a different album id than the caller typed (#1093
#: review round 5, finding 5). Shared by the CLI's ``--after-album-id`` and
#: the API's ``?after_album_id=`` so both surfaces refuse the same inputs —
#: CLI ⇄ API Surface Symmetry (`.claude/rules/code-quality.md`).
_STRICT_ALBUM_ID_CURSOR_RE = re.compile(r"[0-9]+")


def parse_after_album_id_cursor(text: str) -> int:
    """Strictly parse an ``after_album_id`` resume-cursor string.

    Raises :class:`ValueError` on anything but a plain, nonnegative,
    ASCII-digit sequence — no sign, no underscore grouping, no surrounding
    whitespace, no non-ASCII digit characters.
    """
    if not _STRICT_ALBUM_ID_CURSOR_RE.fullmatch(text):
        raise ValueError(f"not a valid album id cursor: {text!r}")
    return int(text)


#: SQLite's signed 64-bit ``INTEGER`` upper bound. An album id parsed from
#: caller input past this can never exist as a real Beets album id (the
#: ``albums`` table's ``INTEGER PRIMARY KEY`` is bounded the same way) and
#: cannot even be BOUND as a query parameter — ``sqlite3`` raises
#: ``OverflowError`` before any query runs. Reject it as invalid input at
#: every surface that parses one from untrusted text, never as a
#: transient/retryable "Beets unavailable" 503 with a swallowed
#: traceback (#1142 review N10).
SQLITE_MAX_INTEGER = 9223372036854775807


def is_valid_album_id(album_id: int) -> bool:
    """Whether ``album_id`` could ever name a real Beets album row —
    bounded above by :data:`SQLITE_MAX_INTEGER`. Deliberately narrower
    than "any ``int``"; every caller that parses an album id from
    untrusted text (a URL path segment, a CLI positional) checks this
    before it ever reaches a SQLite query."""
    return 0 <= album_id <= SQLITE_MAX_INTEGER


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
    #: Every item path considered — read AND refused (#1093 review round 4,
    #: finding 7a; was misleadingly named ``items_read`` while including
    #: never-opened refused items).
    items_scanned: int
    #: Subset of ``items_scanned`` refused by containment and never opened.
    items_refused: int
    #: Subset of ``items_scanned`` classified ``unreadable`` — a superset
    #: of ``items_refused`` (also includes genuine read/parse failures).
    items_unreadable: int
    #: Albums containing >=1 ``diverges`` item — an INDEPENDENT presence
    #: count, not gated by the album's DISPLAY precedence class (#1093
    #: review round 2, finding 4: an album with both an unreadable item and
    #: a diverging one must still count here, even though its display
    #: class reads ``unreadable``).
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
    #: Echoes the ``after_album_id`` this scan was called with — ``None``
    #: iff this call started from the true beginning of the library. A
    #: caller cannot tell "started from a cursor" from "started from the
    #: beginning" any other way once the report is in hand, and that
    #: distinction is exactly what gates ``status == "clean"`` below
    #: (#1093 review round 5, finding 1).
    after_album_id: int | None = None
    #: Populated iff ``complete`` is ``False`` — either the scan was
    #: truncated by ``deadline_seconds`` before reaching the end of its
    #: row set, OR the Beets authority itself was unavailable
    #: (``status == "beets_unavailable"``; #1093 review round 6, finding
    #: 2 — the two are otherwise indistinguishable to a caller running the
    #: documented resume loop, and both must resume the same way) — the
    #: cursor to pass as ``after_album_id`` on the next call to resume
    #: exactly where this one stopped. INVARIANT:
    #: ``complete == (next_after_album_id is None)`` in EVERY case this
    #: module returns, not only a truncated scan — never ``None`` while
    #: ``complete`` is ``False``, even when zero progress was made (see
    #: :func:`scan_retag_divergence`'s own docstring for how a
    #: zero-progress scan represents this; #1093 review round 5, finding
    #: 3 — the previous shape could report ``next_after_album_id=None``
    #: with ``complete=False``, which a caller following the documented
    #: resume loop reads as "done" while having scanned nothing).
    #: ``None`` when ``complete`` is ``True`` — the scan reached the end
    #: of its (optionally cursor-filtered) row set and there is nothing
    #: left to resume.
    next_after_album_id: int | None = None


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
    after_album_id: int | None = None,
) -> RetagDivergenceReport:
    """Census every Beets album's DB identity against its file tags.

    Pure composition over an already-open, already-readable ``beets``
    handle — no availability mediation here; see
    :func:`scan_retag_divergence_from_factory` /
    :func:`scan_retag_divergence_from_borrowed_factory` for that.

    ``deadline_seconds=None`` (the CLI default) never truncates. A
    configured deadline bounds ONLY this function's per-album read loop —
    see the module docstring — and is checked once per album, before that
    album's items are read: coarse-grained, but albums are small (rarely
    >20 items) so overshoot is bounded. A truncated scan reports
    ``complete=False`` over exactly the albums it reached;
    ``albums_scanned``/``items_scanned``/etc. describe only that reached
    prefix, never the full row count.

    ``after_album_id`` restricts the scan to albums with a strictly greater
    id, letting a bounded caller resume a previous truncated scan (see
    ``next_after_album_id`` on the returned report). Passing a non-``None``
    ``after_album_id`` also forces ``status`` away from ``clean`` even on a
    completed, divergence-free scan (#1093 review round 5, finding 1) — this
    call cannot know whether the range the cursor skipped is clean, so it
    reports ``incomplete`` instead of a whole-library verdict it did not
    compute. Chaining calls (feed each report's ``next_after_album_id``
    back in as the next call's ``after_album_id``, starting from ``None``,
    until ``next_after_album_id`` comes back ``None``) still reconstructs a
    genuine full census across multiple bounded requests — the CALLER
    concludes "library-wide clean" by observing that every page in the
    chain found no divergence, not by reading any single page's own
    ``status``.

    ``next_after_album_id`` satisfies ``complete == (next_after_album_id is
    None)`` in every case, including a scan that truncates before reaching
    even one album: it is never left ``None`` while ``complete`` is
    ``False``, even when there is no real album id yet to report (see
    ``_LIBRARY_START_CURSOR``).
    """
    rows = sorted(beets.list_album_mb_identities(), key=lambda row: row.album_id)
    if after_album_id is not None:
        rows = [row for row in rows if row.album_id > after_album_id]
    deadline = None if deadline_seconds is None else time_fn() + deadline_seconds

    built: list[RetagDivergenceAlbum] = []
    truncated = False
    for row in rows:
        if deadline is not None and time_fn() >= deadline:
            truncated = True
            break
        built.append(_build_album(row, read_tag=read_tag))

    items_scanned = sum(album.item_count for album in built)
    items_refused = sum(
        1
        for album in built
        for item in album.items
        if item.detail == REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL
    )
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
        items_scanned=items_scanned,
        items_refused=items_refused,
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
    elif albums or truncated or after_album_id is not None:
        # A resumed call (``after_album_id is not None``) can never claim
        # ``clean`` on its own, even when it completes: it only vouches
        # for the SUFFIX of the library it actually scanned, never the
        # prefix a cursor skipped. Only a call that both started from the
        # true beginning AND ran to completion has actually answered the
        # whole-library question (#1093 review round 5, finding 1).
        status = "incomplete"
    else:
        status = "clean"
    if truncated:
        if built:
            next_after_album_id = built[-1].album_id
        elif after_album_id is not None:
            next_after_album_id = after_album_id
        else:
            # Truncated before reaching even one album, starting from the
            # true beginning: there is no real album id to report, but
            # ``next_after_album_id`` must still be non-``None`` here (see
            # the field's own invariant) — ``_LIBRARY_START_CURSOR``
            # resumes from the beginning exactly like ``None`` would have
            # (#1093 review round 5, finding 3).
            next_after_album_id = _LIBRARY_START_CURSOR
    else:
        next_after_album_id = None
    return RetagDivergenceReport(
        status=status,
        complete=not truncated,
        counts=counts,
        albums=albums,
        after_album_id=after_album_id,
        next_after_album_id=next_after_album_id,
    )


def _empty_counts() -> RetagDivergenceCounts:
    return RetagDivergenceCounts(0, 0, 0, 0, 0, 0, 0, 0)


def _unavailable_report(
    category: str, *, after_album_id: int | None,
) -> RetagDivergenceReport:
    """``complete=False`` here too — the field's own invariant
    (``complete == (next_after_album_id is None)``) applies uniformly, not
    only to a truncated scan. Without this, a caller running the
    documented resume loop against a transiently unavailable Beets
    authority (``SQLITE_BUSY``/``SQLITE_LOCKED``) reads
    ``next_after_album_id=None`` as "done, nothing left to resume" on the
    FIRST unavailable response — the identical "the loop reads it as done"
    shape #1093 review round 5, finding 3 existed to remove, relocated to
    this path (#1093 review round 6, finding 2). Carrying the caller's own
    cursor through (or ``_LIBRARY_START_CURSOR`` when it was already
    ``None``) lets a resumed walk continue past a transient failure
    exactly like it continues past a truncated scan.
    """
    return RetagDivergenceReport(
        status="beets_unavailable",
        complete=False,
        counts=_empty_counts(),
        albums=(),
        unavailable_detail=f"current Beets authority unavailable ({category})",
        after_album_id=after_album_id,
        next_after_album_id=(
            after_album_id if after_album_id is not None else _LIBRARY_START_CURSOR
        ),
    )


class _BeetsAuthorityUnavailable(Exception):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def _open_beets_authority[OwnedBeetsHandle](
    beets_factory: Callable[[], OwnedBeetsHandle],
) -> OwnedBeetsHandle | _BeetsAuthorityUnavailable:
    """Shared open-mediation for both the whole-library and single-album
    scan entrypoints below — generic over the exact handle protocol each
    caller's factory returns, since this function itself only ever calls
    ``beets_factory()`` and never touches the handle's methods."""
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
    after_album_id: int | None,
) -> RetagDivergenceReport | _BeetsAuthorityUnavailable:
    try:
        return scan_retag_divergence(
            beets, read_tag=read_tag,
            deadline_seconds=deadline_seconds, time_fn=time_fn,
            after_album_id=after_album_id,
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
    after_album_id: int | None = None,
) -> RetagDivergenceReport:
    """Own Beets open/query/close; type only expected unavailability."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_report(opened.category, after_album_id=after_album_id)
    with closing(opened):
        result = _scan_with_mediated_beets(
            opened, read_tag=read_tag,
            deadline_seconds=deadline_seconds, time_fn=time_fn,
            after_album_id=after_album_id,
        )
    if isinstance(result, RetagDivergenceReport):
        return result
    return _unavailable_report(result.category, after_album_id=after_album_id)


SingleAlbumRetagCheckStatus = Literal["found", "not_found", "beets_unavailable"]


class SingleAlbumRetagCheckResult(msgspec.Struct, frozen=True):
    """One on-demand per-album retag-divergence check (#1142) — the
    dashboard's cheap recheck of a single album, as opposed to the
    whole-library :class:`RetagDivergenceReport`. ``album`` is populated
    iff ``status == "found"``, regardless of whether that album agrees or
    diverges — unlike the whole-library report, an explicit single-album
    check reports an agreeing album too, since the operator asked about
    THIS album specifically."""

    status: SingleAlbumRetagCheckStatus
    album: RetagDivergenceAlbum | None = None
    #: Populated iff ``status == "beets_unavailable"``.
    unavailable_detail: str | None = None


@runtime_checkable
class SingleAlbumRetagDivergenceBeetsDB(Protocol):
    def get_album_mb_identity(
        self, album_id: int,
    ) -> BeetsAlbumIdentityRow | None: ...


class OwnedSingleAlbumRetagDivergenceBeetsDB(
    SingleAlbumRetagDivergenceBeetsDB, Protocol,
):
    def close(self) -> None: ...


type SingleAlbumRetagDivergenceBeetsFactory = Callable[
    [], OwnedSingleAlbumRetagDivergenceBeetsDB,
]


def scan_retag_divergence_single_album(
    beets: SingleAlbumRetagDivergenceBeetsDB,
    album_id: int,
    *,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
) -> RetagDivergenceAlbum | None:
    """Census exactly one Beets album's DB identity against its file tags.

    Pure composition over an already-open, already-readable ``beets``
    handle — no availability mediation here, mirroring
    :func:`scan_retag_divergence`. Returns ``None`` iff no album with this
    id exists; unlike a zero-item album (a real, reachable ``empty``
    class), a missing album id is not classifiable at all.
    """
    row = beets.get_album_mb_identity(album_id)
    if row is None:
        return None
    return _build_album(row, read_tag=read_tag)


def _unavailable_single_album_result(category: str) -> SingleAlbumRetagCheckResult:
    return SingleAlbumRetagCheckResult(
        status="beets_unavailable",
        unavailable_detail=f"current Beets authority unavailable ({category})",
    )


def _single_album_result_with_mediated_beets(
    beets: SingleAlbumRetagDivergenceBeetsDB,
    album_id: int,
    *,
    read_tag: Callable[[str], str],
) -> SingleAlbumRetagCheckResult | _BeetsAuthorityUnavailable:
    try:
        album = scan_retag_divergence_single_album(
            beets, album_id, read_tag=read_tag,
        )
    except Exception as exc:
        category = beets_authority_availability_category(exc)
        if category is None:
            raise
        return _BeetsAuthorityUnavailable(category)
    if album is None:
        return SingleAlbumRetagCheckResult(status="not_found")
    return SingleAlbumRetagCheckResult(status="found", album=album)


def scan_retag_divergence_single_album_from_factory(
    beets_factory: SingleAlbumRetagDivergenceBeetsFactory,
    album_id: int,
    *,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
) -> SingleAlbumRetagCheckResult:
    """Own Beets open/query/close for one album's check; type only
    expected unavailability — mirrors
    :func:`scan_retag_divergence_from_factory`."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_single_album_result(opened.category)
    with closing(opened):
        result = _single_album_result_with_mediated_beets(
            opened, album_id, read_tag=read_tag,
        )
    if isinstance(result, SingleAlbumRetagCheckResult):
        return result
    return _unavailable_single_album_result(result.category)


def scan_retag_divergence_single_album_from_borrowed_factory(
    beets_factory: SingleAlbumRetagDivergenceBeetsFactory,
    album_id: int,
    *,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
) -> SingleAlbumRetagCheckResult:
    """Mediate a server-owned Beets handle without closing its lifecycle
    — mirrors :func:`scan_retag_divergence_from_borrowed_factory`."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_single_album_result(opened.category)
    result = _single_album_result_with_mediated_beets(
        opened, album_id, read_tag=read_tag,
    )
    if isinstance(result, SingleAlbumRetagCheckResult):
        return result
    return _unavailable_single_album_result(result.category)


def scan_retag_divergence_from_borrowed_factory(
    beets_factory: RetagDivergenceBeetsFactory,
    *,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    deadline_seconds: float | None = None,
    time_fn: Callable[[], float] = time.monotonic,
    after_album_id: int | None = None,
) -> RetagDivergenceReport:
    """Mediate a server-owned Beets handle without closing its lifecycle."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_report(opened.category, after_album_id=after_album_id)
    result = _scan_with_mediated_beets(
        opened, read_tag=read_tag,
        deadline_seconds=deadline_seconds, time_fn=time_fn,
        after_album_id=after_album_id,
    )
    if isinstance(result, RetagDivergenceReport):
        return result
    return _unavailable_report(result.category, after_album_id=after_album_id)


__all__ = [
    "REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL",
    "SQLITE_MAX_INTEGER",
    "OwnedRetagDivergenceBeetsDB",
    "OwnedSingleAlbumRetagDivergenceBeetsDB",
    "RetagDivergenceAlbum",
    "RetagDivergenceAlbumClass",
    "RetagDivergenceBeetsDB",
    "RetagDivergenceBeetsFactory",
    "RetagDivergenceCounts",
    "RetagDivergenceItem",
    "RetagDivergenceItemClass",
    "RetagDivergenceReport",
    "RetagDivergenceStatus",
    "SingleAlbumRetagCheckResult",
    "SingleAlbumRetagCheckStatus",
    "SingleAlbumRetagDivergenceBeetsDB",
    "SingleAlbumRetagDivergenceBeetsFactory",
    "TagReadOk",
    "TagReadOutcome",
    "TagReadUnreadable",
    "album_class_from_items",
    "classify_retag_divergence_item",
    "is_valid_album_id",
    "parse_after_album_id_cursor",
    "read_mb_albumid_tag",
    "scan_retag_divergence",
    "scan_retag_divergence_from_borrowed_factory",
    "scan_retag_divergence_from_factory",
    "scan_retag_divergence_single_album",
    "scan_retag_divergence_single_album_from_borrowed_factory",
    "scan_retag_divergence_single_album_from_factory",
]
