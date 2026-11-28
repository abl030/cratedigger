"""Import-time folder exclusivity guard (issue #1465).

One album per folder is a library invariant (``lib/world_invariants.py``'s
``check_folder_exclusivity``). The ``%aunique`` path templates (#1463/#1464)
keep same-key siblings apart, but nothing stops a destination folder that is
already occupied for another reason: Beets ``replace`` rules, path separator
replacement, filename truncation, or an album whose stored path predates its
sibling. Beets itself then just adds ``.1`` to clashing filenames and moves
the new album in beside the old one.

Beets 2.x removes same-release duplicates (deleting their files) and moves
the new files in one stage (``stages.manipulate_files``), so there is no point
that is both after duplicate removal and before a file mutation. This module
computes the destination the new album WILL get once its duplicates are gone,
without removing anything: ``%aunique`` finds siblings through
``Library.albums``, so filtering the duplicate set out of that one call for
the duration of the computation renders the post-removal folder. The harness
runs this as the last plugin import stage (after the album row is added,
before ``manipulate_files``) and refuses the task before any file is touched.

Runs inside the Beets interpreter: this module must not import ``lib`` (the
main service's harness wrapper has no repository ``PYTHONPATH``, issue #1200).
"""

from __future__ import annotations

import os
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from beets.dbcore.query import Query
    from beets.dbcore.sort import Sort
    from beets.library import Album, Item, Library

# Mirrors ``lib.quality.filetypes.AUDIO_EXTENSIONS`` (dotted). This module
# cannot import ``lib``; ``tests/test_folder_guard.py`` pins the two equal.
AUDIO_EXTENSIONS_DOTTED: frozenset[str] = frozenset({
    ".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav", ".wma",
})


@dataclass(frozen=True)
class FolderOccupant:
    """One Beets album already holding files in the planned folder."""

    album_id: int | None
    mb_albumid: str = ""
    discogs_albumid: str = ""
    albumartist: str = ""
    album: str = ""
    item_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class FolderCollision:
    """Why an import's planned folder is not exclusively its own."""

    folder: str
    planned_paths: tuple[str, ...]
    occupants: tuple[FolderOccupant, ...] = ()
    unowned_files: tuple[str, ...] = ()
    excluded_album_ids: tuple[int, ...] = field(default=())

    def message(self) -> str:
        parts: list[str] = []
        if self.occupants:
            parts.append("beets albums " + ", ".join(
                f"id:{o.album_id} ({o.albumartist} - {o.album}; "
                f"mb={o.mb_albumid or '∅'}, discogs={o.discogs_albumid or '∅'})"
                for o in self.occupants
            ))
        if self.unowned_files:
            parts.append(
                f"{len(self.unowned_files)} file(s) no excluded album owns: "
                + ", ".join(os.path.basename(p) for p in self.unowned_files[:5])
            )
        return (
            f"planned folder {self.folder!r} already holds "
            + "; ".join(parts)
        )

    def to_wire(self) -> dict[str, object]:
        return {
            "folder": self.folder,
            "planned_paths": list(self.planned_paths),
            "occupants": [
                {
                    "beets_album_id": o.album_id,
                    "mb_albumid": o.mb_albumid,
                    "discogs_albumid": o.discogs_albumid,
                    "albumartist": o.albumartist,
                    "album": o.album,
                    "item_paths": list(o.item_paths),
                }
                for o in self.occupants
            ],
            "unowned_files": list(self.unowned_files),
            "excluded_album_ids": list(self.excluded_album_ids),
            "message": self.message(),
        }


def _text(path: bytes | str) -> str:
    return os.fsdecode(path) if isinstance(path, bytes) else path


def _norm(path: bytes | str) -> str:
    return os.path.normcase(os.path.abspath(os.path.normpath(_text(path))))


def _field(model: object, name: str) -> str:
    getter = getattr(model, "get", None)
    value = getter(name, "") if callable(getter) else getattr(model, name, "")
    if value in (None, 0):
        return ""
    return str(value)


@contextmanager
def albums_excluded_from_lookup(
    lib: Library, album_ids: Iterable[int],
) -> Generator[None]:
    """Hide ``album_ids`` from ``lib.albums`` (and the ``%aunique`` memo).

    ``%aunique`` resolves an album's siblings with ``lib.albums(query)`` in
    every Beets era the harness supports, so this renders paths as though
    the excluded albums were already gone, with no database write. The
    memo table is cleared on entry and exit: values memoized while the
    duplicates are hidden must not leak into Beets' own later move.
    """
    hidden = frozenset(album_ids)
    original = lib.albums

    def albums(
        query: str | Sequence[str] | Query | None = None,
        sort: Sort | None = None,
    ) -> list[Album]:
        return [
            album for album in original(query, sort)
            if album.id not in hidden
        ]

    lib._memotable.clear()
    vars(lib)["albums"] = albums
    try:
        yield
    finally:
        vars(lib).pop("albums", None)
        lib._memotable.clear()


def planned_destinations(
    lib: Library,
    items: Sequence[Item],
    excluded_album_ids: Iterable[int],
) -> tuple[str, ...]:
    """Each item's library destination once ``excluded_album_ids`` are gone."""

    with albums_excluded_from_lookup(lib, excluded_album_ids):
        return tuple(_text(item.destination()) for item in items)


def _items_in_folder(lib: Library, folder: str) -> list[Item]:
    try:
        from beets.dbcore.query import PathQuery
    except ImportError:  # Beets 2.1-2.3 kept it beside the library models
        from beets.library import PathQuery

    return [
        item for item in lib.items(PathQuery("path", os.fsencode(folder)))
        if _norm(os.path.dirname(_text(item.path))) == _norm(folder)
    ]


def find_folder_collision(
    lib: Library,
    album: Album,
    items: Sequence[Item],
    duplicate_album_ids: Iterable[int],
) -> FolderCollision | None:
    """Return the collision if ``album``'s planned folder is not its own.

    ``duplicate_album_ids`` are the albums Beets will remove before moving
    (an empty set when the duplicate action is not REMOVE). Their items and
    files are expected in the folder and never count as occupants. Every
    other Beets item in the folder is an occupant, and so is any audio file,
    or any file at a planned destination, that no excluded album owns.
    """

    duplicates = frozenset(duplicate_album_ids)
    # Only the duplicates leave the %aunique sibling set: the new album must
    # stay visible to its own lookup, or a same-key sibling looks unique and
    # loses the bracket that keeps the two apart. Occupancy excludes both.
    planned = planned_destinations(lib, items, duplicates)
    excluded: frozenset[int] = duplicates | frozenset(
        () if album.id is None else (album.id,)
    )
    excluded_ids = tuple(sorted(excluded))
    for folder in sorted({os.path.dirname(path) for path in planned}):
        folder_items = _items_in_folder(lib, folder)
        owned_paths = {
            _norm(_text(item.path))
            for item in folder_items
            if item.album_id in excluded
        }
        occupants_by_album: dict[int | None, list[Item]] = {}
        for item in folder_items:
            album_id = item.album_id
            if album_id in excluded:
                continue
            occupants_by_album.setdefault(album_id, []).append(item)
        occupants: list[FolderOccupant] = []
        for album_id, occupant_items in sorted(
            occupants_by_album.items(), key=lambda kv: (kv[0] is None, kv[0] or 0),
        ):
            first = occupant_items[0]
            occupants.append(FolderOccupant(
                album_id=album_id,
                mb_albumid=_field(first, "mb_albumid"),
                discogs_albumid=_field(first, "discogs_albumid"),
                albumartist=_field(first, "albumartist"),
                album=_field(first, "album"),
                item_paths=tuple(sorted(
                    _text(i.path) for i in occupant_items
                )),
            ))
        db_paths = {
            _norm(_text(item.path)) for item in folder_items
        }
        planned_here = {_norm(p) for p in planned if os.path.dirname(p) == folder}
        unowned: list[str] = []
        if os.path.isdir(folder):
            for entry in sorted(os.listdir(folder)):
                full = os.path.join(folder, entry)
                if not os.path.isfile(full):
                    continue
                normal = _norm(full)
                if normal in owned_paths or normal in db_paths:
                    continue
                if (os.path.splitext(entry)[1].lower() in AUDIO_EXTENSIONS_DOTTED
                        or normal in planned_here):
                    unowned.append(full)
        if occupants or unowned:
            return FolderCollision(
                folder=folder,
                planned_paths=tuple(p for p in planned if os.path.dirname(p) == folder),
                occupants=tuple(occupants),
                unowned_files=tuple(unowned),
                excluded_album_ids=excluded_ids,
            )
    return None
