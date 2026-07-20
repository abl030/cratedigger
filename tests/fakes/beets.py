"""In-memory fake for lib.beets_db.BeetsDB."""

from __future__ import annotations

import copy
import os
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from lib.beets_db import (
    AlbumInfo,
    BeetsWorldAlbum,
    CurrentBeetsAmbiguous,
    CurrentBeetsItem,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    ReleaseLocation,
    _lookup_identity,
)
from lib.release_identity import (
    ReleaseIdentity,
    detect_release_source,
    normalize_release_id,
)


class FakeBeetsDB:
    """In-memory fake for ``lib.beets_db.BeetsDB`` — minimal surface.

    Records state for assertions and lets tests register return values
    for the query methods used by ``harness/import_one.py`` and a few
    web routes. Intentionally narrow — extend only when a test surface
    actually exercises a new method. The real ``BeetsDB`` is the
    contract; the audit's ``test_fake_only_methods_stay_on_the_allowlist``
    catches drift.

    Usage:
        beets = FakeBeetsDB()
        beets.set_album_exists("mbid-123", False)
        beets.set_album_ids_for_release("mbid-123", [77])
        beets.set_album_info(77, AlbumInfo(...))
        beets.set_item_paths("mbid-123", [(11, "/path/01.flac")])
        # ... drive code that calls beets.album_exists("mbid-123") ...
        assert beets.close_calls == 1
    """

    def __init__(
        self,
        *,
        library_db_path: str = "/tmp/fake-beets-library.db",
        library_root: str = "/tmp/fake-beets-library",
    ) -> None:
        self.library_db_path = library_db_path
        self.library_root = library_root
        self._album_exists: dict[str, bool] = {}
        self._album_ids_for_release: dict[str, list[int]] = {}
        self._album_info: dict[str, Any] = {}
        self._item_paths: dict[str, list[tuple[int, str | None]]] = {}
        self._release_identities: list[dict[str, Any]] = []
        self._world_albums: list[BeetsWorldAlbum] = []
        # Default return values for unseeded keys — match the real
        # BeetsDB's "no row" shapes so tests don't crash on missing
        # explicit seeds.
        self._album_exists_default = False
        self._album_ids_default: list[int] = []
        self._album_info_default: Any = None
        self._item_paths_default: list[tuple[int, str]] = []
        self.close_calls: int = 0
        self.album_exists_calls: list[str] = []
        self.get_album_info_calls: list[str] = []
        self.get_all_album_ids_for_release_calls: list[str] = []
        self.get_item_paths_calls: list[str] = []
        self.check_mbids_calls: list[list[str]] = []
        self.check_mbids_detail_calls: list[list[str]] = []
        self.get_album_ids_by_mbids_calls: list[list[str]] = []
        self.get_tracks_by_mb_release_id_calls: list[str] = []
        self._tracks_by_release: dict[str, list[dict[str, Any]]] = {}
        self.get_albums_by_artist_calls: list[tuple[str, str]] = []
        self._mbid_detail: dict[str, dict[str, Any]] = {}
        self._albums_by_artist: dict[str, list[dict[str, Any]]] = {}
        self.list_release_identities_calls: int = 0
        self._album_detail: dict[int, dict[str, Any]] = {}
        self._orphan_item_album_ids: set[int] = set()
        self.get_album_detail_calls: list[int] = []
        self._locate_queue: list[ReleaseLocation] = []
        self.locate_calls: list[str] = []
        self._min_bitrate: dict[str, int | None] = {}
        self._min_bitrate_default: int | None = None
        self.get_min_bitrate_calls: list[str] = []
        self.resolve_current_release_calls: list[ReleaseIdentity] = []
        self._next_synthetic_album_id = 1_000_000

    # --- Seeding helpers ---

    def set_mbid_detail(self, mbid: str, detail: dict[str, Any]) -> None:
        self._mbid_detail[mbid] = detail
        self._clear_presence_override(mbid)
        self._ensure_seeded_album(mbid)

    def set_albums_by_artist(
        self, name: str, albums: list[dict[str, Any]],
    ) -> None:
        self._albums_by_artist[name] = albums

    def set_album_exists(self, release_id: str, value: bool) -> None:
        self._album_exists[release_id] = value
        if value:
            self._ensure_seeded_album(release_id)

    def set_album_ids_for_release(
        self, release_id: str, ids: list[int],
    ) -> None:
        self._clear_presence_override(release_id)
        self._album_ids_for_release[release_id] = list(ids)

    def set_album_info(self, mb_release_id: str, info: Any) -> None:
        self._album_info[mb_release_id] = info
        if isinstance(info, AlbumInfo):
            self._clear_presence_override(mb_release_id)
            self._album_ids_for_release[mb_release_id] = [info.album_id]
            if not any(
                normalize_release_id(seeded)
                == normalize_release_id(mb_release_id)
                for seeded in self._item_paths
            ):
                self._item_paths[mb_release_id] = [(
                    info.album_id * 100,
                    os.path.join(info.album_path, "01.fake"),
                )]

    def set_item_paths(
        self, release_id: str, paths: Sequence[tuple[int, str | None]],
    ) -> None:
        self._item_paths[release_id] = list(paths)
        self._clear_presence_override(release_id)
        self._ensure_seeded_album(release_id)

    def set_release_identities(self, rows: list[dict[str, Any]]) -> None:
        self._release_identities = [copy.deepcopy(r) for r in rows]

    def set_world_albums(self, rows: list[BeetsWorldAlbum]) -> None:
        self._world_albums = list(rows)

    def set_tracks_for_release(
        self, release_id: str, tracks: list[dict[str, Any]],
    ) -> None:
        self._tracks_by_release[release_id] = [
            copy.deepcopy(t) for t in tracks]
        self._clear_presence_override(release_id)
        self._ensure_seeded_album(release_id)

    def set_album_detail(self, album_id: int, detail: dict[str, Any]) -> None:
        self._album_detail[album_id] = copy.deepcopy(detail)

    def set_orphan_items_present(self, album_id: int, present: bool = True) -> None:
        if present:
            self._orphan_item_album_ids.add(album_id)
        else:
            self._orphan_item_album_ids.discard(album_id)

    def set_min_bitrate(self, release_id: str, kbps: int | None) -> None:
        """Seed a per-release min bitrate. Keys are normalized like
        production's locate-backed lookup. The release must also be
        present (album ids seeded, explicit album_exists, or the
        default) — ``get_min_bitrate`` gates on presence first."""
        self._min_bitrate[normalize_release_id(release_id)] = kbps

    def queue_locate_results(self, entries: list[ReleaseLocation]) -> None:
        """Queue ``locate()`` outcomes for before/after-mutation flows.

        Ban-source observes presence around a ``beet remove`` that runs
        behind a patched subprocess, so the fake's seed stores can't
        change mid-flow — the queue models the external mutation.
        Entries are consumed in order; the last entry repeats. An
        ``exact`` entry with empty selectors gets them auto-filled from
        the queried id's shape at call time (the locate contract derives
        selectors from the ID). With an empty queue, ``locate`` is
        state-derived from the ``set_album_ids_for_release`` store.

        Production-impossible locations are rejected up front: an
        ``exact`` hit always carries a real int album_id, and ``absent``
        is always ``(None, ())`` (lib/beets_db.py::locate).
        """
        for entry in entries:
            if entry.kind == "exact":
                assert (isinstance(entry.album_id, int)
                        and not isinstance(entry.album_id, bool)), (
                    f"exact ReleaseLocation needs an int album_id, got "
                    f"{entry.album_id!r} — production cannot construct this")
            elif entry.kind == "absent":
                assert entry.album_id is None and entry.selectors == (), (
                    f"absent ReleaseLocation is always (None, ()), got "
                    f"({entry.album_id!r}, {entry.selectors!r})")
            else:
                assert entry.album_id is None and entry.selectors, (
                    "ambiguous ReleaseLocation needs no album id and explicit "
                    "identity selectors"
                )
        self._locate_queue = list(entries)

    # --- Real-method surface ---

    def _ensure_seeded_album(self, release_id: str) -> None:
        key = normalize_release_id(release_id)
        for seeded, ids in self._album_ids_for_release.items():
            if normalize_release_id(seeded) != key:
                continue
            if ids:
                return
            album_id = self._next_synthetic_album_id
            self._next_synthetic_album_id += 1
            self._album_ids_for_release[seeded] = [album_id]
            return
        album_id = self._next_synthetic_album_id
        self._next_synthetic_album_id += 1
        self._album_ids_for_release[release_id] = [album_id]

    def _clear_presence_override(self, release_id: str) -> None:
        key = normalize_release_id(release_id)
        for seeded in tuple(self._album_exists):
            if normalize_release_id(seeded) == key:
                del self._album_exists[seeded]

    def _album_ids_lookup(self, release_id: str) -> list[int] | None:
        """Normalized view of the ``set_album_ids_for_release`` store.

        Production's ``_batch_lookup_album_ids`` runs every input
        through ``normalize_release_id`` and matches against the stored
        column value — '0012856590' finds the row stored '12856590'.
        Returns ``None`` for "release never seeded" vs ``[]`` for
        "seeded with zero albums".
        """
        key = normalize_release_id(release_id)
        if not key:
            return None
        for seeded, ids in self._album_ids_for_release.items():
            if normalize_release_id(seeded) == key:
                return ids
        return None

    def _presence(self, release_id: str) -> bool:
        """Current presence per the locate seam (issue #121).

        Production answers ``album_exists`` / ``get_min_bitrate`` via
        ``locate``; the fake's equivalent precedence is: queued locate
        head (the most explicit model of "current state around an
        external mutation") → explicit ``set_album_exists`` seed →
        seeded album ids → the ``_album_exists_default``.
        """
        if self._locate_queue:
            return self._locate_queue[0].kind == "exact"
        if release_id in self._album_exists:
            return self._album_exists[release_id]
        ids = self._album_ids_lookup(release_id)
        if ids:
            return True
        return self._album_exists_default

    def album_exists(self, release_id: str) -> bool:
        self.album_exists_calls.append(release_id)
        return self.locate(release_id).kind == "exact"

    def check_mbids(self, mbids: list[str]) -> set[str]:
        self.check_mbids_calls.append(list(mbids))
        return set(self.get_album_ids_by_mbids(mbids))

    def check_mbids_detail(
        self, mbids: list[str],
    ) -> dict[str, dict[str, Any]]:
        self.check_mbids_detail_calls.append(list(mbids))
        result: dict[str, dict[str, Any]] = {}
        for mbid in mbids:
            if mbid not in self._mbid_detail:
                continue
            identity = _lookup_identity(mbid)
            if identity is None:
                continue
            if isinstance(
                self.resolve_current_release(identity),
                CurrentBeetsUnique,
            ):
                result[identity.release_id] = copy.deepcopy(
                    self._mbid_detail[mbid],
                )
        return result

    def get_albums_by_artist(
        self, name: str, mbid: str = "",
    ) -> list[dict[str, Any]]:
        self.get_albums_by_artist_calls.append((name, mbid))
        return copy.deepcopy(self._albums_by_artist.get(name, []))

    def list_release_identities(self) -> list[dict[str, Any]]:
        self.list_release_identities_calls += 1
        return [copy.deepcopy(r) for r in self._release_identities]

    def list_world_albums(self) -> list[BeetsWorldAlbum]:
        return list(self._world_albums)

    def get_all_album_ids_for_release(self, release_id: str) -> list[int]:
        self.get_all_album_ids_for_release_calls.append(release_id)
        ids = self._album_ids_lookup(release_id)
        return list(self._album_ids_default) if ids is None else list(ids)

    def get_album_ids_by_mbids(self, mbids: list[str]) -> dict[str, int]:
        """Mirror of ``BeetsDB.get_album_ids_by_mbids`` — exact hits only.

        Derives from the ``set_album_ids_for_release`` seed store —
        shared with ``get_all_album_ids_for_release``, and seeded album
        ids imply ``album_exists`` — mirroring production's single
        ``_batch_lookup_album_ids`` seam (issue #121). Inputs and result
        keys are normalized like production ('0012856590' hits the row
        seeded '12856590' and the result is keyed '12856590'). First
        seeded album id wins, matching the exact-hit ``locate`` contract.
        """
        self.get_album_ids_by_mbids_calls.append(list(mbids))
        result: dict[str, int] = {}
        for mbid in mbids:
            key = normalize_release_id(mbid)
            if not key:
                continue
            identity = _lookup_identity(key)
            if identity is None:
                continue
            resolution = self.resolve_current_release(identity)
            if isinstance(resolution, CurrentBeetsUnique):
                result[key] = resolution.album_id
        return result

    def get_tracks_by_mb_release_id(
        self, mbid: str,
    ) -> list[dict[str, Any]] | None:
        """Mirror of ``BeetsDB.get_tracks_by_mb_release_id``.

        ``None`` only when the release has no exact beets hit — the
        browse route branches on that. A release seeded with album ids
        but no tracks returns ``[]`` (production: exact hit always
        yields a list), so 'album present but tracks None' is not an
        expressible state.
        """
        self.get_tracks_by_mb_release_id_calls.append(mbid)
        key = normalize_release_id(mbid)
        identity = _lookup_identity(key)
        if identity is None or not isinstance(
            self.resolve_current_release(identity), CurrentBeetsUnique,
        ):
            return None
        for seeded, tracks in self._tracks_by_release.items():
            if normalize_release_id(seeded) == key:
                return [copy.deepcopy(t) for t in tracks]
        return []

    def get_album_detail(self, album_id: int) -> dict[str, Any] | None:
        """Mirror of ``BeetsDB.get_album_detail`` — None when missing."""
        self.get_album_detail_calls.append(album_id)
        detail = self._album_detail.get(album_id)
        return copy.deepcopy(detail) if detail is not None else None

    def album_and_items_absent(self, album_id: int) -> bool:
        return (
            album_id not in self._album_detail
            and album_id not in self._orphan_item_album_ids
        )

    @staticmethod
    def _selectors_for(release_id: str) -> tuple[str, ...]:
        """Selector tuple for an exact hit, derived from the id shape —
        mirrors the dispatch in ``BeetsDB.locate`` (issue #121)."""
        key = normalize_release_id(release_id)
        if detect_release_source(key) == "discogs":
            return (f"discogs_albumid:{key}", f"mb_albumid:{key}")
        return (f"mb_albumid:{key}",)

    def resolve_current_release(
        self,
        identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution:
        """State-respecting fake of the exact current-library resolver."""

        self.resolve_current_release_calls.append(identity)
        for seeded, present in self._album_exists.items():
            if normalize_release_id(seeded) == identity.release_id and not present:
                return CurrentBeetsMissing(identity=identity)
        ids = self._album_ids_lookup(identity.release_id)
        if ids is None:
            if self._album_exists_default and not self._album_ids_default:
                self._ensure_seeded_album(identity.release_id)
                ids = self._album_ids_lookup(identity.release_id)
            else:
                ids = list(self._album_ids_default)
        assert ids is not None
        album_ids = tuple(ids)
        if not album_ids:
            return CurrentBeetsMissing(identity=identity)
        if len(album_ids) != 1:
            return CurrentBeetsAmbiguous(
                identity=identity,
                album_ids=album_ids,
                reason="multiple_matches",
            )

        seeded_paths = None
        for seeded, paths in self._item_paths.items():
            if normalize_release_id(seeded) == identity.release_id:
                seeded_paths = paths
                break
        if seeded_paths is None:
            album_id = album_ids[0]
            seeded_paths = [(
                album_id * 100,
                os.path.join(
                    self.library_root,
                    f"album-{album_id}",
                    "01.fake",
                ),
            )]
        if not seeded_paths:
            return CurrentBeetsAmbiguous(
                identity=identity,
                album_ids=album_ids,
                reason="empty_topology",
            )

        items: list[CurrentBeetsItem] = []
        directories: set[str] = set()
        for item_id, raw_path in seeded_paths:
            if raw_path is None or not raw_path or "\x00" in raw_path:
                return CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=album_ids,
                    reason="invalid_path",
                )
            path = raw_path
            if not os.path.isabs(path):
                if not self.library_root:
                    return CurrentBeetsAmbiguous(
                        identity=identity,
                        album_ids=album_ids,
                        reason="unresolved_relative_path",
                    )
                path = os.path.join(self.library_root, path)
            absolute = os.path.abspath(path)
            items.append(CurrentBeetsItem(id=item_id, path=absolute))
            directories.add(os.path.dirname(absolute))
        if len(directories) != 1:
            return CurrentBeetsAmbiguous(
                identity=identity,
                album_ids=album_ids,
                reason="split_topology",
            )
        return CurrentBeetsUnique(
            identity=identity,
            album_id=album_ids[0],
            album_path=next(iter(directories)),
            items=tuple(items),
            selectors=self._selectors_for(identity.release_id),
        )

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        """Batch facade with the same cardinality semantics as production."""

        return {
            identity: self.resolve_current_release(identity)
            for identity in dict.fromkeys(identities)
        }

    def locate(self, release_id: str) -> ReleaseLocation:
        """Mirror of ``BeetsDB.locate`` — the issue-#121 presence seam.

        Queued results (``queue_locate_results``) win; otherwise the
        answer is state-derived: exact hit iff the release has seeded
        album ids, with selectors derived from the id shape.
        """
        self.locate_calls.append(release_id)
        if self._locate_queue:
            entry = (self._locate_queue.pop(0)
                     if len(self._locate_queue) > 1
                     else self._locate_queue[0])
            if entry.kind == "exact" and not entry.selectors:
                return ReleaseLocation(
                    kind="exact", album_id=entry.album_id,
                    selectors=self._selectors_for(release_id))
            return entry
        identity = _lookup_identity(release_id)
        if identity is None:
            return ReleaseLocation(kind="absent", album_id=None, selectors=())
        resolution = self.resolve_current_release(identity)
        if isinstance(resolution, CurrentBeetsUnique):
            return ReleaseLocation(
                kind="exact", album_id=resolution.album_id,
                selectors=resolution.selectors)
        if isinstance(resolution, CurrentBeetsAmbiguous):
            return ReleaseLocation(
                kind="ambiguous", album_id=None,
                selectors=self._selectors_for(identity.release_id))
        return ReleaseLocation(kind="absent", album_id=None, selectors=())

    def get_min_bitrate(self, mb_release_id: str) -> int | None:
        """Mirror of ``BeetsDB.get_min_bitrate`` (kbps; None = no row).

        Production resolves presence through ``locate`` first and
        returns None for an absent release, so the fake gates on
        ``_presence`` before consulting the (normalized-key) seed
        store / ``_min_bitrate_default``.
        """
        self.get_min_bitrate_calls.append(mb_release_id)
        if self._locate_queue and self._locate_queue[0].kind != "exact":
            return None
        identity = _lookup_identity(mb_release_id)
        if identity is None or not isinstance(
            self.resolve_current_release(identity), CurrentBeetsUnique,
        ):
            return None
        key = normalize_release_id(mb_release_id)
        return self._min_bitrate.get(key, self._min_bitrate_default)

    def get_album_info(
        self, mb_release_id: str, _cfg: Any = None,
    ) -> Any:
        self.get_album_info_calls.append(mb_release_id)
        identity = _lookup_identity(mb_release_id)
        if identity is None:
            return None
        resolution = self.resolve_current_release(identity)
        if not isinstance(resolution, CurrentBeetsUnique):
            return None
        seeded_info = self._album_info_default
        key = normalize_release_id(mb_release_id)
        for seeded, info in self._album_info.items():
            if normalize_release_id(seeded) == key:
                seeded_info = info
                break
        if isinstance(seeded_info, AlbumInfo):
            return replace(
                seeded_info,
                album_id=resolution.album_id,
                album_path=resolution.album_path,
            )
        return seeded_info

    def get_item_paths(self, mb_release_id: str) -> list[tuple[int, str]]:
        self.get_item_paths_calls.append(mb_release_id)
        identity = _lookup_identity(mb_release_id)
        if identity is None:
            return []
        resolution = self.resolve_current_release(identity)
        if not isinstance(resolution, CurrentBeetsUnique):
            return []
        return [(item.id, item.path) for item in resolution.items]

    def close(self) -> None:
        self.close_calls += 1

    def __enter__(self) -> "FakeBeetsDB":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
