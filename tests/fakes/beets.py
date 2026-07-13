"""In-memory fake for lib.beets_db.BeetsDB."""

from __future__ import annotations

import copy
from typing import Any

from lib.beets_db import ReleaseLocation
from lib.release_identity import detect_release_source, normalize_release_id


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

    def __init__(self) -> None:
        self._album_exists: dict[str, bool] = {}
        self._album_ids_for_release: dict[str, list[int]] = {}
        self._album_info: dict[str, Any] = {}
        self._item_paths: dict[str, list[tuple[int, str]]] = {}
        self._release_identities: list[dict[str, Any]] = []
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
        self.get_album_detail_calls: list[int] = []
        self.delete_album_calls: list[int] = []
        self._delete_album_error: Exception | None = None
        self._locate_queue: list[ReleaseLocation] = []
        self.locate_calls: list[str] = []
        self._min_bitrate: dict[str, int | None] = {}
        self._min_bitrate_default: int | None = None
        self.get_min_bitrate_calls: list[str] = []

    # --- Seeding helpers ---

    def set_mbid_detail(self, mbid: str, detail: dict[str, Any]) -> None:
        self._mbid_detail[mbid] = detail

    def set_albums_by_artist(
        self, name: str, albums: list[dict[str, Any]],
    ) -> None:
        self._albums_by_artist[name] = albums

    def set_album_exists(self, release_id: str, value: bool) -> None:
        self._album_exists[release_id] = value

    def set_album_ids_for_release(
        self, release_id: str, ids: list[int],
    ) -> None:
        self._album_ids_for_release[release_id] = list(ids)

    def set_album_info(self, mb_release_id: str, info: Any) -> None:
        self._album_info[mb_release_id] = info

    def set_item_paths(
        self, release_id: str, paths: list[tuple[int, str]],
    ) -> None:
        self._item_paths[release_id] = list(paths)

    def set_release_identities(self, rows: list[dict[str, Any]]) -> None:
        self._release_identities = [copy.deepcopy(r) for r in rows]

    def set_tracks_for_release(
        self, release_id: str, tracks: list[dict[str, Any]],
    ) -> None:
        self._tracks_by_release[release_id] = [
            copy.deepcopy(t) for t in tracks]

    def set_album_detail(self, album_id: int, detail: dict[str, Any]) -> None:
        self._album_detail[album_id] = copy.deepcopy(detail)

    def set_delete_album_error(self, error: Exception | None) -> None:
        self._delete_album_error = error

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
            else:
                assert entry.album_id is None and entry.selectors == (), (
                    f"absent ReleaseLocation is always (None, ()), got "
                    f"({entry.album_id!r}, {entry.selectors!r})")
        self._locate_queue = list(entries)

    # --- Real-method surface ---

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
        return self._presence(release_id)

    def check_mbids(self, mbids: list[str]) -> set[str]:
        self.check_mbids_calls.append(list(mbids))
        return {mbid for mbid in mbids if self.album_exists(mbid)}

    def check_mbids_detail(
        self, mbids: list[str],
    ) -> dict[str, dict[str, Any]]:
        self.check_mbids_detail_calls.append(list(mbids))
        return {
            mbid: copy.deepcopy(self._mbid_detail[mbid])
            for mbid in mbids if mbid in self._mbid_detail
        }

    def get_albums_by_artist(
        self, name: str, mbid: str = "",
    ) -> list[dict[str, Any]]:
        self.get_albums_by_artist_calls.append((name, mbid))
        return copy.deepcopy(self._albums_by_artist.get(name, []))

    def list_release_identities(self) -> list[dict[str, Any]]:
        self.list_release_identities_calls += 1
        return [copy.deepcopy(r) for r in self._release_identities]

    def get_all_album_ids_for_release(self, release_id: str) -> list[int]:
        self.get_all_album_ids_for_release_calls.append(release_id)
        return self._album_ids_for_release.get(
            release_id, list(self._album_ids_default))

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
            ids = self._album_ids_lookup(key)
            if ids is None:
                ids = list(self._album_ids_default)
            if ids:
                result[key] = ids[0]
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
        for seeded, tracks in self._tracks_by_release.items():
            if normalize_release_id(seeded) == key:
                return [copy.deepcopy(t) for t in tracks]
        if self._album_ids_lookup(key):
            return []
        return None

    def get_album_detail(self, album_id: int) -> dict[str, Any] | None:
        """Mirror of ``BeetsDB.get_album_detail`` — None when missing."""
        self.get_album_detail_calls.append(album_id)
        detail = self._album_detail.get(album_id)
        return copy.deepcopy(detail) if detail is not None else None

    def delete_album(self, album_id: int) -> tuple[str, str, list[str]]:
        """Stateful mirror of ``BeetsDB.delete_album``."""
        self.delete_album_calls.append(album_id)
        if self._delete_album_error is not None:
            raise self._delete_album_error
        detail = self._album_detail.pop(album_id, None)
        if detail is None:
            raise ValueError(f"Album {album_id} not found")
        tracks = detail.get("tracks") or []
        paths = [
            str(track["path"])
            for track in tracks
            if isinstance(track, dict) and track.get("path") is not None
        ]
        return (
            str(detail.get("album") or ""),
            str(detail.get("artist") or detail.get("albumartist") or ""),
            paths,
        )

    @staticmethod
    def _selectors_for(release_id: str) -> tuple[str, ...]:
        """Selector tuple for an exact hit, derived from the id shape —
        mirrors the dispatch in ``BeetsDB.locate`` (issue #121)."""
        key = normalize_release_id(release_id)
        if detect_release_source(key) == "discogs":
            return (f"discogs_albumid:{key}", f"mb_albumid:{key}")
        return (f"mb_albumid:{key}",)

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
        key = normalize_release_id(release_id)
        ids = self._album_ids_lookup(key)
        if ids:
            return ReleaseLocation(
                kind="exact", album_id=ids[0],
                selectors=self._selectors_for(key))
        return ReleaseLocation(kind="absent", album_id=None, selectors=())

    def get_min_bitrate(self, mb_release_id: str) -> int | None:
        """Mirror of ``BeetsDB.get_min_bitrate`` (kbps; None = no row).

        Production resolves presence through ``locate`` first and
        returns None for an absent release, so the fake gates on
        ``_presence`` before consulting the (normalized-key) seed
        store / ``_min_bitrate_default``.
        """
        self.get_min_bitrate_calls.append(mb_release_id)
        if not self._presence(mb_release_id):
            return None
        key = normalize_release_id(mb_release_id)
        return self._min_bitrate.get(key, self._min_bitrate_default)

    def get_album_info(
        self, mb_release_id: str, _cfg: Any = None,
    ) -> Any:
        self.get_album_info_calls.append(mb_release_id)
        return self._album_info.get(mb_release_id, self._album_info_default)

    def get_item_paths(self, mb_release_id: str) -> list[tuple[int, str]]:
        self.get_item_paths_calls.append(mb_release_id)
        return self._item_paths.get(
            mb_release_id, list(self._item_paths_default))

    def close(self) -> None:
        self.close_calls += 1

    def __enter__(self) -> "FakeBeetsDB":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
