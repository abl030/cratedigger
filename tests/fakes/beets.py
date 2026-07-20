"""In-memory fake for lib.beets_db.BeetsDB."""

from __future__ import annotations

import copy
import os
import statistics
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
    _reduce_album_format,
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
        self._item_paths: dict[str, list[tuple[int, str | None]]] = {}
        self._item_metadata: dict[str, dict[int, CurrentBeetsItem]] = {}
        self._release_identities: list[dict[str, Any]] = []
        self._world_albums: list[BeetsWorldAlbum] = []
        # Default return values for unseeded keys — match the real
        # BeetsDB's "no row" shapes so tests don't crash on missing
        # explicit seeds.
        self._album_exists_default = False
        self._album_ids_default: list[int] = []
        self.close_calls: int = 0
        self.album_exists_calls: list[str] = []
        self.get_album_info_calls: list[str] = []
        self.get_all_album_ids_for_release_calls: list[str] = []
        self.get_item_paths_calls: list[str] = []
        self.check_mbids_calls: list[list[str]] = []
        self.check_mbids_detail_calls: list[list[str]] = []
        self.get_album_ids_by_mbids_calls: list[list[str]] = []
        self.get_tracks_by_mb_release_id_calls: list[str] = []
        self.get_albums_by_artist_calls: list[tuple[str, str]] = []
        self._albums_by_artist: dict[str, list[dict[str, Any]]] = {}
        self.list_release_identities_calls: int = 0
        self._album_detail: dict[int, dict[str, Any]] = {}
        self._orphan_item_album_ids: set[int] = set()
        self.get_album_detail_calls: list[int] = []
        self._locate_queue: list[ReleaseLocation] = []
        self.locate_calls: list[str] = []
        self.get_min_bitrate_calls: list[str] = []
        self.resolve_current_release_calls: list[ReleaseIdentity] = []
        self._next_synthetic_album_id = 1_000_000

    # --- Seeding helpers ---

    def set_mbid_detail(self, mbid: str, detail: dict[str, Any]) -> None:
        minimum = detail.get("beets_bitrate")
        average = detail.get("beets_avg_bitrate")
        track_count = int(detail.get("beets_tracks") or (3 if minimum else 1))
        album_id = self._album_ids_lookup(mbid)
        current_album_id = album_id[0] if album_id else self._next_synthetic_album_id
        if not album_id:
            self._next_synthetic_album_id += 1
        if isinstance(minimum, int):
            mean = int(average) if isinstance(average, int) else minimum
            self.set_album_info(mbid, AlbumInfo(
                album_id=current_album_id,
                track_count=track_count,
                min_bitrate_kbps=minimum,
                avg_bitrate_kbps=mean,
                median_bitrate_kbps=mean,
                is_cbr=minimum == mean,
                album_path=os.path.join(
                    self.library_root, f"album-{current_album_id}",
                ),
                format=str(detail.get("beets_format") or ""),
            ))
        else:
            self._seed_current_items(
                mbid,
                current_album_id,
                [CurrentBeetsItem(
                    id=current_album_id * 100,
                    path=os.path.join(
                        self.library_root,
                        f"album-{current_album_id}",
                        "01.fake",
                    ),
                )],
            )
        key = normalize_release_id(mbid)
        samplerate = detail.get("beets_samplerate")
        bitdepth = detail.get("beets_bitdepth")
        if isinstance(samplerate, int) or isinstance(bitdepth, int):
            self._item_metadata[key] = {
                item_id: replace(
                    item,
                    samplerate=(samplerate if isinstance(
                        samplerate, int) else item.samplerate),
                    bitdepth=(bitdepth if isinstance(
                        bitdepth, int) else item.bitdepth),
                )
                for item_id, item in self._item_metadata[key].items()
            }

    def set_albums_by_artist(
        self, name: str, albums: list[dict[str, Any]],
    ) -> None:
        self._albums_by_artist[name] = albums

    def set_album_exists(self, release_id: str, value: bool) -> None:
        key = normalize_release_id(release_id)
        self._album_exists[key] = value
        if value:
            self._ensure_seeded_album(key)

    def set_album_ids_for_release(
        self, release_id: str, ids: list[int],
    ) -> None:
        key = normalize_release_id(release_id)
        self._clear_presence_override(key)
        self._album_ids_for_release[key] = list(ids)

    def set_album_info(self, mb_release_id: str, info: Any) -> None:
        if isinstance(info, AlbumInfo):
            bitrates = self._synthesize_bitrates(info)
            self._seed_current_items(
                mb_release_id,
                info.album_id,
                [
                    CurrentBeetsItem(
                        id=info.album_id * 100 + index,
                        path=os.path.join(
                            info.album_path, f"{index + 1:02d}.fake",
                        ),
                        title=f"Track {index + 1}",
                        track=index + 1,
                        disc=1,
                        format=info.format,
                        bitrate=bitrate,
                    )
                    for index, bitrate in enumerate(bitrates)
                ],
            )
        elif info is None:
            key = normalize_release_id(mb_release_id)
            self._album_ids_for_release[key] = []
            self._item_paths.pop(key, None)
            self._item_metadata.pop(key, None)

    def set_item_paths(
        self, release_id: str, paths: Sequence[tuple[int, str | None]],
    ) -> None:
        key = normalize_release_id(release_id)
        self._item_paths[key] = list(paths)
        self._clear_presence_override(key)
        self._ensure_seeded_album(key)

    def set_release_identities(self, rows: list[dict[str, Any]]) -> None:
        self._release_identities = [copy.deepcopy(r) for r in rows]

    def set_world_albums(self, rows: list[BeetsWorldAlbum]) -> None:
        self._world_albums = list(rows)

    def set_tracks_for_release(
        self, release_id: str, tracks: list[dict[str, Any]],
    ) -> None:
        ids = self._album_ids_lookup(release_id)
        if ids:
            album_id = ids[0]
        else:
            album_id = self._next_synthetic_album_id
            self._next_synthetic_album_id += 1
        self._seed_current_items(
            release_id,
            album_id,
            [CurrentBeetsItem(
                id=int(track.get("id") or album_id * 100 + index),
                path=str(track.get("path") or os.path.join(
                    self.library_root,
                    f"album-{album_id}",
                    f"{index + 1:02d}.fake",
                )),
                title=(str(track["title"]) if track.get("title") is not None else None),
                track=(int(track["track"]) if track.get("track") is not None else None),
                disc=(int(track["disc"]) if track.get("disc") is not None else None),
                length=(float(track["length"]) if track.get("length") is not None else None),
                format=(str(track["format"]) if track.get("format") is not None else None),
                bitrate=(int(track["bitrate"]) if track.get("bitrate") is not None else None),
                samplerate=(int(track["samplerate"]) if track.get("samplerate") is not None else None),
                bitdepth=(int(track["bitdepth"]) if track.get("bitdepth") is not None else None),
            ) for index, track in enumerate(tracks)],
        )

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
        present (album ids seeded or explicit album_exists) —
        ``get_min_bitrate`` gates on current unique topology first."""
        ids = self._album_ids_lookup(release_id)
        album_id = ids[0] if ids else self._next_synthetic_album_id
        if not ids:
            self._next_synthetic_album_id += 1
        path = os.path.join(
            self.library_root, f"album-{album_id}", "01.fake",
        )
        self._seed_current_items(
            release_id,
            album_id,
            [CurrentBeetsItem(
                id=album_id * 100,
                path=path,
                bitrate=(kbps * 1000 if kbps is not None else None),
            )],
        )

    @staticmethod
    def _synthesize_bitrates(info: AlbumInfo) -> list[int]:
        """Construct per-item facts whose production reduction is ``info``."""

        count = info.track_count
        assert count > 0, "AlbumInfo with no items is not production-expressible"
        minimum = info.min_bitrate_kbps
        average = info.avg_bitrate_kbps or minimum
        median = info.median_bitrate_kbps or minimum
        assert minimum <= median, "minimum bitrate cannot exceed median"
        if count == 1:
            values = [minimum * 1000]
            assert average == minimum and median == minimum, (
                "one-item AlbumInfo must have identical min/avg/median"
            )
        elif count == 2:
            assert average == median, (
                "two-item AlbumInfo metrics are not jointly expressible"
            )
            low = minimum * 1000
            target_total = max(2 * average * 1000, low * 2)
            assert target_total <= 2 * (average + 1) * 1000 - 1
            values = [low, target_total - low]
        else:
            low_count = (count - 1) // 2
            middle_count = 1 if count % 2 else 2
            high_count = count - low_count - middle_count
            fixed = (
                [minimum * 1000] * low_count
                + [median * 1000] * middle_count
            )
            target_min = count * average * 1000
            target_max = count * (average + 1) * 1000 - 1
            target_total = max(
                target_min,
                sum(fixed) + high_count * median * 1000,
            )
            assert high_count > 0 and target_total <= target_max, (
                "AlbumInfo min/avg/median are not jointly expressible"
            )
            remaining_total = target_total - sum(fixed)
            base, remainder = divmod(remaining_total, high_count)
            highs = [base + (1 if index < remainder else 0)
                     for index in range(high_count)]
            values = fixed + highs
        if not info.is_cbr and len(values) > 1 and len(set(values)) == 1:
            values[-1] += 1
        assert int(min(values) / 1000) == minimum
        assert int(sum(values) / len(values) / 1000) == average
        assert int(statistics.median(values) / 1000) == median
        assert (len(set(values)) == 1) == info.is_cbr, (
            "AlbumInfo is_cbr disagrees with its per-item bitrates"
        )
        return values

    def _seed_current_items(
        self,
        release_id: str,
        album_id: int,
        items: Sequence[CurrentBeetsItem],
    ) -> None:
        key = normalize_release_id(release_id)
        self._clear_presence_override(key)
        self._album_ids_for_release[key] = [album_id]
        self._item_paths[key] = [(item.id, item.path) for item in items]
        self._item_metadata[key] = {
            item.id: item for item in items
        }

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
        ids = self._album_ids_for_release.get(key)
        if ids:
            return
        album_id = self._next_synthetic_album_id
        self._next_synthetic_album_id += 1
        self._album_ids_for_release[key] = [album_id]

    def _clear_presence_override(self, release_id: str) -> None:
        key = normalize_release_id(release_id)
        self._album_exists.pop(key, None)

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
        return self._album_ids_for_release.get(key)

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
        key = normalize_release_id(release_id)
        if key in self._album_exists:
            return self._album_exists[key]
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
            identity = _lookup_identity(mbid)
            if identity is None:
                continue
            current = self.resolve_current_release(identity)
            if not isinstance(current, CurrentBeetsUnique):
                continue
            formats = tuple(dict.fromkeys(
                item.format for item in current.items if item.format
            ))
            bitrates = [
                item.bitrate for item in current.items
                if item.bitrate is not None and item.bitrate > 0
            ]
            samplerates = [
                item.samplerate for item in current.items
                if item.samplerate is not None
            ]
            bitdepths = [
                item.bitdepth for item in current.items
                if item.bitdepth is not None
            ]
            result[identity.release_id] = {
                "beets_tracks": len(current.items),
                "beets_format": ",".join(formats) if formats else None,
                "beets_bitrate": (
                    int(min(bitrates) / 1000) if bitrates else None
                ),
                "beets_avg_bitrate": (
                    int(sum(bitrates) / len(bitrates) / 1000)
                    if bitrates else None
                ),
                "beets_samplerate": min(samplerates) if samplerates else None,
                "beets_bitdepth": max(bitdepths) if bitdepths else None,
            }
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
        if identity is None:
            return None
        current = self.resolve_current_release(identity)
        if not isinstance(current, CurrentBeetsUnique):
            return None
        items = sorted(
            current.items,
            key=lambda item: (item.disc or 0, item.track or 0, item.id),
        )
        return [{
            "title": item.title, "track": item.track, "disc": item.disc,
            "length": item.length, "format": item.format,
            "bitrate": item.bitrate, "samplerate": item.samplerate,
            "bitdepth": item.bitdepth,
        } for item in items]

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
        if self._album_exists.get(identity.release_id) is False:
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

        seeded_paths = self._item_paths.get(identity.release_id)
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
                root = os.path.abspath(self.library_root)
                path = os.path.abspath(os.path.join(root, path))
                if os.path.commonpath((root, path)) != root:
                    return CurrentBeetsAmbiguous(
                        identity=identity,
                        album_ids=album_ids,
                        reason="invalid_path",
                    )
            absolute = os.path.abspath(path)
            metadata = self._item_metadata.get(
                normalize_release_id(identity.release_id), {},
            ).get(item_id)
            items.append(
                replace(metadata, path=absolute)
                if metadata is not None
                else CurrentBeetsItem(id=item_id, path=absolute)
            )
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

        Production derives this value from the same joined item snapshot as
        every other current-release lookup; the fake does the same.
        """
        self.get_min_bitrate_calls.append(mb_release_id)
        if self._locate_queue and self._locate_queue[0].kind != "exact":
            return None
        identity = _lookup_identity(mb_release_id)
        if identity is None:
            return None
        current = self.resolve_current_release(identity)
        if not isinstance(current, CurrentBeetsUnique):
            return None
        bitrates = [
            item.bitrate for item in current.items
            if item.bitrate is not None and item.bitrate > 0
        ]
        return int(min(bitrates) / 1000) if bitrates else None

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
        measured = [
            (item, bitrate)
            for item in resolution.items
            if (bitrate := item.bitrate) is not None and bitrate > 0
        ]
        if not measured:
            return None
        bitrates = [bitrate for _item, bitrate in measured]
        from lib.quality import QualityRankConfig

        cfg = _cfg if _cfg is not None else QualityRankConfig.defaults()
        return AlbumInfo(
            album_id=resolution.album_id,
            track_count=len(measured),
            min_bitrate_kbps=int(min(bitrates) / 1000),
            avg_bitrate_kbps=int(sum(bitrates) / len(bitrates) / 1000),
            median_bitrate_kbps=int(statistics.median(bitrates) / 1000),
            is_cbr=len(set(bitrates)) == 1,
            album_path=resolution.album_path,
            format=_reduce_album_format(
                {item.format for item, _bitrate in measured if item.format},
                cfg,
            ),
        )

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
