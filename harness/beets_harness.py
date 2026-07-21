"""Beets Interactive Import Harness

Subclasses ImportSession to communicate match decisions over JSON via
stdin/stdout. This allows external processes (like Claude Code) to
programmatically control beets' interactive import.

Protocol (newline-delimited JSON):
  stdout → controller:  task descriptions with candidates
  stdin  ← controller:  decision objects

Must run inside beets' Python environment. Use the wrapper:
  ./scripts/run_beets_harness.sh /path/to/import
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from collections.abc import Sequence

from beets import config, library, plugins
from beets.autotag import AlbumInfo, AlbumMatch, TrackInfo, TrackMatch
from beets.dbcore import Query
from beets.importer.actions import Action, DuplicateAction
from beets.importer.session import ImportSession
from beets.importer.tasks import ImportTask as BeetsImportTask
from beets.util import PathBytes

if TYPE_CHECKING:
    from beets.autotag.hooks import JSONDict
    from beets.importer.tasks import ImportTask
    from confuse import ConfigView


def _mutations_log_path() -> str:
    """Append-only JSONL log of every beets album mutation the harness drives.

    Captures MBID swaps that bypass cratedigger's pipeline DB — e.g. the
    tagging-workspace fix_reissues/fix_undated scripts that drive this
    harness with --search-id to intentionally retag existing albums.
    Without this log, those mutations are invisible to cratedigger's audit
    trail (see the 04-14 Lucksmiths case). Derived from the configured
    library path (next to it, so it survives host rebuilds and follows
    whatever library the module-rendered BEETSDIR config points at —
    tier-2 plan U5, no hardcoded operator paths).
    """
    lib_path = config["library"].as_filename()
    return os.path.join(os.path.dirname(lib_path), ".harness-mutations.jsonl")


# Redirect beets logging to stderr so stdout stays clean for JSON protocol
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(levelname)s: %(name)s: %(message)s",
)
# Suppress noisy musicbrainzngs XML parser warnings
logging.getLogger("musicbrainzngs").setLevel(logging.ERROR)


def _serialize_item(item: library.Item) -> dict[str, object]:
    """Serialize a beets Item to a JSON-safe dict. Captures everything
    useful for debugging match decisions."""
    # getattr (not direct `.path`) keeps this Any-typed rather than the
    # narrow `bytes` LibModel declares, so the bytes/str defensive check
    # below stays meaningful to pyright instead of "always true."
    path = getattr(item, "path")
    if isinstance(path, bytes):
        path = path.decode("utf-8", errors="replace")
    return {
        "path": os.path.basename(path),
        "title": getattr(item, "title", None) or "",
        "artist": getattr(item, "artist", None) or "",
        "album": getattr(item, "album", None) or "",
        "track": getattr(item, "track", 0),
        "disc": getattr(item, "disc", 0),
        "length": round(getattr(item, "length", 0) or 0, 1),
        "bitrate": getattr(item, "bitrate", None),
        "format": getattr(item, "format", None) or "",
        "mb_trackid": getattr(item, "mb_trackid", None) or "",
        "data_source": getattr(item, "data_source", None) or "",
    }


def _id_str(value: object) -> str:
    """Coerce an ID-like value to str at the wire boundary.

    Beets' MusicBrainz plugin returns IDs as UUID strings; the Discogs
    plugin returns integers (because the Discogs API returns numbers).
    Consumers in lib/ compare these against DB-stored str mb_release_ids
    with `==`, so a mixed-type wire format silently fails — that was the
    "mbid_not_found" bug for every Discogs validation.
    """
    return str(value) if value else ""


def _serialize_track_info(ti: TrackInfo) -> dict[str, object]:
    """Serialize a TrackInfo to a JSON-safe dict. Full detail for
    debugging track matching and distance calculations."""
    return {
        "title": getattr(ti, "title", None) or "",
        "artist": getattr(ti, "artist", None) or "",
        "index": getattr(ti, "index", None),
        "medium": getattr(ti, "medium", None),
        "medium_index": getattr(ti, "medium_index", None),
        "medium_total": getattr(ti, "medium_total", None),
        "length": round(getattr(ti, "length", 0) or 0, 1),
        "track_id": _id_str(getattr(ti, "track_id", None)),
        "release_track_id": _id_str(getattr(ti, "release_track_id", None)),
        "track_alt": getattr(ti, "track_alt", None),
        "disctitle": getattr(ti, "disctitle", None),
        "data_source": getattr(ti, "data_source", None) or "",
    }


def _serialize_album_candidate(idx: int, candidate: AlbumMatch) -> dict[str, object]:
    """Serialize an AlbumMatch to a JSON-safe dict. Captures everything
    the harness knows: distance breakdown, full AlbumInfo metadata,
    track mapping, extra items/tracks with detail."""
    info: AlbumInfo = candidate.info
    # Build the item→track mapping: which local file matched which MB track
    mapping: list[dict[str, object]] = []
    for item, track in candidate.mapping.items():
        mapping.append({
            "item": _serialize_item(item),
            "track": _serialize_track_info(track),
        })

    info_tracks: list[TrackInfo] = getattr(info, "tracks", []) or []

    return {
        "index": idx,
        "distance": round(float(candidate.distance), 4),
        "distance_breakdown": {
            k: round(float(v), 4) for k, v in candidate.distance.items()
        },
        # AlbumInfo — full metadata
        "artist": getattr(info, "artist", None) or "",
        "album": getattr(info, "album", None) or "",
        "album_id": _id_str(getattr(info, "album_id", None)),
        "albumdisambig": getattr(info, "albumdisambig", None) or "",
        "year": getattr(info, "year", None),
        "original_year": getattr(info, "original_year", None),
        "country": getattr(info, "country", None) or "",
        "label": getattr(info, "label", None) or "",
        "catalognum": getattr(info, "catalognum", None) or "",
        "media": getattr(info, "media", None) or "",
        "mediums": getattr(info, "mediums", None),
        "albumtype": getattr(info, "albumtype", None) or "",
        "albumtypes": getattr(info, "albumtypes", None) or [],
        "albumstatus": getattr(info, "albumstatus", None) or "",
        "releasegroup_id": _id_str(getattr(info, "releasegroup_id", None)),
        "release_group_title": getattr(info, "release_group_title", None) or "",
        "va": getattr(info, "va", False),
        "language": getattr(info, "language", None),
        "script": getattr(info, "script", None),
        "data_source": getattr(info, "data_source", None) or "",
        "barcode": getattr(info, "barcode", None) or "",
        "asin": getattr(info, "asin", None) or "",
        # Track/item counts and lists
        "track_count": len(info_tracks),
        "tracks": [_serialize_track_info(t) for t in info_tracks],
        # Mapping: which local item matched which MB track
        "mapping": mapping,
        # Extra items/tracks with full detail (not just counts)
        "extra_items": [_serialize_item(i) for i in candidate.extra_items],
        "extra_tracks": [_serialize_track_info(t) for t in candidate.extra_tracks],
    }


def _serialize_track_candidate(idx: int, candidate: TrackMatch) -> dict[str, object]:
    """Serialize a TrackMatch to a JSON-safe dict."""
    info: TrackInfo = candidate.info
    return {
        "index": idx,
        "distance": round(float(candidate.distance), 4),
        "title": getattr(info, "title", None) or "",
        "artist": getattr(info, "artist", None) or "",
        "track_id": _id_str(getattr(info, "track_id", None)),
        "length": round(getattr(info, "length", 0) or 0, 1),
    }


def _mbid_swap_event(
    task: ImportTask,
    candidate: AlbumMatch | TrackMatch,
) -> dict[str, object] | None:
    """Return an audit event if applying `candidate` would change the items'
    `mb_albumid`; return None if the mbids already match or there's no
    existing mbid to diff against.

    Pure: takes the task and candidate, returns the dict or None. No I/O.
    The caller (``_apply_decision``) is responsible for writing the log.

    This catches the fix_reissues class of mutation: the items on disk are
    already in beets with some MBID X, and the harness has been told (via
    ``--search-id Y``) to retag them as Y. Without this audit, the swap is
    invisible to cratedigger's pipeline DB (download_log sees nothing —
    different code path) and to beets' built-in import.log (the harness
    bypasses the CLI logger). The 2026-04-14 Lucksmiths case took hours
    of forensics to RC because no single log captured it.
    """
    new_mbid = _id_str(getattr(candidate.info, "album_id", None))
    if not new_mbid:
        return None
    items = list(getattr(task, "items", None) or [])
    existing = {
        _id_str(getattr(it, "mb_albumid", None)) for it in items
        if getattr(it, "mb_albumid", None)
    }
    existing.discard("")
    existing.discard(new_mbid)
    if not existing:
        return None
    # Deterministic pick for tests; in practice items of an album share one mbid.
    old_mbid = sorted(existing)[0]
    path = _path_str(task.paths[0]) if getattr(task, "paths", None) else ""
    return {
        "event": "harness_mbid_swap",
        "ts": datetime.now(timezone.utc).isoformat(),
        "path": path,
        "old_mb_albumid": old_mbid,
        "new_mb_albumid": new_mbid,
        "argv": list(sys.argv),
        "ppid": os.getppid(),
    }


def _neutralize_discogs_provider_ids(candidate: object) -> bool:
    """Blank the mb_* mirrors of a Discogs candidate's numeric provider ids
    so beets does not poison mb_albumid / mb_releasegroupid (issue #570).

    Beets core maps AlbumInfo.album_id -> mb_albumid and releasegroup_id ->
    mb_releasegroupid (Info.MEDIA_FIELD_MAP). The Discogs plugin fills those
    with NUMERIC Discogs ids, so an un-neutralized apply writes a bare integer
    into MUSICBRAINZ_ALBUMID and Jellyfin's `new Guid()` throws. The id is
    preserved in discogs_albumid (a flexattr the plugin already set), which is
    the layout the rest of cratedigger assumes (duplicate_keys = [mb_albumid,
    discogs_albumid], lib/beets_db.py).

    We set the mirrors to "" (not None) so beets' item_data KEEPS and APPLIES
    the empty value, overwriting any previously-poisoned mb_albumid on
    re-import rather than merely skipping it. item_data / raw_data are
    @cached_property on beets' Info; if anything read them before we mutated,
    the cache would hide the change — so we bust both caches to stay
    order-independent.

    `_apply_decision` is shared by `choose_match` (album; info is an
    AlbumInfo, whose __init__ always sets both album_id and releasegroup_id)
    and `choose_item` (singleton; info is a TrackInfo, which has neither
    attribute). We only blank attributes that already exist on `info`, so
    the singleton path stays a true no-op — it never ADDS a stray
    album_id=""/releasegroup_id="" to a TrackInfo that beets would then
    apply.

    Returns True iff it neutralized a Discogs candidate (i.e. at least one
    mirror attribute existed and was blanked). MusicBrainz candidates (UUID
    album_id) and Discogs TrackInfo (neither attribute present) are left
    untouched.
    """
    info = getattr(candidate, "info", None)
    if info is None:
        return False
    if (getattr(info, "data_source", "") or "") != "Discogs":
        return False
    blanked = False
    for attr in ("album_id", "releasegroup_id"):
        if hasattr(info, attr):
            setattr(info, attr, "")
            blanked = True
    if not blanked:
        return False
    # bust beets' @cached_property caches so the neutralized values are what
    # apply_metadata / find_duplicates consume regardless of prior access.
    cache: dict[str, object] | None = getattr(info, "__dict__", None)
    if isinstance(cache, dict):
        cache.pop("item_data", None)
        cache.pop("raw_data", None)
    return True


def _append_mutation_log(
    event: dict[str, object], log_path: str | None = None,
) -> None:
    """Append one JSONL event. Never raises — the audit log must not break
    the import itself. Failures are logged to stderr for operator visibility."""
    try:
        if log_path is None:
            log_path = _mutations_log_path()
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except OSError as e:
        print(f"[harness] mutation log write failed ({log_path}): {e}",
              file=sys.stderr)


def _assert_duplicate_keys_include_mb_albumid(cfg: ConfigView) -> None:
    """Fail loud unless beets duplicate detection uses exact release IDs only.

    Beets reads this strictly from `config["import"]["duplicate_keys"]["album"]`.
    If the key is misplaced (e.g. top-level `duplicate_keys =` in config.yaml)
    the user's override is silently ignored and beets falls back to the default
    `[albumartist, album]`. `find_duplicates()` then matches cross-MBID siblings
    on album title alone, and the harness's duplicate resolution can answer
    `DuplicateAction.REMOVE`, whose `remove_duplicates` blast radius is the
    exact shape of the 2026-04-20 Shearwater "Palo Santo" data-loss event.

    The key set also must not include mutable metadata like albumartist/album:
    live upgrade attempts can carry normalized artist/title differences, which
    makes Beets miss the duplicate callback. Cratedigger no longer owns a
    post-import replacement cleanup state machine, so exact release identifiers
    are the durable duplicate boundary.

    This assertion turns those silent misconfigs into loud startup failures.
    Raises SystemExit(1) on regression.
    """
    keys = list(cfg["import"]["duplicate_keys"]["album"].as_str_seq())
    expected = {"mb_albumid", "discogs_albumid"}
    if set(keys) != expected:
        msg = (
            "FATAL: beets config import.duplicate_keys.album must be exactly "
            f"{sorted(expected)!r} (got {keys!r}). Missing mb_albumid enables "
            "cross-MBID sibling destruction via find_duplicates — the "
            "2026-04-20 Palo Santo bug. Extra mutable keys like albumartist or "
            "album make same-release upgrades miss Beets' duplicate callback, "
            "so replacement would not be atomic. The config is rendered by "
            "the cratedigger NixOS module into ${stateDir}/beets/config.yaml "
            "(BEETSDIR) — duplicate_keys is a hard-coded literal there "
            "(nix/module.nix, tier-2 plan R5), so if this fires the rendered "
            "file was bypassed or hand-edited."
        )
        print(msg, file=sys.stderr)
        sys.exit(1)


def _duplicate_lookup_metadata(task: ImportTask) -> JSONDict:
    """Return album metadata in beets library field names for duplicate lookup.

    Beets 2.9 builds the duplicate query from ``AlbumInfo.copy()`` before
    metadata is applied. MusicBrainz release ids are named ``album_id`` there,
    but the library column and ``duplicate_keys`` field are ``mb_albumid``.
    Without applying AlbumInfo's media-field mapping, Beets queries
    ``albums.mb_albumid = ''`` and never reaches ``get_duplicate_action``.

    Returns beets' own ``JSONDict`` (``dict[str, Any]``, ``beets.autotag.
    hooks``) — this dict is fed straight into ``library.Album(lib, **info)``
    as flexattr kwargs (see ``_find_duplicates_with_mapped_release_ids``),
    the same dynamic-metadata boundary beets' own ``chosen_info()`` and
    ``Info.item_data``/``raw_data`` use that type for.
    """
    info: JSONDict = task.chosen_info()
    data: JSONDict
    # getattr (not `hasattr` + direct `.item_data`) keeps this Any-typed:
    # ``chosen_info()`` always returns a plain dict in the current beets
    # version (no ``item_data`` attribute), so this branch is defensive
    # dead code for an older/different ``chosen_info()`` shape — same
    # None-sentinel semantics as `hasattr` since `item_data` is a dict
    # property, never explicitly `None` when present.
    raw_item_data = getattr(info, "item_data", None)
    if raw_item_data is not None:
        data = dict(raw_item_data)
    else:
        data = dict(info)

    if data.get("album_id") and not data.get("mb_albumid"):
        data["mb_albumid"] = data["album_id"]

    # Preserve beets' original find_duplicates behavior for metadata that
    # still has an item-level artist field.
    if data.get("artist") is not None:
        data["albumartist"] = data["artist"]

    return data


def _find_duplicates_with_mapped_release_ids(
    task: ImportTask,
    lib: library.Library,
) -> list[library.Album]:
    """Beets ``ImportTask.find_duplicates`` with provider IDs mapped first."""
    info = _duplicate_lookup_metadata(task)
    if info.get("albumartist") is None and info.get("artist") is None:
        return []

    tmp_album = library.Album(lib, **info)
    keys = config["import"]["duplicate_keys"]["album"].as_str_seq()
    dup_query = tmp_album.duplicates_query(keys)

    # Same exclusion as upstream beets: a task re-importing exactly the same
    # file paths is not a duplicate replacement.
    task_paths = {i.path for i in task.items if i}
    duplicates: list[library.Album] = []
    for album in lib.albums(dup_query):
        album_paths = {i.path for i in album.items()}
        if not (album_paths <= task_paths):
            duplicates.append(album)
    return duplicates


def _install_release_id_duplicate_lookup() -> None:
    """Patch beets duplicate lookup so release-id duplicate_keys work."""
    current = getattr(BeetsImportTask, "find_duplicates", None)
    if getattr(current, "_cratedigger_release_id_mapping", False):
        return

    def find_duplicates(
        self: BeetsImportTask, lib: library.Library,
    ) -> list[library.Album]:
        return _find_duplicates_with_mapped_release_ids(self, lib)

    setattr(find_duplicates, "_cratedigger_release_id_mapping", True)
    BeetsImportTask.find_duplicates = find_duplicates


def _send(msg: dict[str, object]) -> None:
    """Write a JSON message to stdout."""
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def _recv() -> dict[str, object]:
    """Read a JSON message from stdin. Blocks until a line is available."""
    line = sys.stdin.readline()
    if not line:
        raise EOFError("stdin closed — controller disconnected")
    return json.loads(line.strip())


def _path_str(path: object) -> str:
    """Convert a path (bytes or str) to str."""
    if isinstance(path, bytes):
        return path.decode("utf-8", errors="replace")
    return str(path)


def _is_callable(obj: object) -> bool:
    """``callable()`` without pyright's built-in TypeGuard narrowing.

    Beets' dynamic accessor methods (``Album.items``, ``Album.item_dir``)
    are looked up via ``getattr`` with no static type; pyright's special
    ``callable()`` narrowing collapses any input — even ``Any`` — to a
    synthesized ``(...) -> object`` signature, which turns the harmless
    dynamic call below into a false ``list``/``len`` argument-type error.
    Same boolean result as ``callable()``, without the narrowing side effect.
    """
    return callable(obj)


def _album_item_count(album: object) -> int:
    """Best-effort item count for a beets Album-like object."""
    items = getattr(album, "items", None)
    if items is None or not _is_callable(items):
        return 0
    try:
        return len(list(items()))
    except Exception:
        return 0


def _album_path(album: object) -> str:
    """Best-effort directory path for a beets Album-like object."""
    item_dir = getattr(album, "item_dir", None)
    if item_dir is not None and _is_callable(item_dir):
        try:
            path = item_dir()
            if path:
                return _path_str(path)
        except Exception:
            pass

    items = getattr(album, "items", None)
    if items is not None and _is_callable(items):
        try:
            for item in items():
                path = getattr(item, "path", None)
                if path:
                    return os.path.dirname(_path_str(path))
        except Exception:
            pass
    return ""


def _serialize_duplicate_album(album: object) -> dict[str, object]:
    """Serialize a beets Album from ``found_duplicates``.

    This is the exact album object Beets will feed to ``duplicate_items()``
    when ``get_duplicate_action`` returns ``DuplicateAction.REMOVE``. Keep the
    payload small but diagnostic: album id, release ids, path, item count, and
    human labels.
    """
    return {
        "beets_album_id": getattr(album, "id", None),
        "mb_albumid": _id_str(getattr(album, "mb_albumid", None)),
        "discogs_albumid": _id_str(getattr(album, "discogs_albumid", None)),
        "album_path": _album_path(album),
        "item_count": _album_item_count(album),
        "albumartist": getattr(album, "albumartist", None) or "",
        "album": getattr(album, "album", None) or "",
    }


class HarnessImportSession(ImportSession):
    """ImportSession that communicates decisions over JSON stdin/stdout."""

    def __init__(
        self,
        lib: library.Library,
        loghandler: logging.Handler | None,
        paths: Sequence[PathBytes] | None,
        query: Query | None = None,
        pretend: bool = False,
    ) -> None:
        super().__init__(lib, loghandler, paths, query)
        self._task_counter = 0
        self._pretend = pretend

    def choose_match(self, task: ImportTask) -> AlbumMatch | Action:
        """Present album match candidates as JSON; read decision from stdin."""
        task_id = self._task_counter
        self._task_counter += 1

        # Build the task description. ``task.candidates`` is declared
        # ``Sequence[AlbumMatch | TrackMatch]`` on the shared ImportTask
        # base, but beets always populates ALBUM tasks (this method's only
        # caller) with AlbumMatch candidates exclusively — the assert
        # documents that invariant for the type checker.
        candidates = task.candidates or []
        serialized_candidates: list[dict[str, object]] = []
        for i, c in enumerate(candidates):
            assert isinstance(c, AlbumMatch)
            serialized_candidates.append(_serialize_album_candidate(i, c))
        msg: dict[str, object] = {
            "type": "choose_match",
            "task_id": task_id,
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": task.cur_artist or "",
            "cur_album": task.cur_album or "",
            "item_count": len(task.items),
            "items": [_serialize_item(item) for item in task.items],
            "recommendation": task.rec.name if task.rec else "none",
            "candidate_count": len(candidates),
            "candidates": serialized_candidates,
        }
        _send(msg)

        # Wait for decision
        decision = _recv()
        result = self._apply_decision(task, decision)
        assert isinstance(result, (AlbumMatch, Action))
        return result

    def choose_item(self, task: ImportTask) -> TrackMatch | Action:
        """Present singleton track candidates as JSON; read decision from stdin."""
        task_id = self._task_counter
        self._task_counter += 1

        # Same invariant as choose_match, mirrored for singleton tasks:
        # beets always populates these with TrackMatch candidates only.
        candidates = task.candidates or []
        serialized_candidates: list[dict[str, object]] = []
        for i, c in enumerate(candidates):
            assert isinstance(c, TrackMatch)
            serialized_candidates.append(_serialize_track_candidate(i, c))
        msg: dict[str, object] = {
            "type": "choose_item",
            "task_id": task_id,
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": getattr(task, "cur_artist", "") or "",
            "cur_title": getattr(getattr(task, "item", None), "title", "") if hasattr(task, "item") else "",
            "item": _serialize_item(getattr(task, "item")) if hasattr(task, "item") else {},
            "recommendation": task.rec.name if task.rec else "none",
            "candidate_count": len(candidates),
            "candidates": serialized_candidates,
        }
        _send(msg)

        decision = _recv()
        result = self._apply_decision(task, decision)
        assert isinstance(result, (TrackMatch, Action))
        return result

    def _apply_decision(
        self,
        task: ImportTask,
        decision: dict[str, object],
    ) -> Action | AlbumMatch | TrackMatch:
        """Convert a JSON decision into a beets Action or match object."""
        action = decision.get("action", "skip")
        candidates = task.candidates or []

        if action == "apply":
            idx = decision.get("candidate_index", 0)
            assert isinstance(idx, int)
            if 0 <= idx < len(candidates):
                if self._pretend:
                    # In pretend mode, DON'T return the candidate — that would
                    # cause beets to apply it (DB write + scrub plugin strips
                    # tags from source files). Just skip after reporting.
                    return Action.SKIP
                # Audit any MBID swap before apply mutates the album.
                ev = _mbid_swap_event(task, candidates[idx])
                if ev is not None:
                    _append_mutation_log(ev)
                # Keep Discogs numeric ids out of mb_albumid/mb_releasegroupid (#570).
                _neutralize_discogs_provider_ids(candidates[idx])
                return candidates[idx]
            else:
                _send({
                    "type": "error",
                    "message": f"candidate_index {idx} out of range (0-{len(candidates)-1}), skipping",
                })
                return Action.SKIP
        elif action == "skip":
            return Action.SKIP
        else:
            # Defensive default. Cratedigger's two controllers
            # (lib/beets.py::beets_validate and harness/import_one.py)
            # only ever send "apply" / "skip" / "remove" (the last is
            # handled in get_duplicate_action, not here); the asis /
            # tracks / albums actions beets itself supports are never
            # selected by us. Surface anything unexpected so a future
            # controller change shows up loud instead of silently
            # importing the wrong thing.
            _send({
                "type": "error",
                "message": f"unknown action '{action}', skipping",
            })
            return Action.SKIP

    def get_duplicate_action(
        self, task: ImportTask, found_duplicates: list[library.AnyLibModel]
    ) -> DuplicateAction:
        """Ask the controller how to handle duplicates (beets 2.x hook).

        Beets 2.x replaced the 1.x ``resolve_duplicate`` /
        ``task.should_remove_duplicates`` mechanism: the import pipeline now
        calls ``session.get_duplicate_action(task, found_duplicates)`` and
        stores the returned ``DuplicateAction`` on ``task.duplicate_action``.
        The ``manipulate_files`` stage later calls
        ``task.remove_duplicates(lib)`` iff the action is ``REMOVE`` (atomic
        add-new-then-remove-old), and ``task.skip`` becomes true for ``SKIP``.
        The JSON protocol is unchanged — we emit the same ``resolve_duplicate``
        message and read the controller's decision — only the return contract
        differs (return an enum rather than mutate ``should_remove_duplicates``,
        which no longer exists).

        Emits two parallel arrays, one entry per duplicate (same index):

        - ``duplicate_mbids``: ``mb_albumid`` for each duplicate.
          Empty string for Discogs-sourced pressings (their identifier
          lives in ``discogs_albumid``, and ``mb_albumid`` is empty).
          Used by the controller to detect same-MBID staleness.
        - ``duplicate_album_ids``: ``albums.id`` for each duplicate.
          The beets numeric primary key is unambiguous across MB and
          Discogs — always present, always unique. Used by the
          controller for post-import sibling canonicalization via
          ``beet move -a id:<N>`` (Codex PR #131 round 3 P3: Discogs
          sibling ids were being dropped because the old payload
          only carried mb_albumid).
        """
        duplicate_candidates = [
            _serialize_duplicate_album(dup) for dup in found_duplicates
        ]
        dup_mbids = [c["mb_albumid"] for c in duplicate_candidates]
        dup_album_ids = [c["beets_album_id"] for c in duplicate_candidates]
        msg: dict[str, object] = {
            "type": "resolve_duplicate",
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": task.cur_artist or "",
            "cur_album": task.cur_album or "",
            "duplicate_count": len(found_duplicates),
            "duplicate_mbids": dup_mbids,
            "duplicate_album_ids": dup_album_ids,
            "duplicate_candidates": duplicate_candidates,
        }
        _send(msg)

        decision = _recv()
        resolution = decision.get("action", "skip")

        # Cratedigger's controllers (lib/beets.py::beets_validate and
        # harness/import_one.py) only ever send "skip" (dup-guard refuse) or
        # "remove" (dup-guard allow, beets-owned atomic replacement).
        # "keep" / "merge" were never selected and fold into the defensive
        # SKIP default. Returning SKIP makes ``task.skip`` true so beets never
        # calls ``task.add`` (see ImportTask.skip / _apply_choice); returning
        # REMOVE leaves the new album in place and removes the old duplicate
        # rows in the manipulate_files stage.
        if resolution == "remove":
            return DuplicateAction.REMOVE
        return DuplicateAction.SKIP

    def should_resume(self, path: PathBytes) -> bool:
        """Ask controller whether to resume a previously interrupted import."""
        msg: dict[str, object] = {
            "type": "should_resume",
            "path": _path_str(path),
        }
        _send(msg)

        decision = _recv()
        resume = decision.get("resume", False)
        assert isinstance(resume, bool)
        return resume


def main() -> None:
    import argparse

    # Belt-and-suspenders for the group-writable import boundary — see
    # lib/permissions.py / GH #84. The systemd unit's UMask=0000 is a
    # permissive floor; this explicit 0o002 (not 0) is what narrows newly
    # created files/dirs to group-writable so the shared group can write
    # alongside the media. Runs inside the Nix beets env where lib/ is not
    # on sys.path, so inline the single-line policy rather than import the
    # helper.
    os.umask(0o002)

    parser = argparse.ArgumentParser(
        description="Beets interactive import harness — JSON over stdin/stdout"
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="Paths to import (directories or files)",
    )
    parser.add_argument(
        "--pretend",
        action="store_true",
        help="Dry run — don't actually import, just show what would happen",
    )
    parser.add_argument(
        "--quiet-fallback",
        choices=["skip", "asis"],
        default=None,
        help="If set, auto-decide for strong matches and only ask for uncertain ones",
    )
    parser.add_argument(
        "--noincremental",
        action="store_true",
        help="Disable incremental import (re-process previously seen directories)",
    )
    parser.add_argument(
        "--search-id",
        dest="search_ids",
        action="append",
        default=[],
        help="Force beets to look up a specific MB release ID (can be repeated)",
    )
    parser.add_argument(
        "--upstream",
        action="store_true",
        help="Use upstream musicbrainz.org instead of local mirror (for newly-seeded releases)",
    )
    args = parser.parse_args()

    # Load beets configuration
    config.read()

    # Structural guard against the 2026-04-20 Palo Santo misconfig class.
    # Must fire before any import work touches beets' duplicate-detection path.
    _assert_duplicate_keys_include_mb_albumid(config)
    _install_release_id_duplicate_lookup()

    # Config overrides MUST happen before plugins.load_plugins() because the
    # musicbrainz plugin reads host/https settings at load time.
    if args.noincremental:
        config["import"]["incremental"] = False

    if args.search_ids:
        config["import"]["search_ids"] = args.search_ids

    if args.upstream:
        config["musicbrainz"]["host"] = "musicbrainz.org"
        config["musicbrainz"]["https"] = True
        config["musicbrainz"]["ratelimit"] = 1
        print("Using upstream musicbrainz.org (rate-limited)", file=sys.stderr)

    # Load plugins (critical — chroma, fetchart, etc. participate in lookups)
    # Must happen AFTER config overrides so musicbrainz plugin sees correct host.
    plugins.load_plugins()

    # Pretend mode is handled in HarnessImportSession._apply_decision():
    # we return Action.SKIP instead of the candidate, so beets never calls
    # apply() — no DB writes, no file moves, no scrub plugin side effects.
    # The old approach (copy=False, move=False, write=False) still let beets
    # write to the DB and run scrub, which poisoned the source files.

    # Open the beets library. beets 2.x reorganised the library API:
    # beets.ui.get_path_formats / get_replacements were removed —
    # get_path_formats moved to beets.util.pathformats (and now requires a
    # config subview) and get_replacements became a Library staticmethod.
    # Library() now derives BOTH the path formats (from config["paths"]) and
    # the replacements (from config["replace"]) internally, so the old beets
    # 1.x four-arg form both fails to import and raises TypeError. Passing only
    # (library, directory) preserves the user's configured folder structure and
    # replacements — the cached_property Library.path_formats calls
    # get_path_formats(config["paths"]) and __init__ calls get_replacements().
    lib = library.Library(
        config["library"].as_filename(),
        config["directory"].as_filename(),
    )
    plugins.send("library_opened", lib=lib)

    # Convert paths to bytes (beets convention)
    paths = [p.encode("utf-8") if isinstance(p, str) else p for p in args.paths]

    # Signal that we're starting
    _send({
        "type": "session_start",
        "paths": [_path_str(p) for p in paths],
        "pretend": args.pretend,
        "library": config["library"].as_filename(),
        "directory": config["directory"].as_filename(),
    })

    # Create and run the session
    session = HarnessImportSession(lib, None, paths, pretend=args.pretend)
    try:
        session.run()
    except EOFError:
        print("Controller disconnected — aborting.", file=sys.stderr)
        sys.exit(1)

    _send({"type": "session_end"})


if __name__ == "__main__":
    main()
