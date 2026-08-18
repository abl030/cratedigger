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

import argparse
import importlib
import json
import logging
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TextIO, TypeGuard

from beets import config, library, plugins
from beets.autotag import AlbumInfo, AlbumMatch, TrackInfo, TrackMatch
from beets.dbcore import Query

try:
    from harness import beets_compat
except ModuleNotFoundError:  # direct wrapper execution puts harness/ first
    beets_compat = importlib.import_module("beets_compat")

if TYPE_CHECKING:
    # Current Beets imports are type-only. Runtime aliases below deliberately
    # support its 2.1-2.3 monolithic predecessor as well.
    from beets.importer.actions import Action, DuplicateAction
    from beets.importer.session import ImportSession
    from beets.importer.tasks import ImportTask as BeetsImportTask
    from beets.util import PathBytes
else:
    Action = beets_compat.CAPABILITIES.action
    DuplicateAction = beets_compat.CAPABILITIES.duplicate_action
    ImportSession = beets_compat.CAPABILITIES.importer_session
    BeetsImportTask = beets_compat.CAPABILITIES.import_task
    PathBytes = beets_compat.CAPABILITIES.path_bytes

if TYPE_CHECKING:
    from beets.autotag.hooks import JSONDict
    from beets.dbcore.db import Results
    from beets.importer.tasks import ImportTask

    def _lib_albums(
        lib: library.Library, query: object = None, sort: object = None,
    ) -> Results[library.Album]: ...
else:
    def _lib_albums(lib, query=None, sort=None):
        """``library.Library.albums`` behind a fully-typed wrapper.

        Upstream ``Library.albums(self, query=None, sort=None)`` has no
        parameter annotations (a third-party stub gap, not our code), so
        pyright infers the member's own type as partially unknown at
        every call site — including a declared-type local variable
        assigned from a ``Library`` instance, since the "unknown" taint
        is carried by the *source* expression, not suppressed by the
        target's annotation. A ``TYPE_CHECKING``-only redeclaration of
        this one call as a plain function (mirroring the
        ``cratedigger.py::_grab_most_wanted_impl`` pattern) is the idiom
        that actually breaks the taint: pyright only ever sees the fully
        typed stub signature above; at runtime this branch calls the
        real bound method unchanged.
        """
        return lib.albums(query, sort)


def _mutations_log_path() -> str:
    """Append-only JSONL log of every beets album mutation the harness drives.

    Captures MBID swaps that bypass cratedigger's pipeline DB — e.g. the
    tagging-workspace fix_reissues/fix_undated scripts that drive this
    harness with --search-id to intentionally retag existing albums.
    Without this log, those mutations are invisible to cratedigger's audit
    trail (see the 04-14 Lucksmiths case). Derived from the configured
    library path (next to it, so it survives host rebuilds and follows
    whatever library the deployment-owned BEETSDIR config points at, with no
    hardcoded operator paths).
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
    path = getattr(item, "path")  # noqa: B009 - Beets path may be bytes or str at runtime
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
        "discogs_indexed_component_count": (
            beets_compat.discogs_indexed_component_count(ti)
        ),
        "discogs_indexed_duration_complete": (
            beets_compat.discogs_indexed_duration_complete(ti)
        ),
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
    old_mbid = min(existing)
    path = _path_str(task.paths[0]) if getattr(task, "paths", None) else ""
    return {
        "event": "harness_mbid_swap",
        "ts": datetime.now(UTC).isoformat(),
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
    if hasattr(library.Album, "duplicates_query"):
        dup_query = tmp_album.duplicates_query(keys)
    else:
        # Beets 2.1 predates Album.duplicates_query; its own ImportTask used
        # this equivalent query builder. Keep the real-library lookup active
        # rather than treating a historical API gap as "no duplicates".
        legacy_all_fields_query = _legacy_all_fields_query(library.Album)
        dup_query = legacy_all_fields_query(
            {key: tmp_album[key] for key in keys},
        )

    # Same exclusion as upstream beets: a task re-importing exactly the same
    # file paths is not a duplicate replacement.
    task_paths = {i.path for i in task.items if i}
    duplicates: list[library.Album] = []
    for album in _lib_albums(lib, dup_query):
        album_paths = {i.path for i in album.items()}
        if not (album_paths <= task_paths):
            duplicates.append(album)
    return duplicates


def _legacy_all_fields_query(model: object) -> Callable[[dict[str, object]], Query]:
    """Expose Beets 2.1's inherited query classmethod through a typed seam."""
    method_name = "all_fields_query"
    candidate = getattr(model, method_name)
    if not _is_legacy_query_builder(candidate):
        raise RuntimeError("legacy Album has no callable all_fields_query")
    return candidate


def _is_legacy_query_builder(
    candidate: object,
) -> TypeGuard[Callable[[dict[str, object]], Query]]:
    return callable(candidate)


def _install_release_id_duplicate_lookup() -> None:
    """Patch beets duplicate lookup so release-id duplicate_keys work."""
    current = getattr(BeetsImportTask, "find_duplicates", None)
    if getattr(current, "_cratedigger_release_id_mapping", False):
        return

    def find_duplicates(
        self: BeetsImportTask, lib: library.Library,
    ) -> list[library.Album]:
        return _find_duplicates_with_mapped_release_ids(self, lib)

    setattr(  # noqa: B010 - marker is intentionally attached at runtime
        find_duplicates, "_cratedigger_release_id_mapping", True,
    )
    BeetsImportTask.find_duplicates = find_duplicates


_protocol_stdout: TextIO | None = None


def _reserve_protocol_stdout() -> TextIO:
    """Reserve fd 1 for JSON before Beets can emit ordinary output."""
    sys.stdout.flush()
    protocol_fd = os.dup(1)
    os.dup2(2, 1)
    return os.fdopen(protocol_fd, "w", encoding="utf-8", closefd=True)


def _send(msg: dict[str, object]) -> None:
    """Write one JSON protocol message to the private original stdout fd."""
    if _protocol_stdout is None:
        raise RuntimeError("harness protocol stdout has not been reserved")
    _protocol_stdout.write(json.dumps(msg) + "\n")
    _protocol_stdout.flush()


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
    except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return 0


def _album_path(album: object) -> str:
    """Best-effort directory path for a beets Album-like object."""
    item_dir = getattr(album, "item_dir", None)
    if item_dir is not None and _is_callable(item_dir):
        try:
            path = item_dir()
            if path:
                return _path_str(path)
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
            pass

    items = getattr(album, "items", None)
    if items is not None and _is_callable(items):
        try:
            for item in items():
                path = getattr(item, "path", None)
                if path:
                    return os.path.dirname(_path_str(path))
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
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
        cur_artist, cur_album = beets_compat.task_description(task)
        msg: dict[str, object] = {
            "type": "choose_match",
            "task_id": task_id,
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": cur_artist,
            "cur_album": cur_album,
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
        cur_artist, _cur_album = beets_compat.task_description(task)
        msg: dict[str, object] = {
            "type": "choose_item",
            "task_id": task_id,
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": cur_artist,
            "cur_title": getattr(getattr(task, "item", None), "title", "") if hasattr(task, "item") else "",
            "item": (
                _serialize_item(getattr(task, "item"))  # noqa: B009 - Beets adds it dynamically
                if hasattr(task, "item")
                else {}
            ),
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

    def _duplicate_decision(
        self, task: ImportTask, found_duplicates: list[library.AnyLibModel],
    ) -> str:
        """Ask the controller once; API-era hooks only adapt this result."""
        duplicate_candidates = [_serialize_duplicate_album(dup) for dup in found_duplicates]
        cur_artist, cur_album = beets_compat.task_description(task)
        msg: dict[str, object] = {
            "type": "resolve_duplicate",
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": cur_artist,
            "cur_album": cur_album,
            "duplicate_count": len(found_duplicates),
            "duplicate_mbids": [c["mb_albumid"] for c in duplicate_candidates],
            "duplicate_album_ids": [c["beets_album_id"] for c in duplicate_candidates],
            "duplicate_candidates": duplicate_candidates,
        }
        _send(msg)
        decision = _recv()
        resolution = decision.get("action", "skip")
        return resolution if isinstance(resolution, str) else "skip"

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
        outcome = beets_compat.duplicate_outcome(
            self._duplicate_decision(task, found_duplicates), task)
        assert outcome is not None
        return outcome

    def resolve_duplicate(
        self, task: ImportTask, found_duplicates: list[library.AnyLibModel],
    ) -> None:
        """Legacy Beets hook; mutation is localized in ``beets_compat``."""
        outcome = beets_compat.duplicate_outcome(
            self._duplicate_decision(task, found_duplicates), task)
        assert outcome is None

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
    parser.add_argument(
        "--preserve-discogs-flat-subtracks",
        action="store_true",
        help=(
            "Keep flat Discogs indexed entries such as A2.1/A2.2 as "
            "separate physical tracks"
        ),
    )
    args = parser.parse_args()

    # argparse --help exits above with ordinary stdout intact. Every normal
    # harness path reserves fd 1 before config/plugins can write diagnostics.
    global _protocol_stdout
    _protocol_stdout = _reserve_protocol_stdout()
    try:
        _run_protocol(args)
    finally:
        _protocol_stdout.close()
        _protocol_stdout = None


def _run_protocol(args: argparse.Namespace) -> None:

    # Load beets configuration
    config.read()

    beets_compat.configure_discogs_subtracks(
        preserve_flat=args.preserve_discogs_flat_subtracks,
    )
    beets_compat.configure_discogs_cover_art_fallback()

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

    lib = beets_compat.configured_library(config)
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
