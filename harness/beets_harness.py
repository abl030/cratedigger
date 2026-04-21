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

from beets import config, library, plugins  # type: ignore[attr-defined]
from beets.importer.session import ImportSession
from beets.importer.tasks import Action
from beets.ui import get_path_formats, get_replacements

if TYPE_CHECKING:
    from beets.importer.tasks import ImportTask


# Append-only JSONL log of every beets album mutation the harness drives.
# Captures MBID swaps that bypass cratedigger's pipeline DB — e.g. the
# tagging-workspace fix_reissues/fix_undated scripts that drive this harness
# with --search-id to intentionally retag existing albums. Without this log,
# those mutations are invisible to cratedigger's audit trail (see the 04-14
# Lucksmiths case). Located next to the library so it survives host rebuilds.
MUTATIONS_LOG_PATH = "/mnt/virtio/Music/.harness-mutations.jsonl"


# Redirect beets logging to stderr so stdout stays clean for JSON protocol
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(levelname)s: %(name)s: %(message)s",
)
# Suppress noisy musicbrainzngs XML parser warnings
logging.getLogger("musicbrainzngs").setLevel(logging.ERROR)


def _serialize_item(item) -> dict:
    """Serialize a beets Item to a JSON-safe dict. Captures everything
    useful for debugging match decisions."""
    path = item.path
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


def _id_str(value) -> str:
    """Coerce an ID-like value to str at the wire boundary.

    Beets' MusicBrainz plugin returns IDs as UUID strings; the Discogs
    plugin returns integers (because the Discogs API returns numbers).
    Consumers in lib/ compare these against DB-stored str mb_release_ids
    with `==`, so a mixed-type wire format silently fails — that was the
    "mbid_not_found" bug for every Discogs validation.
    """
    return str(value) if value else ""


def _serialize_track_info(ti) -> dict:
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


def _serialize_album_candidate(idx: int, candidate) -> dict:
    """Serialize an AlbumMatch to a JSON-safe dict. Captures everything
    the harness knows: distance breakdown, full AlbumInfo metadata,
    track mapping, extra items/tracks with detail."""
    info = candidate.info
    # Build the item→track mapping: which local file matched which MB track
    mapping = []
    for item, track in candidate.mapping.items():
        mapping.append({
            "item": _serialize_item(item),
            "track": _serialize_track_info(track),
        })

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
        "track_count": len(getattr(info, "tracks", []) or []),
        "tracks": [
            _serialize_track_info(t) for t in (getattr(info, "tracks", []) or [])
        ],
        # Mapping: which local item matched which MB track
        "mapping": mapping,
        # Extra items/tracks with full detail (not just counts)
        "extra_items": [_serialize_item(i) for i in candidate.extra_items],
        "extra_tracks": [_serialize_track_info(t) for t in candidate.extra_tracks],
    }


def _serialize_track_candidate(idx: int, candidate) -> dict:
    """Serialize a TrackMatch to a JSON-safe dict."""
    info = candidate.info
    return {
        "index": idx,
        "distance": round(float(candidate.distance), 4),
        "title": getattr(info, "title", None) or "",
        "artist": getattr(info, "artist", None) or "",
        "track_id": _id_str(getattr(info, "track_id", None)),
        "length": round(getattr(info, "length", 0) or 0, 1),
    }


def _mbid_swap_event(task, candidate) -> dict | None:
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


def _append_mutation_log(event: dict, log_path: str = MUTATIONS_LOG_PATH) -> None:
    """Append one JSONL event. Never raises — the audit log must not break
    the import itself. Failures are logged to stderr for operator visibility."""
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except OSError as e:
        print(f"[harness] mutation log write failed ({log_path}): {e}",
              file=sys.stderr)


def _assert_duplicate_keys_include_mb_albumid(cfg) -> None:
    """Fail loud if beets' `import.duplicate_keys.album` omits `mb_albumid`.

    Beets reads this strictly from `config["import"]["duplicate_keys"]["album"]`.
    If the key is misplaced (e.g. top-level `duplicate_keys =` in config.yaml)
    the user's override is silently ignored and beets falls back to the default
    `[albumartist, album]`. `find_duplicates()` then matches cross-MBID siblings
    on album title alone, and the harness's duplicate resolution can trigger
    beets' `task.should_remove_duplicates = True` blast radius — the exact shape
    of the 2026-04-20 Shearwater "Palo Santo" data-loss event.

    This assertion turns that silent misconfig into a loud startup failure.
    Raises SystemExit(1) on regression.
    """
    keys = list(cfg["import"]["duplicate_keys"]["album"].as_str_seq())
    if "mb_albumid" not in keys:
        msg = (
            "FATAL: beets config import.duplicate_keys.album does not include "
            f"'mb_albumid' (got {keys!r}). This enables cross-MBID sibling "
            "destruction via find_duplicates — the 2026-04-20 Palo Santo bug. "
            "Fix: move the `duplicate_keys` block under `import:` in "
            "~/.config/beets/config.yaml (or the equivalent Nix module)."
        )
        print(msg, file=sys.stderr)
        sys.exit(1)


def _send(msg: dict):
    """Write a JSON message to stdout."""
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def _recv() -> dict:
    """Read a JSON message from stdin. Blocks until a line is available."""
    line = sys.stdin.readline()
    if not line:
        raise EOFError("stdin closed — controller disconnected")
    return json.loads(line.strip())


def _path_str(path) -> str:
    """Convert a path (bytes or str) to str."""
    if isinstance(path, bytes):
        return path.decode("utf-8", errors="replace")
    return str(path)


class HarnessImportSession(ImportSession):
    """ImportSession that communicates decisions over JSON stdin/stdout."""

    def __init__(self, lib, loghandler, paths, query=None, pretend=False):
        super().__init__(lib, loghandler, paths, query)
        self._task_counter = 0
        self._pretend = pretend

    def choose_match(self, task: ImportTask):
        """Present album match candidates as JSON; read decision from stdin."""
        task_id = self._task_counter
        self._task_counter += 1

        # Build the task description
        candidates = task.candidates or []
        msg = {
            "type": "choose_match",
            "task_id": task_id,
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": task.cur_artist or "",
            "cur_album": task.cur_album or "",
            "item_count": len(task.items),
            "items": [_serialize_item(item) for item in task.items],
            "recommendation": task.rec.name if task.rec else "none",
            "candidate_count": len(candidates),
            "candidates": [
                _serialize_album_candidate(i, c)
                for i, c in enumerate(candidates)
            ],
        }
        _send(msg)

        # Wait for decision
        decision = _recv()
        return self._apply_decision(task, decision)

    def choose_item(self, task: ImportTask):
        """Present singleton track candidates as JSON; read decision from stdin."""
        task_id = self._task_counter
        self._task_counter += 1

        candidates = task.candidates or []
        msg = {
            "type": "choose_item",
            "task_id": task_id,
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": getattr(task, "cur_artist", "") or "",
            "cur_title": getattr(getattr(task, "item", None), "title", "") if hasattr(task, "item") else "",
            "item": _serialize_item(getattr(task, "item")) if hasattr(task, "item") else {},
            "recommendation": task.rec.name if task.rec else "none",
            "candidate_count": len(candidates),
            "candidates": [
                _serialize_track_candidate(i, c)
                for i, c in enumerate(candidates)
            ],
        }
        _send(msg)

        decision = _recv()
        return self._apply_decision(task, decision)

    def _apply_decision(self, task, decision: dict):
        """Convert a JSON decision into a beets Action or match object."""
        action = decision.get("action", "skip")

        if action == "apply":
            idx = decision.get("candidate_index", 0)
            if 0 <= idx < len(task.candidates):
                if self._pretend:
                    # In pretend mode, DON'T return the candidate — that would
                    # cause beets to apply it (DB write + scrub plugin strips
                    # tags from source files). Just skip after reporting.
                    return Action.SKIP
                # Audit any MBID swap before apply mutates the album.
                ev = _mbid_swap_event(task, task.candidates[idx])
                if ev is not None:
                    _append_mutation_log(ev)
                return task.candidates[idx]
            else:
                _send({
                    "type": "error",
                    "message": f"candidate_index {idx} out of range (0-{len(task.candidates)-1}), skipping",
                })
                return Action.SKIP
        elif action == "skip":
            return Action.SKIP
        elif action == "asis":
            return Action.ASIS
        elif action == "tracks":
            return Action.TRACKS
        elif action == "albums":
            return Action.ALBUMS
        else:
            _send({
                "type": "error",
                "message": f"unknown action '{action}', skipping",
            })
            return Action.SKIP

    def resolve_duplicate(self, task: ImportTask, found_duplicates):
        """Ask controller how to handle duplicates.

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
        dup_mbids = []
        dup_album_ids = []
        for dup in found_duplicates:
            mbid = getattr(dup, "mb_albumid", None) or ""
            dup_mbids.append(mbid)
            dup_album_ids.append(getattr(dup, "id", None))
        msg = {
            "type": "resolve_duplicate",
            "path": _path_str(task.paths[0]) if task.paths else "",
            "cur_artist": task.cur_artist or "",
            "cur_album": task.cur_album or "",
            "duplicate_count": len(found_duplicates),
            "duplicate_mbids": dup_mbids,
            "duplicate_album_ids": dup_album_ids,
        }
        _send(msg)

        decision = _recv()
        resolution = decision.get("action", "skip")

        if resolution == "skip":
            task.set_choice(Action.SKIP)
        elif resolution == "keep":
            pass  # Keep both — do nothing
        elif resolution == "remove":
            task.should_remove_duplicates = True
        elif resolution == "merge":
            task.should_merge_duplicates = True
        else:
            task.set_choice(Action.SKIP)

    def should_resume(self, path):
        """Ask controller whether to resume a previously interrupted import."""
        msg = {
            "type": "should_resume",
            "path": _path_str(path),
        }
        _send(msg)

        decision = _recv()
        return decision.get("resume", False)


def main():
    import argparse

    # Belt-and-suspenders for systemd's UMask=0000 — see lib/permissions.py / GH #84.
    # Runs inside the Nix beets env where lib/ is not on sys.path, so inline
    # the single-line policy rather than import the helper.
    os.umask(0)

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

    # Open the beets library — must pass ALL four args to match what the beet CLI
    # does in beets.ui._open_library(). Without path_formats and replacements,
    # Library() falls back to its hardcoded default "$artist/$album/$track $title"
    # which ignores the user's config (wrong folder structure, no year, splits
    # multi-artist albums by track artist instead of albumartist).
    lib = library.Library(
        config["library"].as_filename(),
        config["directory"].as_filename(),
        get_path_formats(),
        get_replacements(),
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
