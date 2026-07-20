"""Pinned-Beets owner for exact, retryable library deletion.

The web process may authorize a deletion, but it does not mutate Beets'
SQLite database or unlink library files.  This module is executed in the
module-rendered Beets Python/config environment and uses Beets' own models and
filesystem helpers.  The album row remains the retry manifest until every
positively-owned artifact has been removed and verified absent.
"""

from __future__ import annotations

import fnmatch
import logging
import os
import subprocess as sp
from pathlib import Path
from typing import Any, Callable, Literal, TypeAlias

import msgspec

from lib.beets_config_contract import BeetsConfigError, validate_beets_config
from lib.release_identity import ReleaseIdentity


log = logging.getLogger("cratedigger")

DELETE_TIMEOUT_SECONDS = 60


class BeetsDeleteRequest(msgspec.Struct, frozen=True):
    album_id: int
    expected_release_id: str
    library_db_path: str
    library_root: str


class BeetsDeleteCompleted(
    msgspec.Struct, frozen=True, tag="completed", tag_field="status",
):
    album_id: int
    album_name: str
    artist_name: str
    former_album_path: str
    deleted_tracks: int
    deleted_artifacts: int
    preserved_paths: tuple[str, ...]


BeetsDeleteFailureReason = Literal[
    "album_not_found",
    "configuration_mismatch",
    "configuration_error",
    "release_mismatch",
    "empty_manifest",
    "path_escape",
    "filesystem_error",
    "postcondition_failed",
    "metadata_error",
    "subprocess_error",
    "protocol_error",
]


class BeetsDeleteFailed(
    msgspec.Struct, frozen=True, tag="failed", tag_field="status",
):
    album_id: int
    reason: BeetsDeleteFailureReason
    detail: str
    album_still_present: bool
    deleted_tracks: int = 0
    deleted_artifacts: int = 0
    remaining_owned_paths: tuple[str, ...] = ()
    preserved_paths: tuple[str, ...] = ()


BeetsDeleteOutcome: TypeAlias = BeetsDeleteCompleted | BeetsDeleteFailed
SubprocessRunFn = Callable[..., sp.CompletedProcess[bytes]]


class _OwnedPath(msgspec.Struct, frozen=True):
    path: str
    kind: Literal["track", "art", "sidecar", "clutter"]


RemoveFn = Callable[[str], None]
PruneFn = Callable[[str], None]
MetadataRemoveFn = Callable[[], None]
ListDirFn = Callable[[Path], tuple[Path, ...]]
PathExistsFn = Callable[[str], bool]
LstatFn = Callable[[str], os.stat_result]


def _same_configured_path(expected: str, configured: str) -> bool:
    """Compare an authorized path with Beets' active config path."""
    if not expected or not configured:
        return False
    try:
        expected_path = Path(expected).resolve(strict=True)
        configured_path = Path(configured).resolve(strict=True)
        return expected_path == configured_path
    except OSError:
        return False


def _configuration_matches(
    request: BeetsDeleteRequest,
    configured_db: str,
    configured_root: str,
) -> bool:
    """Require both preflight authority paths before opening the library."""
    return (
        _same_configured_path(request.library_db_path, configured_db)
        and _same_configured_path(request.library_root, configured_root)
    )


def _remove_album_metadata_atomically(lib: Any, album: Any) -> None:
    """Remove album, item, and flex rows as one rollback-safe Beets phase.

    Beets' transaction context commits even when an exception escapes. Its
    nested model removals therefore need one outer transaction plus an
    explicit rollback before that outer context exits. A process death before
    the outer commit is likewise rolled back by SQLite.
    """
    with lib.transaction():
        try:
            album.remove(delete=False)
        except BaseException:
            lib._connection().rollback()
            raise


def _decode_path(raw: object) -> str:
    if isinstance(raw, bytes):
        return os.fsdecode(raw)
    return str(raw)


def _path_exists(path: str, *, lstat: LstatFn = os.lstat) -> bool:
    """Probe one owned path without collapsing I/O errors into absence."""
    try:
        lstat(path)
    except FileNotFoundError:
        return False
    return True


def _remaining_owned_paths(
    owned_paths: tuple[_OwnedPath, ...],
    path_exists: PathExistsFn,
) -> tuple[str, ...]:
    return tuple(
        item.path for item in owned_paths if path_exists(item.path)
    )


def _confined_path(raw: object, root: Path) -> Path | None:
    """Resolve a Beets path and reject lexical or symlink escapes."""
    decoded = _decode_path(raw)
    candidate = Path(decoded)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError:
        return None
    return candidate


def _list_directory(directory: Path) -> tuple[Path, ...]:
    return tuple(directory.iterdir())


def _remaining_unknown(
    album_dirs: tuple[str, ...],
    owned: set[str],
    *,
    list_dir: ListDirFn = _list_directory,
) -> tuple[str, ...]:
    """Enumerate unknown content, propagating every directory I/O failure."""
    out: set[str] = set()
    for raw_dir in album_dirs:
        directory = Path(raw_dir)
        if not directory.is_dir():
            continue
        for entry in list_dir(directory):
            if str(entry) not in owned:
                out.add(str(entry))
    return tuple(sorted(out))


def _delete_manifest(
    *,
    album_id: int,
    album_name: str,
    artist_name: str,
    owned_paths: tuple[_OwnedPath, ...],
    album_dirs: tuple[str, ...],
    metadata_remove: MetadataRemoveFn,
    album_present: Callable[[], bool],
    remove_path: RemoveFn,
    prune_dir: PruneFn,
    list_dir: ListDirFn = _list_directory,
    path_exists: PathExistsFn = _path_exists,
) -> BeetsDeleteOutcome:
    """Apply the monotonic filesystem -> metadata state transition."""
    owned_path_set = {item.path for item in owned_paths}
    try:
        preserved = _remaining_unknown(
            album_dirs, owned_path_set, list_dir=list_dir,
        )
    except OSError as exc:
        return BeetsDeleteFailed(
            album_id=album_id,
            reason="filesystem_error",
            detail=(
                "directory enumeration failed before deletion: "
                f"{type(exc).__name__}: {exc}"
            ),
            album_still_present=album_present(),
            # Enumeration failed before any mutation. Reporting the complete
            # manifest is conservative and avoids a second fallible probe.
            remaining_owned_paths=tuple(item.path for item in owned_paths),
        )

    deleted_tracks = 0
    deleted_artifacts = 0
    for target in owned_paths:
        try:
            existed = path_exists(target.path)
        except OSError as exc:
            return BeetsDeleteFailed(
                album_id=album_id,
                reason="filesystem_error",
                detail=(
                    "owned-path presence probe failed before deletion: "
                    f"{type(exc).__name__}: {exc}"
                ),
                album_still_present=album_present(),
                deleted_tracks=deleted_tracks,
                deleted_artifacts=deleted_artifacts,
                remaining_owned_paths=tuple(
                    item.path for item in owned_paths
                ),
                preserved_paths=preserved,
            )
        try:
            remove_path(target.path)
        except Exception as exc:  # noqa: BLE001 -- typed subprocess outcome
            try:
                remaining = _remaining_owned_paths(owned_paths, path_exists)
            except OSError as probe_exc:
                return BeetsDeleteFailed(
                    album_id=album_id,
                    reason="filesystem_error",
                    detail=(
                        f"{type(exc).__name__}: {exc}; owned-path presence "
                        "probe also failed after the removal error: "
                        f"{type(probe_exc).__name__}: {probe_exc}"
                    ),
                    album_still_present=album_present(),
                    deleted_tracks=deleted_tracks,
                    deleted_artifacts=deleted_artifacts,
                    remaining_owned_paths=tuple(
                        item.path for item in owned_paths
                    ),
                    preserved_paths=preserved,
                )
            return BeetsDeleteFailed(
                album_id=album_id,
                reason="filesystem_error",
                detail=f"{type(exc).__name__}: {exc}",
                album_still_present=album_present(),
                deleted_tracks=deleted_tracks,
                deleted_artifacts=deleted_artifacts,
                remaining_owned_paths=remaining,
                preserved_paths=preserved,
            )
        try:
            survived = path_exists(target.path)
        except OSError as exc:
            return BeetsDeleteFailed(
                album_id=album_id,
                reason="postcondition_failed",
                detail=(
                    "owned-path presence probe failed after deletion: "
                    f"{type(exc).__name__}: {exc}"
                ),
                album_still_present=album_present(),
                deleted_tracks=deleted_tracks,
                deleted_artifacts=deleted_artifacts,
                remaining_owned_paths=tuple(
                    item.path for item in owned_paths
                ),
                preserved_paths=preserved,
            )
        if existed and not survived:
            deleted_artifacts += 1
            if target.kind == "track":
                deleted_tracks += 1

    try:
        remaining = _remaining_owned_paths(owned_paths, path_exists)
    except OSError as exc:
        return BeetsDeleteFailed(
            album_id=album_id,
            reason="postcondition_failed",
            detail=(
                "owned-path presence probe failed during final verification: "
                f"{type(exc).__name__}: {exc}"
            ),
            album_still_present=album_present(),
            deleted_tracks=deleted_tracks,
            deleted_artifacts=deleted_artifacts,
            remaining_owned_paths=tuple(item.path for item in owned_paths),
            preserved_paths=preserved,
        )
    try:
        preserved = _remaining_unknown(
            album_dirs, owned_path_set, list_dir=list_dir,
        )
    except OSError as exc:
        return BeetsDeleteFailed(
            album_id=album_id,
            reason="filesystem_error",
            detail=(
                "directory enumeration failed before metadata removal: "
                f"{type(exc).__name__}: {exc}"
            ),
            album_still_present=album_present(),
            deleted_tracks=deleted_tracks,
            deleted_artifacts=deleted_artifacts,
            remaining_owned_paths=remaining,
            preserved_paths=preserved,
        )
    if remaining:
        return BeetsDeleteFailed(
            album_id=album_id,
            reason="postcondition_failed",
            detail="one or more Beets-owned artifacts survived deletion",
            album_still_present=album_present(),
            deleted_tracks=deleted_tracks,
            deleted_artifacts=deleted_artifacts,
            remaining_owned_paths=remaining,
            preserved_paths=preserved,
        )

    # Beets' pruning helper owns the directory operation. Unknown content
    # blocks pruning and is reported rather than recursively guessed away.
    if not preserved:
        for directory in sorted(album_dirs, key=len, reverse=True):
            try:
                prune_dir(directory)
            except Exception as exc:  # noqa: BLE001 -- typed outcome
                return BeetsDeleteFailed(
                    album_id=album_id,
                    reason="filesystem_error",
                    detail=f"directory prune failed: {type(exc).__name__}: {exc}",
                    album_still_present=album_present(),
                    deleted_tracks=deleted_tracks,
                    deleted_artifacts=deleted_artifacts,
                    preserved_paths=preserved,
                )

    # Only now is the retry manifest removed, and delete=False guarantees
    # this final phase performs no further filesystem work.
    try:
        metadata_remove()
    except Exception as exc:  # noqa: BLE001 -- Beets commits on exceptions
        return BeetsDeleteFailed(
            album_id=album_id,
            reason="metadata_error",
            detail=f"{type(exc).__name__}: {exc}",
            album_still_present=album_present(),
            deleted_tracks=deleted_tracks,
            deleted_artifacts=deleted_artifacts,
            preserved_paths=preserved,
        )
    if album_present():
        return BeetsDeleteFailed(
            album_id=album_id,
            reason="postcondition_failed",
            detail="Beets album row survived metadata removal",
            album_still_present=True,
            deleted_tracks=deleted_tracks,
            deleted_artifacts=deleted_artifacts,
            preserved_paths=preserved,
        )

    return BeetsDeleteCompleted(
        album_id=album_id,
        album_name=album_name,
        artist_name=artist_name,
        former_album_path=album_dirs[0],
        deleted_tracks=deleted_tracks,
        deleted_artifacts=deleted_artifacts,
        preserved_paths=preserved,
    )


def execute_pinned_beets_delete(request: BeetsDeleteRequest) -> BeetsDeleteOutcome:
    """Delete one exact album using the active pinned Beets configuration."""
    from beets import config, library, plugins, util

    config_dir = os.environ.get("BEETSDIR", "")
    try:
        configured_plugins = validate_beets_config(config_dir)
    except BeetsConfigError as exc:
        return BeetsDeleteFailed(
            album_id=request.album_id,
            reason="configuration_error",
            detail=str(exc),
            album_still_present=True,
        )
    config.read()
    configured_db = config["library"].as_filename()
    configured_root = config["directory"].as_filename()
    if not _configuration_matches(request, configured_db, configured_root):
        return BeetsDeleteFailed(
            album_id=request.album_id,
            reason="configuration_mismatch",
            detail=(
                "active pinned Beets library/database does not match the "
                "preflight authority"
            ),
            album_still_present=True,
        )

    # ImportSource's removal hook can prompt once per item and can delete the
    # separately recorded import source. Neither behavior is authorized by a
    # Cratedigger library delete, so force that common plugin profile into its
    # documented non-suggesting mode for this operation.
    config["importsource"]["suggest_removal"].set(False)
    plugins.load_plugins()
    loaded_plugins = {plugin.name for plugin in plugins.find_plugins()}
    missing_plugins = sorted(configured_plugins - loaded_plugins)
    if missing_plugins:
        return BeetsDeleteFailed(
            album_id=request.album_id,
            reason="configuration_error",
            detail="configured Beets plugins failed to load: " + ", ".join(missing_plugins),
            album_still_present=True,
        )
    root = Path(configured_root).resolve(strict=True)
    lib = library.Library(
        configured_db,
        configured_root,
    )
    plugins.send("library_opened", lib=lib)
    try:
        album = lib.get_album(request.album_id)
        if album is None:
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason="album_not_found",
                detail="exact Beets album primary key is absent",
                album_still_present=False,
            )
        identity = ReleaseIdentity.from_fields(
            album.get("mb_albumid"), album.get("discogs_albumid"),
        )
        expected = ReleaseIdentity.from_id(request.expected_release_id)
        if identity is None or expected is None or identity != expected:
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason="release_mismatch",
                detail="Beets album identity changed before deletion",
                album_still_present=True,
            )

        candidates: list[_OwnedPath] = []
        dirs: set[Path] = set()
        for item in album.items():
            path = _confined_path(item.path, root)
            if path is None:
                return BeetsDeleteFailed(
                    album_id=request.album_id,
                    reason="path_escape",
                    detail=f"track escapes configured library root: {_decode_path(item.path)}",
                    album_still_present=True,
                )
            candidates.append(_OwnedPath(str(path), "track"))
            dirs.add(path.parent)

        if album.artpath:
            art = _confined_path(album.artpath, root)
            if art is None:
                return BeetsDeleteFailed(
                    album_id=request.album_id,
                    reason="path_escape",
                    detail=f"art escapes configured library root: {_decode_path(album.artpath)}",
                    album_still_present=True,
                )
            candidates.append(_OwnedPath(str(art), "art"))
            dirs.add(art.parent)

        if not dirs:
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason="empty_manifest",
                detail="album has no track or art path from which to derive ownership",
                album_still_present=True,
            )

        clutter_patterns = tuple(config["clutter"].as_str_seq())
        for directory in tuple(dirs):
            sidecar = directory / "cratedigger.json"
            candidates.append(_OwnedPath(str(sidecar), "sidecar"))
            if directory.is_dir():
                try:
                    entries = _list_directory(directory)
                except OSError as exc:
                    return BeetsDeleteFailed(
                        album_id=request.album_id,
                        reason="filesystem_error",
                        detail=(
                            "directory enumeration failed while building the "
                            f"owned manifest: {type(exc).__name__}: {exc}"
                        ),
                        album_still_present=True,
                    )
                for entry in entries:
                    if entry.is_dir():
                        continue
                    if any(fnmatch.fnmatch(entry.name, pattern)
                           for pattern in clutter_patterns):
                        candidates.append(_OwnedPath(str(entry), "clutter"))

        # Deduplicate art/sidecar/clutter overlaps while retaining the most
        # specific kind for the track counter.
        by_path: dict[str, _OwnedPath] = {}
        for candidate in candidates:
            confined = _confined_path(candidate.path, root)
            if confined is None:
                return BeetsDeleteFailed(
                    album_id=request.album_id,
                    reason="path_escape",
                    detail=f"owned artifact escapes configured library root: {candidate.path}",
                    album_still_present=True,
                )
            prior = by_path.get(str(confined))
            if prior is None or candidate.kind == "track":
                by_path[str(confined)] = _OwnedPath(str(confined), candidate.kind)

        owned = tuple(sorted(by_path.values(), key=lambda value: value.path))
        album_dirs = tuple(sorted(str(path) for path in dirs))

        def metadata_present() -> bool:
            if lib.get_album(request.album_id) is not None:
                return True
            return next(iter(lib.items(f"album_id:{request.album_id}")), None) is not None

        return _delete_manifest(
            album_id=request.album_id,
            album_name=str(album.get("album") or ""),
            artist_name=str(album.get("albumartist") or ""),
            owned_paths=owned,
            album_dirs=album_dirs,
            metadata_remove=lambda: _remove_album_metadata_atomically(lib, album),
            album_present=metadata_present,
            remove_path=lambda path: util.remove(path),
            prune_dir=lambda path: util.prune_dirs(path),
        )
    finally:
        lib._close()


def run_beets_delete(
    request: BeetsDeleteRequest,
    *,
    runner: SubprocessRunFn = sp.run,
) -> BeetsDeleteOutcome:
    """Run :func:`execute_pinned_beets_delete` in the pinned Beets Python."""
    from lib.util import beets_subprocess_env

    try:
        env = beets_subprocess_env()
        python = env.get("CRATEDIGGER_BEETS_PYTHON", "")
        if not python:
            raise RuntimeError("CRATEDIGGER_BEETS_PYTHON is not configured")
        harness = Path(__file__).resolve().parent.parent / "harness" / "delete_album.py"
        proc = runner(
            [python, str(harness)],
            input=msgspec.json.encode(request),
            capture_output=True,
            timeout=DELETE_TIMEOUT_SECONDS,
            env=env,
        )
    except (OSError, RuntimeError, sp.TimeoutExpired) as exc:
        return BeetsDeleteFailed(
            album_id=request.album_id,
            reason="subprocess_error",
            detail=f"{type(exc).__name__}: {exc}",
            album_still_present=True,
        )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip()
        return BeetsDeleteFailed(
            album_id=request.album_id,
            reason="subprocess_error",
            detail=f"rc={proc.returncode}: {detail[-500:]}",
            album_still_present=True,
        )
    try:
        outcome = msgspec.json.decode(proc.stdout, type=BeetsDeleteOutcome)
        if proc.stdout != msgspec.json.encode(outcome):
            raise ValueError("stdout was not one canonical outcome frame")
        return outcome
    except (msgspec.DecodeError, ValueError) as exc:
        log.error("Pinned Beets delete returned invalid JSON: %s", exc)
        return BeetsDeleteFailed(
            album_id=request.album_id,
            reason="protocol_error",
            detail=str(exc),
            album_still_present=True,
        )
