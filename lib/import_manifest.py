"""Filesystem manifest guards for import candidates."""

from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from lib.quality import AUDIO_EXTENSIONS_DOTTED
from lib.staged_album import staged_filename
from lib.wrong_match_policy import WRONG_MATCH_QUARANTINE_DIR

if TYPE_CHECKING:
    from lib.grab_list import DownloadFile

logger = logging.getLogger("cratedigger")

MutationCheckpoint = Callable[[], None]


@dataclass(frozen=True)
class CuratedMoveResult:
    """Outcome of ``move_failed_import_curated``.

    ``anomaly`` is set only in the unreachable-in-practice residue case
    (issue #1077, B1): real content survived the curated move and
    empty-directory pruning, and got swept into ``target_path`` instead of
    raising. The caller folds this into the persisted ``ValidationResult``
    detail so the anomaly surfaces in Recents, not as a stack trace.
    """
    target_path: str
    anomaly: str | None = None


@dataclass(frozen=True)
class ManifestCheck:
    extra_audio: list[str]
    missing_audio: list[str]

    @property
    def ok(self) -> bool:
        return not self.extra_audio and not self.missing_audio

    def detail(self) -> str:
        parts: list[str] = []
        if self.extra_audio:
            parts.append("extra audio: " + ", ".join(self.extra_audio))
        if self.missing_audio:
            parts.append("missing audio: " + ", ".join(self.missing_audio))
        return "; ".join(parts)


def _is_audio_path(path: str) -> bool:
    return os.path.splitext(path)[1].lower() in AUDIO_EXTENSIONS_DOTTED


def _safe_relpath(path: str) -> str | None:
    rel = os.path.normpath(path).replace("\\", os.sep)
    if os.path.isabs(rel) or rel == "." or rel.startswith(".." + os.sep) or rel == "..":
        return None
    return rel


def _makedirs(
    path: str,
    *,
    exist_ok: bool,
    before_mutation: MutationCheckpoint | None,
) -> None:
    """Create each missing directory as one cancellation-visible mutation."""
    if before_mutation is None:
        os.makedirs(path, exist_ok=exist_ok)
        return
    missing: list[str] = []
    cursor = os.path.abspath(path)
    while not os.path.exists(cursor):
        missing.append(cursor)
        parent = os.path.dirname(cursor)
        if parent == cursor:
            break
        cursor = parent
    if os.path.exists(cursor) and not os.path.isdir(cursor):
        raise NotADirectoryError(cursor)
    if not missing and not exist_ok:
        raise FileExistsError(path)
    for directory in reversed(missing):
        before_mutation()
        try:
            os.mkdir(directory)
        except FileExistsError:
            if not exist_ok or not os.path.isdir(directory):
                raise


def _move(
    source: str,
    destination: str,
    *,
    before_mutation: MutationCheckpoint | None,
) -> None:
    """Use only an atomic rename while cancellation is being monitored."""
    if before_mutation is None:
        shutil.move(source, destination)
        return
    before_mutation()
    os.rename(source, destination)


def _remove_tree(
    path: str,
    *,
    before_mutation: MutationCheckpoint,
) -> None:
    """Delete a private rollback tree one namespace mutation at a time."""
    if not os.path.lexists(path):
        return
    if not os.path.isdir(path) or os.path.islink(path):
        before_mutation()
        os.unlink(path)
        return
    with os.scandir(path) as entries:
        names = sorted(entry.name for entry in entries)
    for name in names:
        _remove_tree(
            os.path.join(path, name),
            before_mutation=before_mutation,
        )
    before_mutation()
    os.rmdir(path)


def _prune_empty_dirs(
    root: str,
    *,
    before_mutation: MutationCheckpoint | None,
) -> None:
    """Remove every empty directory under ``root``, bottom-up.

    The curated move below relocates FILES via ``os.walk`` but never
    removes the directory skeletons it walked through — a benign non-audio
    subdirectory (e.g. ``Scans/``) with every file already moved out of it
    is left behind as an empty shell (issue #1077, B1), indistinguishable
    from a real leftover to a plain ``os.scandir`` presence check. Leaves
    ``root`` itself and any directory that still holds a file at any depth
    untouched.
    """
    if not os.path.isdir(root) or os.path.islink(root):
        return
    with os.scandir(root) as entries:
        subdirs = sorted(
            entry.name for entry in entries
            if entry.is_dir(follow_symlinks=False)
        )
    for name in subdirs:
        child = os.path.join(root, name)
        _prune_empty_dirs(child, before_mutation=before_mutation)
        with os.scandir(child) as remaining:
            still_has_entries = any(remaining)
        if not still_has_entries:
            if before_mutation is not None:
                before_mutation()
            os.rmdir(child)


def _sweep_residue_into_destination(
    src_path: str,
    target_path: str,
    *,
    before_mutation: MutationCheckpoint | None,
) -> None:
    """Move every remaining entry under ``src_path`` into ``target_path``.

    Reached only when real content survives the curated move plus
    empty-directory pruning (issue #1077, B1) — genuinely unexpected, since
    the caller's manifest guard is expected to have already proven an
    exact audio match. "Kept implies visible" (D1) takes priority over a
    clean move: residue merges into the SAME quarantine destination rather
    than being left split outside it, or raising and blocking the
    rejection record that must still be written.
    """
    for dirpath, _dirnames, filenames in os.walk(src_path):
        rel_dir = os.path.relpath(dirpath, src_path)
        ordered_filenames = (
            sorted(filenames) if before_mutation is not None else filenames
        )
        for filename in ordered_filenames:
            full_src = os.path.join(dirpath, filename)
            rel = filename if rel_dir == "." else os.path.join(rel_dir, filename)
            rel = os.path.normpath(rel)
            full_dst = os.path.join(target_path, rel)
            counter = 1
            while os.path.exists(full_dst):
                base, ext = os.path.splitext(full_dst)
                full_dst = f"{base}_leftover{counter}{ext}"
                counter += 1
            _makedirs(
                os.path.dirname(full_dst),
                exist_ok=True,
                before_mutation=before_mutation,
            )
            _move(full_src, full_dst, before_mutation=before_mutation)
    _prune_empty_dirs(src_path, before_mutation=before_mutation)


def _allocate_target(
    src_path: str,
    *,
    quarantine_root: str | None = None,
    before_mutation: MutationCheckpoint | None = None,
) -> str:
    """Allocate a free destination under the Wrong Matches quarantine root.

    Every production caller (issue #1077, D3/D6) is a kept, worklist-visible
    rejection — the historical ``failed_imports`` (non-``bad_files``) branch
    for excluded scenarios had no producer left once ``audio_corrupt`` moved
    to ban+delete, so this always targets ``wrong_matches/`` now. Kept holds
    by construction: there is no second destination for a caller to pick
    wrong.
    """
    parent_dir = (
        os.path.abspath(quarantine_root)
        if quarantine_root is not None
        else os.path.dirname(os.path.abspath(src_path))
    )
    quarantine_dir = os.path.join(parent_dir, WRONG_MATCH_QUARANTINE_DIR)
    _makedirs(
        quarantine_dir,
        exist_ok=True,
        before_mutation=before_mutation,
    )

    folder_name = os.path.basename(os.path.abspath(src_path))
    target_path = os.path.join(quarantine_dir, folder_name)
    counter = 1
    while os.path.exists(target_path):
        target_path = os.path.join(quarantine_dir, f"{folder_name}_{counter}")
        counter += 1
    return target_path


def manifest_trace_summary(files: Iterable[DownloadFile]) -> str:
    """One-line disc-coverage summary of a download manifest (log-only).

    Renders ``files=<n> discs={<disk_no>:<count>,...}`` so ``MANIFEST-TRACE``
    log lines expose, at each lifecycle seam, whether a multi-disc grab is
    under-covering (issue: partial-disc manifest → false ``untracked_audio``).
    Purely diagnostic — never influences a decision.
    """
    file_list = list(files)
    counts: dict[Any, int] = {}
    for file in file_list:
        key = getattr(file, "disk_no", None)
        counts[key] = counts.get(key, 0) + 1
    disc_str = ",".join(
        f"{key}:{counts[key]}"
        for key in sorted(counts, key=lambda value: (value is None, value))
    )
    return f"files={len(file_list)} discs={{{disc_str}}}"


def audio_relative_paths(root: str) -> list[str]:
    """Return relative audio paths under ``root`` in stable order."""
    paths: list[str] = []
    if not os.path.isdir(root):
        return paths
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            if not _is_audio_path(full_path):
                continue
            paths.append(os.path.relpath(full_path, root))
    return sorted(paths)


def tracked_audio_paths_for_downloads(files: Iterable[DownloadFile]) -> list[str]:
    """Return staged relative paths for the selected download audio files."""
    paths: list[str] = []
    for file in files:
        rel = _safe_relpath(staged_filename(file))
        if rel is not None and _is_audio_path(rel):
            paths.append(rel)
    return sorted(set(paths))


def tracked_audio_paths_from_validation_items(
    items: Iterable[dict[str, Any]],
    *,
    root: str,
) -> list[str]:
    """Recover a manifest from beets validation items."""
    root_abs = os.path.abspath(root)
    paths: list[str] = []
    for item in items:
        raw_path = item.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            continue
        candidate = raw_path
        if os.path.isabs(candidate):
            candidate_abs = os.path.abspath(candidate)
            try:
                common = os.path.commonpath([root_abs, candidate_abs])
            except ValueError:
                common = ""
            if common == root_abs:
                candidate = os.path.relpath(candidate_abs, root_abs)
            else:
                # Beets validation items are captured before rejection moves
                # the folder into failed_imports. Recover the staged basename
                # instead of dropping the manifest and falling back to count.
                candidate = os.path.basename(candidate)
        rel = _safe_relpath(candidate)
        if rel is not None and _is_audio_path(rel):
            paths.append(rel)
    return sorted(set(paths))


def check_audio_manifest(root: str, allowed_audio: Iterable[str]) -> ManifestCheck:
    allowed = {rel for rel in (_safe_relpath(p) for p in allowed_audio) if rel}
    actual = set(audio_relative_paths(root))
    return ManifestCheck(
        extra_audio=sorted(actual - allowed),
        missing_audio=sorted(allowed - actual),
    )


def move_failed_import_curated(
    src_path: str,
    *,
    allowed_audio: Iterable[str],
    scenario: str | None = None,
    quarantine_root: str | None = None,
    before_mutation: MutationCheckpoint | None = None,
) -> CuratedMoveResult | None:
    """Move curated files into the Wrong Matches quarantine root.

    Curated means the accepted audio manifest plus non-audio sidecars. Audio
    files not present in ``allowed_audio`` are skipped by the main move and
    never enter Wrong Matches ALONGSIDE it — the caller's manifest guard is
    expected to have already proven an exact match, so this never routes
    anything to a second, silent destination (issue #1077, D1: kept implies
    visible in the worklist, by construction).

    Never raises post-mutation (issue #1077, B1): a benign non-audio
    subdirectory left as an empty shell by the move loop is pruned, not
    mistaken for a leftover. In the genuinely-unexpected case where real
    content still survives that pruning — including an out-of-manifest
    audio file the main loop deliberately skipped — "kept implies visible"
    outranks manifest purity: it is swept into the SAME ``wrong_matches/``
    destination rather than raising and stranding the album with no
    ``download_log`` row, no denylist write, and no requeue. See
    ``CuratedMoveResult.anomaly``.
    """
    src_path = os.path.abspath(src_path)
    if not os.path.isdir(src_path):
        return None

    allowed = {rel for rel in (_safe_relpath(p) for p in allowed_audio) if rel}
    target_path = _allocate_target(
        src_path,
        quarantine_root=quarantine_root,
        before_mutation=before_mutation,
    )
    _makedirs(
        target_path,
        exist_ok=False,
        before_mutation=before_mutation,
    )

    moved: list[tuple[str, str]] = []
    try:
        for dirpath, _dirnames, filenames in os.walk(src_path):
            rel_dir = os.path.relpath(dirpath, src_path)
            ordered_filenames = (
                sorted(filenames)
                if before_mutation is not None
                else filenames
            )
            for filename in ordered_filenames:
                full_src = os.path.join(dirpath, filename)
                rel = filename if rel_dir == "." else os.path.join(rel_dir, filename)
                rel = os.path.normpath(rel)
                if _is_audio_path(rel) and rel not in allowed:
                    continue
                full_dst = os.path.join(target_path, rel)
                _makedirs(
                    os.path.dirname(full_dst),
                    exist_ok=True,
                    before_mutation=before_mutation,
                )
                _move(
                    full_src,
                    full_dst,
                    before_mutation=before_mutation,
                )
                moved.append((full_dst, full_src))
    except Exception:
        if before_mutation is not None:
            before_mutation()
        for full_dst, full_src in reversed(moved):
            if os.path.exists(full_dst):
                _makedirs(
                    os.path.dirname(full_src),
                    exist_ok=True,
                    before_mutation=before_mutation,
                )
                try:
                    _move(
                        full_dst,
                        full_src,
                        before_mutation=before_mutation,
                    )
                except Exception:
                    logger.exception(
                        "Failed to roll back curated failed-import move %s",
                        full_dst,
                    )
        if before_mutation is None:
            shutil.rmtree(target_path, ignore_errors=True)
        else:
            _remove_tree(
                target_path,
                before_mutation=before_mutation,
            )
        raise

    anomaly: str | None = None
    if os.path.exists(src_path):
        # Prune empty directory skeletons FIRST (issue #1077, B1): the move
        # loop above relocates files but never removes the directories it
        # walked through, so a benign non-audio subdirectory (e.g.
        # ``Scans/``) with everything already moved out of it would
        # otherwise read as "leftovers" despite nothing untracked
        # surviving. This is the exact reproduction the reviewer proved
        # against the real Lane A entry point.
        _prune_empty_dirs(src_path, before_mutation=before_mutation)
        with os.scandir(src_path) as entries:
            has_leftovers = any(entries)
        if has_leftovers:
            # Genuinely unexpected — the caller's manifest guard
            # (``_check_staged_audio_manifest`` in
            # ``lib/download_validation.py``) is expected to have already
            # proven the staged folder's actual audio exactly equals
            # ``allowed_audio`` before Lane A (the canonical automation
            # caller) can be reached, and the sealed canonical-processing
            # invariant (CLAUDE.md #9) means nothing can add files
            # afterward there. The non-canonical staged lane (YouTube
            # rescue / operator-staged, ``_evaluate_staged_path_readiness``)
            # only constrains audio, so a non-audio residue is a producible
            # world there. Either way this must never raise post-mutation:
            # a raise here used to strand the album in ``wrong_matches/``
            # with zero download_log rows, zero denylist writes, and no
            # requeue — the exact invisible-quarantine pathology this issue
            # kills. Sweep the residue into the SAME destination instead
            # and surface the anomaly through the caller's own validation
            # detail, never a stack trace.
            logger.warning(
                "Curated move left untracked content behind in %r despite "
                "an exact allowed_audio match — sweeping into %r",
                src_path,
                target_path,
            )
            try:
                _sweep_residue_into_destination(
                    src_path, target_path, before_mutation=before_mutation,
                )
            except Exception:
                logger.exception(
                    "Failed to sweep curated-move residue from %r into %r",
                    src_path,
                    target_path,
                )
            with os.scandir(src_path) as remaining:
                has_leftovers = any(remaining)
            anomaly = (
                "curated move left untracked content behind despite an "
                "exact allowed_audio match; swept into the wrong_matches "
                "quarantine destination"
                + (" (incompletely — some residue could not be moved)"
                   if has_leftovers else "")
            )
        if not has_leftovers:
            if before_mutation is None:
                shutil.rmtree(src_path, ignore_errors=True)
            else:
                before_mutation()
                os.rmdir(src_path)
        # else: best-effort — leave whatever the sweep genuinely could not
        # move rather than raising; the rejection record must still be
        # written regardless (never block on cosmetic cleanup).

    logger.info(
        "Curated rejected import moved to: %s (scenario=%s)",
        target_path,
        scenario,
    )
    return CuratedMoveResult(target_path=target_path, anomaly=anomaly)


def move_failed_import_whole(
    src_path: str,
    *,
    scenario: str | None = None,
    quarantine_root: str | None = None,
    before_mutation: MutationCheckpoint | None = None,
) -> str | None:
    """Atomically retain one complete rejected source directory.

    Unlike curated Wrong Matches moves, this has no untracked-file
    distinction: the complete source, including anything outside the
    downloaded manifest, moves as one unit — the reviewable folder must be
    what was actually rejected (issue #1077, D4: world failures with a
    reviewable folder move whole, not curated). A directory rename is
    therefore both simpler and safer. ``os.rename`` never falls back to a
    cross-filesystem copy, so failure leaves the authoritative source
    untouched instead of creating a split retained state.
    """
    src_path = os.path.abspath(src_path)
    if not os.path.isdir(src_path):
        return None
    target_path = _allocate_target(
        src_path,
        quarantine_root=quarantine_root,
        before_mutation=before_mutation,
    )
    if before_mutation is not None:
        before_mutation()
    os.rename(src_path, target_path)
    logger.info(
        "Complete rejected import moved to: %s (scenario=%s)",
        target_path,
        scenario,
    )
    return target_path
