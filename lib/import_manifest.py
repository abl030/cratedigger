"""Filesystem manifest guards for import candidates."""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from typing import Any, Iterable, TYPE_CHECKING

from lib.quality import AUDIO_EXTENSIONS_DOTTED
from lib.staged_album import staged_filename

if TYPE_CHECKING:
    from lib.grab_list import DownloadFile

logger = logging.getLogger("cratedigger")

_BAD_FILE_SCENARIOS = frozenset({"audio_corrupt", "spectral_reject"})
_LEFTOVER_QUARANTINE_DIR = "untracked_audio"


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


def _allocate_target(src_path: str, *, scenario: str | None) -> str:
    parent_dir = os.path.dirname(os.path.abspath(src_path))
    failed_imports_dir = os.path.join(parent_dir, "failed_imports")
    if scenario in _BAD_FILE_SCENARIOS:
        failed_imports_dir = os.path.join(failed_imports_dir, "bad_files")
    os.makedirs(failed_imports_dir, exist_ok=True)

    folder_name = os.path.basename(os.path.abspath(src_path))
    target_path = os.path.join(failed_imports_dir, folder_name)
    counter = 1
    while os.path.exists(target_path):
        target_path = os.path.join(failed_imports_dir, f"{folder_name}_{counter}")
        counter += 1
    return target_path


def _allocate_leftover_target(src_path: str) -> str:
    parent_dir = os.path.dirname(os.path.abspath(src_path))
    root = os.path.join(parent_dir, "failed_imports", _LEFTOVER_QUARANTINE_DIR)
    os.makedirs(root, exist_ok=True)

    folder_name = os.path.basename(os.path.abspath(src_path))
    target_path = os.path.join(root, folder_name)
    counter = 1
    while os.path.exists(target_path):
        target_path = os.path.join(root, f"{folder_name}_{counter}")
        counter += 1
    return target_path


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


def tracked_audio_paths_for_downloads(files: Iterable["DownloadFile"]) -> list[str]:
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
        raw_path = item.get("path") if isinstance(item, dict) else None
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
) -> str | None:
    """Move only curated files into failed_imports and quarantine leftovers.

    Curated means the accepted audio manifest plus non-audio sidecars. Audio
    files not present in ``allowed_audio`` never enter Wrong Matches.
    """
    src_path = os.path.abspath(src_path)
    if not os.path.isdir(src_path):
        return None

    allowed = {rel for rel in (_safe_relpath(p) for p in allowed_audio) if rel}
    target_path = _allocate_target(src_path, scenario=scenario)
    os.makedirs(target_path, exist_ok=False)

    moved: list[tuple[str, str]] = []
    try:
        for dirpath, _dirnames, filenames in os.walk(src_path):
            rel_dir = os.path.relpath(dirpath, src_path)
            for filename in filenames:
                full_src = os.path.join(dirpath, filename)
                rel = filename if rel_dir == "." else os.path.join(rel_dir, filename)
                rel = os.path.normpath(rel)
                if _is_audio_path(rel) and rel not in allowed:
                    continue
                full_dst = os.path.join(target_path, rel)
                os.makedirs(os.path.dirname(full_dst), exist_ok=True)
                shutil.move(full_src, full_dst)
                moved.append((full_dst, full_src))
    except Exception:
        for full_dst, full_src in reversed(moved):
            if os.path.exists(full_dst):
                os.makedirs(os.path.dirname(full_src), exist_ok=True)
                try:
                    shutil.move(full_dst, full_src)
                except Exception:
                    logger.exception(
                        "Failed to roll back curated failed-import move %s",
                        full_dst,
                    )
        shutil.rmtree(target_path, ignore_errors=True)
        raise

    if os.path.exists(src_path):
        with os.scandir(src_path) as entries:
            has_leftovers = any(entries)
        if has_leftovers:
            leftover_target = _allocate_leftover_target(src_path)
            shutil.move(src_path, leftover_target)
            logger.warning(
                "Quarantined untracked import leftovers: %s -> %s",
                src_path,
                leftover_target,
            )
        else:
            shutil.rmtree(src_path, ignore_errors=True)

    logger.info("Curated failed import moved to: %s", target_path)
    return target_path
