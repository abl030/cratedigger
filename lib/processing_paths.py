"""Shared path helpers for active download processing and staging."""

from __future__ import annotations

import os
import re

AUTO_IMPORT_STAGING_SUBDIR = "auto-import"
POST_VALIDATION_STAGING_SUBDIR = "post-validation"
DUPLICATE_REMOVE_GUARD_SUBDIR = "duplicate-remove-guard"


def sanitize_processing_folder_name(folder_name: str) -> str:
    """Sanitize a filesystem path component for local processing paths."""
    return re.sub(r'[<>:."/\\|?*]', "", folder_name).strip()


def normalize_processing_path(path: str) -> str:
    """Return a normalized absolute path without resolving symlinks."""
    return os.path.abspath(os.path.normpath(path))


def path_is_within_root(path: str, root: str) -> bool:
    """Return True when ``path`` is located under ``root``."""
    if not root:
        return False
    abs_path = normalize_processing_path(path)
    abs_root = normalize_processing_path(root)
    try:
        return os.path.commonpath([abs_path, abs_root]) == abs_root
    except ValueError:
        return False


def canonical_processing_path(
    *,
    artist: str,
    title: str,
    year: str,
    slskd_download_dir: str,
) -> str:
    """Return the canonical local processing directory for a completed album."""
    import_folder_name = sanitize_processing_folder_name(
        f"{artist} - {title} ({year})",
    )
    return os.path.join(slskd_download_dir, import_folder_name)


def stage_to_ai_root(
    *,
    staging_dir: str,
    auto_import: bool | None = None,
) -> str:
    """Return the root staging directory for a given validation branch."""
    if auto_import is None:
        return staging_dir
    subdir = (
        AUTO_IMPORT_STAGING_SUBDIR
        if auto_import
        else POST_VALIDATION_STAGING_SUBDIR
    )
    return os.path.join(staging_dir, subdir)


def stage_to_ai_path(
    *,
    artist: str,
    title: str,
    staging_dir: str,
    request_id: int | None = None,
    auto_import: bool | None = None,
) -> str:
    """Return the beets staging destination for an album."""
    artist_dir = sanitize_processing_folder_name(artist)
    album_dir = sanitize_processing_folder_name(title)
    if request_id is not None:
        album_dir = f"{album_dir} [request-{request_id}]"
    return os.path.join(
        stage_to_ai_root(staging_dir=staging_dir, auto_import=auto_import),
        artist_dir,
        album_dir,
    )


def duplicate_remove_guard_root(*, staging_dir: str) -> str:
    """Return the quarantine root for duplicate-remove guard failures."""
    return os.path.join(staging_dir, DUPLICATE_REMOVE_GUARD_SUBDIR)


def duplicate_remove_guard_path(
    *,
    staging_dir: str,
    source_path: str,
    request_id: int | None = None,
    attempt_id: int | None = None,
) -> str:
    """Return a diagnosable quarantine path for a guarded duplicate failure."""
    basename = os.path.basename(normalize_processing_path(source_path))
    safe_basename = sanitize_processing_folder_name(basename) or "staged-files"
    parts: list[str] = []
    if request_id is not None:
        parts.append(f"request-{request_id}")
    if attempt_id is not None:
        parts.append(f"attempt-{attempt_id}")
    parts.append(safe_basename)
    return os.path.join(
        duplicate_remove_guard_root(staging_dir=staging_dir),
        " - ".join(parts),
    )


def directory_has_entries(path: str) -> bool:
    """Return True when ``path`` exists and contains at least one entry."""
    if not os.path.isdir(path):
        return False
    with os.scandir(path) as entries:
        return any(True for _ in entries)
