"""Shared path helpers for active download processing and staging."""

from __future__ import annotations

import os
import re

AUTO_IMPORT_STAGING_SUBDIR = "auto-import"
POST_VALIDATION_STAGING_SUBDIR = "post-validation"


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


def directory_has_entries(path: str) -> bool:
    """Return True when ``path`` exists and contains at least one entry."""
    if not os.path.isdir(path):
        return False
    with os.scandir(path) as entries:
        return any(True for _ in entries)
