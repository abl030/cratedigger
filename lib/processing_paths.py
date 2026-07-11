"""Shared path helpers for active download processing and staging."""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Protocol, Sequence

AUTO_IMPORT_STAGING_SUBDIR = "auto-import"
POST_VALIDATION_STAGING_SUBDIR = "post-validation"
DUPLICATE_REMOVE_GUARD_SUBDIR = "duplicate-remove-guard"


class CanonicalFolderFile(Protocol):
    """File identity fields used to scope a canonical processing folder."""

    @property
    def username(self) -> str: ...

    @property
    def filename(self) -> str: ...


class CanonicalFolderRow(Protocol):
    """Album fields that uniquely derive an attempt's processing folder."""

    @property
    def artist(self) -> str: ...

    @property
    def title(self) -> str: ...

    @property
    def year(self) -> str: ...

    @property
    def files(self) -> Sequence[CanonicalFolderFile]: ...


def sanitize_processing_folder_name(folder_name: str) -> str:
    """Sanitize a filesystem path component for local processing paths."""
    return re.sub(r'[<>:."/\\|?*]', "", folder_name).strip()


def normalize_source_dirs(values: Sequence[object]) -> list[str]:
    """Return unique non-empty remote source directories in input order."""
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


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


def attempt_fingerprint(pairs: Sequence[tuple[str, str]]) -> str:
    """Short deterministic fingerprint of an attempt's (username, filename) set.

    Mirrors the ``snapshot_fingerprint`` idiom in
    ``lib/quality_evidence.py``: sort the pairs, JSON-encode with no
    whitespace, SHA-256 the UTF-8 bytes, and take the first 8 hex chars —
    enough entropy to distinguish concurrent attempts in a folder name
    while staying short and readable.

    Order-independent (the pairs are sorted before encoding) and
    sensitive to every field: a different source user or a different
    remote path for even one track produces a different fingerprint. The
    empty set hashes the JSON encoding of ``[]`` (a stable, defined
    digest), not an error — same documented behavior as
    ``snapshot_fingerprint``.

    Used to key each download attempt's canonical processing folder to
    its own manifest (issue #550 phase 2) — see ``canonical_processing_path``.
    """
    encoded = json.dumps(
        sorted(pairs),
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:8]


def canonical_processing_path(
    *,
    artist: str,
    title: str,
    year: str,
    slskd_download_dir: str,
    attempt_fingerprint: str = "",
) -> str:
    """Return the canonical local processing directory for a completed album.

    When ``attempt_fingerprint`` is non-empty, it is appended as a
    `` [<fp>]`` suffix so each download attempt (a distinct manifest of
    (username, filename) pairs) materializes into its own folder — no
    attempt ever validates against files another attempt placed there
    (issue #550 phase 2: the canonical folder used to be keyed only on
    artist/title/year, so a stale prior attempt's leftover audio could
    silently blend into a fresh attempt's validation scope). Empty (the
    default) preserves the bare ``"Artist - Title (Year)"`` folder name
    for callers that classify an already-persisted path rather than
    compute a fresh one.
    """
    import_folder_name = sanitize_processing_folder_name(
        f"{artist} - {title} ({year})",
    )
    if attempt_fingerprint:
        suffix = f" [{attempt_fingerprint}]"
        # ext4 caps filenames at 255 bytes; a near-limit sanitized name that
        # fit before must not start failing os.makedirs once suffixed
        # (codex review r2) — truncate the base on a character boundary.
        max_base_bytes = 255 - len(suffix.encode("utf-8"))
        base_bytes = import_folder_name.encode("utf-8")
        if len(base_bytes) > max_base_bytes:
            import_folder_name = base_bytes[:max_base_bytes].decode(
                "utf-8", errors="ignore").rstrip()
        import_folder_name = f"{import_folder_name}{suffix}"
    return os.path.join(slskd_download_dir, import_folder_name)


def canonical_folder_for_row(row: CanonicalFolderRow, root: str) -> str:
    """Derive one attempt-scoped canonical folder from an album row.

    Materialization and active-download reaper protection both call this
    function so their artist/title/year and exact file-identity projection
    cannot drift independently (issue #573 W1).
    """
    fingerprint = attempt_fingerprint([
        (file.username, file.filename) for file in row.files
    ])
    return canonical_processing_path(
        artist=row.artist,
        title=row.title,
        year=row.year,
        slskd_download_dir=root,
        attempt_fingerprint=fingerprint,
    )


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
