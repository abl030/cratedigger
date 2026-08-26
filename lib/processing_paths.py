"""Shared path helpers for active download processing and staging."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Sequence
from typing import Protocol

AUTO_IMPORT_STAGING_SUBDIR = "auto-import"
POST_VALIDATION_STAGING_SUBDIR = "post-validation"
DUPLICATE_REMOVE_GUARD_SUBDIR = "duplicate-remove-guard"
MAX_PATH_COMPONENT_BYTES = 255
_TRUNCATED_COMPONENT_HASH_CHARS = 12
_MAX_PRESERVED_EXTENSION_BYTES = 16


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


class SourceDirectoryFile(Protocol):
    """Remote directory field required by validation audit projection."""

    @property
    def file_dir(self) -> str: ...


class SourceDirectoryAlbum(Protocol):
    """Album-shaped value whose files carry remote source directories."""

    @property
    def files(self) -> Sequence[SourceDirectoryFile]: ...


def sanitize_processing_folder_name(folder_name: str) -> str:
    """Sanitize a filesystem path component for local processing paths."""
    return re.sub(r'[<>:."/\\|?*]', "", folder_name).strip()


def _bounded_component_bytes(value: str, *, suffix: str = "") -> str:
    """Fit ``value`` plus ``suffix`` within one filesystem name.

    Truncation works on UTF-8 bytes and decodes with ``errors="ignore"`` so
    it never cuts a multibyte code point in half. A digest of the complete
    ``value`` prevents two long names with the same retained prefix from
    collapsing onto one entry, and guarantees a non-empty result even when
    the retained prefix strips away entirely. ``suffix`` is reserved
    verbatim.
    """
    complete = f"{value}{suffix}"
    if len(complete.encode("utf-8")) <= MAX_PATH_COMPONENT_BYTES:
        return complete

    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[
        :_TRUNCATED_COMPONENT_HASH_CHARS
    ]
    reserved = f"~{digest}{suffix}"
    max_prefix_bytes = MAX_PATH_COMPONENT_BYTES - len(reserved.encode("utf-8"))
    if max_prefix_bytes < 0:
        raise ValueError("processing path suffix exceeds filesystem limit")
    prefix = value.encode("utf-8")[:max_prefix_bytes].decode(
        "utf-8", errors="ignore",
    ).rstrip()
    return f"{prefix}{reserved}"


def _bounded_processing_component(value: str, *, suffix: str = "") -> str:
    """Fit one sanitized staging component within ext4's byte limit.

    ``suffix`` is reserved verbatim for request ownership markers such as
    `` [request-42]``.
    """
    return _bounded_component_bytes(
        sanitize_processing_folder_name(value), suffix=suffix,
    )


def bounded_staged_filename(name: str) -> str:
    """Fit one untrusted remote basename within ext4's byte limit.

    A peer names its own files and is free to exceed the local 255-byte cap,
    which otherwise fails the copy into the canonical processing album with
    ``OSError: [Errno 36] File name too long`` — request 8867's tracks were
    372-555 bytes. Unlike a folder component the extension has to survive,
    because beets and the quality pipeline both key on it, so it is reserved
    rather than sanitized away.

    ``name`` must already be a basename; bounding only shortens and appends,
    so it can never introduce a separator or a traversal segment, but it
    does not remove one either. Names already within the cap are returned
    byte-identical, which is load-bearing: ``_canonical_manifest_complete``
    recomputes these names and compares them against the published album's
    own directory listing.
    """
    stem, dot, extension = name.rpartition(".")
    if (
        dot
        and stem
        and len(extension.encode("utf-8")) <= _MAX_PRESERVED_EXTENSION_BYTES
    ):
        return _bounded_component_bytes(stem, suffix=f"{dot}{extension}")
    return _bounded_component_bytes(name)


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


def source_dirs_for_album(album: SourceDirectoryAlbum) -> list[str]:
    """Project one album's unique remote directories for audit evidence."""
    return normalize_source_dirs(
        [file.file_dir for file in album.files if file.file_dir],
    )


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


def attempt_fingerprint_of_files(files: Sequence[CanonicalFolderFile]) -> str:
    """The ONE projection from an attempt's files to its fingerprint.

    ``attempt_fingerprint`` above hashes a pair sequence; this is the
    single place the pipeline decides WHICH pairs an attempt's file
    objects yield. Every canonical-folder computation, ledger row, and
    ``active_download_state`` stamp for the same attempt must agree
    exactly or the folder classifies as ``external`` and strands the
    album (issue #550 phase 2), and the cross-request enqueue guard's
    exact-fingerprint join silently widens to "every accepted key this
    owner ever held" (issue #1196 item 1).

    That agreement used to rest on five separate spellings of
    ``[(f.username, f.filename) for f in files]`` matching each other,
    asserted only in prose. Exactly two docstrings said so in those
    words -- ``lib/download.py`` ("agree BY CONSTRUCTION without a
    second formula") and ``lib/download_recovery.py`` ("MUST derive
    from the same (username, filename) pairs") -- while
    ``lib/pipeline_db/transfer_ledger.py``, ``canonical_folder_for_row``
    below, and the since-deleted
    ``lib/download_materialization.py::_attempt_fingerprint_for`` each
    made the same non-drift claim in their own words. Sharing the
    formula is what makes those claims true rather than merely
    maintained.
    """
    return attempt_fingerprint([(f.username, f.filename) for f in files])


def attempt_fingerprint_or_none(
    files: Sequence[CanonicalFolderFile],
) -> str | None:
    """``attempt_fingerprint_of_files``, or ``None`` for an empty attempt.

    The claim-time writers (``lib.download.build_active_download_state``
    and ``lib.enqueue._enqueue_with_claim_outcome``) persist ``None``
    rather than the empty-set digest when an attempt has no files, so a
    file-less claim never mints an attempt identity that a later
    non-empty attempt could collide with. Both sides of the cross-request
    guard's fingerprint equality are written by these two callers, so the
    empty-attempt policy lives here with the derivation instead of being
    duplicated at each of them (issue #1199 review F9 depends on exactly
    when this returns ``None``).
    """
    return attempt_fingerprint_of_files(files) if files else None


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


def processing_albums_dir(processing_dir: str) -> str:
    """Return the private canonical-album child of a processing root."""
    return os.path.join(processing_dir, "albums")


def processing_preview_dir(processing_dir: str) -> str:
    """Return the private preview-scratch child of a processing root."""
    return os.path.join(processing_dir, "preview")


def canonical_folder_for_row(row: CanonicalFolderRow, root: str) -> str:
    """Derive one attempt-scoped canonical folder from an album row.

    Materialization and active-download reaper protection both call this
    function so their artist/title/year and exact file-identity projection
    cannot drift independently (issue #573 W1).
    """
    fingerprint = attempt_fingerprint_of_files(row.files)
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


def protected_staging_roots(
    *, processing_dir: str, beets_staging_dir: str,
) -> frozenset[str]:
    """Every filesystem root a staged-dir empty-parent prune must never
    remove, however empty it looks (issue #1122, review round 2).

    Two deploy-provisioned roots are shared across lanes and workers: the
    canonical processing albums root (``processing_albums_dir``) and the
    externally provisioned auto-import staging root
    (``stage_to_ai_root(auto_import=True)``) the YouTube rescue worker and
    every other auto-import unit stage into. A prune keyed on only one of
    them silently falls through for the other lane's staged path -- the
    exact shape that let a successful YouTube rescue's empty-parent prune
    remove the shared auto-import root out from under every other in-flight
    request (harness/import_one.py and lib/dispatch/core.py's post-commit
    plan both had this gap before this fix).

    ONE derivation owner: every caller of
    ``lib.dispatch.helpers._cleanup_staged_dir`` and
    ``harness.import_one._cleanup_staged_dir`` computes its protected set
    through this function -- never a hand-built string or a single-root
    special case -- so a future third shared root only needs to be added
    here.

    Residual: this guard stops a staged-dir prune from removing either
    shared root outright, but does not address ownership drift if a root
    is ever removed by some other path -- the auto-import root is
    externally provisioned (not cratedigger-owned) on the live host, and
    ``lib.youtube_ingest_service._default_stage_dir``'s
    ``mkdir(parents=True, exist_ok=True)`` would silently recreate a
    missing root owned by the cratedigger service identity on the next
    rescue.
    """
    return frozenset({
        processing_albums_dir(processing_dir),
        stage_to_ai_root(staging_dir=beets_staging_dir, auto_import=True),
    })


def stage_to_ai_path(
    *,
    artist: str,
    title: str,
    staging_dir: str,
    request_id: int | None = None,
    auto_import: bool | None = None,
) -> str:
    """Return the beets staging destination for an album."""
    artist_dir = _bounded_processing_component(artist)
    request_suffix = (
        f" [request-{request_id}]" if request_id is not None else ""
    )
    album_dir = _bounded_processing_component(title, suffix=request_suffix)
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
