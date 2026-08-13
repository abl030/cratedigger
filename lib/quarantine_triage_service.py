"""Read-only lifecycle triage for unreferenced quarantine folders.

``lib.slskd_transfers``'s disk reaper protects ``failed_imports`` and
``wrong_matches`` under the slskd download dir forever (issue #571) — that
reaper never walks the processing tree at all, so its two processing-side
counterparts have no automated protection. Post-rejection cleanup
(``lib.wrong_match_cleanup_service.cleanup_wrong_match``, reached from
``lib.download_rejection`` and ``scripts.importer``) does automatically
rmtree a delete-eligible REFERENCED folder there right after its own
rejection — but no automated sweep ever revisits an UNREFERENCED folder in
either processing-side root, which is exactly why this service's operator-
facing sweep matters there. This service closes that lifecycle loop
without making an irreversible decision: it compares the immediate album
directories on disk with the currently visible Wrong Matches projection and
surfaces only those that have no reference.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Protocol

import msgspec

from lib.processing_paths import processing_albums_dir
from lib.validation_envelope import decode_validation_envelope
from lib.wrong_match_policy import WRONG_MATCH_QUARANTINE_DIR
from lib.wrong_matches import wrong_match_row_is_visible

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db.rows import WrongMatchCandidateRow


FAILED_IMPORTS_DIRECTORY = "failed_imports"
SPECIAL_QUARANTINE_BUCKETS: tuple[str, ...] = (
    "bad_files",
    "untracked_audio",
)


class QuarantineScanError(RuntimeError):
    """Raised when a complete, trustworthy quarantine view is unavailable."""


class QuarantineFolder(msgspec.Struct, frozen=True):
    """One immediate unreferenced album directory in quarantine."""

    name: str
    path: str
    mtime_ns: int


class QuarantineTriageResult(msgspec.Struct, frozen=True):
    """Stable wire result shared by CLI and HTTP adapters."""

    quarantine_root: str
    wrong_matches_root: str
    processing_failed_imports_root: str
    processing_wrong_matches_root: str
    folders: list[QuarantineFolder]
    special_buckets: list[str]


class _WrongMatchesDB(Protocol):
    def get_wrong_matches(self) -> list[WrongMatchCandidateRow]: ...


def _read_runtime_config() -> CratediggerConfig:
    """Read the runtime config once, mapping any failure to the shared
    quarantine-scan-unavailable error (issue #1122 F5: this used to be
    copy-pasted per-field, calling ``read_runtime_config()`` a second time
    whenever both ``download_dir`` and ``processing_dir`` defaulted).
    """
    try:
        from lib.config import read_runtime_config

        return read_runtime_config()
    except Exception as exc:
        raise QuarantineScanError(
            "Could not read runtime configuration for quarantine scan"
        ) from exc


def _configured_download_dir(
    download_dir: str | None, config: CratediggerConfig | None = None,
) -> str:
    if download_dir is None:
        cfg = config if config is not None else _read_runtime_config()
        download_dir = cfg.slskd_download_dir
    if not download_dir:
        raise QuarantineScanError(
            "slskd download directory is not configured"
        )
    return os.path.abspath(download_dir)


def _configured_processing_dir(
    processing_dir: str | None, config: CratediggerConfig | None = None,
) -> str:
    if processing_dir is None:
        cfg = config if config is not None else _read_runtime_config()
        processing_dir = cfg.processing_dir
    if not processing_dir:
        raise QuarantineScanError(
            "processing directory is not configured"
        )
    return os.path.abspath(processing_dir)


def _immediate_quarantine_root_for_reference(
    failed_path: str,
    *,
    download_dir: str,
    quarantine_root: str,
    special_buckets: tuple[str, ...] = (),
) -> str | None:
    """Map a relative/absolute reference to its immediate album root.

    Descendant references protect the containing immediate folder. Paths
    outside the configured quarantine and code-owned special buckets do not
    claim an album root.
    """
    candidate = failed_path
    if not os.path.isabs(candidate):
        candidate = os.path.join(download_dir, candidate)
    candidate = os.path.abspath(os.path.normpath(candidate))
    try:
        if os.path.commonpath([candidate, quarantine_root]) != quarantine_root:
            return None
    except ValueError:
        return None

    relative = os.path.relpath(candidate, quarantine_root)
    if relative in ("", ".") or relative == os.pardir \
            or relative.startswith(os.pardir + os.sep):
        return None
    first_component = relative.split(os.sep, 1)[0]
    if first_component in special_buckets:
        return None
    return os.path.join(quarantine_root, first_component)


def _visible_wrong_match_roots(
    db: _WrongMatchesDB,
    *,
    download_dir: str,
    quarantine_roots: tuple[tuple[str, tuple[str, ...]], ...],
) -> set[str]:
    try:
        rows = db.get_wrong_matches()
    except Exception as exc:
        raise QuarantineScanError(
            "Could not read visible Wrong Matches references"
        ) from exc

    referenced: set[str] = set()
    try:
        for row in rows:
            if not wrong_match_row_is_visible(row):
                continue
            failed_path = decode_validation_envelope(
                row.get("validation_result")
            ).failed_path
            if not failed_path:
                continue
            for quarantine_root, special_buckets in quarantine_roots:
                album_root = _immediate_quarantine_root_for_reference(
                    failed_path,
                    download_dir=download_dir,
                    quarantine_root=quarantine_root,
                    special_buckets=special_buckets,
                )
                if album_root is not None:
                    referenced.add(album_root)
                    break
    except Exception as exc:
        raise QuarantineScanError(
            "Could not decode visible Wrong Matches references"
        ) from exc
    return referenced


def _immediate_quarantine_folders(
    quarantine_root: str,
    *,
    special_buckets: tuple[str, ...] = (),
) -> list[QuarantineFolder]:
    try:
        entries_context = os.scandir(quarantine_root)
    except FileNotFoundError:
        # A genuinely absent quarantine root is a complete empty state.
        return []
    except OSError as exc:
        raise QuarantineScanError(
            f"Could not scan quarantine directory {quarantine_root}: {exc}"
        ) from exc

    try:
        with entries_context as entries:
            folders: list[QuarantineFolder] = []
            for entry in entries:
                if entry.name in special_buckets:
                    continue
                if not entry.is_dir(follow_symlinks=False):
                    continue
                stat = entry.stat(follow_symlinks=False)
                folders.append(QuarantineFolder(
                    name=entry.name,
                    path=os.path.join(quarantine_root, entry.name),
                    mtime_ns=stat.st_mtime_ns,
                ))
    except OSError as exc:
        # Once scandir opened successfully, any disappearance/error means the
        # snapshot is partial and cannot safely be described as empty.
        raise QuarantineScanError(
            f"Could not scan quarantine directory {quarantine_root}: {exc}"
        ) from exc
    folders.sort(key=lambda folder: (folder.name, folder.path))
    return folders


def list_unreferenced_quarantine_folders(
    db: _WrongMatchesDB,
    download_dir: str | None = None,
    processing_dir: str | None = None,
) -> QuarantineTriageResult:
    """Return immediate quarantine album folders absent from Wrong Matches.

    Scans four protected roots, two bases times two markers each: the
    slskd-download-dir-rooted ``failed_imports/`` and ``wrong_matches/``
    (no current writer targets ``failed_imports/`` any more — the historical
    cohort that lives there predates issue #1077 — but ``wrong_matches/``
    is still reachable when a rejection is staged directly under the
    download dir), plus the processing-side
    ``<processing_dir>/albums/failed_imports/`` and
    ``<processing_dir>/albums/wrong_matches/`` — where every CURRENT kept
    rejection actually lands (issue #1077; see
    ``lib.import_manifest._allocate_target``). Both processing-side roots
    share the same code-owned ``bad_files``/``untracked_audio`` exclusion
    as the download-dir-rooted ``failed_imports/`` (issue #1122 F3: a live
    ``failed_imports/bad_files/`` entry on the processing side proved the
    bucket is not download-dir-specific).

    ``lib.fs_authority.open_configured_quarantine_directory`` separately
    enumerates three bases (adding ``beets_staging_dir``) for a DIFFERENT
    purpose — safely opening one caller-supplied candidate path under an
    O_NOFOLLOW descriptor. That is a single-path containment check; this
    function performs a plain immediate-children directory listing across
    every configured root for operator review. The ``beets_staging_dir``
    pair is deliberately NOT scanned here: its legacy quarantine folders
    predate the current rejection pipeline and have not been shown to fit
    this scan's DB-reference model uniformly. That is a known, stated gap,
    not an oversight (issue #1122 F3) — unifying the two enumerations would
    require reshaping one of their call shapes and is out of scope for this
    fix, which is scoped to adding the missing LIVE roots.

    The function never deletes, mutates, or infers ownership. A DB, decode, or
    filesystem error aborts the whole view so adapters cannot misreport a
    partial result as a trustworthy orphan list.
    """
    config = (
        _read_runtime_config()
        if download_dir is None or processing_dir is None
        else None
    )
    configured_dir = _configured_download_dir(download_dir, config)
    configured_processing_dir = _configured_processing_dir(
        processing_dir, config,
    )
    quarantine_root = os.path.join(
        configured_dir, FAILED_IMPORTS_DIRECTORY,
    )
    wrong_matches_root = os.path.join(
        configured_dir, WRONG_MATCH_QUARANTINE_DIR,
    )
    processing_albums_root = processing_albums_dir(configured_processing_dir)
    processing_failed_imports_root = os.path.join(
        processing_albums_root, FAILED_IMPORTS_DIRECTORY,
    )
    processing_wrong_matches_root = os.path.join(
        processing_albums_root, WRONG_MATCH_QUARANTINE_DIR,
    )
    quarantine_roots = (
        (quarantine_root, SPECIAL_QUARANTINE_BUCKETS),
        (wrong_matches_root, ()),
        (processing_failed_imports_root, SPECIAL_QUARANTINE_BUCKETS),
        (processing_wrong_matches_root, ()),
    )
    referenced = _visible_wrong_match_roots(
        db,
        download_dir=configured_dir,
        quarantine_roots=quarantine_roots,
    )
    folders = [
        folder
        for root, special_buckets in quarantine_roots
        for folder in _immediate_quarantine_folders(
            root,
            special_buckets=special_buckets,
        )
        if folder.path not in referenced
    ]
    folders.sort(key=lambda folder: (folder.name, folder.path))
    return QuarantineTriageResult(
        quarantine_root=quarantine_root,
        wrong_matches_root=wrong_matches_root,
        processing_failed_imports_root=processing_failed_imports_root,
        processing_wrong_matches_root=processing_wrong_matches_root,
        folders=folders,
        special_buckets=list(SPECIAL_QUARANTINE_BUCKETS),
    )
