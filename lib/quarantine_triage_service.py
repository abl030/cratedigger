"""Read-only lifecycle triage for unreferenced quarantine folders.

The disk reaper protects both ``failed_imports`` and ``wrong_matches`` forever.
This service closes that lifecycle loop without making an irreversible
decision: it compares the immediate album directories on disk with the
currently visible Wrong Matches projection and surfaces only those that have no
reference.
"""

from __future__ import annotations

import os
from typing import Protocol, TYPE_CHECKING

import msgspec

from lib.validation_envelope import decode_validation_envelope
from lib.wrong_match_policy import WRONG_MATCH_QUARANTINE_DIR
from lib.wrong_matches import wrong_match_row_is_visible

if TYPE_CHECKING:
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
    folders: list[QuarantineFolder]
    special_buckets: list[str]


class _WrongMatchesDB(Protocol):
    def get_wrong_matches(self) -> "list[WrongMatchCandidateRow]": ...


def _configured_download_dir(download_dir: str | None) -> str:
    if download_dir is None:
        try:
            from lib.config import read_runtime_config

            download_dir = read_runtime_config().slskd_download_dir
        except Exception as exc:
            raise QuarantineScanError(
                "Could not read runtime configuration for quarantine scan"
            ) from exc
    if not download_dir:
        raise QuarantineScanError(
            "slskd download directory is not configured"
        )
    return os.path.abspath(download_dir)


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
) -> QuarantineTriageResult:
    """Return immediate quarantine album folders absent from Wrong Matches.

    The function never deletes, mutates, or infers ownership. A DB, decode, or
    filesystem error aborts the whole view so adapters cannot misreport a
    partial result as a trustworthy orphan list.
    """
    configured_dir = _configured_download_dir(download_dir)
    quarantine_root = os.path.join(
        configured_dir, FAILED_IMPORTS_DIRECTORY,
    )
    wrong_matches_root = os.path.join(
        configured_dir, WRONG_MATCH_QUARANTINE_DIR,
    )
    quarantine_roots = (
        (quarantine_root, SPECIAL_QUARANTINE_BUCKETS),
        (wrong_matches_root, ()),
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
        folders=folders,
        special_buckets=list(SPECIAL_QUARANTINE_BUCKETS),
    )
