"""Resumable exact-owner filesystem cleanup for automation imports.

The executor consumes an already-persisted cleanup journal. It never chooses a
path, allocates a collision suffix, reconstructs a manifest from request data,
or applies a terminal database outcome. Every filesystem mutation is bracketed
by append-only revision-CAS checkpoints, and every path is reopened through
held no-follow directory descriptors at the mutation boundary.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Literal, Protocol

import msgspec

from lib.fs_authority import (
    FilesystemAuthorityError,
    FsAuthorityCode,
    errno_symbol,
    open_directory_path,
    open_regular_relative,
    open_relative_directory,
    paths_overlap,
    rename_between_directories_noreplace,
    unlink_if_same,
)
from lib.pipeline_db import (
    CleanupJournalReceipt,
    ProcessingCleanupJournalRow,
)

PROCESSING_CLEANUP_REMOVE_SOURCE = "remove_source_tree"
PROCESSING_CLEANUP_QUARANTINE_SOURCE = "quarantine_source_tree"
PROCESSING_CLEANUP_NO_OP = "no_op"
ProcessingCleanupAction = Literal[
    "remove_source_tree",
    "quarantine_source_tree",
    "no_op",
]
CleanupInspectionStatus = Literal["complete", "missing", "uninspectable"]
ProcessingCleanupErrorCode = Literal[
    "invalid_journal",
    "unsupported_action",
    "source_missing",
    "source_uninspectable",
    "destination_collision",
    "destination_uninspectable",
    "manifest_drift",
    "cross_device",
    "filesystem_mutation_failed",
]

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW


def _validate_relative_path(path: str) -> None:
    if (
        not path
        or os.path.isabs(path)
        or path != os.path.normpath(path)
        or "\\" in path
        or any(part in ("", ".", "..") for part in path.split(os.sep))
    ):
        raise ValueError("cleanup manifest path must be canonical and relative")


class CleanupManifestEntry(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """One exact regular file or directory beneath a cleanup source."""

    kind: Literal["file", "directory"]
    relative_path: str
    size_bytes: int | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        _validate_relative_path(self.relative_path)
        if self.kind == "file":
            if self.size_bytes is None or self.size_bytes < 0:
                raise ValueError("file manifest entry needs non-negative size")
            if self.sha256 is None or _SHA256_RE.fullmatch(self.sha256) is None:
                raise ValueError("file manifest entry needs lowercase SHA-256")
        elif self.size_bytes is not None or self.sha256 is not None:
            raise ValueError("directory manifest entry cannot carry file facts")


CleanupExactManifest = tuple[CleanupManifestEntry, ...]


class CleanupSourceInspection(msgspec.Struct, frozen=True, kw_only=True):
    """Typed distinction between complete absence and failed inspection."""

    status: CleanupInspectionStatus
    manifest: CleanupExactManifest = ()
    manifest_hash: str | None = None
    error_code: FsAuthorityCode | None = None
    reason: str | None = None


class ProcessingCleanupError(RuntimeError):
    """Stable fail-closed executor refusal."""

    def __init__(
        self,
        code: ProcessingCleanupErrorCode,
        message: str,
    ) -> None:
        self.code = code
        super().__init__(message)


class ProcessingCleanupJournalDB(Protocol):
    def checkpoint_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        step_progress: Mapping[str, object],
    ) -> ProcessingCleanupJournalRow: ...

    def complete_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        receipt: CleanupJournalReceipt,
    ) -> ProcessingCleanupJournalRow: ...


@dataclass(frozen=True)
class _CleanupStep:
    operation: Literal["unlink", "rmdir"]
    relative_path: str

    @property
    def label(self) -> str:
        return f"{self.operation}:{self.relative_path}"

    @property
    def before_key(self) -> str:
        return f"before:{self.label}"

    @property
    def after_key(self) -> str:
        return f"after:{self.label}"


def cleanup_manifest_hash(manifest: CleanupExactManifest) -> str:
    """Hash the canonical typed manifest representation."""
    payload = msgspec.to_builtins(manifest)
    return hashlib.sha256(msgspec.json.encode(payload)).hexdigest()


def cleanup_manifest_builtins(
    manifest: CleanupExactManifest,
) -> tuple[dict[str, object], ...]:
    """Return the JSON shape accepted by ``CleanupJournalIntent``."""
    value: list[dict[str, object]] = msgspec.to_builtins(manifest)
    return tuple(value)


def _read_opened_file(opened_fd: int) -> tuple[int, str]:
    before = os.fstat(opened_fd)
    try:
        os.lseek(opened_fd, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        while True:
            try:
                chunk = os.read(opened_fd, 1024 * 1024)
            except OSError as exc:
                raise FilesystemAuthorityError(
                    f"cannot read cleanup source: {exc.strerror}",
                    code="read_failed",
                    errno_symbol=errno_symbol(exc),
                ) from exc
            if not chunk:
                break
            digest.update(chunk)
    finally:
        os.lseek(opened_fd, 0, os.SEEK_SET)
    after = os.fstat(opened_fd)
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
    ):
        raise FilesystemAuthorityError(
            "cleanup source changed during inspection",
            code="read_failed",
        )
    return after.st_size, digest.hexdigest()


def _inspect_open_root(root_fd: int) -> CleanupExactManifest:
    entries: list[CleanupManifestEntry] = []

    def walk(relative_dir: str | None) -> None:
        with ExitStack() as scope:
            if relative_dir is None:
                directory_fd = os.dup(root_fd)
                scope.callback(os.close, directory_fd)
            else:
                directory_fd = scope.enter_context(
                    open_relative_directory(root_fd, relative_dir)
                )
            try:
                with os.scandir(directory_fd) as iterator:
                    children = sorted(
                        iterator,
                        key=lambda entry: entry.name,
                    )
            except OSError as exc:
                raise FilesystemAuthorityError(
                    f"cannot inspect cleanup directory: {exc.strerror}",
                    code="read_failed",
                    errno_symbol=errno_symbol(exc),
                ) from exc
            for child in children:
                relative_path = (
                    child.name
                    if relative_dir is None
                    else os.path.join(relative_dir, child.name)
                )
                _validate_relative_path(relative_path)
                try:
                    is_directory = child.is_dir(follow_symlinks=False)
                    is_file = child.is_file(follow_symlinks=False)
                    is_symlink = child.is_symlink()
                except OSError as exc:
                    raise FilesystemAuthorityError(
                        f"cannot inspect cleanup entry {relative_path}: "
                        f"{exc.strerror}",
                        code="read_failed",
                        errno_symbol=errno_symbol(exc),
                    ) from exc
                if is_symlink:
                    raise FilesystemAuthorityError(
                        f"cleanup manifest contains symlink: {relative_path}",
                        code="unsafe_symlink",
                    )
                if is_directory:
                    entries.append(
                        CleanupManifestEntry(
                            kind="directory",
                            relative_path=relative_path,
                        )
                    )
                    walk(relative_path)
                    continue
                if not is_file:
                    raise FilesystemAuthorityError(
                        f"cleanup manifest contains non-regular entry: "
                        f"{relative_path}",
                        code="not_regular_file",
                    )
                opened = open_regular_relative(root_fd, relative_path)
                try:
                    size_bytes, sha256 = _read_opened_file(opened.fd)
                finally:
                    opened.close()
                entries.append(
                    CleanupManifestEntry(
                        kind="file",
                        relative_path=relative_path,
                        size_bytes=size_bytes,
                        sha256=sha256,
                    )
                )

    walk(None)
    return tuple(
        sorted(entries, key=lambda entry: (entry.relative_path, entry.kind))
    )


def inspect_processing_cleanup_source(path: str) -> CleanupSourceInspection:
    """Inspect one exact directory without conflating absence with failure."""
    try:
        _require_canonical_absolute(path, "source_path")
        with open_directory_path(path) as root_fd:
            manifest = _inspect_open_root(root_fd)
    except FilesystemAuthorityError as exc:
        if exc.code == "missing":
            return CleanupSourceInspection(
                status="missing",
                error_code=exc.code,
                reason=str(exc),
            )
        return CleanupSourceInspection(
            status="uninspectable",
            error_code=exc.code,
            reason=str(exc),
        )
    except OSError as exc:
        return CleanupSourceInspection(
            status="uninspectable",
            error_code="read_failed",
            reason=f"{type(exc).__name__}: {exc}",
        )
    return CleanupSourceInspection(
        status="complete",
        manifest=manifest,
        manifest_hash=cleanup_manifest_hash(manifest),
    )


def _require_canonical_absolute(path: str, field: str) -> None:
    if (
        not path
        or not os.path.isabs(path)
        or path != os.path.normpath(path)
        or path == os.sep
    ):
        raise ProcessingCleanupError(
            "invalid_journal",
            f"{field} must be a canonical absolute non-root path",
        )


def _manifest_from_json(
    raw: object,
    *,
    expected_hash: str,
    field: str,
) -> CleanupExactManifest:
    try:
        manifest = msgspec.convert(raw, type=CleanupExactManifest, strict=True)
    except (TypeError, ValueError, msgspec.ValidationError) as exc:
        raise ProcessingCleanupError(
            "invalid_journal",
            f"{field} is not a typed cleanup manifest",
        ) from exc
    canonical = tuple(
        sorted(manifest, key=lambda entry: (entry.relative_path, entry.kind))
    )
    if canonical != manifest:
        raise ProcessingCleanupError(
            "invalid_journal",
            f"{field} is not in canonical order",
        )
    if len({entry.relative_path for entry in manifest}) != len(manifest):
        raise ProcessingCleanupError(
            "invalid_journal",
            f"{field} contains duplicate paths",
        )
    if cleanup_manifest_hash(manifest) != expected_hash:
        raise ProcessingCleanupError(
            "invalid_journal",
            f"{field} hash does not match its journaled bytes",
        )
    return manifest


def _typed_action(value: str) -> ProcessingCleanupAction:
    if value == PROCESSING_CLEANUP_REMOVE_SOURCE:
        return PROCESSING_CLEANUP_REMOVE_SOURCE
    if value == PROCESSING_CLEANUP_QUARANTINE_SOURCE:
        return PROCESSING_CLEANUP_QUARANTINE_SOURCE
    if value == PROCESSING_CLEANUP_NO_OP:
        return PROCESSING_CLEANUP_NO_OP
    raise ProcessingCleanupError(
        "unsupported_action",
        f"unsupported processing cleanup action: {value}",
    )


def _require_inspection_complete(
    inspection: CleanupSourceInspection,
    *,
    subject: Literal["source", "destination"],
) -> CleanupExactManifest:
    if inspection.status == "complete":
        return inspection.manifest
    if inspection.status == "missing":
        code: ProcessingCleanupErrorCode = (
            "source_missing"
            if subject == "source"
            else "destination_uninspectable"
        )
        raise ProcessingCleanupError(code, f"cleanup {subject} is missing")
    code = (
        "source_uninspectable"
        if subject == "source"
        else "destination_uninspectable"
    )
    raise ProcessingCleanupError(
        code,
        f"cleanup {subject} cannot be inspected: {inspection.reason}",
    )


def _assert_exact_manifest(
    inspection: CleanupSourceInspection,
    expected: CleanupExactManifest,
    expected_hash: str,
    *,
    subject: Literal["source", "destination"],
) -> None:
    actual = _require_inspection_complete(inspection, subject=subject)
    if (
        inspection.manifest_hash != expected_hash
        or actual != expected
    ):
        raise ProcessingCleanupError(
            "manifest_drift",
            f"cleanup {subject} manifest changed",
        )


def _remove_steps(manifest: CleanupExactManifest) -> tuple[_CleanupStep, ...]:
    file_steps = tuple(
        _CleanupStep("unlink", entry.relative_path)
        for entry in manifest
        if entry.kind == "file"
    )
    directory_paths = sorted(
        (
            entry.relative_path
            for entry in manifest
            if entry.kind == "directory"
        ),
        key=lambda path: (-len(path.split(os.sep)), path),
    )
    directory_steps = tuple(
        _CleanupStep("rmdir", path) for path in directory_paths
    )
    return (*file_steps, *directory_steps, _CleanupStep("rmdir", "."))


def _progress_dict(row: ProcessingCleanupJournalRow) -> dict[str, object]:
    return dict(row["step_progress"])


def _validate_progress(
    progress: Mapping[str, object],
    steps: tuple[_CleanupStep, ...],
) -> int | None:
    allowed = {
        key
        for step in steps
        for key in (step.before_key, step.after_key)
    }
    if set(progress) - allowed or any(value is not True for value in progress.values()):
        raise ProcessingCleanupError(
            "invalid_journal",
            "cleanup progress contains an unknown or non-true checkpoint",
        )
    in_progress: int | None = None
    waiting = False
    for index, step in enumerate(steps):
        before = step.before_key in progress
        after = step.after_key in progress
        if after and not before:
            raise ProcessingCleanupError(
                "invalid_journal",
                "cleanup progress completed a step without its pre-checkpoint",
            )
        if waiting and (before or after):
            raise ProcessingCleanupError(
                "invalid_journal",
                "cleanup progress is not a deterministic prefix",
            )
        if before and not after:
            in_progress = index
            waiting = True
        elif not before:
            waiting = True
    return in_progress


def _entry_for_step(
    manifest: CleanupExactManifest,
    step: _CleanupStep,
) -> CleanupManifestEntry | None:
    if step.relative_path == ".":
        return None
    return next(
        entry
        for entry in manifest
        if entry.relative_path == step.relative_path
    )


def _assert_remove_resume_state(
    source_path: str,
    manifest: CleanupExactManifest,
    manifest_hash: str,
    progress: Mapping[str, object],
    steps: tuple[_CleanupStep, ...],
) -> tuple[CleanupSourceInspection, int | None]:
    in_progress = _validate_progress(progress, steps)
    inspection = inspect_processing_cleanup_source(source_path)
    root_step = steps[-1]
    root_before = root_step.before_key in progress
    root_after = root_step.after_key in progress
    if inspection.status == "uninspectable":
        raise ProcessingCleanupError(
            "source_uninspectable",
            f"cleanup source cannot be inspected: {inspection.reason}",
        )
    if inspection.status == "missing":
        if root_before or root_after:
            return inspection, in_progress
        raise ProcessingCleanupError(
            "source_missing",
            "cleanup source vanished before its root-removal checkpoint",
        )

    expected_remaining = tuple(
        entry
        for entry in manifest
        if not any(
            step.after_key in progress
            and _entry_for_step(manifest, step) == entry
            for step in steps[:-1]
        )
    )
    allowed_manifests = {expected_remaining}
    if in_progress is not None:
        current_entry = _entry_for_step(manifest, steps[in_progress])
        if current_entry is not None:
            allowed_manifests.add(
                tuple(
                    entry
                    for entry in expected_remaining
                    if entry != current_entry
                )
            )
    if inspection.manifest not in allowed_manifests:
        raise ProcessingCleanupError(
            "manifest_drift",
            "cleanup source does not match its checkpointed remaining manifest",
        )
    # At the untouched state, retain the hash comparison as a separate guard:
    # it catches a hash contract mutation even if an equality checker drifts.
    if not progress and inspection.manifest_hash != manifest_hash:
        raise ProcessingCleanupError(
            "manifest_drift",
            "cleanup source hash changed before the first checkpoint",
        )
    return inspection, in_progress


def _append_checkpoint(
    db: ProcessingCleanupJournalDB,
    row: ProcessingCleanupJournalRow,
    *,
    key: str,
) -> ProcessingCleanupJournalRow:
    progress = _progress_dict(row)
    progress[key] = True
    return db.checkpoint_processing_cleanup_journal(
        request_id=row["request_id"],
        job_id=row["job_id"],
        expected_revision=row["revision"],
        step_progress=progress,
    )


def _boundary(
    callback: Callable[[str], None] | None,
    label: str,
) -> None:
    if callback is not None:
        callback(label)


def _fsync(fd: int) -> None:
    try:
        os.fsync(fd)
    except OSError as exc:
        raise ProcessingCleanupError(
            "filesystem_mutation_failed",
            f"cannot flush cleanup namespace: {exc}",
        ) from exc


def _entry_matches_opened(
    opened_fd: int,
    expected: CleanupManifestEntry,
) -> bool:
    size_bytes, sha256 = _read_opened_file(opened_fd)
    return (
        expected.kind == "file"
        and expected.size_bytes == size_bytes
        and expected.sha256 == sha256
    )


def _unlink_exact(
    source_path: str,
    expected: CleanupManifestEntry,
    *,
    mutation_checkpoint: Callable[[], None],
) -> None:
    with open_directory_path(source_path) as root_fd:
        opened = open_regular_relative(root_fd, expected.relative_path)
        try:
            if not _entry_matches_opened(opened.fd, expected):
                raise ProcessingCleanupError(
                    "manifest_drift",
                    f"cleanup file changed: {expected.relative_path}",
                )
            mutation_checkpoint()
            if not unlink_if_same(opened):
                raise ProcessingCleanupError(
                    "manifest_drift",
                    f"cleanup file identity changed: {expected.relative_path}",
                )
            _fsync(opened.parent_fd)
        finally:
            opened.close()


def _rmdir_exact(
    source_path: str,
    relative_path: str,
    *,
    mutation_checkpoint: Callable[[], None],
) -> None:
    if relative_path == ".":
        parent_path, name = os.path.split(source_path)
        relative_parent = None
    else:
        parent_relative = os.path.dirname(relative_path)
        name = os.path.basename(relative_path)
        parent_path = source_path
        relative_parent = parent_relative or None

    with (
        open_directory_path(parent_path) as root_or_parent_fd,
        ExitStack() as scope,
    ):
        if relative_path == "." or relative_parent is None:
            parent_fd = os.dup(root_or_parent_fd)
            scope.callback(os.close, parent_fd)
        else:
            parent_fd = scope.enter_context(
                open_relative_directory(
                    root_or_parent_fd,
                    relative_parent,
                )
            )
        try:
            child_fd = os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise FilesystemAuthorityError(
                f"cannot open cleanup directory {relative_path}: "
                f"{exc.strerror}",
                code="open_failed",
                errno_symbol=errno_symbol(exc),
            ) from exc
        try:
            with os.scandir(child_fd) as entries:
                if any(entries):
                    raise ProcessingCleanupError(
                        "manifest_drift",
                        f"cleanup directory is not empty: {relative_path}",
                    )
            held = os.fstat(child_fd)
            current = os.stat(
                name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISDIR(current.st_mode)
                or held.st_dev != current.st_dev
                or held.st_ino != current.st_ino
            ):
                raise ProcessingCleanupError(
                    "manifest_drift",
                    f"cleanup directory identity changed: {relative_path}",
                )
            mutation_checkpoint()
            os.rmdir(name, dir_fd=parent_fd)
            _fsync(parent_fd)
        finally:
            os.close(child_fd)


def _run_remove_source(
    db: ProcessingCleanupJournalDB,
    row: ProcessingCleanupJournalRow,
    *,
    manifest: CleanupExactManifest,
    owner_checkpoint: Callable[[], None],
    after_boundary: Callable[[str], None] | None,
) -> ProcessingCleanupJournalRow:
    if any(
        row[field] is not None
        for field in (
            "destination_path",
            "destination_manifest",
            "destination_manifest_hash",
            "selected_destination_path",
        )
    ):
        raise ProcessingCleanupError(
            "invalid_journal",
            "remove-source journal cannot carry a destination",
        )
    steps = _remove_steps(manifest)
    while True:
        progress = _progress_dict(row)
        inspection, _in_progress = _assert_remove_resume_state(
            row["source_path"],
            manifest,
            row["source_manifest_hash"],
            progress,
            steps,
        )
        next_step = next(
            (step for step in steps if step.after_key not in progress),
            None,
        )
        if next_step is None:
            if inspection.status != "missing":
                raise ProcessingCleanupError(
                    "manifest_drift",
                    "cleanup source still exists after every removal checkpoint",
                )
            return row

        if next_step.before_key not in progress:
            owner_checkpoint()
            row = _append_checkpoint(db, row, key=next_step.before_key)
            _boundary(after_boundary, f"journaled:{next_step.label}")
            progress = _progress_dict(row)
            inspection, _in_progress = _assert_remove_resume_state(
                row["source_path"],
                manifest,
                row["source_manifest_hash"],
                progress,
                steps,
            )

        current_entry = _entry_for_step(manifest, next_step)
        already_mutated = (
            inspection.status == "missing"
            if next_step.relative_path == "."
            else current_entry not in inspection.manifest
        )
        if not already_mutated:
            owner_checkpoint()
            # Reinspect after the durable pre-checkpoint and immediately before
            # mutation. A changed tree is never reconciled by inference.
            inspection, _in_progress = _assert_remove_resume_state(
                row["source_path"],
                manifest,
                row["source_manifest_hash"],
                _progress_dict(row),
                steps,
            )
            if next_step.operation == "unlink":
                assert current_entry is not None
                try:
                    _unlink_exact(
                        row["source_path"],
                        current_entry,
                        mutation_checkpoint=owner_checkpoint,
                    )
                except FilesystemAuthorityError as exc:
                    raise ProcessingCleanupError(
                        "source_uninspectable",
                        f"cleanup source cannot be mutated safely: {exc}",
                    ) from exc
                except OSError as exc:
                    raise ProcessingCleanupError(
                        "filesystem_mutation_failed",
                        f"cleanup unlink failed: {exc}",
                    ) from exc
            else:
                try:
                    _rmdir_exact(
                        row["source_path"],
                        next_step.relative_path,
                        mutation_checkpoint=owner_checkpoint,
                    )
                except FilesystemAuthorityError as exc:
                    raise ProcessingCleanupError(
                        "source_uninspectable",
                        f"cleanup source cannot be mutated safely: {exc}",
                    ) from exc
                except OSError as exc:
                    raise ProcessingCleanupError(
                        "filesystem_mutation_failed",
                        f"cleanup directory removal failed: {exc}",
                    ) from exc
            _boundary(after_boundary, f"mutated:{next_step.label}")

        owner_checkpoint()
        row = _append_checkpoint(db, row, key=next_step.after_key)
        _boundary(after_boundary, f"checkpointed:{next_step.label}")


def _destination_inspection(path: str) -> CleanupSourceInspection:
    return inspect_processing_cleanup_source(path)


def _require_destination_missing(
    inspection: CleanupSourceInspection,
) -> None:
    if inspection.status == "missing":
        return
    if inspection.status == "complete":
        raise ProcessingCleanupError(
            "destination_collision",
            "selected cleanup destination already exists",
        )
    if inspection.error_code in {
        "unsafe_symlink",
        "not_a_directory",
        "not_regular_file",
    }:
        raise ProcessingCleanupError(
            "destination_collision",
            "selected cleanup destination is occupied by an unsafe entry",
        )
    raise ProcessingCleanupError(
        "destination_uninspectable",
        f"selected cleanup destination cannot be inspected: "
        f"{inspection.reason}",
    )


def _ensure_destination_parent(
    destination_path: str,
    *,
    allow_create: bool,
) -> bool:
    """Open or safely create the selected destination's immediate parent.

    The cleanup intent has already durably selected ``destination_path`` before
    this helper can run. Only the final parent component may be created; every
    ancestor is reopened no-follow, so a missing ancestor or unsafe component
    fails closed instead of broadening the journaled namespace.
    """
    destination_parent = os.path.dirname(destination_path)
    parent_parent, parent_name = os.path.split(destination_parent)
    if not parent_name:
        with open_directory_path(destination_parent):
            return False

    try:
        parent_parent_scope = open_directory_path(parent_parent)
        with parent_parent_scope as parent_parent_fd:
            try:
                parent_fd = os.open(
                    parent_name,
                    _DIR_FLAGS,
                    dir_fd=parent_parent_fd,
                )
            except FileNotFoundError:
                if not allow_create:
                    raise ProcessingCleanupError(
                        "destination_uninspectable",
                        "journaled cleanup destination parent disappeared",
                    ) from None
                try:
                    os.mkdir(parent_name, 0o755, dir_fd=parent_parent_fd)
                except FileExistsError:
                    # A concurrent creator may win. It still has to satisfy the
                    # same no-follow directory open below.
                    pass
                except OSError as exc:
                    raise ProcessingCleanupError(
                        "filesystem_mutation_failed",
                        f"cannot create cleanup destination parent: {exc}",
                    ) from exc
                else:
                    _fsync(parent_parent_fd)
                    try:
                        parent_fd = os.open(
                            parent_name,
                            _DIR_FLAGS,
                            dir_fd=parent_parent_fd,
                        )
                    except OSError as exc:
                        raise ProcessingCleanupError(
                            "destination_uninspectable",
                            "created cleanup destination parent cannot be "
                            f"reopened safely: {exc}",
                        ) from exc
                    os.close(parent_fd)
                    return True
                try:
                    parent_fd = os.open(
                        parent_name,
                        _DIR_FLAGS,
                        dir_fd=parent_parent_fd,
                    )
                except OSError as exc:
                    raise ProcessingCleanupError(
                        "destination_uninspectable",
                        "cleanup destination parent was created as an unsafe "
                        f"entry: {exc}",
                    ) from exc
            except OSError as exc:
                raise ProcessingCleanupError(
                    "destination_uninspectable",
                    f"cleanup destination parent is unsafe: {exc}",
                ) from exc
            os.close(parent_fd)
    except FilesystemAuthorityError as exc:
        raise ProcessingCleanupError(
            "destination_uninspectable",
            f"cleanup destination parent cannot be opened safely: {exc}",
        ) from exc
    return False


def _rename_exact_tree(
    source_path: str,
    destination_path: str,
    expected_manifest: CleanupExactManifest,
    expected_hash: str,
    *,
    mutation_checkpoint: Callable[[], None],
) -> None:
    if paths_overlap(source_path, destination_path):
        raise ProcessingCleanupError(
            "invalid_journal",
            "cleanup source and destination cannot overlap",
        )
    source_parent, source_name = os.path.split(source_path)
    destination_parent, destination_name = os.path.split(destination_path)
    with (
        open_directory_path(source_parent) as source_parent_fd,
        open_directory_path(destination_parent) as destination_parent_fd,
    ):
        try:
            os.stat(
                destination_name,
                dir_fd=destination_parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise ProcessingCleanupError(
                "destination_collision",
                "selected cleanup destination became occupied",
            )
        with open_relative_directory(
            source_parent_fd,
            source_name,
        ) as source_fd:
            if (
                os.fstat(source_fd).st_dev
                != os.fstat(destination_parent_fd).st_dev
            ):
                raise ProcessingCleanupError(
                    "cross_device",
                    "cleanup quarantine requires one same-filesystem rename",
                )
            observed = _inspect_open_root(source_fd)
            if (
                observed != expected_manifest
                or cleanup_manifest_hash(observed) != expected_hash
            ):
                raise ProcessingCleanupError(
                    "manifest_drift",
                    "cleanup source changed at quarantine mutation boundary",
                )
            mutation_checkpoint()
            try:
                moved = rename_between_directories_noreplace(
                    source_parent_fd,
                    source_name,
                    destination_parent_fd,
                    destination_name,
                )
            except OSError as exc:
                raise ProcessingCleanupError(
                    "filesystem_mutation_failed",
                    f"cleanup quarantine rename failed: {exc}",
                ) from exc
            if not moved:
                raise ProcessingCleanupError(
                    "destination_collision",
                    "selected cleanup destination lost its no-replace race",
                )
        _fsync(source_parent_fd)
        if source_parent_fd != destination_parent_fd:
            _fsync(destination_parent_fd)


def _run_quarantine_source(
    db: ProcessingCleanupJournalDB,
    row: ProcessingCleanupJournalRow,
    *,
    manifest: CleanupExactManifest,
    owner_checkpoint: Callable[[], None],
    after_boundary: Callable[[str], None] | None,
) -> ProcessingCleanupJournalRow:
    destination_path = row["destination_path"]
    selected_destination = row["selected_destination_path"]
    destination_raw = row["destination_manifest"]
    destination_hash = row["destination_manifest_hash"]
    if (
        destination_path is None
        or selected_destination is None
        or destination_raw is None
        or destination_hash is None
        or destination_path != selected_destination
    ):
        raise ProcessingCleanupError(
            "invalid_journal",
            "quarantine journal needs one exact selected destination",
        )
    _require_canonical_absolute(destination_path, "destination_path")
    destination_manifest = _manifest_from_json(
        destination_raw,
        expected_hash=destination_hash,
        field="destination_manifest",
    )
    if destination_manifest != manifest:
        raise ProcessingCleanupError(
            "invalid_journal",
            "atomic quarantine source/destination manifests must match",
        )

    parent_before_key = "before:destination_parent"
    parent_after_key = "after:destination_parent"
    before_key = "before:rename"
    after_key = "after:rename"
    ordered_progress = (
        parent_before_key,
        parent_after_key,
        before_key,
        after_key,
    )
    progress = _progress_dict(row)
    if set(progress) - set(ordered_progress) or any(
        value is not True for value in progress.values()
    ):
        raise ProcessingCleanupError(
            "invalid_journal",
            "quarantine progress contains an unknown checkpoint",
        )
    missing_checkpoint = False
    for key in ordered_progress:
        if key not in progress:
            missing_checkpoint = True
        elif missing_checkpoint:
            raise ProcessingCleanupError(
                "invalid_journal",
                "quarantine progress is not a deterministic prefix",
            )

    source = inspect_processing_cleanup_source(row["source_path"])
    if parent_before_key not in progress:
        _assert_exact_manifest(
            source,
            manifest,
            row["source_manifest_hash"],
            subject="source",
        )
        _require_destination_missing(
            _destination_inspection(destination_path)
        )
        owner_checkpoint()
        row = _append_checkpoint(db, row, key=parent_before_key)
        _boundary(after_boundary, "journaled:destination_parent")
        progress = _progress_dict(row)
    if parent_after_key not in progress:
        owner_checkpoint()
        created = _ensure_destination_parent(
            destination_path,
            allow_create=True,
        )
        if created:
            _boundary(after_boundary, "mutated:destination_parent")
            owner_checkpoint()
        row = _append_checkpoint(db, row, key=parent_after_key)
        _boundary(after_boundary, "checkpointed:destination_parent")
        progress = _progress_dict(row)
    else:
        _ensure_destination_parent(
            destination_path,
            allow_create=False,
        )

    destination = _destination_inspection(destination_path)
    if before_key not in progress:
        _assert_exact_manifest(
            source,
            manifest,
            row["source_manifest_hash"],
            subject="source",
        )
        _require_destination_missing(destination)
        owner_checkpoint()
        row = _append_checkpoint(db, row, key=before_key)
        _boundary(after_boundary, "journaled:rename")
        progress = _progress_dict(row)
        source = inspect_processing_cleanup_source(row["source_path"])
        destination = _destination_inspection(destination_path)

    if after_key in progress:
        if source.status != "missing":
            raise ProcessingCleanupError(
                "manifest_drift",
                "quarantined source still exists after rename checkpoint",
            )
        _assert_exact_manifest(
            destination,
            destination_manifest,
            destination_hash,
            subject="destination",
        )
        return row

    source_present = source.status == "complete"
    destination_present = destination.status == "complete"
    if source_present and destination.status == "missing":
        _assert_exact_manifest(
            source,
            manifest,
            row["source_manifest_hash"],
            subject="source",
        )
        owner_checkpoint()
        try:
            _rename_exact_tree(
                row["source_path"],
                destination_path,
                manifest,
                row["source_manifest_hash"],
                mutation_checkpoint=owner_checkpoint,
            )
        except FilesystemAuthorityError as exc:
            raise ProcessingCleanupError(
                "source_uninspectable",
                f"cleanup source cannot be quarantined safely: {exc}",
            ) from exc
        _boundary(after_boundary, "mutated:rename")
    elif source.status == "missing" and destination_present:
        _assert_exact_manifest(
            destination,
            destination_manifest,
            destination_hash,
            subject="destination",
        )
    elif source_present and destination_present:
        raise ProcessingCleanupError(
            "destination_collision",
            "cleanup source and selected destination both exist",
        )
    elif source.status == "uninspectable":
        raise ProcessingCleanupError(
            "source_uninspectable",
            f"cleanup source cannot be inspected: {source.reason}",
        )
    elif destination.status == "uninspectable":
        _require_destination_missing(destination)
    else:
        raise ProcessingCleanupError(
            "manifest_drift",
            "cleanup source and selected destination are both missing",
        )

    owner_checkpoint()
    row = _append_checkpoint(db, row, key=after_key)
    _boundary(after_boundary, "checkpointed:rename")
    return row


def _run_no_op(
    db: ProcessingCleanupJournalDB,
    row: ProcessingCleanupJournalRow,
    *,
    manifest: CleanupExactManifest,
    owner_checkpoint: Callable[[], None],
    after_boundary: Callable[[str], None] | None,
) -> ProcessingCleanupJournalRow:
    if manifest:
        raise ProcessingCleanupError(
            "invalid_journal",
            "no-op cleanup must journal an empty source manifest",
        )
    if any(
        row[field] is not None
        for field in (
            "destination_path",
            "destination_manifest",
            "destination_manifest_hash",
            "selected_destination_path",
        )
    ):
        raise ProcessingCleanupError(
            "invalid_journal",
            "no-op cleanup cannot carry a destination",
        )
    before_key = "before:confirm_absent"
    after_key = "after:confirm_absent"
    progress = _progress_dict(row)
    if set(progress) - {before_key, after_key} or any(
        value is not True for value in progress.values()
    ):
        raise ProcessingCleanupError(
            "invalid_journal",
            "no-op cleanup contains an unknown checkpoint",
        )
    if after_key in progress and before_key not in progress:
        raise ProcessingCleanupError(
            "invalid_journal",
            "no-op cleanup completed without its pre-checkpoint",
        )
    inspection = inspect_processing_cleanup_source(row["source_path"])
    if inspection.status == "complete":
        raise ProcessingCleanupError(
            "manifest_drift",
            "no-op cleanup source exists",
        )
    if inspection.status == "uninspectable":
        raise ProcessingCleanupError(
            "source_uninspectable",
            f"no-op source absence cannot be proven: {inspection.reason}",
        )
    if before_key not in progress:
        owner_checkpoint()
        row = _append_checkpoint(db, row, key=before_key)
        _boundary(after_boundary, "journaled:confirm_absent")
    if after_key not in _progress_dict(row):
        owner_checkpoint()
        inspection = inspect_processing_cleanup_source(row["source_path"])
        if inspection.status == "complete":
            raise ProcessingCleanupError(
                "manifest_drift",
                "no-op cleanup source appeared after its pre-checkpoint",
            )
        if inspection.status == "uninspectable":
            raise ProcessingCleanupError(
                "source_uninspectable",
                f"no-op source absence cannot be proven: {inspection.reason}",
            )
        _boundary(after_boundary, "observed:confirm_absent")
        row = _append_checkpoint(db, row, key=after_key)
        _boundary(after_boundary, "checkpointed:confirm_absent")
    return row


def _completed_receipt(
    row: ProcessingCleanupJournalRow,
    *,
    outcome: Literal["completed", "no_op"],
) -> CleanupJournalReceipt:
    return CleanupJournalReceipt(
        outcome=outcome,
        action=row["action"],
        source_path=row["source_path"],
        source_manifest_hash=row["source_manifest_hash"],
        destination_path=row["destination_path"],
        destination_manifest_hash=row["destination_manifest_hash"],
        selected_destination_path=row["selected_destination_path"],
        step_progress=_progress_dict(row),
        details={"executor": "processing-cleanup-v1"},
    )


def execute_processing_cleanup(
    db: ProcessingCleanupJournalDB,
    journal: ProcessingCleanupJournalRow,
    *,
    owner_checkpoint: Callable[[], None],
    after_boundary: Callable[[str], None] | None = None,
) -> ProcessingCleanupJournalRow:
    """Execute or resume one exact journal, leaving terminal state untouched.

    ``owner_checkpoint`` is the caller's combined cancellation, pinned-session,
    exact-job, and execution-lease check. It runs before every database or
    filesystem effect and immediately after each filesystem mutation. If it
    raises after a mutation, the executor propagates without rollback; the
    durable pre-checkpoint makes the next proven owner resume idempotently.
    """
    if journal["completed_receipt"] is not None:
        return journal
    _require_canonical_absolute(journal["source_path"], "source_path")
    action = _typed_action(journal["action"])
    manifest = _manifest_from_json(
        journal["source_manifest"],
        expected_hash=journal["source_manifest_hash"],
        field="source_manifest",
    )

    if action == PROCESSING_CLEANUP_REMOVE_SOURCE:
        row = _run_remove_source(
            db,
            journal,
            manifest=manifest,
            owner_checkpoint=owner_checkpoint,
            after_boundary=after_boundary,
        )
        receipt_outcome: Literal["completed", "no_op"] = "completed"
    elif action == PROCESSING_CLEANUP_QUARANTINE_SOURCE:
        row = _run_quarantine_source(
            db,
            journal,
            manifest=manifest,
            owner_checkpoint=owner_checkpoint,
            after_boundary=after_boundary,
        )
        receipt_outcome = "completed"
    else:
        row = _run_no_op(
            db,
            journal,
            manifest=manifest,
            owner_checkpoint=owner_checkpoint,
            after_boundary=after_boundary,
        )
        receipt_outcome = "no_op"

    owner_checkpoint()
    receipt = _completed_receipt(row, outcome=receipt_outcome)
    completed = db.complete_processing_cleanup_journal(
        request_id=row["request_id"],
        job_id=row["job_id"],
        expected_revision=row["revision"],
        receipt=receipt,
    )
    _boundary(after_boundary, "completed")
    return completed
