"""Private preview snapshots and the job-scoped action copies they become.

Two lifecycles inside Cratedigger's private processing tree, joined at one
hinge. A foreign directory — an operator's local-import folder, a quarantined
wrong match — is boundedly descriptor-copied into ``processing/preview/``
before any media tool touches it, so the inventory evidence is built from
comes off the private copy rather than a second walk of a pathname anything
outside this service can still move. When a force or local-import job decides
to keep what it measured, ``retain_preview_snapshot_for_force_action`` renames
that snapshot into ``processing/albums/`` under a deterministic job-scoped
name, and the importer consumes those exact bytes.

Both halves refuse anything they do not own: a path that is not a direct
child of the private root, a name without the lane's prefix, an action copy
whose name does not belong to the job asking to remove it. Each refusal is
raised before the removal opens anything, so a mismatch leaks a directory
rather than deleting a stranger's.

Split out of ``lib.import_preview`` in issue #1313. Nothing here decides
anything about quality; the preview lane calls in for storage.
"""

from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lib.fs_authority import (
    FilesystemAuthorityError,
    copy_opened_file,
    exclusive_relative_lock,
    open_configured_local_import_directory,
    open_configured_quarantine_directory,
    open_directory_path,
    open_private_child_directory,
    open_private_processing_root,
    open_regular_relative,
    remove_relative_tree,
)
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    cancellation_hook,
    checkpoint,
)
from lib.import_queue import IMPORT_JOB_FORCE, IMPORT_JOB_LOCAL
from lib.media_readiness import normalize_media_metadata
from lib.processing_paths import processing_albums_dir, processing_preview_dir

if TYPE_CHECKING:
    from lib.config import CratediggerConfig


_PREVIEW_MAX_DEPTH = 32
_PREVIEW_MAX_ENTRIES = 5000
_PREVIEW_MAX_FILES = 5000
_PREVIEW_MAX_BYTES = 100 * 1024**3
_PREVIEW_FREE_RESERVE_BYTES = 100 * 1024**2


@dataclass(frozen=True)
class PreviewSnapshotLimits:
    """Bounded-copy policy for one isolated preview snapshot.

    The normal worker uses the module defaults.  Accepting this immutable
    value at the snapshot boundary also lets callers exercise a small bounded
    world without changing global process policy.
    """

    max_depth: int = _PREVIEW_MAX_DEPTH
    max_entries: int = _PREVIEW_MAX_ENTRIES
    max_files: int = _PREVIEW_MAX_FILES
    max_bytes: int = _PREVIEW_MAX_BYTES
    free_reserve_bytes: int = _PREVIEW_FREE_RESERVE_BYTES


PreviewCopyFn = Callable[..., int]
PreviewAvailableBytesFn = Callable[[int], int]


def prepare_preview_media(path: str) -> None:
    """Normalize only a ready private view; measurement owns invalid evidence."""
    normalize_media_metadata(path, fail_closed=False)


def _preview_available_bytes(preview_fd: int) -> int:
    info = os.fstatvfs(preview_fd)
    return info.f_bavail * info.f_frsize


@contextmanager
def _preview_copy_lock(
    cfg: CratediggerConfig,
) -> Generator[int]:
    """Serialize bounded source snapshots before they consume private disk.

    The lock intentionally covers only the untrusted-tree copy and its
    free-space admission check.  Measurement and the harness run after it is
    released, so one slow preview cannot serialize all operator work.
    """
    with open_private_processing_root(
        cfg.processing_dir, cfg.slskd_download_dir,
    ) as processing_fd, exclusive_relative_lock(
        processing_fd, ".preview-snapshot.lock",
    ), open_private_child_directory(
        processing_fd, "preview",
    ) as preview_fd:
        # Keep locks out of the aged preview directory: its contents are
        # ephemeral snapshots and tmpfiles may prune them independently.
        yield preview_fd


def _assert_preview_space(
    preview_fd: int,
    next_write_bytes: int,
    *,
    free_reserve_bytes: int,
    available_bytes_fn: PreviewAvailableBytesFn,
) -> None:
    if available_bytes_fn(preview_fd) - next_write_bytes < free_reserve_bytes:
        raise FilesystemAuthorityError("insufficient private preview space")


def _snapshot_opened_directory(
    source_root_fd: int,
    cfg: CratediggerConfig,
    *,
    limits: PreviewSnapshotLimits | None = None,
    available_bytes_fn: PreviewAvailableBytesFn | None = None,
    copy_fn: PreviewCopyFn | None = None,
    cancellation_token: CancellationToken | None = None,
) -> str:
    """Boundedly copy an already-held source directory into private preview.

    Every byte is copied from an opened regular inode.  The inventory later
    used for evidence is consequently taken from the private copy, never a
    second walk of the externally mutable source pathname.
    """
    effective_limits = limits or PreviewSnapshotLimits()
    effective_available_bytes = available_bytes_fn or _preview_available_bytes
    effective_copy = copy_fn or copy_opened_file
    snapshot_name = f"preview-{secrets.token_hex(16)}"
    snapshot_path = os.path.join(processing_preview_dir(cfg.processing_dir), snapshot_name)
    files = 0
    entries_seen = 0
    copied_bytes = 0
    made_snapshot = False
    try:
        with _preview_copy_lock(cfg) as preview_fd:
            _assert_preview_space(
                preview_fd,
                0,
                free_reserve_bytes=effective_limits.free_reserve_bytes,
                available_bytes_fn=effective_available_bytes,
            )
            checkpoint(cancellation_token)
            os.mkdir(snapshot_name, 0o700, dir_fd=preview_fd)
            made_snapshot = True
            snapshot_fd = os.open(
                snapshot_name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=preview_fd,
            )
            def copy_directory(
                source_dir_fd: int,
                destination_dir_fd: int,
                depth: int,
            ) -> None:
                """Copy depth-first so the descriptor footprint is bounded."""
                nonlocal copied_bytes, entries_seen, files
                try:
                    names: list[str] = []
                    with os.scandir(source_dir_fd) as entries:
                        for entry in entries:
                            entries_seen += 1
                            if entries_seen > effective_limits.max_entries:
                                raise FilesystemAuthorityError(
                                    "preview snapshot entry limit exceeded",
                                )
                            names.append(entry.name)
                    names.sort()
                    for name in names:
                        try:
                            child_fd = os.open(
                                name,
                                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                                dir_fd=source_dir_fd,
                            )
                        except OSError:
                            child_fd = -1
                        if child_fd >= 0:
                            try:
                                if not stat.S_ISDIR(os.fstat(child_fd).st_mode):
                                    raise FilesystemAuthorityError("snapshot contains non-directory")
                                if depth >= effective_limits.max_depth:
                                    raise FilesystemAuthorityError("preview depth limit exceeded")
                                checkpoint(cancellation_token)
                                os.mkdir(name, 0o700, dir_fd=destination_dir_fd)
                                destination_child_fd = os.open(
                                    name,
                                    os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                                    dir_fd=destination_dir_fd,
                                )
                                next_source_fd = child_fd
                                child_fd = -1
                                copy_directory(
                                    next_source_fd, destination_child_fd, depth + 1,
                                )
                            finally:
                                if child_fd >= 0:
                                    os.close(child_fd)
                            continue
                        opened = open_regular_relative(source_dir_fd, name)
                        try:
                            files += 1
                            declared_size = opened.stat_result.st_size
                            if (
                                files > effective_limits.max_files
                                or copied_bytes + declared_size > effective_limits.max_bytes
                            ):
                                raise FilesystemAuthorityError("preview snapshot limit exceeded")
                            checkpoint(cancellation_token)
                            destination_fd = os.open(
                                name,
                                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                                0o600,
                                dir_fd=destination_dir_fd,
                            )
                            try:
                                def assert_space_before_write(count: int) -> None:
                                    checkpoint(cancellation_token)
                                    _assert_preview_space(
                                        preview_fd,
                                        count,
                                        free_reserve_bytes=(
                                            effective_limits.free_reserve_bytes
                                        ),
                                        available_bytes_fn=effective_available_bytes,
                                    )

                                copied = effective_copy(
                                    opened.fd,
                                    destination_fd,
                                    max_bytes=declared_size,
                                    before_write=assert_space_before_write,
                                )
                            finally:
                                os.close(destination_fd)
                            copied_bytes += copied
                        finally:
                            opened.close()
                finally:
                    os.close(source_dir_fd)
                    os.close(destination_dir_fd)

            copy_directory(os.dup(source_root_fd), snapshot_fd, 0)
        return snapshot_path
    except ExecutionCancelled:
        # The private partial tree is recovery evidence. Removing it after
        # cancellation would be a new mutation by an execution that no longer
        # has durable authority.
        raise
    except Exception:
        if made_snapshot:
            checkpoint(cancellation_token)
            with _preview_copy_lock(cfg) as preview_fd:
                checkpoint(cancellation_token)
                remove_relative_tree(
                    preview_fd,
                    snapshot_name,
                    before_mutation=cancellation_hook(cancellation_token),
                )
        raise


def snapshot_authorized_directory(
    path: str,
    cfg: CratediggerConfig,
    *,
    limits: PreviewSnapshotLimits | None = None,
    available_bytes_fn: PreviewAvailableBytesFn | None = None,
    copy_fn: PreviewCopyFn | None = None,
    cancellation_token: CancellationToken | None = None,
) -> str:
    """Snapshot a direct caller path through a held no-follow descriptor."""
    with open_directory_path(path) as source_fd:
        return _snapshot_opened_directory(
            source_fd,
            cfg,
            limits=limits,
            available_bytes_fn=available_bytes_fn,
            copy_fn=copy_fn,
            cancellation_token=cancellation_token,
        )


def snapshot_configured_quarantine_directory(
    raw_path: str,
    cfg: CratediggerConfig,
    *,
    limits: PreviewSnapshotLimits | None = None,
    available_bytes_fn: PreviewAvailableBytesFn | None = None,
    copy_fn: PreviewCopyFn | None = None,
    cancellation_token: CancellationToken | None = None,
) -> str:
    """Copy a failed/wrong-match folder from its held configured authority."""
    with open_configured_quarantine_directory(raw_path, cfg) as source:
        return _snapshot_opened_directory(
            source.fd,
            cfg,
            limits=limits,
            available_bytes_fn=available_bytes_fn,
            copy_fn=copy_fn,
            cancellation_token=cancellation_token,
        )


def snapshot_configured_local_import_directory(
    raw_path: str,
    cfg: CratediggerConfig,
    *,
    limits: PreviewSnapshotLimits | None = None,
    available_bytes_fn: PreviewAvailableBytesFn | None = None,
    copy_fn: PreviewCopyFn | None = None,
    cancellation_token: CancellationToken | None = None,
) -> str:
    """Copy an operator-named local-import folder from its held authority.

    Sibling of :func:`snapshot_configured_quarantine_directory` (issue
    #1176 PR3) — identical descriptor-copy discipline, differing only in
    which authority resolves ``raw_path``:
    :func:`lib.fs_authority.open_configured_local_import_directory` (PR2)
    instead of the quarantine roots.
    """
    with open_configured_local_import_directory(raw_path, cfg) as source:
        return _snapshot_opened_directory(
            source.fd,
            cfg,
            limits=limits,
            available_bytes_fn=available_bytes_fn,
            copy_fn=copy_fn,
            cancellation_token=cancellation_token,
        )


def remove_preview_snapshot(
    path: str,
    cfg: CratediggerConfig,
    *,
    cancellation_token: CancellationToken | None = None,
) -> None:
    """Remove only a direct, service-owned private snapshot directory.

    Before issue #1313 this was two functions: a private ``_remove_preview_tree``
    and a public wrapper that forwarded every argument to it unchanged. In-module
    callers took the private one and out-of-module callers the public one, which
    made them look like different guarantees. They were the same function.
    """
    name = os.path.basename(path)
    if name == path or not name.startswith("preview-"):
        raise FilesystemAuthorityError("not a private preview snapshot")
    if os.path.dirname(path) != processing_preview_dir(cfg.processing_dir):
        raise FilesystemAuthorityError("preview snapshot is outside private root")
    with _preview_copy_lock(cfg) as preview_fd:
        checkpoint(cancellation_token)
        remove_relative_tree(
            preview_fd,
            name,
            before_mutation=cancellation_hook(cancellation_token),
        )


#: The force-import lane's action-copy prefix (issue #1211) — the single
#: named source for a string that used to be spelled independently as a
#: literal at eight functional sites across this module,
#: ``scripts/importer.py``, and ``scripts/import_preview_worker.py``. All
#: eight were byte-identical, so nothing was broken, but editing any one
#: alone would have silently drifted the importer's terminal cleanup
#: comparison in ``cleanup_force_action_copy_for_job`` — which raises
#: ``FilesystemAuthorityError`` BEFORE ever touching the filesystem on a
#: mismatch, leaking the retained action copy permanently and re-raising on
#: every subsequent importer startup recovery sweep. That exact defect
#: already happened once for the local-import lane (see the comment at
#: ``scripts/importer.py``'s ``_cleanup_terminal_force_action``).
FORCE_ACTION_PREFIX = "force-action-"

#: The local-import lane's action-copy prefix (issue #1176 PR3) — passed as
#: ``prefix=`` to every action-copy helper below so a local-import job's
#: retained private copy can never collide on name with a force job's, even
#: though ``import_job_id`` is drawn from the same ``import_jobs`` sequence
#: across every job type.
LOCAL_IMPORT_ACTION_PREFIX = "local-import-action-"

#: The job-scoped-action-copy prefix for each job type that retains one
#: (issue #1176 PR3 review round). Both the importer and preview worker call
#: ``cleanup_force_action_copy_for_job``/``force_action_copy_path`` with a
#: job's own prefix looked up here — a single shared table instead of two,
#: since a missing or wrong prefix compares the path against the WRONG job
#: type's deterministic name and raises ``FilesystemAuthorityError`` before
#: ever touching the filesystem (issue #1176 PR3 F5: the importer's own
#: cleanup site called the force-import path helper with no ``prefix=`` at
#: all, so every local-import terminal cleanup raised, leaked its action
#: copy permanently, and re-raised on every subsequent importer startup
#: recovery sweep). ``youtube_import`` and ``automation_import`` are absent
#: on purpose — neither retains a private action copy under
#: ``processing/albums/`` the way force/local-import do.
ACTION_COPY_PREFIX_BY_JOB_TYPE: dict[str, str] = {
    IMPORT_JOB_FORCE: FORCE_ACTION_PREFIX,
    IMPORT_JOB_LOCAL: LOCAL_IMPORT_ACTION_PREFIX,
}


def retain_preview_snapshot_for_force_action(
    path: str,
    cfg: CratediggerConfig,
    *,
    import_job_id: int,
    prefix: str,
    cancellation_token: CancellationToken | None = None,
) -> str:
    """Promote one verified private snapshot to a job-scoped action copy.

    The source has already crossed the descriptor-copy boundary.  This is a
    rename wholly inside Cratedigger's private processing tree, not another
    copy of the operator's quarantine folder.  The returned path survives
    preview so the importer consumes the exact normalized bytes evidence
    describes.

    ``prefix`` (issue #1176 PR3) is REQUIRED, not defaulted (issue #1211
    PR1 follow-up): the sole caller already always passes its lane's own
    prefix explicitly — ``FORCE_ACTION_PREFIX`` (``"force-action-"``) for
    force, ``LOCAL_IMPORT_ACTION_PREFIX`` (``"local-import-action-"``) for
    local-import — so a force copy and a local-import copy can never
    collide on name even though ``import_job_id`` is drawn from the same
    ``import_jobs`` sequence across every job type. An unreachable default
    that silently supplied force's prefix was exactly the implicit-
    inheritance hazard this module exists to remove: a caller that forgot
    ``prefix=`` would have silently retained every job type's action copy
    under FORCE's deterministic name instead of failing loudly.
    """
    name = os.path.basename(path)
    if name == path or not name.startswith("preview-"):
        raise FilesystemAuthorityError("not a private preview snapshot")
    if os.path.dirname(path) != processing_preview_dir(cfg.processing_dir):
        raise FilesystemAuthorityError("preview snapshot is outside private root")
    action_name = f"{prefix}{import_job_id}"
    with open_private_processing_root(
        cfg.processing_dir, cfg.slskd_download_dir,
    ) as processing_fd, open_private_child_directory(processing_fd, "preview") as preview_fd, open_private_child_directory(processing_fd, "albums") as albums_fd, exclusive_relative_lock(
        albums_fd, f".{action_name}.lock",
    ):
        checkpoint(cancellation_token)
        remove_relative_tree(
            albums_fd,
            action_name,
            before_mutation=cancellation_hook(cancellation_token),
        )
        checkpoint(cancellation_token)
        os.rename(
            name, action_name,
            src_dir_fd=preview_fd, dst_dir_fd=albums_fd,
        )
    return os.path.join(processing_albums_dir(cfg.processing_dir), action_name)


def remove_force_action_copy(
    path: str,
    cfg: CratediggerConfig,
    *,
    prefix: str,
    cancellation_token: CancellationToken | None = None,
) -> None:
    """Remove one unneeded retained job-scoped action copy after a terminal
    result. ``prefix`` — see :func:`retain_preview_snapshot_for_force_action`.
    """
    # F9 (issue #1176 PR3 review round): this function serves both lanes
    # (``prefix`` is the job-type signal), but every FilesystemAuthorityError
    # below said "force" unconditionally — the exact text an operator sees
    # in a failed cleanup's ``force_action_cleanup.error`` receipt (F5).
    lane_label = "local-import" if prefix == LOCAL_IMPORT_ACTION_PREFIX else "force"
    name = os.path.basename(path)
    if name == path or not name.startswith(prefix):
        raise FilesystemAuthorityError(f"not a private {lane_label} action copy")
    if os.path.dirname(path) != processing_albums_dir(cfg.processing_dir):
        raise FilesystemAuthorityError(
            f"{lane_label} action copy is outside private root"
        )
    with open_private_processing_root(
        cfg.processing_dir, cfg.slskd_download_dir,
    ) as processing_fd, open_private_child_directory(processing_fd, "albums") as albums_fd:
        checkpoint(cancellation_token)
        remove_relative_tree(
            albums_fd,
            name,
            before_mutation=cancellation_hook(cancellation_token),
        )


def cleanup_force_action_copy_for_job(
    path: str,
    cfg: CratediggerConfig,
    *,
    import_job_id: int,
    prefix: str,
    cancellation_token: CancellationToken | None = None,
) -> None:
    """Remove only the deterministic action copy owned by this job.
    ``prefix`` — see :func:`retain_preview_snapshot_for_force_action`.
    """
    if path != force_action_copy_path(cfg, import_job_id, prefix=prefix):
        lane_label = (
            "local-import" if prefix == LOCAL_IMPORT_ACTION_PREFIX else "force"
        )
        raise FilesystemAuthorityError(
            f"{lane_label} action copy does not belong to job"
        )
    remove_force_action_copy(
        path,
        cfg,
        prefix=prefix,
        cancellation_token=cancellation_token,
    )


def force_action_copy_path(
    cfg: CratediggerConfig, import_job_id: int, *, prefix: str = FORCE_ACTION_PREFIX,
) -> str:
    """The one reclaimable private action directory for a job.
    ``prefix`` — see :func:`retain_preview_snapshot_for_force_action`.
    """
    return os.path.join(
        processing_albums_dir(cfg.processing_dir), f"{prefix}{import_job_id}",
    )
