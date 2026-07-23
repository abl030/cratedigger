"""No-follow file-descriptor authority for untrusted filesystem trees.

Paths received from slskd, the database, or HTTP are names, not authority.
Callers open an explicitly configured root once and walk every descendant
relative to that descriptor with ``O_NOFOLLOW``.  The returned descriptors
are deliberately the objects consumed by readers/copy routines: resolving a
name and opening it later would re-introduce a symlink/swap race.
"""

from __future__ import annotations

import errno
import fcntl
import os
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass


class FilesystemAuthorityError(ValueError):
    """Configured or untrusted path violates the filesystem boundary."""


_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
_FILE_FLAGS = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK


def _parts(relative_path: str) -> tuple[str, ...]:
    if not relative_path or os.path.isabs(relative_path):
        raise FilesystemAuthorityError("path must be a non-empty relative path")
    parts = tuple(relative_path.split(os.sep))
    if any(part in ("", ".", "..") for part in parts):
        raise FilesystemAuthorityError("path contains an unsafe component")
    return parts


def _raise_path_error(path: str, exc: OSError) -> FilesystemAuthorityError:
    if exc.errno in (errno.ELOOP, errno.ENOTDIR):
        return FilesystemAuthorityError(f"unsafe symlink or non-directory: {path}")
    return FilesystemAuthorityError(f"cannot open {path}: {exc.strerror}")


@contextmanager
def open_directory_path(path: str) -> Iterator[int]:
    """Open an absolute directory while refusing every symlink component."""
    if not os.path.isabs(path):
        raise FilesystemAuthorityError("authority root must be absolute")
    fd = os.open(os.sep, _DIR_FLAGS)
    try:
        relative = path.lstrip(os.sep)
        parts = () if not relative else _parts(relative)
        for part in parts:
            try:
                child = os.open(part, _DIR_FLAGS, dir_fd=fd)
            except OSError as exc:
                raise _raise_path_error(path, exc) from exc
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


def _relative_to(root: str, path: str) -> str:
    if not os.path.isabs(path):
        raise FilesystemAuthorityError("candidate path must be absolute")
    try:
        relative = os.path.relpath(path, root)
    except ValueError as exc:
        raise FilesystemAuthorityError("candidate is not beneath authority root") from exc
    _parts(relative)
    return relative


def paths_overlap(left: str, right: str) -> bool:
    """Lexical overlap check used before either configured root is opened."""
    left_abs = os.path.abspath(os.path.normpath(left))
    right_abs = os.path.abspath(os.path.normpath(right))
    try:
        common = os.path.commonpath((left_abs, right_abs))
    except ValueError:
        return False
    return common == left_abs or common == right_abs


def _assert_private_parent(path: str) -> None:
    """Open every ancestor no-follow and reject replaceable authority.

    A private child under ``/tmp`` is not sufficient: a writer of *any*
    ancestor can replace an intervening directory between validation and a
    future open.  The supported module layout deliberately uses a stable,
    non-writable ancestry such as ``/var/lib``.
    """
    expected_uid = os.geteuid()
    root = os.path.abspath(path)
    root_parts = () if root == os.sep else _parts(root.lstrip(os.sep))
    fd = os.open(os.sep, _DIR_FLAGS)
    current = os.sep
    try:
        for part in ("", *root_parts):
            if part:
                try:
                    child = os.open(part, _DIR_FLAGS, dir_fd=fd)
                except OSError as exc:
                    raise _raise_path_error(root, exc) from exc
                os.close(fd)
                fd = child
                current = os.path.join(current, part)
            try:
                info = os.fstat(fd)
            except OSError as exc:
                raise FilesystemAuthorityError(
                    f"cannot inspect {current}: {exc.strerror}") from exc
            if info.st_mode & 0o022:
                raise FilesystemAuthorityError(
                    f"private processing ancestor is group/other writable: {current}")
            if current == root and info.st_uid != expected_uid:
                raise FilesystemAuthorityError(
                    f"private processing root is not owned by service identity: {current}")
            if current == root and stat.S_IMODE(info.st_mode) != 0o700:
                raise FilesystemAuthorityError(
                    f"private processing root is not mode 0700: {current}")
    finally:
        os.close(fd)


def same_open_directory(path: str, held_fd: int) -> bool:
    """Does a fresh no-follow open of ``path`` name the held directory?"""
    with open_directory_path(path) as reopened_fd:
        reopened = os.fstat(reopened_fd)
    held = os.fstat(held_fd)
    return reopened.st_dev == held.st_dev and reopened.st_ino == held.st_ino


@contextmanager
def exclusive_relative_lock(root_fd: int, name: str) -> Iterator[None]:
    """Hold a no-follow regular lock file beneath an authoritative root."""
    lock_name = _parts(name)
    if len(lock_name) != 1:
        raise FilesystemAuthorityError("lock name must be one safe component")
    try:
        fd = os.open(
            lock_name[0], os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600, dir_fd=root_fd,
        )
    except OSError as exc:
        raise _raise_path_error(name, exc) from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise FilesystemAuthorityError("private lock is not regular")
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def remove_relative_tree(parent_fd: int, name: str) -> None:
    """Delete one service-owned tree via held descriptors only.

    Used solely for materialization transaction directories while their
    per-attempt lock is held. It never reconstructs an absolute filesystem
    path, so a post-check root relocation cannot redirect cleanup.
    """
    part = _parts(name)
    if len(part) != 1:
        raise FilesystemAuthorityError("tree name must be one safe component")
    try:
        fd = os.open(part[0], _DIR_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise _raise_path_error(name, exc) from exc
    try:
        with os.scandir(fd) as entries:
            children = list(entries)
        for entry in children:
            if entry.is_dir(follow_symlinks=False):
                remove_relative_tree(fd, entry.name)
            else:
                os.unlink(entry.name, dir_fd=fd)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rmdir(part[0], dir_fd=parent_fd)
    os.fsync(parent_fd)


@contextmanager
def open_private_processing_root(processing_dir: str, slskd_download_dir: str) -> Iterator[int]:
    """Open the configured private root after its complete trust checks."""
    if not os.path.isabs(processing_dir):
        raise FilesystemAuthorityError("processing_dir must be absolute")
    if paths_overlap(processing_dir, slskd_download_dir):
        raise FilesystemAuthorityError("processing_dir overlaps slskd_download_dir")
    _assert_private_parent(processing_dir)
    # A lexical comparison alone misses a configured root reached through a
    # bind mount or a symlinked slskd ancestor. Both roots must be physically
    # openable without following links and must stay disjoint after canonical
    # resolution.
    with open_directory_path(slskd_download_dir):
        try:
            physical_overlap = paths_overlap(
                os.path.realpath(processing_dir),
                os.path.realpath(slskd_download_dir),
            )
        except OSError as exc:
            raise FilesystemAuthorityError("cannot canonicalize configured roots") from exc
    if physical_overlap:
        raise FilesystemAuthorityError("processing_dir physically overlaps slskd_download_dir")
    with open_directory_path(processing_dir) as fd:
        yield fd


@contextmanager
def open_relative_directory(root_fd: int, relative_path: str) -> Iterator[int]:
    """Walk a directory under an already-authoritative root descriptor."""
    fd = os.dup(root_fd)
    try:
        for part in _parts(relative_path):
            try:
                child = os.open(part, _DIR_FLAGS, dir_fd=fd)
            except OSError as exc:
                raise _raise_path_error(relative_path, exc) from exc
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


@contextmanager
def open_private_child_directory(root_fd: int, name: str) -> Iterator[int]:
    """Open one required 0700 child of the private processing root.

    Nix creates ``albums`` and ``preview``.  Rechecking their owner and
    permissions here turns a misplaced tmpfiles rule or a later chmod into a
    deterministic startup failure instead of an authority downgrade.
    """
    if name not in {"albums", "preview"}:
        raise FilesystemAuthorityError("unknown private processing child")
    try:
        fd = os.open(name, _DIR_FLAGS, dir_fd=root_fd)
    except OSError as exc:
        raise _raise_path_error(name, exc) from exc
    try:
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise FilesystemAuthorityError(
                f"private processing child is not service-owned 0700: {name}")
        yield fd
    finally:
        os.close(fd)


@dataclass
class OpenedRegularFile:
    """A regular file plus the parent/inode identity needed for safe unlink."""

    fd: int
    parent_fd: int
    name: str
    stat_result: os.stat_result

    def close(self) -> None:
        os.close(self.fd)
        os.close(self.parent_fd)


@dataclass
class HeldDirectory:
    """A configured quarantine directory held open for its whole use.

    ``display_path`` is audit/UI metadata only.  Consumers must traverse
    ``fd`` rather than closing it and opening that pathname again.
    """

    fd: int
    display_path: str
    authority_root: str

    def close(self) -> None:
        os.close(self.fd)


@contextmanager
def open_configured_quarantine_directory(raw_path: str, cfg: object) -> Iterator[HeldDirectory]:
    """Resolve a DB/path payload through the configured quarantine roots.

    The required marker is a *path component*, never a string prefix.  In
    particular this permits an importer quarantine nested below Incoming,
    while refusing lookalikes such as ``failed_imports-old``.  The returned
    descriptor remains open until the caller has finished scanning/copying.
    """
    from lib.processing_paths import processing_albums_dir

    roots = (
        (getattr(cfg, "slskd_download_dir"), frozenset({"failed_imports", "wrong_matches"})),
        (getattr(cfg, "beets_staging_dir"), frozenset({"failed_imports"})),
        (
            processing_albums_dir(getattr(cfg, "processing_dir")),
            frozenset({"failed_imports", "wrong_matches"}),
        ),
    )
    if not isinstance(raw_path, str) or not raw_path:
        raise FilesystemAuthorityError("quarantine path is missing")

    for root, markers in roots:
        if not isinstance(root, str) or not os.path.isabs(root):
            continue
        try:
            if os.path.isabs(raw_path):
                relative = _relative_to(root, os.path.abspath(os.path.normpath(raw_path)))
            else:
                relative = os.path.normpath(raw_path)
                _parts(relative)
        except FilesystemAuthorityError:
            continue
        parts = _parts(relative)
        if not markers.intersection(parts):
            continue
        try:
            with open_directory_path(root) as root_fd:
                with open_relative_directory(root_fd, relative) as candidate_fd:
                    held = HeldDirectory(
                        fd=os.dup(candidate_fd),
                        display_path=os.path.abspath(os.path.join(root, relative)),
                        authority_root=root,
                    )
        except FilesystemAuthorityError:
            continue
        try:
            yield held
        finally:
            held.close()
        return
    raise FilesystemAuthorityError("path is outside configured quarantine roots")


def open_regular_relative(root_fd: int, relative_path: str) -> OpenedRegularFile:
    """Open one regular descendant without ever following a pathname link."""
    parts = _parts(relative_path)
    parent_fd = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            try:
                child = os.open(part, _DIR_FLAGS, dir_fd=parent_fd)
            except OSError as exc:
                raise _raise_path_error(relative_path, exc) from exc
            os.close(parent_fd)
            parent_fd = child
        try:
            fd = os.open(parts[-1], _FILE_FLAGS, dir_fd=parent_fd)
        except OSError as exc:
            raise _raise_path_error(relative_path, exc) from exc
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            os.close(fd)
            raise FilesystemAuthorityError(f"not a regular file: {relative_path}")
        return OpenedRegularFile(fd=fd, parent_fd=parent_fd, name=parts[-1], stat_result=info)
    except Exception:
        os.close(parent_fd)
        raise


def open_regular_under_root(root: str, candidate: str) -> OpenedRegularFile:
    """Open an absolute candidate only through the supplied authority root."""
    relative = _relative_to(root, candidate)
    with open_directory_path(root) as root_fd:
        return open_regular_relative(root_fd, relative)


def unlink_if_same(opened: OpenedRegularFile) -> bool:
    """Unlink only while the pathname still names the preflighted inode."""
    try:
        current = os.stat(opened.name, dir_fd=opened.parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    if (
        not stat.S_ISREG(current.st_mode)
        or current.st_dev != opened.stat_result.st_dev
        or current.st_ino != opened.stat_result.st_ino
    ):
        return False
    os.unlink(opened.name, dir_fd=opened.parent_fd)
    os.fsync(opened.parent_fd)
    return True


def copy_opened_file(
    source_fd: int,
    destination_fd: int,
    *,
    max_bytes: int | None = None,
    before_write: Callable[[int], None] | None = None,
) -> int:
    """Copy already-authorized bytes and durably flush the destination.

    ``max_bytes`` is a hard ceiling, not a hint: it prevents a source that
    grows after its descriptor was preflighted from bypassing a caller's
    bounded-copy policy.
    """
    copied = 0
    while True:
        remaining = None if max_bytes is None else max_bytes - copied
        if remaining is not None and remaining < 0:
            raise FilesystemAuthorityError("source grew beyond copy limit")
        read_size = 1024 * 1024 if remaining is None else min(1024 * 1024, remaining + 1)
        chunk = os.read(source_fd, read_size)
        if not chunk:
            break
        if max_bytes is not None and len(chunk) > max_bytes - copied:
            raise FilesystemAuthorityError("source grew beyond copy limit")
        if before_write is not None:
            before_write(len(chunk))
        view = memoryview(chunk)
        while view:
            written = os.write(destination_fd, view)
            view = view[written:]
        copied += len(chunk)
    os.fsync(destination_fd)
    return copied
