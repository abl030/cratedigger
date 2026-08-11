"""No-follow file-descriptor authority for untrusted filesystem trees.

Paths received from slskd, the database, or HTTP are names, not authority.
Callers open an explicitly configured root once and walk every descendant
relative to that descriptor with ``O_NOFOLLOW``.  The returned descriptors
are deliberately the objects consumed by readers/copy routines: resolving a
name and opening it later would re-introduce a symlink/swap race.
"""

from __future__ import annotations

import ctypes
import errno
import fcntl
import os
import stat
from collections.abc import Callable, Generator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from typing import Literal

FsAuthorityCode = Literal[
    "unspecified",
    "path_escape",
    "unsafe_symlink",
    "not_a_directory",
    "not_regular_file",
    "untrusted_ownership",
    "missing",
    "open_failed",
    "read_failed",
    "write_failed",
]
"""Machine-stable classification of one filesystem-authority refusal.

Consumers branch on ``FilesystemAuthorityError.code`` and NEVER on the
exception message (issue #868). The message is human diagnostics and is
free to change; the code is the contract. The families are deliberately
kept apart so a *containment* violation — the security boundary: a
symlink, a path escape, a component that is not a directory, or a
non-regular file — can never be reported as an ordinary storage error,
nor a storage errno as a containment violation.

``path_escape``, ``unsafe_symlink``, ``not_a_directory``,
``not_regular_file`` and ``untrusted_ownership`` are the containment
codes: the name — or the authority the tree is held under — is
untrustworthy. ``untrusted_ownership`` covers the private-tree ownership
and permission assertions (a group/other-writable ancestor, a root that
is not owned by the service identity, a root or child that is not mode
0700). Those are security-relevant authority downgrades, not the
"renameat2 is unsupported" miscellany ``unspecified`` collects, and
lumping them together fused ~13 causes into one reason (issue #868). ``missing``, ``open_failed``, ``read_failed`` and ``write_failed`` are
not — they say nothing about trust. Consumers that translate these into their own vocabulary do
so with an exhaustive ``match`` so a new code cannot be silently lumped
in with either group.

``open_failed``, ``read_failed`` and ``write_failed`` are three DIFFERENT
storage facts and must not borrow each other's nouns: a destination that
ran out of space opened perfectly well, and saying it "could not be
opened" is a specific claim that is simply false (issue #868 review B2).
Each carries its errno.

``unspecified`` is the default for every raise site that predates the
classification (configured-root policy checks, preview/quarantine
guards). It is deliberately NOT a synonym for "safe": consumers that map
codes to their own vocabulary must fail closed on it.
"""


class FilesystemAuthorityError(ValueError):
    """Configured or untrusted path violates the filesystem boundary.

    ``code`` is the structured discriminator (see :data:`FsAuthorityCode`);
    ``errno_symbol`` carries the originating errno name (``"ESTALE"``,
    ``"EIO"``, …) for every storage code — ``"open_failed"``,
    ``"read_failed"`` and ``"write_failed"`` — so a caller can record
    *which* storage failure happened without parsing ``strerror``.
    """

    def __init__(
        self,
        message: str,
        *,
        code: FsAuthorityCode = "unspecified",
        errno_symbol: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code: FsAuthorityCode = code
        self.errno_symbol: str | None = errno_symbol


class SharedDownloadRootError(FilesystemAuthorityError):
    """A refusal to open the UNTRUSTED shared download root.

    Deliberately its own type rather than a flag: WHOSE root failed is a
    dispatch decision, and an ``except`` clause makes it structural.
    Callers that hold both a private tree and a third-party download
    share must never report a refusal of the share as a refusal of their
    own tree — on this deployment the share is a nested virtiofs mount
    with a convicted history of transient ESTALE/EIO, so "the private
    processing tree is sick" would name the wrong subsystem entirely
    (issue #868). The underlying refusal's ``code`` and ``errno_symbol``
    are preserved so the cause survives the re-attribution.
    """

    @classmethod
    def wrapping(cls, exc: FilesystemAuthorityError) -> SharedDownloadRootError:
        return cls(
            f"shared download root refused: {exc}",
            code=exc.code,
            errno_symbol=exc.errno_symbol,
        )


class CopySourceReadError(FilesystemAuthorityError):
    """Reading the already-authorized SOURCE failed mid-copy.

    Its own type for the same reason as :class:`SharedDownloadRootError`:
    the copy phase touches two subjects, and which one failed is a
    dispatch decision. The share is read in full here — it is where a
    flaky mount is most likely to fire — and reporting an ESTALE on the
    source as a failure of our own private tree names the wrong subsystem
    (issue #868 review).
    """


class CopyDestinationWriteError(FilesystemAuthorityError):
    """Writing or flushing the DESTINATION failed (e.g. ENOSPC, EIO).

    Carries ``write_failed`` — never ``open_failed``: the destination was
    opened successfully, and the whole point of the structured code is
    that a consumer's copy can state what actually went wrong.
    """


_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
_FILE_FLAGS = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
_RENAME_NOREPLACE = 1
_LIBC = ctypes.CDLL(None, use_errno=True)


def _parts(relative_path: str) -> tuple[str, ...]:
    if not relative_path or os.path.isabs(relative_path):
        raise FilesystemAuthorityError(
            "path must be a non-empty relative path", code="path_escape")
    parts = tuple(relative_path.split(os.sep))
    if any(part in ("", ".", "..") for part in parts):
        raise FilesystemAuthorityError(
            "path contains an unsafe component", code="path_escape")
    return parts


def errno_symbol(exc: OSError) -> str:
    """Name the errno without ever parsing ``strerror`` (issue #868)."""
    if exc.errno is None:
        return "UNKNOWN"
    return errno.errorcode.get(exc.errno, f"ERRNO{exc.errno}")


# A no-follow ``open`` of a special file fails BEFORE the descriptor
# exists, so ``open_regular_relative``'s ``S_ISREG`` check never gets to
# run: a unix-domain socket answers ENXIO and a device node with no
# driver answers ENODEV. Both describe the *kind* of thing at the name,
# not the health of the storage, so they belong with the containment
# codes — otherwise a socket planted at a stamped path would be reported
# as a storage failure (issue #868).
_SPECIAL_FILE_ERRNOS = (errno.ENXIO, errno.ENODEV)


def classify_path_errno(exc: OSError) -> FsAuthorityCode:
    """Map one failed path syscall onto the structured refusal vocabulary.

    The ONE errno classifier in the repository: both the raising
    descriptor helpers and the non-raising :func:`observe_directory`
    probe read their code from here, so "what does EACCES mean" cannot
    drift between the two (issue #1063).

    ELOOP (a symlink), ENOTDIR (a non-directory used as a path component)
    and the special-file errnos all mean the *name* is not what we
    require — the containment boundary; each gets its own code because a
    code that says "symlink" about a regular-file-as-directory is a lie
    the message would immediately contradict. ENOENT means the name is
    simply gone. Every other errno is the storage layer failing (ESTALE
    and EIO are the live virtiofs shapes, EACCES the live private-tree
    shape); those must never be confused with a containment violation,
    so the errno name travels with the error instead of only reaching a
    human-readable ``strerror``.
    """
    if exc.errno == errno.ELOOP:
        return "unsafe_symlink"
    if exc.errno == errno.ENOTDIR:
        # openat(O_DIRECTORY|O_NOFOLLOW) answers ENOTDIR — not ELOOP — for
        # a symlink to a directory, so this branch cannot tell the two
        # apart and must not claim to (issue #868 review).
        return "not_a_directory"
    if exc.errno in _SPECIAL_FILE_ERRNOS:
        return "not_regular_file"
    if exc.errno == errno.ENOENT:
        return "missing"
    return "open_failed"


def _raise_path_error(path: str, exc: OSError) -> FilesystemAuthorityError:
    """Classify one failed no-follow open into a structured refusal."""
    code = classify_path_errno(exc)
    if code == "unsafe_symlink":
        return FilesystemAuthorityError(
            f"unsafe symlink: {path}", code=code)
    if code == "not_a_directory":
        return FilesystemAuthorityError(
            f"path component is not a directory, or is a symlink to one: {path}",
            code=code)
    if code == "not_regular_file":
        return FilesystemAuthorityError(
            f"not a regular file: {path}", code=code)
    if code == "missing":
        return FilesystemAuthorityError(
            f"cannot open {path}: {exc.strerror}", code=code)
    return FilesystemAuthorityError(
        f"cannot open {path}: {exc.strerror}",
        code=code,
        errno_symbol=errno_symbol(exc),
    )


DirectoryPresence = Literal["present", "absent", "indeterminate"]
"""Whether a name was PROVEN to hold a directory, proven not to, or neither."""


# Only these two codes prove a directory is genuinely not there: the name
# does not exist (ENOENT), or something that is not a directory does
# (ENOTDIR, or a successful stat of a non-directory). Every other refusal
# — EACCES/EPERM on an unreadable parent, EIO/ESTALE on a sick mount, a
# symlink loop — is an observation we were not permitted or able to make.
# Reporting one of those as absence is the issue #1063 defect: an
# indeterminate observation stated as a definitive negative fact.
_ABSENCE_CODES: frozenset[FsAuthorityCode] = frozenset({"missing", "not_a_directory"})


def refusal_is_indeterminate(code: FsAuthorityCode) -> bool | None:
    """Is this refusal a WORLD failure rather than a verdict about the name?

    The single owner of that question for every consumer that translates
    :class:`FilesystemAuthorityError` into its own vocabulary (issue
    #1063). ``True`` means the storage layer refused or failed and the
    caller learned nothing about the path — retryable, never a semantic
    complaint about the operator's input. The ``match`` is exhaustive on
    purpose: a new :data:`FsAuthorityCode` must be classified here rather
    than silently inheriting either group (issue #868's rule).

    Declared ``bool | None`` because that is the truth: adding a
    ``case _`` to satisfy a bare ``-> bool`` would destroy the very
    exhaustiveness this function exists for (and which
    ``TestIndeterminateRefusalVocabulary`` checks against
    ``get_args(FsAuthorityCode)``), so a value outside the ``Literal``
    falls through and returns ``None``. Every consumer treats that
    falsy result as "not indeterminate", which is the fail-safe side:
    an unclassifiable refusal is reported as a refusal, not as a
    retryable blip.
    """
    match code:
        case "open_failed" | "read_failed" | "write_failed":
            return True
        case (
            "unspecified"
            | "path_escape"
            | "unsafe_symlink"
            | "not_a_directory"
            | "not_regular_file"
            | "untrusted_ownership"
            | "missing"
        ):
            return False


_REFUSAL_CHAIN_MAX_DEPTH = 16


def indeterminate_os_refusal(exc: BaseException) -> OSError | None:
    """Find the storage refusal hiding inside a third-party exception.

    Tag readers convert an ``OSError`` into their own vocabulary before
    it ever reaches us — mutagen raises ``MutagenError``, mediafile
    re-raises that as ``UnreadableFileError``, beets wraps it once more
    as ``ReadError`` — so ``except OSError`` never fires and an EACCES
    file is indistinguishable from a corrupt one. The originating error
    survives on the explicit ``__cause__`` / implicit ``__context__``
    chain; this walks it and returns the first ``OSError`` whose errno
    :func:`classify_path_errno` + :func:`refusal_is_indeterminate` call a
    world failure (issue #1063).

    A corrupt, truncated or unsupported file carries no ``OSError``
    anywhere on that chain and returns ``None``: "this file's tags are
    garbage" is a fact ABOUT the file, and reporting it as a refusal
    would re-launder the very distinction this module exists to keep.

    The walk is bounded and cycle-guarded: an exception chain is
    attacker-influenced data in exactly the same way a path is.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    for _ in range(_REFUSAL_CHAIN_MAX_DEPTH):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        if (
            isinstance(current, OSError)
            and refusal_is_indeterminate(classify_path_errno(current)) is True
        ):
            return current
        current = current.__cause__ or current.__context__
    return None


@dataclass(frozen=True)
class DirectoryObservation:
    """One truthful answer to "is there a directory at this name?".

    ``present`` carries the absolute ``path``. ``absent`` is a POSITIVE
    proof of absence and is the only state that may authorize
    absence-driven cleanup. ``indeterminate`` means the probe was
    refused: fail closed, keep the pointer, say so.

    Deliberately NOT ``str | None``: that shape is exactly what let
    ``os.path.isdir`` — which answers ``False`` for EACCES just as it
    does for ENOENT — launder "I could not look" into "it is not there"
    (issue #1063).
    """

    presence: DirectoryPresence
    path: str | None = None
    code: FsAuthorityCode | None = None
    errno_symbol: str | None = None
    detail: str | None = None

    @property
    def present(self) -> bool:
        return self.presence == "present"

    @property
    def absent(self) -> bool:
        return self.presence == "absent"

    @property
    def indeterminate(self) -> bool:
        return self.presence == "indeterminate"

    def unavailable_reason(self) -> str:
        """Stable operator/audit text for an indeterminate observation."""
        symbol = self.errno_symbol or self.code or "UNKNOWN"
        return f"path_unavailable[{symbol}]: {self.detail or 'probe refused'}"


def observe_directory(path: str) -> DirectoryObservation:
    """Probe one absolute-or-relative name without ever lying about it."""
    if not path:
        return DirectoryObservation(
            presence="absent", code="missing", detail="empty path")
    try:
        info = os.stat(path)
    except OSError as exc:
        code = classify_path_errno(exc)
        return DirectoryObservation(
            presence="absent" if code in _ABSENCE_CODES else "indeterminate",
            code=code,
            errno_symbol=errno_symbol(exc),
            detail=f"{path}: {exc.strerror}",
        )
    except ValueError as exc:
        # An embedded NUL (or other un-syscallable name) is not evidence
        # of absence; ``os.path.isdir`` swallowed it as ``False``.
        return DirectoryObservation(
            presence="indeterminate",
            code="path_escape",
            detail=f"{path!r}: {exc}",
        )
    if not stat.S_ISDIR(info.st_mode):
        return DirectoryObservation(
            presence="absent",
            code="not_a_directory",
            detail=f"{path}: not a directory",
        )
    return DirectoryObservation(presence="present", path=os.path.abspath(path))


@contextmanager
def open_directory_path(path: str) -> Generator[int]:
    """Open an absolute directory while refusing every symlink component."""
    if not os.path.isabs(path):
        raise FilesystemAuthorityError(
            "authority root must be absolute", code="path_escape")
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
        raise FilesystemAuthorityError(
            "candidate path must be absolute", code="path_escape")
    try:
        relative = os.path.relpath(path, root)
    except ValueError as exc:
        raise FilesystemAuthorityError(
            "candidate is not beneath authority root", code="path_escape") from exc
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
                # The directory is already OPEN; it is the stat that failed.
                # ``open_failed`` here rendered "could not be opened
                # (ESTALE)" — the same verb-borrow B2 was chartered to
                # remove (issue #868 review F4).
                raise FilesystemAuthorityError(
                    f"cannot inspect {current}: {exc.strerror}",
                    code="read_failed",
                    errno_symbol=errno_symbol(exc),
                ) from exc
            if info.st_mode & 0o022:
                raise FilesystemAuthorityError(
                    f"private processing ancestor is group/other writable: {current}",
                    code="untrusted_ownership")
            if current == root and info.st_uid != expected_uid:
                raise FilesystemAuthorityError(
                    f"private processing root is not owned by service identity: {current}",
                    code="untrusted_ownership")
            if current == root and stat.S_IMODE(info.st_mode) != 0o700:
                raise FilesystemAuthorityError(
                    f"private processing root is not mode 0700: {current}",
                    code="untrusted_ownership")
    finally:
        os.close(fd)


def same_open_directory(path: str, held_fd: int) -> bool:
    """Does a fresh no-follow open of ``path`` name the held directory?"""
    with open_directory_path(path) as reopened_fd:
        reopened = os.fstat(reopened_fd)
    held = os.fstat(held_fd)
    return reopened.st_dev == held.st_dev and reopened.st_ino == held.st_ino


@contextmanager
def exclusive_relative_lock(root_fd: int, name: str) -> Generator[None]:
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


def rename_between_directories_noreplace(
    source_parent_fd: int,
    source: str,
    destination_parent_fd: int,
    destination: str,
) -> bool:
    """Atomically rename one child between held directories, no replacement.

    Linux's ``renameat2(RENAME_NOREPLACE)`` is available on every supported
    NixOS target.  ``False`` is the only recoverable race: a winner already
    exists. Every other errno is surfaced unchanged. Both names are resolved
    only beneath caller-held directory descriptors; no absolute path is
    reopened at the mutation boundary.
    """
    source_part = _parts(source)
    destination_part = _parts(destination)
    if len(source_part) != 1 or len(destination_part) != 1:
        raise FilesystemAuthorityError("rename names must be one safe component")
    try:
        renameat2 = _LIBC.renameat2
    except AttributeError as exc:  # pragma: no cover - unsupported platform
        raise FilesystemAuthorityError("renameat2 is required on this platform") from exc
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        source_parent_fd,
        os.fsencode(source_part[0]),
        destination_parent_fd,
        os.fsencode(destination_part[0]),
        _RENAME_NOREPLACE,
    )
    if result == 0:
        return True
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        return False
    raise OSError(error, os.strerror(error), source)


def rename_relative_noreplace(parent_fd: int, source: str, destination: str) -> bool:
    """Atomically publish one relative name without replacing a destination."""
    return rename_between_directories_noreplace(
        parent_fd,
        source,
        parent_fd,
        destination,
    )


def remove_relative_tree(
    parent_fd: int,
    name: str,
    *,
    before_mutation: Callable[[], None] | None = None,
) -> None:
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
            children = sorted(entries, key=lambda entry: entry.name)
        for entry in children:
            if entry.is_dir(follow_symlinks=False):
                remove_relative_tree(
                    fd,
                    entry.name,
                    before_mutation=before_mutation,
                )
            else:
                if before_mutation is not None:
                    before_mutation()
                os.unlink(entry.name, dir_fd=fd)
        if before_mutation is not None:
            before_mutation()
        os.fsync(fd)
    finally:
        os.close(fd)
    if before_mutation is not None:
        before_mutation()
    os.rmdir(part[0], dir_fd=parent_fd)
    if before_mutation is not None:
        before_mutation()
    os.fsync(parent_fd)


@contextmanager
def open_private_processing_root(
    processing_dir: str, slskd_download_dir: str,
) -> Generator[int]:
    """Open the configured private root after its complete trust checks.

    This opens BOTH roots — the private tree and, for the physical-overlap
    proof, the untrusted shared download share. A refusal of the share is
    raised as :class:`SharedDownloadRootError` so callers can attribute it
    to the right subsystem instead of inferring ownership from ordering or
    from the message (issue #868).
    """
    if not os.path.isabs(processing_dir):
        raise FilesystemAuthorityError("processing_dir must be absolute")
    if paths_overlap(processing_dir, slskd_download_dir):
        raise FilesystemAuthorityError("processing_dir overlaps slskd_download_dir")
    _assert_private_parent(processing_dir)
    # A lexical comparison alone misses a configured root reached through a
    # bind mount or a symlinked slskd ancestor. Both roots must be physically
    # openable without following links and must stay disjoint after canonical
    # resolution.
    with ExitStack() as shared_root_scope:
        # ONLY the share's own open is re-attributed (inside the helper).
        # The overlap proof below concerns the RELATION between two
        # configured roots, not either subsystem's health, so it stays
        # unattributed and fails closed.
        shared_root_scope.enter_context(
            open_shared_download_root(slskd_download_dir))
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
def open_relative_directory(
    root_fd: int, relative_path: str,
) -> Generator[int]:
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
def open_private_child_directory(
    root_fd: int, name: str,
) -> Generator[int]:
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
                f"private processing child is not service-owned 0700: {name}",
                code="untrusted_ownership")
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
    """A configured directory held open for its whole use.

    ``display_path`` is audit/UI metadata and LEXICAL input only.
    Consumers must traverse ``fd`` rather than closing it and opening that
    pathname again — the sanctioned lexical use is computing a candidate's
    relative path for the containment check, which still opens beneath
    ``fd``. Pairing the two here is deliberate: a caller handed a separate
    ``root``/``root_fd`` could pass a mismatched pair and compute the
    relative name against a root the descriptor does not name.
    """

    fd: int
    display_path: str

    def close(self) -> None:
        os.close(self.fd)


@contextmanager
def open_configured_quarantine_directory(
    raw_path: str, cfg: object,
) -> Generator[HeldDirectory]:
    """Resolve a DB/path payload through the configured quarantine roots.

    The required marker is a *path component*, never a string prefix.  In
    particular this permits an importer quarantine nested below Incoming,
    while refusing lookalikes such as ``failed_imports-old``.  The returned
    descriptor remains open until the caller has finished scanning/copying.
    """
    from lib.processing_paths import processing_albums_dir

    roots = (
        (
            getattr(cfg, "slskd_download_dir"),  # noqa: B009 - structural config boundary
            frozenset({"failed_imports", "wrong_matches"}),
        ),
        (
            getattr(cfg, "beets_staging_dir"),  # noqa: B009 - structural config boundary
            frozenset({"failed_imports", "wrong_matches"}),
        ),
        (
            processing_albums_dir(
                getattr(cfg, "processing_dir"),  # noqa: B009 - structural config boundary
            ),
            frozenset({"failed_imports", "wrong_matches"}),
        ),
    )
    if not raw_path:
        raise FilesystemAuthorityError("quarantine path is missing")

    # A refusal AFTER containment was established says nothing about
    # containment. Keeping the first such refusal is what stops the loop
    # from exhausting into the "outside configured quarantine roots"
    # verdict it never evaluated — the live #1063 lie, where EACCES on
    # the 0700 processing root accused the operator's config of a
    # containment problem that did not exist. ``missing`` is the one
    # refusal that legitimately continues: a relative legacy path is
    # contained by every root lexically and must be probed under each.
    contained_refusal: FilesystemAuthorityError | None = None
    contained_missing = False

    for root, markers in roots:
        if not os.path.isabs(root):
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
            with open_directory_path(root) as root_fd, open_relative_directory(root_fd, relative) as candidate_fd:
                held = HeldDirectory(
                    fd=os.dup(candidate_fd),
                    display_path=os.path.abspath(os.path.join(root, relative)),
                )
        except FilesystemAuthorityError as exc:
            if exc.code == "missing":
                contained_missing = True
            elif contained_refusal is None:
                contained_refusal = exc
            continue
        try:
            yield held
        finally:
            held.close()
        return
    if contained_refusal is not None:
        raise FilesystemAuthorityError(
            f"quarantine path is contained but unavailable: {contained_refusal}",
            code=contained_refusal.code,
            errno_symbol=contained_refusal.errno_symbol,
        )
    if contained_missing:
        raise FilesystemAuthorityError(
            f"quarantine path does not exist under its configured root: {raw_path}",
            code="missing",
        )
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
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise FilesystemAuthorityError(
                    f"not a regular file: {relative_path}",
                    code="not_regular_file")
        except Exception:
            # The outer handler only owns ``parent_fd``; an ``fstat`` that
            # raises here would otherwise leak the file descriptor.
            os.close(fd)
            raise
        return OpenedRegularFile(fd=fd, parent_fd=parent_fd, name=parts[-1], stat_result=info)
    except Exception:
        os.close(parent_fd)
        raise


def open_regular_under_held_root(
    held: HeldDirectory, candidate: str,
) -> OpenedRegularFile:
    """Open an absolute candidate through an ALREADY-HELD authority root.

    Replaces the former ``open_regular_under_root``, which re-opened the
    root pathname for every candidate. Two reasons that was wrong for a
    batch (issue #868):

    * Attribution. Each re-open was a fresh chance for the root itself to
      refuse, and the caller had no way to tell "the whole share is
      unreachable" from "this one file is". On a flaky mount that is N
      chances per album, every one of them mis-attributed.
    * Identity. Re-resolving a name per candidate means the second half of
      a manifest can be opened under a *different* inode than the first if
      the root is swapped mid-loop. One held descriptor makes the whole
      batch provably one root — the module's own doctrine.

    Takes the paired :class:`HeldDirectory` rather than a separate
    path/descriptor so the lexical containment check cannot be computed
    against a root the descriptor does not name.
    """
    return open_regular_relative(
        held.fd, _relative_to(held.display_path, candidate))


@contextmanager
def open_shared_download_root(path: str) -> Generator[HeldDirectory]:
    """Hold the UNTRUSTED shared download root open for a whole batch.

    A refusal of the root ITSELF is raised as
    :class:`SharedDownloadRootError`; refusals of anything beneath it stay
    ordinary and are the caller's own to classify. That split is what lets
    a caller say "the share is unreachable" instead of blaming one file
    (issue #868). Only the open is guarded — an exception thrown into the
    body is the caller's and passes through untouched.

    This context manager owns the descriptor; callers must NOT call
    ``close()`` on the yielded :class:`HeldDirectory`.
    """
    with ExitStack() as scope:
        try:
            fd = scope.enter_context(open_directory_path(path))
        except FilesystemAuthorityError as exc:
            raise SharedDownloadRootError.wrapping(exc) from exc
        yield HeldDirectory(fd=fd, display_path=path)


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
        try:
            chunk = os.read(source_fd, read_size)
        except OSError as exc:
            raise CopySourceReadError(
                f"cannot read source: {exc.strerror}",
                code="read_failed",
                errno_symbol=errno_symbol(exc),
            ) from exc
        if not chunk:
            break
        if max_bytes is not None and len(chunk) > max_bytes - copied:
            raise FilesystemAuthorityError("source grew beyond copy limit")
        view = memoryview(chunk)
        while view:
            if before_write is not None:
                before_write(len(view))
            try:
                written = os.write(destination_fd, view)
            except OSError as exc:
                raise CopyDestinationWriteError(
                    f"cannot write destination: {exc.strerror}",
                    code="write_failed",
                    errno_symbol=errno_symbol(exc),
                ) from exc
            view = view[written:]
        copied += len(chunk)
    try:
        os.fsync(destination_fd)
    except OSError as exc:
        raise CopyDestinationWriteError(
            f"cannot flush destination: {exc.strerror}",
            code="write_failed",
            errno_symbol=errno_symbol(exc),
        ) from exc
    return copied
