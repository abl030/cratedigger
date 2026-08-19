"""Completed-download materialization.

This is the sole owner of turning event-stamped slskd file locations into a
request-scoped processing directory and validating an already-staged manifest.
Validation lives in :mod:`lib.download_validation`, completion orchestration in
:mod:`lib.download_processing`, and poll state in :mod:`lib.download`.
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, assert_never

from lib.download_recovery import ProcessingPathLocation, classify_processing_path
from lib.fs_authority import (
    CopyDestinationWriteError,
    CopySourceReadError,
    FilesystemAuthorityError,
    OpenedRegularFile,
    SharedDownloadRootError,
    copy_opened_file,
    errno_symbol,
    exclusive_relative_lock,
    open_private_child_directory,
    open_private_processing_root,
    open_regular_relative,
    open_regular_under_held_root,
    open_relative_directory,
    open_shared_download_root,
    remove_relative_tree,
    rename_relative_noreplace,
    same_open_directory,
    unlink_if_same,
)
from lib.grab_list import DownloadFile, GrabListEntry
from lib.import_execution import CancellationToken
from lib.import_manifest import audio_relative_paths, manifest_trace_summary
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_folder_for_row,
    processing_albums_dir,
)
from lib.staged_album import StagedAlbum

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


def _checkpoint(cancellation_token: CancellationToken | None) -> None:
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()


def _remove_relative_tree_cancellable(
    parent_fd: int,
    name: str,
    cancellation_token: CancellationToken | None,
) -> None:
    if cancellation_token is None:
        remove_relative_tree(parent_fd, name)
        return
    remove_relative_tree(
        parent_fd,
        name,
        before_mutation=cancellation_token.raise_if_cancelled,
    )


# === Materialize failure reasons (issue #868) ===
#
# Short machine-stable codes, never operator copy: the web UI's wording
# is owned elsewhere. Every one is DERIVED — from a structured
# ``FsAuthorityCode`` or from an explicit branch — never parsed back out
# of an exception message.
#
# Historically the four distinguishable source-preflight outcomes
# collapsed into two strings: a missing event stamp and a stamped file
# that had vanished from disk both surfaced as ``event_path_missing``
# (the string-sniffed ``"No such file" in str(exc)`` branch), while a
# genuine containment violation and an ordinary storage error (ESTALE /
# EIO from virtiofs) both surfaced as ``unsafe_source_path``. Those are
# four different operator problems with four different remedies.
#
# THREE subjects can refuse, and each owns its own nouns. Reporting one
# subject's failure in another's vocabulary is the same defect one layer
# up: "the private processing tree is sick" when the shared slskd share
# is, or "this file's event stamp is stale" when the whole share is gone.
REASON_EVENT_PATH_NEVER_STAMPED: Final = "event_path_never_stamped"
"""slskd never emitted a completion event carrying this file's location."""

REASON_EVENT_PATH_GONE_FROM_DISK: Final = "event_path_gone_from_disk"
"""The event stamp exists, but nothing is at that path any more."""

REASON_UNSAFE_SOURCE_PATH: Final = "unsafe_source_path"
"""The stamped name failed the containment boundary (symlink/escape/special)."""

REASON_SOURCE_OPEN_FAILED_PREFIX: Final = "source_open_failed_"
"""Prefix for a storage-layer open failure; suffixed with the errno name."""

REASON_SOURCE_READ_FAILED_PREFIX: Final = "source_read_failed_"
"""Prefix for a read failure on an ALREADY-OPEN stamped file; errno-suffixed.

Distinct from ``source_open_failed_``: the copy phase reads every byte the
share has, and reporting a mid-copy ESTALE as an *open* failure is the
same specific-but-false claim ``processing_open_failed_`` made about
writes (issue #868 review B2)."""

REASON_SOURCE_WRITE_FAILED_PREFIX: Final = "source_write_failed_"
"""Unreachable: the only write this module makes to the share is
``unlink_if_same``'s unlink+flush, which never travels as a
``CopyDestinationWriteError``. Present because every subject answers every
storage fact, so a new raise site cannot land in another subject's noun."""

REASON_SOURCE_PREFLIGHT_REFUSED: Final = "source_preflight_refused"
"""A refusal of one stamped file with no structured code."""

REASON_PROCESSING_AUTHORITY_UNSAFE: Final = "processing_authority_unsafe"
"""Our own private processing tree failed the containment boundary."""

REASON_PROCESSING_PATH_MISSING: Final = "processing_path_missing"
"""A required private processing path was absent."""

REASON_PROCESSING_OPEN_FAILED_PREFIX: Final = "processing_open_failed_"
"""Prefix for a storage-layer open failure on the private tree; errno-suffixed."""

REASON_PROCESSING_READ_FAILED_PREFIX: Final = "processing_read_failed_"
"""Prefix for a read failure on our own already-open private tree."""

REASON_PROCESSING_WRITE_FAILED_PREFIX: Final = "processing_write_failed_"
"""Prefix for a write/flush failure on the private tree; errno-suffixed.

ENOSPC and a failing fsync are the live shapes. They are NOT open
failures — the destination opened fine."""

REASON_MATERIALIZE_AUTHORITY_FAILED: Final = "materialize_authority_failed"
"""An authority refusal on the private tree with no structured code.

Deliberately the LAST resort, and now genuinely narrow. The private
tree's ownership and permission assertions — a group/other-writable
ancestor, a root not owned by the service identity, a root or child that
is not mode 0700 — used to land here alongside "renameat2 is
unsupported", fusing security-relevant authority downgrades with
environment miscellany. They carry ``untrusted_ownership`` since #868's
review and answer ``processing_authority_unsafe`` instead. What remains
here is the residue that says nothing about trust: an unsupported
``renameat2``, a lock that could not be taken, the ``unknown private
processing child`` programming guard.
"""

REASON_SLSKD_ROOT_UNSAFE: Final = "slskd_root_unsafe"
"""The configured shared download root failed the containment boundary."""

REASON_SLSKD_ROOT_MISSING: Final = "slskd_root_missing"
"""The configured shared download root is not there at all."""

REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX: Final = "slskd_root_open_failed_"
"""Prefix for a storage-layer open failure on the shared share; errno-suffixed."""

REASON_SLSKD_ROOT_READ_FAILED_PREFIX: Final = "slskd_root_read_failed_"
"""Prefix for a read failure on the already-open shared share."""

REASON_SLSKD_ROOT_WRITE_FAILED_PREFIX: Final = "slskd_root_write_failed_"
"""Unreachable for the same reason as its source-file sibling; kept so the
vocabulary stays total over every storage fact."""

REASON_SLSKD_ROOT_REFUSED: Final = "slskd_root_refused"
"""A refusal of the shared download root with no structured code."""

REASON_PRIVATE_MATERIALIZE_FAILED: Final = "private_materialize_failed"
"""An unclassified ``OSError`` escaped the private publish transaction."""


@dataclass(frozen=True)
class _ReasonVocabulary:
    """One subject's four nouns for the same four authority outcomes.

    Kept as data rather than three near-identical mappers so the
    containment / missing / storage partition is decided ONCE. A new
    subject cannot accidentally file a storage errno under its
    containment noun, because it never gets to make that choice.
    """

    unsafe: str
    missing: str
    open_failed_prefix: str
    read_failed_prefix: str
    write_failed_prefix: str
    unclassified: str


_SOURCE_FILE_VOCABULARY: Final = _ReasonVocabulary(
    unsafe=REASON_UNSAFE_SOURCE_PATH,
    missing=REASON_EVENT_PATH_GONE_FROM_DISK,
    open_failed_prefix=REASON_SOURCE_OPEN_FAILED_PREFIX,
    read_failed_prefix=REASON_SOURCE_READ_FAILED_PREFIX,
    write_failed_prefix=REASON_SOURCE_WRITE_FAILED_PREFIX,
    # NOT ``unsafe_source_path``: reporting a refusal we could not
    # classify as a containment violation manufactures a security finding
    # out of ignorance, which is the same lie as reporting one subject's
    # failure in another's vocabulary. Every subject says "refused" when
    # it does not know. (Unreachable in practice — every code the source
    # preflight can hit is classified — so this is consistency, not a
    # live branch.)
    unclassified=REASON_SOURCE_PREFLIGHT_REFUSED,
)
_PRIVATE_TREE_VOCABULARY: Final = _ReasonVocabulary(
    unsafe=REASON_PROCESSING_AUTHORITY_UNSAFE,
    missing=REASON_PROCESSING_PATH_MISSING,
    open_failed_prefix=REASON_PROCESSING_OPEN_FAILED_PREFIX,
    read_failed_prefix=REASON_PROCESSING_READ_FAILED_PREFIX,
    write_failed_prefix=REASON_PROCESSING_WRITE_FAILED_PREFIX,
    unclassified=REASON_MATERIALIZE_AUTHORITY_FAILED,
)
_SHARED_ROOT_VOCABULARY: Final = _ReasonVocabulary(
    unsafe=REASON_SLSKD_ROOT_UNSAFE,
    missing=REASON_SLSKD_ROOT_MISSING,
    open_failed_prefix=REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX,
    read_failed_prefix=REASON_SLSKD_ROOT_READ_FAILED_PREFIX,
    write_failed_prefix=REASON_SLSKD_ROOT_WRITE_FAILED_PREFIX,
    unclassified=REASON_SLSKD_ROOT_REFUSED,
)


def _reason_in_vocabulary(
    exc: FilesystemAuthorityError, vocabulary: _ReasonVocabulary,
) -> str:
    """Translate one structured refusal into one subject's vocabulary.

    Total over :data:`~lib.fs_authority.FsAuthorityCode` and exhaustive by
    construction (pyright rejects an unhandled code at this single site).
    Containment codes never produce a storage reason and storage errnos
    never produce a containment reason — that separation is the point:
    ``unsafe_source_path`` is a security finding, ``source_open_failed_ESTALE``
    is a sick mount.
    """
    code = exc.code
    match code:
        case (
            "path_escape"
            | "unsafe_symlink"
            | "not_a_directory"
            | "not_regular_file"
            # An ownership/permission downgrade of the tree we hold is a
            # containment failure too: a group-writable ancestor means the
            # guarantee the whole boundary rests on no longer holds. It
            # used to arrive as ``unspecified`` and fuse with "renameat2 is
            # unsupported" (issue #868 review).
            | "untrusted_ownership"
        ):
            return vocabulary.unsafe
        case "missing":
            return vocabulary.missing
        case "open_failed":
            return f"{vocabulary.open_failed_prefix}{exc.errno_symbol or 'UNKNOWN'}"
        case "read_failed":
            return f"{vocabulary.read_failed_prefix}{exc.errno_symbol or 'UNKNOWN'}"
        case "write_failed":
            return f"{vocabulary.write_failed_prefix}{exc.errno_symbol or 'UNKNOWN'}"
        case "unspecified" | "not_configured":
            # not_configured (issue #1176 PR2) names a DIFFERENT authority
            # (the local-import lane) than any subject this vocabulary
            # describes, and is unreachable through this function in
            # practice — grouped with "unspecified" rather than the
            # containment bucket above for the same reason that bucket's
            # own comment states: calling an unrelated authority's own
            # refusal a containment finding about THIS tree would
            # manufacture a security finding out of ignorance.
            return vocabulary.unclassified
    assert_never(code)


def source_preflight_reason(exc: FilesystemAuthorityError) -> str:
    """Name a refusal of ONE event-stamped file inside the shared share."""
    return _reason_in_vocabulary(exc, _SOURCE_FILE_VOCABULARY)


def materialize_authority_reason(exc: FilesystemAuthorityError) -> str:
    """Name a refusal of OUR OWN private processing tree.

    The source-preflight vocabulary would lie here: nothing about the
    private tree is an event stamp.
    """
    return _reason_in_vocabulary(exc, _PRIVATE_TREE_VOCABULARY)


def shared_download_root_reason(exc: FilesystemAuthorityError) -> str:
    """Name a refusal of the whole configured shared download root.

    Neither other vocabulary fits. ``processing_open_failed_ESTALE``
    accuses our own tree of a fault that belongs to a third-party share on
    a different (and here, historically flaky nested-virtiofs) mount;
    ``event_path_gone_from_disk`` claims something about one file's event
    stamp when the entire share is unreachable.
    """
    return _reason_in_vocabulary(exc, _SHARED_ROOT_VOCABULARY)


# === Tagged results for the completion-processing ownership protocol (#474) ===
#
# ``_materialize_processing_dir`` and ``process_completed_album`` used to
# return an anonymous ``bool | None`` / ``bool | DispatchOutcome | None``
# union where ``None`` meant "leave the row untouched" — a convention
# documented only in ~30-line comment blocks at each call site. These
# frozen dataclasses name each outcome so pyright can exhaustiveness-check
# every consumer (``match``/``isinstance`` + ``typing.assert_never``)
# instead of relying on identity comparisons against ``True``/``False``/
# ``None``. Never persisted — plain ``@dataclass``, not ``msgspec.Struct``
# (see CLAUDE.md "Wire-boundary types").


@dataclass(frozen=True)
class Materialized:
    """``_materialize_processing_dir`` succeeded: the album's tracked files
    are present at ``staged_album.current_path`` (materialized this call,
    or resumed from a prior crashed attempt). Historical bare ``True``."""


@dataclass(frozen=True)
class MaterializeFailed:
    """A local-only materialize failure (missing event stamp, a file-move
    error, a vanished staged directory/file, a failed ``mkdir``). The
    processor owner decides the terminal response; the downloader never
    retries or resets from this result. Historical bare ``False``.

    ``reason`` is a short, machine-stable diagnostic code — consumers
    must branch on the type tag, never on this string. It IS persisted
    as evidence through ``CompletionFailed.reason``, so it is derived
    structurally and never parsed out of a message: see
    :func:`source_preflight_reason` / :func:`materialize_authority_reason`.
    """

    reason: str


@dataclass(frozen=True)
class MaterializeGuarded:
    """The exact processor owner must stop without a generic lifecycle write.

    ``detail`` is diagnostic only — consumers branch on the type tag, never
    on this string. Recovery and terminal lifecycle changes belong to the
    owner-aware commands, not materialization.
    """

    detail: str


MaterializeResult = Materialized | MaterializeFailed | MaterializeGuarded


def _record_materialize_failure(
    request_id: int | None,
    reason: str,
    detail: str,
    *,
    level: int = logging.WARNING,
    exc_info: bool = False,
) -> MaterializeFailed:
    """Journal the cause, then return the tagged failure (issue #868).

    The ONE construction site for ``MaterializeFailed`` in this module.
    Before #868 several of these returns were silent, so failures left no
    recoverable cause anywhere. Every failure now names its request and
    machine-stable reason for the exact processor owner's recovery evidence.

    ``level`` preserves each site's pre-existing severity rather than
    flattening it: the previously-silent returns journal at WARNING, and
    the ones that already shouted keep shouting.
    """
    logger.log(
        level,
        "MATERIALIZE FAILED request=%s reason=%s %s",
        request_id,
        reason,
        detail,
        exc_info=exc_info,
    )
    return MaterializeFailed(reason=reason)


def _fsync_private_directory(fd: int, subject: str) -> None:
    """Flush one private directory, naming the fault if the flush fails.

    ``copy_opened_file``'s own ``fsync`` already answers
    ``CopyDestinationWriteError``; these two directory flushes are the same
    physical fault one line later and used to fall into the generic
    ``OSError`` arm as ``private_materialize_failed`` — one mount failure,
    two vocabularies (issue #868 review B2).
    """
    try:
        os.fsync(fd)
    except OSError as exc:
        raise CopyDestinationWriteError(
            f"cannot flush {subject}: {exc.strerror}",
            code="write_failed",
            errno_symbol=errno_symbol(exc),
        ) from exc


def _materialize_token(canonical_name: str) -> str:
    """Bounded stable token for locks and unpublished transaction names."""
    return hashlib.sha256(canonical_name.encode("utf-8", "surrogateescape")).hexdigest()


def _canonical_manifest_complete(
    albums_fd: int,
    canonical_name: str,
    destination_names: list[str],
) -> bool:
    """Does the no-follow canonical directory contain exactly this manifest?"""
    try:
        with open_relative_directory(albums_fd, canonical_name) as existing_fd:
            if set(os.listdir(existing_fd)) != set(destination_names):
                return False
            for name in destination_names:
                checked = open_regular_relative(existing_fd, name)
                checked.close()
        return True
    except (FileNotFoundError, FilesystemAuthorityError, OSError):
        return False
"""Return type of processor-owned materialization and staged-path recovery."""


# === slskd file locations ===
#
# The authoritative local path of every completed download comes from
# slskd's DownloadFileComplete event, stamped onto
# ``active_download_state.files[].local_path`` by
# ``lib.slskd_events.ingest_download_file_events`` at the top of each
# poll cycle (issue #146). There is no on-disk path inference: a
# completed file without a stamp is a hard failure.

def _attempt_fingerprint_for(files: list[DownloadFile]) -> str:
    """Fingerprint this attempt's exact (username, filename) file set.

    Every canonical-folder computation for the same album must derive
    from this SAME persisted file set — at materialize, at resume
    classification, and at recovery — or a mismatch classifies the
    folder as ``external`` and strands it (issue #550 phase 2).
    """
    return attempt_fingerprint([(f.username, f.filename) for f in files])


def classify_staged_album_location(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
) -> ProcessingPathLocation:
    """Classify a staged album using the persisted attempt identity."""
    return classify_processing_path(
        current_path=staged_album.current_path,
        artist=album_data.artist,
        title=album_data.title,
        year=album_data.year,
        request_id=album_data.db_request_id or 0,
        staging_dir=ctx.cfg.beets_staging_dir,
        canonical_root=processing_albums_dir(ctx.cfg.processing_dir),
        attempt_fingerprint=_attempt_fingerprint_for(album_data.files),
    )


# === Download completion processing ===
def _evaluate_staged_path_readiness(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    *,
    cancellation_token: CancellationToken | None = None,
) -> MaterializeResult:
    """Validate a noncanonical staged manifest without inferring ownership.

    Exact-owner automation always uses its canonical processing path. The only
    production noncanonical input is an already-staged operator/YouTube album,
    so this seam checks only that the persisted manifest is locally present.
    """
    request_id = album_data.db_request_id

    if not os.path.isdir(staged_album.current_path):
        return _record_materialize_failure(
            request_id,
            "staged_path_missing",
            f"current_path={staged_album.current_path}",
            level=logging.ERROR,
        )

    staged_album.bind_import_paths(album_data.files)
    missing_paths: list[str] = []
    for file in album_data.files:
        import_path = file.import_path
        assert import_path is not None
        if not os.path.isfile(import_path):
            missing_paths.append(import_path)
    if missing_paths:
        return _record_materialize_failure(
            request_id,
            "staged_path_missing_tracked_files",
            f"missing_paths={', '.join(missing_paths)}",
            level=logging.ERROR,
        )

    album_data.import_folder = staged_album.current_path
    return Materialized()


def _materialize_processing_dir(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    before_file_copy: Callable[[], None] | None = None,
    before_publish: Callable[[int, str], None] | None = None,
    cancellation_token: CancellationToken | None = None,
) -> MaterializeResult:
    """Ensure ``staged_album.current_path`` holds the album's local files.

    ``before_file_copy`` and ``before_publish`` are optional transaction
    boundary hooks.  They are deliberately called while the private
    descriptor authority and shard lock are held, so observability or a caller
    that coordinates a competing writer cannot redirect the real copy or
    ``renameat2(RENAME_NOREPLACE)`` operations.
    """
    canonical_path = canonical_folder_for_row(
        album_data, processing_albums_dir(ctx.cfg.processing_dir))
    logger.info(
        "MANIFEST-TRACE materialize request=%s %s canonical_exists=%s "
        "canonical_existing_audio=%s current_path=%s canonical=%r",
        album_data.db_request_id,
        manifest_trace_summary(album_data.files),
        os.path.isdir(canonical_path),
        len(audio_relative_paths(canonical_path)),
        staged_album.current_path,
        canonical_path,
    )
    request_id = album_data.db_request_id
    current_path_location = classify_staged_album_location(
        album_data, staged_album, ctx,
    )

    if current_path_location.kind != "canonical":
        return _evaluate_staged_path_readiness(
            album_data,
            staged_album,
            cancellation_token=cancellation_token,
        )

    # One atomic private directory publish replaces the old per-file move /
    # backup transaction.  The source tree is adversarial: names are opened
    # only below its authority fd and the opened inode is retained until the
    # destination is durable.
    destination_names = [os.path.basename(staged_album.import_path_for(file))
                         for file in album_data.files]
    if not destination_names:
        return _record_materialize_failure(
            request_id, "empty_manifest", "the attempt tracked no files",
        )
    if len(destination_names) != len(set(destination_names)):
        return _record_materialize_failure(
            request_id,
            "duplicate_final_basename",
            f"destination_names={sorted(destination_names)}",
        )

    for file in album_data.files:
        file.import_path = staged_album.import_path_for(file)

    processing_dir = ctx.cfg.processing_dir
    albums_name = "albums"
    canonical_name = os.path.basename(canonical_path)
    materialize_token = _materialize_token(canonical_name)
    opened_sources: list[OpenedRegularFile] = []
    try:
        with open_private_processing_root(
            processing_dir, ctx.cfg.slskd_download_dir,
        ) as processing_fd, open_private_child_directory(
            processing_fd, albums_name,
        ) as albums_fd, exclusive_relative_lock(
            albums_fd, f".materialize-lock-shard-{materialize_token[:2]}",
        ):
            # A bounded shard lock avoids unbounded persistent lock files
            # and works even when the canonical basename consumes all
            # NAME_MAX bytes.  Hash collisions only serialize work.
            transaction_prefix = f".materialize-tmp-{materialize_token}-"
            for entry_name in os.listdir(albums_fd):
                if entry_name.startswith(transaction_prefix):
                    _checkpoint(cancellation_token)
                    _remove_relative_tree_cancellable(
                        albums_fd,
                        entry_name,
                        cancellation_token,
                    )

            # An existing destination is valid only when it is a
            # complete exact regular-file manifest. Never add files
            # to it, including an empty directory.
            if _canonical_manifest_complete(
                albums_fd, canonical_name, destination_names,
            ):
                if not same_open_directory(processing_dir, processing_fd):
                    return MaterializeGuarded(detail="processing_root_relocated")
                staged_album.current_path = canonical_path
                album_data.import_folder = canonical_path
                return Materialized()
            try:
                os.stat(canonical_name, dir_fd=albums_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                return MaterializeGuarded(detail="incomplete_or_unsafe_canonical")

            # Preflight *all* event-stamped sources before creating a
            # destination. Missing stamps, path escapes, symlinks and
            # special files all leave every byte untouched.
            #
            # Each refusal reports its own cause. An unstamped file
            # means slskd never told us where it landed; a stamped
            # file that will not open is a different problem
            # entirely, and WHICH open failure it was decides
            # whether the operator is looking at a hostile peer
            # path or a sick mount.
            #
            # The share is opened ONCE and held across the whole
            # manifest. Re-opening it per file gave a flaky mount N
            # chances per album to refuse, and every one of those
            # would have been blamed on a single file's event stamp
            # rather than on the share; a root refusal now leaves
            # here as SharedDownloadRootError and is attributed by
            # the handler below. Holding one proven descriptor also
            # means the whole manifest is opened beneath the SAME
            # inode, so a root swapped mid-loop cannot redirect its
            # second half.
            with open_shared_download_root(
                ctx.cfg.slskd_download_dir,
            ) as slskd_root:
                for file in album_data.files:
                    if file.local_path is None:
                        return _record_materialize_failure(
                            request_id,
                            REASON_EVENT_PATH_NEVER_STAMPED,
                            f"no completion event stamped {file.filename!r}",
                            level=logging.ERROR,
                        )
                    try:
                        opened_sources.append(open_regular_under_held_root(
                            slskd_root, file.local_path,
                        ))
                    except FilesystemAuthorityError as exc:
                        return _record_materialize_failure(
                            request_id,
                            source_preflight_reason(exc),
                            f"local_path={file.local_path!r}: {exc}",
                            level=logging.ERROR,
                        )

            temp_name = f"{transaction_prefix}{secrets.token_hex(16)}"
            _checkpoint(cancellation_token)
            os.mkdir(temp_name, 0o700, dir_fd=albums_fd)
            temp_fd = os.open(
                temp_name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=albums_fd,
            )
            published = False
            try:
                for opened, name in zip(opened_sources, destination_names, strict=True):
                    _checkpoint(cancellation_token)
                    destination_fd = os.open(
                        name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                        0o600,
                        dir_fd=temp_fd,
                    )
                    try:
                        if before_file_copy is not None:
                            before_file_copy()
                        _checkpoint(cancellation_token)
                        copy_opened_file(
                            opened.fd,
                            destination_fd,
                            before_write=lambda _count: _checkpoint(
                                cancellation_token
                            ),
                        )
                    finally:
                        os.close(destination_fd)
                _checkpoint(cancellation_token)
                _fsync_private_directory(
                    temp_fd, "transaction directory")
                if before_publish is not None:
                    before_publish(albums_fd, canonical_name)
                _checkpoint(cancellation_token)
                published = rename_relative_noreplace(
                    albums_fd, temp_name, canonical_name,
                )
                if published:
                    _checkpoint(cancellation_token)
                    _fsync_private_directory(albums_fd, "albums directory")
            finally:
                os.close(temp_fd)
                if (
                    not published
                    and not (
                        cancellation_token is not None
                        and cancellation_token.cancelled
                    )
                ):
                    _checkpoint(cancellation_token)
                    _remove_relative_tree_cancellable(
                        albums_fd,
                        temp_name,
                        cancellation_token,
                    )

            if not published:
                # A writer that bypassed this process's shard lock
                # won between our preflight and publish. Never
                # overwrite it; converge only an exact manifest.
                if not _canonical_manifest_complete(
                    albums_fd, canonical_name, destination_names,
                ):
                    return MaterializeGuarded(
                        detail="incomplete_or_unsafe_canonical",
                    )
                if not same_open_directory(processing_dir, processing_fd):
                    return MaterializeGuarded(detail="processing_root_relocated")
                staged_album.current_path = canonical_path
                album_data.import_folder = canonical_path
                return Materialized()

            # Verify the lexical root still names this held private
            # inode before publishing/persisting its pathname or
            # deleting source bytes.
            if not same_open_directory(processing_dir, processing_fd):
                return MaterializeGuarded(detail="processing_root_relocated")

            # Reopen the winner for convergence proof. A success
            # cannot coexist with a stale transaction under this lock.
            with open_relative_directory(albums_fd, canonical_name) as winner_fd:
                if set(os.listdir(winner_fd)) != set(destination_names):
                    return MaterializeGuarded(detail="published_manifest_mismatch")
                for name in destination_names:
                    winner = open_regular_relative(winner_fd, name)
                    winner.close()

            # The durable private album is now visible. An
            # adversarial slskd replacement is never unlinked.
            for opened in opened_sources:
                _checkpoint(cancellation_token)
                unlink_if_same(opened)
    except CopySourceReadError as exc:
        # The share is read in FULL here, so this is where the convicted
        # nested-virtiofs ESTALE/EIO actually fires. Before #868's review
        # it landed in the generic ``OSError`` arm below as
        # ``private_materialize_failed`` — our own tree blamed for the
        # share's fault, with the errno discarded.
        return _record_materialize_failure(
            request_id,
            source_preflight_reason(exc),
            f"source read failed during copy: {exc}",
            level=logging.ERROR,
        )
    except CopyDestinationWriteError as exc:
        # The mirror image: ENOSPC/EIO writing OUR private tree.
        return _record_materialize_failure(
            request_id,
            materialize_authority_reason(exc),
            f"destination write failed during copy: {exc}",
            level=logging.ERROR,
        )
    except SharedDownloadRootError as exc:
        # ``open_private_processing_root`` opens the shared slskd share
        # too (for its physical-overlap proof), and that share is the one
        # with a live history of transient ESTALE/EIO. Its refusals MUST
        # NOT be reported in the private tree's vocabulary — the except
        # ordering makes that structural rather than inferred.
        return _record_materialize_failure(
            request_id,
            shared_download_root_reason(exc),
            str(exc),
            level=logging.ERROR,
        )
    except FilesystemAuthorityError as exc:
        # Not the source preflight — that returns directly above — and not
        # the shared share. This is our own private processing tree, its
        # shard lock, or its transaction directory refusing. The reason
        # comes from the structured code; splitting the message on its
        # first colon used to truncate the very strerror that told a
        # containment violation and a storage error apart.
        return _record_materialize_failure(
            request_id,
            materialize_authority_reason(exc),
            f"authority refusal: {exc}",
            level=logging.ERROR,
        )
    except OSError as exc:
        return _record_materialize_failure(
            request_id,
            REASON_PRIVATE_MATERIALIZE_FAILED,
            f"unclassified OSError: {exc}",
            level=logging.ERROR,
            exc_info=True,
        )
    finally:
        for opened in opened_sources:
            opened.close()

    staged_album.current_path = canonical_path
    album_data.import_folder = canonical_path
    return Materialized()
