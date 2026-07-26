"""Focused pins for the #663 private processing and descriptor boundary."""

from __future__ import annotations

import errno
import os
import shutil
import socket
import tempfile
import threading
import unittest
import unittest.mock
from collections.abc import Callable
from unittest.mock import MagicMock

from lib.download_materialization import (
    REASON_PROCESSING_AUTHORITY_UNSAFE,
    REASON_PROCESSING_READ_FAILED_PREFIX,
    MaterializeFailed,
    MaterializeGuarded,
    Materialized,
    _materialize_token,
    _materialize_processing_dir,
    materialize_authority_reason,
)
from lib.fs_authority import (
    FilesystemAuthorityError,
    SharedDownloadRootError,
    open_configured_quarantine_directory,
    open_directory_path,
    open_private_child_directory,
    open_private_processing_root,
    open_regular_relative,
    open_regular_under_held_root,
    open_shared_download_root,
)
from lib.grab_list import DownloadFile
from lib.import_preview import (
    PreviewSnapshotLimits,
    _snapshot_authorized_directory,
    remove_preview_snapshot,
)
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry


def assert_publication_invariant(
    *,
    result: object,
    source_exists: bool,
    expected_source_exists: bool,
    destination_names: set[str],
    expected_names: set[str],
    artifact_names: list[str],
    name_max: int,
    allowed_result_types: tuple[type, ...] = (Materialized, MaterializeGuarded),
) -> None:
    """Check the materialize outcome without reimplementing its publication.

    Kept module-level so the known-bad pin proves this proof surface really
    rejects a planted overwrite/source-loss outcome.

    ``allowed_result_types`` lets a REFUSED materialize be held to the same
    artifact contract (issue #868): a preflight that declines still owes an
    ``albums/`` root with no leaked ``.materialize-tmp-*`` transaction.
    """
    if source_exists != expected_source_exists:
        raise AssertionError(
            f"source retention mismatch: {source_exists=} {expected_source_exists=}",
        )
    if destination_names != expected_names:
        raise AssertionError(
            f"destination manifest mismatch: {destination_names=} {expected_names=}",
        )
    if any(len(name.encode("utf-8", "surrogateescape")) > name_max for name in artifact_names):
        raise AssertionError("materialize artifact exceeded NAME_MAX")
    if any(name.startswith(".materialize-tmp-") for name in artifact_names):
        raise AssertionError("unpublished materialize temp was retained")
    if not isinstance(result, allowed_result_types):
        raise AssertionError(f"unexpected materialize result {result!r}")


def assert_preview_copy_invariant(
    *,
    succeeded: bool,
    preview_children: list[str],
    copied_bytes: int,
    expected_bytes: int,
    lock_path: str,
) -> None:
    """A failed private copy cleans its snapshot; a success copies exact bytes."""
    if not os.path.isfile(lock_path):
        raise AssertionError("preview copy lock is missing outside preview cleanup")
    if not succeeded and preview_children:
        raise AssertionError("failed preview copy retained private snapshot artifacts")
    if succeeded and copied_bytes != expected_bytes:
        raise AssertionError(
            f"preview copied {copied_bytes} bytes, expected {expected_bytes}",
        )


def assert_relocation_invariant(
    *,
    result: object,
    source_exists: bool,
    replacement_has_canonical: bool,
) -> None:
    """A replaced lexical processing root cannot receive a committed album."""
    if not isinstance(result, MaterializeGuarded) or result.detail != "processing_root_relocated":
        raise AssertionError(f"relocation was not guarded: {result!r}")
    if not source_exists:
        raise AssertionError("relocation guard lost the authoritative source")
    if replacement_has_canonical:
        raise AssertionError("relocation guard wrote into the replacement root")


# Stated here rather than imported: the containment grouping the tests
# assert against must not be the same object production groups by, or the
# assertion is the implementation echoing itself back (issue #868 I3).
_CONTAINMENT_CODES = frozenset({
    "path_escape", "unsafe_symlink", "not_a_directory", "not_regular_file",
})


class TestAuthorityFailureClassification(unittest.TestCase):
    """Issue #868: the refusal carries a structured code, not prose.

    Before this, callers recovered the cause by sniffing the exception's
    message (``"No such file" in str(exc)``) and by splitting it on its
    first colon — which discarded the very ``strerror`` that separated a
    containment violation from a storage error.
    """

    def test_missing_file_is_classified_missing(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with open_directory_path(root) as root_fd:
                with self.assertRaises(FilesystemAuthorityError) as caught:
                    open_regular_relative(root_fd, "absent.mp3")
        self.assertEqual(caught.exception.code, "missing")
        self.assertIsNone(caught.exception.errno_symbol)

    def test_symlink_is_classified_as_containment_not_storage(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "root")
            outside = os.path.join(parent, "outside")
            os.mkdir(root)
            with open(outside, "wb") as handle:
                handle.write(b"outside")
            os.symlink(outside, os.path.join(root, "track.mp3"))
            with open_directory_path(root) as root_fd:
                with self.assertRaises(FilesystemAuthorityError) as caught:
                    open_regular_relative(root_fd, "track.mp3")
        self.assertEqual(caught.exception.code, "unsafe_symlink")
        self.assertIn(caught.exception.code, _CONTAINMENT_CODES)
        self.assertIsNone(caught.exception.errno_symbol)

    def test_parent_escape_is_classified_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with open_directory_path(root) as root_fd:
                with self.assertRaises(FilesystemAuthorityError) as caught:
                    open_regular_relative(root_fd, "../outside")
        self.assertEqual(caught.exception.code, "path_escape")
        self.assertIn(caught.exception.code, _CONTAINMENT_CODES)

    def test_non_regular_file_is_classified_as_containment(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            os.mkfifo(os.path.join(root, "pipe"))
            with open_directory_path(root) as root_fd:
                with self.assertRaises(FilesystemAuthorityError) as caught:
                    open_regular_relative(root_fd, "pipe")
        self.assertEqual(caught.exception.code, "not_regular_file")
        self.assertIn(caught.exception.code, _CONTAINMENT_CODES)

    def test_unix_socket_is_containment_despite_failing_at_open(self) -> None:
        """A socket answers ENXIO before a descriptor exists, so the
        ``S_ISREG`` check never runs. Classifying by errno alone would
        file a containment-class shape under a storage reason (I3)."""
        with tempfile.TemporaryDirectory() as root:
            sock = socket.socket(socket.AF_UNIX)
            try:
                sock.bind(os.path.join(root, "sock"))
                with open_directory_path(root) as root_fd:
                    with self.assertRaises(FilesystemAuthorityError) as caught:
                        open_regular_relative(root_fd, "sock")
            finally:
                sock.close()
        self.assertEqual(caught.exception.code, "not_regular_file")
        self.assertIn(caught.exception.code, _CONTAINMENT_CODES)
        self.assertIsNone(caught.exception.errno_symbol)

    def test_regular_file_used_as_a_directory_is_not_called_a_symlink(self) -> None:
        """ENOTDIR gets its own code: naming it ``unsafe_symlink`` would
        be a lie the exception's own message immediately contradicts."""
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "notadir"), "wb") as handle:
                handle.write(b"regular")
            with open_directory_path(root) as root_fd:
                with self.assertRaises(FilesystemAuthorityError) as caught:
                    open_regular_relative(root_fd, "notadir/child.mp3")
        self.assertEqual(caught.exception.code, "not_a_directory")
        self.assertIn(caught.exception.code, _CONTAINMENT_CODES)

    def test_storage_open_failure_carries_its_errno_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "track.mp3")
            with open(path, "wb") as handle:
                handle.write(b"audio")
            os.chmod(path, 0o000)
            try:
                with open_directory_path(root) as root_fd:
                    with self.assertRaises(FilesystemAuthorityError) as caught:
                        open_regular_relative(root_fd, "track.mp3")
            finally:
                os.chmod(path, 0o600)
        self.assertEqual(caught.exception.code, "open_failed")
        self.assertEqual(caught.exception.errno_symbol, "EACCES")
        self.assertNotIn(caught.exception.code, _CONTAINMENT_CODES)

    def test_shared_root_refusal_is_typed_but_a_descendant_refusal_is_not(self) -> None:
        """Issue #868 D1: WHICH LEG failed decides the vocabulary.

        The preflight used to re-open the share pathname once per file, so
        a refusal of the whole share arrived indistinguishable from a
        refusal of one stamped file and was reported as
        ``event_path_gone_from_disk`` — a claim about one file's event
        stamp when the entire mount was unreachable.
        """
        with tempfile.TemporaryDirectory() as parent:
            absent_root = os.path.join(parent, "never-created")
            with self.assertRaises(SharedDownloadRootError) as root_caught:
                with open_shared_download_root(absent_root):
                    pass
            self.assertEqual(root_caught.exception.code, "missing")

            healthy_root = os.path.join(parent, "downloads")
            os.mkdir(healthy_root)
            with open_shared_download_root(healthy_root) as held_root:
                with self.assertRaises(FilesystemAuthorityError) as file_caught:
                    open_regular_under_held_root(
                        held_root, os.path.join(healthy_root, "absent.mp3"),
                    )
            # Same code, deliberately NOT the same type: a healthy share
            # missing one file is the file's problem, not the share's.
            self.assertEqual(file_caught.exception.code, "missing")
            self.assertNotIsInstance(file_caught.exception, SharedDownloadRootError)

    def test_unreadable_shared_root_keeps_its_errno_through_re_attribution(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "downloads")
            os.mkdir(root)
            os.chmod(root, 0o000)
            try:
                with self.assertRaises(SharedDownloadRootError) as caught:
                    with open_shared_download_root(root):
                        pass
            finally:
                os.chmod(root, 0o700)
        self.assertEqual(caught.exception.code, "open_failed")
        self.assertEqual(caught.exception.errno_symbol, "EACCES")

    def test_held_root_opens_every_manifest_file_under_one_descriptor(self) -> None:
        """Must-still-work guard for D1: holding the share open once must
        not break an ordinary multi-file manifest."""
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "downloads")
            os.makedirs(os.path.join(root, "peer"))
            names = ["01.mp3", "02.mp3", "03.mp3"]
            for index, name in enumerate(names):
                with open(os.path.join(root, "peer", name), "wb") as handle:
                    handle.write(bytes([index]) * 4)
            with open_shared_download_root(root) as held_root:
                for index, name in enumerate(names):
                    opened = open_regular_under_held_root(
                        held_root, os.path.join(root, "peer", name),
                    )
                    try:
                        self.assertEqual(os.read(opened.fd, 8), bytes([index]) * 4)
                    finally:
                        opened.close()

    def test_unclassified_raise_sites_keep_the_default_code(self) -> None:
        """Every pre-#868 raise site stays ``unspecified`` — deliberately
        not a synonym for safe: consumers fail closed on it."""
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "source")
            os.mkdir(source)
            with self.assertRaises(FilesystemAuthorityError) as caught:
                with open_private_processing_root(source, source):
                    pass
        self.assertEqual(caught.exception.code, "unspecified")


class TestPrivateProcessingAuthority(unittest.TestCase):
    def test_rejects_overlap_and_symlinked_root(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "source")
            processing = os.path.join(parent, "processing")
            os.mkdir(source)
            os.mkdir(processing, 0o700)
            with self.assertRaisesRegex(FilesystemAuthorityError, "overlaps"):
                with open_private_processing_root(source, source):
                    pass
            os.chmod(processing, 0o750)
            with self.assertRaisesRegex(FilesystemAuthorityError, "mode 0700"):
                with open_private_processing_root(processing, source):
                    pass
            os.chmod(processing, 0o700)
            link = os.path.join(parent, "processing-link")
            os.symlink(processing, link)
            with self.assertRaises(FilesystemAuthorityError):
                with open_private_processing_root(link, source):
                    pass

    def test_rejects_group_writable_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "source")
            os.mkdir(source)
            unsafe_ancestor = os.path.join(parent, "unsafe")
            os.mkdir(unsafe_ancestor, 0o770)
            os.chmod(unsafe_ancestor, 0o770)
            processing = os.path.join(unsafe_ancestor, "processing")
            os.mkdir(processing, 0o700)
            with self.assertRaisesRegex(
                FilesystemAuthorityError, "ancestor",
            ) as caught:
                with open_private_processing_root(processing, source):
                    pass
            # Issue #868 review A8: an ownership/permission downgrade of the
            # tree the whole boundary rests on is a containment finding, not
            # the "renameat2 is unsupported" miscellany ``unspecified``
            # collects — it used to fuse ~13 causes into one reason.
            self.assertEqual(caught.exception.code, "untrusted_ownership")
            self.assertEqual(
                materialize_authority_reason(caught.exception),
                REASON_PROCESSING_AUTHORITY_UNSAFE,
            )

    def test_inspecting_an_open_ancestor_is_not_an_open_failure(self) -> None:
        """Review F4: the directory is already open; the stat failed.

        ``open_failed`` rendered "our processing storage could not be
        opened (ESTALE)" — the verb-borrow B2 was chartered to remove.
        """
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "source")
            os.mkdir(source)
            processing = os.path.join(parent, "processing")
            os.mkdir(processing, 0o700)
            real_fstat = os.fstat

            def failing_fstat(fd: int) -> object:
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE))

            with unittest.mock.patch("os.fstat", side_effect=failing_fstat):
                with self.assertRaises(FilesystemAuthorityError) as caught:
                    with open_private_processing_root(processing, source):
                        pass
            self.assertIs(os.fstat, real_fstat)
        self.assertEqual(caught.exception.code, "read_failed")
        self.assertEqual(caught.exception.errno_symbol, "ESTALE")
        self.assertEqual(
            materialize_authority_reason(caught.exception),
            f"{REASON_PROCESSING_READ_FAILED_PREFIX}ESTALE",
        )

    def test_ownership_downgrades_all_carry_the_containment_code(self) -> None:
        """Every private-tree ownership assertion, not just the ancestor."""
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "source")
            os.mkdir(source)
            processing = os.path.join(parent, "processing")
            os.mkdir(processing, 0o750)
            with self.assertRaises(FilesystemAuthorityError) as mode_caught:
                with open_private_processing_root(processing, source):
                    pass
            self.assertEqual(mode_caught.exception.code, "untrusted_ownership")

            os.chmod(processing, 0o700)
            os.mkdir(os.path.join(processing, "albums"), 0o750)
            with open_private_processing_root(processing, source) as root_fd:
                with self.assertRaises(FilesystemAuthorityError) as child:
                    with open_private_child_directory(root_fd, "albums"):
                        pass
            self.assertEqual(child.exception.code, "untrusted_ownership")
            self.assertEqual(
                materialize_authority_reason(child.exception),
                REASON_PROCESSING_AUTHORITY_UNSAFE,
            )

    def test_no_follow_file_open_rejects_symlink_and_parent_escape(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "root")
            outside = os.path.join(parent, "outside")
            os.mkdir(root)
            with open(outside, "wb") as handle:
                handle.write(b"outside")
            os.symlink(outside, os.path.join(root, "track.mp3"))
            from lib.fs_authority import open_directory_path
            with open_directory_path(root) as root_fd:
                with self.assertRaises(FilesystemAuthorityError):
                    open_regular_relative(root_fd, "track.mp3")
                with self.assertRaises(FilesystemAuthorityError):
                    open_regular_relative(root_fd, "../outside")

    def test_quarantine_resolver_requires_exact_component_and_holds_nested_incoming(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            slskd = os.path.join(parent, "slskd")
            incoming = os.path.join(parent, "Incoming")
            processing = os.path.join(parent, "processing")
            for directory in (slskd, incoming, processing):
                os.mkdir(directory, 0o700)
            os.mkdir(os.path.join(processing, "albums"), 0o700)
            os.mkdir(os.path.join(processing, "preview"), 0o700)
            album = os.path.join(incoming, "auto-import", "Artist", "failed_imports", "Album")
            os.makedirs(album)
            cfg = MagicMock()
            cfg.slskd_download_dir = slskd
            cfg.beets_staging_dir = incoming
            cfg.processing_dir = processing
            with open_configured_quarantine_directory(album, cfg) as opened:
                self.assertEqual(os.fstat(opened.fd).st_ino, os.stat(album).st_ino)
            lookalike = os.path.join(incoming, "failed_imports-old", "Album")
            os.makedirs(lookalike)
            with self.assertRaises(FilesystemAuthorityError):
                with open_configured_quarantine_directory(lookalike, cfg):
                    pass


class TestPrivatePreviewCopyBounds(unittest.TestCase):
    def _world(self) -> tuple[tempfile.TemporaryDirectory[str], str, str, MagicMock]:
        parent = tempfile.TemporaryDirectory()
        source = os.path.join(parent.name, "source")
        processing = os.path.join(parent.name, "processing")
        os.mkdir(source)
        os.mkdir(processing, 0o700)
        os.mkdir(os.path.join(processing, "albums"), 0o700)
        os.mkdir(os.path.join(processing, "preview"), 0o700)
        cfg = MagicMock()
        cfg.slskd_download_dir = source
        cfg.processing_dir = processing
        return parent, source, processing, cfg

    def test_reserved_free_space_rejects_and_cleans_private_snapshot(self) -> None:
        parent, source, processing, cfg = self._world()
        with parent:
            with open(os.path.join(source, "track.mp3"), "wb") as handle:
                handle.write(b"audio")
            preview = os.path.join(processing, "preview")
            lock = os.path.join(processing, ".preview-snapshot.lock")
            with self.assertRaisesRegex(FilesystemAuthorityError, "insufficient private preview space"):
                _snapshot_authorized_directory(
                    source,
                    cfg,
                    limits=PreviewSnapshotLimits(free_reserve_bytes=3),
                    available_bytes_fn=lambda _preview_fd: 2,
                )
            assert_preview_copy_invariant(
                succeeded=False,
                preview_children=os.listdir(preview),
                copied_bytes=0,
                expected_bytes=0,
                lock_path=lock,
            )

    def test_source_growth_hits_actual_copy_cap_and_cleans_snapshot(self) -> None:
        parent, source, processing, cfg = self._world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"tiny")
            preview = os.path.join(processing, "preview")
            lock = os.path.join(processing, ".preview-snapshot.lock")
            from lib.fs_authority import copy_opened_file as real_copy_opened_file

            def grow_before_real_copy(
                source_fd: int,
                destination_fd: int,
                *,
                max_bytes: int | None = None,
                before_write: Callable[[int], None] | None = None,
            ) -> int:
                with open(source_path, "ab") as handle:
                    handle.write(b"growth")
                return real_copy_opened_file(
                    source_fd,
                    destination_fd,
                    max_bytes=max_bytes,
                    before_write=before_write,
                )

            with self.assertRaisesRegex(FilesystemAuthorityError, "source grew beyond copy limit"):
                _snapshot_authorized_directory(
                    source,
                    cfg,
                    copy_fn=grow_before_real_copy,
                )
            assert_preview_copy_invariant(
                succeeded=False,
                preview_children=os.listdir(preview),
                copied_bytes=0,
                expected_bytes=0,
                lock_path=lock,
            )

    def test_preview_lock_is_stable_outside_snapshot_cleanup(self) -> None:
        parent, source, processing, cfg = self._world()
        with parent:
            with open(os.path.join(source, "track.mp3"), "wb") as handle:
                handle.write(b"audio")
            lock = os.path.join(processing, ".preview-snapshot.lock")
            snapshot = _snapshot_authorized_directory(source, cfg)
            lock_inode = os.stat(lock).st_ino
            try:
                copied = os.path.join(snapshot, "track.mp3")
                assert_preview_copy_invariant(
                    succeeded=True,
                    preview_children=os.listdir(os.path.join(processing, "preview")),
                    copied_bytes=os.path.getsize(copied),
                    expected_bytes=5,
                    lock_path=lock,
                )
            finally:
                remove_preview_snapshot(snapshot, cfg)
            self.assertTrue(os.path.isfile(lock))
            self.assertEqual(os.stat(lock).st_ino, lock_inode)
            self.assertEqual(os.listdir(os.path.join(processing, "preview")), [])


class TestAtomicPrivateMaterialization(unittest.TestCase):
    def _world(self):
        parent = tempfile.TemporaryDirectory()
        source = os.path.join(parent.name, "source")
        processing = os.path.join(parent.name, "processing")
        os.mkdir(source)
        os.mkdir(processing, 0o700)
        os.mkdir(os.path.join(processing, "albums"), 0o700)
        os.mkdir(os.path.join(processing, "preview"), 0o700)
        return parent, source, processing

    def _ctx(self, source: str, processing: str):
        cfg = MagicMock()
        cfg.slskd_download_dir = source
        cfg.processing_dir = processing
        cfg.beets_staging_dir = os.path.join(processing, "staging")
        return make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg)

    def test_complete_publish_precedes_source_unlink(self) -> None:
        parent, source, processing = self._world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(
                filename="peer\\track.mp3", username="peer", id="1",
                file_dir="peer", size=5,
            )
            file.local_path = source_path
            album = make_grab_list_entry(files=[file], artist="A", title="B", year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            staged = StagedAlbum.from_entry(album, default_path=canonical)
            result = _materialize_processing_dir(album, staged, self._ctx(source, processing))
            self.assertIsInstance(result, Materialized)
            self.assertFalse(os.path.exists(source_path))
            with open(os.path.join(canonical, "track.mp3"), "rb") as handle:
                self.assertEqual(handle.read(), b"audio")

    def test_empty_and_duplicate_manifests_do_not_mutate_source(self) -> None:
        parent, source, processing = self._world()
        with parent:
            empty = make_grab_list_entry(files=[], artist="A", title="B", year="2020")
            empty_staged = StagedAlbum.from_entry(
                empty, default_path=canonical_folder_for_row(empty, processing_albums_dir(processing)),
            )
            empty_result = _materialize_processing_dir(
                empty, empty_staged, self._ctx(source, processing),
            )
            self.assertIsInstance(empty_result, MaterializeFailed)
            assert isinstance(empty_result, MaterializeFailed)
            self.assertEqual(empty_result.reason, "empty_manifest")
            first = os.path.join(source, "first.mp3")
            second = os.path.join(source, "second.mp3")
            for path in (first, second):
                with open(path, "wb") as handle:
                    handle.write(b"audio")
            files = []
            for index, path in enumerate((first, second)):
                file = DownloadFile(
                    filename=f"peer{index}\\same.mp3", username=f"peer{index}",
                    id=str(index), file_dir=f"peer{index}", size=5,
                )
                file.local_path = path
                files.append(file)
            album = make_grab_list_entry(files=files, artist="A", title="B", year="2020")
            staged = StagedAlbum.from_entry(
                album, default_path=canonical_folder_for_row(album, processing_albums_dir(processing)),
            )
            result = _materialize_processing_dir(album, staged, self._ctx(source, processing))
            self.assertIsInstance(result, MaterializeFailed)
            assert isinstance(result, MaterializeFailed)
            self.assertEqual(result.reason, "duplicate_final_basename")
            self.assertTrue(os.path.exists(first))
            self.assertTrue(os.path.exists(second))

    def _stamped_album(self, source: str, processing: str):
        source_path = os.path.join(source, "track.mp3")
        with open(source_path, "wb") as handle:
            handle.write(b"audio")
        file = DownloadFile(
            filename="peer\\track.mp3", username="peer", id="1",
            file_dir="peer", size=5,
        )
        file.local_path = source_path
        album = make_grab_list_entry(
            files=[file], artist="A", title="B", year="2020")
        staged = StagedAlbum.from_entry(
            album,
            default_path=canonical_folder_for_row(
                album, processing_albums_dir(processing)),
        )
        return album, staged, source_path

    def test_private_tree_absence_reports_a_processing_reason(self) -> None:
        """Issue #868: a refusal on OUR OWN tree gets its own vocabulary.

        The retired derivation split the message on its first colon, so
        this failure was persisted as the prose fragment ``cannot open
        albums`` — which is neither machine-stable nor a cause.
        """
        parent, source, processing = self._world()
        with parent:
            album, staged, source_path = self._stamped_album(source, processing)
            os.rmdir(os.path.join(processing, "albums"))
            result = _materialize_processing_dir(
                album, staged, self._ctx(source, processing))
            self.assertIsInstance(result, MaterializeFailed)
            assert isinstance(result, MaterializeFailed)
            self.assertEqual(result.reason, "processing_path_missing")
            self.assertNotIn(":", result.reason)
            self.assertTrue(os.path.exists(source_path))

    def test_absent_shared_download_root_is_not_blamed_on_the_private_tree(self) -> None:
        """Issue #868 B1: ``open_private_processing_root`` opens the shared
        slskd share too, for its physical-overlap proof. A refusal THERE
        must not be persisted in the private tree's vocabulary — otherwise
        "our albums/ dir is gone" and "the whole share is gone" both read
        ``processing_path_missing``, the exact collapse #868 removes.
        """
        parent, source, processing = self._world()
        with parent:
            album, staged, source_path = self._stamped_album(source, processing)
            shutil.rmtree(source)
            result = _materialize_processing_dir(
                album, staged, self._ctx(source, processing))
            self.assertIsInstance(result, MaterializeFailed)
            assert isinstance(result, MaterializeFailed)
            self.assertEqual(result.reason, "slskd_root_missing")
            self.assertNotEqual(result.reason, "processing_path_missing")
            del source_path

    def test_unreadable_shared_download_root_names_the_share_not_our_tree(self) -> None:
        """Issue #868 B1: the live failure is transient ESTALE/EIO on the
        nested-virtiofs share. Reporting it as ``processing_open_failed_*``
        would send the operator to inspect the wrong filesystem. EACCES
        reproduces the shape without a sick mount."""
        parent, source, processing = self._world()
        with parent:
            album, staged, _ = self._stamped_album(source, processing)
            os.chmod(source, 0o000)
            try:
                result = _materialize_processing_dir(
                    album, staged, self._ctx(source, processing))
            finally:
                os.chmod(source, 0o700)
            self.assertIsInstance(result, MaterializeFailed)
            assert isinstance(result, MaterializeFailed)
            self.assertEqual(result.reason, "slskd_root_open_failed_EACCES")
            # The private tree's identical-errno reason is a DIFFERENT
            # string: same errno, different subsystem, different remedy.
            self.assertNotEqual(result.reason, "processing_open_failed_EACCES")

    def test_private_tree_storage_failure_carries_its_errno(self) -> None:
        """Issue #868 I3: our own tree gets the same containment/storage
        separation the slskd source does."""
        parent, source, processing = self._world()
        with parent:
            album, staged, source_path = self._stamped_album(source, processing)
            albums = os.path.join(processing, "albums")
            os.chmod(albums, 0o000)
            try:
                result = _materialize_processing_dir(
                    album, staged, self._ctx(source, processing))
            finally:
                os.chmod(albums, 0o700)
            self.assertIsInstance(result, MaterializeFailed)
            assert isinstance(result, MaterializeFailed)
            self.assertEqual(result.reason, "processing_open_failed_EACCES")
            self.assertTrue(os.path.exists(source_path))

    def test_existing_empty_destination_is_guarded_without_overwrite(self) -> None:
        parent, source, processing = self._world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(filename="peer\\track.mp3", username="peer", id="1", file_dir="peer", size=5)
            file.local_path = source_path
            album = make_grab_list_entry(files=[file], artist="A", title="B", year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            os.mkdir(canonical)
            result = _materialize_processing_dir(
                album, StagedAlbum.from_entry(album, default_path=canonical), self._ctx(source, processing),
            )
            self.assertIsInstance(result, MaterializeGuarded)
            self.assertTrue(os.path.exists(source_path))
            self.assertEqual(os.listdir(canonical), [])

    def test_stale_temp_is_recovered_under_attempt_lock(self) -> None:
        parent, source, processing = self._world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(filename="peer\\track.mp3", username="peer", id="1", file_dir="peer", size=5)
            file.local_path = source_path
            album = make_grab_list_entry(files=[file], artist="A", title="B", year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            token = _materialize_token(os.path.basename(canonical))
            stale = os.path.join(processing, "albums", f".materialize-tmp-{token}-dead")
            os.mkdir(stale)
            with open(os.path.join(stale, "partial.mp3"), "wb") as handle:
                handle.write(b"partial")
            result = _materialize_processing_dir(
                album, StagedAlbum.from_entry(album, default_path=canonical), self._ctx(source, processing),
            )
            self.assertIsInstance(result, Materialized)
            self.assertFalse(os.path.exists(stale))

    def test_maximum_length_canonical_name_materializes(self) -> None:
        """The digest transaction names stay under NAME_MAX at 255 bytes."""
        parent, source, processing = self._world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(
                filename="peer\\track.mp3", username="peer", id="1",
                file_dir="peer", size=5,
            )
            file.local_path = source_path
            album = make_grab_list_entry(
                files=[file], artist="A" * 400, title="B" * 400, year="2020",
            )
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            self.assertEqual(len(os.path.basename(canonical).encode()), 255)
            result = _materialize_processing_dir(
                album, StagedAlbum.from_entry(album, default_path=canonical),
                self._ctx(source, processing),
            )
            self.assertIsInstance(result, Materialized)
            self.assertFalse(os.path.exists(source_path))

    def test_publish_eexist_race_never_overwrites_or_unlinks_source(self) -> None:
        """A non-cooperating publisher wins safely; its partial tree wins no trust."""
        parent, source, processing = self._world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(
                filename="peer\\track.mp3", username="peer", id="1",
                file_dir="peer", size=5,
            )
            file.local_path = source_path
            album = make_grab_list_entry(files=[file], artist="A", title="B", year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))

            def external_winner(albums_fd: int, destination: str) -> None:
                """Publish a competing final immediately before real renameat2."""
                os.mkdir(destination, 0o700, dir_fd=albums_fd)
                winner_fd = os.open(
                    destination,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
                    dir_fd=albums_fd,
                )
                try:
                    fd = os.open(
                        "foreign.mp3",
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                        0o600,
                        dir_fd=winner_fd,
                    )
                    os.close(fd)
                finally:
                    os.close(winner_fd)

            result = _materialize_processing_dir(
                album,
                StagedAlbum.from_entry(album, default_path=canonical),
                self._ctx(source, processing),
                before_publish=external_winner,
            )

            self.assertIsInstance(result, MaterializeGuarded)
            self.assertTrue(os.path.exists(source_path))
            self.assertEqual(os.listdir(canonical), ["foreign.mp3"])
            assert_publication_invariant(
                result=result,
                source_exists=os.path.exists(source_path),
                expected_source_exists=True,
                destination_names=set(os.listdir(canonical)),
                expected_names={"foreign.mp3"},
                artifact_names=os.listdir(os.path.join(processing, "albums")),
                name_max=os.pathconf(os.path.join(processing, "albums"), "PC_NAME_MAX"),
            )

    def test_shard_collision_serializes_materialization(self) -> None:
        """A two-hex hash collision shares exactly one bounded shard lock."""
        parent, source, processing = self._world()
        with parent:
            by_shard: dict[str, tuple[str, str]] = {}
            titles: tuple[str, str] | None = None
            for index in range(2048):
                title = f"Album {index}"
                candidate = canonical_folder_for_row(
                    make_grab_list_entry(
                        files=[DownloadFile(
                            filename="peer\\track.mp3", username="peer", id="1",
                            file_dir="peer", size=5,
                        )],
                        artist="Artist", title=title, year="2020",
                    ),
                    processing_albums_dir(processing),
                )
                name = os.path.basename(candidate)
                shard = _materialize_token(name)[:2]
                previous = by_shard.get(shard)
                if previous is not None and previous[0] != title:
                    titles = (previous[0], title)
                    break
                by_shard[shard] = (title, name)
            else:  # pragma: no cover - 2048 draws make this astronomically unlikely
                self.fail("could not construct a materialize shard collision")
            assert titles is not None

            entries = []
            for index, title in enumerate(titles):
                source_path = os.path.join(source, f"track-{index}.mp3")
                with open(source_path, "wb") as handle:
                    handle.write(b"audio")
                file = DownloadFile(
                    filename="peer\\track.mp3", username="peer", id="1",
                    file_dir="peer", size=5,
                )
                file.local_path = source_path
                album = make_grab_list_entry(files=[file], artist="Artist", title=title, year="2020")
                canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
                entries.append((album, StagedAlbum.from_entry(album, default_path=canonical)))

            entered = threading.Event()
            release = threading.Event()
            calls: list[int] = []
            def blocking_before_file_copy() -> None:
                calls.append(1)
                if len(calls) == 1:
                    entered.set()
                    self.assertTrue(release.wait(timeout=2))

            results: list[object] = []
            def run(entry) -> None:
                results.append(_materialize_processing_dir(
                    entry[0], entry[1], self._ctx(source, processing),
                    before_file_copy=blocking_before_file_copy,
                ))

            first = threading.Thread(target=run, args=(entries[0],))
            second = threading.Thread(target=run, args=(entries[1],))
            first.start()
            self.assertTrue(entered.wait(timeout=2))
            second.start()
            self.assertFalse(entered.wait(timeout=0.05) and len(calls) > 1)
            release.set()
            first.join(timeout=2)
            second.join(timeout=2)

            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(isinstance(result, Materialized) for result in results))

    def test_root_relocation_guard_retains_authoritative_source(self) -> None:
        parent, source, processing = self._world()
        with parent:
            source_path = os.path.join(source, "track.mp3")
            with open(source_path, "wb") as handle:
                handle.write(b"audio")
            file = DownloadFile(filename="peer\\track.mp3", username="peer", id="1", file_dir="peer", size=5)
            file.local_path = source_path
            album = make_grab_list_entry(files=[file], artist="A", title="B", year="2020")
            canonical = canonical_folder_for_row(album, processing_albums_dir(processing))
            relocated = f"{processing}-relocated"

            def relocate_before_publish(_albums_fd: int, _destination: str) -> None:
                # The authoritative descriptors still address the renamed old
                # root.  A fresh lexical root must receive neither a commit
                # nor persistence after the real no-replace publication.
                os.rename(processing, relocated)
                os.mkdir(processing, 0o700)
                os.mkdir(os.path.join(processing, "albums"), 0o700)
                os.mkdir(os.path.join(processing, "preview"), 0o700)

            result = _materialize_processing_dir(
                album,
                StagedAlbum.from_entry(album, default_path=canonical),
                self._ctx(source, processing),
                before_publish=relocate_before_publish,
            )
            assert_relocation_invariant(
                result=result,
                source_exists=os.path.exists(source_path),
                replacement_has_canonical=os.path.exists(canonical),
            )


class TestAuthorityInvariantCheckers(unittest.TestCase):
    """Known-bad self-tests: the proof checkers must reject planted lies."""

    def test_publication_checker_trips_on_overwrite_source_loss(self) -> None:
        with self.assertRaises(AssertionError):
            assert_publication_invariant(
                result=Materialized(),
                source_exists=False,
                expected_source_exists=True,
                destination_names={"foreign.mp3"},
                expected_names={"track.mp3"},
                artifact_names=[".materialize-tmp-orphan"],
                name_max=255,
            )

    def test_publication_checker_trips_on_temp_leaked_by_a_refused_materialize(
        self,
    ) -> None:
        """Issue #868: a REFUSED materialize is held to the same artifact
        contract as a successful one — a leaked transaction directory is a
        finding whichever tag came back."""
        with self.assertRaises(AssertionError):
            assert_publication_invariant(
                result=MaterializeFailed(reason="slskd_root_missing"),
                source_exists=True,
                expected_source_exists=True,
                destination_names=set(),
                expected_names=set(),
                artifact_names=[".materialize-tmp-leaked"],
                name_max=255,
                allowed_result_types=(MaterializeFailed,),
            )

    def test_publication_checker_default_refuses_a_failed_materialize(self) -> None:
        """Issue #882 item 6: the DEFAULT tuple is itself a proof surface.

        Callers that omit ``allowed_result_types`` rely on the default, and
        every other known-bad pin here passes an explicit tuple — so a mutant
        widening the default to admit ``MaterializeFailed`` survived the whole
        suite. It must not: a refusal reaching a call site that asserts
        publication is a finding, not an accepted outcome.
        """
        with self.assertRaises(AssertionError):
            assert_publication_invariant(
                result=MaterializeFailed(reason="slskd_root_missing"),
                source_exists=True,
                expected_source_exists=True,
                destination_names=set(),
                expected_names=set(),
                artifact_names=[],
                name_max=255,
            )

    def test_publication_checker_still_refuses_an_unexpected_result_type(self) -> None:
        with self.assertRaises(AssertionError):
            assert_publication_invariant(
                result=Materialized(),
                source_exists=True,
                expected_source_exists=True,
                destination_names=set(),
                expected_names=set(),
                artifact_names=[],
                name_max=255,
                allowed_result_types=(MaterializeFailed,),
            )

    def test_preview_checker_trips_on_failed_snapshot_residue(self) -> None:
        with self.assertRaises(AssertionError):
            assert_preview_copy_invariant(
                succeeded=False,
                preview_children=["preview-leaked"],
                copied_bytes=0,
                expected_bytes=0,
                lock_path=__file__,
            )

    def test_relocation_checker_trips_on_replacement_write(self) -> None:
        with self.assertRaises(AssertionError):
            assert_relocation_invariant(
                result=MaterializeGuarded(detail="processing_root_relocated"),
                source_exists=True,
                replacement_has_canonical=True,
            )
