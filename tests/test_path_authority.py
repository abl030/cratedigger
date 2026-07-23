"""Focused pins for the #663 private processing and descriptor boundary."""

from __future__ import annotations

import os
import tempfile
import threading
import unittest
from collections.abc import Callable
from unittest.mock import MagicMock, patch

from lib.download_materialization import (
    MaterializeFailed,
    MaterializeGuarded,
    Materialized,
    _materialize_token,
    _materialize_processing_dir,
)
from lib.fs_authority import (
    FilesystemAuthorityError,
    open_configured_quarantine_directory,
    open_private_processing_root,
    open_regular_relative,
    rename_relative_noreplace,
)
from lib.grab_list import DownloadFile
from lib.import_preview import _snapshot_authorized_directory, remove_preview_snapshot
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
) -> None:
    """Check the materialize outcome without reimplementing its publication.

    Kept module-level so the known-bad pin proves this proof surface really
    rejects a planted overwrite/source-loss outcome.
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
    if not isinstance(result, (Materialized, MaterializeGuarded)):
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


class TestPrivateProcessingAuthority(unittest.TestCase):
    def test_rejects_overlap_and_symlinked_root(self) -> None:
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as parent:
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
            processing = os.path.join(parent, "processing")
            os.mkdir(source)
            os.mkdir(processing, 0o700)
            with self.assertRaisesRegex(FilesystemAuthorityError, "ancestor"):
                with open_private_processing_root(processing, source):
                    pass

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
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as parent:
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
        parent = tempfile.TemporaryDirectory(dir=os.getcwd())
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
            fstatvfs = MagicMock(f_bavail=2, f_frsize=1)
            with patch("lib.import_preview._PREVIEW_FREE_RESERVE_BYTES", 3), patch(
                "lib.import_preview.os.fstatvfs", return_value=fstatvfs,
            ):
                with self.assertRaisesRegex(FilesystemAuthorityError, "insufficient private preview space"):
                    _snapshot_authorized_directory(source, cfg)
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

            with patch(
                "lib.import_preview.copy_opened_file",
                side_effect=grow_before_real_copy,
            ):
                with self.assertRaisesRegex(FilesystemAuthorityError, "source grew beyond copy limit"):
                    _snapshot_authorized_directory(source, cfg)
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
        parent = tempfile.TemporaryDirectory(dir=os.getcwd())
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

            def external_winner(albums_fd: int, temp: str, destination: str) -> bool:
                """Timing hook only: publish the foreign winner, then invoke
                the real Linux renameat2(RENAME_NOREPLACE) helper."""
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
                return rename_relative_noreplace(albums_fd, temp, destination)

            with patch(
                "lib.download_materialization.rename_relative_noreplace",
                side_effect=external_winner,
            ):
                result = _materialize_processing_dir(
                    album, StagedAlbum.from_entry(album, default_path=canonical),
                    self._ctx(source, processing),
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
            original_copy = __import__(
                "lib.download_materialization", fromlist=["copy_opened_file"],
            ).copy_opened_file

            def blocking_copy(*args, **kwargs):
                calls.append(1)
                if len(calls) == 1:
                    entered.set()
                    self.assertTrue(release.wait(timeout=2))
                return original_copy(*args, **kwargs)

            results: list[object] = []
            def run(entry) -> None:
                results.append(_materialize_processing_dir(
                    entry[0], entry[1], self._ctx(source, processing),
                ))

            with patch("lib.download_materialization.copy_opened_file", side_effect=blocking_copy):
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
            original_rename = rename_relative_noreplace
            relocated = f"{processing}-relocated"

            def relocate_before_real_publish(
                albums_fd: int,
                temp: str,
                destination: str,
            ) -> bool:
                # The authoritative descriptors still address the renamed old
                # root.  A fresh lexical root must receive neither a commit
                # nor persistence after the real no-replace publication.
                os.rename(processing, relocated)
                os.mkdir(processing, 0o700)
                os.mkdir(os.path.join(processing, "albums"), 0o700)
                os.mkdir(os.path.join(processing, "preview"), 0o700)
                return original_rename(albums_fd, temp, destination)

            with patch(
                "lib.download_materialization.rename_relative_noreplace",
                side_effect=relocate_before_real_publish,
            ):
                result = _materialize_processing_dir(
                    album, StagedAlbum.from_entry(album, default_path=canonical), self._ctx(source, processing),
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
