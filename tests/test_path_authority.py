"""Focused pins for the #663 private processing and descriptor boundary."""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from lib.download_materialization import MaterializeFailed, Materialized, _materialize_processing_dir
from lib.fs_authority import FilesystemAuthorityError, open_private_processing_root, open_regular_relative
from lib.grab_list import DownloadFile
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry


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
