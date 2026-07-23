"""Focused pins for the #663 private processing and descriptor boundary."""

from __future__ import annotations

import os
import tempfile
import threading
import unittest
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
)
from lib.grab_list import DownloadFile
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry


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

            def external_winner(albums_fd: int, _temp: str, destination: str) -> bool:
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
                return False

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
            with patch("lib.download_materialization.same_open_directory", return_value=False):
                result = _materialize_processing_dir(
                    album, StagedAlbum.from_entry(album, default_path=canonical), self._ctx(source, processing),
                )
            self.assertIsInstance(result, MaterializeGuarded)
            self.assertTrue(os.path.exists(source_path))
