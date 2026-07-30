"""Tests for ``lib/staged_album.py``."""
import os
import tempfile
import unittest
from typing import ClassVar

from tests.helpers import make_download_file


class TestStageToAiPath(unittest.TestCase):

    def test_sanitizes_artist_and_title(self):
        from lib.processing_paths import stage_to_ai_path

        dest = stage_to_ai_path(
            artist='Test: "Artist"',
            title="Album/Title?",
            staging_dir="/tmp/staging",
        )

        self.assertEqual(dest, "/tmp/staging/Test Artist/AlbumTitle")

    def test_scopes_auto_import_paths_by_request_id(self):
        from lib.processing_paths import stage_to_ai_path

        dest = stage_to_ai_path(
            artist="Test Artist",
            title="Album",
            staging_dir="/tmp/staging",
            request_id=42,
            auto_import=True,
        )

        self.assertEqual(
            dest,
            "/tmp/staging/auto-import/Test Artist/Album [request-42]",
        )


class TestStagedFilename(unittest.TestCase):

    CASES: ClassVar = [
        ("backslashes only", "user1\\Album\\01 - Track.flac", "01 - Track.flac"),
        ("forward slashes only", "user1/Album/01 - Track.flac", "01 - Track.flac"),
        ("mixed separators", "user1\\Album/Disc 1\\01 - Track.flac", "01 - Track.flac"),
    ]

    def test_extracts_leaf_filename_across_separator_variants(self):
        from lib.staged_album import staged_filename

        for desc, remote_path, expected in self.CASES:
            with self.subTest(desc=desc):
                file = make_download_file(filename=remote_path)
                self.assertEqual(staged_filename(file), expected)


class TestStagedAlbum(unittest.TestCase):

    def test_move_to_moves_contents_without_lifecycle_persistence(self):
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source")
            os.makedirs(source)
            with open(os.path.join(source, "track.mp3"), "w") as fp:
                fp.write("audio")

            dest = os.path.join(tmpdir, "staging", "Artist", "Album")
            staged_album = StagedAlbum(current_path=source, request_id=42)

            result = staged_album.move_to(dest)

            self.assertEqual(result, dest)
            self.assertEqual(staged_album.current_path, dest)
            self.assertTrue(os.path.exists(os.path.join(dest, "track.mp3")))
            self.assertFalse(os.path.exists(source))

    def test_move_to_idempotent_when_source_equals_target(self):
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "staging")
            os.makedirs(target)
            with open(os.path.join(target, "track.mp3"), "w") as fp:
                fp.write("audio")

            staged_album = StagedAlbum(current_path=target, request_id=42)

            result = staged_album.move_to(target)

            self.assertEqual(result, target)
            self.assertTrue(os.path.exists(os.path.join(target, "track.mp3")))

    def test_move_to_cleans_empty_target_on_early_failure(self):
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "missing")
            dest = os.path.join(tmpdir, "staging", "Artist", "Album")

            with self.assertRaises(FileNotFoundError):
                StagedAlbum(current_path=source).move_to(dest)

            self.assertFalse(os.path.exists(dest))

    def test_bind_import_paths_updates_multi_disc_names(self):
        from lib.staged_album import StagedAlbum

        file = make_download_file(
            filename="user1\\CD2\\01 - Track.flac",
            file_dir="user1\\CD2",
        )
        file.disk_no = 2
        file.disk_count = 3
        staged_album = StagedAlbum(current_path="/tmp/staged")

        staged_album.bind_import_paths([file])

        self.assertEqual(
            file.import_path,
            "/tmp/staged/Disk 2 - 01 - Track.flac",
        )
