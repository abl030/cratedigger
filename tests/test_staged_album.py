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


class TestStagedFilenameByteLimit(unittest.TestCase):
    """``staged_filename`` must fit the filesystem's 255-byte name cap.

    A remote peer names its own files. Request 8867's eight tracks were
    372-555 UTF-8 bytes, so copying them into the canonical processing album
    raised ``OSError: [Errno 36] File name too long`` and the album could
    never import even once the transfer itself succeeded.
    """

    # 128 CJK characters: 128 characters but 384 UTF-8 bytes, so it clears a
    # character-based check and still breaks a byte-based one.
    LONG_STEM = "中" * 128
    # The shape that actually surfaced this: combining marks cost ~2 bytes
    # each and render as nothing, so the name looks short and is not.
    COMBINING_STEM = "ʅ" + ("͡" * 40) + "(" + ("̸̢̛̼̞̭͋ͅ" * 20) + ")"

    def _name(self, remote: str, **attrs: int) -> str:
        from lib.staged_album import staged_filename

        file = make_download_file(filename=remote)
        for key, value in attrs.items():
            setattr(file, key, value)
        return staged_filename(file)

    def test_bounds_overlong_remote_basename_to_the_byte_cap(self):
        from lib.processing_paths import MAX_PATH_COMPONENT_BYTES

        for desc, stem in (("cjk", self.LONG_STEM),
                           ("combining marks", self.COMBINING_STEM)):
            with self.subTest(desc=desc):
                name = self._name(f"user\\Album\\{stem}.flac")
                self.assertGreater(len(f"{stem}.flac".encode()), 255)
                self.assertLessEqual(
                    len(name.encode()), MAX_PATH_COMPONENT_BYTES)

    def test_preserves_the_extension(self):
        name = self._name(f"user\\Album\\{self.LONG_STEM}.flac")

        self.assertTrue(name.endswith(".flac"), name[-20:])

    def test_leaves_names_within_the_cap_byte_identical(self):
        # Load-bearing: ``_canonical_manifest_complete`` recomputes these
        # names and compares them against ``os.listdir`` of an already
        # published album, so any churn here would invalidate every
        # canonical album on disk.
        self.assertEqual(
            self._name("user\\Album\\01 - Track.flac"), "01 - Track.flac")

    def test_is_deterministic(self):
        remote = f"user\\Album\\{self.LONG_STEM}.flac"

        self.assertEqual(self._name(remote), self._name(remote))

    def test_distinct_overlong_names_do_not_collapse(self):
        first = self._name(f"user\\Album\\{self.LONG_STEM}one.flac")
        second = self._name(f"user\\Album\\{self.LONG_STEM}two.flac")

        self.assertNotEqual(first, second)

    def test_never_cuts_a_multibyte_character(self):
        name = self._name(f"user\\Album\\{self.LONG_STEM}.flac")

        self.assertEqual(name, name.encode().decode("utf-8"))
        self.assertNotIn("�", name)

    def test_result_carries_no_path_separator(self):
        name = self._name(f"user\\Album\\{self.LONG_STEM}.flac")

        self.assertNotIn("/", name)
        self.assertNotIn("\\", name)

    def test_bounds_the_disc_prefixed_name_too(self):
        from lib.processing_paths import MAX_PATH_COMPONENT_BYTES

        name = self._name(
            f"user\\Album\\{self.LONG_STEM}.flac", disk_no=1, disk_count=2)

        self.assertLessEqual(len(name.encode()), MAX_PATH_COMPONENT_BYTES)
        self.assertTrue(name.startswith("Disk 1 - "), name[:20])

    def test_bounding_never_yields_a_traversal_or_empty_name(self):
        # ``_safe_relpath`` rejects these outright, so bounding must never
        # manufacture one out of an ordinary overlong name.
        for desc, stem in (("dots", "." * 300), ("spaces", " " * 300),
                           ("cjk", self.LONG_STEM)):
            with self.subTest(desc=desc):
                name = self._name(f"user\\Album\\{stem}.flac")
                self.assertNotIn(name, {"", ".", ".."})


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
