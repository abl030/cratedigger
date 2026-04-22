"""Tests for ``lib/staged_album.py``."""

import os
import tempfile
import unittest

from tests.fakes import FakePipelineDB
from tests.helpers import make_download_file, make_request_row


class TestStageToAiPath(unittest.TestCase):

    def test_sanitizes_artist_and_title(self):
        from lib.staged_album import stage_to_ai_path

        dest = stage_to_ai_path(
            artist='Test: "Artist"',
            title="Album/Title?",
            staging_dir="/tmp/staging",
        )

        self.assertEqual(dest, "/tmp/staging/Test Artist/AlbumTitle")


class TestStagedAlbum(unittest.TestCase):

    def test_move_to_moves_contents_and_updates_db(self):
        from lib.staged_album import StagedAlbum

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"filetype": "mp3", "files": []},
        ))

        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source")
            os.makedirs(source)
            with open(os.path.join(source, "track.mp3"), "w") as fp:
                fp.write("audio")

            dest = os.path.join(tmpdir, "staging", "Artist", "Album")
            staged_album = StagedAlbum(current_path=source, request_id=42)

            result = staged_album.move_to(dest, db)

            self.assertEqual(result, dest)
            self.assertEqual(staged_album.current_path, dest)
            self.assertTrue(os.path.exists(os.path.join(dest, "track.mp3")))
            self.assertFalse(os.path.exists(source))
            self.assertEqual(
                db.request(42)["active_download_state"]["current_path"],
                dest,
            )

    def test_move_to_idempotent_when_source_equals_target(self):
        from lib.staged_album import StagedAlbum

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"filetype": "mp3", "files": []},
        ))

        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "staging")
            os.makedirs(target)
            with open(os.path.join(target, "track.mp3"), "w") as fp:
                fp.write("audio")

            staged_album = StagedAlbum(current_path=target, request_id=42)

            result = staged_album.move_to(target, db)

            self.assertEqual(result, target)
            self.assertTrue(os.path.exists(os.path.join(target, "track.mp3")))
            self.assertEqual(
                db.request(42)["active_download_state"]["current_path"],
                target,
            )

    def test_move_to_rolls_back_when_db_persist_fails(self):
        from lib.staged_album import StagedAlbum

        class ExplodingDB:
            def update_download_state_current_path(
                self,
                request_id: int,
                current_path: str | None,
            ) -> None:
                raise RuntimeError("db boom")

        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source")
            os.makedirs(source)
            source_file = os.path.join(source, "track.mp3")
            with open(source_file, "w") as fp:
                fp.write("audio")

            dest = os.path.join(tmpdir, "staging", "Artist", "Album")
            staged_album = StagedAlbum(current_path=source, request_id=42)

            with self.assertRaisesRegex(RuntimeError, "db boom"):
                staged_album.move_to(dest, ExplodingDB())

            self.assertEqual(staged_album.current_path, source)
            self.assertTrue(os.path.exists(source_file))
            self.assertFalse(os.path.exists(os.path.join(dest, "track.mp3")))

    def test_persist_current_path_noop_without_request_id(self):
        from lib.staged_album import StagedAlbum

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"filetype": "mp3", "files": []},
        ))

        StagedAlbum(current_path="/tmp/staged").persist_current_path(db)

        self.assertNotIn(
            "current_path",
            db.request(42)["active_download_state"],
        )

    def test_persist_current_path_noop_without_db(self):
        from lib.staged_album import StagedAlbum

        staged_album = StagedAlbum(current_path="/tmp/staged", request_id=42)

        staged_album.persist_current_path(None)

        self.assertEqual(staged_album.current_path, "/tmp/staged")

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
