#!/usr/bin/env python3
"""Filesystem and beets integration tests for the destructive service."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest

from lib.beets_db import BeetsDB
from lib.destructive_release_service import (
    DeleteAlbumNotFound,
    DeleteRequest,
    DeleteSuccess,
    delete_release_from_library,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_beets_db import _create_test_db, _insert_album


RELEASE_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


class TestDeleteReleaseFromLibrary(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "beets.db")
        _create_test_db(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _seed_album(self) -> str:
        track_path = os.path.join(
            self.tmpdir, "library", "Test Artist", "Test Album", "01 Track.mp3",
        )
        os.makedirs(os.path.dirname(track_path), exist_ok=True)
        with open(track_path, "wb") as handle:
            handle.write(b"mp3")
        _insert_album(
            self.db_path,
            7,
            RELEASE_UUID,
            [(320000, track_path)],
            album="Test Album",
            albumartist="Test Artist",
        )
        return track_path

    def test_success_deletes_exact_album_files_and_pipeline_request(self) -> None:
        track_path = self._seed_album()
        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=RELEASE_UUID,
        ))
        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(
                    album_id=7,
                    purge_pipeline=True,
                    expected_pipeline_id=42,
                    expected_release_id=RELEASE_UUID.upper(),
                ),
            )

        self.assertIsInstance(result, DeleteSuccess)
        assert isinstance(result, DeleteSuccess)
        self.assertEqual(result.deleted_pipeline_id, 42)
        self.assertEqual(result.deleted_files, 1)
        self.assertFalse(os.path.exists(track_path))
        self.assertIsNone(pipeline.get_request(42))
        with BeetsDB(self.db_path) as beets:
            self.assertIsNone(beets.get_album_detail(7))

    def test_relative_item_paths_are_resolved_before_file_delete(self) -> None:
        library_root = os.path.join(self.tmpdir, "library")
        relative_path = os.path.join("Test Artist", "Test Album", "01 Track.mp3")
        absolute_path = os.path.join(library_root, relative_path)
        os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
        with open(absolute_path, "wb") as handle:
            handle.write(b"mp3")
        _insert_album(
            self.db_path,
            7,
            RELEASE_UUID,
            [(320000, relative_path)],
            album="Test Album",
            albumartist="Test Artist",
        )
        with BeetsDB(self.db_path, library_root=library_root) as beets:
            result = delete_release_from_library(
                pipeline_db=FakePipelineDB(),
                beets_db=beets,
                request=DeleteRequest(album_id=7),
            )

        self.assertIsInstance(result, DeleteSuccess)
        self.assertFalse(os.path.exists(absolute_path))

    def test_missing_album_returns_not_found_without_locks(self) -> None:
        pipeline = FakePipelineDB()
        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=999),
            )

        self.assertIsInstance(result, DeleteAlbumNotFound)
        self.assertEqual(pipeline.advisory_lock_calls, [])


if __name__ == "__main__":
    unittest.main()
