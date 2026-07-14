#!/usr/bin/env python3
"""Filesystem and beets integration tests for the destructive service."""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from typing import Iterator

from lib.beets_db import BeetsDB
from lib.beets_delete import BeetsDeleteCompleted, BeetsDeleteRequest
from lib.destructive_release_service import (
    DeleteAlbumNotFound,
    DeleteIncomplete,
    DeletePipelinePurgeFailure,
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

    def _delete_fn(self, album_dir: str):
        def delete(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    "SELECT path FROM items WHERE album_id = ?", (request.album_id,),
                ).fetchall()
                for (raw,) in rows:
                    path = os.fsdecode(raw)
                    if not os.path.isabs(path):
                        path = os.path.join(album_dir, path)
                    if os.path.exists(path):
                        os.remove(path)
                conn.execute("DELETE FROM items WHERE album_id = ?", (request.album_id,))
                conn.execute("DELETE FROM albums WHERE id = ?", (request.album_id,))
            return BeetsDeleteCompleted(
                album_id=request.album_id,
                album_name="Test Album",
                artist_name="Test Artist",
                former_album_path=os.path.dirname(
                    os.path.join(album_dir, "Test Artist", "Test Album", "track")),
                deleted_tracks=len(rows),
                deleted_artifacts=len(rows),
                preserved_paths=(),
            )
        return delete

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
                beets_delete_fn=self._delete_fn(""),
                notify_fn=lambda _path: (),
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
                beets_delete_fn=self._delete_fn(library_root),
                notify_fn=lambda _path: (),
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

    def test_claimed_success_with_surviving_beets_row_is_incomplete(self) -> None:
        self._seed_album()
        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))
        lied = BeetsDeleteCompleted(
            album_id=7,
            album_name="Test Album",
            artist_name="Test Artist",
            former_album_path=os.path.join(self.tmpdir, "library"),
            deleted_tracks=0,
            deleted_artifacts=0,
            preserved_paths=(),
        )
        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=lambda _request: lied,
                notify_fn=lambda _path: (),
            )

        self.assertIsInstance(result, DeleteIncomplete)
        self.assertIsNotNone(pipeline.get_request(42))
        with BeetsDB(self.db_path) as beets:
            self.assertIsNotNone(beets.get_album_detail(7))

    def test_notifier_exception_is_typed_after_locks_release(self) -> None:
        class TrackingPipeline(FakePipelineDB):
            active_locks = 0

            @contextmanager
            def advisory_lock(
                self, namespace: int, key: int,
            ) -> Iterator[bool]:
                with super().advisory_lock(namespace, key) as acquired:
                    if acquired:
                        self.active_locks += 1
                    try:
                        yield acquired
                    finally:
                        if acquired:
                            self.active_locks -= 1

        self._seed_album()
        pipeline = TrackingPipeline()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))

        def notifier(_path: str):
            self.assertEqual(pipeline.active_locks, 0)
            raise RuntimeError("planted notifier failure")

        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7),
                beets_delete_fn=self._delete_fn(""),
                notify_fn=notifier,
            )

        self.assertIsInstance(result, DeleteSuccess)
        assert isinstance(result, DeleteSuccess)
        self.assertEqual(pipeline.active_locks, 0)
        self.assertEqual(
            [(item.provider, item.status) for item in result.notifications],
            [("plex", "warning"), ("jellyfin", "warning")],
        )

    def test_notifier_exception_preserves_pipeline_partial_result(self) -> None:
        class FailingPurgePipeline(FakePipelineDB):
            active_locks = 0

            @contextmanager
            def advisory_lock(
                self, namespace: int, key: int,
            ) -> Iterator[bool]:
                with super().advisory_lock(namespace, key) as acquired:
                    if acquired:
                        self.active_locks += 1
                    try:
                        yield acquired
                    finally:
                        if acquired:
                            self.active_locks -= 1

            def delete_request(self, request_id: int) -> None:
                raise RuntimeError(f"planted PG failure for {request_id}")

        self._seed_album()
        pipeline = FailingPurgePipeline()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))

        def notifier(_path: str):
            self.assertEqual(pipeline.active_locks, 0)
            raise RuntimeError("planted notifier failure")

        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=self._delete_fn(""),
                notify_fn=notifier,
            )

        self.assertIsInstance(result, DeletePipelinePurgeFailure)
        assert isinstance(result, DeletePipelinePurgeFailure)
        self.assertEqual(pipeline.active_locks, 0)
        self.assertIsNotNone(pipeline.get_request(42))
        self.assertEqual(
            [(item.provider, item.status) for item in result.notifications],
            [("plex", "warning"), ("jellyfin", "warning")],
        )


if __name__ == "__main__":
    unittest.main()
