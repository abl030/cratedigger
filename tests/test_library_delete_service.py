#!/usr/bin/env python3
"""Filesystem and beets integration tests for the destructive service."""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess as sp
import sys
import tempfile
import unittest
from contextlib import closing, contextmanager
from typing import Iterator
from unittest.mock import patch

from lib.beets_db import BeetsDB
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailed,
    BeetsDeleteRequest,
    _OwnedPath,
    _delete_manifest,
    run_beets_delete,
)
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
            with closing(sqlite3.connect(self.db_path)) as conn:
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
                conn.commit()
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

    def test_postcommit_subprocess_error_requires_manual_recovery(self) -> None:
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

        track_path = self._seed_album()
        pipeline = TrackingPipeline()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))
        notified: list[str] = []

        def nonzero_after_commit(argv, **_kwargs):
            os.remove(track_path)
            with closing(sqlite3.connect(self.db_path)) as conn:
                conn.execute(
                    "DELETE FROM items WHERE album_id = ?", (7,),
                )
                conn.execute(
                    "DELETE FROM albums WHERE id = ?", (7,),
                )
                conn.commit()
            return sp.CompletedProcess(
                argv, 23, stdout=b"", stderr=b"ack channel closed",
            )

        def commit_then_lose_ack(request: BeetsDeleteRequest):
            return run_beets_delete(request, runner=nonzero_after_commit)

        with (
            patch.dict(os.environ, {
                "BEETSDIR": self.tmpdir,
                "CRATEDIGGER_BEETS_PYTHON": sys.executable,
                "CRATEDIGGER_RUNTIME_CONFIG": os.path.join(
                    self.tmpdir, "missing-runtime.ini",
                ),
            }),
            BeetsDB(self.db_path) as beets,
        ):
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(
                    album_id=7, purge_pipeline=True,
                    expected_pipeline_id=42,
                    expected_release_id=RELEASE_UUID,
                ),
                beets_delete_fn=commit_then_lose_ack,
                notify_fn=lambda path: notified.append(path) or (),
            )
            retry = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=lambda _request: self.fail(
                    "retry must stop at the parent preflight"),
                notify_fn=lambda _path: self.fail(
                    "retry must not notify a second time"),
            )

        self.assertIsInstance(result, DeleteIncomplete)
        assert isinstance(result, DeleteIncomplete)
        self.assertTrue(result.acknowledgement_lost)
        self.assertFalse(result.album_still_present)
        self.assertEqual(result.album_name, "Test Album")
        self.assertEqual(result.artist_name, "Test Artist")
        self.assertEqual(result.former_album_path, os.path.dirname(track_path))
        self.assertEqual(result.pipeline_request_id, 42)
        self.assertEqual(result.pipeline_status, "imported")
        self.assertIsNone(result.deleted_files)
        self.assertIsNone(result.deleted_artifacts)
        self.assertIn("Beets acknowledgement was lost", result.detail)
        self.assertIn("metadata may be gone", result.detail)
        self.assertIn("Pipeline request #42 (imported) was preserved", result.detail)
        self.assertIn("Do not assume files were deleted", result.detail)
        self.assertEqual(notified, [])
        self.assertEqual(pipeline.active_locks, 0)
        self.assertIsNotNone(pipeline.get_request(42))
        self.assertIsInstance(retry, DeleteAlbumNotFound)

    def test_protocol_loss_with_metadata_gone_and_audio_present_fails_closed(
        self,
    ) -> None:
        track_path = self._seed_album()
        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))
        notified: list[str] = []

        def invalid_json_after_commit(argv, **_kwargs):
            with closing(sqlite3.connect(self.db_path)) as conn:
                conn.execute(
                    "DELETE FROM items WHERE album_id = ?", (7,),
                )
                conn.execute(
                    "DELETE FROM albums WHERE id = ?", (7,),
                )
                conn.commit()
            return sp.CompletedProcess(argv, 0, stdout=b"{", stderr=b"")

        def commit_then_lose_protocol(request: BeetsDeleteRequest):
            return run_beets_delete(request, runner=invalid_json_after_commit)

        with (
            patch.dict(os.environ, {
                "BEETSDIR": self.tmpdir,
                "CRATEDIGGER_BEETS_PYTHON": sys.executable,
                "CRATEDIGGER_RUNTIME_CONFIG": os.path.join(
                    self.tmpdir, "missing-runtime.ini",
                ),
            }),
            BeetsDB(self.db_path) as beets,
        ):
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=False),
                beets_delete_fn=commit_then_lose_protocol,
                notify_fn=lambda path: notified.append(path) or (),
            )
            retry = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7),
            )

        self.assertIsInstance(result, DeleteIncomplete)
        assert isinstance(result, DeleteIncomplete)
        self.assertTrue(result.acknowledgement_lost)
        self.assertFalse(result.album_still_present)
        self.assertEqual(result.former_album_path, os.path.dirname(track_path))
        self.assertEqual(result.pipeline_request_id, 42)
        self.assertEqual(result.pipeline_status, "imported")
        self.assertIsNone(result.deleted_files)
        self.assertIsNone(result.deleted_artifacts)
        self.assertTrue(os.path.exists(track_path))
        self.assertIsNotNone(pipeline.get_request(42))
        self.assertEqual(notified, [])
        self.assertIsInstance(retry, DeleteAlbumNotFound)

    def test_ack_loss_with_orphan_items_is_not_reconciled(self) -> None:
        track_path = self._seed_album()
        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))

        def album_only_commit(request: BeetsDeleteRequest) -> BeetsDeleteFailed:
            with closing(sqlite3.connect(self.db_path)) as conn:
                conn.execute(
                    "DELETE FROM albums WHERE id = ?", (request.album_id,),
                )
                conn.commit()
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason="protocol_error",
                detail="planted partial metadata commit",
                album_still_present=False,
            )

        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=album_only_commit,
                notify_fn=lambda _path: self.fail("incomplete delete must not notify"),
            )
            self.assertFalse(beets.album_and_items_absent(7))

        self.assertIsInstance(result, DeleteIncomplete)
        self.assertTrue(os.path.exists(track_path))
        self.assertIsNotNone(pipeline.get_request(42))

    def test_precommit_failure_retains_authority_and_retry_converges(self) -> None:
        track_path = self._seed_album()
        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))
        failure = BeetsDeleteFailed(
            album_id=7,
            reason="filesystem_error",
            detail="planted unlink failure",
            album_still_present=True,
            remaining_owned_paths=(track_path,),
        )

        with BeetsDB(self.db_path) as beets:
            first = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=lambda _request: failure,
                notify_fn=lambda _path: self.fail("failed attempt must not notify"),
            )
            self.assertIsNotNone(beets.get_album_detail(7))
            second = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=self._delete_fn(""),
                notify_fn=lambda _path: (),
            )

        self.assertIsInstance(first, DeleteIncomplete)
        self.assertIsInstance(second, DeleteSuccess)
        self.assertFalse(os.path.exists(track_path))
        self.assertIsNone(pipeline.get_request(42))

    def test_final_unknown_enumeration_error_retains_beets_pg_and_skips_notify(
        self,
    ) -> None:
        track_path = self._seed_album()
        album_dir = os.path.dirname(track_path)
        sentinel = os.path.join(album_dir, "booklet.pdf")
        with open(sentinel, "wb") as handle:
            handle.write(b"preserve")
        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))
        list_calls = 0

        def list_with_final_fault(directory):
            nonlocal list_calls
            list_calls += 1
            if list_calls == 2:
                raise OSError("planted final enumeration fault")
            return tuple(directory.iterdir())

        def enumeration_failure(
            request: BeetsDeleteRequest,
        ) -> BeetsDeleteCompleted | BeetsDeleteFailed:
            return _delete_manifest(
                album_id=request.album_id,
                album_name="Test Album",
                artist_name="Test Artist",
                owned_paths=(_OwnedPath(track_path, "track"),),
                album_dirs=(album_dir,),
                metadata_remove=lambda: self.fail(
                    "enumeration failure must retain Beets metadata",
                ),
                album_present=lambda: True,
                remove_path=os.remove,
                prune_dir=lambda _path: None,
                list_dir=list_with_final_fault,
            )

        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=enumeration_failure,
                notify_fn=lambda _path: self.fail(
                    "incomplete enumeration must not notify",
                ),
            )
            self.assertIsNotNone(beets.get_album_detail(7))

        self.assertIsInstance(result, DeleteIncomplete)
        assert isinstance(result, DeleteIncomplete)
        self.assertEqual(result.reason, "filesystem_error")
        self.assertIn("before metadata removal", result.detail)
        self.assertFalse(os.path.exists(track_path))
        with open(sentinel, "rb") as handle:
            self.assertEqual(handle.read(), b"preserve")
        self.assertEqual(result.preserved_paths, (sentinel,))
        self.assertIsNotNone(pipeline.get_request(42))

    def test_presence_probe_error_retains_beets_pg_and_skips_notify(self) -> None:
        track_path = self._seed_album()
        album_dir = os.path.dirname(track_path)
        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=RELEASE_UUID,
        ))
        probe_calls = 0
        notifications: list[str] = []

        def probe(path: str) -> bool:
            nonlocal probe_calls
            probe_calls += 1
            if probe_calls == 2:
                raise OSError("planted post-delete presence fault")
            try:
                os.lstat(path)
            except FileNotFoundError:
                return False
            return True

        def presence_failure(
            request: BeetsDeleteRequest,
        ) -> BeetsDeleteCompleted | BeetsDeleteFailed:
            return _delete_manifest(
                album_id=request.album_id,
                album_name="Test Album",
                artist_name="Test Artist",
                owned_paths=(_OwnedPath(track_path, "track"),),
                album_dirs=(album_dir,),
                metadata_remove=lambda: self.fail(
                    "presence failure must retain Beets metadata",
                ),
                album_present=lambda: True,
                remove_path=os.remove,
                prune_dir=lambda _path: None,
                path_exists=probe,
            )

        with BeetsDB(self.db_path) as beets:
            result = delete_release_from_library(
                pipeline_db=pipeline,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=presence_failure,
                notify_fn=lambda path: notifications.append(path) or (),
            )
            self.assertIsNotNone(beets.get_album_detail(7))

        self.assertIsInstance(result, DeleteIncomplete)
        assert isinstance(result, DeleteIncomplete)
        self.assertEqual(result.reason, "postcondition_failed")
        self.assertIn("presence probe", result.detail)
        self.assertFalse(os.path.exists(track_path))
        self.assertIsNotNone(pipeline.get_request(42))
        self.assertEqual(notifications, [])

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
