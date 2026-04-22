#!/usr/bin/env python3
"""Tests for ``lib.library_delete_service``.

Issue #153 extracts the `/api/beets/delete` workflow out of the route so
failure-mode semantics live behind one typed service seam.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from lib.beets_db import BeetsDB
from lib.library_delete_service import (
    DeleteBeetsFailure,
    DeletePipelinePurgeFailure,
    DeletePostPurgeBeetsFailure,
    DeletePreflightFailure,
    DeleteRequest,
    DeleteSuccess,
    delete_release_from_library,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_beets_db import _create_test_db, _insert_album

RELEASE_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


class TestDeleteReleaseFromLibrary(unittest.TestCase):
    """Direct service tests for the extracted delete workflow."""

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

    def test_success_deletes_album_files_and_pipeline_request(self) -> None:
        track_path = self._seed_album()
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=RELEASE_UUID,
        ))

        result = delete_release_from_library(
            beets_db_path=self.db_path,
            pipeline_db=fake_db,
            request=DeleteRequest(
                album_id=7,
                purge_pipeline=True,
                release_id=RELEASE_UUID.upper(),
            ),
        )

        self.assertIsInstance(result, DeleteSuccess)
        assert isinstance(result, DeleteSuccess)
        self.assertEqual(result.album_id, 7)
        self.assertEqual(result.album_name, "Test Album")
        self.assertEqual(result.artist_name, "Test Artist")
        self.assertEqual(result.deleted_files, 1)
        self.assertTrue(result.pipeline_deleted)
        self.assertEqual(result.deleted_pipeline_id, 42)
        self.assertFalse(os.path.exists(track_path))
        self.assertFalse(os.path.isdir(os.path.dirname(track_path)))
        self.assertIsNone(fake_db.get_request(42))
        with BeetsDB(self.db_path) as beets:
            self.assertIsNone(beets.get_album_detail(7))

    def test_missing_album_returns_preflight_failure(self) -> None:
        result = delete_release_from_library(
            beets_db_path=self.db_path,
            pipeline_db=FakePipelineDB(),
            request=DeleteRequest(album_id=999),
        )

        self.assertIsInstance(result, DeletePreflightFailure)
        assert isinstance(result, DeletePreflightFailure)
        self.assertEqual(result.reason, "album_not_found")
        self.assertEqual(result.album_id, 999)

    @patch("lib.beets_db.BeetsDB.delete_album")
    def test_pipeline_purge_failure_aborts_before_beets_delete(
        self,
        mock_delete_album: MagicMock,
    ) -> None:
        self._seed_album()
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=RELEASE_UUID,
        ))
        fake_db.delete_request = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]

        result = delete_release_from_library(
            beets_db_path=self.db_path,
            pipeline_db=fake_db,
            request=DeleteRequest(
                album_id=7,
                purge_pipeline=True,
                release_id=RELEASE_UUID,
            ),
        )

        self.assertIsInstance(result, DeletePipelinePurgeFailure)
        assert isinstance(result, DeletePipelinePurgeFailure)
        self.assertEqual(result.album_id, 7)
        self.assertEqual(result.pipeline_request_id, 42)
        self.assertIsNotNone(fake_db.get_request(42))
        mock_delete_album.assert_not_called()

    @patch("lib.beets_db.BeetsDB.delete_album")
    def test_post_purge_beets_failure_returns_partial_success_result(
        self,
        mock_delete_album: MagicMock,
    ) -> None:
        self._seed_album()
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=RELEASE_UUID,
        ))
        mock_delete_album.side_effect = OSError("boom")

        result = delete_release_from_library(
            beets_db_path=self.db_path,
            pipeline_db=fake_db,
            request=DeleteRequest(
                album_id=7,
                purge_pipeline=True,
                release_id=RELEASE_UUID,
            ),
        )

        self.assertIsInstance(result, DeletePostPurgeBeetsFailure)
        assert isinstance(result, DeletePostPurgeBeetsFailure)
        self.assertEqual(result.album_id, 7)
        self.assertEqual(result.deleted_pipeline_id, 42)
        self.assertIsNone(fake_db.get_request(42))

    @patch("lib.beets_db.BeetsDB.delete_album")
    def test_beets_failure_without_pipeline_purge_returns_generic_failure(
        self,
        mock_delete_album: MagicMock,
    ) -> None:
        self._seed_album()
        mock_delete_album.side_effect = OSError("boom")

        result = delete_release_from_library(
            beets_db_path=self.db_path,
            pipeline_db=FakePipelineDB(),
            request=DeleteRequest(album_id=7),
        )

        self.assertIsInstance(result, DeleteBeetsFailure)
        assert isinstance(result, DeleteBeetsFailure)
        self.assertEqual(result.album_id, 7)


if __name__ == "__main__":
    unittest.main()
