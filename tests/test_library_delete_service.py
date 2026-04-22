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
    DeleteAlbumNotFound,
    DeleteBeetsFailure,
    DeleteBeetsDbUnavailable,
    DeletePipelinePurgeFailure,
    DeletePostPurgeBeetsFailure,
    DeleteRequest,
    DeleteSuccess,
    delete_release_from_library,
    resolve_pipeline_request,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_beets_db import _create_test_db, _insert_album

RELEASE_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


class TestResolvePipelineRequest(unittest.TestCase):
    """Shared pipeline-request lookup seam for album detail and delete."""

    def test_returns_none_without_pipeline_db(self) -> None:
        self.assertIsNone(
            resolve_pipeline_request(
                None,
                pipeline_id=42,
                release_id=RELEASE_UUID,
            ),
        )

    def test_prefers_explicit_pipeline_id(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=RELEASE_UUID,
        ))
        fake_db.seed_request(make_request_row(
            id=99,
            status="wanted",
            mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ))

        result = resolve_pipeline_request(
            fake_db,
            pipeline_id=42,
            release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["id"], 42)

    def test_falls_back_to_release_id_when_pipeline_id_missing(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=99,
            status="imported",
            mb_release_id=RELEASE_UUID,
        ))

        result = resolve_pipeline_request(
            fake_db,
            pipeline_id=42,
            release_id=RELEASE_UUID.upper(),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["id"], 99)

    def test_blank_release_id_returns_none(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=RELEASE_UUID,
        ))

        result = resolve_pipeline_request(
            fake_db,
            pipeline_id=None,
            release_id="   ",
        )

        self.assertIsNone(result)


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

        self.assertIsInstance(result, DeleteAlbumNotFound)
        assert isinstance(result, DeleteAlbumNotFound)
        self.assertEqual(result.album_id, 999)

    def test_missing_beets_db_returns_preflight_unavailable(self) -> None:
        result = delete_release_from_library(
            beets_db_path=os.path.join(self.tmpdir, "missing.db"),
            pipeline_db=FakePipelineDB(),
            request=DeleteRequest(album_id=7),
        )

        self.assertIsInstance(result, DeleteBeetsDbUnavailable)
        assert isinstance(result, DeleteBeetsDbUnavailable)
        self.assertEqual(result.album_id, 7)

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
    def test_value_error_after_pipeline_purge_returns_partial_success_result(
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
        mock_delete_album.side_effect = ValueError("gone")

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

    @patch("lib.beets_db.BeetsDB.delete_album")
    def test_value_error_without_pipeline_purge_returns_album_not_found(
        self,
        mock_delete_album: MagicMock,
    ) -> None:
        self._seed_album()
        mock_delete_album.side_effect = ValueError("gone")

        result = delete_release_from_library(
            beets_db_path=self.db_path,
            pipeline_db=FakePipelineDB(),
            request=DeleteRequest(album_id=7),
        )

        self.assertIsInstance(result, DeleteAlbumNotFound)
        assert isinstance(result, DeleteAlbumNotFound)
        self.assertEqual(result.album_id, 7)


if __name__ == "__main__":
    unittest.main()
