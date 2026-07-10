#!/usr/bin/env python3
"""Contract tests for web/routes/library.py: beets search/recent/delete.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import (
    _assert_required_fields,
    _FakeDbWebServerCase,
)

from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row


class _FailingDeleteDB(FakePipelineDB):
    """delete_request raises — pins purge-failure ordering (no beets
    delete may run after the pipeline purge fails)."""

    def delete_request(self, request_id: int) -> None:
        raise RuntimeError("boom")


class TestBeetsRouteContracts(_FakeDbWebServerCase):
    """Contract tests for frontend-consumed beets library routes."""

    ALBUM_REQUIRED_FIELDS = {
        "id", "album", "artist", "year", "mb_albumid", "track_count",
        "mb_releasegroupid", "release_group_title", "added", "formats",
        "min_bitrate", "type", "label", "country", "source",
    }
    DETAIL_REQUIRED_FIELDS = (
        ALBUM_REQUIRED_FIELDS | {
            "artpath", "path", "tracks", "pipeline_id", "pipeline_status",
            "pipeline_source", "pipeline_min_bitrate",
            "search_filetype_override", "target_format", "upgrade_queued",
            "download_history",
        }
    )
    TRACK_REQUIRED_FIELDS = {
        "id", "artist", "disc", "track", "title", "length", "format",
        "bitrate", "samplerate", "bitdepth", "path",
    }
    # `/api/beets/album/<id>` historically forwarded the full LogEntry JSON
    # plus derived verdict/downloaded_label. Keep the explicit list here so
    # route contract coverage catches accidental payload narrowing.
    HISTORY_REQUIRED_FIELDS = {
        "id", "request_id", "outcome", "created_at", "beets_scenario",
        "beets_distance", "beets_detail", "soulseek_username",
        "error_message", "import_result", "validation_result", "filetype",
        "bitrate", "was_converted", "original_filetype", "actual_filetype",
        "actual_min_bitrate", "slskd_filetype", "slskd_bitrate",
        "downloaded_label", "verdict", "comparison_basis",
        "disambiguation_failure",
        "disambiguation_detail", "bad_extensions", "spectral_grade",
        "spectral_bitrate", "existing_min_bitrate",
        "existing_spectral_bitrate", "album_title",
        "artist_name", "mb_release_id", "request_status",
        "request_min_bitrate", "search_filetype_override", "source",
        "wrong_match_triage_action", "wrong_match_triage_summary",
        "wrong_match_triage_reason", "wrong_match_triage_preview_verdict",
        "wrong_match_triage_preview_decision",
        "wrong_match_triage_stage_chain", "wrong_match_triage_detail",
    }
    DELETE_REQUIRED_FIELDS = {
        "status", "id", "album", "artist", "deleted_files",
        "pipeline_deleted", "pipeline_id",
    }

    RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    RG_ID = "11111111-1111-1111-1111-111111111111"

    def setUp(self) -> None:
        super().setUp()
        import web.server as srv

        self._srv = srv
        self._orig_beets = srv._beets
        self._orig_beets_db_path = srv.beets_db_path
        self.beets_db = FakeBeetsDB()
        srv._beets = self.beets_db
        self.db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
        ))
        # One real success row so album-detail download_history flows
        # through the fake's get_download_history query semantics.
        self.db.log_download(
            42, outcome="success", beets_scenario="strong_match",
            beets_distance=0.012, soulseek_username="testuser",
            filetype="mp3", bitrate=320000, was_converted=False,
            actual_filetype="mp3", actual_min_bitrate=320,
            slskd_filetype="mp3", slskd_bitrate=320000, valid=True,
        )

    def tearDown(self) -> None:
        self._srv._beets = self._orig_beets
        self._srv.beets_db_path = self._orig_beets_db_path

    def _album(self) -> dict:
        return {
            "id": 7,
            "album": "Test Album",
            "artist": "Test Artist",
            "year": 2024,
            "mb_albumid": self.RELEASE_ID,
            "track_count": 10,
            "mb_releasegroupid": self.RG_ID,
            "release_group_title": "Test Album",
            "added": 1773651901.0,
            "formats": "MP3",
            "min_bitrate": 320000,
            "type": "album",
            "label": "Test Label",
            "country": "US",
            "source": "musicbrainz",
        }

    def _track(self) -> dict:
        return {
            "id": 11,
            "artist": "Test Artist",
            "disc": 1,
            "track": 1,
            "title": "Track",
            "length": 180.0,
            "format": "MP3",
            "bitrate": 320000,
            "samplerate": 44100,
            "bitdepth": 16,
            "path": "/music/Test Artist/Test Album/01 Track.mp3",
        }

    def _configure_beets_delete_mock(
        self,
        beets_cls: MagicMock,
        *,
        delete_side_effect: object | None = None,
    ) -> None:
        """The class patch swaps the BeetsDB constructor for a seeded
        FakeBeetsDB; ``delete_album`` stays a MagicMock attr — it is the
        allowlisted SQLite-write + file-delete leaf seam, not a query
        collaborator."""
        fake = FakeBeetsDB()
        fake.set_album_detail(7, {"id": 7})
        beets_cls.return_value = fake
        if delete_side_effect is not None:
            beets_cls.delete_album.side_effect = delete_side_effect
            return
        beets_cls.delete_album.return_value = (
            "Test Album",
            "Test Artist",
            ["/music/Test Artist/Test Album/01 Track.mp3"],
        )

    def test_beets_album_detail_contract(self):
        detail = self._album()
        detail["artpath"] = "/music/Test Artist/Test Album/cover.jpg"
        detail["path"] = "/music/Test Artist/Test Album"
        detail["tracks"] = [self._track()]
        self.beets_db.set_album_detail(7, detail)

        status, data = self._get("/api/beets/album/7")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DETAIL_REQUIRED_FIELDS,
                                "beets album detail")
        _assert_required_fields(self, data["tracks"][0], self.TRACK_REQUIRED_FIELDS,
                                "beets album track")
        _assert_required_fields(
            self,
            data["download_history"][0],
            self.HISTORY_REQUIRED_FIELDS,
            "beets album detail history",
        )
        self.assertEqual(data["artpath"], "/music/Test Artist/Test Album/cover.jpg")
        self.assertEqual(data["tracks"][0]["id"], 11)
        self.assertEqual(data["tracks"][0]["path"], "/music/Test Artist/Test Album/01 Track.mp3")
        self.assertEqual(data["download_history"][0]["actual_min_bitrate"], 320)

    def test_beets_album_detail_discogs_contract(self):
        detail = self._album()
        detail["mb_albumid"] = "12856590"
        detail["source"] = "discogs"
        detail["artpath"] = "/music/Test Artist/Test Album/cover.jpg"
        detail["path"] = "/music/Test Artist/Test Album"
        detail["tracks"] = [self._track()]
        self.beets_db.set_album_detail(7, detail)
        self.db.seed_request(make_request_row(
            id=43,
            status="wanted",
            mb_release_id="12856590",
            discogs_release_id="12856590",
        ))
        self.db.log_download(
            43, outcome="success", beets_scenario="strong_match",
            beets_distance=0.012, soulseek_username="testuser",
            filetype="mp3", actual_filetype="mp3", actual_min_bitrate=320,
        )

        status, data = self._get("/api/beets/album/7")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DETAIL_REQUIRED_FIELDS,
                                "beets album detail (discogs)")
        _assert_required_fields(
            self,
            data["download_history"][0],
            self.HISTORY_REQUIRED_FIELDS,
            "beets album detail history (discogs)",
        )
        self.assertEqual(data["source"], "discogs")
        self.assertEqual(data["mb_albumid"], "12856590")

    def test_beets_album_detail_allows_nullable_legacy_fields(self):
        detail = self._album()
        detail["added"] = None
        detail["artpath"] = "/music/Test Artist/Test Album/cover.jpg"
        detail["path"] = "/music/Test Artist/Test Album"
        detail["tracks"] = [
            {
                **self._track(),
                "disc": None,
                "track": None,
                "title": None,
            }
        ]
        self.beets_db.set_album_detail(7, detail)

        status, data = self._get("/api/beets/album/7")

        self.assertEqual(status, 200)
        self.assertIsNone(data["added"])
        self.assertIsNone(data["tracks"][0]["disc"])
        self.assertIsNone(data["tracks"][0]["track"])
        self.assertIsNone(data["tracks"][0]["title"])

    def test_beets_album_detail_preserves_string_added_and_missing_format(self):
        detail = self._album()
        detail["added"] = "2026-03-30T12:00:00+00:00"
        detail["artpath"] = "/music/Test Artist/Test Album/cover.jpg"
        detail["path"] = "/music/Test Artist/Test Album"
        detail.pop("formats")
        track = self._track()
        del track["format"]
        detail["tracks"] = [track]
        self.beets_db.set_album_detail(7, detail)

        status, data = self._get("/api/beets/album/7")

        self.assertEqual(status, 200)
        self.assertEqual(data["added"], "2026-03-30T12:00:00+00:00")
        self.assertEqual(data["formats"], "")
        self.assertIsNone(data["tracks"][0]["format"])

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_contract(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self._configure_beets_delete_mock(beets_cls)

        status, data = self._post("/api/beets/delete", {"id": 7, "confirm": "DELETE"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DELETE_REQUIRED_FIELDS,
                                "beets delete response")

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purges_explicit_pipeline_request(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self.db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(beets_cls)

        status, data = self._post("/api/beets/delete", {
            "id": 7,
            "confirm": "DELETE",
            "purge_pipeline": True,
            "pipeline_id": 42,
            "release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        self.assertTrue(data["pipeline_deleted"])
        self.assertEqual(data["pipeline_id"], 42)
        self.assertIsNone(self.db.get_request(42))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purges_pipeline_request_by_release_id_fallback(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        # Drop the setUp-seeded request 42 (same RELEASE_ID) so the
        # release-id fallback can only resolve to this test's row.
        self.db.delete_request(42)
        self.db.seed_request(make_request_row(
            id=99, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(beets_cls)

        status, data = self._post("/api/beets/delete", {
            "id": 7,
            "confirm": "DELETE",
            "purge_pipeline": True,
            "release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        self.assertTrue(data["pipeline_deleted"])
        self.assertEqual(data["pipeline_id"], 99)
        self.assertIsNone(self.db.get_request(99))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purges_pipeline_request_by_uppercase_release_id(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        # Drop the setUp-seeded request 42 (same RELEASE_ID) so the
        # release-id fallback can only resolve to this test's row.
        self.db.delete_request(42)
        self.db.seed_request(make_request_row(
            id=98, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(beets_cls)

        status, data = self._post("/api/beets/delete", {
            "id": 7,
            "confirm": "DELETE",
            "purge_pipeline": True,
            "release_id": self.RELEASE_ID.upper(),
        })

        self.assertEqual(status, 200)
        self.assertTrue(data["pipeline_deleted"])
        self.assertEqual(data["pipeline_id"], 98)
        self.assertIsNone(self.db.get_request(98))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_without_purge_pipeline_leaves_request_intact(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self.db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(beets_cls)

        status, data = self._post("/api/beets/delete", {
            "id": 7,
            "confirm": "DELETE",
        })

        self.assertEqual(status, 200)
        self.assertFalse(data["pipeline_deleted"])
        self.assertIsNone(data["pipeline_id"])
        self.assertIsNotNone(self.db.get_request(42))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purge_with_no_pipeline_context_is_noop(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self._configure_beets_delete_mock(beets_cls)

        status, data = self._post("/api/beets/delete", {
            "id": 7,
            "confirm": "DELETE",
            "purge_pipeline": True,
        })

        self.assertEqual(status, 200)
        self.assertFalse(data["pipeline_deleted"])
        self.assertIsNone(data["pipeline_id"])
        # The setUp-seeded request is untouched — no pipeline context
        # in the body means no purge, even with purge_pipeline=True.
        self.assertIsNotNone(self.db.get_request(42))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purges_discogs_request_by_numeric_release_id_fallback(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self.db.seed_request(make_request_row(
            id=77,
            mb_release_id=None,
            discogs_release_id="12856590",
            status="imported",
        ))
        self._configure_beets_delete_mock(beets_cls)

        status, data = self._post("/api/beets/delete", {
            "id": 7,
            "confirm": "DELETE",
            "purge_pipeline": True,
            "release_id": "12856590",
        })

        self.assertEqual(status, 200)
        self.assertTrue(data["pipeline_deleted"])
        self.assertEqual(data["pipeline_id"], 77)
        self.assertIsNone(self.db.get_request(77))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_pipeline_failure_aborts_before_beets_delete(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self._configure_beets_delete_mock(beets_cls)
        failing_db = _FailingDeleteDB()
        failing_db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))

        with patch.object(self._srv, "db", failing_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
                "purge_pipeline": True,
                "pipeline_id": 42,
                "release_id": self.RELEASE_ID,
            })

        self.assertEqual(status, 500)
        self.assertIn("error", data)
        beets_cls.delete_album.assert_not_called()

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_failure_after_pipeline_purge_returns_targeted_error(
        self,
        beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self.db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(
            beets_cls,
            delete_side_effect=OSError("boom"),
        )

        status, data = self._post("/api/beets/delete", {
            "id": 7,
            "confirm": "DELETE",
            "purge_pipeline": True,
            "pipeline_id": 42,
            "release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 500)
        self.assertIn("Pipeline request was removed", data["error"])
        self.assertIsNone(self.db.get_request(42))

if __name__ == "__main__":
    unittest.main()
