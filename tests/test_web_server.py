#!/usr/bin/env python3
"""Tests for web/server.py HTTP endpoints.

Starts a real HTTP server on a random port with mocked DB,
verifying response codes, JSON structure, and error handling.
"""

import copy
import json
import os
import sys
import threading
import unittest
from http.server import HTTPServer
from unittest.mock import MagicMock, patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "web"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from tests.helpers import make_request_row

_MOCK_PIPELINE_REQUEST = make_request_row(
    id=100, status="imported", min_bitrate=320,
    imported_path="/mnt/virtio/Music/Beets/Test",
)

_DEFAULT_WRONG_MATCH_ROW = {
    "download_log_id": 42,
    "request_id": 100,
    "artist_name": "Test Artist",
    "album_title": "Test Album",
    "mb_release_id": "abc-123",
    "soulseek_username": "testuser",
    "validation_result": {
        "distance": 0.25,
        "scenario": "high_distance",
        "detail": "distance too high",
        "failed_path": "/mnt/virtio/music/slskd/failed_imports/Test",
        "soulseek_username": "testuser",
        "candidates": [{
            "is_target": True,
            "artist": "Test Artist",
            "album": "Test Album",
            "distance": 0.25,
            "distance_breakdown": {"tracks": 0.15, "album": 0.10},
            "track_count": 10,
            "mapping": [],
            "extra_items": [],
            "extra_tracks": [],
        }],
        "items": [{"path": "01 Track.mp3", "title": "Track"}],
    },
}

_DEFAULT_WRONG_MATCH_ENTRY = {
    "id": 42,
    "request_id": 100,
    "validation_result": {
        "failed_path": "/mnt/virtio/music/slskd/failed_imports/Test",
        "scenario": "high_distance",
    },
}


def _make_server():
    """Create a test server with mocked DB on a random port."""
    import web.server as srv
    # Mock the pipeline DB
    mock_db = MagicMock()
    mock_db.get_log.return_value = [
        {
            "id": 1, "request_id": 100, "outcome": "success",
            "beets_scenario": "strong_match", "beets_distance": 0.012,
            "beets_detail": None, "soulseek_username": "testuser",
            "filetype": "mp3", "bitrate": 320000, "was_converted": False,
            "original_filetype": None, "actual_filetype": "mp3",
            "actual_min_bitrate": 320, "slskd_filetype": "mp3",
            "slskd_bitrate": 320000, "spectral_grade": None,
            "spectral_bitrate": None, "existing_min_bitrate": None,
            "existing_spectral_bitrate": None, "valid": True,
            "error_message": None, "staged_path": None,
            "download_path": None, "sample_rate": None,
            "bit_depth": None, "is_vbr": None,
            "import_result": None, "validation_result": None,
            "created_at": "2026-03-30T12:00:00+00:00",
            "album_title": "Test Album", "artist_name": "Test Artist",
            "mb_release_id": "abc-123", "year": 2024,
            "country": "US", "request_status": "imported",
            "request_min_bitrate": 320, "prev_min_bitrate": None,
            "quality_override": None, "source": "request",
        },
    ]
    mock_db._execute.return_value = MagicMock(
        fetchone=MagicMock(return_value={"total": 1, "imported": 1}))
    mock_db.count_by_status.return_value = {"wanted": 0, "imported": 1, "manual": 0}
    mock_db.get_by_status.return_value = []
    mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
    mock_db.get_tracks.return_value = []
    mock_db.get_download_history.return_value = [
        {
            "id": 1, "request_id": 100, "outcome": "success",
            "beets_scenario": "strong_match", "beets_distance": 0.012,
            "soulseek_username": "testuser", "filetype": "mp3",
            "bitrate": 320000, "was_converted": False,
            "actual_filetype": "mp3", "actual_min_bitrate": 320,
            "spectral_grade": None, "spectral_bitrate": None,
            "existing_min_bitrate": None, "existing_spectral_bitrate": None,
            "created_at": "2026-03-30T12:00:00+00:00",
            "error_message": None, "original_filetype": None,
            "slskd_filetype": "mp3", "slskd_bitrate": 320000,
            "beets_detail": None, "valid": True,
            "staged_path": None, "download_path": None,
            "sample_rate": None, "bit_depth": None, "is_vbr": None,
            "import_result": None, "validation_result": None,
        },
    ]

    mock_db.get_wrong_matches.return_value = [copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)]
    mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
    mock_db.clear_wrong_match_path.return_value = True

    srv.db = mock_db
    srv.beets_db_path = None  # No beets DB in tests

    server = HTTPServer(("127.0.0.1", 0), srv.Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port, mock_db


class TestServerEndpoints(unittest.TestCase):
    """Test HTTP endpoints return expected status and structure."""

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port, cls.mock_db = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _get(self, path: str) -> tuple[int, dict]:
        """GET a path and return (status, json)."""
        url = f"{self.base}{path}"
        try:
            resp = urlopen(url)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    def _post(self, path: str, body: dict) -> tuple[int, dict]:
        """POST JSON and return (status, json)."""
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            resp = urlopen(req)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    # --- GET endpoints ---

    def test_index_returns_html(self):
        resp = urlopen(f"{self.base}/")
        self.assertEqual(resp.status, 200)
        self.assertIn("text/html", resp.headers.get("Content-Type", ""))

    def test_pipeline_log_returns_entries(self):
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        self.assertIn("log", data)
        self.assertIn("counts", data)
        self.assertIsInstance(data["log"], list)
        if data["log"]:
            entry = data["log"][0]
            for key in ("badge", "verdict", "summary", "album_title",
                        "artist_name", "outcome"):
                self.assertIn(key, entry, f"Missing key '{key}' in log entry")

    def test_pipeline_log_filter_imported(self):
        status, data = self._get("/api/pipeline/log?outcome=imported")
        self.assertEqual(status, 200)
        self.assertIn("log", data)
        # Verify the DB was called with the filter
        self.mock_db.get_log.assert_called_with(limit=50, outcome_filter="imported")

    def test_pipeline_log_filter_rejected(self):
        status, data = self._get("/api/pipeline/log?outcome=rejected")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=50, outcome_filter="rejected")

    def test_pipeline_log_filter_invalid_ignored(self):
        status, data = self._get("/api/pipeline/log?outcome=badvalue")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=50, outcome_filter=None)

    def test_pipeline_log_counts_structure(self):
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        counts = data["counts"]
        for key in ("all", "imported", "rejected"):
            self.assertIn(key, counts)
            self.assertIsInstance(counts[key], int)

    def test_pipeline_status(self):
        status, data = self._get("/api/pipeline/status")
        self.assertEqual(status, 200)
        self.assertIn("counts", data)
        self.assertIn("wanted", data)

    def test_pipeline_all(self):
        status, data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertIn("counts", data)
        for key in ("wanted", "downloading", "imported", "manual"):
            self.assertIn(key, data)

    def test_pipeline_status_includes_downloading(self):
        """count_by_status includes downloading when albums are downloading."""
        self.mock_db.count_by_status.return_value = {
            "wanted": 3, "downloading": 2, "imported": 10, "manual": 1}
        status, data = self._get("/api/pipeline/status")
        self.assertEqual(status, 200)
        self.assertEqual(data["counts"]["downloading"], 2)
        # Restore
        self.mock_db.count_by_status.return_value = {"wanted": 0, "imported": 1, "manual": 0}

    def test_pipeline_all_includes_downloading(self):
        """get_pipeline_all returns downloading albums in the response."""
        downloading_row = {
            "id": 200, "album_title": "Downloading Album", "artist_name": "DL Artist",
            "mb_release_id": "dl-uuid", "status": "downloading",
            "year": 2024, "country": "US", "format": None,
            "source": "request", "search_filetype_override": None, "target_format": None,
            "created_at": "2026-04-03T12:00:00+00:00",
            "updated_at": "2026-04-03T12:00:00+00:00",
            "min_bitrate": None, "prev_min_bitrate": None,
            "verified_lossless": False, "last_download_spectral_grade": None,
            "last_download_spectral_bitrate": None, "current_spectral_grade": None,
            "current_spectral_bitrate": None,
            "imported_path": None, "reasoning": None,
            "active_download_state": {"filetype": "flac", "enqueued_at": "now", "files": []},
        }
        self.mock_db.get_by_status.side_effect = lambda s: [downloading_row] if s == "downloading" else []
        self.mock_db.count_by_status.return_value = {"downloading": 1}
        self.mock_db.get_download_history_batch.return_value = {}
        status, data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertIn("downloading", data)
        self.assertEqual(len(data["downloading"]), 1)
        self.assertEqual(data["downloading"][0]["album_title"], "Downloading Album")
        # Restore
        self.mock_db.get_by_status.side_effect = None
        self.mock_db.get_by_status.return_value = []
        self.mock_db.count_by_status.return_value = {"wanted": 0, "imported": 1, "manual": 0}

    def test_pipeline_detail(self):
        status, data = self._get("/api/pipeline/100")
        self.assertEqual(status, 200)
        self.assertIn("request", data)
        self.assertIn("history", data)
        self.assertIn("tracks", data)
        # History items should have verdict
        if data["history"]:
            self.assertIn("verdict", data["history"][0])
            self.assertIn("downloaded_label", data["history"][0])

    def test_pipeline_detail_not_found(self):
        self.mock_db.get_request.return_value = None
        status, data = self._get("/api/pipeline/999")
        self.assertEqual(status, 404)
        # Restore
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST

    def test_unknown_get_returns_404(self):
        status, data = self._get("/api/nonexistent")
        self.assertEqual(status, 404)

    # --- POST endpoints ---

    def test_post_pipeline_add_missing_mbid(self):
        status, data = self._post("/api/pipeline/add", {})
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_post_pipeline_delete_missing_id(self):
        status, data = self._post("/api/pipeline/delete", {})
        self.assertEqual(status, 400)

    def test_post_set_intent_success(self):
        """POST /api/pipeline/set-intent returns ok with required fields."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "lossless"})
        self.assertEqual(status, 200)
        for key in ("status", "id", "intent", "target_format", "requeued"):
            self.assertIn(key, data, f"Missing key '{key}' in set-intent response")
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["intent"], "lossless")

    def test_post_set_intent_backward_compat(self):
        """Old 'flac_only' intent is aliased to 'lossless'."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "flac_only"})
        self.assertEqual(status, 200)
        self.assertEqual(data["intent"], "lossless")

    @patch("routes.pipeline.resolve_failed_path", return_value="/tmp/Test Album")
    @patch("lib.import_dispatch.dispatch_import_from_db")
    def test_post_force_import_passes_source_username(self, mock_dispatch, _mock_resolve):
        self.mock_db.get_download_log_entry.return_value = {
            "id": 42,
            "request_id": 100,
            "soulseek_username": "baduser",
            "validation_result": {
                "failed_path": "/tmp/Test Album",
                "scenario": "high_distance",
            },
        }
        mock_dispatch.return_value = MagicMock(success=True, message="Import successful")

        status, data = self._post("/api/pipeline/force-import", {"download_log_id": 42})

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["artist"], _MOCK_PIPELINE_REQUEST["artist_name"])
        self.assertEqual(data["album"], _MOCK_PIPELINE_REQUEST["album_title"])
        mock_dispatch.assert_called_once_with(
            self.mock_db,
            request_id=100,
            failed_path="/tmp/Test Album",
            force=True,
            outcome_label="force_import",
            source_username="baduser",
        )

    def test_post_set_intent_default_clears_stale_lossless_override(self):
        self.mock_db.get_request.return_value = make_request_row(
            id=100, status="wanted", artist_name="Test Artist",
            album_title="Test Album", target_format="lossless",
            search_filetype_override="lossless",
        )
        self.mock_db.update_request_fields.reset_mock()
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "default"})
        self.assertEqual(status, 200)
        self.assertFalse(data["requeued"])
        self.mock_db.update_request_fields.assert_called_once_with(
            100, target_format=None, search_filetype_override=None)
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST

    def test_post_set_intent_invalid(self):
        """POST /api/pipeline/set-intent with bad intent returns 400."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "garbage"})
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_post_set_intent_missing_id(self):
        """POST /api/pipeline/set-intent without id returns 400."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"intent": "lossless"})
        self.assertEqual(status, 400)

    def test_unknown_post_returns_404(self):
        status, data = self._post("/api/nonexistent", {})
        self.assertEqual(status, 404)

    # --- datetime serialization ---

    def test_log_entries_have_string_dates(self):
        """Datetime fields should be serialized to strings, not objects."""
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        if data["log"]:
            created = data["log"][0].get("created_at")
            self.assertIsInstance(created, str)
            self.assertIn("2026", created)


    def test_disambiguate_endpoint(self):
        """Disambiguate endpoint returns releases with unique track info."""
        fake_releases = [
            {
                "id": "rel-1",
                "title": "Album",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-1",
                    "title": "Album",
                    "primary-type": "Album",
                    "secondary-types": [],
                },
                "media": [{
                    "position": 1,
                    "format": "CD",
                    "track-count": 2,
                    "tracks": [
                        {"position": 1, "number": "1", "title": "Track A",
                         "recording": {"id": "rec-1", "title": "Track A"}},
                        {"position": 2, "number": "2", "title": "Track B",
                         "recording": {"id": "rec-2", "title": "Track B"}},
                    ],
                }],
            },
            {
                "id": "rel-2",
                "title": "Single",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-2",
                    "title": "Single",
                    "primary-type": "Single",
                    "secondary-types": [],
                },
                "media": [{
                    "position": 1,
                    "format": "CD",
                    "track-count": 2,
                    "tracks": [
                        {"position": 1, "number": "1", "title": "Track A",
                         "recording": {"id": "rec-1", "title": "Track A"}},
                        {"position": 2, "number": "2", "title": "B-side",
                         "recording": {"id": "rec-3", "title": "B-side"}},
                    ],
                }],
            },
        ]
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_artist_releases_with_recordings.return_value = fake_releases
            mock_mb.get_artist_name.return_value = "Test Artist"
            status, data = self._get("/api/artist/664c3e0e-42d8-48c1-b209-1efca19c0325/disambiguate")

        self.assertEqual(status, 200)
        self.assertEqual(data["artist_name"], "Test Artist")
        rgs = data["release_groups"]
        self.assertEqual(len(rgs), 2)

        # Album (tier 1) has 2 unique, Single's Track A is covered by Album
        album_rg = [rg for rg in rgs if rg["release_group_id"] == "rg-1"][0]
        single_rg = [rg for rg in rgs if rg["release_group_id"] == "rg-2"][0]
        self.assertEqual(album_rg["unique_track_count"], 2)
        self.assertEqual(single_rg["unique_track_count"], 1)

        # B-side is unique, Track A on single is covered by album
        bside = [t for t in single_rg["tracks"] if t["title"] == "B-side"][0]
        self.assertTrue(bside["unique"])
        track_a = [t for t in single_rg["tracks"] if t["title"] == "Track A"][0]
        self.assertFalse(track_a["unique"])

        # Pressings should be present with recording_ids
        self.assertEqual(len(album_rg["pressings"]), 1)
        self.assertEqual(album_rg["pressings"][0]["release_id"], "rel-1")
        self.assertIn("rec-1", album_rg["pressings"][0]["recording_ids"])

    def test_disambiguate_filters_live(self):
        """Disambiguate endpoint filters out live releases."""
        fake_releases = [
            {
                "id": "rel-1",
                "title": "Studio",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-1",
                    "title": "Studio",
                    "primary-type": "Album",
                    "secondary-types": [],
                },
                "media": [{"position": 1, "format": "CD", "track-count": 1,
                           "tracks": [{"position": 1, "number": "1", "title": "Song",
                                       "recording": {"id": "rec-1", "title": "Song"}}]}],
            },
            {
                "id": "rel-2",
                "title": "Live Album",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-2",
                    "title": "Live",
                    "primary-type": "Album",
                    "secondary-types": ["Live"],
                },
                "media": [{"position": 1, "format": "CD", "track-count": 1,
                           "tracks": [{"position": 1, "number": "1", "title": "Song Live",
                                       "recording": {"id": "rec-2", "title": "Song Live"}}]}],
            },
        ]
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_artist_releases_with_recordings.return_value = fake_releases
            mock_mb.get_artist_name.return_value = "Test Artist"
            status, data = self._get("/api/artist/664c3e0e-42d8-48c1-b209-1efca19c0325/disambiguate")

        self.assertEqual(status, 200)
        self.assertEqual(len(data["release_groups"]), 1)
        self.assertEqual(data["release_groups"][0]["title"], "Studio")


class TestApplyPipelineBitrateOverride(unittest.TestCase):
    """Test the apply_pipeline_bitrate_override helper."""

    def _apply(self, album, pipeline_info):
        from web.server import apply_pipeline_bitrate_override
        apply_pipeline_bitrate_override(album, pipeline_info)

    def test_pipeline_higher_overrides_beets(self):
        album = {"min_bitrate": 192000}
        self._apply(album, {"min_bitrate": 320})
        self.assertEqual(album["min_bitrate"], 320000)

    def test_pipeline_lower_no_override(self):
        album = {"min_bitrate": 320000}
        self._apply(album, {"min_bitrate": 192})
        self.assertEqual(album["min_bitrate"], 320000)

    def test_pipeline_none_no_change(self):
        album = {"min_bitrate": 192000}
        self._apply(album, {"min_bitrate": None})
        self.assertEqual(album["min_bitrate"], 192000)

    def test_beets_none_no_change(self):
        album = {"min_bitrate": None}
        self._apply(album, {"min_bitrate": 320})
        self.assertIsNone(album["min_bitrate"])

    def test_upgrade_queued_flag_set(self):
        album = {}
        self._apply(album, {"status": "wanted", "search_filetype_override": "flac,mp3 v0"})
        self.assertTrue(album.get("upgrade_queued"))

    def test_no_upgrade_queued_when_imported(self):
        album = {}
        self._apply(album, {"status": "imported", "search_filetype_override": "flac"})
        self.assertNotIn("upgrade_queued", album)


class TestWrongMatchesContract(unittest.TestCase):
    """Contract tests: /api/wrong-matches returns all fields the frontend needs."""

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port, cls.mock_db = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _get(self, path: str) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        try:
            resp = urlopen(url)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    def _post(self, path: str, body: dict) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            resp = urlopen(req)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    def setUp(self) -> None:
        self.mock_db.get_wrong_matches.return_value = [copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)]
        self.mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
        self.mock_db.clear_wrong_match_path.reset_mock()

    REQUIRED_FIELDS = {
        "download_log_id", "request_id", "artist", "album", "mb_release_id",
        "failed_path", "files_exist", "distance", "scenario", "detail",
        "soulseek_username", "candidate", "local_items", "in_library",
    }

    def test_response_has_entries_with_required_fields(self):
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertIn("entries", data)
        entries = data["entries"]
        self.assertGreater(len(entries), 0)
        for entry in entries:
            missing = self.REQUIRED_FIELDS - set(entry.keys())
            self.assertFalse(missing, f"Missing fields: {missing}")

    def test_candidate_has_distance_breakdown(self):
        status, data = self._get("/api/wrong-matches")
        entry = data["entries"][0]
        candidate = entry["candidate"]
        self.assertIsNotNone(candidate)
        self.assertIn("distance_breakdown", candidate)
        self.assertIn("mapping", candidate)

    def test_delete_missing_id_returns_error(self):
        status, data = self._post("/api/wrong-matches/delete", {})
        self.assertEqual(status, 400)

    def test_delete_returns_ok(self):
        status, data = self._post("/api/wrong-matches/delete", {"download_log_id": 42})
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")

    @patch("routes.imports.resolve_failed_path", return_value="/mnt/virtio/music/slskd/failed_imports/Test")
    def test_relative_failed_path_uses_resolved_path(self, _mock_resolve):
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["validation_result"]["failed_path"] = "failed_imports/Test"
        self.mock_db.get_wrong_matches.return_value = [row]

        status, data = self._get("/api/wrong-matches")

        self.assertEqual(status, 200)
        entry = data["entries"][0]
        self.assertTrue(entry["files_exist"])
        self.assertEqual(entry["failed_path"], "/mnt/virtio/music/slskd/failed_imports/Test")

    @patch("routes.imports.shutil.rmtree")
    @patch("routes.imports.resolve_failed_path", return_value="/mnt/virtio/music/slskd/failed_imports/Test")
    def test_delete_relative_failed_path_removes_resolved_directory(self, _mock_resolve, mock_rmtree):
        entry = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
        entry["validation_result"]["failed_path"] = "failed_imports/Test"
        self.mock_db.get_download_log_entry.return_value = entry

        status, data = self._post("/api/wrong-matches/delete", {"download_log_id": 42})

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        mock_rmtree.assert_called_once_with("/mnt/virtio/music/slskd/failed_imports/Test")

    def test_entries_in_beets_still_shown(self):
        """Wrong matches should appear even if the album is already in beets."""
        status, data = self._get("/api/wrong-matches")

        self.assertEqual(status, 200)
        self.assertGreater(len(data["entries"]), 0)


class TestLibraryArtistContract(unittest.TestCase):
    """Contract tests: get_library_artist() returns all fields the frontend needs."""

    @classmethod
    def setUpClass(cls):
        import sqlite3
        import tempfile
        cls._tmpdir = tempfile.mkdtemp()
        cls._db_path = os.path.join(cls._tmpdir, "beets.db")
        conn = sqlite3.connect(cls._db_path)
        conn.executescript("""
            CREATE TABLE albums (
                id INTEGER PRIMARY KEY,
                album TEXT, albumartist TEXT, year INTEGER,
                mb_albumid TEXT, discogs_albumid TEXT,
                mb_albumartistid TEXT, mb_albumartistids TEXT,
                mb_releasegroupid TEXT, release_group_title TEXT,
                added REAL, albumtype TEXT, label TEXT, country TEXT,
                format TEXT, artpath BLOB
            );
            CREATE TABLE items (
                id INTEGER PRIMARY KEY, album_id INTEGER,
                bitrate INTEGER, path BLOB, title TEXT, artist TEXT,
                track INTEGER, disc INTEGER, length REAL, format TEXT,
                samplerate INTEGER, bitdepth INTEGER
            );
            INSERT INTO albums (id, album, albumartist, year, mb_albumid,
                mb_albumartistid, mb_releasegroupid, release_group_title,
                added, albumtype, label, country)
            VALUES (1, 'Tallahassee', 'The Mountain Goats', 2002,
                'aaaa-bbbb-cccc', 'dddd-eeee-ffff',
                '1111-2222-3333', 'Tallahassee',
                1773651901.0, 'album', '4AD', 'US');
            INSERT INTO albums (id, album, albumartist, year, mb_albumid,
                mb_albumartistid, mb_releasegroupid, release_group_title,
                added, albumtype, label, country)
            VALUES (2, 'Tallahassee (Deluxe)', 'The Mountain Goats', 2002,
                'xxxx-yyyy-zzzz', 'dddd-eeee-ffff',
                '1111-2222-3333', 'Tallahassee',
                1773651902.0, 'album', '4AD', 'US');
            INSERT INTO items (album_id, bitrate, path, format)
                VALUES (1, 245000, X'2F612F622E6D7033', 'MP3');
            INSERT INTO items (album_id, bitrate, path, format)
                VALUES (2, 320000, X'2F612F632E6D7033', 'MP3');
        """)
        conn.close()

        # Patch the beets DB into server module
        import web.server as srv
        from lib.beets_db import BeetsDB
        cls._beets = BeetsDB(cls._db_path)
        cls._orig_beets = srv._beets
        srv._beets = cls._beets

    @classmethod
    def tearDownClass(cls):
        import web.server as srv
        srv._beets = cls._orig_beets
        import shutil
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    # Fields the frontend (library.js, discography.js) requires for rendering.
    # These must match _album_row_to_dict() output — the single source of truth.
    REQUIRED_FIELDS = {
        "id", "album", "artist", "year", "mb_albumid", "track_count",
        "mb_releasegroupid", "release_group_title", "added",
        "formats", "min_bitrate", "type", "label", "country", "source",
    }

    def test_response_has_all_required_fields(self):
        """Every album dict must include all fields the frontend JS uses."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats", "dddd-eeee-ffff")
        self.assertEqual(len(albums), 2)
        for album in albums:
            missing = self.REQUIRED_FIELDS - set(album.keys())
            self.assertFalse(missing,
                f"Album '{album.get('album')}' missing fields: {missing}")

    def test_release_group_fields_populated(self):
        """mb_releasegroupid and release_group_title must have actual values."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats", "dddd-eeee-ffff")
        for album in albums:
            self.assertIsNotNone(album["mb_releasegroupid"])
            self.assertNotEqual(album["mb_releasegroupid"], "")
            self.assertIsNotNone(album["release_group_title"])

    def test_releases_group_by_release_group_id(self):
        """Two pressings of same release group should share the same rgid."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats", "dddd-eeee-ffff")
        rg_ids = {a["mb_releasegroupid"] for a in albums}
        self.assertEqual(len(rg_ids), 1, "Both pressings should share one release group")
        self.assertEqual(rg_ids.pop(), "1111-2222-3333")

    def test_name_only_lookup(self):
        """Lookup by name only (no mbid) also returns all required fields."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats")
        self.assertGreater(len(albums), 0)
        for album in albums:
            missing = self.REQUIRED_FIELDS - set(album.keys())
            self.assertFalse(missing,
                f"Album '{album.get('album')}' missing fields: {missing}")


if __name__ == "__main__":
    unittest.main()
