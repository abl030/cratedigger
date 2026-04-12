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

from lib.manual_import import FolderInfo, FolderMatch, ImportRequest
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


def _assert_required_fields(
    case: unittest.TestCase,
    payload: dict,
    required_fields: set[str],
    label: str,
) -> None:
    missing = required_fields - set(payload.keys())
    case.assertFalse(missing, f"{label} missing fields: {missing}")


class _WebServerCase(unittest.TestCase):
    """Shared HTTP test harness for endpoint contract tests."""

    server: HTTPServer
    port: int
    base: str
    mock_db: MagicMock

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
            "search_filetype_override": None, "source": "request",
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
        downloading_row = make_request_row(
            id=200, album_title="Downloading Album", artist_name="DL Artist",
            mb_release_id="dl-uuid", status="downloading",
            active_download_state={"filetype": "flac", "enqueued_at": "now", "files": []},
        )
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


class TestRouteContractAudit(unittest.TestCase):
    """Every web/routes.py endpoint must be covered by a frontend contract decision."""

    CLASSIFIED_ROUTES = {
        "/api/search",
        "/api/library/artist",
        r"^/api/artist/([a-f0-9-]+)$",
        r"^/api/artist/([a-f0-9-]+)/disambiguate$",
        r"^/api/release-group/([a-f0-9-]+)$",
        r"^/api/release/([a-f0-9-]+)$",
        "/api/discogs/search",
        r"^/api/discogs/artist/(\d+)$",
        r"^/api/discogs/master/(\d+)$",
        r"^/api/discogs/release/(\d+)$",
        "/api/pipeline/log",
        "/api/pipeline/status",
        "/api/pipeline/recent",
        "/api/pipeline/all",
        "/api/pipeline/constants",
        "/api/pipeline/simulate",
        r"^/api/pipeline/(\d+)$",
        "/api/pipeline/add",
        "/api/pipeline/update",
        "/api/pipeline/upgrade",
        "/api/pipeline/set-quality",
        "/api/pipeline/set-intent",
        "/api/pipeline/ban-source",
        "/api/pipeline/force-import",
        "/api/pipeline/delete",
        "/api/beets/search",
        "/api/beets/recent",
        r"^/api/beets/album/(\d+)$",
        "/api/beets/delete",
        "/api/manual-import/scan",
        "/api/manual-import/import",
        "/api/wrong-matches",
        "/api/wrong-matches/delete",
    }

    def test_all_web_routes_are_classified_for_contract_coverage(self):
        import web.server as srv

        actual = set(srv.Handler._FUNC_GET_ROUTES)
        actual.update(srv.Handler._FUNC_POST_ROUTES)
        actual.update(pattern.pattern for pattern, _fn in srv.Handler._FUNC_GET_PATTERNS)

        self.assertFalse(actual - self.CLASSIFIED_ROUTES,
                         f"Unclassified web routes: {sorted(actual - self.CLASSIFIED_ROUTES)}")
        self.assertFalse(self.CLASSIFIED_ROUTES - actual,
                         f"Stale route classifications: {sorted(self.CLASSIFIED_ROUTES - actual)}")


class TestPipelineRouteContracts(_WebServerCase):
    """Contract tests for frontend-consumed pipeline GET routes."""

    PIPELINE_ITEM_REQUIRED_FIELDS = {
        "id", "artist_name", "album_title", "year", "format", "country",
        "source", "created_at", "status", "search_attempts",
        "download_attempts", "validation_attempts", "beets_distance",
        "mb_release_id", "imported_path", "current_spectral_bitrate",
        "last_download_spectral_bitrate", "current_spectral_grade",
        "last_download_spectral_grade", "verified_lossless",
    }
    LOG_ENTRY_REQUIRED_FIELDS = {
        "id", "request_id", "outcome", "album_title", "artist_name",
        "created_at", "badge", "badge_class", "border_color", "summary",
        "verdict", "in_beets",
    }
    HISTORY_REQUIRED_FIELDS = {
        "id", "request_id", "outcome", "created_at", "soulseek_username",
        "downloaded_label", "verdict", "beets_scenario", "beets_distance",
        "spectral_grade", "spectral_bitrate", "existing_min_bitrate",
        "existing_spectral_bitrate",
    }
    STATUS_WANTED_REQUIRED_FIELDS = {
        "id", "artist", "album", "mb_release_id", "source", "created_at",
    }
    RECENT_REQUIRED_FIELDS = (
        PIPELINE_ITEM_REQUIRED_FIELDS | {"pipeline_tracks", "in_beets", "beets_tracks"}
    )
    CONSTANTS_REQUIRED_FIELDS = {"constants", "paths", "path_labels", "stages"}
    STAGE_REQUIRED_FIELDS = {
        "id", "title", "path", "function", "when", "inputs", "rules",
    }
    SIMULATE_REQUIRED_FIELDS = {
        "stage1_spectral", "stage2_import", "stage3_quality_gate",
        "final_status", "imported", "denylisted", "keep_searching",
        "target_final_format",
    }

    def setUp(self) -> None:
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
        self.mock_db.get_tracks.return_value = [
            {"disc_number": 1, "track_number": 1, "title": "Track", "length_seconds": 180},
        ]
        self.mock_db.get_wanted.return_value = [
            make_request_row(id=101, status="wanted", source="request"),
        ]
        self.mock_db.count_by_status.return_value = {
            "wanted": 1, "downloading": 0, "imported": 1, "manual": 0,
        }
        self.mock_db.get_by_status.side_effect = None
        self.mock_db.get_by_status.return_value = []
        self.mock_db.get_download_history_batch.return_value = {}
        self.mock_db.get_recent.return_value = []
        self.mock_db.get_track_counts.return_value = {}

    def test_pipeline_log_contract(self):
        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"log", "counts"}, "pipeline log response")
        _assert_required_fields(self, data["log"][0], self.LOG_ENTRY_REQUIRED_FIELDS,
                                "pipeline log entry")
        _assert_required_fields(self, data["counts"], {"all", "imported", "rejected"},
                                "pipeline log counts")

    def test_pipeline_status_contract(self):
        status, data = self._get("/api/pipeline/status")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"counts", "wanted"}, "pipeline status response")
        _assert_required_fields(self, data["wanted"][0], self.STATUS_WANTED_REQUIRED_FIELDS,
                                "pipeline status wanted item")

    def test_pipeline_all_contract(self):
        row = make_request_row(id=201, status="wanted", album_title="Wanted Album")
        self.mock_db.get_by_status.side_effect = lambda s: [row] if s == "wanted" else []

        status, data = self._get("/api/pipeline/all")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"counts", "wanted", "downloading", "imported", "manual"},
                                "pipeline all response")
        _assert_required_fields(self, data["wanted"][0], self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline all item")

    def test_pipeline_detail_contract(self):
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"request", "history", "tracks"},
                                "pipeline detail response")
        _assert_required_fields(self, data["request"], self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline detail request")
        _assert_required_fields(self, data["history"][0], self.HISTORY_REQUIRED_FIELDS,
                                "pipeline detail history item")

    def test_pipeline_recent_contract(self):
        row = make_request_row(id=202, status="imported", album_title="Recent Album")
        history = copy.deepcopy(self.mock_db.get_download_history.return_value[0])
        self.mock_db.get_recent.return_value = [row]
        self.mock_db.get_track_counts.return_value = {202: 11}
        self.mock_db.get_download_history_batch.return_value = {202: [history]}

        status, data = self._get("/api/pipeline/recent")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"recent"}, "pipeline recent response")
        _assert_required_fields(self, data["recent"][0], self.RECENT_REQUIRED_FIELDS,
                                "pipeline recent item")

    def test_pipeline_constants_contract(self):
        status, data = self._get("/api/pipeline/constants")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.CONSTANTS_REQUIRED_FIELDS,
                                "pipeline constants response")
        _assert_required_fields(self, data["stages"][0], self.STAGE_REQUIRED_FIELDS,
                                "pipeline constants stage")
        # Issue #60: rank config surfaced to UI for the Decisions tab.
        # Issue #68: within_rank_tolerance_kbps joins gate_min_rank and
        # bitrate_metric as the third rank policy field the UI renders as
        # a labeled badge at the top of the Decisions tab.
        self.assertIn("rank_gate_min_rank", data["constants"])
        self.assertIn("rank_bitrate_metric", data["constants"])
        self.assertIn("rank_within_tolerance_kbps", data["constants"])
        # Pin the type so the frontend can display it without conversion.
        self.assertIsInstance(
            data["constants"]["rank_within_tolerance_kbps"], int)

    def test_pipeline_simulate_contract(self):
        status, data = self._get(
            "/api/pipeline/simulate?is_flac=false&min_bitrate=320&is_cbr=true"
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SIMULATE_REQUIRED_FIELDS,
                                "pipeline simulate response")

    @patch("routes.pipeline.full_pipeline_decision")
    def test_pipeline_simulate_threads_target_format(self, mock_simulate):
        mock_simulate.return_value = {
            "stage1_spectral": None,
            "stage2_import": "import",
            "stage3_quality_gate": "accept",
            "final_status": "imported",
            "imported": True,
            "denylisted": False,
            "keep_searching": False,
            "target_final_format": "flac",
        }

        status, _data = self._get(
            "/api/pipeline/simulate?is_flac=true&min_bitrate=900&target_format=flac"
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            mock_simulate.call_args.kwargs["target_format"], "flac")


class TestPipelineMutationRouteContracts(_WebServerCase):
    """Contract tests for frontend-consumed pipeline mutation routes."""

    ADD_REQUIRED_FIELDS = {"status", "id", "artist", "album", "tracks"}
    EXISTS_REQUIRED_FIELDS = {"status", "id", "current_status"}
    UPDATE_REQUIRED_FIELDS = {"status", "id", "new_status"}
    UPGRADE_REQUIRED_FIELDS = {
        "status", "id", "min_bitrate", "search_filetype_override",
    }
    SET_QUALITY_REQUIRED_FIELDS = {"status", "id", "new_status", "min_bitrate"}
    SET_INTENT_REQUIRED_FIELDS = {
        "status", "id", "intent", "target_format", "requeued",
    }
    BAN_SOURCE_REQUIRED_FIELDS = {"status", "username", "beets_removed"}
    FORCE_IMPORT_REQUIRED_FIELDS = {
        "status", "request_id", "artist", "album", "message",
    }
    DELETE_REQUIRED_FIELDS = {"status", "id"}

    def setUp(self) -> None:
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.add_request.return_value = 501
        self.mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)

    @patch("routes.pipeline.mb_api.get_release")
    def test_pipeline_add_contract(self, mock_get_release):
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "artist-1",
            "artist_name": "Test Artist",
            "title": "Test Album",
            "year": 2024,
            "country": "US",
            "tracks": [{"title": "Track"}],
        }

        status, data = self._post("/api/pipeline/add", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add response")

    def test_pipeline_add_exists_contract(self):
        self.mock_db.get_request_by_mb_release_id.return_value = {
            "id": 502,
            "status": "wanted",
        }

        status, data = self._post("/api/pipeline/add", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.EXISTS_REQUIRED_FIELDS,
                                "pipeline add exists response")

    @patch("routes.pipeline.discogs_api.get_release")
    def test_pipeline_add_discogs_contract(self, mock_get_release):
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        mock_get_release.return_value = {
            "artist_id": "3840",
            "artist_name": "Radiohead",
            "title": "OK Computer",
            "year": 1997,
            "country": "Europe",
            "tracks": [{"title": "Airbag"}],
        }

        status, data = self._post("/api/pipeline/add", {"discogs_release_id": "83182"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add discogs response")
        # Verify both columns populated
        add_call = self.mock_db.add_request.call_args
        self.assertEqual(add_call.kwargs["mb_release_id"], "83182")
        self.assertEqual(add_call.kwargs["discogs_release_id"], "83182")

    def test_pipeline_add_discogs_exists_contract(self):
        self.mock_db.get_request_by_discogs_release_id.return_value = {
            "id": 503,
            "status": "imported",
        }

        status, data = self._post("/api/pipeline/add", {"discogs_release_id": "83182"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.EXISTS_REQUIRED_FIELDS,
                                "pipeline add discogs exists response")

    @patch("routes.pipeline.apply_transition")
    def test_pipeline_update_contract(self, _mock_transition):
        status, data = self._post("/api/pipeline/update", {"id": 100, "status": "manual"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPDATE_REQUIRED_FIELDS,
                                "pipeline update response")

    @patch("routes.pipeline.apply_transition")
    def test_pipeline_upgrade_contract(self, _mock_transition):
        self.mock_db.get_request_by_mb_release_id.return_value = _MOCK_PIPELINE_REQUEST

        status, data = self._post("/api/pipeline/upgrade", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response")

    @patch("routes.pipeline.apply_transition")
    def test_pipeline_set_quality_contract(self, _mock_transition):
        self.mock_db.get_request_by_mb_release_id.return_value = _MOCK_PIPELINE_REQUEST

        status, data = self._post(
            "/api/pipeline/set-quality",
            {"mb_release_id": "abc-123", "status": "manual", "min_bitrate": 245},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_QUALITY_REQUIRED_FIELDS,
                                "pipeline set-quality response")

    def test_pipeline_set_intent_contract(self):
        self.mock_db.get_request.return_value = make_request_row(id=100, status="wanted")

        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "lossless"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_INTENT_REQUIRED_FIELDS,
                                "pipeline set-intent response")

    @patch("routes.pipeline.apply_transition")
    def test_pipeline_ban_source_contract(self, _mock_transition):
        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 100, "username": "baduser", "mb_release_id": "abc-123"},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.BAN_SOURCE_REQUIRED_FIELDS,
                                "pipeline ban-source response")

    @patch("routes.pipeline.resolve_failed_path", return_value="/tmp/Test Album")
    @patch("lib.import_dispatch.dispatch_import_from_db")
    def test_pipeline_force_import_contract(self, mock_dispatch, _mock_resolve):
        mock_dispatch.return_value = MagicMock(success=True, message="Import successful")

        status, data = self._post("/api/pipeline/force-import", {"download_log_id": 42})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.FORCE_IMPORT_REQUIRED_FIELDS,
                                "pipeline force-import response")

    def test_pipeline_delete_contract(self):
        status, data = self._post("/api/pipeline/delete", {"id": 100})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DELETE_REQUIRED_FIELDS,
                                "pipeline delete response")


class TestManualImportRouteContracts(_WebServerCase):
    """Contract tests for manual import routes."""

    FOLDER_REQUIRED_FIELDS = {"name", "path", "artist", "album", "file_count", "match"}
    MATCH_REQUIRED_FIELDS = {"request_id", "artist", "album", "mb_release_id", "score"}
    IMPORT_REQUIRED_FIELDS = {"status", "message", "request_id", "artist", "album"}

    def setUp(self) -> None:
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
        self.mock_db.get_by_status.side_effect = None

    @patch("routes.imports.match_folders_to_requests")
    @patch("routes.imports.scan_complete_folder")
    def test_manual_import_scan_contract(self, mock_scan, mock_match):
        folder = FolderInfo(
            name="Test Artist - Test Album",
            path="/complete/Test Artist - Test Album",
            artist="Test Artist",
            album="Test Album",
            file_count=10,
        )
        request = ImportRequest(
            id=100,
            artist_name="Test Artist",
            album_title="Test Album",
            mb_release_id="abc-123",
        )
        mock_scan.return_value = [folder]
        mock_match.return_value = [FolderMatch(folder=folder, request=request, score=0.91)]
        self.mock_db.get_by_status.return_value = [
            make_request_row(id=100, status="wanted", mb_release_id="abc-123"),
        ]

        status, data = self._get("/api/manual-import/scan")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"folders", "wanted_count"},
                                "manual import scan response")
        _assert_required_fields(self, data["folders"][0], self.FOLDER_REQUIRED_FIELDS,
                                "manual import folder")
        _assert_required_fields(self, data["folders"][0]["match"], self.MATCH_REQUIRED_FIELDS,
                                "manual import match")

    @patch("lib.import_dispatch.dispatch_import_from_db")
    def test_manual_import_post_contract(self, mock_dispatch):
        mock_dispatch.return_value = MagicMock(success=True, message="Imported")

        status, data = self._post(
            "/api/manual-import/import",
            {"request_id": 100, "path": "/complete/Test Artist - Test Album"},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.IMPORT_REQUIRED_FIELDS,
                                "manual import response")


class TestBrowseRouteContracts(_WebServerCase):
    """Contract tests for browse and MusicBrainz-backed routes."""

    ARTIST_SEARCH_REQUIRED_FIELDS = {"id", "name", "disambiguation"}
    RELEASE_SEARCH_REQUIRED_FIELDS = {
        "id", "title", "artist_id", "artist_name", "primary_type",
    }
    ARTIST_RG_REQUIRED_FIELDS = {
        "id", "title", "type", "secondary_types", "first_release_date",
        "artist_credit", "primary_artist_id", "has_official",
    }
    LIBRARY_ALBUM_REQUIRED_FIELDS = {
        "id", "album", "artist", "year", "mb_albumid", "track_count",
        "mb_releasegroupid", "release_group_title", "added", "formats",
        "min_bitrate", "type", "label", "country", "source",
    }
    RELEASE_GROUP_REQUIRED_FIELDS = {
        "id", "title", "country", "date", "format", "track_count", "status",
        "in_library", "pipeline_status", "pipeline_id",
    }
    RELEASE_DETAIL_REQUIRED_FIELDS = {
        "id", "title", "tracks", "in_library", "pipeline_status", "pipeline_id",
    }
    RELEASE_TRACK_REQUIRED_FIELDS = {
        "disc_number", "track_number", "title", "length_seconds",
    }
    DISAMBIGUATE_RESPONSE_REQUIRED_FIELDS = {
        "artist_id", "artist_name", "release_groups",
    }
    DISAMBIGUATE_RG_REQUIRED_FIELDS = {
        "release_group_id", "title", "primary_type", "first_date",
        "release_ids", "pressings", "track_count", "unique_track_count",
        "covered_by", "library_status", "pipeline_status", "pipeline_id",
        "tracks",
    }
    DISAMBIGUATE_PRESSING_REQUIRED_FIELDS = {
        "release_id", "title", "date", "format", "track_count", "country",
        "recording_ids", "in_library", "beets_album_id", "pipeline_status",
        "pipeline_id",
    }
    DISAMBIGUATE_TRACK_REQUIRED_FIELDS = {
        "recording_id", "title", "unique", "also_on",
    }

    ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"
    RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    RG_ID = "11111111-1111-1111-1111-111111111111"

    def test_artist_search_contract(self):
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Test Artist", "disambiguation": ""},
            ]
            status, data = self._get("/api/search?q=test")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"artists"}, "artist search response")
        _assert_required_fields(self, data["artists"][0], self.ARTIST_SEARCH_REQUIRED_FIELDS,
                                "artist search result")

    def test_release_search_contract(self):
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.search_release_groups.return_value = [
                {
                    "id": self.RG_ID,
                    "title": "Test Album",
                    "artist_id": self.ARTIST_ID,
                    "artist_name": "Test Artist",
                    "primary_type": "Album",
                },
            ]
            status, data = self._get("/api/search?q=test&type=release")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"release_groups"}, "release search response")
        _assert_required_fields(self, data["release_groups"][0],
                                self.RELEASE_SEARCH_REQUIRED_FIELDS,
                                "release search result")

    def test_library_artist_route_contract(self):
        album = {
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
        with patch("web.server.get_library_artist", return_value=[album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"albums"}, "library artist response")
        _assert_required_fields(self, data["albums"][0], self.LIBRARY_ALBUM_REQUIRED_FIELDS,
                                "library artist album")

    def test_artist_release_groups_contract(self):
        release_group = {
            "id": self.RG_ID,
            "title": "Test Album",
            "type": "Album",
            "secondary_types": [],
            "first_release_date": "2024-01-01",
            "artist_credit": "Test Artist",
            "primary_artist_id": self.ARTIST_ID,
        }
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_artist_release_groups.return_value = [release_group]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            status, data = self._get(f"/api/artist/{self.ARTIST_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"release_groups"}, "artist response")
        _assert_required_fields(self, data["release_groups"][0], self.ARTIST_RG_REQUIRED_FIELDS,
                                "artist release group")

    def test_release_group_contract(self):
        release = {
            "id": self.RELEASE_ID,
            "title": "Test Album",
            "country": "US",
            "date": "2024-01-01",
            "format": "CD",
            "track_count": 10,
            "status": "Official",
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={self.RELEASE_ID}), \
                patch("web.server.check_pipeline",
                      return_value={self.RELEASE_ID: {"id": 42, "status": "wanted"}}):
            mock_mb.get_release_group_releases.return_value = {"releases": [release]}
            status, data = self._get(f"/api/release-group/{self.RG_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"releases"}, "release group response")
        _assert_required_fields(self, data["releases"][0], self.RELEASE_GROUP_REQUIRED_FIELDS,
                                "release group release")

    def test_release_detail_contract(self):
        release = {
            "id": self.RELEASE_ID,
            "title": "Test Album",
            "tracks": [
                {
                    "disc_number": 1,
                    "track_number": 1,
                    "title": "Track",
                    "length_seconds": 180,
                },
            ],
        }
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=42, status="wanted", mb_release_id=self.RELEASE_ID,
        )
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()):
            mock_mb.get_release.return_value = release
            status, data = self._get(f"/api/release/{self.RELEASE_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.RELEASE_DETAIL_REQUIRED_FIELDS,
                                "release detail response")
        _assert_required_fields(self, data["tracks"][0], self.RELEASE_TRACK_REQUIRED_FIELDS,
                                "release detail track")

    def test_artist_disambiguate_contract(self):
        fake_releases = [
            {
                "id": self.RELEASE_ID,
                "title": "Test Album",
                "date": "2024-01-01",
                "country": "US",
                "status": "Official",
                "release-group": {
                    "id": self.RG_ID,
                    "title": "Test Album",
                    "primary-type": "Album",
                    "secondary-types": [],
                },
                "media": [{
                    "position": 1,
                    "format": "CD",
                    "track-count": 1,
                    "tracks": [
                        {"position": 1, "number": "1", "title": "Track",
                         "recording": {"id": "rec-1", "title": "Track"}},
                    ],
                }],
            },
        ]
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = fake_releases
            mock_mb.get_artist_name.return_value = "Test Artist"
            status, data = self._get(f"/api/artist/{self.ARTIST_ID}/disambiguate")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DISAMBIGUATE_RESPONSE_REQUIRED_FIELDS,
                                "disambiguate response")
        rg = data["release_groups"][0]
        _assert_required_fields(self, rg, self.DISAMBIGUATE_RG_REQUIRED_FIELDS,
                                "disambiguate release group")
        _assert_required_fields(self, rg["pressings"][0], self.DISAMBIGUATE_PRESSING_REQUIRED_FIELDS,
                                "disambiguate pressing")
        _assert_required_fields(self, rg["tracks"][0], self.DISAMBIGUATE_TRACK_REQUIRED_FIELDS,
                                "disambiguate track")


class TestDiscogsBrowseRouteContracts(_WebServerCase):
    """Contract tests for Discogs browse routes."""

    DISCOGS_SEARCH_REQUIRED_FIELDS = {
        "id", "title", "artist_name", "artist_id",
    }
    DISCOGS_MASTER_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "country", "format",
        "in_library", "pipeline_status", "pipeline_id",
    }
    DISCOGS_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "artist_name", "tracks",
        "in_library", "pipeline_status", "pipeline_id",
    }
    DISCOGS_ARTIST_REQUIRED_FIELDS = {
        "artist_id", "artist_name", "release_groups",
    }

    def test_discogs_search_release_contract(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.search_releases.return_value = [
                {
                    "id": "21491",
                    "title": "OK Computer",
                    "artist_id": "3840",
                    "artist_name": "Radiohead",
                    "primary_type": "",
                    "first_release_date": "1997",
                    "artist_disambiguation": "",
                    "score": 100,
                    "is_master": True,
                    "discogs_release_id": "83182",
                },
            ]
            status, data = self._get("/api/discogs/search?q=ok+computer")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"release_groups"}, "discogs search response")
        _assert_required_fields(self, data["release_groups"][0],
                                self.DISCOGS_SEARCH_REQUIRED_FIELDS,
                                "discogs search result")

    def test_discogs_search_artist_contract(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead", "disambiguation": "", "score": 100},
            ]
            status, data = self._get("/api/discogs/search?q=radiohead&type=artist")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"artists"}, "discogs artist search response")

    def test_discogs_artist_contract(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_artist_name.return_value = "Radiohead"
            mock_dg.get_artist_masters.return_value = [
                {
                    "id": "21491",
                    "title": "OK Computer",
                    "type": "",
                    "secondary_types": [],
                    "first_release_date": "1997",
                    "artist_credit": "Radiohead",
                    "primary_artist_id": "3840",
                },
            ]
            status, data = self._get("/api/discogs/artist/3840")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DISCOGS_ARTIST_REQUIRED_FIELDS,
                                "discogs artist response")

    def test_discogs_master_contract(self):
        with patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}):
            mock_dg.get_master_releases.return_value = {
                "title": "OK Computer",
                "type": "",
                "releases": [
                    {
                        "id": "83182",
                        "title": "OK Computer",
                        "date": "1997",
                        "country": "Europe",
                        "status": "Official",
                        "track_count": 12,
                        "format": "CD",
                        "media_count": 1,
                        "labels": [],
                    },
                ],
            }
            status, data = self._get("/api/discogs/master/21491")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data["releases"][0],
                                self.DISCOGS_MASTER_RELEASE_REQUIRED_FIELDS,
                                "discogs master release")

    def test_discogs_release_contract(self):
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        with patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()):
            mock_dg.get_release.return_value = {
                "id": "83182",
                "title": "OK Computer",
                "artist_name": "Radiohead",
                "artist_id": "3840",
                "release_group_id": "21491",
                "date": "1997",
                "year": 1997,
                "country": "Europe",
                "status": "Official",
                "tracks": [
                    {"disc_number": 1, "track_number": 1, "title": "Airbag", "length_seconds": 284},
                ],
                "labels": [],
                "formats": [],
            }
            status, data = self._get("/api/discogs/release/83182")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DISCOGS_RELEASE_REQUIRED_FIELDS,
                                "discogs release detail")


class TestBeetsRouteContracts(_WebServerCase):
    """Contract tests for frontend-consumed beets library routes."""

    ALBUM_REQUIRED_FIELDS = {
        "id", "album", "artist", "year", "mb_albumid", "track_count",
        "mb_releasegroupid", "release_group_title", "added", "formats",
        "min_bitrate", "type", "label", "country", "source",
    }
    DETAIL_REQUIRED_FIELDS = (
        ALBUM_REQUIRED_FIELDS | {
            "path", "tracks", "pipeline_id", "pipeline_status",
            "pipeline_source", "pipeline_min_bitrate",
            "search_filetype_override", "target_format", "upgrade_queued",
            "download_history",
        }
    )
    TRACK_REQUIRED_FIELDS = {
        "disc", "track", "title", "length", "format", "bitrate",
        "samplerate", "bitdepth",
    }
    DELETE_REQUIRED_FIELDS = {"status", "id", "album", "artist", "deleted_files"}

    RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    RG_ID = "11111111-1111-1111-1111-111111111111"

    def setUp(self) -> None:
        import web.server as srv

        self._srv = srv
        self._orig_beets = srv._beets
        self._orig_beets_db_path = srv.beets_db_path
        self.beets = MagicMock()
        srv._beets = self.beets
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=42,
            status="wanted",
            mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
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
            "disc": 1,
            "track": 1,
            "title": "Track",
            "length": 180.0,
            "format": "MP3",
            "bitrate": 320000,
            "samplerate": 44100,
            "bitdepth": 16,
        }

    def test_beets_search_contract(self):
        self.beets.search_albums.return_value = [self._album()]
        with patch("web.server.check_pipeline", return_value={}):
            status, data = self._get("/api/beets/search?q=test")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"albums"}, "beets search response")
        _assert_required_fields(self, data["albums"][0], self.ALBUM_REQUIRED_FIELDS,
                                "beets search album")

    def test_beets_recent_contract(self):
        self.beets.get_recent.return_value = [self._album()]
        with patch("web.server.check_pipeline", return_value={}):
            status, data = self._get("/api/beets/recent")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"albums"}, "beets recent response")
        _assert_required_fields(self, data["albums"][0], self.ALBUM_REQUIRED_FIELDS,
                                "beets recent album")

    def test_beets_album_detail_contract(self):
        detail = self._album()
        detail["path"] = "/music/Test Artist/Test Album"
        detail["tracks"] = [self._track()]
        self.beets.get_album_detail.return_value = detail

        status, data = self._get("/api/beets/album/7")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DETAIL_REQUIRED_FIELDS,
                                "beets album detail")
        _assert_required_fields(self, data["tracks"][0], self.TRACK_REQUIRED_FIELDS,
                                "beets album track")

    @patch("routes.library.os.path.isdir", return_value=False)
    @patch("routes.library.os.path.isfile", return_value=False)
    @patch("routes.library.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB.delete_album")
    def test_beets_delete_contract(
        self,
        mock_delete,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        mock_delete.return_value = (
            "Test Album",
            "Test Artist",
            ["/music/Test Artist/Test Album/01 Track.mp3"],
        )

        status, data = self._post("/api/beets/delete", {"id": 7, "confirm": "DELETE"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DELETE_REQUIRED_FIELDS,
                                "beets delete response")


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

    FIELD_TYPES = {
        "download_log_id": int, "request_id": int, "artist": str,
        "album": str, "failed_path": str, "files_exist": bool,
        "distance": (int, float, type(None)), "in_library": bool,
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
            # Verify types for fields with known expected types
            for field, expected_type in self.FIELD_TYPES.items():
                self.assertIsInstance(entry[field], expected_type,
                    f"{field}={entry[field]!r} should be {expected_type}")

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

    FIELD_TYPES = {
        "id": int, "album": str, "artist": str, "year": int,
        "track_count": int, "min_bitrate": int, "added": float,
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
            # Verify types for critical fields
            for field, expected_type in self.FIELD_TYPES.items():
                self.assertIsInstance(album[field], expected_type,
                    f"{field}={album[field]!r} should be {expected_type}")

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
