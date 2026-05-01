#!/usr/bin/env python3
"""Tests for web/server.py HTTP endpoints.

Starts a real HTTP server on a random port with mocked DB,
verifying response codes, JSON structure, and error handling.
"""

import copy
from datetime import datetime, timezone
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
from lib.import_queue import ImportJob
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from web.library_album_row import LibraryAlbumRow

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
    # Per-attempt evidence (denormalized download_log columns surfaced by
    # get_wrong_matches for the candidate-evidence row payload).
    "spectral_grade": None,
    "spectral_bitrate": None,
    "v0_probe_kind": None,
    "v0_probe_avg_bitrate": None,
    # album_requests quality snapshot (joined in by get_wrong_matches)
    "request_status": "wanted",
    "request_min_bitrate": None,
    "request_verified_lossless": False,
    "request_current_spectral_grade": None,
    "request_current_spectral_bitrate": None,
    "request_imported_path": None,
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
    from lib.release_identity import detect_release_source, normalize_release_id
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

    mock_db.get_search_history.return_value = []
    mock_db.get_wrong_matches.return_value = [copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)]
    mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
    mock_db.clear_wrong_match_path.return_value = True
    mock_db.list_requests_by_artist.return_value = []
    mock_job = ImportJob(
        id=77,
        job_type="force_import",
        status="queued",
        request_id=100,
        dedupe_key="force_import:download_log:42",
        payload={"failed_path": "/tmp/Test Album"},
        result=None,
        message="Import queued",
        error=None,
        attempts=0,
        worker_id=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
    )
    mock_db.enqueue_import_job.return_value = mock_job
    mock_db.get_import_job.return_value = mock_job
    mock_db.list_import_jobs.return_value = [mock_job]
    mock_db.list_import_job_timeline.return_value = [mock_job]
    mock_db.list_active_import_jobs.return_value = []
    mock_db.count_import_jobs_by_status.return_value = {"queued": 1}
    # Default to "no active job" / "no successful uploader" so the
    # bad-rip route's race-check + username-resolution paths take
    # the happy fall-through unless individual tests override.
    mock_db.get_active_import_job_for_request.return_value = None
    mock_db.get_recent_successful_uploader.return_value = None
    mock_db.add_bad_audio_hashes.return_value = 0

    def _get_request_by_release_id(release_id):
        normalized = normalize_release_id(release_id)
        if not normalized:
            return None
        if detect_release_source(normalized) == "discogs":
            req = mock_db.get_request_by_discogs_release_id(normalized)
            if req:
                return req
        return mock_db.get_request_by_mb_release_id(normalized)

    mock_db.get_request_by_release_id.side_effect = _get_request_by_release_id

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

    @patch("web.routes.pipeline.resolve_failed_path", return_value="/tmp/Test Album")
    def test_post_force_import_passes_source_username(self, _mock_resolve):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_dedupe_key

        self.mock_db.get_download_log_entry.return_value = {
            "id": 42,
            "request_id": 100,
            "soulseek_username": "baduser",
            "validation_result": {
                "failed_path": "/tmp/Test Album",
                "scenario": "high_distance",
            },
        }

        status, data = self._post("/api/pipeline/force-import", {"download_log_id": 42})

        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "queued")
        self.assertEqual(data["artist"], _MOCK_PIPELINE_REQUEST["artist_name"])
        self.assertEqual(data["album"], _MOCK_PIPELINE_REQUEST["album_title"])
        self.mock_db.enqueue_import_job.assert_called_once()
        args, kwargs = self.mock_db.enqueue_import_job.call_args
        self.assertEqual(args, (IMPORT_JOB_FORCE,))
        self.assertEqual(kwargs["request_id"], 100)
        self.assertEqual(kwargs["dedupe_key"], force_import_dedupe_key(42))
        self.assertEqual(kwargs["payload"]["failed_path"], "/tmp/Test Album")
        self.assertEqual(kwargs["payload"]["source_username"], "baduser")

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
        "/api/browse/resolve",
        "/api/library/artist",
        "/api/artist/compare",
        r"^/api/artist/([a-f0-9-]+)$",
        r"^/api/artist/([a-f0-9-]+)/disambiguate$",
        r"^/api/release-group/([a-f0-9-]+)$",
        r"^/api/release/([a-f0-9-]+)$",
        "/api/discogs/search",
        r"^/api/discogs/artist/(\d+)$",
        r"^/api/discogs/master/(\d+)$",
        r"^/api/discogs/release/(\d+)$",
        "/api/discogs/label/search",
        r"^/api/discogs/label/(\d+)$",
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
        "/api/import-jobs",
        "/api/import-jobs/timeline",
        r"^/api/import-jobs/(\d+)$",
        "/api/beets/search",
        "/api/beets/recent",
        r"^/api/beets/album/(\d+)$",
        "/api/beets/delete",
        "/api/manual-import/scan",
        "/api/manual-import/import",
        "/api/import-preview",
        "/api/wrong-matches",
        "/api/wrong-matches/converge",
        "/api/wrong-matches/triage",
        "/api/wrong-matches/delete",
        "/api/wrong-matches/delete-group",
        "/api/wrong-matches/delete-transparent-non-flac",
        "/api/wrong-matches/delete-lossless-opus",
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
        # Issue #130: post-import `beet move` failures surface as typed
        # reason + detail so the frontend can render a warning chip.
        # Null on clean rows; the field must always be present.
        "disambiguation_failure", "disambiguation_detail",
        # Postflight bad-extension detection is warning-only but must be
        # surfaced in Recents so it is not buried in JSONB.
        "bad_extensions",
        # Wrong-match triage audit is display-only history metadata; clean
        # rows emit null/empty values so the frontend can render conditionally.
        "wrong_match_triage_action", "wrong_match_triage_summary",
        "wrong_match_triage_reason", "wrong_match_triage_preview_verdict",
        "wrong_match_triage_preview_decision",
        "wrong_match_triage_stage_chain", "wrong_match_triage_detail",
    }
    HISTORY_REQUIRED_FIELDS = {
        "id", "request_id", "outcome", "created_at", "soulseek_username",
        "downloaded_label", "verdict", "beets_scenario", "beets_distance",
        "disambiguation_failure", "disambiguation_detail", "bad_extensions",
        "spectral_grade", "spectral_bitrate", "existing_min_bitrate",
        "existing_spectral_bitrate",
        "wrong_match_triage_action", "wrong_match_triage_summary",
        "wrong_match_triage_reason", "wrong_match_triage_preview_verdict",
        "wrong_match_triage_preview_decision",
        "wrong_match_triage_stage_chain", "wrong_match_triage_detail",
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
        "stage0_spectral_gate",
        "stage1_spectral", "stage2_import", "stage3_quality_gate",
        "final_status", "imported", "denylisted", "keep_searching",
        "target_final_format",
    }
    IMPORT_PREVIEW_REQUIRED_FIELDS = {
        "mode", "verdict", "would_import", "confident_reject", "uncertain",
        "cleanup_eligible", "decision", "reason", "stage_chain",
    }
    WRONG_MATCH_TRIAGE_REQUIRED_FIELDS = {
        "download_log_id", "action", "success", "reason", "preview", "cleanup",
    }
    IMPORT_JOB_REQUIRED_FIELDS = {
        "id", "job_type", "status", "request_id", "dedupe_key", "payload",
        "result", "message", "error", "attempts", "worker_id", "created_at",
        "updated_at", "started_at", "heartbeat_at", "completed_at", "deduped",
        "preview_status", "preview_result", "preview_message", "preview_error",
        "preview_attempts", "preview_worker_id", "preview_started_at",
        "preview_heartbeat_at", "preview_completed_at", "importable_at",
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

    def test_pipeline_log_surfaces_wrong_match_triage_audit(self):
        original_log = copy.deepcopy(self.mock_db.get_log.return_value)
        row = copy.deepcopy(self.mock_db.get_log.return_value[0])
        row.update({
            "outcome": "rejected",
            "beets_scenario": "high_distance",
            "beets_distance": 0.190,
            "soulseek_username": "moundsofass",
            "album_title": "For Screening Purposes Only",
            "artist_name": "Test Icicles",
            "validation_result": {
                "scenario": "wrong_match",
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "reason": "requeue_upgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "requeue_upgrade",
                    "stage_chain": ["mp3_spectral:reject"],
                },
            },
        })
        self.mock_db.get_log.return_value = [row]

        try:
            status, data = self._get("/api/pipeline/log")
        finally:
            self.mock_db.get_log.return_value = original_log

        self.assertEqual(status, 200)
        item = data["log"][0]
        self.assertEqual(item["verdict"], "Wrong match (dist 0.190)")
        self.assertEqual(item["summary"],
                         "Wrong match (dist 0.190) · moundsofass")
        self.assertEqual(item["wrong_match_triage_action"], "deleted_reject")
        self.assertIn("spectral", item["wrong_match_triage_summary"])
        self.assertEqual(item["wrong_match_triage_stage_chain"],
                         ["mp3_spectral:reject"])

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

    DETAIL_RESPONSE_REQUIRED_FIELDS = {
        "request", "history", "tracks", "manual_reason", "last_search",
    }
    LAST_SEARCH_REQUIRED_FIELDS = {
        "variant", "final_state", "outcome", "top_candidates",
    }
    CANDIDATE_SCORE_REQUIRED_FIELDS = {
        "username", "dir", "filetype", "matched_tracks", "total_tracks",
        "avg_ratio", "missing_titles", "file_count",
    }

    def test_pipeline_detail_contract(self):
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DETAIL_RESPONSE_REQUIRED_FIELDS,
                                "pipeline detail response")
        _assert_required_fields(self, data["request"], self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline detail request")
        _assert_required_fields(self, data["history"][0], self.HISTORY_REQUIRED_FIELDS,
                                "pipeline detail history item")
        # Default mock state: no search history → last_search is None and
        # manual_reason is None. Both keys are still present.
        self.assertIsNone(data["last_search"])
        self.assertIsNone(data["manual_reason"])

    def test_pipeline_detail_surfaces_last_search_top_candidates(self):
        """When the latest search_log row has candidates, the route emits the
        top-3 by (matched_tracks DESC, avg_ratio DESC) via msgspec.to_builtins."""
        candidates_blob = [
            {"username": "u1", "dir": "A", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26, "avg_ratio": 0.95,
             "missing_titles": [], "file_count": 26},
            {"username": "u2", "dir": "B", "filetype": "mp3",
             "matched_tracks": 22, "total_tracks": 26, "avg_ratio": 0.80,
             "missing_titles": ["x"], "file_count": 22},
            {"username": "u3", "dir": "C", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26, "avg_ratio": 0.85,
             "missing_titles": [], "file_count": 26},
            {"username": "u4", "dir": "D", "filetype": "flac",
             "matched_tracks": 20, "total_tracks": 26, "avg_ratio": 0.99,
             "missing_titles": ["a", "b"], "file_count": 20},
        ]
        self.mock_db.get_search_history.return_value = [{
            "id": 99, "request_id": 100, "query": "*rtist Album",
            "result_count": 100, "elapsed_s": 1.2, "outcome": "no_match",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": candidates_blob,
            "variant": "v3_artist_only", "final_state": "Completed",
        }]

        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            self.mock_db.get_search_history.return_value = []

        self.assertEqual(status, 200)
        last = data["last_search"]
        self.assertIsNotNone(last)
        _assert_required_fields(self, last, self.LAST_SEARCH_REQUIRED_FIELDS,
                                "last_search payload")
        self.assertEqual(last["variant"], "v3_artist_only")
        self.assertEqual(last["final_state"], "Completed")
        self.assertEqual(last["outcome"], "no_match")
        # Top-3, sorted by (matched_tracks DESC, avg_ratio DESC):
        # u1 (26, 0.95) → u3 (26, 0.85) → u2 (22, 0.80)
        usernames = [c["username"] for c in last["top_candidates"]]
        self.assertEqual(usernames, ["u1", "u3", "u2"])
        for cand in last["top_candidates"]:
            _assert_required_fields(self, cand,
                                    self.CANDIDATE_SCORE_REQUIRED_FIELDS,
                                    "candidate score")

    def test_pipeline_detail_handles_null_candidates_gracefully(self):
        """Historical search_log row with NULL candidates → top_candidates=[]."""
        self.mock_db.get_search_history.return_value = [{
            "id": 1, "request_id": 100, "query": "q",
            "result_count": None, "elapsed_s": None, "outcome": "timeout",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": None, "variant": None, "final_state": None,
        }]
        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            self.mock_db.get_search_history.return_value = []

        self.assertEqual(status, 200)
        self.assertIsNotNone(data["last_search"])
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertIsNone(data["last_search"]["variant"])

    def test_pipeline_detail_handles_empty_candidates_list(self):
        """Latest search row with an empty candidates list → top_candidates=[]."""
        self.mock_db.get_search_history.return_value = [{
            "id": 1, "request_id": 100, "query": "q",
            "result_count": 0, "elapsed_s": 0.5, "outcome": "no_results",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": [], "variant": "v2_artist_album_no_year",
            "final_state": "Completed",
        }]
        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            self.mock_db.get_search_history.return_value = []

        self.assertEqual(status, 200)
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertEqual(data["last_search"]["variant"], "v2_artist_album_no_year")

    def test_pipeline_detail_handles_malformed_candidates_blob(self):
        """Corrupted search_log.candidates JSONB → 200 with top_candidates=[].

        Guard the route against historical rows whose JSONB shape no longer
        matches CandidateScore. The CLI already wraps msgspec.convert in
        try/except msgspec.ValidationError; the web route must do the same so
        a corrupt row does not 500 the detail page.
        """
        self.mock_db.get_search_history.return_value = [{
            "id": 7, "request_id": 100, "query": "q",
            "result_count": 5, "elapsed_s": 0.5, "outcome": "no_match",
            "created_at": "2026-04-29T00:00:00+00:00",
            # Wrong shape — missing every required CandidateScore field.
            "candidates": [{"foo": "bar"}],
            "variant": "v2_artist_album_no_year", "final_state": "Completed",
        }]
        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            self.mock_db.get_search_history.return_value = []

        self.assertEqual(status, 200)
        self.assertIsNotNone(data["last_search"])
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertEqual(data["last_search"]["variant"],
                         "v2_artist_album_no_year")

    def test_pipeline_detail_surfaces_manual_reason(self):
        """manual_reason='search_exhausted' is exposed on the detail response."""
        row = copy.deepcopy(_MOCK_PIPELINE_REQUEST)
        row["manual_reason"] = "search_exhausted"
        row["status"] = "manual"
        self.mock_db.get_request.return_value = row
        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST

        self.assertEqual(status, 200)
        self.assertEqual(data["manual_reason"], "search_exhausted")

    def test_pipeline_detail_history_surfaces_wrong_match_triage_audit(self):
        original_history = copy.deepcopy(self.mock_db.get_download_history.return_value)
        row = copy.deepcopy(self.mock_db.get_download_history.return_value[0])
        row.update({
            "outcome": "rejected",
            "beets_scenario": "high_distance",
            "beets_distance": 0.190,
            "validation_result": {
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "reason": "requeue_upgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "requeue_upgrade",
                    "stage_chain": ["stage1_spectral:reject"],
                },
            },
        })
        self.mock_db.get_download_history.return_value = [row]

        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            self.mock_db.get_download_history.return_value = original_history

        self.assertEqual(status, 200)
        item = data["history"][0]
        self.assertEqual(item["wrong_match_triage_action"], "deleted_reject")
        self.assertIn("spectral", item["wrong_match_triage_summary"])
        self.assertEqual(item["wrong_match_triage_preview_verdict"],
                         "confident_reject")
        self.assertEqual(item["wrong_match_triage_stage_chain"],
                         ["stage1_spectral:reject"])

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

    @patch("web.server.check_beets_by_artist_album",
           create=True, return_value=12)
    @patch("web.server.check_beets_library_detail", return_value={})
    def test_pipeline_recent_in_beets_false_when_mbid_not_in_beets(
            self, _mock_detail, _mock_fuzzy):
        """No exact MBID hit → ``in_beets`` False, no fuzzy fallback.

        Issue #123: ``get_pipeline_recent`` previously fell back to
        ``check_beets_by_artist_album`` when the MBID missed the batch
        lookup. That fuzzy LIKE match could return a track count for
        an unrelated pressing by the same artist. After deleting the
        fuzzy path, the recents row honestly reports ``in_beets=False``
        and ``beets_tracks=0`` when the exact ID is not in beets —
        even if a shim would have returned 12 tracks (mocked here with
        ``create=True`` so the test is RED against the current code).
        """
        row = make_request_row(
            id=303, status="imported", album_title="Recent Album",
            mb_release_id="no-such-id-in-beets")
        self.mock_db.get_recent.return_value = [row]
        self.mock_db.get_track_counts.return_value = {303: 8}
        self.mock_db.get_download_history_batch.return_value = {}

        status, data = self._get("/api/pipeline/recent")
        self.assertEqual(status, 200)
        item = data["recent"][0]
        self.assertFalse(
            item["in_beets"],
            "Issue #123: no exact ID match → in_beets False "
            "(artist/album fuzzy fallback was deleted).")
        self.assertEqual(item["beets_tracks"], 0)

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

    def test_import_preview_values_contract(self):
        status, data = self._post("/api/import-preview", {
            "values": {
                "is_flac": False,
                "min_bitrate": 320,
                "is_cbr": True,
            },
        })

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.IMPORT_PREVIEW_REQUIRED_FIELDS,
                                "import preview response")
        self.assertEqual(data["mode"], "values")

    def test_import_preview_rejects_ambiguous_modes(self):
        status, data = self._post("/api/import-preview", {
            "values": {"min_bitrate": 320},
            "download_log_id": 1,
        })

        self.assertEqual(status, 400)
        self.assertIn("error", data)

    @patch("web.routes.imports.triage_wrong_match")
    def test_wrong_match_triage_contract(self, mock_triage):
        from lib.import_preview import ImportPreviewResult
        from lib.wrong_match_triage import WrongMatchTriageResult

        mock_triage.return_value = WrongMatchTriageResult(
            download_log_id=77,
            action="kept_would_import",
            success=True,
            reason="import",
            preview=ImportPreviewResult(
                mode="download_log",
                verdict="would_import",
                decision="import",
                would_import=True,
            ),
        )

        status, data = self._post("/api/wrong-matches/triage", {
            "download_log_id": 77,
        })

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.WRONG_MATCH_TRIAGE_REQUIRED_FIELDS,
                                "wrong match triage response")
        self.assertEqual(data["action"], "kept_would_import")

    def test_import_jobs_contract(self):
        status, data = self._get("/api/import-jobs")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"jobs", "counts"}, "import jobs response")
        _assert_required_fields(self, data["jobs"][0], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import jobs item")

    def test_import_job_detail_contract(self):
        status, data = self._get("/api/import-jobs/77")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"job"}, "import job detail response")
        _assert_required_fields(self, data["job"], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import job detail")

    def test_import_jobs_timeline_contract(self):
        status, data = self._get("/api/import-jobs/timeline")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"jobs", "counts"},
                                "import jobs timeline response")
        _assert_required_fields(self, data["jobs"][0], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import jobs timeline item")
        _assert_required_fields(self, data["jobs"][0], {"artist_name", "album_title"},
                                "import jobs timeline identity")
        self.mock_db.list_import_job_timeline.assert_called_once_with(limit=50)

    def test_import_jobs_rejects_invalid_filters(self):
        status, data = self._get("/api/import-jobs?status=bad")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

        status, data = self._get("/api/import-jobs?request_id=abc")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    @patch("web.routes.pipeline.preview_import_from_values")
    def test_pipeline_simulate_threads_target_format(self, mock_preview):
        from lib.import_preview import ImportPreviewResult

        mock_preview.return_value = ImportPreviewResult(
            mode="values",
            verdict="would_import",
            simulation={
            "stage0_spectral_gate": "skipped_flac",
            "stage1_spectral": None,
            "stage2_import": "import",
            "stage3_quality_gate": "accept",
            "final_status": "imported",
            "imported": True,
            "denylisted": False,
            "keep_searching": False,
            "target_final_format": "flac",
            },
        )

        status, _data = self._get(
            "/api/pipeline/simulate?is_flac=true&min_bitrate=900&target_format=flac"
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            mock_preview.call_args.args[0].target_format, "flac")

    def test_pipeline_simulate_threads_avg_bitrate_to_stage0(self):
        """Issue #93: the web simulator must accept avg_bitrate and return
        stage0_spectral_gate so the UI can drive/display the new gate.
        """
        # VBR MP3 with high avg → stage 0 must say skipped_vbr_high_avg
        status, data = self._get(
            "/api/pipeline/simulate?"
            "is_flac=false&min_bitrate=240&is_cbr=false&is_vbr=true&avg_bitrate=245"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            data["stage0_spectral_gate"], "skipped_vbr_high_avg",
            "high-avg VBR must short-circuit the spectral gate in the "
            "web simulator (matches production lib.preimport)")

        # VBR MP3 with low avg → stage 0 must say would_run
        status, data = self._get(
            "/api/pipeline/simulate?"
            "is_flac=false&min_bitrate=126&is_cbr=false&is_vbr=true&avg_bitrate=182"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            data["stage0_spectral_gate"], "would_run",
            "Go! Team-shape transcode must trigger the gate in the simulator")


def _kwargs_to_query(kwargs: dict) -> str:
    """Serialize scenario kwargs to a query string the way the form would.

    Mirrors the route's decode rules (see ``get_pipeline_simulate`` in
    ``web/routes/pipeline.py``):
      - ``None`` → omit (route's ``_str``/``_int``/``_opt_bool`` return None
        for absent keys; ``_bool`` returns False).
      - ``True`` / ``False`` → ``"true"`` / ``"false"``.
      - int → ``str(int)``.
      - str → URL-encoded (values like ``"opus 128"`` contain spaces).

    Deliberately dumb — the test depends on the route's decoders to
    round-trip these values. If the route's decoding changes, this
    helper must change too, or the equivalence guarantee breaks.
    """
    from urllib.parse import quote_plus
    parts: list[str] = []
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, bool):
            parts.append(f"{k}={'true' if v else 'false'}")
        else:
            parts.append(f"{k}={quote_plus(str(v))}")
    return "&".join(parts)


class TestPipelineRouteDirectEquivalence(_WebServerCase):
    """Every pure-function web route must return the same value as a
    direct call to the underlying library function with equivalent inputs.

    Why this matters (post-deploy hotfix on PR #94): the route and the
    library function were computing different answers for the same inputs
    because ``web/routes/pipeline.py`` had mixed imports of ``quality`` and
    ``lib.quality``. Python loaded the module twice; ``is EnumMember``
    compared False across the module boundary; the AVG rank policy
    silently fell through to min_bitrate in the web simulator.

    Shape-only contract tests (``SIMULATE_REQUIRED_FIELDS``) were green —
    the response had the right keys with plausible values. Equivalence
    tests catch the divergence that contract tests can't see.

    ``tests/conftest.py`` puts ``lib/`` on sys.path, reproducing the same
    PYTHONPATH ambiguity production has. A future regression of the
    original dual-load bug would fail this test.
    """

    # Scenario table — each is a direct-call kwargs dict. The helper
    # translates to query params for the HTTP side. Coverage spans the
    # stages the route's output exposes + the specific cases that caught
    # review rounds' issues on PR #94 (VBR gate, avg threshold, AVG
    # policy, transcode paths).
    SCENARIOS: list[tuple[str, dict]] = [
        ("cbr_mp3_basic",
         dict(is_flac=False, min_bitrate=320, is_cbr=True)),
        ("vbr_mp3_legacy_no_avg",
         dict(is_flac=False, min_bitrate=245, is_cbr=False)),
        ("vbr_mp3_genuine_v0_high_avg_skips_gate",
         dict(is_flac=False, min_bitrate=200, is_cbr=False,
              is_vbr=True, avg_bitrate=245)),
        ("vbr_mp3_low_avg_triggers_gate",
         dict(is_flac=False, min_bitrate=126, is_cbr=False,
              is_vbr=True, avg_bitrate=182,
              spectral_grade="likely_transcode", spectral_bitrate=96)),
        ("vbr_mp3_low_avg_with_existing_rejected",
         dict(is_flac=False, min_bitrate=126, is_cbr=False,
              is_vbr=True, avg_bitrate=182,
              spectral_grade="likely_transcode", spectral_bitrate=96,
              existing_min_bitrate=200)),
        ("flac_genuine_converted_to_v0",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="genuine", converted_count=10,
              post_conversion_min_bitrate=245)),
        ("flac_suspect_transcode",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="suspect", converted_count=10,
              post_conversion_min_bitrate=190)),
        ("flac_kept_lossless_target_format",
         dict(is_flac=True, min_bitrate=900, is_cbr=False,
              target_format="flac")),
        ("existing_avg_bitrate_threaded",
         dict(is_flac=False, min_bitrate=210, is_cbr=False,
              is_vbr=True, avg_bitrate=210,
              existing_min_bitrate=200, existing_avg_bitrate=245)),
        ("downgrade_rejected",
         dict(is_flac=False, min_bitrate=128, is_cbr=False,
              existing_min_bitrate=256)),
        ("spectral_clamp_with_override",
         dict(is_flac=False, min_bitrate=320, is_cbr=True,
              spectral_grade="suspect", spectral_bitrate=160,
              existing_spectral_grade="genuine",
              existing_spectral_bitrate=160)),
        ("verified_lossless_target_opus",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="genuine", converted_count=10,
              post_conversion_min_bitrate=245,
              verified_lossless_target="opus 128")),
        ("live_mountain_goats_bride_durandurfan_provisional_source",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="likely_transcode", converted_count=1,
              post_conversion_min_bitrate=214,
              candidate_v0_probe_avg=214,
              existing_min_bitrate=320, existing_avg_bitrate=320,
              existing_format="MP3", existing_is_cbr=True,
              verified_lossless_target="opus 128")),
        ("live_iron_wine_creek_maplebug_reject_after_spencertpsn_probe",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="likely_transcode", spectral_bitrate=96,
              existing_spectral_grade="likely_transcode",
              existing_spectral_bitrate=96,
              converted_count=11, post_conversion_min_bitrate=165,
              candidate_v0_probe_avg=171, existing_v0_probe_avg=228,
              existing_min_bitrate=220, existing_avg_bitrate=228,
              existing_format="MP3", existing_is_cbr=False,
              verified_lossless_target="opus 128")),
    ]

    def test_simulate_route_matches_direct_call(self):
        """For every scenario, calling full_pipeline_decision directly
        must produce the same dict as hitting /api/pipeline/simulate."""
        from lib.quality import full_pipeline_decision
        from lib.config import read_runtime_rank_config

        # The route reads the runtime cfg via `_runtime_rank_config()`.
        # In the test env there's no /var/lib/cratedigger/config.ini, so it
        # falls back to CratediggerConfig() defaults. Read it once here so
        # both sides use identical cfg.
        cfg = read_runtime_rank_config()

        for name, kwargs in self.SCENARIOS:
            with self.subTest(scenario=name):
                direct = full_pipeline_decision(**kwargs, cfg=cfg)
                status, route = self._get(
                    f"/api/pipeline/simulate?{_kwargs_to_query(kwargs)}")
                self.assertEqual(status, 200)

                self.assertEqual(
                    set(direct.keys()), set(route.keys()),
                    f"{name}: route result has different keys than direct call")
                for key in direct:
                    self.assertEqual(
                        direct[key], route[key],
                        f"{name}: {key} differs — "
                        f"direct={direct[key]!r}, route={route[key]!r}. "
                        f"Divergence here means the HTTP surface is "
                        f"computing a different answer than the library "
                        f"function, e.g. dual-module-load, cfg mismatch, "
                        f"or a param the route forgot to thread.")

    def test_constants_route_matches_direct_call(self):
        """get_pipeline_constants must return the same tree as
        get_decision_tree(cfg=runtime_cfg), plus the hardcoded spectral
        constants the route overlays."""
        from lib.quality import get_decision_tree
        from lib.config import read_runtime_rank_config

        cfg = read_runtime_rank_config()
        direct = get_decision_tree(cfg=cfg)

        status, route = self._get("/api/pipeline/constants")
        self.assertEqual(status, 200)

        # Route overlays a handful of spectral_check + policy constants
        # on top of the tree. Assert the tree structure matches, then
        # strip the overlay keys before comparing the constants dict.
        self.assertEqual(route["stages"], direct["stages"],
                         "decision tree stages must match direct call")
        self.assertEqual(route["paths"], direct["paths"])
        self.assertEqual(route["path_labels"], direct["path_labels"])

        overlay_keys = {
            "HF_DEFICIT_SUSPECT", "HF_DEFICIT_MARGINAL", "ALBUM_SUSPECT_PCT",
            "MIN_CLIFF_SLICES", "CLIFF_THRESHOLD_DB_PER_KHZ",
            "rank_gate_min_rank", "rank_bitrate_metric",
            "rank_within_tolerance_kbps",
            # Preimport audio_check_mode is loaded from runtime config so
            # the Decisions tab presets reflect the deployment (issue #91).
            "audio_check_mode",
        }
        route_consts = {k: v for k, v in route["constants"].items()
                        if k not in overlay_keys}
        self.assertEqual(
            route_consts, direct["constants"],
            "constants (sans route overlay) must match direct call")


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
    BAN_SOURCE_REQUIRED_FIELDS = {
        "status", "username", "beets_removed", "hashes_recorded",
    }
    FORCE_IMPORT_REQUIRED_FIELDS = {
        "status", "request_id", "artist", "album", "message",
    }
    DELETE_REQUIRED_FIELDS = {"status", "id"}

    def setUp(self) -> None:
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        self.mock_db.add_request.return_value = 501
        self.mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)

    @patch("web.routes.pipeline.mb_api.get_release")
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

    @patch("web.routes.pipeline.discogs_api.get_release")
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

    @patch("lib.transitions.finalize_request")
    def test_pipeline_update_contract(self, _mock_transition):
        status, data = self._post("/api/pipeline/update", {"id": 100, "status": "manual"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPDATE_REQUIRED_FIELDS,
                                "pipeline update response")

    @patch("lib.transitions.finalize_request")
    def test_pipeline_upgrade_contract(self, _mock_transition):
        self.mock_db.get_request_by_mb_release_id.return_value = _MOCK_PIPELINE_REQUEST

        status, data = self._post("/api/pipeline/upgrade", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response")

    @patch("lib.transitions.finalize_request")
    @patch("web.routes.pipeline.discogs_api.get_release")
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_upgrade_discogs_new_request_uses_discogs_api(
        self, mock_mb_get, mock_dg_get, _mock_transition,
    ):
        """Numeric mb_release_id (Discogs) routes to discogs_api, not mb_api."""
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        self.mock_db.add_request.return_value = 999
        mock_dg_get.return_value = {
            "id": "12856590",
            "title": "New.Old.Rare",
            "artist_name": "Blueline Medic",
            "artist_id": "3640",
            "year": 2010,
            "country": "Australia",
            "tracks": [],
        }

        status, data = self._post(
            "/api/pipeline/upgrade", {"mb_release_id": "12856590"},
        )

        self.assertEqual(status, 200)
        mock_dg_get.assert_called_once_with(12856590, fresh=True)
        mock_mb_get.assert_not_called()
        # Confirm Discogs ID is mirrored into both columns for pipeline-compat
        add_kwargs = self.mock_db.add_request.call_args.kwargs
        self.assertEqual(add_kwargs["mb_release_id"], "12856590")
        self.assertEqual(add_kwargs["discogs_release_id"], "12856590")
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response (discogs)")

    @patch("lib.transitions.finalize_request")
    def test_pipeline_set_quality_contract(self, _mock_transition):
        self.mock_db.get_request_by_mb_release_id.return_value = _MOCK_PIPELINE_REQUEST

        status, data = self._post(
            "/api/pipeline/set-quality",
            {"mb_release_id": "abc-123", "status": "manual", "min_bitrate": 245},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_QUALITY_REQUIRED_FIELDS,
                                "pipeline set-quality response")

    @patch("lib.transitions.finalize_request")
    def test_pipeline_set_quality_discogs_request_normalizes_and_falls_back(
        self, _mock_transition,
    ):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=100,
            status="imported",
            mb_release_id="12856590",
            discogs_release_id=None,
        ))

        with patch.object(srv, "db", fake_db):
            status, data = self._post(
                "/api/pipeline/set-quality",
                {"mb_release_id": " 0012856590 ", "status": "manual", "min_bitrate": 245},
            )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_QUALITY_REQUIRED_FIELDS,
                                "pipeline set-quality response (discogs)")

    @patch("lib.transitions.finalize_request")
    def test_pipeline_upgrade_normalizes_uppercase_uuid(self, mock_transition):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=1704,
            status="imported",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            min_bitrate=320,
        ))

        with patch.object(srv, "db", fake_db):
            status, data = self._post(
                "/api/pipeline/upgrade",
                {"mb_release_id": "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"},
            )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response (uppercase)")
        self.assertEqual(mock_transition.call_args.args[1], 1704)

    def test_pipeline_set_intent_contract(self):
        self.mock_db.get_request.return_value = make_request_row(id=100, status="wanted")

        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "lossless"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_INTENT_REQUIRED_FIELDS,
                                "pipeline set-intent response")

    @patch("lib.transitions.finalize_request")
    def test_pipeline_ban_source_contract(self, _mock_transition):
        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 100, "username": "baduser", "mb_release_id": "abc-123"},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.BAN_SOURCE_REQUIRED_FIELDS,
                                "pipeline ban-source response")

    @patch("web.routes.pipeline.resolve_failed_path", return_value="/tmp/Test Album")
    def test_pipeline_force_import_contract(self, _mock_resolve):
        status, data = self._post("/api/pipeline/force-import", {"download_log_id": 42})

        self.assertEqual(status, 202)
        _assert_required_fields(self, data, self.FORCE_IMPORT_REQUIRED_FIELDS,
                                "pipeline force-import response")

    def test_pipeline_delete_contract(self):
        status, data = self._post("/api/pipeline/delete", {"id": 100})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DELETE_REQUIRED_FIELDS,
                                "pipeline delete response")

    # -- fresh=True seam (Codex review on issue #101) ----------------

    @patch("routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_fetches_release_fresh(self, mock_get_release):
        """POST /api/pipeline/add (MusicBrainz) MUST bypass the 24h meta
        cache — the fetched metadata is persisted into `album_requests`
        and `request_tracks`. A stale cached payload from an earlier
        browse would silently bake pre-correction artist / title / tracks
        into the pipeline DB.
        """
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "artist-1",
            "artist_name": "Test Artist",
            "title": "Test Album",
            "year": 2024,
            "country": "US",
            "tracks": [{"title": "Track"}],
        }

        status, _data = self._post("/api/pipeline/add",
                                   {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        mock_get_release.assert_called_once_with("abc-123", fresh=True)

    @patch("routes.pipeline.discogs_api.get_release")
    def test_pipeline_add_discogs_fetches_release_fresh(self, mock_get_release):
        """POST /api/pipeline/add (Discogs) MUST bypass the 24h meta cache."""
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        mock_get_release.return_value = {
            "artist_id": "3840",
            "artist_name": "Radiohead",
            "title": "OK Computer",
            "year": 1997,
            "country": "Europe",
            "tracks": [{"title": "Airbag"}],
        }

        status, _data = self._post("/api/pipeline/add",
                                   {"discogs_release_id": "83182"})

        self.assertEqual(status, 200)
        mock_get_release.assert_called_once_with(83182, fresh=True)

    @patch("lib.transitions.finalize_request")
    @patch("routes.pipeline.mb_api.get_release")
    def test_pipeline_upgrade_new_mb_fetches_release_fresh(
            self, mock_get_release, _mock_transition):
        """POST /api/pipeline/upgrade creating a brand-new MB request
        MUST bypass the meta cache — same rationale as add."""
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        self.mock_db.add_request.return_value = 999
        mock_get_release.return_value = {
            "artist_id": "a-1", "artist_name": "A", "title": "T",
            "year": 2024, "country": "US", "tracks": [],
        }

        status, _data = self._post(
            "/api/pipeline/upgrade",
            {"mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
        )

        self.assertEqual(status, 200)
        mock_get_release.assert_called_once_with(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", fresh=True)

    @patch("lib.transitions.finalize_request")
    @patch("routes.pipeline.discogs_api.get_release")
    def test_pipeline_upgrade_new_discogs_fetches_release_fresh(
            self, mock_get_release, _mock_transition):
        """POST /api/pipeline/upgrade creating a brand-new Discogs request
        MUST bypass the meta cache — same rationale as add."""
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        self.mock_db.add_request.return_value = 999
        mock_get_release.return_value = {
            "id": "12856590", "title": "New.Old.Rare",
            "artist_name": "Blueline Medic", "artist_id": "3640",
            "year": 2010, "country": "Australia", "tracks": [],
        }

        status, _data = self._post(
            "/api/pipeline/upgrade", {"mb_release_id": "12856590"},
        )

        self.assertEqual(status, 200)
        mock_get_release.assert_called_once_with(12856590, fresh=True)


class TestUserRequeueOverridePreservation(_WebServerCase):
    """User-initiated requeue endpoints must preserve a stricter existing
    search_filetype_override — e.g. 'lossless' set by the quality gate after a
    CBR 320 import. Clicking Upgrade or flipping status back to wanted must not
    re-open MP3 tiers the gate intentionally closed (which would trigger
    redundant re-downloads of the same-or-worse quality).

    ban_source already does the right thing via `req.get(...) or QUALITY_UPGRADE_TIERS`;
    this class guards upgrade + update against regressing to a blind clobber,
    and pins ban_source's behaviour so future refactors don't drop it.
    """

    RELEASE_ID = "c6cd62c4-da2a-4a89-a219-adba66d6c7d4"

    def setUp(self) -> None:
        import web.server as srv
        self._srv = srv
        self._orig_beets = srv._beets
        # Beets stub: update() only hits this via album_exists / get_min_bitrate.
        # A live beets DB is the usual preceding state for a requeue.
        self._beets = MagicMock()
        self._beets.album_exists.return_value = True
        self._beets.get_min_bitrate.return_value = 320
        # Ban-source now also calls ``get_item_paths`` for the bad-rip
        # hash-capture step (plan 2026-04-29-005, U4). Default to "no
        # tracks" so legacy ban-source tests don't trip over the new
        # gate; tests that exercise hash capture override this.
        self._beets.get_item_paths.return_value = []
        # Ban-source now routes through ``BeetsDB.locate`` (issue #121).
        # Default the mock to 'album present before and removed after'
        # so the legacy `album_exists.side_effect = [True, False]`
        # tests read as "exact → absent" in the new vocabulary.
        # Individual tests override this via ``_set_locate_sequence``.
        self._set_locate_sequence([
            ("exact", 1, ()),  # selectors filled per-test via helper
            ("absent", None, ()),
        ])
        srv._beets = self._beets

    def _set_locate_sequence(
            self, results: list[tuple[str, object, tuple]]) -> None:
        """Program ``self._beets.locate`` to return a sequence of results.

        Each tuple is ``(kind, album_id, selectors)``. Yields one
        ReleaseLocation-shaped SimpleNamespace per call; extra calls
        reuse the final entry. Kept local to this test class because
        ban-source is the main caller that reasons about the before /
        after pair.
        """
        from types import SimpleNamespace
        results_copy = list(results)

        def _side_effect(release_id, *_args, **_kwargs):
            if not results_copy:
                return SimpleNamespace(kind="absent", album_id=None, selectors=())
            kind, album_id, selectors = (
                results_copy[0] if len(results_copy) == 1
                else results_copy.pop(0))
            # Auto-fill selectors for 'exact' when the test left them blank
            # — the locate seam's contract is that selectors are driven by
            # the ID shape, so it's OK for tests to defer to it.
            if kind == "exact" and not selectors:
                from lib.release_identity import detect_release_source
                if detect_release_source(str(release_id)) == "discogs":
                    selectors = (f"discogs_albumid:{release_id}",
                                 f"mb_albumid:{release_id}")
                else:
                    selectors = (f"mb_albumid:{release_id}",)
            return SimpleNamespace(
                kind=kind, album_id=album_id, selectors=selectors)

        self._beets.locate.side_effect = _side_effect

    def tearDown(self) -> None:
        self._srv._beets = self._orig_beets

    def _override_passed(self, mock_transition) -> object:
        """Extract the search override from the last routed transition."""
        self.assertTrue(mock_transition.call_args_list,
                        "finalize_request was not called")
        transition = mock_transition.call_args_list[-1].args[2]
        return transition.fields.get(
            "search_filetype_override",
            "<MISSING>",
        )

    # -- Upgrade --------------------------------------------------------

    @patch("lib.transitions.finalize_request")
    def test_upgrade_preserves_stricter_override(self, mock_transition):
        """Upgrade on an imported album with override='lossless' must keep it."""
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=1704, status="imported", min_bitrate=320,
            search_filetype_override="lossless",
        )

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless")

    @patch("lib.transitions.finalize_request")
    def test_upgrade_preserves_narrowed_override(self, mock_transition):
        """Upgrade must preserve a post-downgrade-narrow like 'lossless,mp3 v0'."""
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=1704, status="imported", min_bitrate=320,
            search_filetype_override="lossless,mp3 v0",
        )

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless,mp3 v0")

    @patch("lib.transitions.finalize_request")
    def test_upgrade_falls_back_to_full_tiers_when_no_override(self, mock_transition):
        """Upgrade on an imported album with no override falls back to the full ladder."""
        from lib.quality import QUALITY_UPGRADE_TIERS

        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=1704, status="imported", min_bitrate=160,
            search_filetype_override=None,
        )

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition),
                         QUALITY_UPGRADE_TIERS)

    @patch("lib.transitions.finalize_request")
    def test_upgrade_omits_min_bitrate_when_beets_lookup_misses(
            self, mock_transition):
        """Missing Beets quality data must not clear the existing DB baseline."""
        self._beets.get_min_bitrate.return_value = None
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=1704, status="imported", min_bitrate=320,
            search_filetype_override="lossless",
        )

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        transition = mock_transition.call_args.args[2]
        self.assertNotIn("min_bitrate", transition.fields)
        self.assertEqual(transition.fields["search_filetype_override"], "lossless")

    # -- Update (status → wanted) ---------------------------------------

    @patch("lib.transitions.finalize_request")
    def test_update_to_wanted_preserves_stricter_override(self, mock_transition):
        """Flipping an imported album back to wanted must preserve 'lossless'."""
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            search_filetype_override="lossless",
        )

        status, _data = self._post("/api/pipeline/update",
                                    {"id": 1704, "status": "wanted"})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless")

    @patch("lib.transitions.finalize_request")
    def test_update_to_wanted_falls_back_to_full_tiers_when_no_override(
            self, mock_transition):
        """Flipping imported→wanted with no override uses the full upgrade ladder."""
        from lib.quality import QUALITY_UPGRADE_TIERS

        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=160,
            search_filetype_override=None,
        )

        status, _data = self._post("/api/pipeline/update",
                                    {"id": 1704, "status": "wanted"})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition),
                         QUALITY_UPGRADE_TIERS)

    # -- Ban source (regression pin) ------------------------------------

    @patch("lib.transitions.finalize_request")
    def test_ban_source_preserves_stricter_override(self, mock_transition):
        """Pin: ban_source already preserves override. Guard against future regression."""
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            search_filetype_override="lossless",
        )

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless")

    @patch("lib.beets_album_op.sp.run")
    @patch("lib.transitions.finalize_request")
    def test_ban_source_clears_on_disk_quality_fields(
            self, _mock_transition, mock_subprocess):
        """After ``beet remove -d``, pipeline DB must forget on-disk quality.

        ``current_spectral_*`` and ``verified_lossless`` describe files that
        live in beets. Once the ban flow wipes those files, leaving the
        fields populated misleads every downstream consumer (wrong-matches
        UI shows ghost quality, library views, quality gate uses stale
        baselines). The write-side invariant: remove-from-beets implies
        clear-on-disk-quality. Issue #121 couples both sides via
        ``lib.release_cleanup.remove_and_reset_release``.
        """
        self.mock_db.clear_on_disk_quality_fields.reset_mock()
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="", stderr="")
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            verified_lossless=False,
        )
        # First locate: was present. Second (after remove): gone.
        self._set_locate_sequence([
            ("exact", 1, ()),
            ("absent", None, ()),
        ])

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        self.mock_db.clear_on_disk_quality_fields.assert_called_once_with(1704)

    @patch("lib.beets_album_op.sp.run")
    @patch("lib.transitions.finalize_request")
    def test_ban_source_skips_clear_when_beet_remove_failed(
            self, _mock_transition, mock_subprocess):
        """Conservative: if beets still holds the album after the remove
        attempts (e.g. permissions error, wrong column and no legacy
        fallback matched), the on-disk quality state is still accurate,
        so don't clear it. Modelled by ``locate`` returning 'exact'
        both before and after the subprocess calls. The non-zero rc
        also surfaces in ``cleanup_errors`` so the UI can tell the
        user the ban committed but the on-disk remove was incomplete
        (issue #123 PR B).
        """
        self.mock_db.clear_on_disk_quality_fields.reset_mock()
        mock_subprocess.return_value = MagicMock(
            returncode=1, stdout="", stderr="beet failed")
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            current_spectral_grade="genuine",
            verified_lossless=True,
        )
        # Album is still there after the remove attempt. Seed the
        # selector tuple so the remove loop has something to iterate.
        self._set_locate_sequence([
            ("exact", 1, (f"mb_albumid:{self.RELEASE_ID}",)),
            ("exact", 1, (f"mb_albumid:{self.RELEASE_ID}",)),
        ])

        status, data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        self.mock_db.clear_on_disk_quality_fields.assert_not_called()
        # #123 PR B + plan 2026-04-29-005 U4: the non-zero rc now
        # surfaces under ``partial_failures.cleanup_errors`` (the
        # unified shape). Distinguishes "banned cleanly" from
        # "banned but album still on disk".
        cleanup_errors = data["partial_failures"]["cleanup_errors"]
        self.assertEqual(len(cleanup_errors), 1)
        self.assertEqual(cleanup_errors[0]["reason"], "nonzero_rc")
        self.assertFalse(data["beets_removed"])

    @patch("lib.beets_album_op.sp.run")
    @patch("lib.transitions.finalize_request")
    def test_ban_source_uses_discogs_selector_for_numeric_id(
            self, _mock_transition, mock_subprocess):
        """Discogs-backed requests carry a numeric ID. ``beet remove -d``
        must try ``discogs_albumid:<id>`` (the new layout) AND
        ``mb_albumid:<id>`` (the legacy layout documented in
        artist_compare.py / webui-primer.md), otherwise one of the two
        layouts goes unremoved and the banned copy stays on disk.
        After issue #121 the selectors come from ``BeetsDB.locate`` so
        every caller that asks 'is this release on disk?' agrees on
        the same selector set.
        """
        self.mock_db.clear_on_disk_quality_fields.reset_mock()
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="", stderr="")
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id="12856590",
            min_bitrate=320,
        )
        # Was there (with BOTH Discogs selectors); after both removes, gone.
        self._set_locate_sequence([
            ("exact", 1, ("discogs_albumid:12856590", "mb_albumid:12856590")),
            ("absent", None, ()),
        ])

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "username": "baduser",
            "mb_release_id": " 0012856590 ",
        })

        self.assertEqual(status, 200)
        argvs = [call.args[0] for call in mock_subprocess.call_args_list]
        flattened = [token for argv in argvs for token in argv]
        self.assertIn("discogs_albumid:12856590", flattened,
                      "Must attempt the new-layout selector.")
        self.assertIn("mb_albumid:12856590", flattened,
                      "Must also attempt the legacy mb_albumid selector "
                      "so older beets libraries don't regress.")

    @patch("lib.beets_album_op.sp.run")
    @patch("lib.transitions.finalize_request")
    def test_ban_source_clears_stale_state_when_album_already_gone(
            self, _mock_transition, mock_subprocess):
        """Ghost state can pre-date the handler: a user runs
        ``beet rm mb_albumid:X`` manually, then days later bans the
        source. ``locate`` returns 'absent' before ban-source even
        starts, so no ``beet remove`` runs — but the pipeline DB still
        carries the old ``current_spectral_*`` / ``imported_path``.
        The handler must still clear those fields so ``dispatch_import_core``
        doesn't keep deriving ``--override-min-bitrate`` from phantom
        baselines on the next import attempt.
        """
        self.mock_db.clear_on_disk_quality_fields.reset_mock()
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="", stderr="")
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            imported_path="/mnt/virtio/Music/Beets/Stale/Path",
        )
        # Album was already gone when ban-source ran (earlier beet rm).
        self._set_locate_sequence([
            ("absent", None, ()),
            ("absent", None, ()),
        ])

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        self.mock_db.clear_on_disk_quality_fields.assert_called_once_with(1704)
        # No remove ran — the handler had nothing to remove.
        mock_subprocess.assert_not_called()

    @patch("lib.transitions.finalize_request")
    def test_ban_source_rejects_missing_mb_release_id(self, _mock_transition):
        """Plan 2026-04-29-005 U4: ``mb_release_id`` is now required so
        the bad-rip flow can locate the audio files to hash before
        ``remove_and_reset_release`` deletes them. Without it, there is
        no album to ban — return 400 rather than silently skip.
        """
        self.mock_db.clear_on_disk_quality_fields.reset_mock()
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported",
            min_bitrate=320,
            current_spectral_grade="genuine",
            verified_lossless=True,
        )

        status, data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "username": "baduser",
            # No mb_release_id.
        })

        self.assertEqual(status, 400)
        self.assertIn("mb_release_id", data.get("error", ""))
        self.mock_db.clear_on_disk_quality_fields.assert_not_called()


class TestBanSourceBadRipExtensions(_WebServerCase):
    """Plan 2026-04-29-005 U4: bad-rip hash capture + server-side
    username resolution + importer-race 409 + unified
    ``partial_failures`` response shape on ``POST /api/pipeline/ban-source``.
    """

    RELEASE_ID = "c6cd62c4-da2a-4a89-a219-adba66d6c7d4"
    # Two distinct fake hashes (32 bytes each) — content doesn't matter
    # for the route, only that ``hash_audio_content`` returned something.
    HASH_A = b"\x01" * 32
    HASH_B = b"\x02" * 32

    def setUp(self) -> None:
        import web.server as srv
        self._srv = srv
        self._orig_beets = srv._beets
        self._beets = MagicMock()
        # Default: no tracks — individual tests override.
        self._beets.get_item_paths.return_value = []
        # locate seam returns "absent" so ``remove_and_reset_release``
        # is a no-op unless overridden per-test.
        from types import SimpleNamespace
        self._beets.locate.return_value = SimpleNamespace(
            kind="absent", album_id=None, selectors=()
        )
        srv._beets = self._beets

        # Reset bad-rip-related mocks so cross-test state doesn't leak.
        self.mock_db.get_active_import_job_for_request.reset_mock()
        self.mock_db.get_active_import_job_for_request.return_value = None
        self.mock_db.get_recent_successful_uploader.reset_mock()
        self.mock_db.get_recent_successful_uploader.return_value = None
        self.mock_db.add_bad_audio_hashes.reset_mock()
        self.mock_db.add_bad_audio_hashes.return_value = 0
        self.mock_db.add_denylist.reset_mock()
        self.mock_db.log_download.reset_mock()
        self.mock_db.get_request.return_value = make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
        )

    def tearDown(self) -> None:
        self._srv._beets = self._orig_beets

    # AE1, AE2 — body-without-username, server resolves uploader, hashes recorded.
    @patch("web.routes.pipeline.hash_audio_content")
    @patch("lib.transitions.finalize_request")
    def test_resolves_username_and_records_hashes(
            self, _mock_transition, mock_hash):
        """POST {request_id, mb_release_id} only — server resolves
        ``reported_username`` from the most recent successful
        download_log, hashes every track via ``hash_audio_content``,
        and persists them with the resolved username (R3, R5, R7).
        """
        self.mock_db.get_recent_successful_uploader.return_value = "Hxrco"
        self._beets.get_item_paths.return_value = [
            (1, "/mnt/Music/Beets/A/track-01.flac"),
            (2, "/mnt/Music/Beets/A/track-02.flac"),
        ]
        # Distinct digests per call so the route inserts both rows.
        mock_hash.side_effect = [self.HASH_A, self.HASH_B]
        self.mock_db.add_bad_audio_hashes.return_value = 2

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["username"], "Hxrco")
        self.assertEqual(data["hashes_recorded"], 2)
        # Happy path: no partial_failures on the response.
        self.assertNotIn("partial_failures", data)
        # add_bad_audio_hashes called with the resolved username + reason.
        self.mock_db.add_bad_audio_hashes.assert_called_once()
        call_args = self.mock_db.add_bad_audio_hashes.call_args
        self.assertEqual(call_args.args[0], 1704)
        self.assertEqual(call_args.args[1], "Hxrco")
        self.assertEqual(call_args.args[2], "manually banned via web UI")
        hashes_arg = call_args.args[3]
        self.assertEqual(len(hashes_arg), 2)
        self.assertEqual(hashes_arg[0].hash_value, self.HASH_A)
        self.assertEqual(hashes_arg[0].audio_format, "flac")
        self.assertEqual(hashes_arg[1].hash_value, self.HASH_B)
        # Denylist written for the resolved user.
        self.mock_db.add_denylist.assert_called_once_with(
            1704, "Hxrco", "manually banned via web UI"
        )
        # #188 follow-up: a download_log row records the ban event.
        self.mock_db.log_download.assert_called_once()
        log_kwargs = self.mock_db.log_download.call_args.kwargs
        self.assertEqual(log_kwargs["request_id"], 1704)
        self.assertEqual(log_kwargs["soulseek_username"], "Hxrco")
        self.assertEqual(log_kwargs["outcome"], "curator_ban")
        self.assertIn("Marked bad rip", log_kwargs["beets_detail"])
        ban_meta = json.loads(log_kwargs["validation_result"])
        self.assertEqual(ban_meta["scenario"], "curator_ban")
        self.assertEqual(ban_meta["hashes_recorded"], 2)
        self.assertEqual(ban_meta["denylisted_username"], "Hxrco")

    # AE4 — partial hash failure does not block the ban.
    @patch("web.routes.pipeline.hash_audio_content")
    @patch("lib.transitions.finalize_request")
    def test_hash_failure_partial_does_not_block_ban(
            self, _mock_transition, mock_hash):
        """One unreadable track → ``hashes_recorded`` reflects the
        succeeded count, ``partial_failures.hash_capture_errors``
        names the failed path, denylist + remove + requeue still run.
        """
        self.mock_db.get_recent_successful_uploader.return_value = "Hxrco"
        self._beets.get_item_paths.return_value = [
            (1, "/mnt/Music/Beets/A/track-01.flac"),
            (2, "/mnt/Music/Beets/A/track-02.flac"),
            (3, "/mnt/Music/Beets/A/track-03.flac"),
        ]
        # Track 2 raises; tracks 1 and 3 succeed.
        from lib.audio_hash import AudioHashError
        mock_hash.side_effect = [
            self.HASH_A,
            AudioHashError("ffmpeg failed (rc=1): truncated mp3"),
            self.HASH_B,
        ]
        self.mock_db.add_bad_audio_hashes.return_value = 2

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["hashes_recorded"], 2)
        self.assertIn("partial_failures", data)
        errors = data["partial_failures"]["hash_capture_errors"]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["track_path"],
                         "/mnt/Music/Beets/A/track-02.flac")
        self.assertIn("truncated", errors[0]["reason"])
        # Denylist still runs for the resolved user.
        self.mock_db.add_denylist.assert_called_once()
        # ``add_bad_audio_hashes`` called with the two SUCCESSFUL hashes only.
        hashes_arg = self.mock_db.add_bad_audio_hashes.call_args.args[3]
        self.assertEqual(len(hashes_arg), 2)

    # E1.1 — no successful uploader on record.
    @patch("web.routes.pipeline.hash_audio_content")
    @patch("lib.transitions.finalize_request")
    def test_no_uploader_records_hashes_with_null_username(
            self, _mock_transition, mock_hash):
        """No successful download_log → ``username: null`` returned,
        ``add_denylist`` not called, but hashes ARE recorded with
        ``reported_username=None`` (the bytes are still protected).
        """
        self.mock_db.get_recent_successful_uploader.return_value = None
        self._beets.get_item_paths.return_value = [
            (1, "/mnt/Music/Beets/A/track-01.mp3"),
        ]
        mock_hash.return_value = self.HASH_A
        self.mock_db.add_bad_audio_hashes.return_value = 1

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertIsNone(data["username"])
        self.assertEqual(data["hashes_recorded"], 1)
        self.assertNotIn("partial_failures", data)
        # #188 follow-up: ban event still logged with NULL username.
        self.mock_db.log_download.assert_called_once()
        log_kwargs = self.mock_db.log_download.call_args.kwargs
        self.assertEqual(log_kwargs["outcome"], "curator_ban")
        self.assertIsNone(log_kwargs["soulseek_username"])
        # Hashes recorded with username=None.
        call_args = self.mock_db.add_bad_audio_hashes.call_args
        self.assertIsNone(call_args.args[1])
        # No denylist call when no user resolved.
        self.mock_db.add_denylist.assert_not_called()

    # E1.2 — album not in beets / no track paths.
    @patch("lib.transitions.finalize_request")
    def test_no_tracks_in_beets_records_capture_error(
            self, _mock_transition):
        """``get_item_paths`` empty → response includes
        ``partial_failures.hash_capture_errors`` with one
        ``no_tracks_in_beets`` entry; denylist still runs if
        username resolved; no hashes recorded.
        """
        self.mock_db.get_recent_successful_uploader.return_value = "Hxrco"
        self._beets.get_item_paths.return_value = []

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["hashes_recorded"], 0)
        self.assertIn("partial_failures", data)
        errors = data["partial_failures"]["hash_capture_errors"]
        self.assertEqual(len(errors), 1)
        self.assertIsNone(errors[0]["track_path"])
        self.assertEqual(errors[0]["reason"], "no_tracks_in_beets")
        # Denylist still written.
        self.mock_db.add_denylist.assert_called_once_with(
            1704, "Hxrco", "manually banned via web UI"
        )
        # No add_bad_audio_hashes call (empty list short-circuit).
        self.mock_db.add_bad_audio_hashes.assert_not_called()

    # E1.3 — importer race: 409 before any work.
    def test_importer_busy_returns_409_no_writes(self):
        """``import_jobs`` row exists with status running → 409, body
        ``{error: "importer_busy", retry_after_seconds: 30}``. No
        denylist, no hashes, no beets_db calls.
        """
        self.mock_db.get_active_import_job_for_request.return_value = {
            "id": 99, "request_id": 1704, "status": "running",
        }

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "mb_release_id": self.RELEASE_ID,
             "username": "anyone"},
        )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "importer_busy")
        self.assertEqual(data["retry_after_seconds"], 30)
        # No mutation of any kind.
        self.mock_db.add_denylist.assert_not_called()
        self.mock_db.add_bad_audio_hashes.assert_not_called()
        self._beets.get_item_paths.assert_not_called()
        self._beets.locate.assert_not_called()

    # E1.6 — idempotency: second click is a no-op insert.
    @patch("web.routes.pipeline.hash_audio_content")
    @patch("lib.transitions.finalize_request")
    def test_idempotent_second_click_records_zero_new_hashes(
            self, _mock_transition, mock_hash):
        """Second call inserts 0 new rows (ON CONFLICT DO NOTHING in
        the DB layer; ``add_bad_audio_hashes`` returns 0). Response
        is 200 with ``hashes_recorded: 0`` and no ``partial_failures``.
        """
        self.mock_db.get_recent_successful_uploader.return_value = "Hxrco"
        self._beets.get_item_paths.return_value = [
            (1, "/mnt/Music/Beets/A/track-01.flac"),
        ]
        mock_hash.return_value = self.HASH_A
        # DB layer returns 0 — every (hash, format) already present.
        self.mock_db.add_bad_audio_hashes.return_value = 0

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["hashes_recorded"], 0)
        self.assertNotIn("partial_failures", data)


class TestManualImportRouteContracts(_WebServerCase):
    """Contract tests for manual import routes."""

    FOLDER_REQUIRED_FIELDS = {"name", "path", "artist", "album", "file_count", "match"}
    MATCH_REQUIRED_FIELDS = {"request_id", "artist", "album", "mb_release_id", "score"}
    IMPORT_REQUIRED_FIELDS = {"status", "message", "request_id", "artist", "album"}

    def setUp(self) -> None:
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
        self.mock_db.get_by_status.side_effect = None

    @patch("web.routes.imports.match_folders_to_requests")
    @patch("web.routes.imports.scan_complete_folder")
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

    @patch("web.routes.imports.resolve_failed_path",
           return_value="/complete/Test Artist - Test Album")
    def test_manual_import_post_contract(self, _mock_resolve):
        status, data = self._post(
            "/api/manual-import/import",
            {"request_id": 100, "path": "/complete/Test Artist - Test Album"},
        )

        self.assertEqual(status, 202)
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
    LIBRARY_ALBUM_REQUIRED_FIELDS = set(LibraryAlbumRow.__struct_fields__)
    RELEASE_GROUP_REQUIRED_FIELDS = {
        "id", "title", "country", "date", "format", "track_count", "status",
        "in_library", "beets_album_id", "pipeline_status", "pipeline_id",
    }
    RELEASE_DETAIL_REQUIRED_FIELDS = {
        "id", "title", "tracks", "in_library", "beets_album_id",
        "pipeline_status", "pipeline_id",
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

    def test_library_artist_route_includes_pipeline_only_requests(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id=self.RELEASE_ID,
            mb_release_group_id=self.RG_ID,
            mb_artist_id=self.ARTIST_ID,
            artist_name="Test Artist",
            album_title="Wanted Album",
            year=2024,
            country="US",
            format="CD",
            source="request",
            status="wanted",
            min_bitrate=320,
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
            search_filetype_override="flac",
        ))
        fake_db.set_tracks(42, [
            {"track_number": i + 1, "title": f"Track {i + 1}"}
            for i in range(10)
        ])

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        _assert_required_fields(self, data["albums"][0], self.LIBRARY_ALBUM_REQUIRED_FIELDS,
                                "pipeline-only library artist album")
        self.assertEqual(data["albums"][0]["album"], "Wanted Album")
        self.assertFalse(data["albums"][0]["in_library"])
        self.assertEqual(data["albums"][0]["pipeline_status"], "wanted")
        self.assertEqual(data["albums"][0]["pipeline_id"], 42)
        self.assertIsNone(data["albums"][0]["beets_album_id"])
        self.assertIsNone(data["albums"][0]["library_rank"])
        self.assertEqual(data["albums"][0]["release_group_title"], "Wanted Album")

    def test_library_artist_route_dedups_pipeline_row_when_beets_row_has_same_release_id(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id=self.RELEASE_ID,
            mb_release_group_id=self.RG_ID,
            mb_artist_id=self.ARTIST_ID,
            artist_name="Test Artist",
            album_title="Duplicate Pipeline Row",
            status="wanted",
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
        ))
        beets_album = {
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

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        self.assertEqual(data["albums"][0]["id"], 7)
        self.assertEqual(data["albums"][0]["pipeline_id"], 42)
        self.assertTrue(data["albums"][0]["in_library"])

    def test_library_artist_route_dedups_discogs_pipeline_row_when_beets_row_has_same_discogs_id(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=55,
            mb_release_id=None,
            discogs_release_id="12856590",
            mb_artist_id=None,
            artist_name="Test Artist",
            album_title="Discogs Import",
            source="request",
            status="wanted",
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
        ))
        beets_album = {
            "id": 8,
            "album": "Discogs Import",
            "artist": "Test Artist",
            "year": 2001,
            "mb_albumid": None,
            "discogs_albumid": "12856590",
            "track_count": 10,
            "mb_releasegroupid": None,
            "release_group_title": "Discogs Import",
            "added": 1773651902.0,
            "formats": "MP3",
            "min_bitrate": 320000,
            "type": "album",
            "label": "Test Label",
            "country": "AU",
            "source": "discogs",
        }

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        self.assertEqual(data["albums"][0]["id"], 8)
        self.assertEqual(data["albums"][0]["mb_albumid"], "12856590")
        self.assertEqual(data["albums"][0]["pipeline_id"], 55)
        self.assertTrue(data["albums"][0]["in_library"])

    def test_library_artist_route_ignores_discogs_zero_sentinel_on_blank_row(self):
        import web.server as srv

        beets_album = {
            "id": 10,
            "album": "Unknown Import",
            "artist": "Test Artist",
            "year": 2002,
            "mb_albumid": "",
            "discogs_albumid": "0",
            "track_count": 8,
            "mb_releasegroupid": None,
            "release_group_title": "Unknown Import",
            "added": 1773651904.0,
            "formats": "MP3",
            "min_bitrate": 192000,
            "type": "album",
            "label": "Test Label",
            "country": "AU",
            "source": "unknown",
        }

        with patch.object(srv, "db", FakePipelineDB()), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        self.assertIsNone(data["albums"][0]["mb_albumid"])
        self.assertIsNone(data["albums"][0]["pipeline_id"])

    def test_library_artist_route_sorts_merged_rows_after_dedup(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=50,
            mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            mb_release_group_id="22222222-2222-2222-2222-222222222222",
            mb_artist_id=self.ARTIST_ID,
            artist_name="Test Artist",
            album_title="Older Request",
            year=1997,
            status="wanted",
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
        ))
        beets_album = {
            "id": 9,
            "album": "Later Library Album",
            "artist": "Test Artist",
            "year": 2005,
            "mb_albumid": self.RELEASE_ID,
            "track_count": 11,
            "mb_releasegroupid": self.RG_ID,
            "release_group_title": "Later Library Album",
            "added": 1773651903.0,
            "formats": "MP3",
            "min_bitrate": 320000,
            "type": "album",
            "label": "Test Label",
            "country": "US",
            "source": "musicbrainz",
        }

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual([row["album"] for row in data["albums"]], [
            "Older Request",
            "Later Library Album",
        ])

    def test_artist_compare_contract(self):
        """Compare endpoint returns mb_artist, discogs_artist, and three buckets."""
        mb_rg = {
            "id": self.RG_ID,
            "title": "OK Computer",
            "type": "Album",
            "secondary_types": [],
            "first_release_date": "1997-05-21",
            "artist_credit": "Radiohead",
            "primary_artist_id": self.ARTIST_ID,
        }
        discogs_rg = {
            "id": "21491",
            "title": "OK Computer",
            "type": "Album",
            "secondary_types": [],
            "first_release_date": "1997",
            "artist_credit": "Radiohead",
            "primary_artist_id": "3840",
        }
        # server.py loads routes via `from routes import browse` (sys.path hack),
        # so the canonical module is `routes.browse`, not `web.routes.browse`.
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg:
            mock_mb.search_artists.return_value = [{"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [{"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"
            status, data = self._get("/api/artist/compare?name=Radiohead")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data,
            {"mb_artist", "discogs_artist", "both", "mb_only", "discogs_only"},
            "artist compare response",
        )
        # Same title + same year → matched
        self.assertEqual(len(data["both"]), 1)
        self.assertEqual(data["mb_only"], [])
        self.assertEqual(data["discogs_only"], [])
        self.assertEqual(data["both"][0]["mb"]["id"], self.RG_ID)
        self.assertEqual(data["both"][0]["discogs"]["id"], "21491")
        # Bootleg classification flows through to frontend.
        self.assertTrue(data["both"][0]["mb"]["has_official"])

    def test_artist_compare_marks_bootleg_only_rgs(self):
        """Release groups absent from get_official_release_group_ids land
        with has_official=False so the frontend can route them into the
        Bootleg-only collapsible section."""
        official_rg = {
            "id": self.RG_ID, "title": "Real Album", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Artist", "primary_artist_id": self.ARTIST_ID,
        }
        bootleg_rg = {
            "id": "00000000-0000-0000-0000-000000000099",
            "title": "Live Bootleg 99", "type": "Album",
            "secondary_types": [], "first_release_date": "1999",
            "artist_credit": "Artist", "primary_artist_id": self.ARTIST_ID,
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg:
            mock_mb.search_artists.return_value = [{"id": self.ARTIST_ID, "name": "Artist"}]
            mock_mb.get_artist_release_groups.return_value = [official_rg, bootleg_rg]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            mock_mb.get_artist_name.return_value = "Artist"
            mock_dg.search_artists.return_value = []
            mock_dg.get_artist_releases.return_value = []
            mock_dg.get_artist_name.return_value = ""
            status, data = self._get("/api/artist/compare?name=Artist")

        self.assertEqual(status, 200)
        # Both RGs land in mb_only (no Discogs counterpart). Both carry
        # has_official so the frontend can split them.
        self.assertEqual(len(data["mb_only"]), 2)
        by_id = {r["id"]: r for r in data["mb_only"]}
        self.assertTrue(by_id[self.RG_ID]["has_official"])
        self.assertFalse(by_id["00000000-0000-0000-0000-000000000099"]["has_official"])

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

    def test_artist_release_groups_in_library_when_name_passed(self):
        """When the frontend passes ?name=, each RG gets in_library: bool
        based on a beets lookup. Without name, the field stays absent
        (backwards-compatible)."""
        release_group = {
            "id": self.RG_ID, "title": "Owned Album", "type": "Album",
            "secondary_types": [], "first_release_date": "2024",
            "artist_credit": "Test Artist", "primary_artist_id": self.ARTIST_ID,
        }
        owned_album = {
            "mb_albumid": "00000000-0000-0000-0000-000000000001",
            "mb_releasegroupid": self.RG_ID,
            "album": "Owned Album",
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.get_library_artist", return_value=[owned_album]):
            mock_mb.get_artist_release_groups.return_value = [release_group]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            status, data = self._get(
                f"/api/artist/{self.ARTIST_ID}?name=Test%20Artist"
            )

        self.assertEqual(status, 200)
        self.assertTrue(data["release_groups"][0]["in_library"])

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
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {self.RELEASE_ID: 7}
        mock_beets.check_mbids_detail.return_value = {self.RELEASE_ID: {}}
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={self.RELEASE_ID}), \
                patch("web.server._beets_db", return_value=mock_beets), \
                patch("web.server.check_pipeline",
                      return_value={self.RELEASE_ID: {"id": 42, "status": "wanted"}}):
            mock_mb.get_release_group_releases.return_value = {"releases": [release]}
            status, data = self._get(f"/api/release-group/{self.RG_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"releases"}, "release group response")
        _assert_required_fields(self, data["releases"][0], self.RELEASE_GROUP_REQUIRED_FIELDS,
                                "release group release")
        self.assertEqual(data["releases"][0]["beets_album_id"], 7)

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
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {self.RELEASE_ID: 7}
        mock_beets.check_mbids_detail.return_value = {self.RELEASE_ID: {}}
        mock_beets.get_tracks_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=42, status="wanted", mb_release_id=self.RELEASE_ID,
        )
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={self.RELEASE_ID}), \
                patch("web.server._beets_db", return_value=mock_beets):
            mock_mb.get_release.return_value = release
            status, data = self._get(f"/api/release/{self.RELEASE_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.RELEASE_DETAIL_REQUIRED_FIELDS,
                                "release detail response")
        _assert_required_fields(self, data["tracks"][0], self.RELEASE_TRACK_REQUIRED_FIELDS,
                                "release detail track")
        self.assertEqual(data["beets_album_id"], 7)

    @patch("web.routes.browse.discogs_api.get_release")
    def test_release_detail_numeric_id_forwards_to_discogs(self, mock_discogs_get):
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {"12856590": 8}
        mock_beets.check_mbids_detail.return_value = {"12856590": {}}
        mock_beets.get_tracks_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = make_request_row(
            id=42, status="wanted", mb_release_id="12856590", discogs_release_id="12856590",
        )
        mock_discogs_get.return_value = {
            "id": "12856590",
            "title": "Discogs Album",
            "tracks": [
                {
                    "disc_number": 1,
                    "track_number": 1,
                    "title": "Track",
                    "length_seconds": 180,
                },
            ],
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={"12856590"}), \
                patch("web.server._beets_db", return_value=mock_beets):
            status, data = self._get("/api/release/0012856590")

        self.assertEqual(status, 200)
        mock_discogs_get.assert_called_once_with(12856590)
        mock_mb.get_release.assert_not_called()
        _assert_required_fields(self, data, self.RELEASE_DETAIL_REQUIRED_FIELDS,
                                "release detail response (discogs forward)")
        _assert_required_fields(self, data["tracks"][0], self.RELEASE_TRACK_REQUIRED_FIELDS,
                                "release detail track (discogs forward)")
        self.assertEqual(data["beets_album_id"], 8)

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
        "primary_type", "first_release_date",
    }
    DISCOGS_MASTER_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "country", "format",
        "in_library", "beets_album_id", "pipeline_status", "pipeline_id",
    }
    DISCOGS_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "artist_name", "tracks",
        "in_library", "beets_album_id", "pipeline_status", "pipeline_id",
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
                    "primary_type": "Album",
                    "first_release_date": "1997",
                    "artist_disambiguation": "",
                    "score": 9,
                    "is_master": True,
                    "discogs_release_id": "83182",
                },
            ]
            status, data = self._get("/api/discogs/search?q=ok+computer&type=release")

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
            mock_dg.get_artist_releases.return_value = [
                {
                    "id": "21491",
                    "title": "OK Computer",
                    "type": "Album",
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
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {"83182": 9}
        mock_beets.check_mbids_detail.return_value = {"83182": {}}
        with patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value={"83182"}), \
                patch("web.server._beets_db", return_value=mock_beets), \
                patch("web.server.check_pipeline", return_value={}):
            mock_dg.get_master_releases.return_value = {
                "title": "OK Computer",
                "type": "Album",
                "first_release_date": "1997",
                "artist_credit": "Radiohead",
                "primary_artist_id": "3840",
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
        self.assertEqual(data["releases"][0]["beets_album_id"], 9)

    def test_discogs_release_contract(self):
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {"83182": 10}
        mock_beets.check_mbids_detail.return_value = {"83182": {}}
        mock_beets.get_tracks_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        with patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value={"83182"}), \
                patch("web.server._beets_db", return_value=mock_beets):
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
        self.assertEqual(data["beets_album_id"], 10)


class TestSearchByIdResolveContract(_WebServerCase):
    """Contract tests for /api/browse/resolve — the search-by-ID resolver."""

    REQUIRED_FIELDS = {
        "source", "kind", "artist_id", "artist_name",
        "is_va", "expand_id", "leaf_id",
    }

    MB_RELEASE_ID = "c1f6a2c9-bcba-4e69-96f5-233c85b2830a"
    MB_RG_ID = "11111111-1111-1111-1111-111111111111"
    MB_ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"
    MB_VA_MBID = "89ad4ac3-39f7-470e-963a-56509c546377"

    def test_mb_release_resolved(self):
        """Happy path: ?source=mb&id=<mbid>&kind=release returns leaf shape."""
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_release.return_value = {
                "id": self.MB_RELEASE_ID,
                "title": "Test Release",
                "artist_id": self.MB_ARTIST_ID,
                "artist_name": "Test Artist",
                "release_group_id": self.MB_RG_ID,
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=release")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["source"], "mb")
        self.assertEqual(data["kind"], "release")
        self.assertEqual(data["artist_id"], self.MB_ARTIST_ID)
        self.assertEqual(data["artist_name"], "Test Artist")
        self.assertFalse(data["is_va"])
        self.assertEqual(data["expand_id"], self.MB_RG_ID)
        self.assertEqual(data["leaf_id"], self.MB_RELEASE_ID)

    def test_mb_release_group_resolved(self):
        """Happy path: ?source=mb&id=<mbid>&kind=release-group returns group shape."""
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_release_group.return_value = {
                "id": self.MB_RG_ID,
                "title": "Test RG",
                "artist_id": self.MB_ARTIST_ID,
                "artist_name": "Test Artist",
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RG_ID}&kind=release-group")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["kind"], "release-group")
        self.assertEqual(data["expand_id"], self.MB_RG_ID)
        self.assertIsNone(data["leaf_id"])

    def test_discogs_release_resolved_with_master(self):
        """Discogs release with non-null master_id → leaf shape, expand=master."""
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_release.return_value = {
                "id": "32457180",
                "title": "Rock Christmas",
                "artist_id": "194",
                "artist_name": "Various",
                "release_group_id": "3673686",
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=32457180&kind=release")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["source"], "discogs")
        self.assertEqual(data["kind"], "release")
        self.assertEqual(data["expand_id"], "3673686")
        self.assertEqual(data["leaf_id"], "32457180")
        # artists[0].id == 194 → VA
        self.assertTrue(data["is_va"])

    def test_discogs_master_resolved(self):
        """Discogs master ID → group shape, no leaf."""
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_master_releases.return_value = {
                "title": "Some Master",
                "type": "Album",
                "first_release_date": "1997",
                "artist_credit": "Real Artist",
                "primary_artist_id": "3840",
                "releases": [],
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=3673686&kind=master")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["kind"], "master")
        self.assertEqual(data["expand_id"], "3673686")
        self.assertIsNone(data["leaf_id"])
        self.assertFalse(data["is_va"])

    def test_discogs_masterless_release(self):
        """Masterless Discogs release: release_group_id is None → expand=leaf."""
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_release.return_value = {
                "id": "999",
                "title": "Masterless",
                "artist_id": "3840",
                "artist_name": "Some Artist",
                "release_group_id": None,
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=999&kind=release")

        self.assertEqual(status, 200)
        # When master_id is None, the bare release is its own expand target
        # so the artist view rings the masterless rg row in place.
        self.assertEqual(data["expand_id"], "999")
        self.assertEqual(data["leaf_id"], "999")

    def test_mb_va_release(self):
        """MB release whose artist matches VA_MBID → is_va: true."""
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_release.return_value = {
                "id": self.MB_RELEASE_ID,
                "title": "VA Comp",
                "artist_id": self.MB_VA_MBID,
                "artist_name": "Various Artists",
                "release_group_id": self.MB_RG_ID,
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=release")

        self.assertEqual(status, 200)
        self.assertTrue(data["is_va"])

    def test_unknown_kind_falls_back_mb_release_to_rg(self):
        """kind=unknown: leaf endpoint 404 → falls back to release-group."""
        with patch("web.server.mb_api") as mock_mb:
            from urllib.error import HTTPError
            mock_mb.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=None, fp=None)
            mock_mb.get_release_group.return_value = {
                "id": self.MB_RG_ID,
                "title": "RG",
                "artist_id": self.MB_ARTIST_ID,
                "artist_name": "Artist",
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RG_ID}&kind=unknown")

        self.assertEqual(status, 200)
        self.assertEqual(data["kind"], "release-group")
        # Confirms TWO upstream calls: release tried, then release-group
        self.assertEqual(mock_mb.get_release.call_count, 1)
        self.assertEqual(mock_mb.get_release_group.call_count, 1)

    def test_unknown_kind_falls_back_discogs_release_to_master(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            from urllib.error import HTTPError
            mock_dg.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=None, fp=None)
            mock_dg.get_master_releases.return_value = {
                "title": "M", "type": "Album", "first_release_date": "1997",
                "artist_credit": "Artist", "primary_artist_id": "3840",
                "releases": [],
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=3673686&kind=unknown")

        self.assertEqual(status, 200)
        self.assertEqual(data["kind"], "master")

    def test_kind_hint_release_does_not_probe_group_on_404(self):
        """kind=release explicit: 404 returns 404 immediately, no group probe.

        Guards the URL-disambiguation optimisation from regressing into
        always-probe-both behaviour. If the URL said 'release', we trust it.
        """
        with patch("web.server.mb_api") as mock_mb:
            from urllib.error import HTTPError
            mock_mb.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=None, fp=None)
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RG_ID}&kind=release")

        self.assertEqual(status, 404)
        # release-group endpoint MUST NOT have been called
        self.assertEqual(mock_mb.get_release_group.call_count, 0)

    def test_not_found_both_endpoints(self):
        with patch("web.server.mb_api") as mock_mb:
            from urllib.error import HTTPError
            mock_mb.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=None, fp=None)
            mock_mb.get_release_group.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=None, fp=None)
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=unknown")

        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_missing_id(self):
        status, data = self._get("/api/browse/resolve?source=mb")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_missing_source(self):
        status, data = self._get(f"/api/browse/resolve?id={self.MB_RELEASE_ID}")
        self.assertEqual(status, 400)

    def test_invalid_source(self):
        status, data = self._get(
            f"/api/browse/resolve?source=apple&id={self.MB_RELEASE_ID}")
        self.assertEqual(status, 400)

    def test_invalid_kind(self):
        status, data = self._get(
            f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=garbage")
        self.assertEqual(status, 400)


class TestLabelRouteContracts(_WebServerCase):
    """Contract tests for the Discogs label routes (Phase A)."""

    LABEL_HIT_REQUIRED_FIELDS = {
        "source", "id", "name", "country", "profile",
        "parent_label_id", "parent_label_name", "release_count",
    }
    # Required fields the frontend reads on each release row in the
    # label-detail response. Mirrors `web/js/discography.js` and
    # `web/js/badges.js`. The overlay sets `library_format` /
    # `library_min_bitrate` / `library_rank` only when a row is in
    # the beets library — same convention as the existing
    # `DISCOGS_MASTER_RELEASE_REQUIRED_FIELDS`. The JS reads them
    # defensively (`item.library_format || ''`), so the contract
    # asserts only the always-present overlay fields here, plus the
    # label-specific `sub_label_name`. The integration test below
    # exercises the populated path explicitly.
    LABEL_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "artist_name", "date", "format", "primary_type",
        "sub_label_name", "in_library", "beets_album_id",
        "pipeline_status", "pipeline_id",
    }
    LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS = {
        "label", "releases", "sub_labels", "pagination", "include_sublabels",
        "sub_labels_dropped",
    }

    def _make_label_entity(self, **overrides):
        """Build a `LabelEntity` with sensible defaults for tests."""
        from web.discogs import LabelEntity
        defaults = {
            "source": "discogs",
            "id": "757",
            "name": "Hymen Records",
            "country": None,
            "profile": "Industrial / IDM label",
            "parent_label_id": None,
            "parent_label_name": None,
            "release_count": 42,
        }
        defaults.update(overrides)
        return LabelEntity(**defaults)

    def _make_release_row(self, **overrides):
        """Build a release row matching `get_label_releases` adapter shape."""
        row = {
            "id": "1001",
            "title": "Roniwasp",
            "country": "Germany",
            "date": "2002-01-01",
            "year": 2002,
            "primary_type": "Album",
            "release_group_id": None,
            "master_title": None,
            "master_first_released": None,
            "artist_name": "Gridlock",
            "artist_id": "1234",
            "label_id": "757",
            "sub_label_name": None,
            "format": "CD",
            "media_count": 1,
            "labels": [],
            "formats": [],
        }
        row.update(overrides)
        return row

    def test_label_search_contract(self):
        """Search hits expose every disambiguation field the UI needs."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.search_labels.return_value = [
                self._make_label_entity(),
                self._make_label_entity(
                    id="999", name="Hymen Substream",
                    parent_label_id="757", parent_label_name="Hymen Records",
                    release_count=7),
            ]
            status, data = self._get("/api/discogs/label/search?q=hymen")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"results"}, "label search response")
        self.assertEqual(len(data["results"]), 2)
        for hit in data["results"]:
            _assert_required_fields(self, hit, self.LABEL_HIT_REQUIRED_FIELDS,
                                    "label search hit")
        self.assertEqual(data["results"][1]["parent_label_id"], "757")

    def test_label_search_missing_query(self):
        status, data = self._get("/api/discogs/label/search?q=")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_label_detail_contract(self):
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [self._make_release_row()],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 1},
                "include_sublabels": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data,
                                self.LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS,
                                "label detail response")
        _assert_required_fields(self, data["label"],
                                self.LABEL_HIT_REQUIRED_FIELDS,
                                "label detail entity")
        self.assertEqual(len(data["releases"]), 1)
        _assert_required_fields(self, data["releases"][0],
                                self.LABEL_RELEASE_REQUIRED_FIELDS,
                                "label release row")

    def test_label_detail_forwards_sub_labels(self):
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity(
                sub_labels=[
                    {"id": 25693, "name": "Hymen Substream", "release_count": 7},
                ]
            )
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        self.assertEqual(data["sub_labels"], [
            {"id": 25693, "name": "Hymen Substream", "release_count": 7},
        ])

    def test_label_detail_overlay_integration(self):
        """End-to-end overlay: with one release in library AND one in
        pipeline, both rows are correctly annotated. This is the test
        that proves the overlay actually runs — not just that helpers
        were called."""
        held_id = "1001"
        in_pipeline_id = "1002"
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {held_id: 17}
        mock_beets.check_mbids_detail.return_value = {
            held_id: {"beets_format": "FLAC", "beets_bitrate": 1100},
        }

        def _compute_rank(fmt, br):
            return "lossless" if fmt == "FLAC" else "transparent"

        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library",
                      return_value={held_id}), \
                patch("web.server.check_pipeline",
                      return_value={in_pipeline_id: {"id": 99, "status": "wanted"}}), \
                patch("web.server._beets_db", return_value=mock_beets), \
                patch("web.server.compute_library_rank",
                      side_effect=_compute_rank):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [
                    self._make_release_row(id=held_id, title="Roniwasp"),
                    self._make_release_row(
                        id=in_pipeline_id, title="Formless",
                        sub_label_name="Hymen Substream"),
                ],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 2},
                "include_sublabels": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        held_row = next(r for r in data["releases"] if r["id"] == held_id)
        pipeline_row = next(r for r in data["releases"] if r["id"] == in_pipeline_id)

        # In-library row: overlay populated, pipeline empty
        self.assertTrue(held_row["in_library"])
        self.assertEqual(held_row["beets_album_id"], 17)
        self.assertEqual(held_row["library_format"], "FLAC")
        self.assertEqual(held_row["library_min_bitrate"], 1100)
        self.assertEqual(held_row["library_rank"], "lossless")
        self.assertIsNone(held_row["pipeline_status"])
        self.assertIsNone(held_row["pipeline_id"])

        # In-pipeline row: pipeline populated, library empty
        self.assertFalse(pipeline_row["in_library"])
        self.assertIsNone(pipeline_row["beets_album_id"])
        self.assertEqual(pipeline_row["pipeline_status"], "wanted")
        self.assertEqual(pipeline_row["pipeline_id"], 99)
        self.assertEqual(pipeline_row["sub_label_name"], "Hymen Substream")

    def test_label_detail_404(self):
        """Adapter raises HTTPError(404) → route returns 404 JSON, not 5xx."""
        from urllib.error import HTTPError
        from io import BytesIO

        def _raise_404(_label_id):
            raise HTTPError(
                "https://discogs.ablz.au/api/labels/99999999",
                404, "Not Found", hdrs=None, fp=BytesIO(b""),  # type: ignore[arg-type]
            )

        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.side_effect = _raise_404
            status, data = self._get("/api/discogs/label/99999999")

        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_label_detail_include_sublabels_param_forwarded(self):
        """`?include_sublabels=false` flows through to the adapter call."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": False,
            }
            status, _data = self._get("/api/discogs/label/757?include_sublabels=false")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=False, page=1, per_page=100)

    def test_label_detail_auto_flips_include_sublabels_for_big_labels(self):
        """Big label (release_count > BIG_LABEL_THRESHOLD) without an
        explicit `include_sublabels=` query param auto-flips to False so
        the recursive sub-label CTE never hits the upstream timeout."""
        big_entity = self._make_label_entity(
            id="1", name="Universal Music Group", release_count=5000)
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = big_entity
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": False,
            }
            status, _data = self._get("/api/discogs/label/1")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "1", include_sublabels=False, page=1, per_page=100)

    def test_label_detail_respects_explicit_include_sublabels_on_big_labels(self):
        """If the caller explicitly opts in via `?include_sublabels=true`,
        the auto-flip MUST NOT override their choice — even for big
        labels. This is the API consumer's escape hatch."""
        big_entity = self._make_label_entity(
            id="1", name="Universal Music Group", release_count=5000)
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = big_entity
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
            }
            status, _data = self._get("/api/discogs/label/1?include_sublabels=true")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "1", include_sublabels=True, page=1, per_page=100)

    def test_label_detail_does_not_auto_flip_small_labels(self):
        """Boutique labels (release_count <= threshold) keep the
        default `include_sublabels=True` even with no explicit param."""
        small_entity = self._make_label_entity(
            id="757", name="Hymen Records", release_count=42)
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = small_entity
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
            }
            status, _data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=True, page=1, per_page=100)

    def test_label_detail_rejects_malformed_include_sublabels(self):
        """`?include_sublabels=` must be one of true/false/1/0 (case-
        insensitive). Anything else → 400. Silently coercing typos
        masks frontend bugs and lets bots pollute caches."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get("/api/discogs/label/757?include_sublabels=yes")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        # And get_label_releases should NEVER be called when the param is bad.
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_accepts_truthy_synonyms(self):
        """`include_sublabels=1` and `=0` are valid spellings."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": False,
            }
            status, _data = self._get("/api/discogs/label/757?include_sublabels=0")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=False, page=1, per_page=100)

    def test_label_detail_releases_404_propagates(self):
        """If `get_label` succeeds but `get_label_releases` raises 404
        (label vanished mid-flight), surface 404 to the client — not a
        generic 500."""
        from urllib.error import HTTPError
        from io import BytesIO

        def _raise_404(_label_id, **_kwargs):
            raise HTTPError(
                "https://discogs.ablz.au/api/labels/757/releases",
                404, "Not Found", hdrs=None, fp=BytesIO(b""),  # type: ignore[arg-type]
            )

        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.side_effect = _raise_404
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_label_detail_forwards_pagination_params(self):
        """`?page=2&per_page=50` flows through to the adapter — Plan 003 U1."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 2, "per_page": 50, "pages": 3, "items": 120},
                "include_sublabels": True,
            }
            status, data = self._get(
                "/api/discogs/label/757?page=2&per_page=50")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=True, page=2, per_page=50)
        self.assertEqual(data["pagination"]["page"], 2)
        self.assertEqual(data["pagination"]["per_page"], 50)

    def test_label_detail_clamps_per_page(self):
        """`?per_page=500` clamps to the mirror's 100-row label-release max."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 0},
                "include_sublabels": True,
            }
            status, _data = self._get(
                "/api/discogs/label/757?per_page=500")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=True, page=1, per_page=100)

    def test_label_detail_rejects_non_integer_page(self):
        """`?page=foo` returns 400 — silently coercing to 1 would mask
        frontend pagination bugs."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?page=foo")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_rejects_zero_page(self):
        """`?page=0` returns 400 — pages are 1-indexed."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?page=0")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_rejects_non_integer_per_page(self):
        """`?per_page=foo` returns 400."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?per_page=foo")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_rejects_zero_per_page(self):
        """`?per_page=0` returns 400 — would otherwise cause divide-by-zero
        on the pages calculation."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?per_page=0")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_forwards_sub_labels_dropped(self):
        """Plan 002 U3: when the adapter signals a 503 fallback, the route
        forwards `sub_labels_dropped=True` so the UI can surface a banner."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 0},
                "include_sublabels": False,
                "sub_labels_dropped": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        self.assertTrue(data["sub_labels_dropped"])

    def test_label_detail_default_sub_labels_dropped_false(self):
        """Plan 002 U3: every label-detail response carries the field with
        default False so the contract is stable."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
                "sub_labels_dropped": False,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        self.assertIn("sub_labels_dropped", data)
        self.assertFalse(data["sub_labels_dropped"])


class TestBeetsRouteContracts(_WebServerCase):
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
        "downloaded_label", "verdict", "disambiguation_failure",
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
        self.mock_db.get_request_by_discogs_release_id.return_value = None

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
        mock_beets_cls: MagicMock,
        *,
        delete_side_effect: object | None = None,
    ) -> None:
        beets = mock_beets_cls.return_value.__enter__.return_value
        beets.get_album_detail.return_value = {"id": 7}
        if delete_side_effect is not None:
            mock_beets_cls.delete_album.side_effect = delete_side_effect
            return
        mock_beets_cls.delete_album.return_value = (
            "Test Album",
            "Test Artist",
            ["/music/Test Artist/Test Album/01 Track.mp3"],
        )

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
        detail["artpath"] = "/music/Test Artist/Test Album/cover.jpg"
        detail["path"] = "/music/Test Artist/Test Album"
        detail["tracks"] = [self._track()]
        self.beets.get_album_detail.return_value = detail

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
        self.beets.get_album_detail.return_value = detail
        self.mock_db.get_request_by_discogs_release_id.return_value = make_request_row(
            id=42,
            status="wanted",
            mb_release_id="12856590",
            discogs_release_id="12856590",
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
        self.beets.get_album_detail.return_value = detail

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
        self.beets.get_album_detail.return_value = detail

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
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        self._srv.beets_db_path = "/tmp/beets.db"
        self._configure_beets_delete_mock(mock_beets_cls)

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
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(mock_beets_cls)

        with patch.object(srv, "db", fake_db):
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
        self.assertIsNone(fake_db.get_request(42))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purges_pipeline_request_by_release_id_fallback(
        self,
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=99, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(mock_beets_cls)

        with patch.object(srv, "db", fake_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
                "purge_pipeline": True,
                "release_id": self.RELEASE_ID,
            })

        self.assertEqual(status, 200)
        self.assertTrue(data["pipeline_deleted"])
        self.assertEqual(data["pipeline_id"], 99)
        self.assertIsNone(fake_db.get_request(99))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purges_pipeline_request_by_uppercase_release_id(
        self,
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=98, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(mock_beets_cls)

        with patch.object(srv, "db", fake_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
                "purge_pipeline": True,
                "release_id": self.RELEASE_ID.upper(),
            })

        self.assertEqual(status, 200)
        self.assertTrue(data["pipeline_deleted"])
        self.assertEqual(data["pipeline_id"], 98)
        self.assertIsNone(fake_db.get_request(98))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_without_purge_pipeline_leaves_request_intact(
        self,
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(mock_beets_cls)

        with patch.object(srv, "db", fake_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
            })

        self.assertEqual(status, 200)
        self.assertFalse(data["pipeline_deleted"])
        self.assertIsNone(data["pipeline_id"])
        self.assertIsNotNone(fake_db.get_request(42))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purge_with_no_pipeline_context_is_noop(
        self,
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        fake_db = FakePipelineDB()
        self._configure_beets_delete_mock(mock_beets_cls)

        with patch.object(srv, "db", fake_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
                "purge_pipeline": True,
            })

        self.assertEqual(status, 200)
        self.assertFalse(data["pipeline_deleted"])
        self.assertIsNone(data["pipeline_id"])

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_purges_discogs_request_by_numeric_release_id_fallback(
        self,
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=77,
            mb_release_id=None,
            discogs_release_id="12856590",
            status="imported",
        ))
        self._configure_beets_delete_mock(mock_beets_cls)

        with patch.object(srv, "db", fake_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
                "purge_pipeline": True,
                "release_id": "12856590",
            })

        self.assertEqual(status, 200)
        self.assertTrue(data["pipeline_deleted"])
        self.assertEqual(data["pipeline_id"], 77)
        self.assertIsNone(fake_db.get_request(77))

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_pipeline_failure_aborts_before_beets_delete(
        self,
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        self._configure_beets_delete_mock(mock_beets_cls)
        failing_db = MagicMock()
        failing_db.get_request.return_value = make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        )
        failing_db.delete_request.side_effect = RuntimeError("boom")

        with patch.object(srv, "db", failing_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
                "purge_pipeline": True,
                "pipeline_id": 42,
                "release_id": self.RELEASE_ID,
            })

        self.assertEqual(status, 500)
        self.assertIn("error", data)
        mock_beets_cls.delete_album.assert_not_called()

    @patch("lib.library_delete_service.os.path.isdir", return_value=False)
    @patch("lib.library_delete_service.os.path.isfile", return_value=False)
    @patch("lib.library_delete_service.os.path.exists", return_value=True)
    @patch("lib.beets_db.BeetsDB")
    def test_beets_delete_failure_after_pipeline_purge_returns_targeted_error(
        self,
        mock_beets_cls,
        _mock_exists,
        _mock_isfile,
        _mock_isdir,
    ):
        import web.server as srv

        self._srv.beets_db_path = "/tmp/beets.db"
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        ))
        self._configure_beets_delete_mock(
            mock_beets_cls,
            delete_side_effect=OSError("boom"),
        )

        with patch.object(srv, "db", fake_db):
            status, data = self._post("/api/beets/delete", {
                "id": 7,
                "confirm": "DELETE",
                "purge_pipeline": True,
                "pipeline_id": 42,
                "release_id": self.RELEASE_ID,
            })

        self.assertEqual(status, 500)
        self.assertIn("Pipeline request was removed", data["error"])
        self.assertIsNone(fake_db.get_request(42))


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
    """Contract tests: /api/wrong-matches returns grouped-by-release shape.

    Issue #113: every rejection with a failed_path must be reachable. The
    route returns ``{groups: [{request_id, artist, album, mb_release_id,
    in_library, pending_count, entries: [...]}]}`` so the frontend can
    collapse by release and expand to per-candidate actions.
    """

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
        self.mock_db.get_request.return_value = copy.deepcopy(_MOCK_PIPELINE_REQUEST)
        self.mock_db.get_wrong_matches.return_value = [copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)]
        self.mock_db.get_download_log_entry.reset_mock()
        self.mock_db.get_download_log_entry.side_effect = None
        self.mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
        self.mock_db.clear_wrong_match_path.reset_mock()
        self.mock_db.clear_wrong_match_path.return_value = True
        self.mock_db.clear_wrong_match_paths.reset_mock()
        self.mock_db.clear_wrong_match_paths.return_value = 1
        self.mock_db.enqueue_import_job.reset_mock()
        self.mock_db.enqueue_import_job.side_effect = None
        self.mock_db.enqueue_import_job.return_value = self._job(
            77, 100, 42, "/mnt/virtio/music/slskd/failed_imports/Test")
        self.mock_db.get_download_history_batch.return_value = {}
        # Default: treat every failed_path as existing so the group survives
        # filtering. Individual tests override this to exercise missing-file
        # and mixed-existence cases. Also stub rmtree so delete tests don't
        # touch the real filesystem.
        resolve_patch = patch("web.routes.imports.resolve_failed_path",
                              side_effect=lambda p: p if p else None)
        rmtree_patch = patch("web.routes.imports.shutil.rmtree")
        self.mock_resolve_failed_path = resolve_patch.start()
        self.mock_rmtree = rmtree_patch.start()
        self.addCleanup(resolve_patch.stop)
        self.addCleanup(rmtree_patch.stop)

    GROUP_REQUIRED_FIELDS = {
        "request_id", "artist", "album", "mb_release_id",
        "in_library", "pending_count", "entries",
        # Quality summary for the collapsed card (issue: "show quality on disk").
        "status", "min_bitrate", "format", "verified_lossless",
        "current_spectral_grade", "current_spectral_bitrate",
        "quality_label", "quality_rank",
        # Summary of the last successful import for the request — tells the
        # user what's actually on disk, not the most recent attempt.
        "latest_import",
    }
    ENTRY_REQUIRED_FIELDS = {
        "download_log_id", "soulseek_username", "failed_path", "files_exist",
        "distance", "scenario", "detail", "candidate", "local_items",
        # Per-candidate stored evidence (R1+R2 of the spectral-evidence
        # plan) — surfaced from download_log so the operator can eyeball
        # candidates by audio quality. Always present in the payload;
        # values are None when the underlying row lacks evidence.
        "spectral_grade", "spectral_bitrate",
        "v0_probe_kind", "v0_probe_avg_bitrate",
    }

    GROUP_FIELD_TYPES = {
        "request_id": int,
        "artist": str,
        "album": str,
        "in_library": bool,
        "pending_count": int,
        "entries": list,
        "status": str,
        "verified_lossless": bool,
    }
    ENTRY_FIELD_TYPES = {
        "download_log_id": int,
        "failed_path": str,
        "files_exist": bool,
        "distance": (int, float, type(None)),
    }

    def _row(self, download_log_id: int, request_id: int, username: str,
             failed_path: str, artist: str = "Test Artist",
             album: str = "Test Album",
             mb_release_id: str | None = "abc-123",
             scenario: str = "high_distance",
             distance: float = 0.25) -> dict:
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["download_log_id"] = download_log_id
        row["request_id"] = request_id
        row["artist_name"] = artist
        row["album_title"] = album
        row["mb_release_id"] = mb_release_id
        row["soulseek_username"] = username
        row["validation_result"]["failed_path"] = failed_path
        row["validation_result"]["scenario"] = scenario
        row["validation_result"]["distance"] = distance
        row["validation_result"]["candidates"][0]["distance"] = distance
        return row

    def _entry(self, download_log_id: int, request_id: int,
               failed_path: str) -> dict:
        return {
            "id": download_log_id,
            "request_id": request_id,
            "validation_result": {
                "failed_path": failed_path,
                "scenario": "high_distance",
            },
        }

    def _job(self, job_id: int, request_id: int, download_log_id: int,
             failed_path: str, *, deduped: bool = False) -> ImportJob:
        return ImportJob(
            id=job_id,
            job_type="force_import",
            status="queued",
            request_id=request_id,
            dedupe_key=f"force_import:download_log:{download_log_id}",
            payload={
                "download_log_id": download_log_id,
                "failed_path": failed_path,
            },
            result=None,
            message="Import queued",
            error=None,
            attempts=0,
            worker_id=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            started_at=None,
            heartbeat_at=None,
            completed_at=None,
            deduped=deduped,
        )

    def test_response_has_groups(self):
        """RED for issue #113: payload must be {groups: [...]}, not {entries: [...]}."""
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertIn("groups", data,
                      "Response must expose a `groups` array keyed by release.")

    def test_group_has_required_fields_and_types(self):
        status, data = self._get("/api/wrong-matches")
        self.assertGreater(len(data["groups"]), 0)
        for group in data["groups"]:
            _assert_required_fields(
                self, group, self.GROUP_REQUIRED_FIELDS,
                f"group request={group.get('request_id')}")
            for field, expected_type in self.GROUP_FIELD_TYPES.items():
                self.assertIsInstance(
                    group[field], expected_type,
                    f"group.{field}={group[field]!r} should be {expected_type}")

    def test_entry_has_required_fields_and_types(self):
        status, data = self._get("/api/wrong-matches")
        for group in data["groups"]:
            self.assertGreater(len(group["entries"]), 0)
            for entry in group["entries"]:
                _assert_required_fields(
                    self, entry, self.ENTRY_REQUIRED_FIELDS,
                    f"entry dl_id={entry.get('download_log_id')}")
                for field, expected_type in self.ENTRY_FIELD_TYPES.items():
                    self.assertIsInstance(
                        entry[field], expected_type,
                        f"entry.{field}={entry[field]!r} should be {expected_type}")

    def test_entry_surfaces_stored_spectral_and_v0_probe_evidence(self):
        """Covers AE1 — per-candidate stored evidence reaches the row payload.

        Plumbs the four per-attempt download_log columns
        (spectral_grade/spectral_bitrate/v0_probe_kind/v0_probe_avg_bitrate)
        from get_wrong_matches() through to the entry dict so the operator
        can eyeball candidates by audio quality.
        """
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["spectral_grade"] = "suspect"
        row["spectral_bitrate"] = 320
        row["v0_probe_kind"] = "lossless_source_v0"
        row["v0_probe_avg_bitrate"] = 265
        self.mock_db.get_wrong_matches.return_value = [row]

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["spectral_grade"], "suspect")
        self.assertEqual(entry["spectral_bitrate"], 320)
        self.assertEqual(entry["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(entry["v0_probe_avg_bitrate"], 265)

    def test_entry_evidence_keys_present_when_null(self):
        """Covers AE2 — missing evidence is missing data, not a trigger.

        Legacy rows lacking spectral and V0 probe evidence still produce
        the four keys with ``None`` values (never absent), and the entry
        payload exposes no preview action / preview button / async
        preview hook (R3 — this feature does not introduce a preview
        workflow).
        """
        # _DEFAULT_WRONG_MATCH_ROW already has all four evidence fields
        # set to None; this test pins that the resulting entry mirrors that.
        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        for field in ("spectral_grade", "spectral_bitrate",
                      "v0_probe_kind", "v0_probe_avg_bitrate"):
            self.assertIn(field, entry)
            self.assertIsNone(entry[field])
        # R3 regression guard: no preview-related keys leak into the
        # entry dict as part of this feature.
        for key in entry.keys():
            self.assertFalse(
                key.lower().startswith("preview"),
                f"entry exposed unexpected preview-related key: {key!r}")

    def test_multiple_rejections_for_same_request_collapse_to_single_group(self):
        """RED for issue #113: 3 rejections on one request → 1 group with 3 entries."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(3584, 515, "ascalaphid", "/fi/path_9"),
            self._row(3565, 515, "gatybfb",    "/fi/path_8"),
            self._row(3559, 515, "jazzush",    "/fi/path_7"),
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        groups = data["groups"]
        self.assertEqual(len(groups), 1,
                         "3 rejections on one request must collapse to 1 group.")
        group = groups[0]
        self.assertEqual(group["request_id"], 515)
        self.assertEqual(len(group["entries"]), 3)
        self.assertEqual(group["pending_count"], 3)
        ids = [e["download_log_id"] for e in group["entries"]]
        self.assertEqual(ids, [3584, 3565, 3559],
                         "Entries must be ordered newest download_log_id first.")

    def test_multiple_releases_return_separate_groups(self):
        self.mock_db.get_wrong_matches.return_value = [
            self._row(200, 1, "u1", "/fi/a", artist="A1", album="B1",
                      mb_release_id="mb-1"),
            self._row(201, 1, "u2", "/fi/b", artist="A1", album="B1",
                      mb_release_id="mb-1"),
            self._row(300, 2, "u3", "/fi/c", artist="A2", album="B2",
                      mb_release_id="mb-2"),
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        groups = data["groups"]
        self.assertEqual(len(groups), 2)
        by_req = {g["request_id"]: g for g in groups}
        self.assertEqual(len(by_req[1]["entries"]), 2)
        self.assertEqual(len(by_req[2]["entries"]), 1)

    @patch("web.server.check_beets_library_detail",
           return_value={"abc-123": {"beets_format": "MP3",
                                     "beets_bitrate": 207,
                                     "beets_tracks": 12}})
    def test_group_shows_current_quality_when_imported(self, _mock_beets):
        """Imported album: quality_label, quality_rank, verified_lossless reflect on-disk state."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 207
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "genuine"
        row["request_imported_path"] = "/mnt/virtio/Music/Beets/Artist/Album"
        self.mock_db.get_wrong_matches.return_value = [row]
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertEqual(group["status"], "imported")
        self.assertEqual(group["min_bitrate"], 207)
        self.assertTrue(group["verified_lossless"])
        self.assertEqual(group["current_spectral_grade"], "genuine")
        self.assertEqual(group["format"], "MP3")
        # `quality_label` is bitrate-only: 207 kbps lands in the V2 band on
        # the label function (V0 starts at ≥220). The rank is independent —
        # it applies `compute_library_rank` which uses the codec-aware tiers.
        self.assertIsInstance(group["quality_label"], str)
        self.assertTrue(group["quality_label"].startswith("MP3"))
        self.assertIsInstance(group["quality_rank"], str)

    def test_group_shows_nothing_on_disk_when_wanted(self):
        """Wanted album: no files in library yet — fields are null, label signals 'not on disk'."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        row["request_status"] = "wanted"
        row["request_min_bitrate"] = None
        row["request_verified_lossless"] = False
        self.mock_db.get_wrong_matches.return_value = [row]
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertEqual(group["status"], "wanted")
        self.assertIsNone(group["min_bitrate"])
        self.assertFalse(group["verified_lossless"])
        # No on-disk state → label and rank may be None; the frontend can render
        # a 'not on disk' badge from `status` and absent label.
        self.assertTrue(group["quality_label"] is None or isinstance(group["quality_label"], str))

    def test_group_hides_stale_quality_when_not_in_beets(self):
        """Pipeline DB can hold stale on-disk fields after beet remove.

        After a ban-source path, ``album_requests`` rows can keep the
        ``min_bitrate`` / ``current_spectral_*`` values from a prior import
        even though ``beet remove -d`` has wiped the files. The wrong-matches
        card must not surface those ghost fields — otherwise the user sees
        "320k likely_transcode" for a release with nothing on disk and
        force-imports based on false quality data.
        """
        row = self._row(42, 100, "testuser", "/fi/Test")
        row["request_status"] = "wanted"
        row["request_min_bitrate"] = 320                  # stale
        row["request_verified_lossless"] = False
        row["request_current_spectral_grade"] = "likely_transcode"  # stale
        row["request_current_spectral_bitrate"] = 160                # stale
        self.mock_db.get_wrong_matches.return_value = [row]
        # No beets mock — _is_in_beets returns False, so every on-disk
        # field in the response should reflect "nothing on disk".
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        group = data["groups"][0]
        self.assertFalse(group["in_library"],
                         "Precondition: test requires album absent from beets.")
        self.assertIsNone(group["min_bitrate"],
                          "min_bitrate must not leak from stale DB when not in beets.")
        self.assertIsNone(group["current_spectral_grade"],
                          "current_spectral_grade must not leak from stale DB.")
        self.assertIsNone(group["current_spectral_bitrate"],
                          "current_spectral_bitrate must not leak from stale DB.")
        self.assertFalse(group["verified_lossless"],
                         "verified_lossless must read False when nothing is on disk.")

    @patch("web.server.check_beets_by_artist_album",
           create=True, return_value=12)
    @patch("web.server.check_beets_library_detail", return_value={})
    def test_group_in_library_false_when_mbid_not_in_beets(
            self, _mock_detail, _mock_fuzzy):
        """No exact MBID hit → ``in_library`` is False, quality blanks.

        Issue #123: the old behavior was a fuzzy artist+album fallback
        that turned on the badge for a sibling pressing match. That
        conflated identity and presence and silently attributed stale
        pipeline DB quality fields to whatever row fuzzy happened to
        catch. After deleting the fuzzy path, 'in library' means
        'beets holds this exact release ID' and nothing else.

        The fuzzy shim is mocked with ``create=True`` so the test is
        RED against the current code (which would call it and flip the
        badge on) and GREEN after the deletion (the call site vanishes,
        so the mock sits unused). If a user has an untagged legacy copy
        of the album, the honest UI answer is 'not in library' — re-tag
        it or add it to the pipeline.
        """
        row = self._row(42, 100, "testuser", "/fi/Test",
                         mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 245
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "genuine"
        row["request_current_spectral_bitrate"] = None
        self.mock_db.get_wrong_matches.return_value = [row]

        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        group = data["groups"][0]
        self.assertFalse(
            group["in_library"],
            "Issue #123: no exact ID match → in_library False "
            "(fuzzy fallback was deleted).")
        self.assertIsNone(group["min_bitrate"])
        self.assertFalse(group["verified_lossless"])
        self.assertIsNone(group["current_spectral_grade"])
        self.assertIsNone(group["quality_label"])
        self.assertIsNone(group["quality_rank"])

    @patch("web.server.check_beets_by_artist_album",
           create=True, return_value=12)
    @patch("web.server.check_beets_library_detail", return_value={})
    def test_group_in_library_false_for_mbidless_request(
            self, _mock_detail, _mock_fuzzy):
        """Request with no MBID → always ``in_library`` False (issue #123).

        A request that never had an MBID (edge case — shouldn't happen
        in current flows but persists in old rows) cannot pattern-match
        anything exact. After fuzzy deletion, the only honest answer
        is 'not in library' — even if a fuzzy artist+album shim would
        have returned a match (mocked here with ``create=True`` so the
        test is RED against the current code).
        """
        row = self._row(42, 100, "testuser", "/fi/Test", mb_release_id=None)
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 245
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "genuine"
        self.mock_db.get_wrong_matches.return_value = [row]

        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        group = data["groups"][0]
        self.assertFalse(group["in_library"])
        self.assertIsNone(group["min_bitrate"])
        self.assertFalse(group["verified_lossless"])
        self.assertIsNone(group["current_spectral_grade"])

    def test_group_latest_import_picks_most_recent_success(self):
        """latest_import shows the last successful import, not the newest attempt.

        A rejection that happened after a successful import doesn't change what
        beets has — the earlier success is still what's on disk.
        """
        row = self._row(42, 100, "testuser", "/fi/Test")
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_history_batch.return_value = {
            100: [
                # Newest = rejected (a later force-import attempt that failed).
                {"id": 999, "outcome": "rejected",
                 "created_at": "2026-04-19T09:00:00+00:00",
                 "soulseek_username": "newestuser",
                 "actual_filetype": "mp3", "actual_min_bitrate": 192,
                 "beets_scenario": "high_distance"},
                # Then an older force_import — this is what's actually on disk.
                {"id": 900, "outcome": "force_import",
                 "created_at": "2026-04-10T09:00:00+00:00",
                 "soulseek_username": "forceuser",
                 "actual_filetype": "mp3", "actual_min_bitrate": 207,
                 "beets_scenario": "force_import"},
                {"id": 800, "outcome": "success",
                 "created_at": "2026-03-10T12:00:00+00:00",
                 "soulseek_username": "olderuser",
                 "actual_filetype": "flac", "actual_min_bitrate": 900},
            ],
        }
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        latest = group["latest_import"]
        self.assertIsNotNone(latest)
        self.assertEqual(latest["id"], 900,
                         "Must pick the most recent success/force/manual import, "
                         "not the newest rejection.")
        self.assertEqual(latest["outcome"], "force_import")
        self.assertEqual(latest["soulseek_username"], "forceuser")

    def test_group_latest_import_none_when_never_imported(self):
        """Release that has only rejections → latest_import is None."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_history_batch.return_value = {
            100: [
                {"id": 999, "outcome": "rejected",
                 "created_at": "2026-04-19T09:00:00+00:00",
                 "soulseek_username": "u1"},
                {"id": 998, "outcome": "timeout",
                 "created_at": "2026-04-18T09:00:00+00:00",
                 "soulseek_username": "u2"},
            ],
        }
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertIsNone(group["latest_import"])

    def test_group_latest_import_none_when_batch_empty(self):
        """Edge case: no history rows at all → latest_import is None."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_history_batch.return_value = {}
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertIsNone(group["latest_import"])

    def test_group_dropped_when_no_entries_have_existing_files(self):
        """If every entry's files are gone, the group is excluded from the UI."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(10, 5, "u1", "/gone/a"),
            self._row(11, 5, "u2", "/gone/b"),
        ]
        with patch("web.routes.imports.resolve_failed_path", return_value=None):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertEqual(data["groups"], [])

    def test_group_pending_count_reflects_existing_entries_only(self):
        """pending_count counts entries with files still on disk."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(20, 7, "present", "/on-disk/a"),
            self._row(21, 7, "missing", "/gone/b"),
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p if p.startswith("/on-disk") else None):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        groups = data["groups"]
        self.assertEqual(len(groups), 1)
        group = groups[0]
        self.assertEqual(group["pending_count"], 1)
        self.assertEqual([e["download_log_id"] for e in group["entries"]], [20])

    def test_candidate_has_distance_breakdown(self):
        status, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
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

    @patch("web.routes.imports.resolve_failed_path",
           return_value="/mnt/virtio/music/slskd/failed_imports/Test")
    def test_relative_failed_path_uses_resolved_path(self, _mock_resolve):
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["validation_result"]["failed_path"] = "failed_imports/Test"
        self.mock_db.get_wrong_matches.return_value = [row]

        status, data = self._get("/api/wrong-matches")

        self.assertEqual(status, 200)
        entry = data["groups"][0]["entries"][0]
        self.assertTrue(entry["files_exist"])
        self.assertEqual(entry["failed_path"],
                         "/mnt/virtio/music/slskd/failed_imports/Test")

    @patch("web.routes.imports.shutil.rmtree")
    @patch("web.routes.imports.resolve_failed_path",
           return_value="/mnt/virtio/music/slskd/failed_imports/Test")
    def test_delete_relative_failed_path_removes_resolved_directory(
            self, _mock_resolve, mock_rmtree):
        entry = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
        entry["validation_result"]["failed_path"] = "failed_imports/Test"
        self.mock_db.get_download_log_entry.return_value = entry

        status, data = self._post("/api/wrong-matches/delete", {"download_log_id": 42})

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        mock_rmtree.assert_called_once_with(
            "/mnt/virtio/music/slskd/failed_imports/Test", ignore_errors=True)

    def test_delete_group_missing_request_id_returns_error(self):
        status, data = self._post("/api/wrong-matches/delete-group", {})
        self.assertEqual(status, 400)

    def test_delete_group_removes_every_candidate_for_request(self):
        """Bulk delete: every wrong-match entry for the given request_id is removed."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a"),
            self._row(101, 42, "u2", "/fi/b"),
            self._row(102, 42, "u3", "/fi/c"),
            self._row(200, 99, "u-other", "/fi/other"),  # different request
        ]
        self.mock_db.get_download_log_entry.side_effect = lambda lid: (
            copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
        )
        self.mock_db.clear_wrong_match_path.return_value = True

        status, data = self._post(
            "/api/wrong-matches/delete-group", {"request_id": 42})

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["request_id"], 42)
        self.assertEqual(data["deleted"], 3,
                         "All three candidates for request 42 should delete "
                         "(request 99 must be left alone).")
        # clear_wrong_match_path called once per candidate in the group, not
        # for the unrelated row.
        called_ids = {c.args[0] for c in self.mock_db.clear_wrong_match_path.call_args_list}
        self.assertEqual(called_ids, {100, 101, 102})

    def test_delete_group_zero_matches_still_succeeds(self):
        """Idempotent: calling delete-group for a request with no candidates returns deleted=0."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a"),
        ]
        status, data = self._post(
            "/api/wrong-matches/delete-group", {"request_id": 999})
        self.assertEqual(status, 200)
        self.assertEqual(data["deleted"], 0)

    @patch("web.server.compute_library_rank", return_value="transparent")
    @patch("web.server.check_beets_library_detail")
    def test_delete_transparent_non_flac_removes_only_non_flac_candidates(
            self, mock_beets_detail, _mock_rank):
        """Top-level cleanup deletes MP3 wrong matches only when on-disk rank is transparent."""
        rows = [
            self._row(100, 42, "u1", "/fi/a", mb_release_id="mb-mp3"),
            self._row(101, 42, "u2", "/fi/b", mb_release_id="mb-mp3"),
            self._row(200, 43, "u3", "/fi/c", mb_release_id="mb-flac"),
        ]
        rows[0]["validation_result"]["items"] = [{"path": "01.mp3", "format": "MP3"}]
        rows[1]["validation_result"]["items"] = [{"path": "02.mp3", "format": "MP3"}]
        rows[2]["validation_result"]["items"] = [{"path": "01.flac", "format": "FLAC"}]
        self.mock_db.get_wrong_matches.return_value = rows
        entries = {
            100: self._entry(100, 42, "/fi/a"),
            101: self._entry(101, 42, "/fi/b"),
            200: self._entry(200, 43, "/fi/c"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )
        mock_beets_detail.return_value = {
            "mb-mp3": {"beets_format": "MP3", "beets_bitrate": 245},
            "mb-flac": {"beets_format": "MP3", "beets_bitrate": 245},
        }

        status, data = self._post(
            "/api/wrong-matches/delete-transparent-non-flac",
            {},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["groups_deleted"], 1)
        self.assertEqual(data["deleted"], 2)
        self.assertEqual(data["deleted_request_ids"], [42])
        self.assertEqual(
            [call.args[0] for call in self.mock_rmtree.call_args_list],
            ["/fi/a", "/fi/b"],
        )
        self.assertEqual(
            [call.args[0] for call in self.mock_db.clear_wrong_match_path.call_args_list],
            [100, 101],
        )

    @patch("web.server.check_beets_library_detail", return_value={
        "mb-transparent": {"beets_format": "MP3", "beets_bitrate": 245},
        "mb-opus": {"beets_format": "Opus", "beets_bitrate": 128},
    })
    def test_delete_lossless_opus_removes_verified_opus_groups_only(
            self, _mock_beets):
        transparent = self._row(
            100, 42, "u1", "/fi/transparent", mb_release_id="mb-transparent")
        transparent["request_status"] = "imported"
        transparent["request_min_bitrate"] = 245
        transparent["request_verified_lossless"] = False

        lossless_opus = self._row(
            101, 43, "u2", "/fi/lossless-opus", mb_release_id="mb-opus")
        lossless_opus["request_status"] = "imported"
        lossless_opus["request_min_bitrate"] = 128
        lossless_opus["request_verified_lossless"] = True

        self.mock_db.get_wrong_matches.return_value = [transparent, lossless_opus]
        entries = {
            100: self._entry(100, 42, "/fi/transparent"),
            101: self._entry(101, 43, "/fi/lossless-opus"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )

        status, data = self._post(
            "/api/wrong-matches/delete-lossless-opus",
            {},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["groups_deleted"], 1)
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["deleted_request_ids"], [43])
        # AE4 baseline: NULL on-disk spectral grade is safe — the
        # group is deleted as it would have been today, with no
        # spectral-suspect skip entries.
        self.assertEqual(data.get("groups_skipped_spectral_suspect", 0), 0)
        self.assertEqual(
            [s for s in data.get("skipped", []) if s.get("reason") == "spectral_suspect"],
            [],
        )
        called_ids = [c.args[0] for c in self.mock_db.clear_wrong_match_path.call_args_list]
        self.assertEqual(called_ids, [101])

    @patch("web.server.check_beets_library_detail", return_value={
        "mb-opus-suspect": {"beets_format": "Opus", "beets_bitrate": 128},
    })
    def test_delete_lossless_opus_skips_when_on_disk_spectral_is_likely_transcode(
            self, _mock_beets):
        """Covers AE3 — likely_transcode on-disk grade blocks deletion."""
        row = self._row(200, 50, "u1", "/fi/opus-likely",
                        mb_release_id="mb-opus-suspect")
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 128
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "likely_transcode"
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: self._entry(200, 50, "/fi/opus-likely"))

        status, data = self._post(
            "/api/wrong-matches/delete-lossless-opus", {})

        self.assertEqual(status, 200)
        self.assertEqual(data["deleted"], 0)
        self.assertEqual(data["groups_deleted"], 0)
        self.assertEqual(data["deleted_request_ids"], [])
        self.assertEqual(data["eligible_groups"], 1)
        self.assertEqual(data["groups_skipped_spectral_suspect"], 1)
        skipped = data["skipped"]
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["reason"], "spectral_suspect")
        self.assertEqual(skipped[0]["download_log_id"], 200)
        self.assertEqual(skipped[0]["request_id"], 50)
        # Files on disk untouched.
        self.assertEqual(self.mock_rmtree.call_count, 0)
        self.assertEqual(self.mock_db.clear_wrong_match_path.call_count, 0)

    @patch("web.server.check_beets_library_detail", return_value={
        "mb-opus-suspect2": {"beets_format": "Opus", "beets_bitrate": 128},
    })
    def test_delete_lossless_opus_skips_when_on_disk_spectral_is_suspect(
            self, _mock_beets):
        """Covers AE3 alt grade — 'suspect' blocks deletion identically."""
        row = self._row(201, 51, "u1", "/fi/opus-suspect",
                        mb_release_id="mb-opus-suspect2")
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 128
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "suspect"
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: self._entry(201, 51, "/fi/opus-suspect"))

        status, data = self._post(
            "/api/wrong-matches/delete-lossless-opus", {})

        self.assertEqual(status, 200)
        self.assertEqual(data["deleted"], 0)
        self.assertEqual(data["groups_skipped_spectral_suspect"], 1)
        self.assertEqual(data["skipped"][0]["reason"], "spectral_suspect")

    @patch("web.server.check_beets_library_detail", return_value={
        "mb-opus-genuine": {"beets_format": "Opus", "beets_bitrate": 128},
        "mb-opus-marginal": {"beets_format": "Opus", "beets_bitrate": 128},
    })
    def test_delete_lossless_opus_proceeds_for_safe_grades(self, _mock_beets):
        """Covers R5 — genuine and marginal grades both delete."""
        genuine = self._row(210, 60, "u1", "/fi/opus-genuine",
                            mb_release_id="mb-opus-genuine")
        genuine["request_status"] = "imported"
        genuine["request_min_bitrate"] = 128
        genuine["request_verified_lossless"] = True
        genuine["request_current_spectral_grade"] = "genuine"

        marginal = self._row(211, 61, "u2", "/fi/opus-marginal",
                             mb_release_id="mb-opus-marginal")
        marginal["request_status"] = "imported"
        marginal["request_min_bitrate"] = 128
        marginal["request_verified_lossless"] = True
        marginal["request_current_spectral_grade"] = "marginal"

        self.mock_db.get_wrong_matches.return_value = [genuine, marginal]
        entries = {
            210: self._entry(210, 60, "/fi/opus-genuine"),
            211: self._entry(211, 61, "/fi/opus-marginal"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid]))

        status, data = self._post(
            "/api/wrong-matches/delete-lossless-opus", {})

        self.assertEqual(status, 200)
        self.assertEqual(data["deleted"], 2)
        self.assertEqual(data["groups_deleted"], 2)
        self.assertEqual(sorted(data["deleted_request_ids"]), [60, 61])
        self.assertEqual(data["groups_skipped_spectral_suspect"], 0)
        self.assertEqual(
            [s for s in data["skipped"] if s.get("reason") == "spectral_suspect"],
            [],
        )

    @patch("web.server.check_beets_library_detail", return_value={
        "mb-opus-good": {"beets_format": "Opus", "beets_bitrate": 128},
        "mb-opus-bad": {"beets_format": "Opus", "beets_bitrate": 128},
    })
    def test_delete_lossless_opus_mixed_batch_partitions_safe_and_unsafe(
            self, _mock_beets):
        """Covers R4 + R6 — mixed batch deletes safe groups, skips unsafe."""
        good_row = self._row(300, 70, "u1", "/fi/opus-good",
                             mb_release_id="mb-opus-good")
        good_row["request_status"] = "imported"
        good_row["request_min_bitrate"] = 128
        good_row["request_verified_lossless"] = True
        good_row["request_current_spectral_grade"] = "genuine"

        bad_row_a = self._row(301, 71, "u2a", "/fi/opus-bad-a",
                              mb_release_id="mb-opus-bad")
        bad_row_a["request_status"] = "imported"
        bad_row_a["request_min_bitrate"] = 128
        bad_row_a["request_verified_lossless"] = True
        bad_row_a["request_current_spectral_grade"] = "likely_transcode"

        bad_row_b = self._row(302, 71, "u2b", "/fi/opus-bad-b",
                              mb_release_id="mb-opus-bad")
        bad_row_b["request_status"] = "imported"
        bad_row_b["request_min_bitrate"] = 128
        bad_row_b["request_verified_lossless"] = True
        bad_row_b["request_current_spectral_grade"] = "likely_transcode"

        self.mock_db.get_wrong_matches.return_value = [
            good_row, bad_row_a, bad_row_b]
        entries = {
            300: self._entry(300, 70, "/fi/opus-good"),
            301: self._entry(301, 71, "/fi/opus-bad-a"),
            302: self._entry(302, 71, "/fi/opus-bad-b"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid]))

        status, data = self._post(
            "/api/wrong-matches/delete-lossless-opus", {})

        self.assertEqual(status, 200)
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["groups_deleted"], 1)
        self.assertEqual(data["deleted_request_ids"], [70])
        self.assertEqual(data["eligible_groups"], 2)
        self.assertEqual(data["groups_skipped_spectral_suspect"], 1)
        suspect_rows = [s for s in data["skipped"]
                        if s.get("reason") == "spectral_suspect"]
        self.assertEqual(len(suspect_rows), 2)
        self.assertEqual({s["request_id"] for s in suspect_rows}, {71})
        self.assertEqual({s["download_log_id"] for s in suspect_rows},
                         {301, 302})
        # Only the safe group was actually deleted.
        called_ids = [c.args[0] for c in self.mock_db.clear_wrong_match_path.call_args_list]
        self.assertEqual(called_ids, [300])

    @patch("web.server.check_beets_library_detail", return_value={
        "mb-mp3-bad": {"beets_format": "MP3", "beets_bitrate": 245},
    })
    def test_delete_lossless_opus_does_not_apply_safety_to_non_eligible_groups(
            self, _mock_beets):
        """Regression A — safety gate must not relax _is_lossless_opus_group.

        verified_lossless=False with format='MP3' is excluded by the
        existing predicate; the spectral grade never gets evaluated.
        """
        row = self._row(400, 80, "u1", "/fi/mp3-not-lossless",
                        mb_release_id="mb-mp3-bad")
        row["request_status"] = "imported"
        row["request_verified_lossless"] = False
        row["request_current_spectral_grade"] = "likely_transcode"
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: self._entry(400, 80, "/fi/mp3-not-lossless"))

        status, data = self._post(
            "/api/wrong-matches/delete-lossless-opus", {})

        self.assertEqual(status, 200)
        self.assertEqual(data["eligible_groups"], 0)
        self.assertEqual(data["groups_skipped_spectral_suspect"], 0)
        self.assertEqual(data["skipped"], [])
        self.assertEqual(data["deleted"], 0)

    @patch("web.server.check_beets_library_detail", return_value={
        "mb-opus-fail": {"beets_format": "Opus", "beets_bitrate": 128},
    })
    def test_delete_lossless_opus_delete_failed_shape_unchanged(
            self, _mock_beets):
        """Regression C — delete_failed skip rows keep their original
        shape (no request_id), preserving parity with
        post_wrong_match_delete_transparent_non_flac."""
        row = self._row(500, 90, "u1", "/fi/opus-fail",
                        mb_release_id="mb-opus-fail")
        row["request_status"] = "imported"
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "genuine"
        self.mock_db.get_wrong_matches.return_value = [row]
        # Force the delete helper path to fail by returning None for the
        # log entry — this matches how the route reports delete_failed.
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: None)

        status, data = self._post(
            "/api/wrong-matches/delete-lossless-opus", {})

        self.assertEqual(status, 200)
        delete_failed = [s for s in data["skipped"]
                         if s.get("reason") == "delete_failed"]
        self.assertEqual(len(delete_failed), 1)
        self.assertEqual(delete_failed[0]["download_log_id"], 500)
        self.assertNotIn(
            "request_id", delete_failed[0],
            "delete_failed rows must keep their original shape "
            "(no request_id) so parity with delete-transparent-non-flac "
            "is preserved.")

    def test_groups_in_beets_still_shown(self):
        """Wrong matches still appear when the release is already in the library."""
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertGreater(len(data["groups"]), 0)

    def test_converge_queues_green_candidates_and_deletes_unmatched(self):
        """Converge queues green rows and deletes high-distance leftovers."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
            self._row(101, 42, "u2", "/fi/b", distance=0.180),
            self._row(102, 42, "u3", "/fi/c", distance=0.226),
            self._row(200, 99, "other", "/fi/other", distance=0.100),
        ]
        entries = {
            100: self._entry(100, 42, "/fi/a"),
            101: self._entry(101, 42, "/fi/b"),
            102: self._entry(102, 42, "/fi/c"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )
        self.mock_db.enqueue_import_job.side_effect = [
            self._job(900, 42, 100, "/fi/a"),
            self._job(901, 42, 101, "/fi/b"),
        ]

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
            "delete_unmatched": False,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["queued"], 2)
        self.assertEqual(data["selected_count"], 2)
        self.assertEqual(data["unmatched_count"], 1)
        self.assertTrue(data["delete_unmatched"])
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["dismissed"], 2)
        self.assertEqual(data["remaining"], 0)
        self.assertTrue(data["group_empty"])
        self.assertEqual(
            {item["download_log_id"] for item in data["selected"]},
            {100, 101},
        )
        self.assertEqual(
            [call.kwargs["dedupe_key"]
             for call in self.mock_db.enqueue_import_job.call_args_list],
            [
                "force_import:download_log:100",
                "force_import:download_log:101",
            ],
        )
        self.assertEqual(self.mock_db.clear_wrong_match_paths.call_count, 2)
        self.mock_rmtree.assert_called_once_with("/fi/c", ignore_errors=True)
        self.mock_db.clear_wrong_match_path.assert_called_once_with(102)

    def test_converge_deletes_unmatched_when_legacy_client_requests_it(self):
        """Legacy true payloads still delete non-green rows while selected rows are dismissed."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
            self._row(101, 42, "u2", "/fi/b", distance=0.180),
            self._row(102, 42, "u3", "/fi/c", distance=0.226),
        ]
        entries = {
            100: self._entry(100, 42, "/fi/a"),
            101: self._entry(101, 42, "/fi/b"),
            102: self._entry(102, 42, "/fi/c"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )
        self.mock_db.enqueue_import_job.side_effect = [
            self._job(900, 42, 100, "/fi/a"),
            self._job(901, 42, 101, "/fi/b"),
        ]

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
            "delete_unmatched": True,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["queued"], 2)
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["remaining"], 0)
        self.assertTrue(data["group_empty"])
        self.mock_rmtree.assert_called_once_with("/fi/c", ignore_errors=True)
        self.mock_db.clear_wrong_match_path.assert_called_once_with(102)

    def test_converge_skips_missing_green_files(self):
        """A green row with no surviving failed_path is not queued or dismissed."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/gone/a", distance=0.167),
        ]

        with patch("web.routes.imports.resolve_failed_path", return_value=None):
            status, data = self._post("/api/wrong-matches/converge", {
                "request_id": 42,
                "threshold_milli": 180,
                "delete_unmatched": False,
            })

        self.assertEqual(status, 202)
        self.assertEqual(data["queued"], 0)
        self.assertEqual(data["remaining"], 1)
        self.assertEqual(data["skipped"], [
            {"download_log_id": 100, "reason": "files_missing"},
        ])
        self.mock_db.enqueue_import_job.assert_not_called()
        self.mock_db.clear_wrong_match_paths.assert_not_called()
        self.mock_rmtree.assert_not_called()

    def test_converge_reports_deduped_jobs(self):
        """Existing active force-import jobs still count as selected and dismissed."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
        ]
        self.mock_db.get_download_log_entry.return_value = self._entry(100, 42, "/fi/a")
        self.mock_db.enqueue_import_job.return_value = self._job(
            900, 42, 100, "/fi/a", deduped=True)

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["queued"], 1)
        self.assertEqual(data["deduped"], 1)
        self.assertTrue(data["selected"][0]["deduped"])
        self.assertEqual(data["dismissed"], 1)

    def test_converge_missing_request_id_returns_error(self):
        status, _data = self._post("/api/wrong-matches/converge", {})
        self.assertEqual(status, 400)


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


class TestOverlayNotBakedIntoRoutingCache(_WebServerCase):
    """Issue #101: endpoints that enrich MB/Discogs metadata with per-user
    pipeline/library overlay state MUST NOT be cached at the routing level.

    Pre-fix, /api/release/<id> and friends were cached under web:<url> at
    TTL_LIBRARY=300s. A pipeline-side UPDATE (e.g. status wanted→downloading)
    bypasses the web UI's POST-invalidation paths, so a second GET in the
    300s window returned a stale pipeline_status baked into the cached
    payload.

    Fix: drop every overlay-baking endpoint from Handler._CACHE_TTLS and
    move pure MB/Discogs metadata into a separate meta: namespace at the
    API helper layer (web/mb.py, web/discogs.py). Local DB lookups
    (check_pipeline, check_beets_library) run on every request — cheap.
    """

    # The exact endpoint prefixes proven to bake overlay state — every
    # single one of these was confirmed by the Explore audit to mutate
    # the response with at least one of: pipeline_status, pipeline_id,
    # in_library, library_rank, library_format, library_min_bitrate,
    # beets_album_id, beets_tracks, upgrade_queued, in_beets, library_status.
    FORBIDDEN_ROUTING_CACHE_PREFIXES = (
        "/api/release-group",
        "/api/release",
        "/api/discogs/master",
        "/api/discogs/release",
        "/api/discogs/artist",
        "/api/artist",              # /api/artist/<id> + /api/artist/<id>/disambiguate + /api/artist/compare
        "/api/library",             # /api/library/artist
        "/api/beets",               # /api/beets/search + /api/beets/album + /api/beets/recent
        "/api/pipeline/recent",
        "/api/pipeline/all",
        "/api/pipeline/log",
        "/api/pipeline/status",
    )

    def test_forbidden_prefixes_are_not_in_routing_cache_ttls(self) -> None:
        """Handler._CACHE_TTLS must not contain any overlay-baking prefix."""
        import web.server as srv
        ttls: dict[str, int] = getattr(srv.Handler, "_CACHE_TTLS", {})
        leaked = set(ttls) & set(self.FORBIDDEN_ROUTING_CACHE_PREFIXES)
        self.assertFalse(
            leaked,
            f"Overlay-baking prefixes must not be in _CACHE_TTLS — "
            f"they would bake per-user pipeline/library state into Redis "
            f"and leak stale badges when the pipeline writes to Postgres "
            f"outside the web UI's POST paths. Offenders: {sorted(leaked)}")


class _CachedServerCase(_WebServerCase):
    """Shared harness: _WebServerCase but with a FakeRedis wired up so we
    can observe routing-cache behaviour in isolation. Pre-fix this would
    exhibit the stale-badge bug; post-fix it proves the overlay recomputes."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        import web.cache as cache
        from tests.test_web_cache import FakeRedis
        cls._cache = cache
        cls._saved_redis = cache._redis
        cache._redis = FakeRedis()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._cache._redis = cls._saved_redis
        super().tearDownClass()


class TestReleaseEndpointReflectsPipelineWrite(_CachedServerCase):
    """Regression test for issue #101.

    The bug: /api/release/<id> cached the full response including
    pipeline_status. When the pipeline wrote status='downloading'
    directly to Postgres (outside the web UI's POST invalidation
    paths), a second GET within 300s returned the stale 'wanted'
    status. Badges lagged by up to 5 minutes.

    Post-fix: the overlay is recomputed on every request, so external
    DB writes show up immediately.
    """

    RELEASE_ID = "c6cd62c4-da2a-4a89-a219-adba66d6c7d4"

    def setUp(self) -> None:
        # Clear any state left behind by a previous test that shares the
        # FakeRedis instance, so each scenario starts cold. `_redis` is
        # typed `object | None` on the module; narrow to FakeRedis here.
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)
        fake._store.clear()

    def _call_release_detail(self) -> dict:
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()):
            mock_mb.get_release.return_value = {
                "id": self.RELEASE_ID,
                "title": "Test Album",
                "tracks": [],
            }
            _status, data = self._get(f"/api/release/{self.RELEASE_ID}")
            return data

    def test_release_reflects_external_status_write(self) -> None:
        """Pipeline writes status='downloading' directly to Postgres
        between two GETs. The second GET must see 'downloading'."""
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=42, status="wanted", mb_release_id=self.RELEASE_ID,
        )
        first = self._call_release_detail()
        self.assertEqual(first["pipeline_status"], "wanted")

        # Simulate cratedigger pipeline flipping status outside the web UI.
        # No POST to /api/cache/invalidate, no web-UI cache-group flush —
        # this is the exact sequence that produced the stale-badge bug.
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=42, status="downloading", mb_release_id=self.RELEASE_ID,
        )
        second = self._call_release_detail()
        self.assertEqual(
            second["pipeline_status"], "downloading",
            "Second GET must see the fresh DB state, not a baked-in "
            "pipeline_status from a cached response. If this fails, the "
            "routing-level cache is still capturing the overlay.")

    def test_release_reflects_external_library_state_flip(self) -> None:
        """Same bug for the in_library flag. After an album is imported
        the 'in_library' flag flips true in beets; a second GET within
        the cache window must reflect that without an explicit flush."""
        self.mock_db.get_request_by_mb_release_id.return_value = make_request_row(
            id=42, status="imported", mb_release_id=self.RELEASE_ID,
        )
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()):
            mock_mb.get_release.return_value = {
                "id": self.RELEASE_ID, "title": "T", "tracks": [],
            }
            _s, first = self._get(f"/api/release/{self.RELEASE_ID}")
        self.assertFalse(first["in_library"])

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library",
                      return_value={self.RELEASE_ID}):
            mock_mb.get_release.return_value = {
                "id": self.RELEASE_ID, "title": "T", "tracks": [],
            }
            _s, second = self._get(f"/api/release/{self.RELEASE_ID}")
        self.assertTrue(
            second["in_library"],
            "Second GET must recompute the overlay against current beets "
            "state instead of returning a cached in_library=False.")


class TestAnalysisSkeletonCachedSeparately(_CachedServerCase):
    """Issue #101 Codex round 3 — the `/api/artist/<id>/disambiguate`
    and `/api/artist/compare` endpoints run expensive pure analysis on
    top of MB metadata (`filter_non_live` + `analyse_artist_releases`,
    `merge_discographies`). After the response-cache removal, naïvely
    running that analysis on every request regresses warm-load latency
    from ~5ms (full response cached) to ~50-300ms (analysis re-runs).

    Fix: cache the pre-overlay skeleton separately under `meta:`. It's
    a pure function of pure-metadata inputs — safe. Overlay (live DB
    state) still runs on every request.

    These tests pin the split: skeleton is cached across calls, and
    the overlay reflects live DB state even when the skeleton is warm.
    """

    ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"
    RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    RG_ID = "11111111-1111-1111-1111-111111111111"

    _RAW_RELEASES = [
        {
            "id": RELEASE_ID,
            "title": "Album",
            "date": "2020-01-01",
            "country": "US",
            "status": "Official",
            "release-group": {
                "id": RG_ID,
                "title": "Album",
                "primary-type": "Album",
                "secondary-types": [],
            },
            "media": [{
                "position": 1, "format": "CD", "track-count": 1,
                "tracks": [{
                    "position": 1, "number": "1", "title": "Track",
                    "recording": {"id": "rec-1", "title": "Track"},
                }],
            }],
        },
    ]

    def setUp(self) -> None:
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)
        fake._store.clear()

    # -- Disambiguate ------------------------------------------------

    def test_disambiguate_skeleton_cached_in_meta_namespace(self) -> None:
        """First GET computes the skeleton; second GET reuses it. We
        assert the skeleton ended up under `meta:` and the pure-
        analysis fetch is only issued once across both requests."""
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"

            s1, _ = self._get(f"/api/artist/{self.ARTIST_ID}/disambiguate")
            s2, _ = self._get(f"/api/artist/{self.ARTIST_ID}/disambiguate")

            self.assertEqual(s1, 200)
            self.assertEqual(s2, 200)
            # The pure MB fetch helper was called once — either this is
            # the first call (skeleton miss) or the route's own meta-
            # cached skeleton short-circuited to avoid re-calling it.
            self.assertEqual(
                mock_mb.get_artist_releases_with_recordings.call_count, 1,
                "skeleton caching must reuse the analysis across calls "
                "— the expensive pure-python analysis should NOT re-run "
                "on warm loads")

        # Skeleton key is in the meta: namespace — not web:, so it
        # survives pipeline/library group invalidations.
        meta_keys = [k for k in fake._store
                     if k.startswith("meta:") and self.ARTIST_ID in k]
        self.assertTrue(
            meta_keys,
            f"expected a meta: key for artist {self.ARTIST_ID}, got: "
            f"{sorted(fake._store.keys())}")

    def test_disambiguate_overlay_reflects_live_state_across_skeleton_cache(
            self) -> None:
        """Skeleton cache is warm; change live DB state; next GET must
        still reflect the new pipeline_status via overlay."""
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline",
                      return_value={self.RELEASE_ID: {"id": 42, "status": "wanted"}}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, first = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")

        self.assertEqual(
            first["release_groups"][0]["pressings"][0]["pipeline_status"],
            "wanted")

        # External DB write — status flips to 'downloading'. No POST
        # invalidation (same bug class as the release-detail test).
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline",
                      return_value={self.RELEASE_ID: {"id": 42, "status": "downloading"}}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, second = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")

        self.assertEqual(
            second["release_groups"][0]["pressings"][0]["pipeline_status"],
            "downloading",
            "Even with the skeleton cached in meta:, the overlay must "
            "recompute against current DB state — otherwise the skeleton "
            "cache reintroduces the stale-badge bug.")
        # RG-level pipeline_status must also flip.
        self.assertEqual(
            second["release_groups"][0]["pipeline_status"], "downloading")

    def test_disambiguate_overlay_reflects_library_flip(self) -> None:
        """Same guarantee for in_library — beets state flips, overlay must
        see it without invalidating the skeleton cache."""
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, first = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")
        self.assertFalse(
            first["release_groups"][0]["pressings"][0]["in_library"])

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library",
                      return_value={self.RELEASE_ID}), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = \
                self._RAW_RELEASES
            mock_mb.get_artist_name.return_value = "Test Artist"
            _s, second = self._get(
                f"/api/artist/{self.ARTIST_ID}/disambiguate")
        self.assertTrue(
            second["release_groups"][0]["pressings"][0]["in_library"])
        self.assertEqual(
            second["release_groups"][0]["library_status"], "in_library")

    # -- Compare -----------------------------------------------------

    def test_compare_skeleton_cached_in_meta_namespace(self) -> None:
        """merge_discographies is pure — its output is cacheable."""
        from tests.test_web_cache import FakeRedis
        fake = self._cache._redis
        assert isinstance(fake, FakeRedis)

        mb_rg = {
            "id": self.RG_ID, "title": "OK Computer", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Radiohead", "primary_artist_id": self.ARTIST_ID,
        }
        discogs_rg = {
            "id": "21491", "title": "OK Computer", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Radiohead", "primary_artist_id": "3840",
        }

        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.get_library_artist", return_value=[]):
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"

            s1, _ = self._get("/api/artist/compare?name=Radiohead")
            s2, _ = self._get("/api/artist/compare?name=Radiohead")
            self.assertEqual(s1, 200)
            self.assertEqual(s2, 200)
            # Pure MB/Discogs discography fetches are called once across
            # both requests — their outputs went into the skeleton cache.
            self.assertEqual(mock_mb.get_artist_release_groups.call_count, 1)
            self.assertEqual(mock_dg.get_artist_releases.call_count, 1)

        meta_keys = [k for k in fake._store if k.startswith("meta:")
                     and "compare" in k]
        self.assertTrue(
            meta_keys,
            "expected a compare skeleton under meta:, got: "
            f"{sorted(fake._store.keys())}")

    def test_compare_artist_names_are_canonical_not_user_supplied(self) -> None:
        """Codex round 4: previously the compare skeleton cached
        user-supplied artist names inside the response body, so the
        first request's `name=` query param won for 24h. Canonical
        names from the MB/Discogs API must be used instead.
        """
        mb_rg = {
            "id": self.RG_ID, "title": "OK Computer", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Radiohead", "primary_artist_id": self.ARTIST_ID,
        }
        discogs_rg = {
            "id": "21491", "title": "OK Computer", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Radiohead", "primary_artist_id": "3840",
        }

        # First request — misspelled name. Skeleton gets cached.
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.get_library_artist", return_value=[]):
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"
            _s, first = self._get(
                "/api/artist/compare?name=Radiohea&"
                f"mbid={self.ARTIST_ID}&discogs_id=3840")

        # mb_artist name must be canonical from MB, not the typo.
        self.assertEqual(
            (first["mb_artist"] or {}).get("name"), "Radiohead",
            "mb_artist.name must be the canonical name from MB, not "
            "the user-supplied ?name= query param — otherwise a typo "
            "on the first request poisons the 24h skeleton cache.")

        # Second request — different (correct) name. Must STILL return
        # the canonical Radiohead, and the skeleton cache must have been
        # reused (no re-fetch of the release-group metadata).
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.get_library_artist", return_value=[]):
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"
            _s, second = self._get(
                "/api/artist/compare?name=Radiohead&"
                f"mbid={self.ARTIST_ID}&discogs_id=3840")
            # Expensive metadata fetch was served from cache (skeleton
            # still reusable despite different ?name=).
            self.assertEqual(mock_mb.get_artist_release_groups.call_count, 0)

        self.assertEqual(
            (second["mb_artist"] or {}).get("name"), "Radiohead")

    def test_compare_overlay_reflects_library_flip(self) -> None:
        """Even with the compare skeleton cached, annotate_in_library
        must run on every request so badges flip with beets state."""
        mb_rg = {
            "id": self.RG_ID, "title": "OK Computer", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Radiohead", "primary_artist_id": self.ARTIST_ID,
        }
        discogs_rg = {
            "id": "21491", "title": "OK Computer", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Radiohead", "primary_artist_id": "3840",
        }

        def _run(lib_albums: list[dict]) -> dict:
            with patch("web.server.mb_api") as mock_mb, \
                    patch("web.routes.browse.discogs_api") as mock_dg, \
                    patch("web.server.get_library_artist",
                          return_value=lib_albums):
                mock_mb.search_artists.return_value = [
                    {"id": self.ARTIST_ID, "name": "Radiohead"}]
                mock_mb.get_artist_release_groups.return_value = [mb_rg]
                mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
                mock_mb.get_artist_name.return_value = "Radiohead"
                mock_dg.search_artists.return_value = [
                    {"id": "3840", "name": "Radiohead"}]
                mock_dg.get_artist_releases.return_value = [discogs_rg]
                mock_dg.get_artist_name.return_value = "Radiohead"
                _s, data = self._get("/api/artist/compare?name=Radiohead")
                return data

        first = _run([])
        self.assertFalse(first["both"][0]["mb"].get("in_library"))

        # Library flips — beets now holds this album.
        lib_album = {
            "mb_albumid": self.RELEASE_ID,
            "mb_releasegroupid": self.RG_ID,
            "album": "OK Computer",
            "formats": "MP3",
            "min_bitrate": 320000,
        }
        second = _run([lib_album])
        self.assertTrue(
            second["both"][0]["mb"].get("in_library"),
            "Compare overlay must run per-request — a warm skeleton "
            "cache must not mask a library-state change.")


class TestFuzzyShimRemoved(unittest.TestCase):
    """Issue #123: ``web.server.check_beets_by_artist_album`` deleted.

    Guard against accidental reintroduction — the shim was the only
    path from the web layer into the fuzzy fallback, so deleting it
    completes the closure.
    """

    def test_check_beets_by_artist_album_no_longer_exposed(self) -> None:
        from web import server
        self.assertFalse(
            hasattr(server, "check_beets_by_artist_album"),
            "check_beets_by_artist_album was deleted in issue #123 "
            "— the fuzzy artist+album shim must not return.",
        )


if __name__ == "__main__":
    unittest.main()
