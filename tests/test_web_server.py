#!/usr/bin/env python3
"""Tests for web/server.py HTTP endpoints.

Starts a real HTTP server on a random port with mocked DB,
verifying response codes, JSON structure, and error handling.
"""

import contextlib
import copy
from datetime import datetime, timezone
import email.message
import json
import logging
import os
import sys
import tempfile
import threading
import unittest
from http.server import HTTPServer
from unittest.mock import MagicMock, patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Eager import: lib.beets_distance pins the real ``beets`` package
# (see lib/beets_distance.py:49-55) — we must trigger it *before* the
# next two ``sys.path.insert`` calls add ``lib/`` ahead of site-
# packages, otherwise downstream imports of lib.youtube_album_service
# (which imports lib.beets_distance lazily inside the route handler)
# fail with "cannot import name 'library' from 'beets'" because
# ``beets`` would resolve to ``lib/beets.py``.
import lib.beets_distance  # noqa: F401,E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "web"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from lib.manual_import import FolderInfo
from lib.import_preview import ImportPreviewResult
from lib.import_queue import ImportJob
from lib.pipeline_db import SearchPlanProvenance
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
    # Release-group id surfaces in the wrong-matches group payload
    # so the frontend can render the Replace button (R7).
    "mb_release_group_id": "rg-abc-123",
    "soulseek_username": "testuser",
    # Per-attempt evidence — surfaced via the LEFT JOIN to
    # album_quality_evidence in PipelineDB.get_wrong_matches (with
    # COALESCE against the legacy denorm columns for spectral/V0).
    "spectral_grade": None,
    "spectral_bitrate": None,
    "v0_probe_kind": None,
    "v0_probe_avg_bitrate": None,
    "evidence_storage_format": None,
    "evidence_min_bitrate": None,
    "evidence_verified_lossless": False,
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


def _pipeline_db_test_harness(fake: "FakePipelineDB | None" = None) -> MagicMock:
    """Build the contract-test pipeline-DB harness.

    Returns a MagicMock that ``wraps`` a real :class:`FakePipelineDB`. The
    fake is the source of truth for state (seeded request rows live there
    via :meth:`seed_request`); the MagicMock layer records every call so
    contract tests can keep using ``.return_value = X`` / ``.assert_called_*``
    overrides without re-engineering ~300 sites in a single PR.

    The audit rule (``code-quality.md`` § MOCKS: LEAF-SEAM ONLY) forbids
    constructing ``MagicMock()`` directly for ``mock_db`` / ``failing_db``
    style variables because pure MagicMocks let production code rot
    unobserved. Wrapping a FakePipelineDB closes that loophole: any call
    the test does NOT explicitly override falls through to the typed
    fake, so production code paths still hit real state mutations and
    state-based assertions remain available via the underlying fake.

    Tests can reach the underlying fake through the ``_fake`` attribute on
    the mock (or by accepting the helper's return value directly in
    bespoke harnesses such as the ``failing_db`` site).
    """
    backing = fake if fake is not None else FakePipelineDB()
    harness = MagicMock(wraps=backing)
    # FakePipelineDB.set_tracks requires every track dict to carry
    # ``track_number``. The legacy MagicMock harness silently accepted
    # slimmer test fixtures (``[{"title": "..."}]``); preserve that
    # behaviour by short-circuiting the write — these contract tests
    # exercise the route's response shape, not the track-row layout.
    # Production callers always emit a ``track_number`` field.
    harness.set_tracks = MagicMock(return_value=None)
    harness._fake = backing
    return harness


def _make_server():
    """Create a test server with mocked DB on a random port."""
    import web.server as srv
    from lib.release_identity import detect_release_source, normalize_release_id
    # Mock the pipeline DB. The MagicMock wraps a real FakePipelineDB so
    # unmocked methods fall through to typed state — see
    # ``_pipeline_db_test_harness`` for the rationale.
    mock_db = _pipeline_db_test_harness()
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
    mock_db._execute.return_value = MagicMock(fetchone=MagicMock(return_value={
        "total": 1,
        "imported": 1,
        "matches_24h": 3,
        "matches_6h": 1,
    }))
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
    mock_db.get_search_plan_stats_history.return_value = []
    mock_db.get_legacy_search_log_summary.return_value = (0, [])
    mock_db.get_pipeline_dashboard_metrics.return_value = {
        "generated_at": "2026-05-05T00:00:00+00:00",
        "searches": {
            "windows": [
                {
                    "label": "24h",
                    "hours": 24,
                    "searches": 12,
                    "distinct_requests": 8,
                    "searches_per_hour": 0.5,
                    "searches_per_24h": 12,
                    "avg_elapsed_s": 4.2,
                    "median_elapsed_s": 3.1,
                    "p95_elapsed_s": 9.9,
                    "max_elapsed_s": 11.0,
                    "outcomes": {
                        "found": 2,
                        "no_match": 4,
                        "no_results": 5,
                        "exhausted": 0,
                        "errors": 1,
                    },
                    "cursor_wraps": 0,
                    "stale_completions": 0,
                    "non_consuming": 1,
                    "cache_attribution_level": "cycle_only",
                },
                {
                    "label": "6h",
                    "hours": 6,
                    "searches": 5,
                    "distinct_requests": 5,
                    "searches_per_hour": 0.8333333333,
                    "searches_per_24h": 20,
                    "avg_elapsed_s": 3.0,
                    "median_elapsed_s": 2.8,
                    "p95_elapsed_s": 5.0,
                    "max_elapsed_s": 5.5,
                    "outcomes": {
                        "found": 1,
                        "no_match": 1,
                        "no_results": 3,
                        "exhausted": 0,
                        "errors": 0,
                    },
                    "cursor_wraps": 0,
                    "stale_completions": 0,
                    "non_consuming": 0,
                    "cache_attribution_level": "cycle_only",
                },
            ],
        },
        "cycles": {
            "windows": [
                {
                    "label": "24h",
                    "hours": 24,
                    "cycles": 10,
                    "avg_cycle_s": 320.0,
                    "median_cycle_s": 300.0,
                    "p95_cycle_s": 700.0,
                    "max_cycle_s": 900.0,
                    "median_search_s": 250.0,
                    "watchdog_kills": 1,
                    "find_download_queued": 30,
                    "find_download_completed": 28,
                    "cache_errors": 0,
                    "cache_write_errors": 0,
                    "cache_fuse_tripped": 0,
                    "peers_browsed": 100,
                    "peers_browsed_lazy": 2,
                    "fanout_waves": 20,
                },
                {
                    "label": "6h",
                    "hours": 6,
                    "cycles": 3,
                    "avg_cycle_s": 290.0,
                    "median_cycle_s": 280.0,
                    "p95_cycle_s": 360.0,
                    "max_cycle_s": 380.0,
                    "median_search_s": 220.0,
                    "watchdog_kills": 0,
                    "find_download_queued": 8,
                    "find_download_completed": 8,
                    "cache_errors": 0,
                    "cache_write_errors": 0,
                    "cache_fuse_tripped": 0,
                    "peers_browsed": 25,
                    "peers_browsed_lazy": 0,
                    "fanout_waves": 6,
                },
            ],
            "recent": [
                {
                    "id": 1,
                    "started_at": "2026-05-05T00:00:00+00:00",
                    "created_at": "2026-05-05T00:05:00+00:00",
                    "cycle_total_s": 300.0,
                    "browse_time_s": 20.0,
                    "match_time_s": 10.0,
                    "search_time_s": 240.0,
                    "watchdog_kills": 0,
                    "find_download_queued": 4,
                    "find_download_completed": 4,
                    "find_download_drain_time_s": 1.0,
                    "cache_errors": 0,
                    "cache_write_errors": 0,
                    "cache_fuse_tripped": 0,
                    "peers_browsed": 8,
                    "peers_browsed_lazy": 0,
                    "fanout_waves": 2,
                },
            ],
            "outliers": [
                {
                    "id": 2,
                    "started_at": "2026-05-04T00:00:00+00:00",
                    "created_at": "2026-05-04T00:15:00+00:00",
                    "cycle_total_s": 900.0,
                    "browse_time_s": 80.0,
                    "match_time_s": 30.0,
                    "search_time_s": 760.0,
                    "watchdog_kills": 1,
                    "find_download_queued": 2,
                    "find_download_completed": 1,
                    "find_download_drain_time_s": 5.0,
                    "cache_errors": 0,
                    "cache_write_errors": 0,
                    "cache_fuse_tripped": 0,
                    "peers_browsed": 22,
                    "peers_browsed_lazy": 1,
                    "fanout_waves": 5,
                },
            ],
        },
        "coverage": {
            "wanted_total": 10,
            "wanted_searched_24h": 8,
            "wanted_searched_6h": 5,
            "wanted_unsearched_24h": 2,
            "wanted_unsearched_6h": 5,
            "wanted_never_searched": 1,
            "active_wanted_searches_24h": 12,
            "active_wanted_searches_6h": 5,
            "oldest_last_search_at": "2026-05-04T00:00:00+00:00",
            "matches_24h": 3,
            "matches_6h": 1,
            "matches_per_hour_24h": 0.125,
            "matches_per_hour_6h": 0.1666666667,
            "match_rate_series_24h": [
                {
                    "bucket_start": "2026-05-04T23:00:00+00:00",
                    "matches": 1,
                    "matches_per_hour": 1,
                },
                {
                    "bucket_start": "2026-05-05T00:00:00+00:00",
                    "matches": 2,
                    "matches_per_hour": 2,
                },
            ],
            "match_rate_series_28d": [
                {
                    "bucket_start": "2026-05-04T00:00:00+00:00",
                    "matches": 1,
                    "matches_per_day": 1,
                },
                {
                    "bucket_start": "2026-05-05T00:00:00+00:00",
                    "matches": 2,
                    "matches_per_day": 2,
                },
            ],
            "wanted_trend": {
                "current_wanted": 10,
                "latest_sample_at": "2026-05-05T00:00:00+00:00",
                "series_24h": [
                    {
                        "sampled_at": "2026-05-04T23:00:00+00:00",
                        "wanted_total": 12,
                    },
                    {
                        "sampled_at": "2026-05-05T00:00:00+00:00",
                        "wanted_total": 10,
                        "synthetic": True,
                    },
                ],
                "windows": [
                    {
                        "label": "6h",
                        "hours": 6,
                        "sample_count": 1,
                        "start_sample_at": "2026-05-04T23:00:00+00:00",
                        "end_sample_at": "2026-05-05T00:00:00+00:00",
                        "start_wanted": 12,
                        "end_wanted": 10,
                        "delta": -2,
                        "delta_per_hour": -2.0,
                        "drain_per_hour": 2.0,
                        "eta_hours": 5.0,
                        "trend": "down",
                    },
                ],
            },
            "top_10_share_24h": 0.75,
            "top_loop_suspects": [
                {
                    "request_id": 100,
                    "artist_name": "Test Artist",
                    "album_title": "Loop Album",
                    "status": "wanted",
                    "last_search_at": "2026-05-05T00:00:00+00:00",
                    "searches_24h": 4,
                    "searches_6h": 2,
                    "found_24h": 0,
                    "no_match_24h": 2,
                    "no_results_24h": 2,
                    "reset_24h": 0,
                    "problem_24h": 0,
                },
            ],
            "stale_wanted": [
                {
                    "request_id": 101,
                    "artist_name": "Test Artist",
                    "album_title": "Stale Album",
                    "status": "wanted",
                    "last_search_at": None,
                    "hours_since_search": None,
                    "searches_24h": 0,
                    "searches_6h": 0,
                    "found_24h": 0,
                    "no_match_24h": 0,
                    "no_results_24h": 0,
                    "reset_24h": 0,
                    "problem_24h": 0,
                },
            ],
        },
        "peer_dirs": {
            "heavy_query_hours": 24,
            "heavy_queries": [
                {
                    "search_log_id": 88,
                    "request_id": 100,
                    "mb_release_id": "dash-heavy-mbid",
                    "artist_name": "Test Artist",
                    "album_title": "Loop Album",
                    "status": "wanted",
                    "created_at": "2026-05-05T00:03:00+00:00",
                    "query": "loop heavy tokens",
                    "variant": "track_0",
                    "outcome": "no_match",
                    "result_count": 500,
                    "elapsed_s": 12.0,
                    "browse_time_s": 42.0,
                    "match_time_s": 1.0,
                    "peers_browsed": 110,
                    "peers_browsed_lazy": 5,
                    "peer_dirs": 115,
                    "fanout_waves": 6,
                },
            ],
            "totals": {
                "known_combos": 1200,
                "known_peers": 220,
                "known_dirs": 980,
                "new_24h": 80,
                "cold_seen_24h": 95,
                "days_with_new": 2,
                "tracked_since": "2026-05-04T00:00:00+00:00",
            },
            "days": [
                {
                    "date": "2026-05-05",
                    "new_combos": 80,
                    "new_peers": 22,
                    "new_dirs": 75,
                },
            ],
        },
        "plan_readiness": {
            "generator_id": "search-plan/2026-05-19-1",
            "wanted_total": 10,
            "wanted_searchable": 7,
            "wanted_legacy": 1,
            "wanted_failed_deterministic": 1,
            "wanted_failed_transient": 1,
            "wanted_no_plan": 0,
        },
    }
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

    def test_pipeline_log_limit_param(self):
        status, data = self._get("/api/pipeline/log?outcome=rejected&limit=300")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=300, outcome_filter="rejected")

    def test_pipeline_log_limit_param_is_capped(self):
        status, data = self._get("/api/pipeline/log?limit=5000")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=500, outcome_filter=None)

    def test_pipeline_log_filter_invalid_ignored(self):
        status, data = self._get("/api/pipeline/log?outcome=badvalue")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=50, outcome_filter=None)

    def test_pipeline_log_counts_structure(self):
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        counts = data["counts"]
        for key in ("all", "imported", "rejected", "matches_24h", "matches_6h"):
            self.assertIn(key, counts)
            self.assertIsInstance(counts[key], int)
        for key in ("matches_per_hour_24h", "matches_per_hour_6h"):
            self.assertIn(key, counts)
            self.assertIsInstance(counts[key], (int, float))

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

    def test_pipeline_downloading_returns_current_downloads_only(self):
        downloading_row = make_request_row(
            id=201, album_title="Active Download", artist_name="DL Artist",
            mb_release_id="dl-uuid", status="downloading",
            active_download_state={
                "filetype": "mp3 320",
                "enqueued_at": "2026-05-05T12:00:00+00:00",
                "files": [{"username": "peer", "bytes_transferred": 1, "size": 2}],
            },
        )
        self.mock_db.get_by_status.side_effect = (
            lambda s: [downloading_row] if s == "downloading" else []
        )
        self.mock_db.count_by_status.return_value = {"downloading": 1}
        self.mock_db.get_download_history_batch.reset_mock()
        self.mock_db.get_download_history_batch.return_value = {}

        status, data = self._get("/api/pipeline/downloading")

        self.assertEqual(status, 200)
        self.assertEqual(data["counts"]["downloading"], 1)
        self.assertEqual(len(data["downloading"]), 1)
        self.assertEqual(data["downloading"][0]["album_title"], "Active Download")
        self.mock_db.get_by_status.assert_called_with("downloading")
        self.mock_db.get_download_history_batch.assert_any_call([201])

        self.mock_db.get_by_status.side_effect = None
        self.mock_db.get_by_status.return_value = []
        self.mock_db.get_download_history_batch.reset_mock()
        self.mock_db.count_by_status.return_value = {
            "wanted": 0, "imported": 1, "manual": 0}

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
        # Pydantic adapter populates ``errors`` with structured field-path
        # entries — the frontend uses ``loc`` + ``msg`` + ``type`` to
        # render real validation messages instead of a single string.
        self.assertIn("errors", data)
        self.assertIsInstance(data["errors"], list)
        self.assertTrue(data["errors"])
        first = data["errors"][0]
        self.assertIn("loc", first)
        self.assertIn("msg", first)
        self.assertIn("type", first)

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
                "source_dirs": ["baduser\\Artist\\Album"],
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
        self.assertEqual(kwargs["payload"]["source_dirs"], ["baduser\\Artist\\Album"])

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
        # U18 step 2: self-documenting API surface.
        "/api/_index",
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
        "/api/pipeline/downloading",
        "/api/pipeline/dashboard",
        "/api/pipeline/constants",
        "/api/pipeline/simulate",
        r"^/api/pipeline/(\d+)$",
        r"^/api/pipeline/(\d+)/search-plan$",
        r"^/api/pipeline/(\d+)/search-plan/dry-run$",
        r"^/api/pipeline/(\d+)/search-plan/saturation$",
        r"^/api/pipeline/(\d+)/search-plan/history$",
        r"^/api/pipeline/(\d+)/search-plan/regenerate$",
        r"^/api/pipeline/(\d+)/search-plan/advance$",
        r"^/api/pipeline/(\d+)/replace$",
        r"^/api/pipeline/(\d+)/resolve-rg$",
        r"^/api/pipeline/requests-by-rg/([a-f0-9-]{36})$",
        r"^/api/beets-distance/(\d+)/([a-f0-9-]{36})$",
        "/api/pipeline/active-rgs",
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
        "/api/wrong-matches/audio",
        "/api/wrong-matches/delete",
        "/api/wrong-matches/delete-group",
        "/api/wrong-matches/converge",
        "/api/wrong-matches/triage",
        "/api/wrong-matches/explorer",
        # U17: /api/triage HTTP endpoints. Per-request composition and
        # cohort listing both wrap ``lib.triage_service`` (U15) — same
        # service as ``pipeline-cli triage`` (U16) per CLI ⇄ API symmetry.
        "/api/triage/list",
        r"^/api/triage/(\d+)$",
        # U8: YouTube Music album resolver. Wraps
        # ``lib.youtube_album_service.resolve_youtube_album`` — same
        # service as ``pipeline-cli youtube-album`` (U7) per
        # CLI ⇄ API symmetry. Outcome → HTTP status from
        # ``OUTCOME_HTTP_STATUS`` (single source of truth).
        "/api/youtube-album",
    }

    def test_all_web_routes_are_classified_for_contract_coverage(self):
        import web.server as srv

        actual = set(srv.Handler._FUNC_GET_ROUTES)
        actual.update(srv.Handler._FUNC_POST_ROUTES)
        actual.update(pattern.pattern for pattern, _fn in srv.Handler._FUNC_GET_PATTERNS)
        actual.update(
            pattern.pattern for pattern, _fn
            in getattr(srv.Handler, "_FUNC_POST_PATTERNS", []))

        self.assertFalse(actual - self.CLASSIFIED_ROUTES,
                         f"Unclassified web routes: {sorted(actual - self.CLASSIFIED_ROUTES)}")
        self.assertFalse(self.CLASSIFIED_ROUTES - actual,
                         f"Stale route classifications: {sorted(self.CLASSIFIED_ROUTES - actual)}")

    def test_every_registered_route_has_a_description(self):
        """U18 step 3: every registered route must carry a human-readable
        one-liner in the parallel description dispatch tables. Fails if a
        future route is added without one — fixing it is a one-line edit
        in the route module."""
        import web.server as srv

        get_paths = set(srv.Handler._FUNC_GET_ROUTES.keys())
        post_paths = set(srv.Handler._FUNC_POST_ROUTES.keys())
        get_pattern_strs = {
            p.pattern for p, _fn in srv.Handler._FUNC_GET_PATTERNS}
        post_pattern_strs = {
            p.pattern for p, _fn in srv.Handler._FUNC_POST_PATTERNS}

        get_desc_paths = set(srv.Handler._FUNC_GET_DESCRIPTIONS.keys())
        post_desc_paths = set(srv.Handler._FUNC_POST_DESCRIPTIONS.keys())
        get_pattern_desc_strs = {
            p.pattern for p, _d in srv.Handler._FUNC_GET_PATTERN_DESCRIPTIONS}
        post_pattern_desc_strs = {
            p.pattern for p, _d in srv.Handler._FUNC_POST_PATTERN_DESCRIPTIONS}

        missing_get = get_paths - get_desc_paths
        missing_post = post_paths - post_desc_paths
        missing_get_patterns = get_pattern_strs - get_pattern_desc_strs
        missing_post_patterns = post_pattern_strs - post_pattern_desc_strs

        self.assertFalse(
            missing_get,
            f"GET routes missing descriptions: {sorted(missing_get)}",
        )
        self.assertFalse(
            missing_post,
            f"POST routes missing descriptions: {sorted(missing_post)}",
        )
        self.assertFalse(
            missing_get_patterns,
            "GET pattern routes missing descriptions: "
            f"{sorted(missing_get_patterns)}",
        )
        self.assertFalse(
            missing_post_patterns,
            "POST pattern routes missing descriptions: "
            f"{sorted(missing_post_patterns)}",
        )

        # Empty-string registration would pass the presence check above
        # and defeat the U18 intent — every route must carry a non-empty
        # one-liner. Surface each offender by name so the fix is
        # one-route-at-a-time.
        def _empty_desc_paths(registered: dict[str, str]) -> list[str]:
            return sorted(p for p, d in registered.items() if not (d and d.strip()))

        empty_get = _empty_desc_paths(srv.Handler._FUNC_GET_DESCRIPTIONS)
        empty_post = _empty_desc_paths(srv.Handler._FUNC_POST_DESCRIPTIONS)
        empty_get_pat = sorted(
            p.pattern
            for p, d in srv.Handler._FUNC_GET_PATTERN_DESCRIPTIONS
            if not (d and d.strip())
        )
        empty_post_pat = sorted(
            p.pattern
            for p, d in srv.Handler._FUNC_POST_PATTERN_DESCRIPTIONS
            if not (d and d.strip())
        )
        self.assertFalse(
            empty_get,
            f"GET routes with empty description string: {empty_get}",
        )
        self.assertFalse(
            empty_post,
            f"POST routes with empty description string: {empty_post}",
        )
        self.assertFalse(
            empty_get_pat,
            f"GET pattern routes with empty description string: {empty_get_pat}",
        )
        self.assertFalse(
            empty_post_pat,
            f"POST pattern routes with empty description string: {empty_post_pat}",
        )


class TestRouteDescriptionMechanism(unittest.TestCase):
    """U18 step 1: structural test that the route-description dispatch tables exist.

    Proves the registration plumbing mirrors the GET_ROUTES / POST_ROUTES /
    GET_PATTERNS / POST_PATTERNS pattern in web/server.py. Contents are
    populated in U18 step 2; empty is fine here.
    """

    def test_description_dispatch_tables_exist_with_correct_shapes(self):
        import re
        import web.server as srv

        # All four class attributes must exist.
        self.assertTrue(hasattr(srv.Handler, "_FUNC_GET_DESCRIPTIONS"))
        self.assertTrue(hasattr(srv.Handler, "_FUNC_POST_DESCRIPTIONS"))
        self.assertTrue(hasattr(srv.Handler, "_FUNC_GET_PATTERN_DESCRIPTIONS"))
        self.assertTrue(hasattr(srv.Handler, "_FUNC_POST_PATTERN_DESCRIPTIONS"))

        get_desc = srv.Handler._FUNC_GET_DESCRIPTIONS
        post_desc = srv.Handler._FUNC_POST_DESCRIPTIONS
        get_pattern_desc = srv.Handler._FUNC_GET_PATTERN_DESCRIPTIONS
        post_pattern_desc = srv.Handler._FUNC_POST_PATTERN_DESCRIPTIONS

        # Dict shapes: path (str) → description (str).
        self.assertIsInstance(get_desc, dict)
        self.assertIsInstance(post_desc, dict)
        for path, desc in get_desc.items():
            self.assertIsInstance(path, str)
            self.assertIsInstance(desc, str)
        for path, desc in post_desc.items():
            self.assertIsInstance(path, str)
            self.assertIsInstance(desc, str)

        # List-of-tuple shapes: (re.Pattern, str).
        self.assertIsInstance(get_pattern_desc, list)
        self.assertIsInstance(post_pattern_desc, list)
        for entry in get_pattern_desc:
            self.assertIsInstance(entry, tuple)
            self.assertEqual(len(entry), 2)
            self.assertIsInstance(entry[0], re.Pattern)
            self.assertIsInstance(entry[1], str)
        for entry in post_pattern_desc:
            self.assertIsInstance(entry, tuple)
            self.assertEqual(len(entry), 2)
            self.assertIsInstance(entry[0], re.Pattern)
            self.assertIsInstance(entry[1], str)


class TestApiIndexRouteContract(_WebServerCase):
    """U18 step 2: contract test for the self-documenting ``/api/_index``."""

    INDEX_ENTRY_REQUIRED_FIELDS = {
        "method", "path", "description", "request_model",
    }

    def test_api_index_returns_classified_routes_with_pydantic_models(self):
        status, data = self._get("/api/_index")
        self.assertEqual(status, 200)
        self.assertIsInstance(data, list)
        # We register ~40+ routes; assert a healthy floor so a regression
        # that empties the merge can't silently sneak through.
        self.assertGreaterEqual(len(data), 30, msg=f"only {len(data)} entries")

        for entry in data:
            _assert_required_fields(
                self, entry, self.INDEX_ENTRY_REQUIRED_FIELDS,
                f"_index entry {entry.get('path')!r}",
            )
            self.assertIn(entry["method"], {"GET", "POST"})
            self.assertIsInstance(entry["path"], str)
            self.assertIsInstance(entry["description"], str)

        # The Pydantic introspection must surface at least one known
        # POST handler so we know the regex is biting the real source.
        post_models = {
            (e["path"], e["request_model"])
            for e in data if e["method"] == "POST"
        }
        self.assertIn(
            ("/api/pipeline/add", "PipelineAddRequest"),
            post_models,
            f"PipelineAddRequest not surfaced in post_models: {post_models}",
        )

        # Sort invariant — operators consume this as a stable index.
        sorted_entries = sorted(
            data, key=lambda e: (str(e["method"]), str(e["path"])))
        self.assertEqual(data, sorted_entries)


class TestPipelineRouteContracts(_WebServerCase):
    """Contract tests for frontend-consumed pipeline GET routes."""

    PIPELINE_ITEM_REQUIRED_FIELDS = {
        "id", "artist_name", "album_title", "year", "format", "country",
        "source", "created_at", "status", "search_attempts",
        "download_attempts", "validation_attempts", "beets_distance",
        "mb_release_id",
        # Release-group id surfaces so the pipeline-row Replace button
        # (R7) can render — both the standard-mode source label and
        # the picker's inverted-row sibling lookup need it.
        "mb_release_group_id",
        "imported_path", "current_spectral_bitrate",
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
        "status", "processed", "deleted", "deleted_verified_lossless_parent",
        "kept_would_import", "kept_uncertain",
        "skipped_candidate_evidence_missing", "skipped_candidate_evidence_stale",
        "skipped_current_evidence_missing", "skipped_current_evidence_stale",
        "skipped_current_evidence_failed",
        "skipped_active_job", "skipped_invalid_row", "skipped_missing_path",
        "skipped_operational", "delete_failed", "results",
    }
    IMPORT_JOB_REQUIRED_FIELDS = {
        "id", "job_type", "status", "request_id", "dedupe_key", "payload",
        "result", "message", "error", "attempts", "worker_id", "created_at",
        "updated_at", "started_at", "heartbeat_at", "completed_at", "deduped",
        "preview_status", "preview_result", "preview_message", "preview_error",
        "preview_attempts", "preview_worker_id", "preview_started_at",
        "preview_heartbeat_at", "preview_completed_at", "importable_at",
    }
    DASHBOARD_REQUIRED_FIELDS = {
        "generated_at", "redis", "searches", "cycles", "coverage",
        "peer_dirs", "plan_readiness",
    }
    DASHBOARD_SEARCH_WINDOW_FIELDS = {
        "label", "hours", "searches", "distinct_requests",
        "searches_per_hour", "searches_per_24h", "avg_elapsed_s",
        "median_elapsed_s", "p95_elapsed_s", "max_elapsed_s", "outcomes",
        # Persisted-search-plans rollout (U7): wrap/stale/non-consuming
        # counts replace the exhausted-based reset signal. Cache
        # attribution is surfaced honestly (search_log has no per-search
        # cache columns today) so the dashboard cannot imply per-slot
        # cache numbers exist.
        "cursor_wraps", "stale_completions", "non_consuming",
        "cache_attribution_level",
    }
    DASHBOARD_PLAN_READINESS_FIELDS = {
        "generator_id", "wanted_total", "wanted_searchable",
        "wanted_legacy", "wanted_failed_deterministic",
        "wanted_failed_transient", "wanted_no_plan",
    }
    DASHBOARD_CYCLE_WINDOW_FIELDS = {
        "label", "hours", "cycles", "avg_cycle_s", "median_cycle_s",
        "p95_cycle_s", "max_cycle_s", "median_search_s", "watchdog_kills",
        "find_download_queued", "find_download_completed", "cache_errors",
        "cache_write_errors", "cache_fuse_tripped", "peers_browsed",
        "peers_browsed_lazy", "fanout_waves",
    }
    DASHBOARD_COVERAGE_FIELDS = {
        "wanted_total", "wanted_searched_24h", "wanted_searched_6h",
        "wanted_unsearched_24h", "wanted_unsearched_6h",
        "wanted_never_searched", "active_wanted_searches_24h",
        "active_wanted_searches_6h", "oldest_last_search_at",
        "matches_24h", "matches_6h", "matches_per_hour_24h",
        "matches_per_hour_6h", "match_rate_series_24h",
        "match_rate_series_28d", "wanted_trend", "top_10_share_24h",
        "top_loop_suspects", "stale_wanted",
    }
    DASHBOARD_WANTED_TREND_FIELDS = {
        "current_wanted", "latest_sample_at", "series_24h", "windows",
    }
    DASHBOARD_WANTED_TREND_POINT_FIELDS = {
        "sampled_at", "wanted_total",
    }
    DASHBOARD_WANTED_TREND_WINDOW_FIELDS = {
        "label", "hours", "sample_count", "start_sample_at",
        "end_sample_at", "start_wanted", "end_wanted", "delta",
        "delta_per_hour", "drain_per_hour", "eta_hours", "trend",
    }
    DASHBOARD_MATCH_RATE_POINT_FIELDS = {
        "bucket_start", "matches", "matches_per_hour",
    }
    DASHBOARD_DAILY_MATCH_RATE_POINT_FIELDS = {
        "bucket_start", "matches", "matches_per_day",
    }
    DASHBOARD_PEER_DIR_FIELDS = {
        "totals", "days", "heavy_queries", "heavy_query_hours",
    }
    DASHBOARD_PEER_DIR_TOTAL_FIELDS = {
        "known_combos", "known_peers", "known_dirs", "new_24h",
        "cold_seen_24h", "days_with_new", "tracked_since",
    }
    DASHBOARD_PEER_DIR_DAY_FIELDS = {
        "date", "new_combos", "new_peers", "new_dirs",
    }
    DASHBOARD_PEER_DIR_HEAVY_QUERY_FIELDS = {
        "search_log_id", "request_id", "mb_release_id", "artist_name",
        "album_title", "status", "created_at", "query", "variant",
        "outcome", "result_count", "elapsed_s", "browse_time_s",
        "match_time_s", "peers_browsed", "peers_browsed_lazy",
        "peer_dirs", "fanout_waves",
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
        _assert_required_fields(
            self,
            data["counts"],
            {
                "all", "imported", "rejected", "matches_24h",
                "matches_6h", "matches_per_hour_24h",
                "matches_per_hour_6h",
            },
            "pipeline log counts",
        )

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

    def test_pipeline_dashboard_contract(self):
        status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DASHBOARD_REQUIRED_FIELDS,
                                "pipeline dashboard response")
        _assert_required_fields(self, data["redis"], {"enabled", "status", "error"},
                                "pipeline dashboard redis")
        _assert_required_fields(self, data["searches"]["windows"][0],
                                self.DASHBOARD_SEARCH_WINDOW_FIELDS,
                                "pipeline dashboard search window")
        _assert_required_fields(self, data["searches"]["windows"][0]["outcomes"],
                                {"found", "no_match", "no_results", "exhausted", "errors"},
                                "pipeline dashboard search outcomes")
        _assert_required_fields(self, data["cycles"]["windows"][0],
                                self.DASHBOARD_CYCLE_WINDOW_FIELDS,
                                "pipeline dashboard cycle window")
        _assert_required_fields(self, data["coverage"],
                                self.DASHBOARD_COVERAGE_FIELDS,
                                "pipeline dashboard coverage")
        _assert_required_fields(self, data["peer_dirs"],
                                self.DASHBOARD_PEER_DIR_FIELDS,
                                "pipeline dashboard peer dirs")
        _assert_required_fields(self, data["peer_dirs"]["totals"],
                                self.DASHBOARD_PEER_DIR_TOTAL_FIELDS,
                                "pipeline dashboard peer dir totals")
        _assert_required_fields(self, data["peer_dirs"]["days"][0],
                                self.DASHBOARD_PEER_DIR_DAY_FIELDS,
                                "pipeline dashboard peer dir day")
        _assert_required_fields(self, data["peer_dirs"]["heavy_queries"][0],
                                self.DASHBOARD_PEER_DIR_HEAVY_QUERY_FIELDS,
                                "pipeline dashboard peer dir heavy query")
        self.assertIsInstance(data["coverage"]["top_loop_suspects"], list)
        self.assertIsInstance(data["coverage"]["stale_wanted"], list)
        self.assertIsInstance(data["coverage"]["match_rate_series_24h"], list)
        self.assertIsInstance(data["coverage"]["match_rate_series_28d"], list)
        _assert_required_fields(
            self,
            data["coverage"]["match_rate_series_24h"][0],
            self.DASHBOARD_MATCH_RATE_POINT_FIELDS,
            "pipeline dashboard match rate point",
        )
        _assert_required_fields(
            self,
            data["coverage"]["match_rate_series_28d"][0],
            self.DASHBOARD_DAILY_MATCH_RATE_POINT_FIELDS,
            "pipeline dashboard daily match rate point",
        )
        _assert_required_fields(
            self,
            data["coverage"]["wanted_trend"],
            self.DASHBOARD_WANTED_TREND_FIELDS,
            "pipeline dashboard wanted trend",
        )
        _assert_required_fields(
            self,
            data["coverage"]["wanted_trend"]["series_24h"][0],
            self.DASHBOARD_WANTED_TREND_POINT_FIELDS,
            "pipeline dashboard wanted trend point",
        )
        _assert_required_fields(
            self,
            data["coverage"]["wanted_trend"]["windows"][0],
            self.DASHBOARD_WANTED_TREND_WINDOW_FIELDS,
            "pipeline dashboard wanted trend window",
        )
        _assert_required_fields(
            self,
            data["coverage"]["top_loop_suspects"][0],
            {"reset_24h", "problem_24h"},
            "pipeline dashboard loop suspect",
        )
        # Persisted-search-plans plan-readiness panel (U7). Replaces
        # exhausted-based reporting with explicit plan-state buckets.
        _assert_required_fields(
            self,
            data["plan_readiness"],
            self.DASHBOARD_PLAN_READINESS_FIELDS,
            "pipeline dashboard plan readiness",
        )
        readiness = data["plan_readiness"]
        # Sum of buckets must equal wanted_total. Off-by-one means the
        # classifier dropped a row on the floor.
        self.assertEqual(
            readiness["wanted_total"],
            (readiness["wanted_searchable"]
             + readiness["wanted_legacy"]
             + readiness["wanted_failed_deterministic"]
             + readiness["wanted_failed_transient"]
             + readiness["wanted_no_plan"]),
            "plan_readiness buckets must sum to wanted_total",
        )
        # Cache attribution level on every search window is the honest
        # surface, not a per-slot number.
        self.assertEqual(
            data["searches"]["windows"][0]["cache_attribution_level"],
            "cycle_only",
        )

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

    def test_pipeline_simulate_threads_candidate_v0_probe_min(self):
        status, data = self._get(
            "/api/pipeline/simulate?"
            "is_flac=true&is_cbr=false&spectral_grade=likely_transcode"
            "&spectral_bitrate=160&converted_count=12"
            "&post_conversion_min_bitrate=237"
            "&candidate_v0_probe_avg=276&candidate_v0_probe_min=237"
            "&verified_lossless_target=opus%20128"
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["stage2_import"], "import")
        self.assertTrue(data["verified_lossless"])
        self.assertEqual(data["final_status"], "imported")
        self.assertFalse(data["keep_searching"])

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

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_wrong_match_triage_contract(self, mock_cleanup):
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
        mock_cleanup.return_value = WrongMatchCleanupSummary(
            processed=2,
            deleted=1,
            kept_uncertain=1,
        )
        status, data = self._post("/api/wrong-matches/triage", {
            "confirm_all_wrong_matches": True,
        })

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.WRONG_MATCH_TRIAGE_REQUIRED_FIELDS,
                                "wrong match triage response")
        mock_cleanup.assert_called_once_with(
            self.mock_db,
            confirm_all_wrong_matches=True,
        )
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["processed"], 2)
        self.assertEqual(data["deleted"], 1)

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_wrong_match_triage_requires_full_queue_confirmation(self, mock_cleanup):
        status, data = self._post("/api/wrong-matches/triage", {})

        self.assertEqual(status, 400)
        self.assertIn("confirm_all_wrong_matches", data.get("message") or data.get("error") or "")
        mock_cleanup.assert_not_called()

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

    def test_pipeline_simulate_threads_target_format(self):
        """Real ``preview_import_from_values`` honours the ``target_format``
        query param.

        Drives the real preview engine; the simulator's own coverage lives in
        ``tests/test_import_preview.py``. This test asserts only the wire-
        boundary contract: query params → response JSON.

        Threading proof: with ``verified_lossless_target=opus+128`` set,
        the simulator's ``target_final_format`` defaults to ``opus 128``.
        Adding ``target_format=flac`` overrides that — FLAC is a no-convert
        target and the simulator emits ``target_final_format=None``. If the
        route dropped ``target_format``, the simulator would fall back to
        the lossless target and the assertion would fail.
        """
        status, data = self._get(
            "/api/pipeline/simulate?is_flac=true&min_bitrate=900"
            "&spectral_grade=genuine&converted_count=12"
            "&post_conversion_min_bitrate=128"
            "&verified_lossless_target=opus+128"
            "&target_format=flac"
        )

        self.assertEqual(status, 200)
        # target_format=flac overrides verified_lossless_target=opus 128 →
        # target_final_format is None (no conversion happens for FLAC).
        self.assertIsNone(data.get("target_final_format"))
        # Sanity: without the target_format=flac override the simulator
        # would expose verified_lossless_target as target_final_format.
        status2, data2 = self._get(
            "/api/pipeline/simulate?is_flac=true&min_bitrate=900"
            "&spectral_grade=genuine&converted_count=12"
            "&post_conversion_min_bitrate=128"
            "&verified_lossless_target=opus+128"
        )
        self.assertEqual(status2, 200)
        self.assertEqual(data2.get("target_final_format"), "opus 128")

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
            "web simulator (matches production lib.measurement)")

        # VBR MP3 with low avg → stage 0 must say would_run
        status, data = self._get(
            "/api/pipeline/simulate?"
            "is_flac=false&min_bitrate=126&is_cbr=false&is_vbr=true&avg_bitrate=182"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            data["stage0_spectral_gate"], "would_run",
            "Go! Team-shape transcode must trigger the gate in the simulator")


class TestPipelineSearchPlanContract(_WebServerCase):
    """U6 contract for ``GET /api/pipeline/<id>/search-plan``.

    The frontend doesn't consume this in v1, but the route must be
    wired so the future dashboard can. The required-fields set is the
    contract operators / dashboard authors get to depend on.
    """

    REQUIRED_FIELDS = {
        "request_id",
        "request",
        "current_generator_id",
        "currentness",
        "active_plan",
        "latest_failed_deterministic",
        "latest_failed_transient",
        "superseded_count",
        "legacy_logs",
        "stats",
    }
    STATS_REQUIRED_FIELDS = {
        "request_id", "current", "superseded_and_legacy",
    }
    STATS_BUCKET_REQUIRED_FIELDS = {
        "slots", "query_groups", "legacy_bucket",
        "cache_attribution_level", "cache_per_search_available",
    }
    STATS_GROUP_REQUIRED_FIELDS = {
        "identity", "attempts", "consumed_attempts",
        "non_consuming_attempts", "stale_completion_attempts",
        "outcome_counts", "elapsed_s_mean", "elapsed_s_p95",
        "result_count_mean", "browse_time_s_mean", "match_time_s_mean",
        "peers_browsed_mean", "fanout_waves_mean", "last_seen_at",
    }
    REQUEST_REQUIRED_FIELDS = {
        "id", "status", "artist_name", "album_title",
        "mb_release_id", "discogs_release_id", "year", "source",
    }
    CURRENTNESS_REQUIRED_FIELDS = {
        "is_wanted",
        "has_active_plan",
        "active_plan_generator_id",
        "current_generator_searchable",
        "generator_id_mismatch",
        "has_deterministic_failure",
        "has_retryable_failure",
    }
    ACTIVE_PLAN_REQUIRED_FIELDS = {"plan", "items", "next_ordinal", "cycle_count"}
    PLAN_ROW_REQUIRED_FIELDS = {
        "id", "request_id", "generator_id", "status", "failure_class",
        "metadata_snapshot", "provenance", "error_message",
        "superseded_at", "superseded_by_plan_id", "created_at",
    }
    PLAN_ITEM_REQUIRED_FIELDS = {
        "id", "plan_id", "ordinal", "strategy", "query",
        "canonical_query_key", "repeat_group", "provenance",
    }
    LEGACY_LOGS_REQUIRED_FIELDS = {"count", "head"}
    LEGACY_HEAD_REQUIRED_FIELDS = {
        "id", "created_at", "outcome", "variant", "query",
        "result_count", "elapsed_s", "final_state",
    }

    def _wire_inspection(
        self,
        *,
        request_status: str = "wanted",
        request_id: int = 100,
        active: object = None,
        latest_failed_deterministic: object = None,
        latest_failed_transient: object = None,
        superseded_count: int = 0,
        legacy_log_count: int = 0,
        legacy_head_history: list[dict] | None = None,
    ) -> None:
        from lib.pipeline_db import (
            SearchPlanInspection,
            SearchPlanStats,
            SearchPlanStatsBucket,
            CACHE_ATTRIBUTION_CYCLE_ONLY,
        )
        from tests.helpers import make_request_row
        self.mock_db.get_request.return_value = make_request_row(
            id=request_id, status=request_status,
        )
        self.mock_db.get_search_history.reset_mock()
        self.mock_db.get_legacy_search_log_summary.reset_mock()
        self.mock_db.get_search_plan_stats_history.reset_mock()
        self.mock_db.get_search_plan_inspection.return_value = (
            SearchPlanInspection(
                request_id=request_id,
                active=active,  # type: ignore[arg-type]
                latest_failed_deterministic=latest_failed_deterministic,  # type: ignore[arg-type]
                latest_failed_transient=latest_failed_transient,  # type: ignore[arg-type]
                superseded_count=superseded_count,
                legacy_search_log_count=legacy_log_count,
            ))
        self.mock_db.get_legacy_search_log_summary.return_value = (
            legacy_log_count, legacy_head_history or [])
        self.mock_db.get_search_plan_stats_history.return_value = []
        empty_bucket = SearchPlanStatsBucket(
            slots=[], query_groups=[], legacy_bucket=None,
            cache_attribution_level=CACHE_ATTRIBUTION_CYCLE_ONLY,
            cache_per_search_available=False,
        )
        self.mock_db.get_search_plan_stats.return_value = SearchPlanStats(
            request_id=request_id,
            current=empty_bucket,
            superseded_and_legacy=empty_bucket,
        )

    def _make_active(
        self, *,
        generator_id: str = "search-plan/2026-05-25-1",
        items_count: int = 2,
        next_ordinal: int = 0,
        cycle_count: int = 0,
        plan_provenance: SearchPlanProvenance | None = None,
    ):
        from datetime import datetime, timezone
        from lib.pipeline_db import (
            ActiveSearchPlan, SearchPlanItemRow, SearchPlanRow,
            SearchPlanMetadataSnapshot, SearchPlanItemProvenance,
            SearchPlanProvenance,
        )
        plan = SearchPlanRow(
            id=11, request_id=100, generator_id=generator_id,
            status="active", failure_class=None,
            metadata_snapshot=SearchPlanMetadataSnapshot(artist_name="X"),
            provenance=plan_provenance,
            error_message=None, superseded_at=None,
            superseded_by_plan_id=None,
            created_at=datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc),
        )
        items = [
            SearchPlanItemRow(
                id=1000 + i, plan_id=11, ordinal=i,
                strategy=("default" if i == 0 else f"strategy_{i}"),
                query=f"q{i}", canonical_query_key=f"k{i}",
                repeat_group=("default-3" if i == 0 else None),
                provenance=SearchPlanItemProvenance(values={"src": "gen"}) if i == 0 else None,
            )
            for i in range(items_count)
        ]
        return ActiveSearchPlan(
            plan=plan, items=items,
            next_ordinal=next_ordinal, cycle_count=cycle_count,
        )

    def _make_failed_plan(
        self, *,
        plan_id: int,
        status: str,
        failure_class: str,
        error_message: str = "boom",
        generator_id: str = "search-plan/2026-05-19-1",
    ):
        from datetime import datetime, timezone
        from lib.pipeline_db import SearchPlanRow, SearchPlanProvenance
        return SearchPlanRow(
            id=plan_id, request_id=100, generator_id=generator_id,
            status=status, failure_class=failure_class,
            metadata_snapshot=None,
            provenance=SearchPlanProvenance(values={"reason": failure_class}),
            error_message=error_message, superseded_at=None,
            superseded_by_plan_id=None,
            created_at=datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc),
        )

    def tearDown(self) -> None:
        # Reset mocks so other suites in this class don't see leakage.
        from tests.helpers import make_request_row
        self.mock_db.get_request.return_value = make_request_row(
            id=100, status="imported", min_bitrate=320,
            imported_path="/mnt/virtio/Music/Beets/Test",
        )
        self.mock_db.get_legacy_search_log_summary.return_value = (0, [])
        self.mock_db.get_search_plan_stats_history.return_value = []
        self.mock_db.get_search_plan_inspection.reset_mock(
            return_value=True, side_effect=True)
        self.mock_db.get_search_plan_stats.reset_mock(
            return_value=True, side_effect=True)

    # -- happy path: active + failures + legacy logs --

    def test_search_plan_route_returns_full_inspection_payload(self):
        from lib.pipeline_db import SearchPlanProvenance
        active = self._make_active(plan_provenance=SearchPlanProvenance(values={
            "omitted_candidates": [{"q": "x", "why": "low_entropy"}],
            "dropped_low_entropy_tokens": ["the", "and"],
        }))
        det = self._make_failed_plan(
            plan_id=22, status="failed_deterministic",
            failure_class="no_runnable_query",
            error_message="all queries dropped")
        trans = self._make_failed_plan(
            plan_id=23, status="failed_transient",
            failure_class="resolver_unavailable",
            error_message="resolver 503")
        self._wire_inspection(
            active=active,
            latest_failed_deterministic=det,
            latest_failed_transient=trans,
            superseded_count=2,
            legacy_log_count=5,
            legacy_head_history=[
                {"id": 1, "request_id": 100, "outcome": "no_match",
                 "variant": "v1", "query": "q1", "result_count": 0,
                 "elapsed_s": 1.0, "final_state": "Completed",
                 "created_at": "2026-04-01T00:00:00+00:00",
                 "plan_id": None},
                {"id": 2, "request_id": 100, "outcome": "found",
                 "variant": "v1", "query": "q2", "result_count": 5,
                 "elapsed_s": 2.0, "final_state": "Completed",
                 "created_at": "2026-04-02T00:00:00+00:00",
                 "plan_id": None},
            ],
        )

        status, data = self._get("/api/pipeline/100/search-plan")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS,
                                "search-plan response")
        _assert_required_fields(self, data["request"],
                                self.REQUEST_REQUIRED_FIELDS,
                                "search-plan request")
        _assert_required_fields(self, data["currentness"],
                                self.CURRENTNESS_REQUIRED_FIELDS,
                                "search-plan currentness")
        self.assertTrue(data["currentness"]["current_generator_searchable"])
        self.assertFalse(data["currentness"]["generator_id_mismatch"])
        self.assertTrue(data["currentness"]["has_deterministic_failure"])
        self.assertTrue(data["currentness"]["has_retryable_failure"])
        self.assertIsNotNone(data["active_plan"])
        _assert_required_fields(self, data["active_plan"],
                                self.ACTIVE_PLAN_REQUIRED_FIELDS,
                                "search-plan active_plan")
        _assert_required_fields(self, data["active_plan"]["plan"],
                                self.PLAN_ROW_REQUIRED_FIELDS,
                                "search-plan active_plan.plan")
        for item in data["active_plan"]["items"]:
            _assert_required_fields(
                self, item, self.PLAN_ITEM_REQUIRED_FIELDS,
                "search-plan active_plan.items[]")
        # Items must be ordinal-ordered.
        ordinals = [it["ordinal"] for it in data["active_plan"]["items"]]
        self.assertEqual(ordinals, sorted(ordinals))
        # Failures are present and required-field-shaped.
        _assert_required_fields(
            self, data["latest_failed_deterministic"],
            self.PLAN_ROW_REQUIRED_FIELDS,
            "search-plan latest_failed_deterministic")
        _assert_required_fields(
            self, data["latest_failed_transient"],
            self.PLAN_ROW_REQUIRED_FIELDS,
            "search-plan latest_failed_transient")
        self.assertEqual(data["superseded_count"], 2)
        _assert_required_fields(
            self, data["legacy_logs"], self.LEGACY_LOGS_REQUIRED_FIELDS,
            "search-plan legacy_logs")
        self.assertEqual(data["legacy_logs"]["count"], 5)
        self.mock_db.get_legacy_search_log_summary.assert_called_once_with(
            100, limit=5)
        self.mock_db.get_search_plan_stats_history.assert_called_once_with(100)
        self.mock_db.get_search_history.assert_not_called()
        for row in data["legacy_logs"]["head"]:
            _assert_required_fields(
                self, row, self.LEGACY_HEAD_REQUIRED_FIELDS,
                "search-plan legacy_logs.head[]")
        # Stats payload structural contract.
        _assert_required_fields(
            self, data["stats"], self.STATS_REQUIRED_FIELDS,
            "search-plan stats")
        _assert_required_fields(
            self, data["stats"]["current"],
            self.STATS_BUCKET_REQUIRED_FIELDS,
            "search-plan stats.current")
        _assert_required_fields(
            self, data["stats"]["superseded_and_legacy"],
            self.STATS_BUCKET_REQUIRED_FIELDS,
            "search-plan stats.superseded_and_legacy")
        # Cache attribution must be honest about cycle-only counters.
        self.assertEqual(
            data["stats"]["current"]["cache_attribution_level"],
            "cycle_only")
        self.assertFalse(
            data["stats"]["current"]["cache_per_search_available"])

    # -- 404 missing request --

    def test_search_plan_missing_request_returns_404_with_error_body(self):
        self.mock_db.get_request.return_value = None
        status, data = self._get("/api/pipeline/9999/search-plan")
        self.assertEqual(status, 404)
        self.assertIn("error", data)

    # -- request with no plan at all (transient failure) --

    def test_search_plan_no_active_plan_with_only_transient_failure(self):
        trans = self._make_failed_plan(
            plan_id=23, status="failed_transient",
            failure_class="resolver_unavailable")
        self._wire_inspection(
            active=None, latest_failed_transient=trans,
        )
        status, data = self._get("/api/pipeline/100/search-plan")
        self.assertEqual(status, 200)
        self.assertIsNone(data["active_plan"])
        self.assertFalse(data["currentness"]["has_active_plan"])
        self.assertFalse(data["currentness"]["current_generator_searchable"])
        self.assertTrue(data["currentness"]["has_retryable_failure"])

    # -- legacy logs section is non-empty when only legacy rows exist --

    def test_search_plan_surfaces_legacy_logs_when_no_plan_context(self):
        self._wire_inspection(
            active=None,
            legacy_log_count=2,
            legacy_head_history=[
                {"id": 1, "request_id": 100, "outcome": "no_match",
                 "variant": "v1", "query": "q1", "result_count": 0,
                 "elapsed_s": 0.5, "final_state": "Completed",
                 "created_at": "2026-04-01T00:00:00+00:00",
                 "plan_id": None},
                {"id": 2, "request_id": 100, "outcome": "no_results",
                 "variant": "v2", "query": "q2", "result_count": 0,
                 "elapsed_s": 0.6, "final_state": "Completed",
                 "created_at": "2026-04-02T00:00:00+00:00",
                 "plan_id": None},
            ],
        )
        status, data = self._get("/api/pipeline/100/search-plan")
        self.assertEqual(status, 200)
        self.assertEqual(data["legacy_logs"]["count"], 2)
        self.assertEqual(len(data["legacy_logs"]["head"]), 2)

    # -- generator id drift: active plan but stale generator id --

    def test_search_plan_flags_generator_id_mismatch_when_stale(self):
        active = self._make_active(generator_id="search-plan/2026-01-01-old")
        self._wire_inspection(active=active)
        status, data = self._get("/api/pipeline/100/search-plan")
        self.assertEqual(status, 200)
        self.assertTrue(data["currentness"]["has_active_plan"])
        self.assertTrue(data["currentness"]["generator_id_mismatch"])
        self.assertFalse(data["currentness"]["current_generator_searchable"])
        self.assertEqual(
            data["currentness"]["active_plan_generator_id"],
            "search-plan/2026-01-01-old")


class TestPipelineSearchPlanDryRunContract(_WebServerCase):
    """U6 contract for ``GET /api/pipeline/<id>/search-plan/dry-run``.

    Read-only generator simulator. The route wraps
    ``SearchPlanService.dry_run_for_request``; both this route and
    ``pipeline-cli search-plan dry-run`` go through the same service
    method so input semantics + outcome mapping cannot drift. The
    contract guarantees the payload shape used by the future search-
    plan dashboard.
    """

    REQUIRED_FIELDS = {
        "request_id",
        "outcome",
        "current_generator_id",
        "request",
        "plan",
        "would_supersede_active",
        "error_message",
    }
    REQUEST_REQUIRED_FIELDS = {
        "id", "status", "artist_name", "album_title",
        "mb_release_id", "discogs_release_id", "year",
        "release_group_year", "source",
    }
    PLAN_REQUIRED_FIELDS = {
        "generator_id", "status", "items", "provenance",
        "failure_reason", "metadata_snapshot",
    }
    ITEM_REQUIRED_FIELDS = {
        "ordinal", "strategy", "query",
        "canonical_query_key", "repeat_group", "provenance",
    }

    def setUp(self) -> None:
        from tests.helpers import make_request_row
        self.mock_db.get_request.return_value = make_request_row(
            id=100, status="wanted",
            artist_name="Radiohead", album_title="Kid A",
            year=2008, release_group_year=2000,
        )
        self.mock_db.get_tracks.return_value = [
            {"disc_number": 1, "track_number": 1,
             "title": "Everything In Its Right Place"},
            {"disc_number": 1, "track_number": 2, "title": "Kid A"},
            {"disc_number": 1, "track_number": 3,
             "title": "The National Anthem"},
        ]
        self.mock_db.get_active_search_plan.return_value = None
        # Route reads runtime config — patch to a default CratediggerConfig.
        from lib.config import CratediggerConfig
        import configparser
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        self._cfg_patcher = patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp),
        )
        self._cfg_patcher.start()

    def tearDown(self) -> None:
        self._cfg_patcher.stop()
        self.mock_db.get_active_search_plan.reset_mock(
            return_value=True, side_effect=True)
        self.mock_db.get_tracks.return_value = []

    def test_dry_run_happy_path_returns_200_with_required_fields(self):
        status, data = self._get(
            "/api/pipeline/100/search-plan/dry-run")
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.REQUIRED_FIELDS, "dry-run")
        self.assertEqual(data["request_id"], 100)
        self.assertEqual(data["outcome"], "success")
        self.assertIsNotNone(data["request"])
        _assert_required_fields(
            self, data["request"], self.REQUEST_REQUIRED_FIELDS,
            "dry-run request")
        self.assertEqual(data["request"]["release_group_year"], 2000)
        self.assertIsNotNone(data["plan"])
        _assert_required_fields(
            self, data["plan"], self.PLAN_REQUIRED_FIELDS,
            "dry-run plan")
        self.assertGreater(len(data["plan"]["items"]), 0)
        for item in data["plan"]["items"]:
            _assert_required_fields(
                self, item, self.ITEM_REQUIRED_FIELDS,
                "dry-run plan.items[]")
        # Ordinals must be sorted.
        ordinals = [it["ordinal"] for it in data["plan"]["items"]]
        self.assertEqual(ordinals, sorted(ordinals))
        # No active plan was wired — would_supersede_active is False.
        self.assertFalse(data["would_supersede_active"])

    def test_dry_run_reports_active_plan_would_be_superseded(self):
        # When an active plan exists, the response flags
        # would_supersede_active=True so operators understand the
        # current plan would be replaced by a real regeneration call.
        self.mock_db.get_active_search_plan.return_value = MagicMock()
        status, data = self._get(
            "/api/pipeline/100/search-plan/dry-run")
        self.assertEqual(status, 200)
        self.assertTrue(data["would_supersede_active"])

    def test_dry_run_request_not_found_returns_404(self):
        self.mock_db.get_request.return_value = None
        status, data = self._get(
            "/api/pipeline/9999/search-plan/dry-run")
        self.assertEqual(status, 404)
        # 404 body must carry the same structured shape so clients can
        # introspect outcome / plan without status-code branching.
        _assert_required_fields(
            self, data, self.REQUIRED_FIELDS, "dry-run 404")
        self.assertEqual(data["outcome"], "request_not_found")
        self.assertEqual(data["request_id"], 9999)
        self.assertIsNone(data["plan"])
        self.assertIsNone(data["request"])
        self.assertIn("error", data)

    def test_dry_run_request_with_no_tracks_succeeds(self):
        # Generator handles empty tracks — emits a plan without
        # track-fallback slots; the dry-run reflects exactly that.
        self.mock_db.get_tracks.return_value = []
        status, data = self._get(
            "/api/pipeline/100/search-plan/dry-run")
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "success")
        self.assertIsNotNone(data["plan"])
        # No track_* slots should appear.
        strategies = [
            it["strategy"] for it in data["plan"]["items"]
        ]
        self.assertFalse(
            any(s.startswith("track_") for s in strategies),
            f"unexpected track-fallback slots in zero-tracks plan: "
            f"{strategies}")

    def test_dry_run_does_not_persist_or_write_plan_rows(self):
        # The simulator must never call into create_successful_search_plan
        # / supersede_search_plan_with_replacement / create_failed_search_plan.
        self.mock_db.create_successful_search_plan.reset_mock()
        self.mock_db.supersede_search_plan_with_replacement.reset_mock()
        self.mock_db.create_failed_search_plan.reset_mock()
        status, _ = self._get(
            "/api/pipeline/100/search-plan/dry-run")
        self.assertEqual(status, 200)
        self.mock_db.create_successful_search_plan.assert_not_called()
        self.mock_db.supersede_search_plan_with_replacement.assert_not_called()
        self.mock_db.create_failed_search_plan.assert_not_called()

    def test_dry_run_carries_current_generator_id(self):
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        status, data = self._get(
            "/api/pipeline/100/search-plan/dry-run")
        self.assertEqual(status, 200)
        self.assertEqual(
            data["current_generator_id"], SEARCH_PLAN_GENERATOR_ID)
        # The plan the simulator emits must be pinned to the same id.
        self.assertEqual(
            data["plan"]["generator_id"], SEARCH_PLAN_GENERATOR_ID)


class TestPipelineSearchPlanSaturationContract(_WebServerCase):
    """U7 contract for ``GET /api/pipeline/<id>/search-plan/saturation``.

    Read-only telemetry aggregator. The route wraps
    ``SearchPlanService.saturation_for_request``; both this route and
    ``pipeline-cli search-plan saturation`` go through the same
    service method so input semantics + outcome mapping cannot drift.
    The contract guarantees the payload shape consumed by the future
    search-plan dashboard.
    """

    REQUIRED_FIELDS = {
        "total_searches",
        "saturated_searches",
        "saturation_rate",
        "total_pre_filter_skips",
        "window_days",
    }
    FULL_REQUIRED_FIELDS = REQUIRED_FIELDS | {
        "request_id", "outcome", "error_message",
    }

    def setUp(self) -> None:
        from tests.helpers import make_request_row
        # Production-shape mock: use real datetime + UUID values on the
        # request row so the JSON encoder path is exercised end-to-end.
        # The route does not consult timestamps directly, but the
        # ``get_request`` call walks through the JSON encoder boundary
        # if anything else does — keep the row shape honest.
        import uuid
        self.mock_db.get_request.return_value = make_request_row(
            id=100, status="wanted",
            artist_name="Radiohead", album_title="Kid A",
            mb_release_id=str(uuid.uuid4()),
        )
        # Route reads runtime config — patch to a default config.
        from lib.config import CratediggerConfig
        import configparser
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        self._cfg_patcher = patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp),
        )
        self._cfg_patcher.start()
        # Default saturation summary — happy-path scenario.
        from lib.pipeline_db import SaturationSummary
        self.mock_db.get_saturation_summary.return_value = SaturationSummary(
            total_searches=30,
            saturated_searches=10,
            saturation_rate=10 / 30,
            total_pre_filter_skips=50,
            window_days=14,
        )

    def tearDown(self) -> None:
        self._cfg_patcher.stop()
        self.mock_db.get_saturation_summary.reset_mock(
            return_value=True, side_effect=True)

    def test_happy_path_returns_200_with_required_fields(self):
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation")
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.FULL_REQUIRED_FIELDS, "saturation")
        self.assertEqual(data["request_id"], 100)
        self.assertEqual(data["outcome"], "success")
        self.assertEqual(data["total_searches"], 30)
        self.assertEqual(data["saturated_searches"], 10)
        self.assertAlmostEqual(data["saturation_rate"], 10 / 30)
        self.assertEqual(data["total_pre_filter_skips"], 50)
        self.assertEqual(data["window_days"], 14)
        self.assertIsNone(data["error_message"])

    def test_empty_window_is_200_with_zeros_not_404(self):
        # Found-but-quiet: request exists, window is just empty.
        from lib.pipeline_db import SaturationSummary
        self.mock_db.get_saturation_summary.return_value = SaturationSummary(
            total_searches=0, saturated_searches=0,
            saturation_rate=0.0, total_pre_filter_skips=0,
            window_days=14,
        )
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation")
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "success")
        # Crucial: rate is 0.0, NOT NaN — JSON parses cleanly.
        self.assertEqual(data["saturation_rate"], 0.0)
        self.assertEqual(data["total_searches"], 0)

    def test_request_not_found_returns_404(self):
        self.mock_db.get_request.return_value = None
        status, data = self._get(
            "/api/pipeline/9999/search-plan/saturation")
        self.assertEqual(status, 404)
        _assert_required_fields(
            self, data, self.FULL_REQUIRED_FIELDS, "saturation 404")
        self.assertEqual(data["outcome"], "request_not_found")
        self.assertEqual(data["request_id"], 9999)
        # All summary counts zero-filled — clients can read without
        # branching on status code first.
        self.assertEqual(data["total_searches"], 0)
        self.assertEqual(data["saturation_rate"], 0.0)
        self.assertIn("error", data)
        # The DB aggregator must NOT be called when the request row
        # itself is missing — wastes a query and risks misleading zeros.
        self.mock_db.get_saturation_summary.assert_not_called()

    def test_window_days_query_string_propagates_to_service(self):
        from lib.pipeline_db import SaturationSummary
        self.mock_db.get_saturation_summary.return_value = SaturationSummary(
            total_searches=5, saturated_searches=1,
            saturation_rate=0.2, total_pre_filter_skips=3,
            window_days=7,
        )
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation?window_days=7")
        self.assertEqual(status, 200)
        self.assertEqual(data["window_days"], 7)
        # The aggregator was called with the requested window.
        self.mock_db.get_saturation_summary.assert_called_with(
            100, window_days=7)

    def test_invalid_window_days_non_int_returns_400(self):
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation?window_days=abc")
        self.assertEqual(status, 400)

    def test_invalid_window_days_out_of_range_returns_400(self):
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation?window_days=0")
        self.assertEqual(status, 400)
        _assert_required_fields(
            self, data, self.FULL_REQUIRED_FIELDS, "saturation 400")
        self.assertEqual(data["outcome"], "input_invalid")

    def test_default_window_is_14_when_omitted(self):
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation")
        self.assertEqual(status, 200)
        self.assertEqual(data["window_days"], 14)


class TestPipelineSearchPlanRegenerateContract(_WebServerCase):
    """U8 contract for ``POST /api/pipeline/<id>/search-plan/regenerate``.

    The endpoint must wrap ``SearchPlanService.generate_for_request``
    with status-code mapping that mirrors the CLI exit codes:
      * 200 — success / noop
      * 404 — request not found
      * 422 — deterministic failure (sticky)
      * 503 — transient failure (retryable)
    """

    REGEN_REQUIRED_FIELDS = {
        "request_id", "outcome", "plan_id", "is_supersede",
        "failure_class", "error_message",
        "request_status", "executable",
    }

    def _patch_service(self, *, outcome, plan_id=None, is_supersede=False,
                       failure_class=None, error_message=None):
        from unittest.mock import patch as _patch
        from lib.search_plan_service import ServiceResult

        result = ServiceResult(
            outcome=outcome, plan_id=plan_id, is_supersede=is_supersede,
            failure_class=failure_class, error_message=error_message,
        )
        return _patch(
            "lib.search_plan_service.SearchPlanService.generate_for_request",
            return_value=result,
        )

    def setUp(self) -> None:
        from tests.helpers import make_request_row
        self.mock_db.get_request.return_value = make_request_row(
            id=100, status="wanted",
        )
        # The route reads the runtime config — patch it to a default.
        from lib.config import CratediggerConfig
        import configparser
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        self._cfg_patcher = patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp),
        )
        self._cfg_patcher.start()

    def tearDown(self) -> None:
        self._cfg_patcher.stop()

    def test_regenerate_success_returns_200_with_required_fields(self):
        with self._patch_service(outcome="success", plan_id=99,
                                 is_supersede=True):
            status, data = self._post(
                "/api/pipeline/100/search-plan/regenerate", {})
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REGEN_REQUIRED_FIELDS,
                                "search-plan regenerate response")
        self.assertEqual(data["outcome"], "success")
        self.assertEqual(data["plan_id"], 99)
        self.assertTrue(data["is_supersede"])
        # Wanted request -> executable=True after success.
        self.assertTrue(data["executable"])
        self.assertEqual(data["request_status"], "wanted")

    def test_regenerate_success_for_imported_request_marks_not_executable(self):
        from tests.helpers import make_request_row
        self.mock_db.get_request.return_value = make_request_row(
            id=100, status="imported",
        )
        with self._patch_service(outcome="success", plan_id=99):
            status, data = self._post(
                "/api/pipeline/100/search-plan/regenerate", {})
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "success")
        self.assertFalse(data["executable"])
        self.assertEqual(data["request_status"], "imported")

    def test_regenerate_request_not_found_returns_404(self):
        self.mock_db.get_request.return_value = None
        with self._patch_service(outcome="request_not_found"):
            status, data = self._post(
                "/api/pipeline/9999/search-plan/regenerate", {})
        self.assertEqual(status, 404)
        # 404 body must carry the same structured shape as 422/503 so
        # clients can introspect outcome / plan_id without status-code
        # branching (#8). plan_id is None on the not-found path.
        self.assertIn("error", data)
        self.assertEqual(data["outcome"], "request_not_found")
        self.assertIn("plan_id", data)
        self.assertIsNone(data["plan_id"])
        self.assertEqual(data["request_id"], 9999)

    def test_regenerate_noop_returns_200_with_noop_outcome(self):
        """#7: NOOP outcome from the service surfaces as 200 with the
        outcome string and the existing plan_id, not as a generic 200."""
        with self._patch_service(
                outcome="noop_active_plan_exists",
                plan_id=77, is_supersede=False):
            status, data = self._post(
                "/api/pipeline/100/search-plan/regenerate", {})
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REGEN_REQUIRED_FIELDS,
                                "search-plan regenerate noop response")
        self.assertEqual(data["outcome"], "noop_active_plan_exists")
        self.assertEqual(data["plan_id"], 77)
        self.assertFalse(data["is_supersede"])

    def test_regenerate_deterministic_failure_returns_422(self):
        with self._patch_service(
                outcome="failed_deterministic",
                failure_class="no_runnable_query",
                error_message="all queries dropped"):
            status, data = self._post(
                "/api/pipeline/100/search-plan/regenerate", {})
        self.assertEqual(status, 422)
        self.assertEqual(data["outcome"], "failed_deterministic")
        self.assertEqual(data["failure_class"], "no_runnable_query")
        self.assertEqual(data["error"], "all queries dropped")

    def test_regenerate_transient_failure_returns_503(self):
        with self._patch_service(
                outcome="failed_transient",
                failure_class="resolver_unavailable",
                error_message="resolver 503"):
            status, data = self._post(
                "/api/pipeline/100/search-plan/regenerate", {})
        self.assertEqual(status, 503)
        self.assertEqual(data["outcome"], "failed_transient")
        self.assertEqual(data["failure_class"], "resolver_unavailable")
        self.assertEqual(data["error"], "resolver 503")

    def test_regenerate_passes_prepend_artist_flag_to_service(self):
        from unittest.mock import patch as _patch
        from lib.search_plan_service import ServiceResult
        with _patch(
            "lib.search_plan_service.SearchPlanService.generate_for_request",
            return_value=ServiceResult(outcome="success", plan_id=1),
        ) as mock_gen:
            status, _ = self._post(
                "/api/pipeline/100/search-plan/regenerate",
                {"prepend_artist": True})
        self.assertEqual(status, 200)
        mock_gen.assert_called_once()
        kwargs = mock_gen.call_args.kwargs
        self.assertTrue(kwargs.get("prepend_artist"))
        self.assertTrue(kwargs.get("regenerate"))

    def test_regenerate_passes_missing_prepend_artist_as_default(self):
        from unittest.mock import patch as _patch
        from lib.search_plan_service import ServiceResult
        with _patch(
            "lib.search_plan_service.SearchPlanService.generate_for_request",
            return_value=ServiceResult(outcome="success", plan_id=1),
        ) as mock_gen:
            status, _ = self._post(
                "/api/pipeline/100/search-plan/regenerate", {})
        self.assertEqual(status, 200)
        self.assertIsNone(mock_gen.call_args.kwargs.get("prepend_artist"))

    def test_regenerate_rejects_string_prepend_artist(self):
        """Strict-bool field rejects the string ``"false"`` (Pydantic
        v2 lax mode would coerce it). The exact phrasing comes from
        Pydantic now; assert the field name appears in the error so
        the frontend can render a sensible message regardless of which
        validator wrote it.
        """
        from unittest.mock import patch as _patch
        with _patch(
            "lib.search_plan_service.SearchPlanService.generate_for_request",
        ) as mock_gen:
            status, data = self._post(
                "/api/pipeline/100/search-plan/regenerate",
                {"prepend_artist": "false"})
        self.assertEqual(status, 400)
        self.assertIn("prepend_artist", data["error"])
        mock_gen.assert_not_called()

    def test_regenerate_rejects_non_dict_body_with_400(self):
        """F2: a non-dict JSON body (e.g. raw string) must return 400,
        not 500 from AttributeError on body.get()."""
        from unittest.mock import patch as _patch
        raw_body = b'"hello"'  # valid JSON but not an object
        req = Request(
            f"{self.base}/api/pipeline/100/search-plan/regenerate",
            data=raw_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with _patch(
            "lib.search_plan_service.SearchPlanService.generate_for_request",
        ) as mock_gen:
            try:
                resp = urlopen(req, timeout=5)
                status = resp.status
                data = json.loads(resp.read())
            except HTTPError as e:
                status = e.code
                data = json.loads(e.read())
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        mock_gen.assert_not_called()


class TestPipelineSearchPlanAdvanceContract(_WebServerCase):
    """Contract for ``POST /api/pipeline/<id>/search-plan/advance``.

    The endpoint wraps ``SearchPlanService.advance_for_request``. Both
    the CLI (``pipeline-cli search-plan advance``) and the API live or
    die on the same service contract — see ``CLAUDE.md`` § "CLI ⇄ API
    surface symmetry"; touching one without the other is a contract
    drift waiting to happen.

    Status-code mapping mirrors the CLI exit codes:
      * 200 — RESULT_ADVANCED
      * 400 — body validation failure
      * 404 — RESULT_REQUEST_NOT_FOUND
      * 409 — RESULT_NO_ACTIVE_PLAN
      * 422 — RESULT_INVALID_TARGET
      * 503 — RESULT_FAILED_TRANSIENT
    """

    ADVANCE_REQUIRED_FIELDS = {
        "request_id", "outcome", "plan_id", "previous_ordinal",
        "new_ordinal", "new_strategy", "new_query", "error_message",
    }

    def setUp(self) -> None:
        from lib.config import CratediggerConfig
        import configparser
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        self._cfg_patcher = patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp),
        )
        self._cfg_patcher.start()

    def tearDown(self) -> None:
        self._cfg_patcher.stop()

    def _patch_service(self, **result_kwargs):
        from unittest.mock import patch as _patch
        from lib.search_plan_service import AdvanceResult
        return _patch(
            "lib.search_plan_service.SearchPlanService.advance_for_request",
            return_value=AdvanceResult(**result_kwargs),
        )

    def test_advance_success_returns_200_with_required_fields(self):
        with self._patch_service(
                outcome="advanced", request_id=100, plan_id=42,
                previous_ordinal=1, new_ordinal=7,
                new_strategy="track_0", new_query="Love Till Tuesday"):
            status, data = self._post(
                "/api/pipeline/100/search-plan/advance",
                {"to_ordinal": 7})
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADVANCE_REQUIRED_FIELDS,
                                "search-plan advance response")
        self.assertEqual(data["outcome"], "advanced")
        self.assertEqual(data["new_ordinal"], 7)
        self.assertEqual(data["new_strategy"], "track_0")

    def test_advance_request_not_found_returns_404(self):
        with self._patch_service(
                outcome="request_not_found", request_id=9999,
                error_message="request 9999 not found"):
            status, data = self._post(
                "/api/pipeline/9999/search-plan/advance",
                {"to_ordinal": 7})
        self.assertEqual(status, 404)
        _assert_required_fields(self, data, self.ADVANCE_REQUIRED_FIELDS,
                                "404 advance response")
        self.assertIn("error", data)

    def test_advance_no_active_plan_returns_409(self):
        with self._patch_service(
                outcome="no_active_plan", request_id=100,
                error_message="request 100 has no active plan"):
            status, data = self._post(
                "/api/pipeline/100/search-plan/advance",
                {"to_ordinal": 1})
        self.assertEqual(status, 409)
        self.assertEqual(data["outcome"], "no_active_plan")
        self.assertIn("error", data)

    def test_advance_invalid_target_returns_422(self):
        with self._patch_service(
                outcome="invalid_target", request_id=100, plan_id=42,
                previous_ordinal=5, error_message="target 3 must be > 5"):
            status, data = self._post(
                "/api/pipeline/100/search-plan/advance",
                {"to_ordinal": 3})
        self.assertEqual(status, 422)
        self.assertEqual(data["outcome"], "invalid_target")
        self.assertIn("error", data)

    def test_advance_lock_contention_returns_503(self):
        with self._patch_service(
                outcome="failed_transient", request_id=100,
                error_message="another writer holds the plan lock"):
            status, data = self._post(
                "/api/pipeline/100/search-plan/advance",
                {"to_ordinal": 1})
        self.assertEqual(status, 503)
        self.assertEqual(data["outcome"], "failed_transient")
        self.assertIn("error", data)

    def test_advance_passes_to_strategy_to_service(self):
        from unittest.mock import patch as _patch
        from lib.search_plan_service import AdvanceResult
        with _patch(
            "lib.search_plan_service.SearchPlanService.advance_for_request",
            return_value=AdvanceResult(
                outcome="advanced", request_id=100, plan_id=1,
                previous_ordinal=0, new_ordinal=7,
                new_strategy="track_0", new_query="X"),
        ) as mock_adv:
            status, _ = self._post(
                "/api/pipeline/100/search-plan/advance",
                {"to_strategy": "track"})
        self.assertEqual(status, 200)
        mock_adv.assert_called_once()
        kwargs = mock_adv.call_args.kwargs
        self.assertEqual(kwargs.get("to_strategy"), "track")
        self.assertIsNone(kwargs.get("to_ordinal"))

    def test_advance_rejects_missing_target(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.search_plan_service.SearchPlanService.advance_for_request",
        ) as mock_adv:
            status, data = self._post(
                "/api/pipeline/100/search-plan/advance", {})
        self.assertEqual(status, 400)
        self.assertIn("to_ordinal", data["error"])
        mock_adv.assert_not_called()

    def test_advance_rejects_both_targets(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.search_plan_service.SearchPlanService.advance_for_request",
        ) as mock_adv:
            status, data = self._post(
                "/api/pipeline/100/search-plan/advance",
                {"to_ordinal": 1, "to_strategy": "track"})
        self.assertEqual(status, 400)
        mock_adv.assert_not_called()

    def test_advance_rejects_non_int_ordinal(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.search_plan_service.SearchPlanService.advance_for_request",
        ) as mock_adv:
            status, data = self._post(
                "/api/pipeline/100/search-plan/advance",
                {"to_ordinal": "seven"})
        self.assertEqual(status, 400)
        self.assertIn("integer", data["error"])
        mock_adv.assert_not_called()


class TestReplacedFilterContract(_WebServerCase):
    """U10 backend tests for the ``?include_replaced`` query parameter
    on pipeline + wrong-matches list endpoints, plus the descendant_*
    fields surfaced from ``post_pipeline_add`` when the existing row is
    ``status='replaced'``.
    """

    def setUp(self) -> None:
        # Default mock for the supersede-lookup so the add-flow tests
        # below can override per-test.
        self.mock_db.get_request_by_replaces_request_id.return_value = None

    def test_pipeline_all_default_excludes_replaced(self):
        captured: list[tuple[str, ...]] = []
        def fake_get_by_status(status):
            captured.append((status,))
            return []
        self.mock_db.get_by_status.side_effect = fake_get_by_status
        self.mock_db.count_by_status.return_value = {}
        self.mock_db.get_download_history_batch.return_value = {}
        status, _ = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        statuses = [c[0] for c in captured]
        self.assertNotIn("replaced", statuses)

    def test_pipeline_all_include_replaced_true_fetches_replaced(self):
        captured: list[tuple[str, ...]] = []
        def fake_get_by_status(status):
            captured.append((status,))
            return []
        self.mock_db.get_by_status.side_effect = fake_get_by_status
        self.mock_db.count_by_status.return_value = {}
        self.mock_db.get_download_history_batch.return_value = {}
        status, _ = self._get("/api/pipeline/all?include_replaced=true")
        self.assertEqual(status, 200)
        statuses = [c[0] for c in captured]
        self.assertIn("replaced", statuses)

    def test_post_pipeline_add_with_replaced_existing_surfaces_descendant(self):
        # The harness routes get_request_by_release_id through
        # get_request_by_mb_release_id (see _make_server), so mock that.
        self.mock_db.get_request_by_mb_release_id.return_value = {
            "id": 42, "status": "replaced",
            "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        }
        self.mock_db.get_request_by_replaces_request_id.return_value = {
            "id": 99, "status": "wanted",
            "mb_release_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        }
        status, data = self._post(
            "/api/pipeline/add",
            {"mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "exists")
        self.assertEqual(data["current_status"], "replaced")
        self.assertEqual(data["descendant_request_id"], 99)
        self.assertEqual(data["descendant_status"], "wanted")

    def test_post_pipeline_add_with_non_replaced_existing_omits_descendant(self):
        self.mock_db.get_request_by_mb_release_id.return_value = {
            "id": 42, "status": "wanted",
            "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        }
        status, data = self._post(
            "/api/pipeline/add",
            {"mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["current_status"], "wanted")
        self.assertNotIn("descendant_request_id", data)


class TestPipelineReplaceContract(_WebServerCase):
    """Contract for ``POST /api/pipeline/<id>/replace`` plus the two
    auxiliary endpoints (``GET /api/pipeline/requests-by-rg/<rg>`` and
    ``GET /api/pipeline/active-rgs``).

    The endpoint wraps ``MbidReplaceService.replace_request_mbid``. The
    CLI counterpart (``pipeline-cli replace``) must stay in sync — see
    ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"; touching one without
    the other is a contract drift waiting to happen.

    Status-code mapping mirrors the CLI exit codes:
      * 200 — RESULT_REPLACED
      * 400 — body validation failure (missing/empty target)
      * 404 — RESULT_NOT_FOUND
      * 409 — RESULT_WRONG_STATE, RESULT_TARGET_COLLISION_REQUEST
      * 422 — RESULT_TARGET_INVALID, RESULT_TARGET_RELEASE_GROUP_MISMATCH,
              RESULT_TARGET_SAME_AS_CURRENT
      * 503 — RESULT_TRANSIENT
    """

    REPLACE_REQUIRED_FIELDS = {
        "outcome", "request_id", "new_request_id", "current_status",
        "descendant_request_id", "error_message", "warnings",
    }
    REQUESTS_BY_RG_FIELDS = {
        "id", "mb_release_id", "mb_release_group_id", "status",
        "artist_name", "album_title",
    }

    def setUp(self) -> None:
        from lib.config import CratediggerConfig
        import configparser
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        self._cfg_patcher = patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp),
        )
        self._cfg_patcher.start()

    def tearDown(self) -> None:
        self._cfg_patcher.stop()

    def _patch_service(self, **result_kwargs):
        from unittest.mock import patch as _patch
        from lib.mbid_replace_service import ReplaceResult
        return _patch(
            "lib.mbid_replace_service.MbidReplaceService"
            ".replace_request_mbid",
            return_value=ReplaceResult(**result_kwargs),
        )

    def test_replace_success_returns_200(self):
        with self._patch_service(
            outcome="replaced", request_id=100, new_request_id=200,
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REPLACE_REQUIRED_FIELDS,
                                "replace response")
        self.assertEqual(data["outcome"], "replaced")
        self.assertEqual(data["new_request_id"], 200)

    def test_replace_not_found_returns_404(self):
        with self._patch_service(
            outcome="not_found", request_id=9999,
            error_message="request 9999 not found",
        ):
            status, data = self._post(
                "/api/pipeline/9999/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_replace_wrong_state_lock_contention_returns_409(self):
        with self._patch_service(
            outcome="wrong_state", request_id=100,
            error_message="importer holds the lock",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 409)
        self.assertIsNone(data["descendant_request_id"])

    def test_replace_wrong_state_source_already_replaced_carries_descendant(self):
        with self._patch_service(
            outcome="wrong_state", request_id=42, descendant_request_id=99,
            error_message="already replaced",
        ):
            status, data = self._post(
                "/api/pipeline/42/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 409)
        self.assertEqual(data["descendant_request_id"], 99)

    def test_replace_collision_carries_current_status(self):
        with self._patch_service(
            outcome="target_collision_request", request_id=100,
            current_status="wanted",
            error_message="target held by request 43",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 409)
        self.assertEqual(data["current_status"], "wanted")

    def test_replace_target_invalid_returns_422(self):
        with self._patch_service(
            outcome="target_invalid", request_id=100,
            error_message="MB lookup empty",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "bogus"},
            )
        self.assertEqual(status, 422)

    def test_replace_rg_mismatch_returns_422(self):
        with self._patch_service(
            outcome="target_release_group_mismatch", request_id=100,
            error_message="rg mismatch",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "other-rg"},
            )
        self.assertEqual(status, 422)

    def test_replace_same_as_current_returns_422(self):
        with self._patch_service(
            outcome="target_same_as_current", request_id=100,
            error_message="target == source",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "same-uuid"},
            )
        self.assertEqual(status, 422)

    def test_replace_transient_returns_503(self):
        """503 maps to RESULT_TRANSIENT — typically an MB-mirror
        network blip / timeout / JSON decode error during the fresh
        target lookup. The response body must still carry the full
        REPLACE_REQUIRED_FIELDS contract so the frontend can show the
        "Retry" affordance and the error message uniformly with the
        other outcomes."""
        with self._patch_service(
            outcome="transient", request_id=100,
            error_message="MB mirror unreachable",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.REPLACE_REQUIRED_FIELDS,
            "replace 503 response",
        )
        self.assertEqual(data["outcome"], "transient")
        self.assertEqual(data["request_id"], 100)
        self.assertEqual(
            data["error_message"], "MB mirror unreachable",
        )
        # Optional payload fields stay null on a transient outcome
        # (no new row, no current_status, no descendant).
        self.assertIsNone(data["new_request_id"])
        self.assertIsNone(data["current_status"])
        self.assertIsNone(data["descendant_request_id"])

    def test_replace_missing_target_returns_400(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.mbid_replace_service.MbidReplaceService"
            ".replace_request_mbid"
        ) as mock_svc:
            status, data = self._post(
                "/api/pipeline/100/replace", {},
            )
        self.assertEqual(status, 400)
        self.assertIn("target_mb_release_id", data["error"])
        mock_svc.assert_not_called()

    def test_replace_empty_target_returns_400(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.mbid_replace_service.MbidReplaceService"
            ".replace_request_mbid"
        ) as mock_svc:
            status, _ = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "  "},
            )
        self.assertEqual(status, 400)
        mock_svc.assert_not_called()

    def test_requests_by_rg_returns_200_with_required_fields(self):
        self.mock_db.list_requests_in_release_group.return_value = [
            {
                "id": 42, "mb_release_id": "old-uuid",
                "mb_release_group_id": "rg-1",
                "status": "wanted",
                "artist_name": "Pet Grief", "album_title": "X",
            },
        ]
        status, data = self._get(
            "/api/pipeline/requests-by-rg/"
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        self.assertEqual(status, 200)
        self.assertIn("requests", data)
        self.assertEqual(len(data["requests"]), 1)
        _assert_required_fields(
            self, data["requests"][0],
            self.REQUESTS_BY_RG_FIELDS,
            "requests-by-rg row",
        )

    def test_requests_by_rg_empty_list(self):
        self.mock_db.list_requests_in_release_group.return_value = []
        status, data = self._get(
            "/api/pipeline/requests-by-rg/"
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["requests"], [])

    def test_active_rgs_returns_sorted_list(self):
        self.mock_db.list_active_release_group_ids.return_value = {
            "rg-bbbb", "rg-aaaa",
        }
        status, data = self._get("/api/pipeline/active-rgs")
        self.assertEqual(status, 200)
        self.assertEqual(data["release_group_ids"], ["rg-aaaa", "rg-bbbb"])

    def test_active_rgs_empty(self):
        self.mock_db.list_active_release_group_ids.return_value = set()
        status, data = self._get("/api/pipeline/active-rgs")
        self.assertEqual(status, 200)
        self.assertEqual(data["release_group_ids"], [])


class TestPipelineResolveRgContract(_WebServerCase):
    """Contract for ``POST /api/pipeline/<id>/resolve-rg``.

    Lazy-backfill ``mb_release_group_id`` for legacy rows. The Replace
    picker calls this in standard mode when the row has a null RG so the
    sibling-fetch can proceed.

    Status-code mapping:
      * 200 — ``status='resolved'`` (RG found or already set)
      * 404 — request not found
      * 422 — non-UUID release id (Discogs) or MB returned no RG
      * 503 — transient MB-mirror failure
    """

    RESOLVE_RG_REQUIRED_FIELDS = {
        "request_id", "mb_release_group_id", "status",
    }

    def setUp(self) -> None:
        # _WebServerCase shares ``mock_db`` across tests in the class via
        # setUpClass; reset call counts here so per-test assertions don't
        # see stale calls from earlier tests in the same class.
        self.mock_db.reset_mock()

    def test_resolve_rg_already_set_returns_200(self):
        """Idempotent: row already has a RG → return it untouched
        and do NOT hit the MB mirror or write to the DB."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": "rrrrrrrr-rrrr-rrrr-rrrr-rrrrrrrrrrrr",
        }
        with patch("web.mb.get_release") as mock_mb:
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg already-set response",
        )
        self.assertEqual(data["status"], "resolved")
        self.assertEqual(
            data["mb_release_group_id"],
            "rrrrrrrr-rrrr-rrrr-rrrr-rrrrrrrrrrrr",
        )
        mock_mb.assert_not_called()
        self.mock_db.update_request_fields.assert_not_called()

    def test_resolve_rg_lazy_backfill_happy_path_returns_200(self):
        """Row has no RG → MB lookup → UPDATE row → 200."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": None,
        }
        with patch(
            "web.mb.get_release",
            return_value={"release_group_id": "rrrr-rrrr-rrrr"},
        ) as mock_mb:
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg happy-path response",
        )
        self.assertEqual(data["status"], "resolved")
        self.assertEqual(
            data["mb_release_group_id"], "rrrr-rrrr-rrrr",
        )
        mock_mb.assert_called_once_with(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", fresh=False,
        )
        self.mock_db.update_request_fields.assert_called_once_with(
            42, mb_release_group_id="rrrr-rrrr-rrrr",
        )

    def test_resolve_rg_not_found_returns_404(self):
        self.mock_db.get_request.return_value = None
        with patch("web.mb.get_release") as mock_mb:
            status, data = self._post(
                "/api/pipeline/9999/resolve-rg", {},
            )
        self.assertEqual(status, 404)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg not-found response",
        )
        self.assertEqual(data["status"], "not_found")
        self.assertIsNone(data["mb_release_group_id"])
        mock_mb.assert_not_called()

    def test_resolve_rg_no_release_group_returns_422(self):
        """MB returns a payload but no release_group_id (e.g. mirror
        anomaly, or a release whose RG is missing upstream)."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": None,
        }
        with patch(
            "web.mb.get_release",
            return_value={"release_group_id": None},
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 422)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg 422 response",
        )
        self.assertEqual(data["status"], "no_release_group")
        self.mock_db.update_request_fields.assert_not_called()

    def test_resolve_rg_discogs_release_id_returns_422(self):
        """Numeric Discogs release id → 422 short-circuit, no MB hit."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "12345",
            "mb_release_group_id": None,
        }
        with patch("web.mb.get_release") as mock_mb:
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 422)
        self.assertEqual(data["status"], "non_mb_release_id")
        mock_mb.assert_not_called()
        self.mock_db.update_request_fields.assert_not_called()

    def test_resolve_rg_transient_returns_503(self):
        """Network blip / timeout → 503 retryable."""
        from urllib.error import URLError
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": None,
        }
        with patch(
            "web.mb.get_release",
            side_effect=URLError("connection refused"),
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg 503 response",
        )
        self.assertEqual(data["status"], "transient")
        self.mock_db.update_request_fields.assert_not_called()


class TestPipelineSearchPlanHistoryContract(_WebServerCase):
    """Contract for ``GET /api/pipeline/<id>/search-plan/history``.

    The endpoint wraps ``SearchPlanService.history_for_request``. Both
    the CLI (``pipeline-cli search-plan history``) and the API live or
    die on the same service contract — see ``CLAUDE.md`` § "CLI ⇄ API
    surface symmetry"; touching one without the other is a contract
    drift waiting to happen.

    Status-code mapping:
      * 200 — RESULT_HISTORY_PAGE_SUCCESS
      * 400 — input validation (non-int / out-of-bounds limit/before_id)
      * 404 — RESULT_REQUEST_NOT_FOUND
    """

    HISTORY_REQUIRED_FIELDS = {"request_id", "rows", "next_before_id"}

    def setUp(self) -> None:
        from lib.config import CratediggerConfig
        import configparser
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        self._cfg_patcher = patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp),
        )
        self._cfg_patcher.start()

    def tearDown(self) -> None:
        self._cfg_patcher.stop()

    def _patch_service(self, **result_kwargs):
        from unittest.mock import patch as _patch
        from lib.search_plan_service import SearchLogHistoryPageResult
        return _patch(
            "lib.search_plan_service.SearchPlanService.history_for_request",
            return_value=SearchLogHistoryPageResult(**result_kwargs),
        )

    def test_history_success_returns_200_with_required_fields(self):
        rows = [{
            "id": 12340, "request_id": 100, "query": "q1",
            "outcome": "no_match",
        }]
        with self._patch_service(
                outcome="success", request_id=100, rows=rows,
                next_before_id=12300):
            status, data = self._get(
                "/api/pipeline/100/search-plan/history?limit=50")
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.HISTORY_REQUIRED_FIELDS,
                                "search-plan history response")
        self.assertEqual(data["request_id"], 100)
        self.assertEqual(data["rows"], rows)
        self.assertEqual(data["next_before_id"], 12300)

    def test_history_success_with_null_cursor_when_exhausted(self):
        with self._patch_service(
                outcome="success", request_id=100, rows=[],
                next_before_id=None):
            status, data = self._get(
                "/api/pipeline/100/search-plan/history?limit=50")
        self.assertEqual(status, 200)
        self.assertEqual(data["rows"], [])
        self.assertIsNone(data["next_before_id"])

    def test_history_request_not_found_returns_404(self):
        with self._patch_service(
                outcome="request_not_found", request_id=9999,
                rows=[],
                error_message="request 9999 not found"):
            status, data = self._get(
                "/api/pipeline/9999/search-plan/history?limit=50")
        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_history_default_limit_when_not_supplied(self):
        """No ``limit`` query param uses the default; service receives
        an int — never passed-through query string."""
        from unittest.mock import patch as _patch
        from lib.search_plan_service import SearchLogHistoryPageResult
        with _patch(
            "lib.search_plan_service.SearchPlanService.history_for_request",
            return_value=SearchLogHistoryPageResult(
                outcome="success", request_id=100, rows=[],
                next_before_id=None),
        ) as mock_hist:
            status, _ = self._get(
                "/api/pipeline/100/search-plan/history")
        self.assertEqual(status, 200)
        kwargs = mock_hist.call_args.kwargs
        self.assertIsInstance(kwargs.get("limit"), int)
        self.assertGreaterEqual(kwargs["limit"], 1)
        self.assertLessEqual(kwargs["limit"], 200)
        self.assertIsNone(kwargs.get("before_id"))

    def test_history_passes_before_id_through_to_service(self):
        from unittest.mock import patch as _patch
        from lib.search_plan_service import SearchLogHistoryPageResult
        with _patch(
            "lib.search_plan_service.SearchPlanService.history_for_request",
            return_value=SearchLogHistoryPageResult(
                outcome="success", request_id=100, rows=[],
                next_before_id=None),
        ) as mock_hist:
            status, _ = self._get(
                "/api/pipeline/100/search-plan/history"
                "?limit=50&before_id=12300")
        self.assertEqual(status, 200)
        kwargs = mock_hist.call_args.kwargs
        self.assertEqual(kwargs.get("limit"), 50)
        self.assertEqual(kwargs.get("before_id"), 12300)

    def test_history_rejects_non_int_limit(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.search_plan_service.SearchPlanService.history_for_request",
        ) as mock_hist:
            status, data = self._get(
                "/api/pipeline/100/search-plan/history?limit=abc")
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        mock_hist.assert_not_called()

    def test_history_rejects_non_int_before_id(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.search_plan_service.SearchPlanService.history_for_request",
        ) as mock_hist:
            status, data = self._get(
                "/api/pipeline/100/search-plan/history?limit=50&before_id=abc")
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        mock_hist.assert_not_called()

    def test_history_rejects_out_of_bounds_limit(self):
        """Service-level bounds enforcement bubbles to 400 at the route."""
        with self._patch_service(
                outcome="input_invalid", request_id=100, rows=[],
                error_message="limit must be in [1, 200]"):
            status, data = self._get(
                "/api/pipeline/100/search-plan/history?limit=500")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_history_datetime_rows_are_serialized_to_strings(self):
        """F1: rows with datetime created_at must not 500 — _serialize_row
        must be applied before JSON encoding."""
        from datetime import datetime, timezone
        rows = [{
            "id": 12340,
            "request_id": 100,
            "query": "q1",
            "outcome": "no_match",
            "created_at": datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc),
        }]
        with self._patch_service(
                outcome="success", request_id=100, rows=rows,
                next_before_id=None):
            status, data = self._get(
                "/api/pipeline/100/search-plan/history?limit=50")
        self.assertEqual(status, 200)
        self.assertIsInstance(data["rows"][0]["created_at"], str,
                              "created_at must be a string (ISO format) on the wire")
        # Full round-trip: the response body must be valid JSON.
        import json as _json
        _json.dumps(data)  # raises TypeError if any datetime slipped through

    def test_history_404_body_shape_matches_neighbor_routes(self):
        """F3: 404 body must be {error: ...} only — no rows/next_before_id
        to match the h._error() shape used by get_pipeline_detail etc."""
        with self._patch_service(
                outcome="request_not_found", request_id=9999,
                rows=[],
                error_message="request 9999 not found"):
            status, data = self._get(
                "/api/pipeline/9999/search-plan/history?limit=50")
        self.assertEqual(status, 404)
        self.assertIn("error", data)
        self.assertNotIn("rows", data,
                         "404 body must not include rows key")
        self.assertNotIn("next_before_id", data,
                         "404 body must not include next_before_id key")


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
        # ``mock_db`` is class-scoped; reset call history so U4's
        # assertions on ``update_request_fields.call_args_list`` see only
        # the calls from the current test, not the previous one.
        self.mock_db.reset_mock()
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
        self.mock_db.get_request_by_mb_release_id.return_value = None
        self.mock_db.get_request_by_discogs_release_id.return_value = None
        self.mock_db.add_request.return_value = 501
        self.mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
        # MB add path also calls ``get_release_raw`` (for the resolver's
        # raw payload) alongside the existing ``get_release`` (for slim
        # add_request fields). Class-wide stub so individual tests only
        # need to mock ``get_release``; the resolver receives an empty
        # dict and records ``unresolved_field_missing_upstream`` for
        # catalog/track_artist as before. Tests that care about
        # catalog_number / track_artist / VA Rule 2 resolution mock
        # ``get_release_raw`` themselves.
        _patch_raw = patch(
            "web.routes.pipeline.mb_api.get_release_raw",
            return_value={},
        )
        _patch_raw.start()
        self.addCleanup(_patch_raw.stop)

    @patch("web.routes.pipeline.mb_api.get_release_group_year",
           return_value=2024)
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_contract(self, mock_get_release, _mock_rgy):
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

    @patch("web.routes.pipeline.mb_api.get_release_group_year",
           return_value=2014)
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_runs_plan_generation_after_set_tracks(
        self, mock_get_release, _mock_rgy,
    ):
        """Web add path generates a search plan after `set_tracks()`,
        consistent with the CLI add path. Failures must not break the
        HTTP response."""
        import web.server as srv
        fake_db = FakePipelineDB()
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "artist-1",
            "artist_name": "Tycho",
            "title": "Awake",
            "year": 2014,
            "country": "US",
            "tracks": [
                {"title": "Awake", "track_number": 1, "disc_number": 1},
                {"title": "Montana", "track_number": 2, "disc_number": 1},
                {"title": "L", "track_number": 3, "disc_number": 1},
                {"title": "Apogee", "track_number": 4, "disc_number": 1},
            ],
        }

        with patch.object(srv, "db", fake_db):
            status, data = self._post(
                "/api/pipeline/add", {"mb_release_id": "abc-plan-1"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add response (plan)")
        new_id = data["id"]
        active = fake_db.get_active_search_plan(new_id)
        self.assertIsNotNone(active)
        assert active is not None
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        self.assertEqual(active.plan.generator_id, SEARCH_PLAN_GENERATOR_ID)
        self.assertEqual(active.next_ordinal, 0)

    def test_pipeline_add_exists_contract(self):
        self.mock_db.get_request_by_mb_release_id.return_value = {
            "id": 502,
            "status": "wanted",
        }

        status, data = self._post("/api/pipeline/add", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.EXISTS_REQUIRED_FIELDS,
                                "pipeline add exists response")

    @patch("web.routes.pipeline.mb_api.get_release_group_year")
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_persists_release_group_year_reissue(
        self, mock_get_release, mock_rgy,
    ):
        """U4: reissue MB release → ``release_group_year`` is fetched
        from the mirror via the resolver service and written via
        ``update_request_fields`` after the row is inserted (the resolver
        needs a real request_id for the FK in
        ``album_request_field_resolutions``)."""
        mock_get_release.return_value = {
            "release_group_id": "rg-kid-a",
            "artist_id": "rh-1",
            "artist_name": "Radiohead",
            "title": "Kid A",
            "year": 2008,  # reissue
            "country": "US",
            "tracks": [{"title": "Everything In Its Right Place"}],
        }
        mock_rgy.return_value = 2000  # release-group's first year

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "kid-a-mbid"})

        self.assertEqual(status, 200)
        mock_rgy.assert_called_once_with("rg-kid-a")
        add_kwargs = self.mock_db.add_request.call_args.kwargs
        self.assertEqual(add_kwargs["year"], 2008)
        # add_request no longer carries release_group_year directly; the
        # resolver service writes it via update_request_fields once the
        # FK in album_request_field_resolutions is satisfiable.
        update_calls = self.mock_db.update_request_fields.call_args_list
        rg_year_writes = [
            c for c in update_calls
            if "release_group_year" in c.kwargs
        ]
        self.assertEqual(len(rg_year_writes), 1)
        self.assertEqual(
            rg_year_writes[0].kwargs["release_group_year"], 2000,
        )
        self.assertEqual(
            rg_year_writes[0].kwargs.get("is_va_compilation"), False,
        )

    @patch("web.routes.pipeline.mb_api.get_release_group_year")
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_persists_release_group_year_original(
        self, mock_get_release, mock_rgy,
    ):
        """U4: original release MB release → ``release_group_year``
        equals the per-release year."""
        mock_get_release.return_value = {
            "release_group_id": "rg-self",
            "artist_id": "willow-1",
            "artist_name": "Willow",
            "title": "Willow",
            "year": 2007,
            "country": "AU",
            "tracks": [{"title": "And Finally I Can Breathe"}],
        }
        mock_rgy.return_value = 2007

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "willow-mbid"})

        self.assertEqual(status, 200)
        add_kwargs = self.mock_db.add_request.call_args.kwargs
        self.assertEqual(add_kwargs["year"], 2007)
        update_calls = self.mock_db.update_request_fields.call_args_list
        rg_year_writes = [
            c for c in update_calls
            if "release_group_year" in c.kwargs
        ]
        self.assertEqual(len(rg_year_writes), 1)
        self.assertEqual(
            rg_year_writes[0].kwargs["release_group_year"], 2007,
        )

    @patch("web.routes.pipeline.mb_api.get_release_group_year")
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_release_group_404_leaves_column_null(
        self, mock_get_release, mock_rgy,
    ):
        """U4: 404 from the release-group fetch → ``release_group_year``
        is NULL on the new row, no error raised, request still added.
        The resolver service surfaces 404 / unparseable as
        ``unresolved_field_missing_upstream``; the helper writes
        ``is_va_compilation`` but never writes a NULL
        ``release_group_year`` (only resolved values land on the row)."""
        mock_get_release.return_value = {
            "release_group_id": "rg-missing",
            "artist_id": "a-1",
            "artist_name": "A",
            "title": "T",
            "year": 2020,
            "country": "US",
            "tracks": [{"title": "Track"}],
        }
        mock_rgy.return_value = None  # mirror returned 404 / unparseable

        status, data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-rgmiss"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add response (rg 404)")
        add_kwargs = self.mock_db.add_request.call_args.kwargs
        self.assertEqual(add_kwargs["year"], 2020)
        update_calls = self.mock_db.update_request_fields.call_args_list
        rg_year_writes = [
            c for c in update_calls
            if "release_group_year" in c.kwargs
        ]
        self.assertEqual(rg_year_writes, [],
                         "unresolved rg_year must NOT be written")

    @patch("web.routes.pipeline.mb_api.get_release_group_year")
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_skips_rgy_lookup_when_no_rg_id(
        self, mock_get_release, mock_rgy,
    ):
        """U4: when MB doesn't return a ``release_group_id`` (e.g. very
        old data), the resolver's release-group-year branch sees no
        rg_id and returns ``unresolved_malformed`` without touching the
        mirror; ``release_group_year`` is left NULL on the row."""
        mock_get_release.return_value = {
            # No release_group_id key — get() returns None.
            "artist_id": "a-1",
            "artist_name": "A",
            "title": "T",
            "year": 2020,
            "country": "US",
            "tracks": [{"title": "Track"}],
        }

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-norg"})

        self.assertEqual(status, 200)
        mock_rgy.assert_not_called()
        update_calls = self.mock_db.update_request_fields.call_args_list
        rg_year_writes = [
            c for c in update_calls
            if "release_group_year" in c.kwargs
        ]
        self.assertEqual(rg_year_writes, [],
                         "unresolved rg_year must NOT be written")

    @patch("web.routes.pipeline.mb_api.get_release_raw")
    @patch("web.routes.pipeline.mb_api.get_release_group_year")
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_va_compilation_flag_set(
        self, mock_get_release, mock_rgy, mock_get_raw,
    ):
        """U4 web happy path for VA: a release-group typed as
        Compilation flips ``is_va_compilation=True`` once at enqueue,
        via the resolver service detecting the type on rule 2.

        VA Rule 2 reads ``release-group.primary-type`` from the raw MB
        payload — so the test mocks ``get_release_raw`` (the new
        primary fetcher) with a shape that has the rg nested. The
        slim ``get_release`` mock supplies the fields ``add_request``
        / ``set_tracks`` need.
        """
        mock_get_release.return_value = {
            "release_group_id": "rg-va",
            "artist_id": "a-1",
            "artist_name": "Various Artists",
            "title": "Tarantino Presents",
            "year": 2008,
            "country": "US",
            "tracks": [{"title": "T1"}],
        }
        # Real-VA shape (post-#373): Compilation rg AND per-track
        # artist credits diverge from the album-level credit. The
        # divergence is what flips Rule 2 in `detect_va_compilation`.
        mock_get_raw.return_value = {
            "id": "va-mbid",
            "release-group": {"primary-type": "Compilation"},
            "artist-credit": [{"name": "Various Artists"}],
            "media": [{
                "position": 1,
                "tracks": [
                    {"position": 1, "title": "T1",
                     "artist-credit": [{"name": "Artist A"}]},
                    {"position": 2, "title": "T2",
                     "artist-credit": [{"name": "Artist B"}]},
                ],
            }],
        }
        mock_rgy.return_value = 2008

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "va-mbid"})
        self.assertEqual(status, 200)
        update_calls = self.mock_db.update_request_fields.call_args_list
        va_writes = [
            c for c in update_calls
            if c.kwargs.get("is_va_compilation") is True
        ]
        self.assertGreaterEqual(len(va_writes), 1,
                                "is_va_compilation=True must be written")

    @patch("web.routes.pipeline.mb_api.get_release_raw")
    @patch("web.routes.pipeline.mb_api.get_release_group_year")
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_va_compilation_emits_va_plan(
        self, mock_get_release, mock_rgy, mock_get_raw,
    ):
        """PR2 Apply #2: when the resolver flips ``is_va_compilation``
        on the add path, the SAME add call must produce a VA-shaped
        plan (``va_track_artist_<idx>`` slots from ``_generate_va_plan``)
        — not a normal-shaped plan that would have to wait for the
        next operator regeneration to flip.

        Uses a real ``FakePipelineDB`` because the active plan needs
        to be fetched after the add lands; ``mock_db`` MagicMock can't
        round-trip a plan through ``store_search_plan`` /
        ``get_active_search_plan``.
        """
        import web.server as srv
        from tests.fakes import FakePipelineDB
        fake_db = FakePipelineDB()

        mock_get_release.return_value = {
            "release_group_id": "rg-va",
            "artist_id": "a-1",
            "artist_name": "Various Artists",
            "title": "Tarantino Presents",
            "year": 2008,
            "country": "US",
            "tracks": [
                {"title": "T1", "track_number": 1, "disc_number": 1},
                {"title": "T2", "track_number": 2, "disc_number": 1},
                {"title": "T3", "track_number": 3, "disc_number": 1},
            ],
        }
        mock_get_raw.return_value = {
            "id": "va-plan-mbid",
            "release-group": {"primary-type": "Compilation"},
            "artist-credit": [{"name": "Various Artists"}],
            "media": [{
                "position": 1,
                "tracks": [
                    {"position": 1, "title": "T1",
                     "artist-credit": [{"name": "Artist A"}]},
                    {"position": 2, "title": "T2",
                     "artist-credit": [{"name": "Artist B"}]},
                    {"position": 3, "title": "T3",
                     "artist-credit": [{"name": "Artist C"}]},
                ],
            }],
        }
        mock_rgy.return_value = 2008

        with patch.object(srv, "db", fake_db):
            status, data = self._post(
                "/api/pipeline/add", {"mb_release_id": "va-plan-mbid"})
        self.assertEqual(status, 200)

        new_id = data["id"]
        # VA flag landed.
        row = fake_db.get_request(new_id)
        assert row is not None
        self.assertTrue(row["is_va_compilation"])

        # And the plan respects it — at least one va_track_artist_*
        # slot from ``_generate_va_plan``. Pre-fix, the add path
        # silently passed ``is_va_compilation=False`` into the
        # generator and the plan was the normal-shape (default /
        # literal / literal_flac).
        active = fake_db.get_active_search_plan(new_id)
        assert active is not None
        strategies = [item.strategy for item in active.items]
        self.assertTrue(
            any(s.startswith("va_track_artist_") for s in strategies),
            f"VA add path must emit va_track_artist_* slot; got "
            f"{strategies}",
        )

    @patch("web.routes.pipeline.mb_api.get_release_raw")
    @patch("web.routes.pipeline.mb_api.get_release_group_year",
           return_value=2010)
    @patch("web.routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_resolves_catalog_number_from_raw_payload(
        self, mock_get_release, _mock_rgy, mock_get_raw,
    ):
        """Fix #2 regression guard: when the raw MB payload carries
        ``label-info``, the resolver service extracts the catno and
        the helper writes it to ``album_requests.catalog_number``.

        Pre-fix, ``post_pipeline_add`` passed the slim ``get_release``
        shape to the resolver — which doesn't include ``label-info`` —
        and the catno landed as ``unresolved_field_missing_upstream``
        every single time. Post-fix the inline path also fetches
        ``get_release_raw`` and passes that as ``mb_release_payload``,
        so the catno reaches the resolver.
        """
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "a-1",
            "artist_name": "Artist",
            "title": "Album",
            "year": 2010,
            "country": "GB",
            "tracks": [{"title": "T1"}],
        }
        # Raw MB JSON shape with label-info present.
        mock_get_raw.return_value = {
            "id": "abc-mbid",
            "label-info": [{"catalog-number": "STRMRT-001"}],
        }

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-mbid"})
        self.assertEqual(status, 200)

        catno_writes = [
            c for c in self.mock_db.update_request_fields.call_args_list
            if c.kwargs.get("catalog_number") == "STRMRT-001"
        ]
        self.assertGreaterEqual(
            len(catno_writes), 1,
            "resolver-extracted catalog_number must be persisted",
        )

    def test_pipeline_add_mb_integration_persists_release_group_year(self):
        """U4 integration: full add-from-web flow against ``FakePipelineDB``
        creates the new row with ``release_group_year`` populated and
        the request reads back correctly."""
        import web.server as srv
        fake_db = FakePipelineDB()
        with patch("web.routes.pipeline.mb_api.get_release") as mock_rel, \
             patch("web.routes.pipeline.mb_api.get_release_group_year",
                   return_value=2000) as mock_rgy, \
             patch.object(srv, "db", fake_db):
            mock_rel.return_value = {
                "release_group_id": "rg-kid-a",
                "artist_id": "rh-1",
                "artist_name": "Radiohead",
                "title": "Kid A",
                "year": 2008,
                "country": "US",
                "tracks": [
                    {"title": "Everything In Its Right Place",
                     "track_number": 1, "disc_number": 1},
                ],
            }
            status, data = self._post(
                "/api/pipeline/add", {"mb_release_id": "kid-a-int"})

        self.assertEqual(status, 200)
        new_id = data["id"]
        row = fake_db.get_request(new_id)
        assert row is not None
        self.assertEqual(row["year"], 2008)
        self.assertEqual(row["release_group_year"], 2000)
        mock_rgy.assert_called_once_with("rg-kid-a")

    def test_pipeline_add_duplicate_does_not_regenerate(self):
        """Duplicate add returns the existing request without generating
        a second plan."""
        import web.server as srv
        fake_db = FakePipelineDB()
        # Pre-seed an existing request matching the release id.
        fake_db.add_request(
            mb_release_id="abc-dupe",
            artist_name="Dupe", album_title="Existing", source="request",
        )
        before = len(fake_db.search_plans)

        with patch.object(srv, "db", fake_db):
            status, data = self._post(
                "/api/pipeline/add", {"mb_release_id": "abc-dupe"})

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "exists")
        self.assertEqual(len(fake_db.search_plans), before)

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

    @patch("web.routes.pipeline.finalize_request")
    def test_pipeline_update_contract(self, _mock_transition):
        status, data = self._post("/api/pipeline/update", {"id": 100, "status": "manual"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPDATE_REQUIRED_FIELDS,
                                "pipeline update response")

    @patch("web.routes.pipeline.finalize_request")
    def test_pipeline_upgrade_contract(self, _mock_transition):
        self.mock_db.get_request_by_mb_release_id.return_value = _MOCK_PIPELINE_REQUEST

        status, data = self._post("/api/pipeline/upgrade", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response")

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
    def test_pipeline_set_quality_contract(self, _mock_transition):
        self.mock_db.get_request_by_mb_release_id.return_value = _MOCK_PIPELINE_REQUEST

        status, data = self._post(
            "/api/pipeline/set-quality",
            {"mb_release_id": "abc-123", "status": "manual", "min_bitrate": 245},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_QUALITY_REQUIRED_FIELDS,
                                "pipeline set-quality response")

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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
        # Default: no descendant — delete succeeds.
        self.mock_db.get_request_by_replaces_request_id.return_value = None
        status, data = self._post("/api/pipeline/delete", {"id": 100})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DELETE_REQUIRED_FIELDS,
                                "pipeline delete response")

    def test_pipeline_delete_with_descendant_returns_409(self):
        """Deleting a request that has a superseding descendant is
        blocked by the ON DELETE RESTRICT FK on
        ``album_requests.replaces_request_id`` (migration 023). The
        route walks the descendant chain and returns 409 with the
        list of descendant IDs so the operator can prune the lineage
        leaf-first."""
        # Reset shared-class mock state from prior tests.
        self.mock_db.delete_request.reset_mock()
        self.mock_db.delete_request.side_effect = None
        # Chain: 100 → 200 → 300.
        def descendant_of(req_id):
            if req_id == 100:
                return {"id": 200, "status": "wanted",
                        "replaces_request_id": 100}
            if req_id == 200:
                return {"id": 300, "status": "imported",
                        "replaces_request_id": 200}
            return None
        self.mock_db.get_request_by_replaces_request_id.side_effect = (
            descendant_of
        )
        status, data = self._post("/api/pipeline/delete", {"id": 100})

        self.assertEqual(status, 409)
        self.assertIn("error", data)
        self.assertIn("descendant_request_ids", data)
        self.assertEqual(data["descendant_request_ids"], [200, 300])
        # delete_request must NOT have been called on the route's
        # happy path when the descendant block fires.
        self.mock_db.delete_request.assert_not_called()
        # Clear side_effect for downstream tests.
        self.mock_db.get_request_by_replaces_request_id.side_effect = None
        self.mock_db.get_request_by_replaces_request_id.return_value = None

    def test_pipeline_delete_fk_violation_returns_409(self):
        """Defensive race-window guard: a descendant landed between the
        route's read and the delete. The FK violation surfaces as 409
        rather than a 500, mirroring the pre-check shape."""
        import psycopg2.errors
        self.mock_db.get_request_by_replaces_request_id.side_effect = [
            # First call (pre-check) sees no descendant.
            None,
            # Second call (post-FK error walk) sees the descendant.
            {"id": 250, "status": "wanted", "replaces_request_id": 100},
            # Third call (chain walk) sees no further descendants.
            None,
        ]
        self.mock_db.delete_request.side_effect = (
            psycopg2.errors.ForeignKeyViolation("descendant landed")
        )
        status, data = self._post("/api/pipeline/delete", {"id": 100})
        self.assertEqual(status, 409)
        self.assertIn("error", data)
        self.assertEqual(data["descendant_request_ids"], [250])
        # Reset side_effects for downstream tests.
        self.mock_db.delete_request.side_effect = None

    # -- fresh=True seam (Codex review on issue #101) ----------------

    @patch("routes.pipeline.mb_api.get_release_group_year",
           return_value=2024)
    @patch("routes.pipeline.mb_api.get_release")
    def test_pipeline_add_mb_fetches_release_fresh(
        self, mock_get_release, _mock_rgy,
    ):
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
        # ``get_release`` is now called multiple times — once by the
        # add handler and again by the U4 resolver service's release_group_id /
        # track_artist / catalog_number resolvers. Every call MUST go
        # through ``fresh=True`` so the pipeline DB never persists a
        # stale cache snapshot.
        self.assertGreaterEqual(mock_get_release.call_count, 1)
        for call in mock_get_release.call_args_list:
            self.assertEqual(call.args, ("abc-123",))
            self.assertEqual(call.kwargs, {"fresh": True})

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
        # Same as the MB branch: post-U4 the resolver service also goes
        # through ``get_release(fresh=True)``. Every call must bypass
        # the cache.
        self.assertGreaterEqual(mock_get_release.call_count, 1)
        for call in mock_get_release.call_args_list:
            self.assertEqual(call.args, (83182,))
            self.assertEqual(call.kwargs, {"fresh": True})

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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
    @patch("web.routes.pipeline.finalize_request")
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

    @patch("web.routes.imports.scan_complete_folder")
    def test_manual_import_scan_contract(self, mock_scan):
        # Drive the real ``match_folders_to_requests`` against folder +
        # request inputs that share an artist/album token set so the
        # fuzzy matcher produces a high-confidence FolderMatch. Patching
        # the matcher away would hide its contract — score field,
        # match-or-skip threshold, sort order — from this test.
        folder = FolderInfo(
            name="Test Artist - Test Album",
            path="/complete/Test Artist - Test Album",
            artist="Test Artist",
            album="Test Album",
            file_count=10,
        )
        mock_scan.return_value = [folder]
        self.mock_db.get_by_status.return_value = [
            make_request_row(
                id=100,
                status="wanted",
                mb_release_id="abc-123",
                artist_name="Test Artist",
                album_title="Test Album",
            ),
        ]

        status, data = self._get("/api/manual-import/scan")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"folders", "wanted_count"},
                                "manual import scan response")
        _assert_required_fields(self, data["folders"][0], self.FOLDER_REQUIRED_FIELDS,
                                "manual import folder")
        _assert_required_fields(self, data["folders"][0]["match"], self.MATCH_REQUIRED_FIELDS,
                                "manual import match")
        # Real matcher should report a perfect score for identical
        # artist/album tokens — this is the actual production contract.
        self.assertEqual(data["folders"][0]["match"]["request_id"], 100)
        self.assertGreaterEqual(data["folders"][0]["match"]["score"], 0.99)

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
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
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
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
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
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RG_ID}&kind=release")

        self.assertEqual(status, 404)
        # release-group endpoint MUST NOT have been called
        self.assertEqual(mock_mb.get_release_group.call_count, 0)

    def test_not_found_both_endpoints(self):
        with patch("web.server.mb_api") as mock_mb:
            from urllib.error import HTTPError
            mock_mb.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
            mock_mb.get_release_group.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
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
        # Wrap a real FakePipelineDB so unmocked methods fall through to
        # typed state — same rationale as ``_pipeline_db_test_harness``.
        failing_db = _pipeline_db_test_harness()
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
        self.mock_db.get_wrong_matches.side_effect = None
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
        self.mock_db.list_active_import_jobs_for_wrong_match.return_value = []
        # Default: treat every failed_path as existing so the group survives
        # filtering. Individual tests override this to exercise missing-file
        # and mixed-existence cases. Converge deletion is service-backed.
        # Regression-guard sentinel: import cleanup_wrong_match into the route
        # module's namespace so the converge tests can assert it is NOT called.
        # See project_converge_operator_authority memory + post_wrong_match_converge
        # docstring — converge must route deletion through delete_wrong_match,
        # never through cleanup_wrong_match.
        from lib.wrong_match_cleanup_service import cleanup_wrong_match as _cwm_sentinel
        import web.routes.imports as _imports_mod
        _imports_mod.cleanup_wrong_match = _cwm_sentinel  # pyright: ignore[reportAttributeAccessIssue]
        cleanup_patch = patch(
            "web.routes.imports.cleanup_wrong_match",
            side_effect=lambda _db, lid: self._cleanup_result(lid),
        )
        manual_cleanup_patch = patch(
            "web.routes.imports.delete_wrong_match",
            side_effect=lambda _db, lid, **_kwargs: self._manual_cleanup_result(lid),
        )
        manual_group_cleanup_patch = patch(
            "web.routes.imports.delete_wrong_match_group",
            side_effect=lambda _db, rid: self._manual_group_cleanup_result(rid),
        )
        resolve_patch = patch("web.routes.imports.resolve_failed_path",
                              side_effect=lambda p: p if p else None)
        self.mock_cleanup = cleanup_patch.start()
        self.mock_manual_cleanup = manual_cleanup_patch.start()
        self.mock_manual_group_cleanup = manual_group_cleanup_patch.start()
        self.mock_resolve_failed_path = resolve_patch.start()
        self.addCleanup(cleanup_patch.stop)
        self.addCleanup(manual_cleanup_patch.stop)
        self.addCleanup(manual_group_cleanup_patch.stop)
        self.addCleanup(resolve_patch.stop)
        self.addCleanup(lambda: delattr(_imports_mod, "cleanup_wrong_match"))

    GROUP_REQUIRED_FIELDS = {
        "request_id", "artist", "album", "mb_release_id",
        # Release-group id surfaces so the frontend can render the
        # Replace button (R7) — it asks "what RG is this row in?".
        "mb_release_group_id",
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
        "distance", "scenario", "detail", "source_dirs", "candidate", "local_items",
        # Per-candidate stored evidence (R1+R2 of the spectral-evidence
        # plan) — surfaced from download_log so the operator can eyeball
        # candidates by audio quality. Always present in the payload;
        # values are None when the underlying row lacks evidence.
        "spectral_grade", "spectral_bitrate",
        "v0_probe_kind", "v0_probe_avg_bitrate",
        # Storage format + min bitrate + computed quality rank — read
        # from album_quality_evidence via download_log.candidate_evidence_id
        # so wrong-match rows show their actual codec/rank instead of
        # dashes from the legacy denorm columns. Drives entry sort order.
        "format", "min_bitrate", "verified_lossless", "quality_rank",
    }
    DELETE_RESULT_REQUIRED_FIELDS = {
        "status", "download_log_id", "outcome", "success", "request_id",
        "entry_found", "visible", "raw_failed_path", "failed_path_hint",
        "resolved_path", "deleted_path", "path_missing", "cleared_rows",
        "skipped", "reason", "error",
    }
    DELETE_GROUP_REQUIRED_FIELDS = {
        "status", "request_id", "outcome", "success", "processed", "deleted",
        "deleted_paths", "cleared", "skipped", "errors", "remaining",
        "group_empty", "results",
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
        "source_dirs": list,
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

    def _cleanup_result(self, log_id: int, *, outcome: str = "deleted"):
        from lib.wrong_match_cleanup_service import (
            OUTCOME_DELETED,
            WrongMatchCleanupOutcome,
        )

        return WrongMatchCleanupOutcome(
            download_log_id=log_id,
            outcome=outcome,
            success=outcome == OUTCOME_DELETED,
            verdict="confident_reject" if outcome == OUTCOME_DELETED else "uncertain",
            cleanup_eligible=outcome == OUTCOME_DELETED,
            cleared_rows=1 if outcome == OUTCOME_DELETED else 0,
        )

    def _manual_cleanup_result(self, log_id: int):
        from lib.wrong_match_delete_service import (
            OUTCOME_DELETED,
            WrongMatchDeleteResult,
        )

        return WrongMatchDeleteResult(
            download_log_id=log_id,
            outcome=OUTCOME_DELETED,
            success=True,
            entry_found=True,
            visible=True,
            request_id=42,
            raw_failed_path="/mnt/virtio/music/slskd/failed_imports/Test",
            resolved_path="/mnt/virtio/music/slskd/failed_imports/Test",
            deleted_path="/mnt/virtio/music/slskd/failed_imports/Test",
            cleared_rows=1,
        )

    def _manual_group_cleanup_result(self, request_id: int):
        from lib.wrong_match_delete_service import WrongMatchDeleteSummary

        results = (
            self._manual_cleanup_result(100),
            self._manual_cleanup_result(101),
        )
        return WrongMatchDeleteSummary(
            request_id=request_id,
            outcome="deleted",
            success=True,
            processed=2,
            deleted=2,
            deleted_paths=2,
            cleared=2,
            skipped=0,
            errors=0,
            remaining=0,
            group_empty=True,
            results=results,
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

    def test_entry_surfaces_preserved_source_dirs(self):
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["validation_result"]["source_dirs"] = [
            "baduser\\Artist\\Album",
            "baduser\\Artist\\Album\\CD2",
        ]
        self.mock_db.get_wrong_matches.return_value = [row]

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(
            entry["source_dirs"],
            ["baduser\\Artist\\Album", "baduser\\Artist\\Album\\CD2"],
        )

    def test_wrong_match_explorer_lists_audio_files_and_source_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"fake mp3 bytes")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                    "source_dirs": ["baduser\\Artist\\Album"],
                },
            }

            status, data = self._get("/api/wrong-matches/explorer?download_log_id=42")

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["source_dirs"], ["baduser\\Artist\\Album"])
        self.assertEqual(data["audio_file_count"], 1)
        self.assertEqual(data["files"][0]["relative_path"], "01 - Track.mp3")
        self.assertTrue(data["files"][0]["playable"])
        self.assertIn("/api/wrong-matches/audio?download_log_id=42", data["files"][0]["stream_url"])

    def test_wrong_match_explorer_normalizes_raw_id3_tags_and_skips_artwork_frames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"fake mp3 bytes")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                },
            }

            class _FakeInfo:
                length = 181.0
                bitrate = 320000

            class _FakeAudio:
                tags = {
                    "APIC:": ["embedded cover art"],
                    "TALB": ["Shut Up And Listen To Majosha"],
                    "TCON": ["Funk Rock"],
                    "TDRC": ["1989"],
                    "TPE1": ["Majosha"],
                    "TPE2": ["Majosha"],
                    "TPOS": ["2"],
                    "TXXX:MusicBrainz Album Id": ["20f1e791-34cd-4b47-8783-51492b90218a"],
                }
                info = _FakeInfo()

            with patch("mutagen.File", return_value=_FakeAudio()):
                status, data = self._get("/api/wrong-matches/explorer?download_log_id=42")

        self.assertEqual(status, 200)
        tags = data["files"][0]["tags"]
        self.assertNotIn("apic:", tags)
        self.assertEqual(tags["album"], ["Shut Up And Listen To Majosha"])
        self.assertEqual(tags["genre"], ["Funk Rock"])
        self.assertEqual(tags["date"], ["1989"])
        self.assertEqual(tags["artist"], ["Majosha"])
        self.assertEqual(tags["albumartist"], ["Majosha"])
        self.assertEqual(tags["discnumber"], ["2"])
        self.assertEqual(
            tags["musicbrainz_albumid"],
            ["20f1e791-34cd-4b47-8783-51492b90218a"],
        )

    def test_wrong_match_explorer_returns_files_in_beets_matched_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for filename in ("a.mp3", "b.mp3", "c.mp3"):
                with open(os.path.join(tmpdir, filename), "wb") as handle:
                    handle.write(b"fake mp3 bytes")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                    "candidates": [{
                        "is_target": True,
                        "mapping": [
                            {
                                "item": {"path": "c.mp3", "title": "Third", "track": 12, "disc": 1},
                                "track": {"medium_index": 1, "medium": 1, "title": "Target One"},
                            },
                            {
                                "item": {"path": "a.mp3", "title": "First", "track": 1, "disc": 1},
                                "track": {"medium_index": 2, "medium": 1, "title": "Target Two"},
                            },
                            {
                                "item": {"path": "b.mp3", "title": "Second", "track": 7, "disc": 1},
                                "track": {"medium_index": 3, "medium": 1, "title": "Target Three"},
                            },
                        ],
                    }],
                },
            }

            class _FakeInfo:
                length = 181.0
                bitrate = 320000

            def _fake_audio(path: str):
                basename = os.path.basename(path)

                class _FakeAudio:
                    info = _FakeInfo()
                    if basename == "a.mp3":
                        tags = {"title": ["First"], "tracknumber": ["1/14"], "discnumber": ["1/1"]}
                    elif basename == "b.mp3":
                        tags = {"title": ["Second"], "tracknumber": ["7/14"], "discnumber": ["1/1"]}
                    else:
                        tags = {"title": ["Third"], "tracknumber": ["12/14"], "discnumber": ["1/1"]}

                return _FakeAudio()

            with patch("mutagen.File", side_effect=_fake_audio):
                status, data = self._get("/api/wrong-matches/explorer?download_log_id=42")

        self.assertEqual(status, 200)
        self.assertEqual(data["ordered_by"], "matched")
        self.assertEqual(
            [file["relative_path"] for file in data["files"]],
            ["c.mp3", "a.mp3", "b.mp3"],
        )
        self.assertEqual(
            [file["matched_order"] for file in data["files"]],
            [1, 2, 3],
        )

    def test_wrong_match_audio_supports_byte_ranges(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"abcdef")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                },
            }

            req = Request(
                f"{self.base}/api/wrong-matches/audio?download_log_id=42&path=01%20-%20Track.mp3",
                headers={"Range": "bytes=1-3"},
            )
            with urlopen(req) as resp:
                body = resp.read()
                status = resp.status
                content_range = resp.headers["Content-Range"]
                accept_ranges = resp.headers["Accept-Ranges"]

        self.assertEqual(status, 206)
        self.assertEqual(body, b"bcd")
        self.assertEqual(content_range, "bytes 1-3/6")
        self.assertEqual(accept_ranges, "bytes")

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
        self.assertEqual(entry["source_dirs"], [])
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

    def test_entry_surfaces_evidence_derived_quality(self):
        """Per-candidate format/bitrate/rank come from album_quality_evidence.

        get_wrong_matches() LEFT JOINs the evidence row addressed by
        download_log.candidate_evidence_id; the route layer surfaces
        storage_format → entry.format, min_bitrate_kbps → entry.min_bitrate,
        verified_lossless → entry.verified_lossless, and computes
        quality_rank from format + bitrate via compute_library_rank.
        """
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["evidence_storage_format"] = "FLAC"
        row["evidence_min_bitrate"] = 0
        row["evidence_verified_lossless"] = True
        self.mock_db.get_wrong_matches.return_value = [row]

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["format"], "FLAC")
        self.assertEqual(entry["min_bitrate"], 0)
        self.assertTrue(entry["verified_lossless"])
        self.assertEqual(entry["quality_rank"], "lossless")

    def test_entries_sort_best_quality_first(self):
        """Entries within a group sort lossless → transparent → ... → unknown.

        Mixed-quality reject queue: FLAC, MP3 320, MP3 192, opus 128, and
        an evidence-less row. The frontend operator wants the best
        candidate at the top so they can force-import without scrolling.
        """
        def _row(log_id: int, fmt: str | None, kbps: int | None) -> dict:
            r = self._row(log_id, 770, f"user{log_id}", f"/fi/p{log_id}",
                          artist="A", album="B", mb_release_id="mb-x",
                          distance=0.20)
            r["evidence_storage_format"] = fmt
            r["evidence_min_bitrate"] = kbps
            r["evidence_verified_lossless"] = fmt == "FLAC"
            return r

        self.mock_db.get_wrong_matches.return_value = [
            _row(901, None,   None),   # unknown
            _row(902, "opus", 128),    # transparent
            _row(903, "MP3",  320),    # transparent
            _row(904, "FLAC", 0),      # lossless
            _row(905, "MP3",  192),    # good
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        entries = data["groups"][0]["entries"]
        ranks = [e["quality_rank"] for e in entries]
        ids = [e["download_log_id"] for e in entries]
        # Lossless first, then transparent (two tied — broken by id desc),
        # then good, then unknown last.
        self.assertEqual(
            ranks,
            ["lossless", "transparent", "transparent", "good", "unknown"],
            f"unexpected rank order {ranks} (ids={ids})")
        self.assertEqual(entries[0]["download_log_id"], 904)
        self.assertEqual(entries[-1]["download_log_id"], 901)

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

    def test_manual_delete_route_deletes_single_wrong_match(self):
        status, data = self._post(
            "/api/wrong-matches/delete",
            {"download_log_id": 42},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(
            self,
            data,
            self.DELETE_RESULT_REQUIRED_FIELDS,
            "wrong-match delete response",
        )
        self.assertEqual(data["status"], "ok")
        self.assertTrue(data["success"])
        self.assertEqual(data["deleted_path"],
                         "/mnt/virtio/music/slskd/failed_imports/Test")
        self.mock_manual_cleanup.assert_called_once_with(
            self.mock_db,
            42,
            require_visible=True,
        )

    def test_manual_delete_route_blocks_active_import_job(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            WrongMatchDeleteResult,
        )

        self.mock_manual_cleanup.side_effect = None
        self.mock_manual_cleanup.return_value = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_SKIPPED_ACTIVE_JOB,
            skipped=True,
            reason="active_import_job",
        )

        status, data = self._post(
            "/api/wrong-matches/delete",
            {"download_log_id": 42},
        )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "active_import_job")
        self.mock_manual_cleanup.assert_called_once_with(
            self.mock_db,
            42,
            require_visible=True,
        )

    def test_manual_delete_route_reports_lock_contention_as_retryable(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_LOCKED,
            WrongMatchDeleteResult,
        )

        self.mock_manual_cleanup.side_effect = None
        self.mock_manual_cleanup.return_value = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_SKIPPED_LOCKED,
            skipped=True,
            reason="cleanup_lock_unavailable",
        )

        status, data = self._post(
            "/api/wrong-matches/delete",
            {"download_log_id": 42},
        )

        self.assertEqual(status, 503)
        self.assertEqual(data["error"], "cleanup_lock_unavailable")

    def test_manual_delete_group_deletes_request_rows(self):
        status, data = self._post(
            "/api/wrong-matches/delete-group",
            {"request_id": 42},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(
            self,
            data,
            self.DELETE_GROUP_REQUIRED_FIELDS,
            "wrong-match delete-group response",
        )
        self.assertEqual(data["status"], "ok")
        self.assertTrue(data["success"])
        self.assertEqual(data["processed"], 2)
        self.assertEqual(data["deleted"], 2)
        self.assertEqual(data["cleared"], 2)
        self.assertEqual(data["deleted_paths"], 2)
        self.assertTrue(data["group_empty"])
        self.mock_manual_group_cleanup.assert_called_once_with(self.mock_db, 42)

    def test_manual_delete_group_reports_partial_when_rows_are_skipped(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            WrongMatchDeleteResult,
            WrongMatchDeleteSummary,
        )

        skipped = WrongMatchDeleteResult(
            download_log_id=100,
            outcome=OUTCOME_SKIPPED_ACTIVE_JOB,
            success=False,
            request_id=42,
            skipped=True,
            reason="active_import_job",
        )
        self.mock_manual_group_cleanup.side_effect = None
        self.mock_manual_group_cleanup.return_value = WrongMatchDeleteSummary(
            request_id=42,
            outcome="partial",
            success=False,
            processed=1,
            deleted=0,
            deleted_paths=0,
            cleared=0,
            skipped=1,
            errors=0,
            remaining=1,
            group_empty=False,
            results=(skipped,),
        )

        status, data = self._post(
            "/api/wrong-matches/delete-group",
            {"request_id": 42},
        )

        self.assertEqual(status, 409)
        self.assertEqual(data["status"], "partial")
        self.assertFalse(data["success"])
        self.assertEqual(data["skipped"], 1)
        self.assertEqual(data["remaining"], 1)

    def test_retired_heuristic_delete_routes_are_removed(self):
        for path in (
            "/api/wrong-matches/delete-transparent-non-flac",
            "/api/wrong-matches/delete-lossless-opus",
        ):
            with self.subTest(path=path):
                status, _data = self._post(path, {})
                self.assertEqual(status, 404)

    def test_bulk_triage_requires_full_queue_confirmation(self):
        status, data = self._post("/api/wrong-matches/triage", {})

        self.assertEqual(status, 400)
        self.assertIn("confirm_all_wrong_matches", data.get("message") or data.get("error") or "")

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_bulk_triage_runs_full_wrong_matches_queue(self, mock_cleanup):
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        mock_cleanup.return_value = WrongMatchCleanupSummary(
            processed=3,
            deleted=2,
            kept_would_import=1,
        )

        status, data = self._post(
            "/api/wrong-matches/triage",
            {"confirm_all_wrong_matches": True},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["processed"], 3)
        self.assertEqual(data["deleted"], 2)
        mock_cleanup.assert_called_once_with(
            self.mock_db,
            confirm_all_wrong_matches=True,
        )

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
        self.mock_db.get_wrong_matches.return_value[0]["validation_result"]["source_dirs"] = [
            "u1\\Artist\\Album",
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
        def manual_delete_after_enqueue(_db, log_id, **_kwargs):
            self.assertEqual(self.mock_db.enqueue_import_job.call_count, 2)
            return self._manual_cleanup_result(log_id)

        self.mock_manual_cleanup.side_effect = manual_delete_after_enqueue

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
        self.assertEqual(data["dismissed"], 0)
        self.assertEqual(data["remaining"], 2)
        self.assertFalse(data["group_empty"])
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
        self.mock_db.clear_wrong_match_paths.assert_not_called()
        self.assertEqual(
            self.mock_db.enqueue_import_job.call_args_list[0].kwargs["payload"]["source_dirs"],
            ["u1\\Artist\\Album"],
        )
        self.mock_manual_cleanup.assert_called_once_with(self.mock_db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_deletes_unmatched_when_legacy_client_requests_it(self):
        """Legacy true payloads still delete non-green rows while selected rows stay visible."""
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
        self.assertEqual(data["remaining"], 2)
        self.assertFalse(data["group_empty"])
        self.mock_manual_cleanup.assert_called_once_with(self.mock_db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_deletes_unmatched_unconditionally_without_classifier(self):
        """Operator-authority contract: converge does NOT route deletion through cleanup_wrong_match.

        Regression guard for the issue where unmatched rows with kept_would_import
        or stale-evidence verdicts would silently stay visible because cleanup's
        evidence-based classifier blocked deletion. Converge has already collected
        operator intent; the unmatched row dies.
        """
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
            self._row(102, 42, "u3", "/fi/c", distance=0.226),
        ]
        entries = {
            100: self._entry(100, 42, "/fi/a"),
            102: self._entry(102, 42, "/fi/c"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )
        self.mock_db.enqueue_import_job.return_value = self._job(900, 42, 100, "/fi/a")

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
            "delete_unmatched": True,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["remaining"], 1)
        self.mock_manual_cleanup.assert_called_once_with(self.mock_db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

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
        self.mock_cleanup.assert_not_called()

    def test_converge_reports_deduped_jobs(self):
        """Existing active force-import jobs still count as selected but remain visible."""
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
        self.assertEqual(data["dismissed"], 0)
        self.assertEqual(data["remaining"], 1)

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
        "/api/pipeline/dashboard",
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


class TestClientDisconnectHandling(unittest.TestCase):
    """Issue #233 wedge regression: client closes mid-response.

    Before this fix, a BrokenPipeError raised inside do_GET/do_POST
    (typically from _json's wfile.write to a closed socket) reached
    the bare ``except Exception`` catch-all, which unconditionally
    called _try_reconnect_db() and tried to write a 500 body back to
    the dead socket — causing a second BrokenPipeError, a 30-line
    chained traceback in journald, and a costly DB reconnect.
    Sustained client-disconnect traffic (cratedigger.py's end-of-cycle
    POST that closed mid-body, see U1) compounded reconnects until the
    single-threaded server wedged in poll().

    U2's fix: a typed ``except (BrokenPipeError, ConnectionResetError,
    ConnectionAbortedError)`` clause sits before the existing catch-all
    in both do_GET and do_POST. Disconnect errors get a single WARNING
    log line and a clean return — no DB reconnect, no second body-write.

    These tests inject the exception via ``mock_db.<method>.side_effect``
    rather than via raw-socket mid-body close. The mechanism is the same
    code path (typed except clause inside do_POST's try block); the
    side_effect approach is deterministic where raw-socket timing is
    flaky on localhost. R3 regression-guard tests confirm the existing
    catch-all still fires for real handler errors.
    """

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port, cls.mock_db = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        # Ensure each test starts with a clean side_effect.
        self.mock_db.update_request_fields.side_effect = None
        self.mock_db.update_request_fields.return_value = None

    def _post(self, path, body):
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            resp = urlopen(req, timeout=2)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())
        except Exception:
            # Connection-level failures (which is exactly what the wedge
            # produces server-side) surface client-side as URLError or
            # similar. Tests assert on server-side observable state, not
            # on the client response.
            return None, None

    def _assert_no_reconnect_no_traceback(self, mock_reconnect, log_records, kind):
        """Shared assertions for the disconnect-class tests."""
        # The narrowed clause caught the disconnect — no reconnect attempted.
        self.assertEqual(
            mock_reconnect.call_count, 0,
            f"{kind} must not trigger _try_reconnect_db",
        )
        # A single WARNING-level record about the disconnect.
        warnings = [r for r in log_records if r.levelname == "WARNING"]
        self.assertGreaterEqual(
            len(warnings), 1,
            f"Expected at least one WARNING log line for {kind}, "
            f"got records: {[(r.levelname, r.getMessage()) for r in log_records]}",
        )
        self.assertTrue(
            any("Client disconnect" in r.getMessage() for r in warnings),
            f"Expected 'Client disconnect' in WARNING for {kind}, got: "
            f"{[r.getMessage() for r in warnings]}",
        )
        # No log.exception (ERROR with exc_info) — that's the 30-line
        # traceback pattern the wedge produced.
        errors_with_exc = [
            r for r in log_records
            if r.levelname == "ERROR" and r.exc_info is not None
        ]
        self.assertEqual(
            len(errors_with_exc), 0,
            f"Disconnect ({kind}) must not emit a traceback "
            f"(would be the journald-flood signature). Found: "
            f"{[r.getMessage() for r in errors_with_exc]}",
        )

    @patch("web.server._try_reconnect_db")
    def test_brokenpipe_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Wedge regression: BrokenPipeError reaches the typed except
        clause first, never the catch-all that reconnects."""
        self.mock_db.update_request_fields.side_effect = BrokenPipeError(32, "Broken pipe")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "BrokenPipeError")

    @patch("web.server._try_reconnect_db")
    def test_connection_reset_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Sibling disconnect class — same handling expected."""
        self.mock_db.update_request_fields.side_effect = ConnectionResetError(104, "Connection reset by peer")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "ConnectionResetError")

    @patch("web.server._try_reconnect_db")
    def test_connection_aborted_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Sibling disconnect class — same handling expected."""
        self.mock_db.update_request_fields.side_effect = ConnectionAbortedError(103, "Software caused connection abort")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "ConnectionAbortedError")

    @patch("web.server._try_reconnect_db")
    def test_real_db_error_still_triggers_reconnect(self, mock_reconnect):
        """R3 regression guard: psycopg2.OperationalError must still hit
        the catch-all and trigger _try_reconnect_db. The narrowing must
        not change behaviour for real DB errors."""
        import psycopg2
        self.mock_db.update_request_fields.side_effect = psycopg2.OperationalError("simulated PG outage")
        with self.assertLogs("cratedigger-web", level="ERROR") as cm:
            status, _ = self._post("/api/pipeline/set-intent",
                                   {"id": 100, "intent": "default"})
        # Real DB error → catch-all fires → reconnect attempted.
        self.assertEqual(
            mock_reconnect.call_count, 1,
            "Real psycopg2.OperationalError must still trigger _try_reconnect_db",
        )
        # log.exception emits ERROR with exc_info — that's the existing behaviour.
        errors_with_exc = [
            r for r in cm.records
            if r.levelname == "ERROR" and r.exc_info is not None
        ]
        self.assertGreaterEqual(
            len(errors_with_exc), 1,
            "Real DB error must produce a log.exception traceback for diagnosis",
        )
        # Server returns 500.
        self.assertEqual(status, 500)

    @patch("web.server._try_reconnect_db")
    def test_other_exception_in_handler_still_triggers_reconnect(self, mock_reconnect):
        """Unchanged-behaviour regression guard: a real handler bug
        (e.g. ValueError) still hits the catch-all. Narrowing this
        further (so non-DB exceptions skip the reconnect) is explicitly
        out of scope per the plan — see follow-up #234."""
        self.mock_db.update_request_fields.side_effect = ValueError("simulated handler bug")
        with self.assertLogs("cratedigger-web", level="ERROR") as cm:
            status, _ = self._post("/api/pipeline/set-intent",
                                   {"id": 100, "intent": "default"})
        self.assertEqual(
            mock_reconnect.call_count, 1,
            "Existing broad catch-all behaviour preserved: any non-disconnect "
            "exception still triggers _try_reconnect_db. Narrowing is deferred to #234.",
        )
        errors_with_exc = [
            r for r in cm.records
            if r.levelname == "ERROR" and r.exc_info is not None
        ]
        self.assertGreaterEqual(len(errors_with_exc), 1)
        self.assertEqual(status, 500)

    @patch("web.server._try_reconnect_db")
    def test_normal_post_no_reconnect_no_warning(self, mock_reconnect):
        """Happy path regression guard: a successful POST does not
        trigger any reconnect or disconnect-warning side effects."""
        # mock_db.set_intent_for_request returns True by default per setUp.
        # Use assertNoLogs (Python 3.10+) to assert no WARNING/ERROR records.
        # Some logger setups still emit INFO; we only care that no WARNING
        # for "Client disconnect" appears and no ERROR is emitted.
        with self.assertLogs("cratedigger-web", level="DEBUG") as cm:
            # assertLogs requires at least one record; a trivial DEBUG log
            # ensures the context manager is satisfied even on a quiet path.
            logging.getLogger("cratedigger-web").debug("test marker: normal POST path")
            status, _ = self._post("/api/pipeline/set-intent",
                                   {"id": 100, "intent": "default"})
        self.assertEqual(mock_reconnect.call_count, 0)
        disconnect_warnings = [
            r for r in cm.records
            if r.levelname == "WARNING" and "Client disconnect" in r.getMessage()
        ]
        self.assertEqual(len(disconnect_warnings), 0,
            "Normal POST must not emit a disconnect WARNING")
        errors = [r for r in cm.records if r.levelname == "ERROR"]
        self.assertEqual(len(errors), 0,
            f"Normal POST must not produce ERROR logs, got: "
            f"{[r.getMessage() for r in errors]}")


class TestBeetsDistanceRouteContract(_WebServerCase):
    """Contract for ``GET /api/beets-distance/<download_log_id>/<mbid>``.

    Service-layer correctness is covered by ``tests.test_beets_distance``.
    Here we pin the HTTP wrapper: every ``BeetsDistanceResult.outcome``
    maps to the documented status code, every required response field
    is present, and the route is registered (the
    ``TestRouteContractAudit`` guard catches missing classification).

    The service function is patched at its import site
    (``web.routes.pipeline.compute_beets_distance``-equivalent — actually
    imported lazily inside the handler so we patch the module
    attribute) and we drive each outcome through the wrapper. The real
    beets distance pipeline is not exercised in this class; the
    integration slice in ``tests.test_beets_distance`` is the
    authority on that.
    """

    REQUIRED_FIELDS = {
        "outcome",
        "distance",
        "matched_tracks",
        "total_local_tracks",
        "total_mb_tracks",
        "extra_local_tracks",
        "extra_mb_tracks",
        "components",
        "request_release_group_id",
        "candidate_release_group_id",
        "candidate_mbid",
        "download_log_id",
        "request_id",
        "folder_path",
        "error_message",
        "duration_ms",
    }

    UUID_A = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    UUID_B = "12345678-1234-1234-1234-123456789abc"

    def setUp(self) -> None:
        self.mock_db.reset_mock()
        from lib.beets_distance import BeetsDistanceResult
        self._Result = BeetsDistanceResult

    def _patch_service(self, **kwargs):
        from unittest.mock import patch as _patch
        return _patch(
            "lib.beets_distance.compute_beets_distance",
            return_value=self._Result(**kwargs),
        )

    def test_ok_returns_200_with_distance_and_required_fields(self):
        with self._patch_service(
            outcome="ok",
            distance=0.07,
            matched_tracks=12,
            total_local_tracks=12,
            total_mb_tracks=12,
            extra_local_tracks=0,
            extra_mb_tracks=0,
            components={"album": 0.0, "artist": 0.0},
            request_release_group_id="rg-1",
            candidate_release_group_id="rg-1",
            candidate_mbid=self.UUID_A,
            download_log_id=100,
            request_id=7,
            folder_path="/tmp/x",
            duration_ms=8,
        ):
            status, data = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS,
                                "beets-distance ok response")
        self.assertEqual(data["outcome"], "ok")
        self.assertAlmostEqual(data["distance"], 0.07, places=4)
        self.assertEqual(data["matched_tracks"], 12)

    def test_download_log_not_found_returns_404(self):
        with self._patch_service(
            outcome="download_log_not_found",
            download_log_id=999,
            candidate_mbid=self.UUID_A,
            error_message="download_log #999 not found",
        ):
            status, _ = self._get(f"/api/beets-distance/999/{self.UUID_A}")
        self.assertEqual(status, 404)

    def test_request_not_found_returns_404(self):
        with self._patch_service(
            outcome="request_not_found",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="request #7 not found",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 404)

    def test_wrong_release_group_returns_422(self):
        """The cross-RG guardrail surfaces as 422 (semantic violation)."""
        with self._patch_service(
            outcome="wrong_release_group",
            download_log_id=100,
            request_id=7,
            request_release_group_id="rg-source",
            candidate_release_group_id="rg-other",
            candidate_mbid=self.UUID_A,
            error_message="MBID is in a different release group",
        ):
            status, data = self._get(
                f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 422)
        self.assertEqual(data["outcome"], "wrong_release_group")

    def test_mb_no_release_group_returns_422(self):
        with self._patch_service(
            outcome="mb_no_release_group",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="MB release has no release_group_id",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 422)

    def test_folder_missing_returns_410(self):
        with self._patch_service(
            outcome="folder_missing",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="failed_path is gone",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 410)

    def test_no_audio_returns_410(self):
        with self._patch_service(
            outcome="no_audio",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            folder_path="/tmp/empty",
            error_message="no readable audio files",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 410)

    def test_mb_lookup_failed_returns_503(self):
        with self._patch_service(
            outcome="mb_lookup_failed",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="MB mirror unreachable",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 503)

    def test_distance_failed_returns_500(self):
        with self._patch_service(
            outcome="distance_failed",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="beets blew up",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 500)

    def test_route_pattern_requires_uuid_shape(self):
        """The route pattern only matches MBID UUIDs — a malformed
        MBID (e.g. a Discogs numeric id) doesn't even hit the handler.

        This is a route-table contract: keep the regex strict so we
        never accidentally compute a distance against a non-MB id.
        """
        # Numeric id (Discogs-shaped) — pattern shouldn't match.
        status, _ = self._get("/api/beets-distance/100/2048516")
        self.assertEqual(status, 404)


class TestTriageRouteContracts(_WebServerCase):
    """U17 contracts for ``GET /api/triage/<id>`` and ``GET /api/triage/list``.

    Both endpoints wrap ``lib.triage_service`` (U15) — the same service
    layer ``pipeline-cli triage show/list`` (U16) wraps. The wire shape
    on the cohort + composition payloads is the
    ``msgspec.to_builtins(TriageResult)`` shape verbatim, so the same
    Struct round-trips through ``msgspec.convert`` on both sides (CLI
    ⇄ API surface symmetry).

    Tests drive the real ``compose_triage_for_request`` and
    ``list_triage`` paths against a real :class:`FakePipelineDB`
    (reached via ``self.mock_db._fake``) — no service-layer mocking,
    per ``code-quality.md`` § MOCKS: LEAF-SEAM ONLY. Seeded rows use
    production-shape values: ``datetime.datetime`` for timestamps via
    ``make_request_row``'s defaults, real ``FieldResolutionRow`` /
    ``SearchLogRow`` via the typed seed helpers.
    """

    # The frontend triage drawer renders these top-level fields out of
    # ``msgspec.to_builtins(TriageResult)``. Pin every one so a future
    # field rename can't silently break the JS without flipping a test.
    SHOW_REQUIRED_FIELDS = {
        "request_meta", "unfindable", "field_quality", "search_forensics",
    }

    # ``request_meta`` fields the frontend depends on for the "Artist –
    # Album (year) #N" header + identity probes (failure_class, source,
    # search_filetype_override).
    SHOW_REQUEST_META_FIELDS = {
        "id", "artist_name", "album_title", "year", "status", "source",
        "mb_release_id", "discogs_release_id", "release_group_year",
        "is_va_compilation", "catalog_number", "failure_class",
        "search_filetype_override",
    }

    LIST_REQUIRED_FIELDS = {"results", "next_after", "page_size", "filter"}

    # MagicMock attribute names whose pre-set ``.return_value`` /
    # ``.side_effect`` from ``_make_server`` would short-circuit a call
    # to the wrapped fake. We need the triage path to hit the fresh
    # FakePipelineDB on every method ``compose_triage_for_request`` /
    # ``list_triage`` touches, so each test resets the relevant child
    # mocks to forwarding ``side_effect`` lambdas that call through to
    # the fresh backing fake.
    _TRIAGE_DB_METHODS = (
        "get_request",
        "list_triage_page",
        "get_field_resolutions_for_requests",
        "get_search_summaries_for_requests",
        "get_recent_search_log_for_requests",
    )

    def setUp(self) -> None:
        # Each test gets its own FakePipelineDB so seeded rows from one
        # test never bleed into the next. Re-wrap the harness so the
        # MagicMock layer keeps recording but `._fake` points at the
        # fresh fake.
        fresh = FakePipelineDB()
        self._old_backing = self.mock_db._fake
        self.mock_db._mock_wraps = fresh
        self.mock_db._fake = fresh
        # Snapshot the pre-existing child-mock state for the methods
        # the triage service touches, then force them to forward to the
        # fresh fake. Without this, _make_server's static
        # ``mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST``
        # would short-circuit every composed triage to request_id=100.
        self._triage_method_state: dict[str, MagicMock] = {}
        for name in self._TRIAGE_DB_METHODS:
            self._triage_method_state[name] = getattr(self.mock_db, name)
            forwarder = MagicMock(side_effect=getattr(fresh, name))
            setattr(self.mock_db, name, forwarder)

    def tearDown(self) -> None:
        for name, prev in self._triage_method_state.items():
            setattr(self.mock_db, name, prev)
        self.mock_db._mock_wraps = self._old_backing
        self.mock_db._fake = self._old_backing

    @property
    def _fake(self) -> "FakePipelineDB":
        return self.mock_db._fake

    # --- /api/triage/<id> -------------------------------------------------

    def test_show_returns_200_with_required_fields_and_roundtrips(self):
        """Happy path: a seeded request composes through to a 200 with
        the full TriageResult shape, and the response body round-trips
        through ``msgspec.convert(payload, type=TriageResult)`` — the
        wire-boundary contract per CLI ⇄ API symmetry."""
        from lib.triage_service import TriageResult
        self._fake.seed_request(make_request_row(
            id=4242,
            artist_name="Triage Artist",
            album_title="Triage Album",
            status="wanted",
            failure_class="search_not_converting",
            unfindable_category="artist_absent",
            unfindable_categorised_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            last_artist_probe_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
            last_artist_probe_match_count=0,
        ))

        status, data = self._get("/api/triage/4242")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SHOW_REQUIRED_FIELDS,
                                "triage show response")
        _assert_required_fields(self, data["request_meta"],
                                self.SHOW_REQUEST_META_FIELDS,
                                "triage show request_meta")
        # The wire shape is exactly the Struct shape — round-trip proves
        # no field drift / coercion happened at the boundary.
        composed = msgspec.convert(data, type=TriageResult)
        self.assertEqual(composed.request_meta.id, 4242)
        self.assertEqual(composed.request_meta.artist_name, "Triage Artist")
        self.assertEqual(
            composed.request_meta.failure_class, "search_not_converting",
        )
        # Unfindable struct populated because the seeded row has signals.
        self.assertIsNotNone(composed.unfindable)
        assert composed.unfindable is not None
        self.assertEqual(composed.unfindable.category, "artist_absent")

    def test_show_returns_404_when_request_id_missing(self):
        """Unknown request id → 404 with ``error`` + ``request_id`` in body
        so the frontend can surface "not found" with the right id."""
        status, data = self._get("/api/triage/99999")
        self.assertEqual(status, 404)
        self.assertIn("error", data)
        self.assertEqual(data["request_id"], 99999)

    def test_show_returns_404_for_non_int_path(self):
        """A non-numeric path segment doesn't even match the regex
        (which requires ``\\d+``), so the route table itself replies
        404. This test pins the route-table contract (no silent
        coercion to a different handler)."""
        status, _ = self._get("/api/triage/not-an-int")
        # The regex r"^/api/triage/(\d+)$" does not match — falls
        # through to the catch-all 404.
        self.assertEqual(status, 404)

    # --- /api/triage/list --------------------------------------------------

    def test_list_filter_unfindable_returns_200_with_required_fields(self):
        """A seeded unfindable request shows up under
        ``filter=unfindable`` with the documented envelope shape."""
        from lib.triage_service import TriageResult
        self._fake.seed_request(make_request_row(
            id=10, artist_name="Stuck Artist",
            unfindable_category="artist_absent",
            unfindable_categorised_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
        ))
        # Decoy row without any unfindable signal — must NOT appear in
        # the filtered cohort.
        self._fake.seed_request(make_request_row(
            id=11, artist_name="Healthy Artist", status="imported",
        ))

        status, data = self._get("/api/triage/list?filter=unfindable")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.LIST_REQUIRED_FIELDS,
                                "triage list response")
        self.assertEqual(data["filter"], "unfindable")
        self.assertEqual(data["page_size"], 50)
        # Only the unfindable row should be returned.
        self.assertEqual(len(data["results"]), 1)
        composed = msgspec.convert(data["results"][0], type=TriageResult)
        self.assertEqual(composed.request_meta.id, 10)
        # Page is shorter than page_size → next_after is None
        # (cohort exhausted).
        self.assertIsNone(data["next_after"])

    def test_list_filter_data_quality_status_filters_by_status_column(self):
        """``filter=data_quality:status=<status>`` (issue #374 canonical
        form) returns only requests with at least one
        ``album_request_field_resolutions`` row whose ``status`` column
        matches the spec. Mirrors what
        ``lib/field_resolver_service.py::_classify_lookup_exception``
        actually writes."""
        # Seeded request A: has a release_group_year resolution in the
        # sticky 4xx-client bucket — matches.
        self._fake.seed_request(make_request_row(id=20))
        self._fake.record_field_resolution(
            request_id=20, field_name="release_group_year",
            status="unresolved_4xx_client", reason_code="http_400",
        )
        # Seeded request B: has a field resolution but a different
        # status bucket — must NOT appear.
        self._fake.seed_request(make_request_row(id=21))
        self._fake.record_field_resolution(
            request_id=21, field_name="catalog_number",
            status="unresolved_mirror_unavailable",
            reason_code="ConnectionError",
        )

        status, data = self._get(
            "/api/triage/list?filter=data_quality:status=unresolved_4xx_client"
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            data["filter"], "data_quality:status=unresolved_4xx_client",
        )
        self.assertEqual(len(data["results"]), 1)
        self.assertEqual(data["results"][0]["request_meta"]["id"], 20)

    def test_list_filter_data_quality_reason_filters_by_reason_code(self):
        """``filter=data_quality:reason=<code>`` filters on the
        ``reason_code`` column (HTTP code specifier — http_400,
        http_410, http_422, etc.)."""
        # Seeded request A: 4xx-client status, reason_code=http_400 — matches.
        self._fake.seed_request(make_request_row(id=22))
        self._fake.record_field_resolution(
            request_id=22, field_name="release_group_year",
            status="unresolved_4xx_client", reason_code="http_400",
        )
        # Seeded request B: 4xx-client status but reason_code=http_410 — excluded.
        self._fake.seed_request(make_request_row(id=23))
        self._fake.record_field_resolution(
            request_id=23, field_name="catalog_number",
            status="unresolved_4xx_client", reason_code="http_410",
        )

        status, data = self._get(
            "/api/triage/list?filter=data_quality:reason=http_400"
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["filter"], "data_quality:reason=http_400")
        self.assertEqual(len(data["results"]), 1)
        self.assertEqual(data["results"][0]["request_meta"]["id"], 22)

    def test_list_invalid_filter_returns_400_with_valid_filters_array(self):
        """An unparseable filter spec surfaces as a 400 carrying
        ``error`` + a ``valid_filters`` array, so the operator can
        self-correct without leaving the network response."""
        status, data = self._get("/api/triage/list?filter=garbage_value")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertIn("valid_filters", data)
        self.assertIsInstance(data["valid_filters"], list)
        # The four canonical scalar forms must be advertised.
        self.assertIn("all", data["valid_filters"])
        self.assertIn("unfindable", data["valid_filters"])
        self.assertIn("data_quality", data["valid_filters"])
        self.assertIn("search_not_converting", data["valid_filters"])

    def test_list_limit_caps_results_and_emits_next_after_cursor(self):
        """When the page is exactly ``limit`` long the response carries
        ``next_after`` = last request_id so the operator can paginate."""
        for rid in (30, 31, 32):
            self._fake.seed_request(make_request_row(
                id=rid, status="imported",
            ))

        status, data = self._get("/api/triage/list?filter=all&limit=2")

        self.assertEqual(status, 200)
        self.assertEqual(data["page_size"], 2)
        self.assertEqual(len(data["results"]), 2)
        # Page was full → next_after is the last id in the page.
        self.assertEqual(data["next_after"],
                         data["results"][-1]["request_meta"]["id"])

    def test_list_default_filter_when_query_string_omitted(self):
        """Missing ``filter=`` defaults to ``all`` so a bare hit on
        ``/api/triage/list`` is meaningful."""
        self._fake.seed_request(make_request_row(id=40, status="imported"))

        status, data = self._get("/api/triage/list")

        self.assertEqual(status, 200)
        self.assertEqual(data["filter"], "all")
        self.assertGreaterEqual(len(data["results"]), 1)

    def test_list_rejects_non_int_limit(self):
        status, data = self._get("/api/triage/list?filter=all&limit=abc")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_list_rejects_out_of_bounds_limit(self):
        status, data = self._get("/api/triage/list?filter=all&limit=500")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_list_rejects_non_int_after(self):
        status, data = self._get(
            "/api/triage/list?filter=all&after=not-an-int")
        self.assertEqual(status, 400)
        self.assertIn("error", data)


class TestYoutubeRouteContracts(_WebServerCase):
    """U8 contract for ``GET /api/youtube-album?identifier=<id>``.

    Mirrors the CLI surface ``pipeline-cli youtube-album`` (U7); the
    route is the HTTP adapter wrapping
    ``lib.youtube_album_service.resolve_youtube_album``. The
    service-layer behaviour is the authority — these tests pin the
    HTTP-side contract: required response fields, the
    ``OUTCOME_HTTP_STATUS`` mapping (re-exported from the service),
    400 on missing ``identifier``, and the ``?refresh=true`` query
    forwarded to the service as ``refresh=True``.

    The service is patched at the route module's import site
    (``web.routes.youtube.resolve_youtube_album``) with fixture
    ``YoutubeAlbumResolverResult`` instances — production-shaped per
    the contract-test-mocks-must-mirror-production-shape rule (real
    typed Structs, not bare dicts).
    """

    REQUIRED_FIELDS = {
        "outcome",
        "release_group_identifier",
        "source",
        "from_cache",
        "youtube_releases",
        "error_message",
        "duration_ms",
    }

    REQUIRED_RELEASE_FIELDS = {
        "yt_browse_id",
        "yt_audio_playlist_id",
        "yt_url",
        "year",
        "track_count",
        "tracks",
        "distances",
    }

    REQUIRED_DISTANCE_FIELDS = {
        "mbid",
        "outcome",
        "distance",
        "components",
        "matched_tracks",
        "total_local_tracks",
        "total_mb_tracks",
        "extra_local_tracks",
        "extra_mb_tracks",
        "error_message",
    }

    UUID_A = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    UUID_B = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"

    def setUp(self) -> None:
        self.mock_db.reset_mock()
        from lib.youtube_album_service import (
            ResolvedDistance,
            ResolvedYoutubeRelease,
            YoutubeAlbumResolverResult,
        )
        from lib.beets_distance import SyntheticItem
        self._Result = YoutubeAlbumResolverResult
        self._Release = ResolvedYoutubeRelease
        self._Distance = ResolvedDistance
        self._SyntheticItem = SyntheticItem

    def _ok_result(self, *, from_cache: bool = False,
                   error_message: str | None = None):
        """Production-shaped ``ok`` result with one YT release × one MB
        sibling — exercises every required field on the wire."""
        track = self._SyntheticItem(
            title="Reckoner", artist="Radiohead", album="In Rainbows",
            albumartist="Radiohead", track=8, tracktotal=10,
            disc=1, disctotal=1, length=290.0,
        )
        distance = self._Distance(
            mbid=self.UUID_A,
            outcome="ok",
            distance=0.05,
            components={"album": 0.0, "artist": 0.0, "tracks": 0.05},
            matched_tracks=10,
            total_local_tracks=10,
            total_mb_tracks=10,
            error_message=None,
        )
        release = self._Release(
            yt_browse_id="MPREb_aaa",
            yt_audio_playlist_id="OLAK5uy_aaa",
            yt_url="https://music.youtube.com/playlist?list=OLAK5uy_aaa",
            year=2007,
            track_count=10,
            tracks=[track],
            distances=[distance],
        )
        return self._Result(
            outcome="ok",
            release_group_identifier="rg-1234",
            source="mb",
            from_cache=from_cache,
            youtube_releases=[release],
            error_message=error_message,
            duration_ms=42,
        )

    def _bare_result(self, outcome: str, *,
                     error_message: str | None = None):
        """Outcome-only result (no matrix) for failure-mode tests."""
        return self._Result(
            outcome=outcome,
            release_group_identifier=None,
            source=None,
            from_cache=False,
            youtube_releases=[],
            error_message=error_message,
            duration_ms=12,
        )

    @contextlib.contextmanager
    def _patch_service(self, return_value):
        """Patch the resolver call AND the collaborator constructors.

        The route handler constructs ``YTMusic`` and ``_RedisYoutubeCache``
        *before* calling ``resolve_youtube_album`` so the production path
        wires everything up cleanly. In the contract test we only care
        about the service call's return value, so we stub the
        construction helpers to return harmless sentinels. This also
        makes the test robust to other tests that may have
        monkey-patched ``requests.Session`` (ytmusicapi runs
        ``isinstance(requests_session, requests.Session)`` at
        construction time and crashes when the Session class is not a
        real type).

        ``_build_youtube_client`` returns ``(yt_client, session)`` so the
        route can close the session in ``finally`` (finding #18 — Session
        leak). The test stub mimics that shape; the fake session exposes
        a no-op ``close()`` method.
        """
        class _FakeSession:
            close_calls = 0

            def close(self) -> None:
                # Class-level counter so the test fixture can assert
                # close() was actually called (round 2 P2-2). Without
                # this, the close() helper in the route module could
                # be deleted and no test would catch it.
                type(self).close_calls += 1
                return None

        # Reset the counter for each ``_patch_service`` invocation so
        # close-count assertions are scoped to one test call.
        _FakeSession.close_calls = 0
        self._fake_session_cls = _FakeSession

        with patch(
            "web.routes.youtube._build_youtube_client",
            return_value=(object(), _FakeSession()),
        ), patch(
            "web.routes.youtube._RedisYoutubeCache",
            return_value=object(),
        ), patch(
            "web.routes.youtube.resolve_youtube_album",
            return_value=return_value,
        ) as mock_resolve:
            yield mock_resolve

    def test_status_mapping_is_imported_from_service(self):
        """``web.routes.youtube`` must re-export ``OUTCOME_HTTP_STATUS``
        from ``lib.youtube_album_service`` — single source of truth per
        the PR #381 lesson."""
        from web.routes import youtube as route_mod
        from lib import youtube_album_service as svc_mod
        self.assertIs(
            route_mod.OUTCOME_HTTP_STATUS,
            svc_mod.OUTCOME_HTTP_STATUS,
        )

    def test_ok_returns_200_with_required_fields(self):
        with self._patch_service(self._ok_result()):
            status, data = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS,
                                "youtube-album ok response")
        self.assertEqual(data["outcome"], "ok")
        self.assertEqual(data["source"], "mb")
        self.assertFalse(data["from_cache"])
        self.assertEqual(len(data["youtube_releases"]), 1)
        _assert_required_fields(
            self, data["youtube_releases"][0],
            self.REQUIRED_RELEASE_FIELDS,
            "youtube_releases[0] entry",
        )
        self.assertEqual(len(data["youtube_releases"][0]["distances"]), 1)
        _assert_required_fields(
            self, data["youtube_releases"][0]["distances"][0],
            self.REQUIRED_DISTANCE_FIELDS,
            "distances[0] entry",
        )

    def test_ok_from_cache_with_error_message_still_200(self):
        """AE6: cache fallback path — service returns ``ok`` with
        ``from_cache=True`` and a non-empty ``error_message`` (the YT
        upstream failed but the cache served a useful result). The
        route returns 200 because the matrix is real."""
        with self._patch_service(self._ok_result(
                from_cache=True,
                error_message="YT 429 — served from cache")):
            status, data = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        self.assertTrue(data["from_cache"])
        self.assertEqual(data["error_message"],
                         "YT 429 — served from cache")

    def test_missing_identifier_returns_400(self):
        status, data = self._get("/api/youtube-album")
        self.assertEqual(status, 400)
        self.assertEqual(
            data.get("error"),
            "identifier query parameter is required",
        )

    def test_empty_identifier_returns_400(self):
        status, data = self._get("/api/youtube-album?identifier=")
        self.assertEqual(status, 400)
        self.assertEqual(
            data.get("error"),
            "identifier query parameter is required",
        )

    def test_not_found_returns_404(self):
        with self._patch_service(self._bare_result(
                "not_found",
                error_message="identifier 'nope' is neither MB nor Discogs")):
            status, data = self._get(
                "/api/youtube-album?identifier=nope")
        self.assertEqual(status, 404)
        self.assertEqual(data["outcome"], "not_found")

    def test_no_release_group_returns_422(self):
        # Service outcome renamed from ``mb_no_release_group`` to
        # ``no_release_group`` per ce-code-review finding #12 — the old
        # name was MB-specific but the Discogs path also produced it.
        with self._patch_service(self._bare_result(
                "no_release_group",
                error_message="MB release has no release_group_id")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 422)

    def test_unresolved_4xx_client_returns_503(self):
        with self._patch_service(self._bare_result(
                "unresolved_4xx_client",
                error_message="YT 429 throttled")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_unresolved_mirror_unavailable_returns_503(self):
        with self._patch_service(self._bare_result(
                "unresolved_mirror_unavailable",
                error_message="YT 503")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_unresolved_timeout_returns_503(self):
        with self._patch_service(self._bare_result(
                "unresolved_timeout",
                error_message="requests.Timeout")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_youtube_parse_failed_returns_503(self):
        with self._patch_service(self._bare_result(
                "youtube_parse_failed",
                error_message="ytmusicapi parse error")):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 503)

    def test_refresh_true_is_forwarded_to_service(self):
        """AE5: ``?refresh=true`` must reach the service as
        ``refresh=True`` so the cache bypass actually happens."""
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}&refresh=true")
        self.assertEqual(status, 200)
        self.assertEqual(mock_resolve.call_count, 1)
        kwargs = mock_resolve.call_args.kwargs
        self.assertIs(kwargs["refresh"], True)

    def test_refresh_omitted_defaults_to_false(self):
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        kwargs = mock_resolve.call_args.kwargs
        self.assertIs(kwargs["refresh"], False)

    def test_refresh_false_string_is_not_truthy(self):
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}&refresh=false")
        self.assertEqual(status, 200)
        kwargs = mock_resolve.call_args.kwargs
        self.assertIs(kwargs["refresh"], False)

    def test_identifier_is_forwarded_to_service(self):
        with self._patch_service(self._ok_result()) as mock_resolve:
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        # First positional arg is the identifier.
        self.assertEqual(mock_resolve.call_args.args[0], self.UUID_A)

    def test_session_close_called_on_happy_path(self):
        """Round 2 P2-2: the route's ``finally`` block must call
        ``session.close()`` so the requests Session's connection pool
        is released (finding #18). Without an assertion here, a
        regression that removed the close call would not trip any
        existing test.
        """
        with self._patch_service(self._ok_result()):
            status, _ = self._get(
                f"/api/youtube-album?identifier={self.UUID_A}")
        self.assertEqual(status, 200)
        self.assertEqual(
            self._fake_session_cls.close_calls, 1,
            msg="route must call session.close() exactly once on "
                "happy-path resolves (round 2 P2-2)",
        )

    def test_session_close_called_when_service_raises(self):
        """If ``resolve_youtube_album`` raises mid-request, the
        ``finally`` clause still releases the session — the route
        must not leak a connection pool because of an exception.
        """
        from lib.youtube_album_service import OUTCOME_HTTP_STATUS  # noqa: F401

        class _FakeSession:
            close_calls = 0

            def close(self) -> None:
                type(self).close_calls += 1
                return None

        _FakeSession.close_calls = 0

        def _raising_resolver(*_a, **_kw):
            raise RuntimeError("simulated mid-request failure")

        with patch(
            "web.routes.youtube._build_youtube_client",
            return_value=(object(), _FakeSession()),
        ), patch(
            "web.routes.youtube._RedisYoutubeCache",
            return_value=object(),
        ), patch(
            "web.routes.youtube.resolve_youtube_album",
            side_effect=_raising_resolver,
        ):
            # The route will 500 because the resolver raised; we only
            # care that the session was still closed.
            try:
                self._get(f"/api/youtube-album?identifier={self.UUID_A}")
            except Exception:
                pass

        self.assertEqual(
            _FakeSession.close_calls, 1,
            msg="route must close the session even when the resolver "
                "raises mid-request (round 2 P2-2)",
        )


if __name__ == "__main__":
    unittest.main()
