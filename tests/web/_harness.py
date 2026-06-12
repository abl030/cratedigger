#!/usr/bin/env python3
"""Shared HTTP test harness for the web route contract tests (#408).

Starts a real HTTP server on a random port; the ``tests/web/test_*.py``
modules verify response codes, JSON structure, and error handling
against it. Two base classes during the #430 migration:

- :class:`_FakeDbWebServerCase` — per-test bare ``FakePipelineDB``.
  Subclass THIS for new and migrated modules.
- :class:`_WebServerCase` — legacy class-level MagicMock-wrapped DB
  with canned ``.return_value`` defaults. Dies when the ratchet
  baseline in ``tests/_mock_audit_scanner.py`` is empty.
"""

import copy
from datetime import datetime, timezone
import json
import os
import sys
import threading
import unittest
from http.server import HTTPServer, ThreadingHTTPServer
from unittest.mock import MagicMock, patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Eager import: lib.beets_distance pins the real ``beets`` package
# (see lib/beets_distance.py:49-55) — we must trigger it *before* the
# next two ``sys.path.insert`` calls add ``lib/`` ahead of site-
# packages, otherwise downstream imports of lib.youtube_album_service
# (which imports lib.beets_distance lazily inside the route handler)
# fail with "cannot import name 'library' from 'beets'" because
# ``beets`` would resolve to ``lib/beets.py``.
import lib.beets_distance  # noqa: F401,E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "web"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "lib"))

from lib.import_queue import ImportJob
from tests.fakes import FakePipelineDB
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


class _FakeDbWebServerCase(_WebServerCase):
    """Contract-test base with a bare per-test :class:`FakePipelineDB`.

    Migration target for #430. ``setUp`` installs a fresh fake as
    ``web.server.db`` (the same module-global swap production uses for
    DSN-less handles — ``_db()`` returns it directly), so every test
    starts from empty typed state. Tests seed what they need
    (``self.db.seed_request(...)``, ``self.db.log_download(...)``,
    ``self.db.update_status(...)``) and assertions hit the fake's real
    query semantics — there is no MagicMock layer to shape-match.

    The inherited class-level ``mock_db`` (legacy MagicMock harness)
    must not be touched in subclasses — the ratchet in
    ``tests/_mock_audit_scanner.py`` is the guard that enforces this.
    Note the textual guard is load-bearing: configuration of the
    disconnected mock fails loudly at the route, but a negative
    assertion (``assert_not_called``) against it would pass VACUOUSLY
    (the mock genuinely receives no calls), so don't rely on runtime
    behaviour to catch accidental use.
    """

    db: FakePipelineDB

    def setUp(self) -> None:
        super().setUp()
        import web.server as srv
        self.db = FakePipelineDB()
        patcher = patch.object(srv, "db", self.db)
        patcher.start()
        self.addCleanup(patcher.stop)


def _fresh_triage_runner(case: unittest.TestCase):
    """Swap in a fresh runner so triage tests don't share sweep state."""
    from web import triage_runner as triage_runner_module
    from web.routes import imports as imports_module
    previous = imports_module._triage_runner
    runner = triage_runner_module.TriageRunner()
    imports_module._triage_runner = runner
    case.addCleanup(
        setattr, imports_module, "_triage_runner", previous,
    )
    return runner


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
    the mock. Transitional (#430): migrated route modules subclass
    :class:`_FakeDbWebServerCase` instead; this helper dies with the last
    ratchet entry in ``tests/_mock_audit_scanner.py``.
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
            "search_filetype_override": None, "source": "slskd",
            "request_source": "request",
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
    mock_db.get_latest_download_summaries.return_value = {}
    mock_db.search_requests.return_value = []
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
            "source": "slskd", "request_source": "request",
            "youtube_metadata": None,
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
        "peers": {
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
                "known_peers": 220,
                "new_24h": 80,
                "seen_24h": 95,
                "tracked_since": "2026-05-04T00:00:00+00:00",
            },
            "days": [
                {
                    "date": "2026-05-05",
                    "new_peers": 22,
                    "total_peers": 220,
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

    # Mirror production: ThreadingHTTPServer + the same Handler.
    server = ThreadingHTTPServer(("127.0.0.1", 0), srv.Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port, mock_db


